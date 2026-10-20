use soulsteads_messaging::{config, core, runtime};

pub mod actual_web {
    include!("../src/web.rs");

    #[cfg(test)]
    mod delivery_tests {
        use super::*;
        use std::{collections::BTreeMap, time::Duration};

        #[derive(Clone, Copy)]
        enum Failure {
            Absent,
            Closed,
            Saturated,
        }

        fn bridge(root: &std::path::Path, actions: Value) -> Arc<Bridge> {
            let script = root.join("script.json");
            std::fs::write(&script, actions.to_string()).unwrap();
            let configuration = crate::config::load(
                &json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":root.join("counter")}}}).to_string(),
                &BTreeMap::new(),
            ).unwrap();
            Arc::new(Bridge::new(configuration, BTreeMap::new(), root.join("sessions")).unwrap())
        }

        fn sink(bridge: Arc<Bridge>) -> (WebSink, mpsc::Receiver<Value>) {
            let (sender, receiver) = mpsc::channel(64);
            let client = "delivery-test".to_owned();
            let sockets = HashMap::from([(client.clone(), (uuid::Uuid::new_v4(), sender))]);
            (
                WebSink {
                    state: WebState {
                        bridge,
                        config: json!({}),
                        clients: Arc::new(AtomicUsize::new(0)),
                        sockets: Arc::new(Mutex::new(sockets)),
                    },
                    client,
                    presentation: Arc::default(),
                },
                receiver,
            )
        }

        async fn fail_queue(sink: &WebSink, failure: Failure) -> Option<mpsc::Receiver<Value>> {
            let mut sockets = sink.state.sockets.lock().await;
            match failure {
                Failure::Absent => {
                    sockets.remove(&sink.client);
                    None
                }
                Failure::Closed => {
                    let (sender, receiver) = mpsc::channel(1);
                    drop(receiver);
                    sockets.insert(sink.client.clone(), (uuid::Uuid::new_v4(), sender));
                    None
                }
                Failure::Saturated => {
                    let (sender, receiver) = mpsc::channel(1);
                    sender.try_send(json!({"type":"sentinel"})).unwrap();
                    sockets.insert(sink.client.clone(), (uuid::Uuid::new_v4(), sender));
                    Some(receiver)
                }
            }
        }

        async fn assert_emit_failure(failure: Failure, expected: &str) {
            let root = tempfile::tempdir().unwrap();
            let (sink, _receiver) = sink(bridge(root.path(), json!([])));
            let mut blocked = fail_queue(&sink, failure).await;
            let started = tokio::time::Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(8),
                sink.emit(json!({"type":"response","text":"not admitted"})),
            )
            .await
            .expect("emit must remain bounded");
            assert!(result.is_err(), "failed queue must not report success");
            assert!(result.unwrap_err().to_string().contains(expected));
            if let Some(receiver) = blocked.as_mut() {
                assert!(started.elapsed() >= Duration::from_secs(5));
                assert_eq!(receiver.try_recv().unwrap(), json!({"type":"sentinel"}));
                assert!(
                    receiver.try_recv().is_err(),
                    "timed-out send must not enqueue later"
                );
            }
        }

        #[tokio::test]
        async fn web_owned_turn_sink_preserves_recovered_suffix_and_distinct_fallback() {
            let root = tempfile::tempdir().unwrap();
            let (sink, mut receiver) = sink(bridge(root.path(), json!([])));
            sink.emit(json!({"type":"thinking"})).await.unwrap();
            sink.owned()
                .unwrap()
                .emit(json!({"type":"chunk","text":"captured "}))
                .await
                .unwrap();
            sink.emit(json!({"type":"response","text":"captured recovered"}))
                .await
                .unwrap();
            sink.emit(json!({"type":"response","text":"distinct fallback"}))
                .await
                .unwrap();
            let mut events = Vec::new();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            assert_eq!(
                events,
                vec![
                    json!({"type":"thinking"}),
                    json!({"type":"chunk","text":"captured "}),
                    json!({"type":"chunk","text":"recovered"}),
                    json!({"type":"chunk","text":"distinct fallback"}),
                ]
            );
        }

        #[tokio::test]
        async fn web_recovered_without_chunks_uses_source_chunk_contract_but_commands_keep_response()
         {
            let root = tempfile::tempdir().unwrap();
            let (sink, mut receiver) = sink(bridge(root.path(), json!([])));
            sink.emit(json!({"type":"response","text":"command output"}))
                .await
                .unwrap();
            sink.emit(json!({"type":"thinking"})).await.unwrap();
            sink.emit(json!({"type":"response","text":"recovered text"}))
                .await
                .unwrap();
            sink.emit(json!({"type":"done"})).await.unwrap();
            let mut events = Vec::new();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            assert_eq!(
                events,
                vec![
                    json!({"type":"response","text":"command output"}),
                    json!({"type":"thinking"}),
                    json!({"type":"chunk","text":"recovered text"}),
                    json!({"type":"done"}),
                ]
            );
        }

        #[tokio::test]
        async fn web_delivery_emit_absent_socket_errors() {
            assert_emit_failure(Failure::Absent, "No active WebSocket").await;
        }

        #[tokio::test]
        async fn web_delivery_emit_closed_receiver_errors() {
            assert_emit_failure(Failure::Closed, "WebSocket output queue closed").await;
        }

        #[tokio::test]
        async fn web_delivery_emit_saturated_queue_times_out_without_late_enqueue() {
            assert_emit_failure(Failure::Saturated, "WebSocket output queue timed out").await;
        }

        struct UploadFailureSink {
            web: WebSink,
            failure: Failure,
            errors: Mutex<Vec<Option<String>>>,
        }

        #[async_trait]
        impl Sink for UploadFailureSink {
            async fn emit(&self, event: Value) -> Result<()> {
                self.web.emit(event).await
            }

            async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<()> {
                let active = self.web.state.sockets.lock().await[&self.web.client].clone();
                let mut blocked = fail_queue(&self.web, self.failure).await;
                let result = self.web.upload(name, mime, bytes).await;
                self.web
                    .state
                    .sockets
                    .lock()
                    .await
                    .insert(self.web.client.clone(), active);
                if let Some(receiver) = blocked.as_mut() {
                    assert_eq!(receiver.try_recv().unwrap(), json!({"type":"sentinel"}));
                    assert!(
                        receiver.try_recv().is_err(),
                        "failed image must not enqueue later"
                    );
                }
                self.errors
                    .lock()
                    .await
                    .push(result.as_ref().err().map(ToString::to_string));
                result
            }
        }

        async fn assert_runtime_failure(failure: Failure, expected: &str) {
            let root = tempfile::tempdir().unwrap();
            let mut bytes = Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(1, 1)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .unwrap();
            let data = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
            let bridge = bridge(
                root.path(),
                json!([
                    {"updates":[{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":"image/png","data":data,"alt":"failed image"}}}],"stopReason":"end_turn"},
                    {"text":"next answer","stopReason":"end_turn"}
                ]),
            );
            let (web, mut receiver) = sink(bridge.clone());
            let sink = UploadFailureSink {
                web,
                failure,
                errors: Mutex::new(Vec::new()),
            };
            tokio::time::timeout(
                Duration::from_secs(15),
                bridge.handle(Event::web("delivery-test", "image only"), &sink),
            )
            .await
            .unwrap()
            .unwrap();
            let mut events = Vec::new();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            let observation = events
                .iter()
                .find(|event| event["type"] == "prompt_end")
                .unwrap();
            assert_eq!(
                observation["uploadFailures"], 1,
                "actual WebSink upload failure must be counted"
            );
            assert_eq!(observation["uploadedImages"], 0);
            assert_eq!(observation["uploadedMedia"], 0);
            assert_eq!(observation["usableOutput"], false);
            assert_eq!(observation["deliveryFailed"], false);
            assert_eq!(observation["imageCount"], 1);
            assert_eq!(observation["stopReason"], "end_turn");
            assert!(!events.iter().any(|event| event["type"] == "image"));
            assert_eq!(
                std::fs::read_to_string(root.path().join("counter")).unwrap(),
                "1"
            );
            let errors = sink.errors.lock().await;
            assert_eq!(errors.len(), 1, "one actual upload attempt, no retry");
            assert!(
                errors[0]
                    .as_deref()
                    .is_some_and(|error| error.contains(expected))
            );
            drop(errors);
            bridge
                .handle(Event::web("delivery-test", "followup"), &sink)
                .await
                .unwrap();
            events.clear();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            let observation = events
                .iter()
                .find(|event| event["type"] == "prompt_end")
                .unwrap();
            assert_eq!(observation["uploadFailures"], 0);
            assert_eq!(observation["uploadedImages"], 0);
            assert_eq!(observation["imageCount"], 0);
            assert_eq!(observation["usableOutput"], true);
            assert!(
                events
                    .iter()
                    .any(|event| event["type"] == "chunk" && event["text"] == "next answer")
            );
            assert!(!events.iter().any(|event| event["type"] == "image"));
            assert_eq!(sink.errors.lock().await.len(), 1);
            assert_eq!(
                std::fs::read_to_string(root.path().join("counter")).unwrap(),
                "2"
            );
            bridge.shutdown().await.unwrap();
        }

        #[tokio::test]
        async fn web_delivery_runtime_absent_socket_counts_failure_without_replay() {
            assert_runtime_failure(Failure::Absent, "No active WebSocket").await;
        }

        #[tokio::test]
        async fn web_delivery_runtime_closed_receiver_counts_failure_without_replay() {
            assert_runtime_failure(Failure::Closed, "WebSocket output queue closed").await;
        }

        #[tokio::test]
        async fn web_delivery_runtime_saturated_queue_counts_failure_without_replay() {
            assert_runtime_failure(Failure::Saturated, "WebSocket output queue timed out").await;
        }
    }
}
