use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
struct Fixture {
    root: tempfile::TempDir,
    bridge: Arc<Bridge>,
    sink: Recorder,
}
#[derive(Clone)]
struct ScopedSink {
    recorder: Recorder,
    raw_thread: String,
    delivery: Option<Arc<tokio::sync::Semaphore>>,
    fail: bool,
}
#[async_trait]
impl Sink for ScopedSink {
    fn matrix_raw_thread(&self) -> Option<String> {
        Some(self.raw_thread.clone())
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        let approval = value["text"]
            .as_str()
            .is_some_and(|body| body.contains("/approve "));
        self.recorder.emit(value).await?;
        if approval {
            if let Some(delivery) = &self.delivery {
                delivery.acquire().await.unwrap().forget();
            }
            if self.fail {
                anyhow::bail!("Hermetic notice delivery failure");
            }
        }
        Ok(())
    }
}
impl Fixture {
    fn new(extra: Value, patch: Value, mode: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut matrix = json!({"interactivePermissions":true,"respondToAllMessages":true,"threadIsolation":false,
            "allowedUsers":["@requester:local","@other:local"],"allowedRooms":["!approval:local","!other:local"],"strictAllowedRooms":true});
        matrix
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let cfg = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
            "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],
                "env":{"APPROVAL_AUDIT":root.path().join("rpc.jsonl"),"APPROVAL_REQUEST":patch.to_string(),"APPROVAL_MODE":mode}},
            "matrix":matrix
        }).to_string(),&BTreeMap::new()).unwrap();
        let bridge =
            Arc::new(Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap());
        Self {
            root,
            bridge,
            sink: Recorder::default(),
        }
    }
    fn start(&self, origin: Event) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let bridge = self.bridge.clone();
        let sink = self.sink.clone();
        tokio::spawn(async move { bridge.handle(origin, &sink).await })
    }
    fn start_with(
        &self,
        origin: Event,
        sink: ScopedSink,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let bridge = self.bridge.clone();
        tokio::spawn(async move { bridge.handle(origin, &sink).await })
    }
    async fn notice(&self) -> String {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(text) = self
                    .sink
                    .0
                    .lock()
                    .await
                    .iter()
                    .filter_map(|value| value["text"].as_str())
                    .find(|text| text.contains("/approve "))
                {
                    break text.to_owned();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval notice")
    }
    fn audit(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("rpc.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    fn replies(&self) -> Vec<Value> {
        self.audit()
            .into_iter()
            .filter(|value| value["id"] == 900 && value.get("method").is_none())
            .collect()
    }
    async fn finish(&self, task: tokio::task::JoinHandle<anyhow::Result<()>>) {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_approval_after_ordinary_deadline_and_before_broker_expiry_reaches_acp_once() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let task = fixture.start(event("work"));
    let notice = fixture.notice().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(301)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        !task.is_finished(),
        "ordinary 300-second deadline cancelled an approval"
    );
    tokio::time::advance(Duration::from_secs(2398)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        !task.is_finished(),
        "approval expired before broker's 45-minute boundary"
    );
    tokio::time::resume();
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-once"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}

#[tokio::test]
async fn matrix_broker_expiry_after_full_window_cancels_and_rejects_late_approval() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let task = fixture.start(event("work"));
    let notice = fixture.notice().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2700)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::resume();
    fixture.finish(task).await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}

#[tokio::test]
async fn matrix_cancellation_after_ordinary_deadline_rejects_late_approval() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let task = fixture.start(event("work"));
    let notice = fixture.notice().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(301)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(!task.is_finished());
    tokio::time::resume();
    fixture
        .bridge
        .handle(event("/cancel"), &fixture.sink)
        .await
        .unwrap();
    fixture.finish(task).await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}

async fn followup_permission_decision(cancel: bool) {
    let fixture = Fixture::new(json!({}), json!({}), "followup");
    let original = fixture.start(event("original"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture
            .audit()
            .iter()
            .any(|v| v["method"] == "session/prompt")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let followup = fixture.start(event("additional instruction"));
    let notice = fixture.notice().await;
    assert!(
        !original.is_finished(),
        "original collector must retain pending follow-up"
    );
    let decision = if cancel {
        "/cancel".to_owned()
    } else {
        format!("/deny {}", request_id(&notice))
    };
    fixture
        .bridge
        .handle(event(&decision), &fixture.sink)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), original)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), followup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    let replies = fixture.replies();
    assert_eq!(replies.len(), 1);
    assert_eq!(
        replies[0]["result"]["outcome"],
        if cancel {
            json!({"outcome":"cancelled"})
        } else {
            json!({"outcome":"selected","optionId":"native-deny"})
        }
    );
    let prompts: Vec<_> = fixture
        .audit()
        .into_iter()
        .filter(|v| v["method"] == "session/prompt")
        .collect();
    assert_eq!(prompts.len(), 2, "late grant cannot replay either input");
    assert!(
        prompts
            .iter()
            .all(|v| v["params"]["sessionId"] == "approval-session")
    );
    fixture.bridge.shutdown().await.unwrap();
}
#[tokio::test]
async fn matrix_followup_permission_denial_keeps_original_collector_and_rejects_late_grant() {
    followup_permission_decision(false).await;
}
#[tokio::test]
async fn matrix_followup_permission_cancel_keeps_original_collector_and_rejects_late_grant() {
    followup_permission_decision(true).await;
}

fn request_id(body: &str) -> &str {
    body.split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
}

#[tokio::test]
async fn permission_scope_rejects_wrong_authorized_sender_room_thread_missing_id_and_replay() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let task = fixture.start(event("work"));
    let notice = fixture.notice().await;
    let id = request_id(&notice);
    assert!(uuid::Uuid::parse_str(id).is_ok());
    let mut wrong_sender = event(&format!("/approve {id}"));
    wrong_sender.user = "@other:local".into();
    let mut wrong_room = event(&format!("/approve {id}"));
    wrong_room.room = "!other:local".into();
    let mut wrong_thread = event(&format!("/approve {id}"));
    wrong_thread.thread = "$wrong-thread".into();
    let mut missing_id = event(&format!("/approve {id}"));
    missing_id.id.clear();
    for reply in [
        wrong_sender,
        wrong_room,
        wrong_thread,
        missing_id,
        event("/approve fabricated"),
        event(&format!("/approve {id} extra")),
    ] {
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.bridge.handle(reply, &fixture.sink),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(fixture.replies().is_empty());
        assert!(!task.is_finished());
    }
    let reply = event(&format!("/approve {id}"));
    fixture
        .bridge
        .handle(reply.clone(), &fixture.sink)
        .await
        .unwrap();
    fixture.bridge.handle(reply, &fixture.sink).await.unwrap();
    fixture.finish(task).await;
    fixture
        .bridge
        .handle(event(&format!("/approve {id}")), &fixture.sink)
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected", "optionId":"native-once"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|value| value["method"] == "session/prompt")
            .count(),
        1
    );
    assert!(
        fixture
            .sink
            .0
            .lock()
            .await
            .iter()
            .any(|value| value["text"] == "reader-progress")
    );
}

#[tokio::test]
async fn permission_denial_only_selects_an_offered_reject_once() {
    for options in [
        json!([{"kind":"allow_once","optionId":"native-once"},{"kind":"reject_once","optionId":"native-deny"}]),
        json!([{"kind":"allow_once","optionId":"native-once"},{"kind":"reject_always","optionId":"never-select"}]),
    ] {
        let fixture = Fixture::new(json!({}), json!({"options":options}), "");
        let task = fixture.start(event("work"));
        let notice = fixture.notice().await;
        fixture
            .bridge
            .handle(
                event(&format!("/deny {}", request_id(&notice))),
                &fixture.sink,
            )
            .await
            .unwrap();
        fixture.finish(task).await;
        let expected = if options
            .as_array()
            .unwrap()
            .iter()
            .any(|option| option["kind"] == "reject_once")
        {
            json!({"outcome":"selected","optionId":"native-deny"})
        } else {
            json!({"outcome":"cancelled"})
        };
        assert_eq!(fixture.replies()[0]["result"]["outcome"], expected);
    }
}

#[tokio::test]
async fn permission_opt_in_is_exact_true_and_matrix_with_nonempty_origin_only() {
    for value in [json!(false), json!("true"), json!(1), Value::Null] {
        let fixture = Fixture::new(json!({"interactivePermissions":value}), json!({}), "");
        fixture.finish(fixture.start(event("work"))).await;
        assert_eq!(
            fixture.replies()[0]["result"]["outcome"],
            json!({"outcome":"cancelled"})
        );
        assert!(
            !fixture
                .sink
                .0
                .lock()
                .await
                .iter()
                .any(|value| value.to_string().contains("/approve "))
        );
    }
    for field in ["id", "user", "platform"] {
        let fixture = Fixture::new(json!({"allowedUsers":[]}), json!({}), "");
        let mut origin = event("work");
        match field {
            "id" => origin.id.clear(),
            "user" => origin.user.clear(),
            _ => {
                origin.platform = "web".into();
                origin.direct = true;
            }
        }
        fixture.finish(fixture.start(origin)).await;
        assert_eq!(
            fixture.replies()[0]["result"]["outcome"],
            json!({"outcome":"cancelled"})
        );
        assert!(
            !fixture
                .sink
                .0
                .lock()
                .await
                .iter()
                .any(|value| value.to_string().contains("/approve "))
        );
    }
}

#[tokio::test]
async fn permission_unsafe_or_ambiguous_operations_cancel_without_preview() {
    let tool = |input: Value| json!({"kind":"execute", "toolCallId":"operation", "rawInput":input});
    let mut patches = vec![
        json!({"toolCall":tool(json!({"command":"echo safe", "cmd":"echo hidden"}))}),
        json!({"toolCall":tool(json!({"command":"echo safe", "cwd":".", "workdir":"/hidden"}))}),
        json!({"toolCall":tool(json!({"command":"echo safe", "hidden":"echo hidden"}))}),
        json!({"toolCall":tool(json!({"command":""}))}),
        json!({"toolCall":tool(json!({"command":12}))}),
        json!({"toolCall":tool(json!({"command":"echo safe", "cwd":12}))}),
        json!({"toolCall":{"kind":"write", "toolCallId":"operation", "rawInput":{"command":"echo safe"}}}),
        json!({"toolCall":{"kind":"execute", "toolCallId":"", "rawInput":{"command":"echo safe"}}}),
        json!({"toolCall":tool(json!({"command":"echo safe", "timeout":"hidden"}))}),
        json!({"options":null}),
        json!({"options":[]}),
        json!({"options":[{"kind":"allow_always", "optionId":"never-select"}]}),
        json!({"options":[{"kind":"allow_once", "optionId":"one"},{"kind":"allow_once", "optionId":"two"}]}),
        json!({"options":[{"kind":"allow_once", "optionId":"one"},{"kind":"reject_once", "optionId":"one"}]}),
        json!({"options":[{"kind":"allow_once", "optionId":"one"},{"kind":"reject_once", "optionId":12}]}),
        json!({"sessionId":"foreign-secret-session"}),
    ];
    for command in [
        "token=fixture-secret".to_string(),
        "echo Bearer fixture-secret".into(),
        "echo ghp_fixture".into(),
        "echo https://user:pass@host".into(),
        "echo \u{001b}hidden".into(),
        "echo \u{202e}hidden".into(),
        "echo \u{200f}hidden".into(),
        "echo \u{0085}hidden".into(),
        "echo \u{feff}hidden".into(),
        "printf 'éghp_A'".into(),
        "x".repeat(8001),
    ] {
        patches.push(json!({"toolCall":tool(json!({"command":command}))}));
    }
    for patch in patches {
        let fixture = Fixture::new(json!({}), patch.clone(), "");
        fixture.finish(fixture.start(event("work"))).await;
        assert_eq!(
            fixture.replies()[0]["result"]["outcome"],
            json!({"outcome":"cancelled"}),
            "{patch}"
        );
        assert!(
            !fixture
                .sink
                .0
                .lock()
                .await
                .iter()
                .any(|value| value.to_string().contains("/approve ")),
            "unsafe preview for {patch}"
        );
        assert!(
            !fixture
                .sink
                .0
                .lock()
                .await
                .iter()
                .any(|value| value.to_string().contains("fixture-secret")
                    || value.to_string().contains("foreign-secret"))
        );
    }
}

#[tokio::test]
async fn permission_raw_thread_is_independent_of_synthesized_presentation_thread() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let sink = ScopedSink {
        recorder: fixture.sink.clone(),
        raw_thread: "".into(),
        delivery: None,
        fail: false,
    };
    let mut origin = event("work");
    origin.thread = "$synthesized-origin".into();
    let task = fixture.start_with(origin, sink.clone());
    let notice = fixture.notice().await;
    let mut reply = event(&format!("/approve {}", request_id(&notice)));
    reply.thread = "$synthesized-reply".into();
    let wrong = ScopedSink {
        raw_thread: "$foreign-raw-root".into(),
        ..sink.clone()
    };
    fixture.bridge.handle(reply.clone(), &wrong).await.unwrap();
    assert!(fixture.replies().is_empty());
    reply.id = uuid::Uuid::new_v4().to_string();
    fixture.bridge.handle(reply, &sink).await.unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected", "optionId":"native-once"})
    );
}

#[tokio::test]
async fn permission_replies_before_delivery_ack_never_approve_and_failed_delivery_cancels() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let delivery = Arc::new(tokio::sync::Semaphore::new(0));
    let sink = ScopedSink {
        recorder: fixture.sink.clone(),
        raw_thread: "$origin-thread".into(),
        delivery: Some(delivery.clone()),
        fail: false,
    };
    let task = fixture.start_with(event("work"), sink);
    let notice = fixture.notice().await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert!(fixture.replies().is_empty());
    assert!(!task.is_finished());
    delivery.add_permits(1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected", "optionId":"native-once"})
    );

    let fixture = Fixture::new(json!({}), json!({}), "");
    let sink = ScopedSink {
        recorder: fixture.sink.clone(),
        raw_thread: "$origin-thread".into(),
        delivery: None,
        fail: true,
    };
    fixture
        .finish(fixture.start_with(event("work"), sink))
        .await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
}

#[tokio::test]
async fn permission_prompt_abort_while_notice_delivery_blocked_removes_pending_state() {
    let fixture = Fixture::new(json!({}), json!({}), "");
    let sink = ScopedSink {
        recorder: fixture.sink.clone(),
        raw_thread: "$origin-thread".into(),
        delivery: Some(Arc::new(tokio::sync::Semaphore::new(0))),
        fail: false,
    };
    let task = fixture.start_with(event("work"), sink);
    let notice = fixture.notice().await;
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.replies().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
    fixture.bridge.shutdown().await.unwrap();
}
fn event(text: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!approval:local".into(),
        thread: "$origin-thread".into(),
        id: uuid::Uuid::new_v4().to_string(),
        user: "@requester:local".into(),
        text: text.into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    }
}
#[tokio::test]
async fn opted_in_matrix_permission_waits_for_scoped_approve_once() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let cfg = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],
            "env":{"APPROVAL_AUDIT":audit}},
        "matrix":{"interactivePermissions":true,"respondToAllMessages":true,"threadIsolation":false,
            "allowedUsers":["@requester:local"],"allowedRooms":["!approval:local"],"strictAllowedRooms":true}
    }).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Arc::new(Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let task = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("work"), &sink).await })
    };
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = sink.0.lock().await.clone();
            if let Some(body) = events
                .iter()
                .filter_map(|v| v["text"].as_str())
                .find(|body| body.contains("/approve "))
            {
                break Some(body.to_owned());
            }
            if task.is_finished() {
                break None;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or(None);
    if observed.is_none() {
        let events = sink.0.lock().await.clone();
        let requests = std::fs::read_to_string(&audit).unwrap_or_default();
        task.abort();
        let _ = task.await;
        bridge.shutdown().await.unwrap();
        panic!(
            "opted-in permission was not published and held for an originating Matrix decision; events={events:?}; actual ACP audit={requests}"
        );
    }
    let body = observed.unwrap();
    assert!(body.contains("printf approval-fixture"));
    assert!(
        !task.is_finished(),
        "prompt must wait for explicit approval"
    );
    let requests = std::fs::read_to_string(&audit).unwrap();
    assert!(
        !requests.lines().any(|line| {
            let v: Value = serde_json::from_str(line).unwrap();
            v["id"] == 900 && v.get("method").is_none()
        }),
        "ACP must not receive a permission decision before approval"
    );
    let id = body
        .split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    bridge
        .handle(event(&format!("/approve {id}")), &sink)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let replies: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|v| v["id"] == 900 && v.get("method").is_none())
        .collect();
    assert_eq!(
        replies,
        vec![
            json!({"jsonrpc":"2.0","id":900,"result":{"outcome":{"outcome":"selected","optionId":"native-once"}}})
        ]
    );
    bridge.shutdown().await.unwrap();
}
