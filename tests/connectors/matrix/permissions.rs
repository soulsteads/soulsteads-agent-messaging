use super::*;
fn scope() -> Scope {
    Scope {
        sender: "@requester:local".into(),
        room: "!room:local".into(),
        thread: "$root".into(),
        context: "conversation".into(),
        cwd: "/tmp/session".into(),
    }
}
fn request() -> Value {
    json!({"toolCall":{"kind":"execute", "toolCallId":"operation", "rawInput":{"command":"printf safe"}}, "options":[{"kind":"allow_once", "optionId":"native-once"}]})
}
#[test]
fn permission_preview_rejects_blank_operation_identity() {
    let mut params = request();
    params["toolCall"]["toolCallId"] = " \t".into();
    assert!(
        preview(&scope(), &params).is_none(),
        "blank toolCallId is not a valid operation identity"
    );
}
#[test]
fn permission_preview_preserves_source_ascii_secret_boundary() {
    let mut params = request();
    params["toolCall"]["rawInput"]["command"] = "printf 'éghp_A'".into();
    assert!(
        preview(&scope(), &params).is_none(),
        "Source screens ASCII token boundaries after non-ASCII text"
    );
}
#[test]
fn permission_literal_preview_resolves_cwd_without_normalizing_command() {
    let mut params = request();
    params["toolCall"]["rawInput"] = json!({"cmd":"printf 'hello\nworld'\t", "workdir":"../chosen", "description":"fixture", "timeout":42});
    let (display, once, deny) = preview(&scope(), &params).unwrap();
    assert_eq!(
        display,
        "Working directory: /tmp/chosen\nOperation (literal shell command):\nprintf 'hello\nworld'\t"
    );
    assert_eq!(once, "native-once");
    assert_eq!(deny, None);
    params["toolCall"]["rawInput"]["workdir"] = "/absolute/./chosen/../target".into();
    assert!(
        preview(&scope(), &params)
            .unwrap()
            .0
            .starts_with("Working directory: /absolute/target\n")
    );
}
struct Notice(tokio::sync::Mutex<Option<String>>);
#[async_trait::async_trait]
impl Sink for Notice {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        *self.0.lock().await = value["text"].as_str().map(str::to_owned);
        Ok(())
    }
}
#[tokio::test]
async fn permission_expiry_and_drop_remove_broker_state_without_grant() {
    for abort in [false, true] {
        let broker = Broker::default();
        let sink = Notice(tokio::sync::Mutex::new(None));
        let (reply, outcome) = oneshot::channel();
        let invocation = PermissionRequest {
            params: request(),
            current: Arc::new(AtomicBool::new(true)),
            reply,
        };
        let expiry = tokio::time::Instant::now() + Duration::from_millis(30);
        let pending = broker.request_until(scope(), invocation, &sink, expiry);
        tokio::pin!(pending);
        tokio::select! { _ = &mut pending => panic!("request finished early"), _ = tokio::time::sleep(Duration::from_millis(5)) => {} }
        let id = sink
            .0
            .lock()
            .await
            .clone()
            .unwrap()
            .split("/approve ")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .to_owned();
        assert_eq!(broker.0.lock().unwrap().len(), 1);
        if abort {
            broker
                .0
                .lock()
                .unwrap()
                .get(&id)
                .unwrap()
                .current
                .store(false, Ordering::SeqCst);
            assert!(!broker.decide(&scope(), &id, true, "$reply"));
        }
        pending.await;
        assert_eq!(outcome.await.unwrap(), cancelled());
        assert!(broker.0.lock().unwrap().is_empty());
        assert!(!broker.decide(&scope(), &id, true, "$replay"));
    }
    let broker = Broker::default();
    let sink = Notice(tokio::sync::Mutex::new(None));
    let (reply, mut outcome) = oneshot::channel();
    let invocation = PermissionRequest {
        params: request(),
        current: Arc::new(AtomicBool::new(true)),
        reply,
    };
    let mut pending = Box::pin(broker.request(scope(), invocation, &sink));
    tokio::select! { _ = &mut pending => panic!("request finished early"), _ = tokio::time::sleep(Duration::from_millis(5)) => {} }
    drop(pending);
    assert!(outcome.try_recv().is_err());
    assert!(broker.0.lock().unwrap().is_empty());
}
