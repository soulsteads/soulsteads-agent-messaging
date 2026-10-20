use super::*;
use crate::config;

#[test]
fn retention_policy_parser_matches_executed_pinned_source() {
    let oracle: Value =
        serde_json::from_str(include_str!("../tests/oracles/retention-policy.json")).unwrap();
    for case in oracle["cases"].as_array().unwrap() {
        let environment = case["raw"]
            .as_str()
            .map(|raw| BTreeMap::from([("SESSION_RETENTION_MINS".into(), raw.into())]))
            .unwrap_or_default();
        let policy = SessionRetentionPolicy::from_environment(&environment);
        for platform in [
            "matrix",
            "discord",
            "mattermost",
            "telegram",
            "slack",
            "whatsapp",
            "web",
        ] {
            assert_eq!(
                policy
                    .minutes(platform)
                    .map(|minutes| format!("{:016x}", minutes.to_bits())),
                case["bits"][platform].as_str().map(str::to_owned),
                "{platform}: {:?}",
                case["raw"]
            );
        }
    }
}

#[tokio::test]
async fn retention_policy_producer_consumer_table_all_seven() {
    let (_directory, bridge) = fixture();
    let oracle: Value =
        serde_json::from_str(include_str!("../tests/oracles/retention-policy.json")).unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let raw = case["raw"].as_str();
        let environment = raw
            .map(|raw| BTreeMap::from([("SESSION_RETENTION_MINS".into(), raw.into())]))
            .unwrap_or_default();
        let policy = SessionRetentionPolicy::from_environment(&environment);
        for platform in [
            "matrix",
            "discord",
            "mattermost",
            "telegram",
            "slack",
            "whatsapp",
            "web",
        ] {
            let context = format!("policy-{index}-{platform}");
            let mut event = Event::web(
                &context,
                &format!("{} hello", string(&bridge.config, "trigger")),
            );
            event.platform = platform.into();
            bridge.handle(event, &Recorder::default()).await.unwrap();
            let slot = bridge.conversation(platform, &context).await.unwrap();
            slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
            let mapping = bridge.store.get(platform, &context, "opencode").unwrap();
            assert!(
                mapping.is_some(),
                "live fixture mapping required for {platform} case {index}"
            );
            assert!(
                slot.lock().await.client.is_some(),
                "live fixture client required"
            );
            let should_expire = case["minutes"][platform]
                .as_f64()
                .is_some_and(|minutes| minutes <= 33.0);
            let result = bridge
                .expire_with_policy(&policy, Instant::now() + Duration::from_secs(31 * 60))
                .await
                .unwrap();
            assert_eq!(result, usize::from(should_expire), "{platform} raw={raw:?}");
            assert_eq!(
                slot.lock().await.client.is_none(),
                should_expire,
                "{platform} case {index}"
            );
            if should_expire {
                assert!(
                    bridge
                        .store
                        .get(platform, &context, "opencode")
                        .unwrap()
                        .is_none()
                );
            } else {
                assert_eq!(
                    bridge.store.get(platform, &context, "opencode").unwrap(),
                    mapping
                );
            }
            bridge.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn retention_policy_routes_opposite_slack_matrix_policies() {
    for (base_minutes, slack_minutes, removed) in
        [(None, Some(1.0), "slack"), (Some(1.0), None, "matrix")]
    {
        let (_directory, bridge) = fixture();
        let matrix = open_stale(&bridge, "matrix", "opposite").await;
        let slack = open_stale(&bridge, "slack", "opposite").await;
        let policy = SessionRetentionPolicy {
            base_minutes,
            slack_minutes,
        };
        assert_eq!(
            bridge
                .expire_with_policy(&policy, Instant::now())
                .await
                .unwrap(),
            1
        );
        assert_eq!(matrix.lock().await.client.is_none(), removed == "matrix");
        assert_eq!(slack.lock().await.client.is_none(), removed == "slack");
        for platform in ["matrix", "slack"] {
            assert_eq!(
                bridge
                    .store
                    .get(platform, "opposite", "opencode")
                    .unwrap()
                    .is_none(),
                platform == removed
            );
        }
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn retention_policy_disabled_still_retries_owned_cleanup() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "disabled-cleanup").await;
    let client = slot.lock().await.client.clone().unwrap();
    client.fail_expiry_shutdown_once();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    assert!(slot.expiry_cleanup.lock().await.is_some());
    let policy = SessionRetentionPolicy::from_environment(&BTreeMap::new());
    assert_eq!(
        bridge
            .expire_with_policy(&policy, Instant::now())
            .await
            .unwrap(),
        0
    );
    assert!(slot.expiry_cleanup.lock().await.is_none());
    assert_eq!(client.session_id().await, None);
    assert!(
        bridge
            .store
            .get("web", "disabled-cleanup", "opencode")
            .unwrap()
            .is_none()
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn retention_policy_public_duration_remains_uniform_and_zero_noop() {
    let (_directory, bridge) = fixture();
    let slack = open_stale(&bridge, "slack", "uniform").await;
    let matrix = open_stale(&bridge, "matrix", "uniform").await;
    assert_eq!(bridge.expire(Duration::ZERO).await.unwrap(), 0);
    assert!(slack.lock().await.client.is_some());
    assert!(matrix.lock().await.client.is_some());
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 2);
    bridge.shutdown().await.unwrap();
}

#[derive(Default, Clone)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait::async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, event: Value) -> Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

fn fixture() -> (tempfile::TempDir, Bridge) {
    let directory = tempfile::Builder::new()
        .prefix("expiry-private-")
        .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join(".build-tmp"))
        .unwrap();
    let executable = std::env::current_exe().unwrap();
    let command = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("acp-fixture");
    assert!(
        command.is_file(),
        "build the offline acp-fixture binary before library tests"
    );
    let configuration = config::load(
        &json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"sessionPicker":{"enabled":true},"acp":{"command":command,"args":[],"env":{"ACP_FIXTURE_MATRIX_AUDIT":directory.path().join("audit.jsonl")}}}).to_string(),
        &BTreeMap::new(),
    ).unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    (directory, bridge)
}

async fn open_stale(bridge: &Bridge, platform: &str, context: &str) -> Arc<ConversationSlot> {
    let mut event = Event::web(context, "hello");
    event.platform = platform.into();
    bridge.handle(event, &Recorder::default()).await.unwrap();
    let slot = bridge.conversation(platform, context).await.unwrap();
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    slot
}

#[tokio::test]
async fn zero_expiry_is_disabled_at_runtime_seam() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "zero").await;
    let mapping = bridge.store.get("web", "zero", "opencode").unwrap();
    let expired = bridge.expire(Duration::ZERO).await.unwrap();
    let alive = slot.lock().await.client.is_some();
    let after = bridge.store.get("web", "zero", "opencode").unwrap();
    bridge.shutdown().await.unwrap();
    assert_eq!(expired, 0, "a zero runtime retention disables expiry");
    assert!(alive);
    assert_eq!(after, mapping);
}

#[tokio::test]
async fn pending_matrix_control_blocks_expiry() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "matrix", "control").await;
    slot.matrix_admission.lock().unwrap().control_pending = true;
    let expired = bridge.expire(Duration::from_secs(60)).await.unwrap();
    let alive = slot.lock().await.client.is_some();
    slot.matrix_admission.lock().unwrap().control_pending = false;
    bridge.shutdown().await.unwrap();
    assert_eq!(expired, 0, "admitted session controls must exclude expiry");
    assert!(alive);
}

async fn start_mirror(bridge: &Bridge, slot: &ConversationSlot, context: &str, sink: &Recorder) {
    let entry = bridge
        .store
        .get("web", context, "opencode")
        .unwrap()
        .unwrap();
    slot.lock().await.picker =
        vec![json!({"sessionId":entry.session_id,"cwd":entry.cwd,"title":"Fixture"})];
    bridge
        .handle(Event::web(context, "/m 1"), sink)
        .await
        .unwrap();
    let mut state = slot.lock().await;
    assert!(state.mirror.is_some());
    state.last_activity = Instant::now() - Duration::from_secs(120);
    state.mirror.as_mut().unwrap().next_poll = Instant::now();
}

#[tokio::test]
async fn logical_expiry_stops_mirror_output() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "mirror").await;
    let sink = Recorder::default();
    start_mirror(&bridge, &slot, "mirror", &sink).await;
    slot.lock().await.mirror.as_mut().unwrap().fingerprint = "changed".into();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    let cleared = slot.lock().await.mirror.is_none();
    let emitted = bridge.poll_mirrors().await.unwrap();
    bridge.shutdown().await.unwrap();
    assert!(cleared, "logical expiry must clear its mirror state");
    assert_eq!(emitted, 0, "an expired mirror must never publish output");
}

#[tokio::test]
async fn shutdown_error_is_contained_and_other_candidates_commit() {
    let (_directory, bridge) = fixture();
    let failed = open_stale(&bridge, "web", "failed").await;
    open_stale(&bridge, "web", "other").await;
    let client = failed.lock().await.client.clone().unwrap();
    client.fail_expiry_shutdown_once();
    let outcome = bridge.expire(Duration::from_secs(60)).await;
    let other_deleted = bridge
        .store
        .get("web", "other", "opencode")
        .unwrap()
        .is_none();
    assert!(failed.lock().await.client.is_none());
    assert!(
        failed
            .expiry_cleanup
            .lock()
            .await
            .as_ref()
            .unwrap()
            .same_expiry_client(&client)
    );
    assert_eq!(client.session_id().await.as_deref(), Some("fixture"));
    assert!(
        bridge
            .store
            .get("web", "failed", "opencode")
            .unwrap()
            .is_none()
    );
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert!(failed.expiry_cleanup.lock().await.is_none());
    assert_eq!(client.session_id().await, None);
    bridge.shutdown().await.unwrap();
    assert_eq!(
        outcome.unwrap(),
        2,
        "expired counts committed logical evictions even when shutdown fails"
    );
    assert!(
        other_deleted,
        "a candidate's cleanup failure cannot abort the sweep"
    );
}

#[tokio::test]
async fn shutdown_deadline_is_bounded_and_other_candidates_commit() {
    let (_directory, bridge) = fixture();
    let blocked = open_stale(&bridge, "web", "blocked").await;
    open_stale(&bridge, "web", "other").await;
    let client = blocked.lock().await.client.clone().unwrap();
    let child = client.hold_expiry_shutdown().await;
    let process = child.id().unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        bridge.expire(Duration::from_secs(60)),
    )
    .await;
    let committed = bridge
        .store
        .get("web", "blocked", "opencode")
        .unwrap()
        .is_none();
    assert!(
        bridge
            .store
            .get("web", "other", "opencode")
            .unwrap()
            .is_none()
    );
    assert!(
        blocked
            .expiry_cleanup
            .lock()
            .await
            .as_ref()
            .unwrap()
            .same_expiry_client(&client)
    );
    assert!(blocked.lock().await.client.is_none());
    assert_eq!(client.session_id().await.as_deref(), Some("fixture"));
    #[cfg(unix)]
    assert_eq!(
        unsafe { libc::kill(process as i32, 0) },
        0,
        "mapping removal commits before owned process shutdown can acquire the child"
    );
    drop(child);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert!(blocked.expiry_cleanup.lock().await.is_none());
    assert_eq!(client.session_id().await, None);
    #[cfg(unix)]
    assert_eq!(
        unsafe { libc::kill(process as i32, 0) },
        -1,
        "cleanup retry reaps the owned ACP process"
    );
    bridge.shutdown().await.unwrap();
    assert!(outcome.is_ok(), "expiry must bound owned ACP cleanup waits");
    assert_eq!(outcome.unwrap().unwrap(), 2);
    assert!(committed);
}

#[tokio::test]
async fn corrupt_store_keeps_identical_client_and_mirror_for_retry() {
    let (directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "retry").await;
    start_mirror(&bridge, &slot, "retry", &Recorder::default()).await;
    let original = slot.lock().await.client.clone().unwrap();
    let generation = slot.lock().await.mirror.as_ref().unwrap().generation;
    let saved = std::fs::read(&bridge.store.0).unwrap();
    let audit = std::fs::read(directory.path().join("audit.jsonl")).unwrap();
    std::fs::write(&bridge.store.0, b"{invalid").unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    {
        let state = slot.lock().await;
        assert!(state.client.as_ref().unwrap().same_expiry_client(&original));
        assert_eq!(state.mirror.as_ref().unwrap().generation, generation);
        assert_eq!(state.backend, "opencode");
    }
    assert_eq!(original.session_id().await.as_deref(), Some("fixture"));
    assert_eq!(std::fs::read(&bridge.store.0).unwrap(), b"{invalid");
    assert_eq!(
        std::fs::read(directory.path().join("audit.jsonl")).unwrap(),
        audit
    );
    std::fs::write(&bridge.store.0, saved).unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    assert!(slot.lock().await.client.is_none());
    assert!(slot.lock().await.mirror.is_none());
    assert_eq!(original.session_id().await, None);
    assert!(
        bridge
            .store
            .get("web", "retry", "opencode")
            .unwrap()
            .is_none()
    );
    bridge.shutdown().await.unwrap();
}

#[derive(Default)]
struct BlockedSink {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    only: Option<&'static str>,
}
#[async_trait::async_trait]
impl Sink for BlockedSink {
    async fn emit(&self, event: Value) -> Result<()> {
        if self.only.is_some_and(|kind| event["type"] != kind) {
            return Ok(());
        }
        self.started.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[tokio::test]
async fn mirror_output_admission_excludes_expiry_until_delivery_finishes() {
    let (_directory, bridge) = fixture();
    let bridge = Arc::new(bridge);
    let slot = open_stale(&bridge, "web", "mirror-race").await;
    start_mirror(&bridge, &slot, "mirror-race", &Recorder::default()).await;
    let sink = Arc::new(BlockedSink::default());
    {
        let mut state = slot.lock().await;
        let mirror = state.mirror.as_mut().unwrap();
        mirror.fingerprint = "changed".into();
        mirror.sink = sink.clone();
    }
    let polling_bridge = bridge.clone();
    let polling = tokio::spawn(async move { polling_bridge.poll_mirrors().await });
    tokio::time::timeout(Duration::from_secs(3), sink.started.notified())
        .await
        .unwrap();
    let expired_during_output = bridge.expire(Duration::from_secs(60)).await.unwrap();
    sink.release.notify_one();
    assert_eq!(polling.await.unwrap().unwrap(), 1);
    bridge.expire(Duration::from_secs(60)).await.unwrap();
    bridge.shutdown().await.unwrap();
    assert_eq!(
        expired_during_output, 0,
        "mirror output must be admitted under the same conversation lock as expiry"
    );
}

#[tokio::test]
async fn admitted_web_prompt_remains_busy_after_acp_collector_closes() {
    let (_directory, bridge) = fixture();
    let bridge = Arc::new(bridge);
    let slot = open_stale(&bridge, "web", "prompt-admission").await;
    let client = slot.lock().await.client.clone().unwrap();
    let sink = Arc::new(BlockedSink {
        only: Some("prompt_end"),
        ..Default::default()
    });
    let handling_bridge = bridge.clone();
    let handling_sink = sink.clone();
    let handling = tokio::spawn(async move {
        handling_bridge
            .handle(
                Event::web("prompt-admission", "error"),
                handling_sink.as_ref(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), sink.started.notified())
        .await
        .unwrap();
    assert!(
        !client.accepts_followups(),
        "the ACP collector must already be closed"
    );
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    let expired = bridge.expire(Duration::from_secs(60)).await.unwrap();
    sink.release.notify_one();
    assert!(handling.await.unwrap().is_err());
    bridge.shutdown().await.unwrap();
    assert_eq!(
        expired, 0,
        "an admitted prompt remains busy through final presentation"
    );
}

#[tokio::test]
async fn logical_expiry_resets_session_counters_without_resetting_user_choices() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "logical").await;
    {
        let mut state = slot.lock().await;
        assert!(state.message_count > 0 && state.input_chars > 0 && state.output_chars > 0);
        state.selected_cwd = Some("user-chosen-workspace".into());
        state.projects = vec!["user-project".into()];
        state.picker = vec![json!({"sessionId":"independent-history"})];
    }
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    let state = slot.lock().await;
    assert!(state.client.is_none());
    assert_eq!(state.backend, "opencode");
    assert_eq!(state.selected_cwd.as_deref(), Some("user-chosen-workspace"));
    assert_eq!(state.projects, ["user-project"]);
    assert_eq!(state.picker, [json!({"sessionId":"independent-history"})]);
    assert_eq!(
        (state.message_count, state.input_chars, state.output_chars),
        (0, 0, 0),
        "expired session statistics must not be reused by the next client"
    );
}

#[tokio::test]
async fn fresh_busy_and_active_turns_preserve_mapping_until_idle_stale() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "matrix", "busy").await;
    let mapping = bridge.store.get("matrix", "busy", "opencode").unwrap();
    {
        let _busy = slot.lock().await;
        assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    }
    slot.matrix_admission.lock().unwrap().turn = Some(Arc::new(MatrixTurn::default()));
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    slot.matrix_admission.lock().unwrap().turn = None;
    slot.lock().await.last_activity = Instant::now();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert_eq!(
        bridge.store.get("matrix", "busy", "opencode").unwrap(),
        mapping
    );
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn contended_admission_is_skipped_without_waiting() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "matrix", "contended").await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let locking_slot = slot.clone();
    let locking = std::thread::spawn(move || {
        let _guard = locking_slot.matrix_admission.lock().unwrap();
        started.send(()).unwrap();
        released.recv().unwrap();
    });
    ready.await.unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_millis(500),
        bridge.expire(Duration::from_secs(60)),
    )
    .await;
    release.send(()).unwrap();
    locking.join().unwrap();
    assert_eq!(outcome.unwrap().unwrap(), 0);
    assert!(slot.lock().await.client.is_some());
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_live_client_does_not_delete_persisted_or_mirror_only_history() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "no-client").await;
    start_mirror(&bridge, &slot, "no-client", &Recorder::default()).await;
    let mapping = bridge.store.get("web", "no-client", "opencode").unwrap();
    let client = slot.lock().await.client.take().unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert_eq!(
        bridge.store.get("web", "no-client", "opencode").unwrap(),
        mapping
    );
    assert!(slot.lock().await.mirror.is_some());
    bridge.shutdown().await.unwrap();
}

async fn wait_for_audit(directory: &Path, text: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let audit = std::fs::read_to_string(directory.join("audit.jsonl")).unwrap();
            if audit
                .lines()
                .any(|line| serde_json::from_str::<Value>(line).unwrap()["text"] == text)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_matrix_prompt_and_pending_followup_are_never_expired() {
    let (directory, bridge) = fixture();
    let bridge = Arc::new(bridge);
    let slot = open_stale(&bridge, "matrix", "followup").await;
    let client = slot.lock().await.client.clone().unwrap();
    let prompting_bridge = bridge.clone();
    let prompting = tokio::spawn(async move {
        let mut event = Event::web("followup", "wait");
        event.platform = "matrix".into();
        prompting_bridge.handle(event, &Recorder::default()).await
    });
    wait_for_audit(directory.path(), "wait").await;
    assert!(client.accepts_followups());
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    let forwarding_bridge = bridge.clone();
    let forwarding = tokio::spawn(async move {
        let mut event = Event::web("followup", "delayed-followup");
        event.platform = "matrix".into();
        forwarding_bridge.handle(event, &Recorder::default()).await
    });
    wait_for_audit(directory.path(), "delayed-followup").await;
    let mut event = Event::web("followup", "followup");
    event.platform = "matrix".into();
    bridge.handle(event, &Recorder::default()).await.unwrap();
    assert!(
        client.accepts_followups(),
        "pending followup keeps the actual ACP collector active"
    );
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    tokio::time::timeout(Duration::from_secs(3), forwarding)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), prompting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!client.accepts_followups());
    slot.lock().await.last_activity = Instant::now() - Duration::from_secs(120);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    bridge.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_store_delete_preserves_client_and_target_then_retries() {
    use std::os::unix::fs::symlink;
    let (directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "unsafe").await;
    let client = slot.lock().await.client.clone().unwrap();
    let safe = directory.path().join("safe-store.json");
    std::fs::rename(&bridge.store.0, &safe).unwrap();
    let original = std::fs::read(&safe).unwrap();
    symlink(&safe, &bridge.store.0).unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert!(
        slot.lock()
            .await
            .client
            .as_ref()
            .unwrap()
            .same_expiry_client(&client)
    );
    assert_eq!(client.session_id().await.as_deref(), Some("fixture"));
    assert_eq!(std::fs::read(&safe).unwrap(), original);
    assert!(
        std::fs::symlink_metadata(&bridge.store.0)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    std::fs::remove_file(&bridge.store.0).unwrap();
    std::fs::rename(safe, &bridge.store.0).unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    assert!(
        bridge
            .store
            .get("web", "unsafe", "opencode")
            .unwrap()
            .is_none()
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn inflight_mirror_snapshot_cannot_emit_after_logical_expiry() {
    let (directory, mut bridge) = fixture();
    let slot = open_stale(&bridge, "web", "snapshot").await;
    let sink = Recorder::default();
    start_mirror(&bridge, &slot, "snapshot", &sink).await;
    let before = sink.0.lock().await.len();
    let started = directory.path().join("snapshot-started");
    let release = directory.path().join("snapshot-release");
    bridge.config["acp"]["env"]["ACP_FIXTURE_INIT_STARTED_FILE"] = started.to_string_lossy().into();
    bridge.config["acp"]["env"]["ACP_FIXTURE_INIT_RELEASE_FILE"] = release.to_string_lossy().into();
    let bridge = Arc::new(bridge);
    let polling_bridge = bridge.clone();
    let polling = tokio::spawn(async move { polling_bridge.poll_mirrors().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !started.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    std::fs::write(release, "release").unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), polling)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(sink.0.lock().await.len(), before);
    assert!(slot.lock().await.mirror.is_none());
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn active_acp_collector_alone_blocks_expiry() {
    let (directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "collector").await;
    let client = slot.lock().await.client.clone().unwrap();
    let prompting_client = client.clone();
    let prompting = tokio::spawn(async move { prompting_client.prompt("wait", vec![]).await });
    wait_for_audit(directory.path(), "wait").await;
    assert_eq!(slot.matrix_admission.lock().unwrap().requests, 0);
    assert!(client.accepts_followups());
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert!(
        bridge
            .store
            .get("web", "collector", "opencode")
            .unwrap()
            .is_some()
    );
    client.forward("followup").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), prompting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_mirror_notice_is_admitted_before_expiry() {
    let (directory, bridge) = fixture();
    let bridge = Arc::new(bridge);
    let slot = open_stale(&bridge, "web", "mirror-error").await;
    start_mirror(&bridge, &slot, "mirror-error", &Recorder::default()).await;
    let sink = Arc::new(BlockedSink::default());
    {
        let mut state = slot.lock().await;
        let mirror = state.mirror.as_mut().unwrap();
        mirror.session["cwd"] = directory.path().join("missing").to_string_lossy().into();
        mirror.sink = sink.clone();
    }
    let polling_bridge = bridge.clone();
    let polling = tokio::spawn(async move { polling_bridge.poll_mirrors().await });
    tokio::time::timeout(Duration::from_secs(3), sink.started.notified())
        .await
        .unwrap();
    let expired = bridge.expire(Duration::from_secs(60)).await.unwrap();
    sink.release.notify_one();
    assert_eq!(polling.await.unwrap().unwrap(), 0);
    assert_eq!(expired, 0);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_cleanup_cannot_be_lost_or_restore_mapping_during_reopen() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "reopen-cleanup").await;
    let old = slot.lock().await.client.clone().unwrap();
    old.fail_expiry_shutdown_once();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    assert!(
        bridge
            .store
            .get("web", "reopen-cleanup", "opencode")
            .unwrap()
            .is_none()
    );
    bridge
        .handle(Event::web("reopen-cleanup", "hello"), &Recorder::default())
        .await
        .unwrap();
    let new = slot.lock().await.client.clone().unwrap();
    assert!(!new.same_expiry_client(&old));
    assert!(
        slot.expiry_cleanup
            .lock()
            .await
            .as_ref()
            .unwrap()
            .same_expiry_client(&old)
    );
    let mapping = bridge
        .store
        .get("web", "reopen-cleanup", "opencode")
        .unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(3600)).await.unwrap(), 0);
    assert!(slot.expiry_cleanup.lock().await.is_none());
    assert_eq!(old.session_id().await, None);
    assert!(
        slot.lock()
            .await
            .client
            .as_ref()
            .unwrap()
            .same_expiry_client(&new)
    );
    assert_eq!(
        bridge
            .store
            .get("web", "reopen-cleanup", "opencode")
            .unwrap(),
        mapping
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_mapping_still_counts_one_live_client_invalidation() {
    let (_directory, bridge) = fixture();
    let slot = open_stale(&bridge, "web", "absent").await;
    bridge
        .store
        .remove("web", "absent", Some("opencode"))
        .unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 1);
    assert!(slot.lock().await.client.is_none());
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_sweep_retains_owned_post_commit_cleanup() {
    let (_directory, bridge) = fixture();
    let bridge = Arc::new(bridge);
    let slot = open_stale(&bridge, "web", "cancelled").await;
    let client = slot.lock().await.client.clone().unwrap();
    let child = client.hold_expiry_shutdown().await;
    let expiring_bridge = bridge.clone();
    let expiring =
        tokio::spawn(async move { expiring_bridge.expire(Duration::from_secs(60)).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while bridge
            .store
            .get("web", "cancelled", "opencode")
            .unwrap()
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    expiring.abort();
    assert!(expiring.await.unwrap_err().is_cancelled());
    assert!(slot.lock().await.client.is_none());
    assert!(
        slot.expiry_cleanup
            .lock()
            .await
            .as_ref()
            .unwrap()
            .same_expiry_client(&client)
    );
    drop(child);
    assert_eq!(bridge.expire(Duration::from_secs(60)).await.unwrap(), 0);
    assert!(slot.expiry_cleanup.lock().await.is_none());
    assert_eq!(client.session_id().await, None);
    assert!(
        bridge
            .store
            .get("web", "cancelled", "opencode")
            .unwrap()
            .is_none()
    );
    bridge.shutdown().await.unwrap();
}
