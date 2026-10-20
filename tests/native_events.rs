#[path = "../src/adapters/opencode/native_events.rs"]
mod native_events;

use native_events::{EventProjector, SseDecoder};
use serde_json::json;

#[test]
fn native_event_updates_only_matching_session_and_reports_authoritative_terminal() {
    let mut p = EventProjector::new("ses_fixture");
    assert!(p.push(json!({"id":"a","type":"session.text.delta","data":{"sessionID":"other","delta":"ignore"}})).is_empty());
    let out = p.push(json!({"id":"b","type":"session.text.delta","data":{"sessionID":"ses_fixture","assistantMessageID":"m1","delta":"hello"}}));
    assert_eq!(
        out,
        vec![native_events::Update::TextDelta {
            message_id: "m1".into(),
            text: "hello".into()
        }]
    );
    assert!(p.push(json!({"id":"b","type":"session.text.delta","data":{"sessionID":"ses_fixture","assistantMessageID":"m1","delta":"hello"}})).is_empty());
    let terminal = p.push(
        json!({"id":"c","type":"session.execution.succeeded","data":{"sessionID":"ses_fixture"}}),
    );
    assert_eq!(terminal, vec![native_events::Update::Finished]);
}

#[test]
fn sse_decoder_handles_chunking_multiple_frames_crlf_and_utf8() {
    let mut d = SseDecoder::new(1024);
    assert!(
        d.feed(b"data: {\"id\":\"1\",\"type\":\"x\",\"data\":{\"s\":\"")
            .unwrap()
            .is_empty()
    );
    let got = d
        .feed("é\"}}\r\n\r\ndata: {\"id\":\"2\",\"type\":\"y\",\"data\":{}}\n\n".as_bytes())
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["data"]["s"], "é");
    assert_eq!(got[1]["type"], "y");
}

#[test]
fn durable_messages_recover_reply_in_oldest_first_order() {
    let msgs = json!({"data":[
      {"id":"m2","type":"assistant","time":{"created":20},"content":[{"type":"text","text":"second"}]},
      {"id":"m1","type":"assistant","time":{"created":10},"content":[{"type":"text","text":"first"}]},
      {"id":"u","type":"user","time":{"created":5},"text":"prompt"}
    ]});
    assert_eq!(
        native_events::durable_messages(&msgs, "ses_fixture").unwrap(),
        vec![
            native_events::DurableMessage {
                id: "u".into(),
                role: "user".into(),
                text: "prompt".into()
            },
            native_events::DurableMessage {
                id: "m1".into(),
                role: "assistant".into(),
                text: "first".into()
            },
            native_events::DurableMessage {
                id: "m2".into(),
                role: "assistant".into(),
                text: "second".into()
            },
        ]
    );
}

#[test]
fn malformed_and_oversized_sse_are_errors_not_silent_empty_success() {
    let mut decoder = SseDecoder::new(32);
    assert!(decoder.feed_checked(b"data: not-json\n\n").is_err());
    assert!(decoder.feed_checked(&[b'x'; 64]).is_err());
    let mut decoder = SseDecoder::new(64);
    assert!(
        decoder
            .feed_checked(b"data: {\"value\":\"\xff\"}\n\n")
            .is_err()
    );
}

#[test]
fn sse_requires_native_envelope_not_just_valid_json() {
    for payload in [
        json!(true),
        json!({"id":"evt_bad","type":"x"}),
        json!({"id":"evt_bad","type":"x","data":null}),
        json!({"id":"","type":"x","data":{}}),
    ] {
        let mut decoder = SseDecoder::new(1024);
        assert!(
            decoder
                .feed(format!("data: {payload}\n\n").as_bytes())
                .is_err()
        );
    }
}

#[test]
fn non_durable_replay_beyond_dedup_window_never_reapplies_old_text_or_tools() {
    let mut projector = EventProjector::new("ses_fixture");
    let original = json!({"id":"evt_original","type":"session.text.delta","data":{"sessionID":"ses_fixture","assistantMessageID":"msg_original","delta":"original"}});
    assert!(!projector.push(original.clone()).is_empty());
    let mut signalled = false;
    for index in 1..=4097 {
        let updates = projector.push(json!({"id":format!("evt_{index}"),"type":"ignored","data":{"sessionID":"ses_fixture"}}));
        signalled |= !updates.is_empty();
    }
    assert!(
        projector.push(original).is_empty(),
        "an evicted non-durable event must not be applied again"
    );
    assert!(projector.push(json!({"id":"evt_tool_after_overflow","type":"session.tool.success","data":{"sessionID":"ses_fixture","assistantMessageID":"msg_original","id":"call_original","content":[]}})).is_empty(), "overflow must latch projection closed until durable recovery");
    assert!(
        signalled,
        "bounded window exhaustion must request reconciliation"
    );
    assert!(projector.dedup_len() <= 4096);
}

#[test]
fn native_projector_bounds_dedup_and_uses_schema_verified_tools() {
    let mut projector = EventProjector::new("ses_fixture");
    for index in 0..10000 {
        projector.push(
            json!({"id":format!("evt_{index}"),"type":"ignored","data":{"sessionID":"foreign"}}),
        );
    }
    assert_eq!(projector.dedup_len(), 0);
    for index in 0..10000 {
        projector.push(json!({"id":format!("evt_{index}"),"type":"ignored","data":{"sessionID":"ses_fixture"}}));
    }
    assert!(projector.dedup_len() <= 4096);
    assert!(projector.push(json!({"id":"evt_after_overflow","type":"session.execution.succeeded","data":{"sessionID":"ses_fixture"}})).is_empty());
    let mut projector = EventProjector::new("ses_fixture");
    let updates = projector.push(json!({"id":"evt_tool","type":"session.tool.success","data":{"sessionID":"ses_fixture","assistantMessageID":"msg_assistant","id":"call_1","content":[{"type":"text","text":"ok"}],"executed":true}}));
    assert!(
        matches!(updates.as_slice(), [native_events::Update::Tool { kind, .. }] if kind == "session.tool.success"),
        "actual native tool success must not disappear"
    );
    assert!(!projector.push(json!({"id":"evt_failed","type":"session.execution.failed","data":{"sessionID":"ses_fixture","error":{"type":"provider"}}})).is_empty());
}
