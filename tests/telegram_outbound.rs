use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::any,
};
use serde_json::{Value, json};
use soulsteads_messaging::{
    connectors::http::{Platform, PlatformSink},
    runtime::{Event, Sink},
    tools::{OutputPresenter, TurnPresenter},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::IntoFuture,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
use tokio::sync::Mutex;

type Requests = Arc<Mutex<Vec<(String, Value)>>>;

#[derive(Clone)]
struct TelegramFixture {
    requests: Requests,
    messages: Arc<Mutex<BTreeMap<i64, Value>>>,
    next_id: Arc<AtomicI64>,
    failures: Arc<Mutex<BTreeSet<usize>>>,
}

impl Deref for TelegramFixture {
    type Target = Mutex<Vec<(String, Value)>>;

    fn deref(&self) -> &Self::Target {
        &self.requests
    }
}

async fn endpoint(State(fixture): State<TelegramFixture>, request: Request) -> impl IntoResponse {
    let path = request.uri().path().to_owned();
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let ordinal = {
        let mut requests = fixture.requests.lock().await;
        requests.push((path.clone(), body.clone()));
        requests.len()
    };
    if fixture.failures.lock().await.remove(&ordinal) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"ok":false})),
        )
            .into_response();
    }
    let result = if path.ends_with("/sendMessage") {
        let id = fixture.next_id.fetch_add(1, Ordering::SeqCst);
        fixture.messages.lock().await.insert(id, body);
        json!({"message_id":id})
    } else if path.ends_with("/editMessageText") {
        let id = body["message_id"].as_i64().unwrap();
        let mut messages = fixture.messages.lock().await;
        if !messages.contains_key(&id) {
            return (StatusCode::BAD_REQUEST, axum::Json(json!({"ok":false}))).into_response();
        }
        messages.insert(id, body);
        json!({"message_id":id})
    } else if path.ends_with("/deleteMessage") {
        let id = body["message_id"].as_i64().unwrap();
        fixture.messages.lock().await.remove(&id);
        json!(true)
    } else {
        json!(true)
    };
    (
        StatusCode::OK,
        axum::Json(json!({"ok":true,"result":result})),
    )
        .into_response()
}

async fn fixture() -> (
    Platform,
    Event,
    TelegramFixture,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    fixture_with_failures(&[]).await
}

async fn fixture_with_failures(
    failures: &[usize],
) -> (
    Platform,
    Event,
    TelegramFixture,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let fixture = TelegramFixture {
        requests: Requests::default(),
        messages: Arc::default(),
        next_id: Arc::new(AtomicI64::new(100)),
        failures: Arc::new(Mutex::new(failures.iter().copied().collect())),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    let platform = Platform::new("telegram", &base, "fixture-token", "42", "fixture_bot");
    let event = platform
        .map(&json!({"message":{
            "message_id":1,"chat":{"id":-123,"type":"supergroup"},
            "from":{"id":7},"is_topic_message":true,"message_thread_id":9,"text":"!oc hello"
        }}))
        .unwrap();
    (platform, event, fixture, server)
}

fn options(mode: &str, arguments: bool) -> Value {
    json!({"mode":mode,"showCalls":true,"showArguments":arguments,"showOutputFor":["*"],"maxTraceEntries":1})
}

#[tokio::test]
async fn telegram_tool_metadata_uses_native_entities_and_completion_edit() {
    let (platform, event, requests, server) = fixture().await;
    let sink = PlatformSink::new(platform, event);
    let mut calls = TurnPresenter::new(options("hermes", true), "telegram", json!({"mode":"off"}));
    for update in [
        json!({"sessionUpdate":"tool_call","toolCallId":"one","title":"bash","status":"in_progress","rawInput":{"command":"printf '😀 ``` <b>literal</b>'"}}),
        json!({"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed"}),
    ] {
        for output in calls.update(&update) {
            sink.emit(output).await.unwrap();
        }
    }
    let bodies = requests.lock().await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0].0, "/botfixture-token/sendMessage");
    assert!(
        bodies[0].1["text"]
            .as_str()
            .unwrap()
            .contains("💻 terminal — running")
    );
    assert_eq!(bodies[0].1["entities"][0]["type"], "bold");
    assert!(
        bodies[0].1["entities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entity| entity["type"] == "pre")
    );
    assert_eq!(bodies[1].0, "/botfixture-token/editMessageText");
    assert!(bodies[1].1["text"].as_str().unwrap().contains("completed"));
    assert!(
        bodies
            .iter()
            .all(|(_, body)| body.get("parse_mode").is_none())
    );
    drop(bodies);
    server.abort();
}

#[tokio::test]
async fn telegram_events_completion_edits_the_started_activity() {
    let (platform, event, requests, server) = fixture().await;
    let sink = PlatformSink::new(platform, event);
    let mut calls = TurnPresenter::new(options("events", false), "telegram", json!({"mode":"off"}));
    for update in [
        json!({"sessionUpdate":"tool_call","toolCallId":"one","title":"bash","status":"in_progress","rawInput":{"command":"private marker"}}),
        json!({"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed"}),
    ] {
        for output in calls.update(&update) {
            sink.emit(output).await.unwrap();
        }
    }
    let bodies = requests.lock().await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0].0, "/botfixture-token/sendMessage");
    assert_eq!(bodies[1].0, "/botfixture-token/editMessageText");
    assert!(bodies[1].1["text"].as_str().unwrap().contains("completed"));
    assert!(
        bodies
            .iter()
            .all(|(_, body)| !body["text"].as_str().unwrap().contains("private marker"))
    );
    drop(bodies);
    server.abort();
}

#[tokio::test]
async fn telegram_http_activity_matrix_edits_each_mode_with_both_argument_policies() {
    for mode in ["events", "trace", "status", "hermes"] {
        for arguments in [false, true] {
            let (platform, event, requests, server) = fixture().await;
            let sink = PlatformSink::new(platform, event);
            let mut calls =
                TurnPresenter::new(options(mode, arguments), "telegram", json!({"mode":"off"}));
            for update in [
                json!({"sessionUpdate":"tool_call","toolCallId":"one","title":"odd label","status":"in_progress","rawInput":{"command":"hidden marker 😀"}}),
                json!({"sessionUpdate":"tool_call_update","toolCallId":"one","status":"failed"}),
            ] {
                for output in calls.update(&update) {
                    sink.emit(output).await.unwrap();
                }
            }
            let bodies = requests.lock().await;
            assert!(bodies.len() >= 2, "{mode}/{arguments}");
            assert!(
                bodies
                    .iter()
                    .any(|(path, _)| path.ends_with("/sendMessage")),
                "{mode}/{arguments}"
            );
            assert!(
                bodies
                    .iter()
                    .any(|(path, _)| path.ends_with("/editMessageText")),
                "{mode}/{arguments}"
            );
            assert!(bodies.iter().any(|(_, body)| {
                body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("failed"))
            }));
            assert_eq!(
                bodies
                    .iter()
                    .any(|(_, body)| body["text"].as_str().unwrap().contains("hidden marker")),
                arguments,
                "{mode}/{arguments}"
            );
            drop(bodies);
            server.abort();
        }
    }
}

#[tokio::test]
async fn telegram_multichunk_activity_edits_chunks_without_resending_them() {
    let (platform, event, requests, server) = fixture().await;
    let sink = PlatformSink::new(platform, event);
    let entries = (0..40)
        .map(|index| {
            json!({
                "tool":"bash", "status":"running",
                "arguments": format!("{{\"command\":\"{index}-{}\"}}", "😀<b>```".repeat(90)),
            })
        })
        .collect::<Vec<_>>();
    let started = json!({"type":"activity","activityId":"trace:0","message":"ignored","toolPresentation":{
        "kind":"tool_page","mode":"trace","entries":entries,"completed":0,"active":null,"page":1,"pages":1
    }});
    sink.emit(started).await.unwrap();
    let sent = requests.lock().await.len();
    assert!(sent > 1, "expected a multi-message activity");
    drop(requests.lock().await);
    let completed_entries = (0..40)
        .map(|index| {
            json!({
                "tool":"bash", "status":"completed",
                "arguments": format!("{{\"command\":\"{index}-{}\"}}", "😀<b>```".repeat(90)),
            })
        })
        .collect::<Vec<_>>();
    sink.emit(json!({"type":"activity","activityId":"trace:0","message":"ignored","toolPresentation":{
        "kind":"tool_page","mode":"trace","entries":completed_entries,"completed":40,"active":null,"page":1,"pages":1
    }})).await.unwrap();
    let bodies = requests.lock().await;
    assert_eq!(
        bodies.len(),
        sent * 2,
        "each existing chunk is updated in place"
    );
    assert!(
        bodies[sent..]
            .iter()
            .all(|(path, _)| path.ends_with("/editMessageText"))
    );
    assert!(
        bodies
            .iter()
            .all(|(_, body)| body["text"].as_str().unwrap().encode_utf16().count() <= 4096)
    );
    drop(bodies);
    server.abort();
}

#[tokio::test]
async fn telegram_initial_partial_send_retry_preserves_first_chunk_id() {
    let (platform, event, fixture, server) = fixture_with_failures(&[2]).await;
    let sink = PlatformSink::new(platform, event);
    let entries = (0..40)
        .map(|index| {
            json!({
                "tool":"bash", "status":"running",
                "arguments": format!("{index}-{}", "😀<b>```".repeat(90)),
            })
        })
        .collect::<Vec<_>>();
    let activity = json!({"type":"activity","activityId":"partial","message":"ignored","toolPresentation":{
        "kind":"tool_page","mode":"trace","entries":entries,"completed":0,"active":null,"page":1,"pages":1
    }});

    assert!(sink.emit(activity.clone()).await.is_err());
    assert_eq!(
        fixture.messages.lock().await.len(),
        1,
        "first chunk was accepted"
    );
    sink.emit(activity).await.unwrap();

    let requests = fixture.requests.lock().await;
    let sends = requests
        .iter()
        .filter(|(path, _)| path.ends_with("/sendMessage"))
        .collect::<Vec<_>>();
    let ids = fixture
        .messages
        .lock()
        .await
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let first_text = fixture.messages.lock().await[&100]["text"].clone();
    assert_eq!(
        sends.len(),
        ids.len() + 1,
        "only the injected failure may lack an ID"
    );
    assert_eq!(
        ids.first(),
        Some(&100),
        "the first accepted chunk was not resent"
    );
    assert_eq!(
        sends
            .iter()
            .filter(|(_, body)| body["text"] == first_text)
            .count(),
        1,
        "the first accepted chunk was resent after the partial failure",
    );
    drop(requests);
    server.abort();
}

#[tokio::test]
async fn telegram_partial_edit_retry_keeps_successful_chunks_and_converges_store() {
    let (platform, event, fixture, server) = fixture().await;
    let sink = PlatformSink::new(platform, event);
    let entries = |status| {
        (0..40)
            .map(|index| json!({"tool":"odd label", "status":status, "arguments":format!("{index}-{}", "😀`<b>".repeat(90))}))
            .collect::<Vec<_>>()
    };
    let page = |entries: Vec<Value>| {
        json!({"type":"activity","activityId":"edits","message":"ignored","toolPresentation":{
            "kind":"tool_page","mode":"trace","entries":entries,"completed":0,"active":null,"page":1,"pages":1
        }})
    };
    sink.emit(page(entries("running"))).await.unwrap();
    let sent = fixture.messages.lock().await.len();
    *fixture.failures.lock().await = [sent + 2].into_iter().collect();
    assert!(sink.emit(page(entries("completed"))).await.is_err());
    let after_failure = fixture.requests.lock().await.len();
    sink.emit(page(entries("completed"))).await.unwrap();
    let after_retry = fixture.requests.lock().await.len();
    assert!(after_retry > after_failure);
    let requests = fixture.requests.lock().await;
    let successful_first_edit = requests
        .iter()
        .filter(|(path, body)| {
            path.ends_with("/editMessageText")
                && body["message_id"] == 100
                && body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("completed"))
        })
        .count();
    assert_eq!(
        successful_first_edit, 1,
        "successful edit must not be retried"
    );
    assert!(fixture.messages.lock().await.values().all(|body| {
        body["text"]
            .as_str()
            .is_some_and(|text| !text.contains("— running"))
    }));
    drop(requests);
    server.abort();
}

#[tokio::test]
async fn telegram_tool_output_and_ordinary_response_remain_distinct() {
    let (platform, event, requests, server) = fixture().await;
    let sink = PlatformSink::new(platform, event);
    let mut output = OutputPresenter::new(options("events", false), "telegram");
    sink.emit(output.update(&json!({
        "sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"completed",
        "rawOutput":{"output":"`@here` <b>literal</b> 😀"}
    })).unwrap()).await.unwrap();
    sink.emit(json!({"type":"response","text":"**ordinary** `literal` <b>text</b>"}))
        .await
        .unwrap();
    let bodies = requests.lock().await;
    let header = "💻 terminal — output";
    assert_eq!(
        bodies[0].1["text"],
        format!("{header}\n`@here` <b>literal</b> 😀")
    );
    assert_eq!(
        bodies[0].1["entities"][0],
        json!({"type":"bold","offset":0,"length":header.encode_utf16().count()})
    );
    assert_eq!(bodies[0].1["entities"][1]["type"], "pre");
    assert_eq!(bodies[1].1["text"], "**ordinary** `literal` <b>text</b>");
    assert!(bodies[1].1.get("entities").is_none());
    drop(bodies);
    server.abort();
}
