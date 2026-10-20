//! Real Matrix SDK sync ingress with audited HTTP media and ACP seams.
use super::*;
use axum::{Router, body::Body, extract::Request, response::IntoResponse, routing::any};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    base: String,
    late: bool,
    syncs: Arc<AtomicUsize>,
    downloads: Arc<AtomicUsize>,
    redirects: Arc<AtomicUsize>,
    replies: Arc<tokio::sync::Mutex<Vec<Value>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start(events: Vec<Value>, mode: &'static str, room_id: &'static str) -> Self {
        let media_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let syncs = Arc::new(AtomicUsize::new(0));
        let downloads = Arc::new(AtomicUsize::new(0));
        let redirects = Arc::new(AtomicUsize::new(0));
        let replies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router=Router::new().fallback(any({
            let media_ready=media_ready.clone(); let cancel_sent=cancel_sent.clone();
            let syncs=syncs.clone(); let downloads=downloads.clone(); let redirects=redirects.clone(); let replies=replies.clone(); let base=base.clone();
            move |request: Request| {
                let media_ready=media_ready.clone();let cancel_sent=cancel_sent.clone();
                let syncs=syncs.clone();let downloads=downloads.clone();let redirects=redirects.clone();let replies=replies.clone();let events=events.clone();let base=base.clone();
                async move {
                    let path=request.uri().path().to_owned();
                    if path.contains("/media/download/") {
                        downloads.fetch_add(1,Ordering::SeqCst);
                        assert_eq!(request.headers().get("authorization").unwrap(),"Bearer fixture");
                        if mode=="redirect" { return (axum::http::StatusCode::TEMPORARY_REDIRECT,[("location",format!("{base}/redirect-target"))]).into_response(); }
                        if mode=="timeout" { std::future::pending::<()>().await; }
                        if mode=="late-followup" { tokio::time::sleep(Duration::from_secs(2)).await; media_ready.store(true,Ordering::SeqCst); }
                        let chunks:Vec<Result<Vec<u8>,std::io::Error>>=match mode {
                            "oversize" => (0..65).map(|_|Ok(vec![b'a';4096])).collect(),
                            "binary" => vec![Ok(vec![0xff,0,1])],
                            _ => vec![Ok(b"# Exact fixture\n\nReference bytes\n".to_vec())],
                        };
                        return axum::http::Response::builder().header("content-type","text/markdown")
                            .body(Body::from_stream(futures_util::stream::iter(chunks))).unwrap().into_response();
                    }
                    if path=="/redirect-target" { redirects.fetch_add(1,Ordering::SeqCst); return "redirect leaked".into_response(); }
                    if path.contains("/state/m.room.encryption") {
                        return (axum::http::StatusCode::NOT_FOUND,axum::Json(json!({"errcode":"M_NOT_FOUND","error":"plaintext"}))).into_response();
                    }
                    let value=if path.ends_with("versions") {json!({"versions":["v1.12"],"unstable_features":{}})}
                    else if path.ends_with("whoami") {json!({"user_id":"@bot:local","device_id":"ATTACH"})}
                    else if path.ends_with("joined_members") {json!({"joined":{"@bot:local":{},"@human:local":{},"@third:local":{}}})}
                    else if path.ends_with("sync") {
                        let n=syncs.fetch_add(1,Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(25)).await;
                        let late_cancel=mode=="late-followup" && media_ready.load(Ordering::SeqCst) && !cancel_sent.swap(true,Ordering::SeqCst);
                        if late_cancel { tokio::time::sleep(Duration::from_millis(300)).await; }
                        let mut state=vec![json!({"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}})];
                        for (i,user) in ["@bot:local","@human:local","@third:local"].iter().enumerate() {state.push(json!({"type":"m.room.member","event_id":format!("$member{i}"),"origin_server_ts":1,"state_key":user,"sender":user,"content":{"membership":"join"}}));}
                        json!({"next_batch":format!("batch-{n}"),"rooms":{"join":{room_id:{"state":{"events":state},"timeline":{"events":if mode=="late-followup" { if late_cancel {vec![json!({"type":"m.room.message","event_id":"$cancel","origin_server_ts":10,"sender":"@human:local","content":{"msgtype":"m.text","body":"/cancel"}})]} else {match n {1=>vec![events[0].clone()],8|9=>vec![events[1].clone()],_=>vec![]}} }else if n==1||n==2{events}else{vec![]},"limited":false},"ephemeral":{"events":[]},"account_data":{"events":[]}}}}})
                    } else if path.contains("/send/") {
                        let bytes=axum::body::to_bytes(request.into_body(),1024*1024).await.unwrap();
                        replies.lock().await.push(serde_json::from_slice(&bytes).unwrap());json!({"event_id":"$sent"})
                    } else {json!({})};
                    axum::Json(value).into_response()
                }
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            base,
            late: mode == "late-followup",
            syncs,
            downloads,
            redirects,
            replies,
            server,
        }
    }
    fn config(&self, root: &Path, all: bool, rate: u64) -> Value {
        crate::config::load(&json!({"sessionStorePath":root.join("sessions.json"),"rateLimitSeconds":rate,"toolMessages":{"mode":"off"},"acp":{"command":"/usr/bin/python3","args":[if self.late {concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-attachments-peer.py")} else {concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-mvp-peer.py")},root.join("audit.jsonl")],"backendId":"attachment-fixture"},"matrix":{"homeserver":self.base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false},"respondToAllMessages":all,"allowedRooms":["!room:local"],"allowedUsers":["@human:local"],"strictAllowedRooms":true,"threadIsolation":false}}).to_string(),&Default::default()).unwrap()
    }
}
fn file(sender: &str, filename: &str, caption: &str) -> Value {
    json!({"type":"m.room.message","event_id":"$file","origin_server_ts":3,"sender":sender,"content":{"msgtype":"m.file","filename":filename,"body":caption,"url":"mxc://local/testfile","info":{"mimetype":"text/markdown","size":1}}})
}
async fn exercise(
    events: Vec<Value>,
    mode: &'static str,
    room: &'static str,
    all: bool,
    rate: u64,
    expected_reply: bool,
) -> (usize, usize, Vec<Value>, Vec<Value>) {
    let fixture = Fixture::start(events, mode, room).await;
    let root = tempfile::tempdir().unwrap();
    let bridge = Arc::new(
        Bridge::new(
            fixture.config(root.path(), all, rate),
            Default::default(),
            root.path().join("cwd"),
        )
        .unwrap(),
    );
    let client = connect(&bridge.config["matrix"], &root.path().join("platform"))
        .await
        .unwrap();
    let (shutdown, receive) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(super::run(client, bridge.clone(), receive));
    tokio::time::timeout(Duration::from_secs(27), async {
        loop {
            if (mode == "late-followup" && fixture.syncs.load(Ordering::SeqCst) > 150)
                || (mode != "late-followup"
                    && ((expected_reply && !fixture.replies.lock().await.is_empty())
                        || (!expected_reply && fixture.syncs.load(Ordering::SeqCst) > 15)))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge.shutdown().await.unwrap();
    let audit = std::fs::read_to_string(root.path().join("audit.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let replies = fixture.replies.lock().await.clone();
    (
        fixture.downloads.load(Ordering::SeqCst),
        fixture.redirects.load(Ordering::SeqCst),
        replies,
        audit,
    )
}
#[tokio::test]
async fn sdk_file_caption_and_bytes_reach_acp_once() {
    let (downloads, _, replies, audit) = exercise(
        vec![file("@human:local", "plan.md", "/reset")],
        "text",
        "!room:local",
        true,
        0,
        true,
    )
    .await;
    assert_eq!(downloads, 1, "duplicate sync downloaded twice");
    let prompts: Vec<_> = audit
        .iter()
        .filter(|v| v["method"] == "session/prompt")
        .collect();
    assert_eq!(prompts.len(), 1);
    let text = prompts[0]["text"].as_str().unwrap();
    assert!(text.starts_with("Attached document"));
    assert!(text.contains("User caption:\n/reset"));
    assert!(text.ends_with("# Exact fixture\n\nReference bytes\n"));
    assert!(replies.iter().any(|r| {
        r["body"]
            .as_str()
            .is_some_and(|s| s.contains("Reference bytes"))
    }));
}
#[tokio::test]
async fn sdk_denied_inputs_never_download_or_open_acp() {
    for (sender, room, all) in [
        ("@denied:local", "!room:local", true),
        ("@bot:local", "!room:local", true),
        ("@human:local", "!other:local", true),
        ("@human:local", "!room:local", false),
    ] {
        let (downloads, _, replies, audit) = exercise(
            vec![file(sender, "plan.md", "Review this file")],
            "text",
            room,
            all,
            0,
            false,
        )
        .await;
        assert_eq!(
            downloads, 0,
            "denied file downloaded: {sender} {room} {all}"
        );
        assert!(replies.is_empty());
        assert!(audit.is_empty());
    }
}
#[tokio::test]
async fn sdk_rejections_are_explicit_and_never_prompt_acp() {
    for (filename, mode) in [
        ("document.pdf", "text"),
        ("binary.txt", "binary"),
        ("oversize.txt", "oversize"),
        ("redirect.md", "redirect"),
        ("timeout.md", "timeout"),
    ] {
        let (downloads, redirects, replies, audit) = exercise(
            vec![file("@human:local", filename, "Review this file")],
            mode,
            "!room:local",
            true,
            0,
            true,
        )
        .await;
        assert_eq!(downloads, usize::from(filename != "document.pdf"));
        assert_eq!(redirects, 0, "credentials sent after redirect");
        assert!(audit.is_empty());
        assert!(replies.iter().any(|r| {
            r["body"]
                .as_str()
                .is_some_and(|s| s.contains("Attachment rejected:"))
        }));
        assert_eq!(replies.len(), 1, "duplicate rejection reply");
    }
}
#[tokio::test]
async fn sdk_delayed_file_after_original_completion_remains_cancellable() {
    let original = json!({"type":"m.room.message","event_id":"$original","origin_server_ts":3,"sender":"@human:local","content":{"msgtype":"m.text","body":"ATTACHMENT-ORIGINAL"}});
    let (downloads, _, replies, audit) = exercise(
        vec![
            original,
            file("@human:local", "plan.md", "slow file followup"),
        ],
        "late-followup",
        "!room:local",
        true,
        0,
        true,
    )
    .await;
    assert_eq!(downloads, 1);
    assert_eq!(
        audit
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        2
    );
    assert!(
        audit.iter().any(|v| v["method"] == "session/cancel"),
        "loaded file lost its cancellation owner after the original prompt completed: {replies:?}"
    );
    assert!(replies.iter().any(|r| {
        r["body"]
            .as_str()
            .is_some_and(|s| s.contains("Cancellation requested."))
    }));
}

#[tokio::test]
async fn sdk_rate_limit_reserves_before_second_file_download() {
    let first = file("@human:local", "plan.md", "Review this file");
    let mut second = first.clone();
    second["event_id"] = "$second".into();
    let (downloads, _, _, audit) =
        exercise(vec![first, second], "text", "!room:local", true, 60, true).await;
    assert_eq!(downloads, 1);
    assert_eq!(
        audit
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}
