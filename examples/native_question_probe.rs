use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
    fn matrix_user_id(&self) -> Option<String> {
        Some("@bot:localhost".into())
    }
    fn matrix_raw_thread(&self) -> Option<String> {
        Some("$probe-thread".into())
    }
}
fn event(text: &str, user: &str, thread: &str, id: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!probe:localhost".into(),
        thread: thread.into(),
        id: id.into(),
        reply_to: None,
        user: user.into(),
        text: text.into(),
        direct: false,
        mentioned: true,
        bot: false,
        images: vec![],
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let mut a = std::env::args().skip(1);
    let command = a.next().context("OpenCode executable required")?;
    let root = PathBuf::from(a.next().context("isolated root required")?);
    ensure!(
        root.is_absolute() && a.next().is_none(),
        "absolute root and no extra arguments required"
    );
    let mut env: BTreeMap<String, String> = BTreeMap::new();
    for k in [
        "PATH",
        "LANG",
        "TMPDIR",
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "OPENCODE_CONFIG_CONTENT",
    ] {
        if let Some(v) = std::env::var_os(k) {
            env.insert(k.into(), v.to_string_lossy().into_owned());
        }
    }
    let cfg=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.join("bridge-store.json"),"acp":{"command":command,"args":[],"transport":"native","env":env.iter().filter(|(k,_)|k.starts_with("OPENCODE_")).map(|(k,v)|(k.clone(),v.clone())).collect::<BTreeMap<_,_>>()},"matrix":{"interactiveQuestions":true,"respondToAllMessages":true,"allowedUsers":["@probe:localhost"],"allowedRooms":["!probe:localhost"],"strictAllowedRooms":true}}).to_string(),&BTreeMap::new())?;
    let bridge = Arc::new(Bridge::new(
        cfg,
        BTreeMap::new(),
        root.join("bridge-state"),
    )?);
    let sink = Recorder::default();
    let mut first = event(
        "Please ask the three questions",
        "@probe:localhost",
        "$probe-thread",
        "$probe-start",
    );
    first.mentioned = true;
    let task = {
        let b = bridge.clone();
        let s = sink.clone();
        tokio::spawn(async move { b.handle(first, &s).await })
    };
    let notice = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            if let Some(t) = sink
                .0
                .lock()
                .await
                .iter()
                .filter_map(|v| v["text"].as_str())
                .find(|t| t.contains("/answer "))
                .map(str::to_owned)
            {
                return Ok::<String, anyhow::Error>(t);
            }
            if task.is_finished() {
                return Err(anyhow::anyhow!(
                    "Bridge completed before question notice: {}",
                    serde_json::to_string(&*sink.0.lock().await)?
                ));
            }
            tokio::time::sleep(Duration::from_millis(40)).await
        }
    })
    .await
    .context("no question notice from native Bridge")??;
    let token = notice
        .split("/answer ")
        .nth(1)
        .context("missing answer token")?
        .split_whitespace()
        .next()
        .context("empty token")?
        .to_owned();
    let answer = format!(
        "/answer {token} {{\"q0\":\"alpha\",\"q1\":[\"red\",\"green\"],\"q2\":\"custom-text\"}}"
    );
    bridge
        .handle(
            event(&answer, "@intruder:localhost", "$probe-thread", "$foreign"),
            &sink,
        )
        .await?;
    bridge
        .handle(
            event(
                &answer,
                "@probe:localhost",
                "$wrong-thread",
                "$wrong-thread",
            ),
            &sink,
        )
        .await?;
    ensure!(
        !task.is_finished(),
        "foreign or wrong-thread answer incorrectly settled question"
    );
    let notices = sink.0.lock().await.clone();
    ensure!(
        notices.iter().any(|v| v["text"]
            .as_str()
            .is_some_and(|s| s.contains("not active for this user, room, or thread"))),
        "wrong-scope rejection notice missing"
    );
    bridge
        .handle(
            event(&answer, "@probe:localhost", "$probe-thread", "$valid"),
            &sink,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(50), task)
        .await
        .context("bridge request did not finish")???;
    ensure!(
        sink.0.lock().await.iter().any(|v| v["text"]
            .as_str()
            .is_some_and(|s| s.contains("OpenCode confirmed the question answer"))),
        "backend answer readback acknowledgement missing"
    );
    ensure!(
        sink.0.lock().await.iter().any(|v| v["text"]
            .as_str()
            .is_some_and(|s| s.contains("QUESTION_E2E_VERIFIED"))),
        "real provider did not resume with the typed question answers"
    );
    bridge
        .handle(
            event(&answer, "@probe:localhost", "$probe-thread", "$duplicate"),
            &sink,
        )
        .await?;
    bridge.shutdown().await?;
    println!(
        "{}",
        json!({"native_bridge_question":true,"three_questions":true,"wrong_scope_rejected":true,"backend_answer_readback":true,"provider_resumed":true,"clean_bridge_shutdown":true})
    );
    Ok(())
}
