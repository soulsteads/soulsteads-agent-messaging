use super::matrix_permissions::Scope;
use crate::runtime::Sink;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const TTL: Duration = Duration::from_secs(45 * 60);
const MAX_TEXT: usize = 4096;
struct Pending {
    scope: Scope,
    form: Value,
    current: Arc<AtomicBool>,
    expires: Instant,
    published: Arc<AtomicBool>,
    reply: oneshot::Sender<Option<Value>>,
}
#[derive(Clone, Default)]
pub(crate) struct Broker(Arc<Mutex<HashMap<String, Pending>>>);
struct Guard(Broker, String);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.1);
    }
}
fn selection_limits(field: &Value) -> Result<(usize, usize)> {
    let bound = |name: &str, default: usize| -> Result<usize> {
        match field.get(name) {
            None => Ok(default),
            Some(value) => {
                let n = usize::try_from(
                    value
                        .as_u64()
                        .context("Selection limit must be an unsigned integer")?,
                )?;
                if n > 64 {
                    bail!("Selection limit exceeds supported bounds");
                }
                Ok(n)
            }
        }
    };
    let minimum = bound("min", 0)?;
    let maximum = bound("max", 64)?;
    if minimum > maximum
        || (field["custom"] != true
            && minimum
                > field["options"]
                    .as_array()
                    .context("Missing multiselect options")?
                    .len())
    {
        bail!("Impossible multiselect constraints");
    }
    Ok((minimum, maximum))
}
fn fields(form: &Value) -> Result<&Vec<Value>> {
    if form["metadata"]["kind"] != "question" {
        bail!("Only OpenCode question forms are supported");
    }
    let fields = form["fields"]
        .as_array()
        .context("Missing question fields")?;
    if fields.is_empty() || fields.len() > 16 || form.to_string().len() > 32768 {
        bail!("Question exceeds display limits");
    }
    let mut keys = HashSet::new();
    for field in fields {
        let key = field["key"].as_str().context("Missing field key")?;
        if key.is_empty()
            || key.len() > 64
            || !key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            || !keys.insert(key)
        {
            bail!("Invalid or duplicate field key");
        }
        if !matches!(field["type"].as_str(), Some("string" | "multiselect")) {
            bail!("Unsupported question field type");
        }
        for property in ["title", "description"] {
            if field[property].as_str().is_some_and(|s| s.len() > MAX_TEXT) {
                bail!("Question text exceeds limits");
            }
        }
        if let Some(options) = field.get("options") {
            let options = options.as_array().context("Invalid question options")?;
            if options.len() > 64 {
                bail!("Too many question options");
            }
            let mut values = HashSet::new();
            for option in options {
                let value = option["value"].as_str().context("Invalid option value")?;
                if value.len() > MAX_TEXT
                    || !values.insert(value)
                    || option["label"].as_str().is_some_and(|s| s.len() > MAX_TEXT)
                    || option["description"]
                        .as_str()
                        .is_some_and(|s| s.len() > MAX_TEXT)
                {
                    bail!("Invalid or oversized question option");
                }
            }
        } else if field["type"] == "multiselect" {
            bail!("Missing multiselect options");
        }
        if field["type"] == "multiselect" {
            selection_limits(field)?;
        }
    }
    Ok(fields)
}
fn render(form: &Value, token: &str) -> Result<String> {
    let fields = fields(form)?;
    let mut lines = vec![format!("Question · {token}")];
    for field in fields {
        lines.push(format!(
            "{}: {}",
            field["key"].as_str().unwrap(),
            field["title"].as_str().unwrap_or("Question")
        ));
        if let Some(description) = field["description"].as_str() {
            lines.push(description.to_owned());
        }
        if let Some(options) = field["options"].as_array() {
            for option in options {
                lines.push(format!(
                    "- {}{}",
                    serde_json::to_string(&option["value"])?,
                    option["description"]
                        .as_str()
                        .map(|s| format!(": {s}"))
                        .unwrap_or_default()
                ));
            }
        }
        lines.push(
            if field["type"] == "multiselect" {
                "Choose multiple values using a JSON array."
            } else {
                "Choose one value using a JSON string."
            }
            .to_owned(),
        );
        if field["custom"] == true {
            lines.push("Custom text is allowed.".to_owned());
        }
    }
    let example: serde_json::Map<String, Value> = fields
        .iter()
        .map(|f| {
            (
                f["key"].as_str().unwrap().to_owned(),
                if f["type"] == "multiselect" {
                    json!(["choice"])
                } else {
                    json!("choice")
                },
            )
        })
        .collect();
    lines.push(format!(
        "Answer: /answer {token} {}",
        Value::Object(example)
    ));
    lines.push(format!(
        "Dismiss: /dismiss {token} · expires in 45 minutes."
    ));
    let text = lines.join("\n");
    if text.len() > 32768 {
        bail!("Question exceeds display limits");
    }
    Ok(text)
}
fn validate(form: &Value, answer: &Value) -> Result<()> {
    let fields = fields(form)?;
    let map = answer
        .as_object()
        .context("Answers must be a JSON object")?;
    if map
        .keys()
        .any(|k| !fields.iter().any(|f| f["key"].as_str() == Some(k)))
    {
        bail!("Unknown answer field");
    }
    for f in fields {
        let key = f["key"].as_str().unwrap();
        let Some(v) = map.get(key) else {
            if f["required"] == true {
                bail!("Missing required answer: {key}");
            } else {
                continue;
            }
        };
        let values: Vec<&str> = if f["type"] == "multiselect" {
            let a = v
                .as_array()
                .context("Multiple-choice answers must be arrays")?;
            let (minimum, maximum) = selection_limits(f)?;
            if a.len() < minimum || a.len() > maximum {
                bail!("Invalid selection count");
            }
            a.iter()
                .map(|v| v.as_str().context("Selections must be strings"))
                .collect::<Result<_>>()?
        } else {
            vec![v.as_str().context("Single answers must be strings")?]
        };
        let mut unique = HashSet::new();
        for value in values {
            if value.len() > MAX_TEXT || !unique.insert(value) {
                bail!("Oversized or duplicate selection");
            }
            if let Some(options) = f["options"].as_array()
                && f["custom"] != true
                && !options.iter().any(|o| o["value"].as_str() == Some(value))
            {
                bail!("Unknown option for {key}");
            }
        }
    }
    Ok(())
}
impl Broker {
    pub(crate) async fn request(
        &self,
        scope: Scope,
        form: Value,
        current: Arc<AtomicBool>,
        reply: oneshot::Sender<Option<Value>>,
        sink: &dyn Sink,
    ) {
        if !current.load(Ordering::Acquire) {
            let _ = reply.send(None);
            return;
        }
        let token = uuid::Uuid::new_v4().simple().to_string();
        let text = match render(&form, &token) {
            Ok(text) => text,
            Err(_) => {
                let _ = reply.send(None);
                let _ = sink.emit(json!({"type":"notice", "text":"Question dismissed: unsupported or oversized form."})).await;
                return;
            }
        };
        let (sender, receiver) = oneshot::channel();
        let published = Arc::new(AtomicBool::new(false));
        let expires = Instant::now() + TTL;
        self.0.lock().unwrap_or_else(|e| e.into_inner()).insert(
            token.clone(),
            Pending {
                scope,
                form,
                current: current.clone(),
                published: published.clone(),
                expires,
                reply: sender,
            },
        );
        let guard = Guard(self.clone(), token);
        // A visible pre-ACK answer may enter the LOCAL oneshot, but cannot reach
        // OpenCode until publication succeeds. Failure/drop discards that value.
        if sink
            .emit(json!({"type":"notice", "text":text}))
            .await
            .is_err()
            || !current.load(Ordering::Acquire)
        {
            let _ = reply.send(None);
            return;
        }
        published.store(true, Ordering::Release);
        let cancelled = async {
            loop {
                if !current.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        let answer = tokio::select! {
            result = receiver => result.ok().flatten(),
            _ = cancelled => None,
            _ = tokio::time::sleep(expires.saturating_duration_since(Instant::now())) => None,
        };
        drop(guard);
        let answer = if current.load(Ordering::Acquire) && Instant::now() < expires {
            answer
        } else {
            None
        };
        let _ = reply.send(answer);
    }
    pub(crate) async fn decide(&self, scope: &Scope, text: &str, sink: &dyn Sink) -> Result<bool> {
        let mut words = text.trim().splitn(3, char::is_whitespace);
        let command = words.next().unwrap_or("").to_ascii_lowercase();
        if !matches!(command.as_str(), "/answer" | "/dismiss") {
            return Ok(false);
        }
        let token = words.next().unwrap_or("");
        let input = words.next().unwrap_or("").trim();
        let message = {
            let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(item) = pending.get(token)
                && item.scope.sender == scope.sender
                && item.scope.room == scope.room
                && item.scope.thread == scope.thread
                && item.scope.context == scope.context
                && item.current.load(Ordering::Acquire)
                && item.expires > Instant::now()
            {
                let answer = if command == "/dismiss" && input.is_empty() {
                    Ok(None)
                } else if command == "/answer" && input.len() <= 16384 {
                    serde_json::from_str::<Value>(input)
                        .map_err(anyhow::Error::from)
                        .and_then(|answer| {
                            validate(&item.form, &answer)?;
                            Ok(Some(answer))
                        })
                } else {
                    Err(anyhow::anyhow!("Invalid answer command"))
                };
                match answer {
                    Ok(answer) => {
                        let item = pending.remove(token).unwrap();
                        if item.current.load(Ordering::Acquire) && item.reply.send(answer).is_ok() {
                            if item.published.load(Ordering::Acquire) {
                                "Answer or dismissal submitted; awaiting OpenCode confirmation."
                            } else {
                                "Answer or dismissal queued until question delivery is confirmed."
                            }
                        } else {
                            "Question is no longer active."
                        }
                    }
                    Err(_) => {
                        "Invalid answer. Use the displayed JSON field keys and choice values; the question remains open."
                    }
                }
            } else {
                "Question is not active for this user, room, or thread."
            }
        };
        sink.emit(json!({"type":"notice", "text":message})).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::Ordering;
    use tokio::sync::Mutex;
    #[derive(Default)]
    struct Recorder(Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for Recorder {
        async fn emit(&self, event: Value) -> anyhow::Result<()> {
            self.0.lock().await.push(event);
            Ok(())
        }
    }
    fn scope() -> Scope {
        Scope {
            sender: "@human:local".into(),
            room: "!room:local".into(),
            thread: "$root".into(),
            context: "context".into(),
            cwd: "/fixture".into(),
        }
    }
    fn form() -> Value {
        json!({"id":"frm_fixture", "sessionID":"session", "title":"Questions", "metadata":{"kind":"question"}, "fields":[{"key":"q0","type":"string","title":"Direction","description":"Which direction?","options":[{"value":"A","label":"A","description":"First"},{"value":"B","label":"B"}],"custom":true},{"key":"q1","type":"multiselect","title":"Features","description":"Choose features","options":[{"value":"X","label":"X"},{"value":"Y","label":"Y"}],"custom":true}]})
    }
    #[test]
    fn typed_question_answers_are_checked_against_exact_fields() {
        let mut question = form();
        for field in question["fields"].as_array_mut().unwrap() {
            field["required"] = true.into();
        }
        assert!(validate(&question, &json!({"q0":"B","q1":["X","Y"]})).is_ok());
        assert!(
            validate(
                &question,
                &json!({"q0":"custom text","q1":["custom feature"]})
            )
            .is_ok()
        );
        for answer in [
            json!({"q0":"B"}),
            json!({"q0":"B","q1":["X"],"unknown":"no"}),
            json!({"q0":["B"],"q1":["X"]}),
            json!({"q0":"B","q1":"X"}),
            json!({"q0":"B","q1":["X","X"]}),
            json!({"q0":"B","q1":[1]}),
        ] {
            assert!(validate(&question, &answer).is_err());
        }
        question["fields"][0]["custom"] = false.into();
        assert!(validate(&question, &json!({"q0":"not an option","q1":["X"]})).is_err());
        question["fields"][1]["custom"] = false.into();
        assert!(validate(&question, &json!({"q0":"A","q1":["not an option"]})).is_err());
    }

    #[test]
    fn impossible_multiselect_constraints_are_rejected_before_publication() {
        for (minimum, maximum) in [
            (json!(65), json!(65)),
            (json!(3), json!(2)),
            (json!(-1), json!(2)),
            (json!(1), json!("2")),
            (json!(u64::MAX), json!(u64::MAX)),
        ] {
            let mut question = form();
            question["fields"][1]["min"] = minimum;
            question["fields"][1]["max"] = maximum;
            assert!(
                fields(&question).is_err(),
                "impossible selection limits must be refused: {}",
                question["fields"][1]
            );
        }
        let mut question = form();
        question["fields"][1]["custom"] = false.into();
        question["fields"][1]["min"] = 3.into();
        assert!(fields(&question).is_err());
    }

    #[test]
    fn unsupported_question_schemas_are_not_armed() {
        let base = form();
        assert!(fields(&base).is_ok());
        for kind in ["boolean", "number", "object", "password"] {
            let mut question = base.clone();
            question["fields"][0]["type"] = kind.into();
            assert!(fields(&question).is_err());
        }
        let mut question = base.clone();
        question["fields"][1]["key"] = "q0".into();
        assert!(fields(&question).is_err());
        question = base;
        question["metadata"]["kind"] = "other".into();
        assert!(fields(&question).is_err());
    }

    #[tokio::test]
    async fn question_is_presented_instead_of_silently_dismissed() {
        let broker = Arc::new(Broker::default());
        let sink = Arc::new(Recorder::default());
        let current = Arc::new(AtomicBool::new(true));
        let (reply, mut answer) = oneshot::channel();
        let task = {
            let broker = broker.clone();
            let sink = sink.clone();
            let current = current.clone();
            tokio::spawn(async move {
                broker
                    .request(scope(), form(), current, reply, sink.as_ref())
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while sink.0.lock().await.is_empty() && !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let notices = sink.0.lock().await.clone();
        assert!(
            !notices.is_empty(),
            "question must be published to Matrix, not silently cancelled"
        );
        let body = notices[0]["text"].as_str().unwrap();
        assert!(
            body.contains("Which direction?")
                && body.contains("Choose features")
                && body.contains("/answer ")
                && body.contains("/dismiss ")
        );
        assert!(
            answer.try_recv().is_err(),
            "publishing question cannot invent an answer"
        );
        current.store(false, Ordering::SeqCst);
        task.abort();
    }
}
