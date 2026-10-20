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
