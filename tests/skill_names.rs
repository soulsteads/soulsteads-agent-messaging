use soulsteads_messaging::tools::{Presenter, hermes, matrix_notice};
use serde_json::{Value, json};

#[test]
fn skill_names_support_opencode_id_and_legacy_name() {
    for (tool, input) in [
        ("skill", json!({"id":"forge"})),
        ("skill", json!({"name":"forge"})),
        ("skill_view", json!({"name":"forge"})),
    ] {
        let entry = json!({"tool":tool,"details":input,"status":"running"});
        assert_eq!(hermes(&entry, true), "📚 Loading skill forge");
        assert_eq!(hermes(&entry, false), "📚 Loading skill");
    }
}

#[test]
fn skill_name_arriving_after_pending_updates_existing_matrix_notice() {
    for input in [
        json!({"id":"forge"}),
        Value::String(r#"{"id":"forge"}"#.into()),
    ] {
        let mut presenter = Presenter::new(json!({"mode":"hermes","showArguments":true}));
        let initial = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"s1","title":"skill","status":"pending"}));
        assert_eq!(initial[0]["message"], "📚 Loading skill");
        let updates = presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"s1","status":"in_progress","rawInput":input}));
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0]["message"], "📚 Loading skill forge");
        assert_eq!(updates[0]["activityId"], initial[0]["activityId"]);
        assert_eq!(
            matrix_notice(updates[0]["message"].as_str().unwrap(), true)["formatted_body"],
            "📚 Loading skill forge"
        );
        assert!(presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"s1","status":"completed"})).is_empty());
    }
}

#[test]
fn skill_names_remain_bounded_escaped_and_ignore_non_string_ids() {
    let entry = json!({"tool":"skill","details":{"id":"<skill>&name"},"status":"running"});
    assert_eq!(
        matrix_notice(&hermes(&entry, true), true)["formatted_body"],
        "📚 Loading skill &lt;skill&gt;&amp;name"
    );
    for id in [
        json!(null),
        json!(42),
        json!({"name":"do-not-render"}),
        json!(" "),
    ] {
        assert_eq!(
            hermes(&json!({"tool":"skill","details":{"id":id}}), true),
            "📚 Loading skill"
        );
    }
    let message = hermes(
        &json!({"tool":"skill","details":{"id":"x".repeat(400)}}),
        true,
    );
    assert_eq!(
        message.encode_utf16().count(),
        "📚 Loading skill ".encode_utf16().count() + 180
    );
    assert_eq!(
        hermes(
            &json!({"tool":"skill","details":{"name":"legacy","id":"new"}}),
            true
        ),
        "📚 Loading skill legacy"
    );
}
