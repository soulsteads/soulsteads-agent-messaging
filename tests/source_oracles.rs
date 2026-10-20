use soulsteads_messaging::{connectors::http::retry_delay, core, runtime::session_dir, tools};
use serde_json::{Value, json};
use std::path::Path;
fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("")
}
#[test]
fn core_source_derived_corpus() {
    let oracle: Value = serde_json::from_str(include_str!("oracles/core.json")).unwrap();
    for case in oracle["sessionDirs"].as_array().unwrap() {
        assert_eq!(
            session_dir(Path::new("/test/sessions"), "matrix", text(case, "id")).to_string_lossy(),
            text(case, "expected")
        );
    }
    for case in oracle["media"].as_array().unwrap() {
        let input = text(case, "text");
        let (images, clean) = core::media(input, false);
        assert_eq!(json!(images), case["images"], "{input}");
        assert_eq!(clean, text(case, "imageClean"));
        let (docs, clean) = core::media(input, true);
        assert_eq!(json!(docs), case["docs"]);
        assert_eq!(clean, text(case, "docClean"));
        assert_eq!(core::sanitize(input), text(case, "sanitized"));
    }
    for case in oracle["empty"].as_array().unwrap() {
        assert_eq!(
            core::diagnostic(text(case, "acp"), text(case, "bridge"), text(case, "clean")),
            case["expected"]
        );
    }
    for case in oracle["botName"].as_array().unwrap() {
        assert_eq!(
            json!(core::name_query(text(case, "text"), text(case, "name"))),
            case["expected"]
        );
    }
    for case in oracle["matrixContexts"].as_array().unwrap() {
        assert_eq!(
            core::normalize("matrix", &case["input"], case["isolated"] == true),
            case["expected"]
        );
    }
    for case in oracle["matrixPolicy"].as_array().unwrap() {
        assert_eq!(
            core::room_allowed(case, "!room:server"),
            case["allowed"] == true
        );
        assert_eq!(
            case["autoJoin"] == true && core::room_allowed(case, "!room:server"),
            case["join"] == true
        );
    }
}
#[test]
fn platform_context_admission_attachment_corpus() {
    let oracle: Value = serde_json::from_str(include_str!("oracles/platform.json")).unwrap();
    for platform in ["slack", "mattermost", "telegram"] {
        for case in oracle[platform].as_array().unwrap() {
            assert_eq!(
                core::normalize(platform, &case["input"], case["isolated"] == true),
                case["expected"],
                "{platform}: {}",
                case["input"]
            );
        }
    }
    for kind in [
        "slackAdmission",
        "mattermostAdmission",
        "telegramImplicit",
        "telegramBotReply",
        "telegramAttachmentBypass",
    ] {
        for case in oracle[kind].as_array().unwrap() {
            assert_eq!(
                core::admission(kind, &case["input"]),
                case["expected"] == true,
                "{kind}: {}",
                case["input"]
            );
        }
    }
    for case in oracle["attachments"].as_array().unwrap() {
        assert_eq!(core::attachments(&case["input"]), case["expected"]);
    }
    for case in oracle["retryDelays"].as_array().unwrap() {
        assert_eq!(json!(retry_delay(case["input"].as_f64())), case["expected"]);
    }
    for case in oracle["extensions"].as_array().unwrap() {
        assert_eq!(
            core::extension(&case["input"], "bin"),
            text(case, "expected")
        );
    }
}
#[test]
fn empty_and_missing_tool_ids_have_stable_source_fallback_and_counter_correlation() {
    for mode in ["events", "trace", "status", "hermes"] {
        let mut presenter = tools::Presenter::new(json!({"mode":mode,"showArguments":true}));
        let mut output = vec![];
        for (index, title) in [Some("read"), Some("grep"), None, None]
            .into_iter()
            .enumerate()
        {
            let mut start =
                json!({"sessionUpdate":"tool_call","rawInput":{"path":format!("path-{index}")}});
            if index % 2 == 0 {
                start["toolCallId"] = "".into();
            }
            if let Some(title) = title {
                start["title"] = title.into();
            }
            let mut finished = start.clone();
            finished["sessionUpdate"] = "tool_call_update".into();
            finished["status"] = "completed".into();
            output.extend(presenter.update(&start));
            output.extend(presenter.update(&finished));
        }
        assert!(
            output.iter().all(|activity| activity["activityId"]
                .as_str()
                .is_some_and(|id| !id.is_empty())),
            "{mode}: {output:?}"
        );
        if mode == "events" {
            assert_eq!(
                output
                    .iter()
                    .map(|activity| activity["activityId"].clone())
                    .collect::<Vec<_>>(),
                vec![
                    json!("read"),
                    json!("grep"),
                    json!("tool:0"),
                    json!("tool:1")
                ]
            );
        } else {
            assert!(
                output
                    .iter()
                    .all(|activity| activity["activityId"] == "trace:0")
            );
            let message = output.last().unwrap()["message"].as_str().unwrap();
            match mode {
                "status" => assert_eq!(message, "Completed 4 tools."),
                "trace" => assert_eq!(message.matches("[unknown]").count(), 2, "{message}"),
                "hermes" => assert!(message.contains("⚙️ unknown (×2)"), "{message}"),
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn tool_progress_presentation_and_safe_html_corpus() {
    let oracle: Value = serde_json::from_str(include_str!("oracles/tools.json")).unwrap();
    for case in oracle["progress"].as_array().unwrap() {
        assert_eq!(
            tools::hermes(&case["entry"], case["showArguments"] == true),
            text(case, "expected"),
            "{}",
            case["entry"]
        );
    }
    for case in oracle["html"].as_array().unwrap() {
        assert_eq!(
            tools::matrix_notice(text(case, "text"), case["rich"] == true),
            case["expected"]
        );
    }
    for case in oracle["calls"].as_array().unwrap() {
        assert_eq!(
            json!(tools::call(
                &case["activity"],
                &case["options"],
                case["editable"] == true
            )),
            case["expected"]
        );
    }
}
