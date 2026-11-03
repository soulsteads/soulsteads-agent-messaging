use serde_json::{Value, json};
use soulsteads_messaging::tools::{OutputPresenter, TOOL_PAGE_MAX_WIRE_BYTES, TurnPresenter};

fn options(mode: &str, calls: bool, arguments: bool, output: Value, maximum: u64) -> Value {
    json!({
        "mode": mode,
        "showCalls": calls,
        "showArguments": arguments,
        "showOutputFor": output,
        "maxTraceEntries": maximum,
    })
}

fn call(id: &str, title: &str, input: Value) -> Value {
    json!({
        "sessionUpdate":"tool_call", "toolCallId":id, "title":title,
        "status":"in_progress", "rawInput":input,
    })
}

#[test]
fn connector_presentation_is_targeted_and_preserves_literal_visible_arguments() {
    let input = json!({"command":"printf '@ ``` 😀'", "private":"only when visible"});
    for platform in ["telegram"] {
        let mut presenter = TurnPresenter::new(
            options("events", true, true, json!([]), 20),
            platform,
            json!({"mode":"off"}),
        );
        let event = presenter
            .update(&call("call", "bash", input.clone()))
            .remove(0);
        assert_eq!(event["toolPresentation"]["kind"], "tool_call", "{platform}");
        assert_eq!(event["toolPresentation"]["tool"], "bash", "{platform}");
        assert_eq!(
            serde_json::from_str::<Value>(event["toolPresentation"]["arguments"].as_str().unwrap())
                .unwrap(),
            input,
            "{platform}"
        );
    }
    for platform in ["matrix", "web", "cli"] {
        let mut presenter = TurnPresenter::new(
            options("events", true, true, json!([]), 20),
            platform,
            json!({"mode":"off"}),
        );
        let event = presenter
            .update(&call("call", "bash", input.clone()))
            .remove(0);
        assert!(
            event.get("toolPresentation").is_none(),
            "{platform}: {event}"
        );
        assert_eq!(
            event,
            json!({
                "type":"activity", "activityId":"call", "message":"command=printf '@ ``` 😀', private=only when visible [bash]"
            }),
            "{platform}"
        );
    }
}

#[test]
fn every_accepted_call_mode_emits_targeted_page_metadata_without_collapsing_pages() {
    for mode in ["status", "trace", "hermes"] {
        let mut presenter = TurnPresenter::new(
            options(mode, true, true, json!([]), 1),
            "telegram",
            json!({"mode":"off"}),
        );
        let first = presenter.update(&call("one", "bash", json!({"command":"first @ ```"})));
        assert_eq!(first.len(), 1, "{mode}");
        assert_eq!(first[0]["toolPresentation"]["kind"], "tool_page", "{mode}");
        assert_eq!(first[0]["toolPresentation"]["mode"], mode, "{mode}");
        assert_eq!(
            serde_json::from_str::<Value>(
                first[0]["toolPresentation"]["entries"][0]["arguments"]
                    .as_str()
                    .unwrap()
            )
            .unwrap(),
            json!({"command":"first @ ```"}),
            "{mode}"
        );

        let second = presenter.update(&call("two", "read", json!({"path":"second.rs"})));
        if mode == "status" {
            assert_eq!(second.len(), 1, "{mode}");
            assert_eq!(second[0]["toolPresentation"]["completed"], 0, "{mode}");
            assert_eq!(
                second[0]["toolPresentation"]["active"]["tool"], "read",
                "{mode}"
            );
        } else {
            // Both native page styles refresh the earlier page's visible
            // position when a second page is created.
            assert_eq!(second.len(), 2, "{mode}");
            assert_eq!(second[0]["activityId"], "trace:0", "{mode}");
            assert_eq!(second[0]["toolPresentation"]["page"], 1, "{mode}");
            assert_eq!(second[0]["toolPresentation"]["pages"], 2, "{mode}");
            let page = &second[1];
            assert_eq!(page["toolPresentation"]["page"], 2, "{mode}");
            assert_eq!(page["toolPresentation"]["pages"], 2, "{mode}");
            assert_eq!(page["activityId"], "trace:1", "{mode}");
            assert_eq!(
                page["toolPresentation"]["entries"]
                    .as_array()
                    .unwrap()
                    .len(),
                1,
                "{mode}"
            );
            assert_eq!(
                page["toolPresentation"]["entries"][0]["tool"], "read",
                "{mode}"
            );
        }
    }
}

#[test]
fn matrix_web_and_cli_output_contracts_are_exact_and_connector_output_is_literal() {
    let update = json!({
        "sessionUpdate":"tool_call_update", "toolCallId":"out", "title":"bash",
        "status":"completed", "rawOutput":{"output":" @ ``` 😀 "}
    });
    for platform in ["matrix", "web", "cli"] {
        let mut presenter =
            OutputPresenter::new(options("events", true, false, json!(["*"]), 20), platform);
        let event = presenter.update(&update);
        if platform == "cli" {
            assert!(event.is_none());
        } else if platform == "web" {
            assert_eq!(
                event,
                Some(json!({"type":"tool_result", "tool":"bash", "text":"@ ``` 😀"}))
            );
        } else {
            assert_eq!(event, Some(json!({"type":"response", "text":"@ ``` 😀"})));
        }
    }
    let mut targeted =
        OutputPresenter::new(options("events", true, false, json!(["*"]), 20), "telegram");
    let event = targeted.update(&update).unwrap();
    assert_eq!(event["text"], "@ ``` 😀");
    assert_eq!(
        event["toolPresentation"],
        json!({"kind":"tool_output", "tool":"bash"})
    );
}

#[test]
fn privacy_gates_sparse_updates_dedup_and_bounds_remain_independent() {
    let call_update = call("one", "bash", json!({"command":"secret"}));
    for mode in ["off", "events", "status", "trace", "hermes"] {
        for calls in [false, true] {
            for arguments in [false, true] {
                let mut presenter = TurnPresenter::new(
                    options(mode, calls, arguments, json!(["bash"]), 1),
                    "telegram",
                    json!({"mode":"off"}),
                );
                let events = presenter.update(&call_update);
                assert_eq!(
                    events.is_empty(),
                    mode == "off" || !calls,
                    "{mode}/{calls}/{arguments}"
                );
                for event in events {
                    let presentation = &event["toolPresentation"];
                    let actual = if mode == "events" {
                        &presentation["arguments"]
                    } else {
                        &presentation["entries"][0]["arguments"]
                    };
                    assert_eq!(actual.is_null(), !arguments, "{mode}/{calls}/{arguments}");
                }
            }
        }
    }
    let output = json!({"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"in_progress","rawOutput":{"output":"a😀"}});
    let mut presenter = TurnPresenter::new(
        options("off", false, false, json!(["bash"]), 1),
        "telegram",
        json!({"mode":"off"}),
    );
    assert!(presenter.update(&call_update).is_empty());
    let first = presenter.update(&output);
    assert_eq!(
        first,
        vec![
            json!({"type":"response","text":"a😀","toolPresentation":{"kind":"tool_output","tool":"bash"}})
        ]
    );
    assert!(presenter.update(&output).is_empty());
    let second = presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"in_progress","rawOutput":{"output":"a😀b"}}));
    assert_eq!(second[0]["text"], "b");

    let mut bounded = TurnPresenter::new(
        options("events", true, false, json!([]), 1),
        "telegram",
        json!({"mode":"off"}),
    );
    for index in 0..1024 {
        bounded.update(&call(&format!("{index}"), "bash", json!({"command":index})));
    }
    assert!(
        bounded
            .update(&call("overflow", "bash", json!({"command":"lost"})))
            .is_empty()
    );
}

#[test]
fn connector_metadata_uses_bounded_literal_arguments_for_acp_strings_and_pages() {
    // Immutable-base Matrix/Web/CLI fixture from `git show 05d5ea9:tests/matrix_file_labels.rs`.
    // This is an expected event fixture, not a second invocation of the current Presenter.
    let base_update = call("base", "bash", json!({"command":"printf base"}));
    let base_event = json!({
        "type":"activity", "activityId":"base", "message":"command=printf base [bash]"
    });
    for platform in ["matrix", "web", "cli"] {
        let mut presenter = TurnPresenter::new(
            options("events", true, true, json!([]), 2),
            platform,
            json!({"mode":"off"}),
        );
        assert_eq!(
            presenter.update(&base_update),
            vec![base_event.clone()],
            "{platform}"
        );
    }

    let raw =
        r#"{"command":"  @ `code` <b>😀</b>\n","html":"<&>","unicode":"é😀","escaped":"\"\\\\\n"}"#;
    let semantic: Value = serde_json::from_str(raw).unwrap();
    let expected_literal = serde_json::to_string(&semantic).unwrap();
    let update = json!({
        "sessionUpdate":"tool_call", "toolCallId":"acp", "title":"bash",
        "status":"in_progress", "rawInput":raw,
    });
    let mut presenter = TurnPresenter::new(
        options("events", true, true, json!([]), 2),
        "telegram",
        json!({"mode":"off"}),
    );
    let event = presenter.update(&update).remove(0);
    let literal = event["toolPresentation"]["arguments"].as_str().unwrap();
    assert_eq!(literal, expected_literal);
    assert_eq!(serde_json::from_str::<Value>(literal).unwrap(), semantic);
    assert!(literal.contains("@ `code` <b>😀</b>"));

    let huge = json!({"command": format!("@`<tag>{}", "\u{0001}".repeat(10_000))}).to_string();
    let mut pages = TurnPresenter::new(
        options("trace", true, true, json!([]), 2),
        "telegram",
        json!({"mode":"off"}),
    );
    let first = pages.update(&json!({
        "sessionUpdate":"tool_call", "toolCallId":"one", "title":"bash", "rawInput":huge,
    }));
    let page = &first[0]["toolPresentation"];
    let bounded = page["entries"][0]["arguments"].as_str().unwrap();
    assert!(bounded.encode_utf16().count() <= 180, "{bounded:?}");
    assert!(bounded.ends_with("… (truncated)"), "{bounded:?}");
    assert!(serde_json::to_vec(page).unwrap().len() <= 1024, "{page}");
    let second = pages.update(&json!({
        "sessionUpdate":"tool_call", "toolCallId":"two", "title":"bash", "rawInput":huge,
    }));
    let page = &second[0]["toolPresentation"];
    assert_eq!(page["entries"].as_array().unwrap().len(), 2);
    assert!(serde_json::to_vec(page).unwrap().len() <= 2048, "{page}");

    let mut sparse = TurnPresenter::new(
        options("status", true, true, json!([]), 2),
        "telegram",
        json!({"mode":"off"}),
    );
    sparse.update(&json!({
        "sessionUpdate":"tool_call", "toolCallId":"late", "title":"bash", "status":"pending"
    }));
    let late = sparse.update(&json!({
        "sessionUpdate":"tool_call_update", "toolCallId":"late", "rawInput":raw
    }));
    let active = &late[0]["toolPresentation"]["active"];
    assert_eq!(
        serde_json::from_str::<Value>(active["arguments"].as_str().unwrap()).unwrap(),
        semantic
    );
    let hidden = TurnPresenter::new(
        options("events", true, false, json!([]), 2),
        "telegram",
        json!({"mode":"off"}),
    )
    .update(&update)
    .remove(0);
    assert!(hidden["toolPresentation"]["arguments"].is_null());

    let oversized = "malformed-😀<b>".repeat(100);
    let output = OutputPresenter::new(options("events", true, false, json!(["*"]), 2), "telegram")
        .update(&json!({
            "sessionUpdate":"tool_call_update", "toolCallId":"huge-output", "title":oversized,
            "status":"completed", "rawOutput":{"output":"done"}
        }))
        .unwrap();
    assert!(
        output["toolPresentation"]["tool"]
            .as_str()
            .unwrap()
            .encode_utf16()
            .count()
            <= 180
    );
    assert!(
        output["toolPresentation"]["tool"]
            .as_str()
            .unwrap()
            .ends_with("… (truncated)")
    );
}

#[test]
fn connector_page_wire_budget_covers_1024_hidden_hostile_labels() {
    // A title is source-controlled only after admission; metadata must not copy it whole.
    let title = format!("hostile-{}", "\"\\\u{0800}😀".repeat(256));
    let mut presenter = TurnPresenter::new(
        options("trace", true, false, json!([]), 1024),
        "telegram",
        json!({"mode":"off"}),
    );
    let mut last = Vec::new();
    for index in 0..1024 {
        last = presenter.update(&call(
            &format!("id-{index}"),
            &title,
            json!({"ignored": index}),
        ));
    }
    assert_eq!(last.len(), 1);
    let page = &last[0]["toolPresentation"];
    let entries = page["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1024);
    assert!(entries.iter().all(|entry| entry["arguments"].is_null()));
    assert!(entries.iter().all(|entry| {
        entry["tool"]
            .as_str()
            .is_some_and(|label| label.encode_utf16().count() <= 180)
    }));
    assert!(
        page["active"]["tool"]
            .as_str()
            .is_some_and(|label| label.encode_utf16().count() <= 180)
    );
    let bytes = serde_json::to_vec(page).unwrap().len();
    assert!(
        bytes <= TOOL_PAGE_MAX_WIRE_BYTES,
        "{bytes} bytes exceeds {TOOL_PAGE_MAX_WIRE_BYTES}"
    );
}

#[test]
fn immutable_base_matrix_web_cli_status_trace_hermes_sparse_pages_and_output() {
    // Exact base schemas transcribed from `git show 05d5ea9:src/tools.rs`; these
    // expected events never invoke the current Presenter to derive an oracle.
    let first = call("one", "bash", json!({"command":"first"}));
    let second = call("two", "grep", json!({"query":"second"}));
    let expected = [
        (
            "status",
            vec![
                json!({"type":"activity","activityId":"trace:0","message":"Working...\nCurrent: [running] [bash]\nCompleted: 0 tools"}),
            ],
            vec![
                json!({"type":"activity","activityId":"trace:0","message":"Working...\nCurrent: [running] [grep]\nCompleted: 0 tools"}),
            ],
        ),
        (
            "trace",
            vec![
                json!({"type":"activity","activityId":"trace:0","message":"Tool trace (working)\n\n[running] [bash]"}),
            ],
            vec![
                json!({"type":"activity","activityId":"trace:0","message":"Tool trace (part 1/2, working)\n\n[running] [bash]"}),
                json!({"type":"activity","activityId":"trace:1","message":"Tool trace (part 2/2, working)\n\n[running] [grep]"}),
            ],
        ),
        (
            "hermes",
            vec![json!({"type":"activity","activityId":"trace:0","message":"💻 terminal"})],
            vec![json!({"type":"activity","activityId":"trace:1","message":"🔎 Searching files"})],
        ),
    ];
    for platform in ["matrix", "web", "cli"] {
        for (mode, first_expected, second_expected) in &expected {
            let mut presenter = TurnPresenter::new(
                options(mode, true, false, json!([]), 1),
                platform,
                json!({"mode":"off"}),
            );
            assert_eq!(
                presenter.update(&first),
                *first_expected,
                "{platform}/{mode}/first"
            );
            assert_eq!(
                presenter.update(&second),
                *second_expected,
                "{platform}/{mode}/paged"
            );
        }
    }
    let output = json!({"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"completed","rawOutput":{"output":"base output"}});
    assert_eq!(
        OutputPresenter::new(options("events", true, false, json!(["*"]), 1), "matrix")
            .update(&output),
        Some(json!({"type":"response","text":"base output"}))
    );
    assert_eq!(
        OutputPresenter::new(options("events", true, false, json!(["*"]), 1), "web")
            .update(&output),
        Some(json!({"type":"tool_result","tool":"bash","text":"base output"}))
    );
    assert!(
        OutputPresenter::new(options("events", true, false, json!(["*"]), 1), "cli")
            .update(&output)
            .is_none()
    );
}

#[test]
fn nontelegram_platforms_match_immutable_head_events_and_outputs_without_presentation_metadata() {
    let call = json!({
        "sessionUpdate":"tool_call", "toolCallId":"one", "title":"bash",
        "status":"in_progress", "rawInput":{"command":"safe\n```\n**NOT_LITERAL**"}
    });
    let output = json!({
        "sessionUpdate":"tool_call_update", "toolCallId":"one", "title":"bash",
        "status":"completed", "rawOutput":{"output":"safe\n```\n**NOT_LITERAL**"}
    });
    for platform in [
        "matrix",
        "web",
        "cli",
        "discord",
        "mattermost",
        "slack",
        "whatsapp",
    ] {
        let event = TurnPresenter::new(
            options("events", true, false, json!([]), 1),
            platform,
            json!({"mode":"off"}),
        )
        .update(&call);
        assert_eq!(
            event,
            vec![json!({"type":"activity","activityId":"one","message":"[bash]"})],
            "{platform}"
        );
        let actual =
            OutputPresenter::new(options("events", true, false, json!(["*"]), 1), platform)
                .update(&output);
        let expected = if platform == "cli" {
            None
        } else if platform == "web" {
            Some(json!({"type":"tool_result","tool":"bash","text":"safe\n```\n**NOT_LITERAL**"}))
        } else {
            Some(json!({"type":"response","text":"safe\n```\n**NOT_LITERAL**"}))
        };
        assert_eq!(actual, expected, "{platform}");
    }
}

#[test]
fn telegram_native_pages_keep_terminal_status_and_truthful_phase_for_both_argument_gates() {
    for mode in ["status", "trace", "hermes"] {
        for arguments in [false, true] {
            let mut presenter = TurnPresenter::new(
                options(mode, true, arguments, json!([]), 1),
                "telegram",
                json!({"mode":"off"}),
            );
            let started = presenter.update(&call(
                "one",
                "odd label",
                json!({"command":"hidden marker 😀"}),
            ));
            let completed = presenter.update(&json!({
                "sessionUpdate":"tool_call_update", "toolCallId":"one", "status":"failed"
            }));
            let page = completed
                .iter()
                .find(|event| event["toolPresentation"]["kind"] == "tool_page")
                .expect("terminal page refresh");
            assert!(
                page["toolPresentation"]["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["status"] == "failed"),
                "{mode}/{arguments}"
            );
            assert!(
                !serde_json::to_string(&completed)
                    .unwrap()
                    .contains("hidden marker")
                    || arguments,
                "{mode}/{arguments}"
            );
            assert!(
                started
                    .iter()
                    .all(|event| event["toolPresentation"]["kind"].is_string())
            );
        }
    }
}
