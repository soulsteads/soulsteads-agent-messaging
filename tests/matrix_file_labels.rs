use soulsteads_messaging::tools::TurnPresenter;
use serde_json::{Value, json};

fn options(mode: &str, arguments: bool) -> Value {
    json!({"mode":mode,"showCalls":true,"showArguments":arguments,"showOutputFor":[],"maxTraceEntries":20})
}

#[test]
fn matrix_read_notices_show_requested_lines_without_other_arguments() {
    for mode in ["events", "trace", "status", "hermes"] {
        for tool in ["read", "read_file", "readfile"] {
            let mut presenter =
                TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"off"}));
            let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"range","title":tool,"rawInput":{"filePath":"/private/config.rs","offset":20,"limit":10,"content":"secret"}}));
            let message = events[0]["message"].as_str().unwrap();
            assert!(message.contains("config.rs"), "{mode}/{tool}: {message}");
            assert!(
                message.contains("requested lines 20–29"),
                "{mode}/{tool}: {message}"
            );
            assert!(!message.contains("private") && !message.contains("secret"));
        }
    }
}

#[test]
fn matrix_read_partial_ranges_are_explicit_and_malformed_ranges_are_omitted() {
    for (range, expected) in [
        (json!({"offset":20}), "requested from line 20"),
        (json!({"limit":10}), "requested lines 1–10"),
        (json!({"offset":1,"limit":1}), "requested lines 1–1"),
        (json!({}), ""),
        (json!({"offset":0,"limit":10}), ""),
        (json!({"offset":-1,"limit":10}), ""),
        (json!({"offset":"20","limit":10}), ""),
        (json!({"offset":1.5,"limit":10}), ""),
        (json!({"offset":20,"limit":0}), ""),
        (json!({"offset":20,"limit":null}), ""),
        (json!({"offset":u64::MAX,"limit":2}), ""),
    ] {
        for mode in ["events", "trace", "status", "hermes"] {
            for show_arguments in [false, true] {
                let mut presenter = TurnPresenter::new(
                    options(mode, show_arguments),
                    "matrix",
                    json!({"mode":"off"}),
                );
                let mut input = range.clone();
                input["path"] = json!("/private/config.rs");
                let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"range","title":"Read /private/config.rs","kind":"read","rawInput":input.to_string()}));
                let text = events[0]["message"].as_str().unwrap();
                if expected.is_empty() {
                    assert!(!text.contains("requested"), "{range}/{mode}: {text}");
                } else {
                    assert!(text.contains(expected), "{range}/{mode}: {text}");
                }
                assert!(!text.contains("/private"));
            }
        }
    }
}

#[test]
fn matrix_read_range_survives_completion_and_respects_full_paths() {
    for mode in ["trace", "status", "hermes"] {
        let mut presenter =
            TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"off"}))
                .with_matrix_full_paths(true);
        let start = json!({"sessionUpdate":"tool_call","toolCallId":"range","title":"read","rawInput":{"filePath":"/private/config.rs","offset":20,"limit":10}});
        let first = presenter.update(&start);
        let text = first[0]["message"].as_str().unwrap();
        assert!(text.contains("/private/config.rs") && text.contains("requested lines 20–29"));
        let completed = presenter.update(
            &json!({"sessionUpdate":"tool_call_update","toolCallId":"range","status":"completed"}),
        );
        if mode == "trace" {
            assert!(
                completed[0]["message"]
                    .as_str()
                    .unwrap()
                    .contains("requested lines 20–29")
            );
        }
        assert_eq!(start["rawInput"]["offset"], 20);
    }
    let mut presenter =
        TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
    let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"edit","title":"edit","rawInput":{"path":"config.rs","offset":20,"limit":10}}));
    assert_eq!(events[0]["message"], "🔧 Editing config.rs");
}

#[test]
fn full_path_opt_in_applies_to_every_matrix_activity_style() {
    for mode in ["events", "trace", "status", "hermes"] {
        let mut presenter =
            TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"auto"}))
                .with_matrix_full_paths(true);
        let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"file","title":"edit","status":"in_progress","rawInput":{"filePath":"/private/project/config.rs","oldString":"secretold"}}));
        let text = events[0]["message"].as_str().unwrap();
        assert!(
            text.contains("/private/project/config.rs"),
            "{mode}: {text}"
        );
        assert!(!text.contains("secretold"));
    }
}

#[test]
fn other_platforms_keep_their_existing_argument_visibility() {
    for platform in ["web", "discord", "telegram", "mattermost", "slack", "cli"] {
        for mode in ["events", "trace", "status", "hermes"] {
            for show_arguments in [false, true] {
                let update = json!({"sessionUpdate":"tool_call","toolCallId":"read","title":"read","rawInput":{"filePath":"/private/project/config.rs","offset":20,"limit":10}});
                let config = options(mode, show_arguments);
                let expected = soulsteads_messaging::tools::Presenter::new(config.clone()).update(&update);
                let actual = TurnPresenter::new(config, platform, json!({"mode":"auto"}))
                    .with_matrix_full_paths(false)
                    .update(&update);
                assert_eq!(actual, expected, "{platform}/{mode}/{show_arguments}");
            }
        }
    }
}

#[test]
fn disabled_calls_remain_disabled_and_file_output_is_not_rewritten() {
    let update = json!({"sessionUpdate":"tool_call","toolCallId":"read","title":"read","rawInput":{"filePath":"/private/project/config.rs","offset":20,"limit":10}});
    for config in [
        json!({"mode":"off","showOutputFor":[]}),
        json!({"mode":"hermes","showCalls":false,"showOutputFor":[]}),
    ] {
        assert!(
            TurnPresenter::new(config, "matrix", json!({"mode":"auto"}))
                .update(&update)
                .is_empty()
        );
    }
    let mut config = options("hermes", false);
    config["showOutputFor"] = json!(["read"]);
    let mut presenter = TurnPresenter::new(config, "matrix", json!({"mode":"off"}));
    presenter.update(&update);
    let events=presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"read","title":"read","status":"completed","rawOutput":{"output":"source mentions /private/project/config.rs"}}));
    assert!(
        events.iter().any(|v| v["type"] == "response"
            && v["text"] == "source mentions /private/project/config.rs")
    );
}

#[test]
fn matrix_human_edit_title_identifies_patch_targets() {
    let mut presenter =
        TurnPresenter::new(options("hermes", true), "matrix", json!({"mode":"off"}));
    let input = json!({"patchText":"*** Begin Patch\n*** Update File: /private/project/config.rs\n@@\n-old secret\n+new secret\n*** End Patch"});
    let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"patch","title":"Editing","kind":"edit","rawInput":input}));
    assert_eq!(events[0]["message"], "🔧 Editing config.rs");
}

#[test]
fn matrix_execute_shows_code_and_keeps_late_input_on_same_activity() {
    for title in ["execute", "Executing"] {
        let mut presenter =
            TurnPresenter::new(options("hermes", true), "matrix", json!({"mode":"off"}));
        presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"code","title":title,"kind":"execute","status":"pending"}));
        let events = presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"code","status":"in_progress","rawInput":{"code":"console.log(await tools.shell({command: 'git status --short'}))"}}));
        let text = events[0]["message"].as_str().unwrap();
        assert!(text.contains("git status --short"), "{title}: {text}");
        assert!(text.contains("```"), "{title}: {text}");
        assert_eq!(events[0]["activityId"], "trace:0");
    }
}

#[test]
fn matrix_shell_shows_command_without_tool_output() {
    let mut presenter =
        TurnPresenter::new(options("hermes", true), "matrix", json!({"mode":"off"}));
    let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"shell","title":"shell","kind":"execute","rawInput":{"command":"git status --short"},"rawOutput":{"output":"private output"}}));
    let text = events[0]["message"].as_str().unwrap();
    assert!(text.contains("git status --short"));
    assert!(!text.contains("private output"));
}

#[test]
fn matrix_execute_terminal_threads_are_scoped_and_keep_late_code_updates() {
    for title in ["execute", "exec_command", "Executing", "Run task"] {
        for platform in ["matrix", "discord"] {
            let mut presenter =
                TurnPresenter::new(options("hermes", true), platform, json!({"mode":"auto"}));
            let start=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"exec","title":title,"kind":"execute"}));
            if platform == "matrix" {
                assert_eq!(start[0]["type"], "terminal_call");
                let late=presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"exec","rawInput":{"code":"return await tools.bash({command:'git status --short'});"}}));
                assert!(late.iter().any(|e| {
                    e["type"] == "activity"
                        && e["terminalToolCallId"] == "exec"
                        && e["message"]
                            .as_str()
                            .unwrap()
                            .contains("git status --short")
                }));
            } else {
                assert_eq!(start[0]["type"], "activity");
            }
        }
    }
}

#[test]
fn matrix_execute_respects_visibility_and_prefers_command_over_code() {
    for title in ["execute", "exec_command", "Executing"] {
        for visible in [false, true] {
            let mut presenter =
                TurnPresenter::new(options("hermes", visible), "matrix", json!({"mode":"off"}));
            let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"exec","title":title,"kind":"execute","rawInput":{"command":"git status","code":"secret ignored code"}}));
            let text = events[0]["message"].as_str().unwrap();
            assert_eq!(text.contains("git status"), visible);
            assert!(!text.contains("secret ignored code"));
        }
    }
}

#[test]
fn matrix_subagent_shows_role_and_short_task_on_late_input_only_when_visible() {
    for visible in [true, false] {
        for platform in ["matrix", "discord"] {
            for input in [
                json!({"subagent_type":"reviewer","description":"Review Matrix progress","prompt":"PRIVATE FULL PROMPT","api_key":"PRIVATE KEY"}),
                json!(
                    "{\"agent\":\"reviewer\",\"description\":\"Review Matrix progress\",\"prompt\":\"PRIVATE FULL PROMPT\"}"
                ),
            ] {
                let mut presenter =
                    TurnPresenter::new(options("hermes", visible), platform, json!({"mode":"off"}));
                let initial = presenter.update(
                    &json!({"sessionUpdate":"tool_call","toolCallId":"child","title":"subagent"}),
                );
                let later=presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"child","rawInput":input}));
                if platform == "matrix" && visible {
                    assert_eq!(later[0]["activityId"], initial[0]["activityId"]);
                    assert_eq!(
                        later[0]["message"],
                        "🤖 Subagent reviewer — Review Matrix progress"
                    );
                } else if let Some(event) = later.first() {
                    assert!(
                        !event["message"]
                            .as_str()
                            .unwrap()
                            .contains("Review Matrix progress")
                    );
                }
                for event in &later {
                    assert!(!event["message"].as_str().unwrap().contains("PRIVATE"));
                }
            }
        }
    }
}

#[test]
fn matrix_subagent_details_are_bounded_and_html_escaped() {
    let mut presenter =
        TurnPresenter::new(options("hermes", true), "matrix", json!({"mode":"off"}));
    let label = format!("<script>bad()</script> {}", "😀".repeat(200));
    let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"child","title":"subagent","rawInput":{"subagent_type":"reviewer","description":label}}));
    let text = events[0]["message"].as_str().unwrap();
    assert!(text.encode_utf16().count() <= 200);
    assert!(text.ends_with('…'));
    let notice = soulsteads_messaging::tools::matrix_notice(text, true);
    let html = notice["formatted_body"].as_str().unwrap();
    assert!(html.contains("&lt;script&gt;"));
    assert!(!html.contains("<script>"));
}

#[test]
fn matrix_acp_edit_kind_uses_the_filename_not_a_path_bearing_title() {
    let mut presenter =
        TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
    let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"human-title","title":"Edit /private/project/config.rs","kind":"edit","status":"in_progress","rawInput":"{\"filePath\":\"/private/project/config.rs\"}"}));
    assert_eq!(events[0]["message"], "🔧 Editing config.rs");
}

#[test]
fn matrix_malformed_file_paths_are_not_displayed_as_raw_json() {
    for mode in ["events", "trace", "status", "hermes"] {
        let mut presenter =
            TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"off"}));
        let events=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"bad","title":"edit","status":"completed","rawInput":{"filePath":["/private/one.rs","/private/two.rs"]}}));
        assert!(
            !serde_json::to_string(&events).unwrap().contains("/private"),
            "{mode}: {events:?}"
        );
    }
}

#[test]
fn matrix_full_paths_are_disabled_by_default_and_require_a_boolean_opt_in() {
    assert_eq!(
        soulsteads_messaging::config::defaults()["matrix"]["showFullPaths"],
        false
    );
    for value in [
        json!(null),
        json!("true"),
        json!(1),
        json!([]),
        json!({}),
        json!(false),
        json!(true),
    ] {
        let loaded = soulsteads_messaging::config::load(
            &json!({"matrix":{"showFullPaths":value}}).to_string(),
            &Default::default(),
        )
        .unwrap();
        assert_eq!(loaded["matrix"]["showFullPaths"], value == true, "{value}");
    }
}

#[test]
fn matrix_file_labels_are_visible_in_each_activity_mode() {
    for mode in ["events", "trace", "status", "hermes"] {
        for (tool, key) in [
            ("read", "filePath"),
            ("read_file", "file_path"),
            ("edit", "path"),
            ("write", "target_file"),
        ] {
            let mut presenter =
                TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"off"}));
            let mut arguments = json!({"content":"private body", "oldString":"private old"});
            arguments[key] = "C:\\private\\project\\résumé.md".into();
            let event = json!({"sessionUpdate":"tool_call","toolCallId":"call","title":tool,"status":"in_progress","rawInput":arguments});
            let events = presenter.update(&event);
            let message = events[0]["message"].as_str().unwrap();
            assert!(message.contains("résumé.md"), "{mode}/{tool}: {message}");
            assert!(!message.contains("private"), "{mode}/{tool}: {message}");
            assert_eq!(
                event["rawInput"][key], "C:\\private\\project\\résumé.md",
                "presentation must not mutate provider inputs"
            );
        }
    }
}

#[test]
fn matrix_patch_headers_identify_edited_files_without_disclosing_patch_content() {
    for mode in ["hermes", "events", "trace", "status"] {
        let mut presenter =
            TurnPresenter::new(options(mode, false), "matrix", json!({"mode":"off"}));
        let update = json!({"sessionUpdate":"tool_call","toolCallId":"patch-1","title":"patch","status":"in_progress","rawInput":{"mode":"patch","patch":"*** Begin Patch\n*** Update File: /private/project/config.rs\n@@\n-secret old\n+secret new\n*** Add File: /private/project/tests/config_test.rs\n+secret body\n*** End Patch"}});
        let events = presenter.update(&update);
        let text = events[0]["message"].as_str().unwrap();
        assert!(
            text.contains("config.rs") && text.contains("config_test.rs"),
            "{mode}: {text}"
        );
        assert!(
            !text.contains("private") && !text.contains("secret"),
            "{mode}: {text}"
        );
    }
}

#[test]
fn matrix_patch_aliases_paths_and_completion_are_presented_safely() {
    for tool in ["patch", "apply_patch"] {
        for key in ["patch", "patchText", "patch_text"] {
            for full_paths in [false, true] {
                let mut presenter =
                    TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}))
                        .with_matrix_full_paths(full_paths);
                let mut input = json!({});
                input[key] = json!(
                    "*** Begin Patch\n*** Delete File: C:\\private\\old.rs\n*** Update File: /private/résumé.rs\n*** Move to: /private/new.rs\n@@\n+*** Update File: /private/not-a-target.rs\n*** End Patch"
                );
                let update = json!({"sessionUpdate":"tool_call","toolCallId":"patch","title":tool,"rawInput":input.to_string()});
                let events = presenter.update(&update);
                let text = events[0]["message"].as_str().unwrap();
                assert!(
                    text.contains("old.rs") && text.contains("résumé.rs"),
                    "{text}"
                );
                assert_eq!(text.contains("new.rs"), tool == "apply_patch", "{text}");
                assert!(!text.contains("not-a-target"));
                assert_eq!(text.contains("private"), full_paths);
                assert_eq!(update["rawInput"], input.to_string());
                let completed = presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"patch","status":"completed"}));
                assert!(
                    completed.is_empty(),
                    "completion must retain the existing label"
                );
            }
        }
    }
}

#[test]
fn matrix_invalid_patches_do_not_expose_content_and_direct_paths_take_precedence() {
    for patch in [
        json!(null),
        json!(["/private/config.rs"]),
        json!("secret /private/config.rs"),
        json!("*** Begin Patch\n*** Update File: /private/config.rs"),
        json!(format!(
            "*** Begin Patch\n*** Update File: /private/config.rs\n+{}\n*** End Patch",
            "secret".repeat(200_000)
        )),
    ] {
        let mut presenter =
            TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
        let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"patch","title":"patch","rawInput":{"patch":patch}}));
        assert_eq!(events[0]["message"], "🔧 Editing");
    }
    let mut presenter =
        TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
    let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"patch","title":"patch","rawInput":{"path":"/private/actual.rs","patch":"*** Begin Patch\n*** Update File: /private/other.rs\n*** End Patch"}}));
    assert_eq!(events[0]["message"], "🔧 Editing actual.rs");
}

#[test]
fn matrix_patch_move_headers_in_hunks_are_not_exposed() {
    for tool in ["patch", "apply_patch"] {
        let mut presenter =
            TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
        let patch = "*** Begin Patch\n*** Update File: actual.rs\n@@\n*** Move to: private-body-text\n*** End Patch";
        let update = json!({"sessionUpdate":"tool_call","toolCallId":"patch","title":tool,"rawInput":{"patch":patch}});
        let events = presenter.update(&update);
        let text = events[0]["message"].as_str().unwrap();
        assert!(text.contains("actual.rs"), "{tool}: {text}");
        assert!(!text.contains("private-body-text"), "{tool}: {text}");
    }
}

#[test]
fn matrix_patch_targets_prevent_unrelated_edits_from_collapsing_into_a_count() {
    let mut presenter =
        TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
    for (id, path) in [("a", "first.rs"), ("b", "second.rs")] {
        let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":id,"title":"apply_patch","rawInput":{"patchText":format!("*** Begin Patch\n*** Update File: /private/{path}\n*** End Patch")}}));
        let text = events[0]["message"].as_str().unwrap();
        assert!(text.contains(path));
        if id == "b" {
            assert_eq!(text, "🔧 Editing first.rs\n🔧 Editing second.rs");
        }
    }
}

#[test]
fn matrix_edit_identifies_the_file_without_disclosing_arguments_or_directories() {
    let mut presenter =
        TurnPresenter::new(options("hermes", false), "matrix", json!({"mode":"off"}));
    let events = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"edit-1","title":"edit","kind":"edit","status":"in_progress","rawInput":{"filePath":"/private/project/src/config.rs","oldString":"private old content","newString":"private replacement content"}}));
    let message = events[0]["message"].as_str().unwrap();
    assert_eq!(message, "🔧 Editing config.rs");
    assert!(!message.contains("/private") && !message.contains("content"));
}
