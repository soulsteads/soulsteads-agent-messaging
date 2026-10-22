#![cfg(target_os = "linux")]

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Stdio},
};

fn root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("cli-images-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}

fn png() -> Vec<u8> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
}

fn text(value: &str) -> Value {
    json!({"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":value}}})
}

fn inline(alt: Option<&str>) -> Value {
    let mut content = json!({"type":"image","mimeType":"image/png","data":STANDARD.encode(png())});
    if let Some(alt) = alt {
        content["alt"] = alt.into();
    }
    json!({"update":{"sessionUpdate":"agent_message_chunk","content":content}})
}

fn setup(root: &Path, actions: Value, renderer: bool) {
    std::fs::create_dir(root.join("bin")).unwrap();
    std::fs::set_permissions(root.join("bin"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir(root.join("doclibrary_cache")).unwrap();
    std::fs::set_permissions(
        root.join("doclibrary_cache"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    if renderer {
        std::fs::write(root.join("bin/chafa"), r#"#!/usr/bin/python3
import json, os, sys
mode = os.environ.get('RENDER_MODE', '')
if mode == 'replace':
    original = os.environ['REPLACE_PATH']
    os.rename(original, original + '.old')
    open(original, 'wb').write(b'external-replacement')
elif mode == 'symlink':
    original = os.environ['REPLACE_PATH']
    os.rename(original, original + '.old')
    os.symlink(os.environ['EXTERNAL_PATH'], original)
elif mode == 'root-replace':
    original = os.environ['REPLACE_PATH']
    os.rename(original, original + '.old')
    os.mkdir(original, 0o700)
    open(original + '/report.png', 'wb').write(b'external-replacement')
elif mode == 'hang':
    import time
    child = os.fork()
    if child == 0:
        time.sleep(60)
        sys.exit(0)
    open(os.environ['CHILD_PID'], 'w').write(str(child))
    time.sleep(60)
if mode == 'oversize':
    print('X' * (128 * 1024 + 1), flush=True)
    sys.exit(0)
if mode == 'fail':
    print('SYNTHETIC-ERROR-BYTES', flush=True)
    print('SYNTHETIC-ERROR-BYTES', file=sys.stderr, flush=True)
    sys.exit(19)
if mode == 'remove':
    os.unlink(sys.argv[0])
if mode.startswith('renderer-'):
    original = sys.argv[0]
    replacement = b'#!/usr/bin/python3\nprint("REPLACED-RENDERER")\n'
    if mode == 'renderer-directory':
        directory = os.path.dirname(original)
        os.rename(directory, directory + '.old')
        os.mkdir(directory, 0o755)
    elif mode == 'renderer-replace':
        os.rename(original, original + '.old')
    elif mode == 'renderer-link':
        os.unlink(original)
        os.symlink('replacement', original)
    with open(original, 'wb') as changed:
        changed.write(replacement)
    os.chmod(original, 0o700)
import fcntl, stat
metadata = os.stat(sys.argv[-1])
write_rejected = False
try:
    with open(sys.argv[-1], 'r+b') as snapshot:
        snapshot.write(b'not-admitted')
except OSError:
    write_rejected = True
with open(os.environ['RENDER_AUDIT'], 'a') as audit:
    audit.write(json.dumps({'executable': sys.argv[0], 'args': sys.argv[1:], 'bytes': list(open(sys.argv[-1], 'rb').read()), 'mode': stat.S_IMODE(metadata.st_mode), 'write_rejected': write_rejected}) + '\n')
print('RENDERED', flush=True)
"#).unwrap();
        std::fs::set_permissions(
            root.join("bin/chafa"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
    std::fs::write(root.join("script.json"), actions.to_string()).unwrap();
    let settings = json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"timeoutMs":2000,"env":{"ACP_FIXTURE_MATRIX_SCRIPT":root.join("script.json"),"ACP_FIXTURE_MATRIX_COUNTER":root.join("counter"),"ACP_FIXTURE_MATRIX_AUDIT":root.join("acp-audit")}}});
    std::fs::write(root.join("soulsteads-messaging.json"), settings.to_string()).unwrap();
}

fn run(root: &Path, no_images: bool, input: Option<&str>) -> String {
    run_with(root, no_images, input, &[])
}

fn run_with(
    root: &Path,
    no_images: bool,
    input: Option<&str>,
    environment: &[(&str, String)],
) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"));
    command
        .current_dir(root)
        .env_clear()
        .env("HOME", root)
        .env("TMPDIR", root)
        .env("SESSION_BASE_DIR", root.join("sessions"))
        .env("PATH", root.join("bin"))
        .env("RENDER_AUDIT", root.join("render-audit"));
    command.envs(environment.iter().map(|(key, value)| (*key, value)));
    if no_images {
        command.arg("--no-images");
    }
    if input.is_none() {
        command.arg("synthetic");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(input) = input {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "CLI failed: {:?}", output.status);
    if environment
        .iter()
        .any(|(key, _)| *key == "EXPECT_ACP_ERROR")
    {
        assert_eq!(
            output.stderr,
            b"Sorry, something went wrong processing your request.\n"
        );
    } else {
        assert!(output.stderr.is_empty(), "CLI must not dump diagnostics");
    }
    String::from_utf8(output.stdout).unwrap()
}

fn audit(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("render-audit"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn prompts(root: &Path) -> usize {
    std::fs::read_to_string(root.join("acp-audit"))
        .unwrap()
        .lines()
        .filter(|line| serde_json::from_str::<Value>(line).unwrap()["method"] == "session/prompt")
        .count()
}

#[test]
fn cli_response_path_is_rendered_after_streamed_text_without_replay() {
    let root = root();
    let path = root.path().join("doclibrary_cache/report_page1.png");
    setup(
        root.path(),
        json!([{ "updates":[text(&format!("Image file: {}\n", path.display())),text("TAIL\n")], "stopReason":"end_turn" }]),
        true,
    );
    std::fs::write(&path, png()).unwrap();
    let output = run(root.path(), false, None);
    assert!(
        output.contains("RENDERED"),
        "response image must reach renderer: {output}"
    );
    assert!(output.find("TAIL").unwrap() < output.find("RENDERED").unwrap());
    assert_eq!(output.matches("TAIL").count(), 1);
    let calls = audit(root.path());
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["args"].as_array().unwrap()[..3],
        [
            json!("--size=60"),
            json!("--colors=256"),
            json!("--symbols=block")
        ]
    );
    assert_eq!(calls[0]["bytes"], json!(png()));
    assert_eq!(prompts(root.path()), 1);
    assert!(root.path().join("sessions/web/cli").is_dir());
    assert!(!root.path().join("sessions/cli/cli").exists());
    let store: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("store.json")).unwrap()).unwrap();
    assert_eq!(store["version"], 2);
    assert_eq!(store["sessions"][0]["connector"], "web");
    assert_eq!(store["sessions"][0]["threadId"], "cli");
}

#[test]
fn cli_no_images_preserves_stream_and_inline_notices_without_rendering() {
    let root = root();
    let path = root.path().join("doclibrary_cache/report.png");
    setup(
        root.path(),
        json!([{ "updates":[text("HEAD\n"),inline(Some("synthetic alt")),text(&format!("Image file: {}\nTAIL\n", path.display()))], "stopReason":"end_turn" }]),
        true,
    );
    std::fs::write(path, png()).unwrap();
    let output = run(root.path(), true, None);
    assert!(
        output.contains("[Image received: synthetic alt]"),
        "inline notice must survive --no-images: {output}"
    );
    assert!(output.contains("Image file:"));
    assert!(output.find("HEAD").unwrap() < output.find("Image received").unwrap());
    assert!(output.find("Image received").unwrap() < output.find("TAIL").unwrap());
    assert!(!output.contains("RENDERED"));
    assert!(audit(root.path()).is_empty());
    assert_eq!(prompts(root.path()), 1);
}

#[test]
fn cli_inline_bytes_are_not_rendered() {
    let root = root();
    setup(
        root.path(),
        json!([{ "updates":[inline(None)], "stopReason":"end_turn" }]),
        true,
    );
    let output = run(root.path(), false, None);
    assert!(
        output.contains("[Image received: image/png]"),
        "MIME fallback notice missing: {output}"
    );
    assert!(audit(root.path()).is_empty());
    assert_eq!(prompts(root.path()), 1);
}

#[test]
fn cli_interactive_three_forms_two_variants_deduplicate_actual_paths_and_reset() {
    let root = root();
    let cache = root.path().join("doclibrary_cache");
    let first = cache.join("book_page1.png");
    let second = cache.join("other_page_2.png");
    let response = format!(
        "Image file: {}/book_page_1.png\n`{}`\n{}/other_page2.png\nFIRST-TAIL\n",
        cache.display(),
        first.display(),
        cache.display()
    );
    setup(
        root.path(),
        json!([
            {"updates":[text(&response[..12]),text(&response[12..])],"stopReason":"end_turn"},
            {"updates":[text(&format!("Image file: {}\nSECOND-TAIL\n", first.display()))],"stopReason":"end_turn"},
            {"updates":[text("PLAIN-THIRD\n")],"stopReason":"end_turn"}
        ]),
        true,
    );
    std::fs::write(first, png()).unwrap();
    std::fs::write(second, png()).unwrap();
    let output = run(root.path(), false, Some("first\nsecond\nthird\n/quit\n"));
    assert_eq!(
        audit(root.path()).len(),
        3,
        "both forms dedup per actual path, reset per prompt: {output}"
    );
    assert_eq!(output.matches("RENDERED").count(), 3);
    assert!(output.find("FIRST-TAIL").unwrap() < output.find("RENDERED").unwrap());
    assert!(output.find("SECOND-TAIL").unwrap() < output.rfind("RENDERED").unwrap());
    assert!(output.rfind("RENDERED").unwrap() < output.find("PLAIN-THIRD").unwrap());
    assert_eq!(output.matches("FIRST-TAIL").count(), 1);
    assert_eq!(prompts(root.path()), 3);
    assert!(root.path().join("sessions/cli/cli").is_dir());
    assert!(!root.path().join("sessions/web/cli").exists());
}

#[test]
fn cli_missing_path_and_absent_failing_removed_renderers_fall_back_without_replay() {
    for mode in ["absent", "fail", "remove", "oversize"] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        let response = format!(
            "Image file: {}\nImage file: {}/missing.png\nEND\n",
            path.display(),
            path.parent().unwrap().display()
        );
        setup(
            root.path(),
            json!([
                {"updates":[text(&response),inline(Some("inline"))],"stopReason":"end_turn"},
                {"updates":[text(&response)],"stopReason":"end_turn"}
            ]),
            mode != "absent",
        );
        std::fs::write(&path, png()).unwrap();
        let output = run_with(
            root.path(),
            false,
            Some("first\nsecond\n/quit\n"),
            &[("RENDER_MODE", mode.into())],
        );
        let fallbacks = if mode == "remove" { 1 } else { 2 };
        assert_eq!(
            output
                .matches(&format!("[Image: {}]", path.display()))
                .count(),
            fallbacks,
            "{mode}: {output}"
        );
        assert!(!output.contains("[Image not found:"));
        assert!(!output.contains("SYNTHETIC-ERROR-BYTES"));
        assert!(!output.contains(&"X".repeat(100)));
        assert!(output.contains("[Image received: inline]"));
        assert_eq!(prompts(root.path()), 2);
    }
}

#[test]
fn cli_session_relative_png_and_quoted_filename_use_exact_safe_argv() {
    let root = root();
    let filename = "report ';$(not-a-command) space.PNG";
    setup(
        root.path(),
        json!([{ "updates":[text(&format!("Image file: {filename}\n"))], "stopReason":"end_turn" }]),
        true,
    );
    let session = root.path().join("sessions/web/cli");
    std::fs::create_dir_all(&session).unwrap();
    std::fs::write(session.join(filename), png()).unwrap();
    let output = run(root.path(), false, None);
    assert_eq!(output.matches("RENDERED").count(), 1);
    let calls = audit(root.path());
    assert_eq!(calls[0]["args"].as_array().unwrap().len(), 4);
    assert!(
        calls[0]["args"][3]
            .as_str()
            .unwrap()
            .starts_with("/proc/self/fd/")
    );
    assert_eq!(calls[0]["bytes"], json!(png()));
    assert_eq!(prompts(root.path()), 1);
}

#[test]
fn cli_owned_snapshot_is_private_sealed_and_survives_post_admission_path_replacement() {
    for mode in ["replace", "symlink", "root-replace"] {
        let root = root();
        let cache = root.path().join("doclibrary_cache");
        let path = cache.join("report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(&path, png()).unwrap();
        let external = root.path().join("external.png");
        std::fs::write(&external, b"external-sentinel").unwrap();
        let target = if mode == "root-replace" {
            &cache
        } else {
            &path
        };
        let output = run_with(
            root.path(),
            false,
            None,
            &[
                ("RENDER_MODE", mode.into()),
                ("REPLACE_PATH", target.to_string_lossy().into_owned()),
                ("EXTERNAL_PATH", external.to_string_lossy().into_owned()),
            ],
        );
        assert_eq!(output.matches("RENDERED").count(), 1);
        let calls = audit(root.path());
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["bytes"], json!(png()));
        assert_eq!(calls[0]["mode"], 0o600);
        assert_eq!(calls[0]["write_rejected"], true);
        assert_eq!(std::fs::read(external).unwrap(), b"external-sentinel");
        assert_eq!(prompts(root.path()), 1);
    }
}

#[test]
fn cli_unsupported_external_symlink_hardlink_nonregular_and_size_paths_are_not_rendered() {
    use std::os::unix::fs::symlink;
    for kind in [
        "external",
        "leaf-link",
        "parent-link",
        "root-link",
        "hardlink",
        "directory",
        "fifo",
        "oversize",
        "invalid-png",
        "traversal",
        "unsafe-cache",
    ] {
        let root = root();
        let cache = root.path().join("doclibrary_cache");
        let path = cache.join("report.png");
        setup(
            root.path(),
            json!([{ "updates":[inline(Some("safe notice"))], "stopReason":"end_turn" }]),
            true,
        );
        let external = root.path().join("external");
        std::fs::create_dir(&external).unwrap();
        std::fs::write(external.join("report.png"), png()).unwrap();
        let mentioned = match kind {
            "external" => external.join("report.png"),
            "leaf-link" => {
                symlink(external.join("report.png"), &path).unwrap();
                path.clone()
            }
            "parent-link" => {
                symlink(&external, cache.join("link")).unwrap();
                cache.join("link/report.png")
            }
            "root-link" => {
                std::fs::remove_dir(&cache).unwrap();
                symlink(&external, &cache).unwrap();
                path.clone()
            }
            "hardlink" => {
                std::fs::hard_link(external.join("report.png"), &path).unwrap();
                path.clone()
            }
            "directory" => {
                std::fs::create_dir(&path).unwrap();
                path.clone()
            }
            "fifo" => {
                let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                path.clone()
            }
            "oversize" => {
                std::fs::File::create(&path)
                    .unwrap()
                    .set_len(20 * 1024 * 1024 + 1)
                    .unwrap();
                path.clone()
            }
            "invalid-png" => {
                std::fs::write(&path, b"not-png").unwrap();
                path.clone()
            }
            "traversal" => cache.join("../external/report.png"),
            "unsafe-cache" => {
                std::fs::write(&path, png()).unwrap();
                std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o777)).unwrap();
                path.clone()
            }
            _ => unreachable!(),
        };
        let actions = json!([{ "updates":[text(&format!("Image file: {}\nTAIL\n", mentioned.display())),inline(Some("safe notice"))], "stopReason":"end_turn" }]);
        std::fs::write(root.path().join("script.json"), actions.to_string()).unwrap();
        let output = run(root.path(), false, None);
        assert!(
            audit(root.path()).is_empty(),
            "rejected {kind} must not hand off bytes"
        );
        assert!(!output.contains("[Image:"));
        assert!(output.contains("[Image received: safe notice]"));
        assert_eq!(prompts(root.path()), 1);
        assert_eq!(std::fs::read(external.join("report.png")).unwrap(), png());
    }
}

#[test]
fn cli_renderer_timeout_kills_owned_group_and_next_prompt_runs_without_replay() {
    let root = root();
    let path = root.path().join("doclibrary_cache/report.png");
    setup(
        root.path(),
        json!([
            { "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" },
            { "updates":[text("AFTER-TIMEOUT\n")], "stopReason":"end_turn" }
        ]),
        true,
    );
    std::fs::write(&path, png()).unwrap();
    let pid_file = root.path().join("child-pid");
    let started = std::time::Instant::now();
    let output = run_with(
        root.path(),
        false,
        Some("first\nsecond\n/quit\n"),
        &[
            ("RENDER_MODE", "hang".into()),
            ("CHILD_PID", pid_file.to_string_lossy().into_owned()),
        ],
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(8));
    assert!(output.contains(&format!("[Image: {}]", path.display())));
    assert!(output.contains("AFTER-TIMEOUT"));
    assert_eq!(prompts(root.path()), 2);
    let pid: u32 = std::fs::read_to_string(pid_file).unwrap().parse().unwrap();
    for _ in 0..100 {
        let state = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        if state.as_ref().is_err() || state.unwrap().split_whitespace().nth(2) == Some("Z") {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("owned renderer descendant still running");
}

#[test]
fn cli_backtick_path_with_spaces_is_rendered_without_label_or_bare_match() {
    let root = root();
    let path = root.path().join("doclibrary_cache/report with spaces.png");
    setup(
        root.path(),
        json!([{ "updates":[text(&format!("`{}`\n", path.display()))], "stopReason":"end_turn" }]),
        true,
    );
    std::fs::write(path, png()).unwrap();
    assert_eq!(run(root.path(), false, None).matches("RENDERED").count(), 1);
    assert_eq!(audit(root.path()).len(), 1);
}

#[test]
fn cli_no_images_interactive_inline_repeats_and_buffer_resets_after_acp_failure() {
    let root = root();
    let path = root.path().join("doclibrary_cache/report.png");
    setup(
        root.path(),
        json!([
            { "updates":[text(&format!("Image file: {}\nFIRST\n", path.display())),inline(Some("repeated")),inline(Some("repeated"))], "error":true },
            { "updates":[inline(None),text("SECOND\n")], "stopReason":"end_turn" },
            { "updates":[text("THIRD\n")], "stopReason":"end_turn" }
        ]),
        true,
    );
    std::fs::write(path, png()).unwrap();
    let output = run_with(
        root.path(),
        true,
        Some("first\nsecond\nthird\n/quit\n"),
        &[("EXPECT_ACP_ERROR", "1".into())],
    );
    assert_eq!(output.matches("[Image received: repeated]").count(), 2);
    assert_eq!(output.matches("[Image received: image/png]").count(), 1);
    assert!(audit(root.path()).is_empty());
    assert!(!output.contains("[Image:"));
    assert_eq!(output.matches("FIRST").count(), 1);
    assert_eq!(output.matches("SECOND").count(), 1);
    assert_eq!(output.matches("THIRD").count(), 1);
    assert_eq!(prompts(root.path()), 3);
}

#[test]
fn cli_single_prompt_reopens_existing_web_identity_without_migrating_store_or_cwd() {
    use soulsteads_messaging::store::{Entry, Store};
    let root = root();
    setup(
        root.path(),
        json!([{ "updates":[inline(Some("restored"))], "stopReason":"end_turn" }]),
        false,
    );
    let session = root.path().join("sessions/web/cli");
    std::fs::create_dir_all(&session).unwrap();
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("soulsteads-messaging.json")).unwrap())
            .unwrap();
    config["acp"]["backendId"] = "synthetic-cli-backend".into();
    std::fs::write(root.path().join("soulsteads-messaging.json"), config.to_string()).unwrap();
    let store = Store::new(root.path().join("store.json"));
    let entry = Entry {
        connector: "web".into(),
        thread_id: "cli".into(),
        session_id: "fixture".into(),
        cwd: session.to_string_lossy().into_owned(),
        backend_id: "synthetic-cli-backend".into(),
        updated_at: "2026-10-05T00:00:00Z".into(),
        backend: "opencode".into(),
    };
    store.set(entry.clone(), true).unwrap();
    let before = std::fs::read(root.path().join("store.json")).unwrap();
    assert!(run(root.path(), false, None).contains("[Image received: restored]"));
    let after = store.get("web", "cli", "opencode").unwrap().unwrap();
    assert_eq!(after.connector, entry.connector);
    assert_eq!(after.thread_id, entry.thread_id);
    assert_eq!(after.cwd, entry.cwd);
    assert_eq!(after.session_id, entry.session_id);
    assert!(store.get("cli", "cli", "opencode").unwrap().is_none());
    let audit = std::fs::read_to_string(root.path().join("acp-audit")).unwrap();
    assert!(audit.contains("session/load"));
    assert!(!audit.contains("session/new"));
    let previous: Value = serde_json::from_slice(&before).unwrap();
    let current: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("store.json")).unwrap()).unwrap();
    assert_eq!(previous["version"], current["version"]);
    assert_eq!(
        previous["sessions"].as_array().unwrap().len(),
        current["sessions"].as_array().unwrap().len()
    );
}

#[test]
fn cli_response_image_attempts_are_bounded() {
    let root = root();
    let cache = root.path().join("doclibrary_cache");
    let response = (0..18)
        .map(|index| format!("Image file: {}/report{index}.png\n", cache.display()))
        .collect::<String>();
    setup(
        root.path(),
        json!([{ "updates":[text(&response)], "stopReason":"end_turn" }]),
        true,
    );
    for index in 0..18 {
        std::fs::write(cache.join(format!("report{index}.png")), png()).unwrap();
    }
    let output = run(root.path(), false, None);
    assert_eq!(output.matches("RENDERED").count(), 16);
    assert_eq!(audit(root.path()).len(), 16);
    assert_eq!(prompts(root.path()), 1);
}

#[test]
fn cli_renderer_discovery_rejects_unsafe_writability() {
    for (target, mode) in [
        ("bin/chafa", 0o770),
        ("bin/chafa", 0o707),
        ("bin", 0o770),
        ("bin", 0o707),
    ] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\nEND\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(&path, png()).unwrap();
        std::fs::set_permissions(
            root.path().join(target),
            std::fs::Permissions::from_mode(mode),
        )
        .unwrap();
        let output = run(root.path(), false, None);
        assert!(
            output.contains(&format!("[Image: {}]", path.display())),
            "unsafe {target} {mode:o} executed: {output}"
        );
        assert!(audit(root.path()).is_empty());
        assert_eq!(output.matches("END").count(), 1);
        assert_eq!(prompts(root.path()), 1);
    }
}

#[test]
fn cli_renderer_discovery_rejects_observed_replacement_without_rediscovery() {
    for mode in [
        "renderer-replace",
        "renderer-rewrite",
        "renderer-directory",
        "renderer-link",
    ] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        let response = format!("Image file: {}\nEND\n", path.display());
        setup(
            root.path(),
            json!([
                { "updates":[text(&response)], "stopReason":"end_turn" },
                { "updates":[text(&response)], "stopReason":"end_turn" }
            ]),
            true,
        );
        std::fs::write(&path, png()).unwrap();
        let output = run_with(
            root.path(),
            false,
            Some("first\nsecond\n/quit\n"),
            &[("RENDER_MODE", mode.into())],
        );
        assert!(!output.contains("REPLACED-RENDERER"), "{mode}: {output}");
        assert_eq!(output.matches("RENDERED").count(), 1, "{mode}: {output}");
        assert_eq!(
            output
                .matches(&format!("[Image: {}]", path.display()))
                .count(),
            1,
            "{mode}: {output}"
        );
        assert_eq!(output.lines().filter(|line| *line == "END").count(), 2);
        assert_eq!(audit(root.path()).len(), 1);
        assert_eq!(prompts(root.path()), 2);
    }
}

#[test]
fn cli_renderer_discovery_bounds_path_bytes_and_entry_count() {
    for boundary in ["entries", "bytes"] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(&path, png()).unwrap();
        let search = if boundary == "entries" {
            format!(
                "{}{}",
                "missing:".repeat(64),
                root.path().join("bin").display()
            )
        } else {
            format!(
                "{}:{}",
                root.path().join("bin").display(),
                "x".repeat(16 * 1024)
            )
        };
        let output = run_with(root.path(), false, None, &[("PATH", search)]);
        assert!(
            output.contains(&format!("[Image: {}]", path.display())),
            "{boundary}: {output}"
        );
        assert!(audit(root.path()).is_empty());
        assert_eq!(prompts(root.path()), 1);
    }
}

#[test]
fn cli_renderer_discovery_keeps_process_cwd_relative_and_empty_path_semantics() {
    for search in ["bin", "./bin", "bin/../bin", "", ":missing"] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(path, png()).unwrap();
        if search.is_empty() || search.starts_with(':') {
            std::fs::rename(root.path().join("bin/chafa"), root.path().join("chafa")).unwrap();
        }
        let output = run_with(root.path(), false, None, &[("PATH", search.into())]);
        assert_eq!(output.matches("RENDERED").count(), 1, "{search}: {output}");
        assert_eq!(audit(root.path()).len(), 1);
    }
}

#[test]
fn cli_renderer_discovery_allows_safe_system_style_directory_and_leaf_symlinks() {
    use std::os::unix::fs::symlink;
    for leaf_link in [false, true] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(path, png()).unwrap();
        std::fs::create_dir(root.path().join("usr")).unwrap();
        std::fs::set_permissions(
            root.path().join("usr"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        std::fs::rename(root.path().join("bin"), root.path().join("usr/bin")).unwrap();
        symlink("usr/bin", root.path().join("bin")).unwrap();
        if leaf_link {
            std::fs::rename(
                root.path().join("usr/bin/chafa"),
                root.path().join("usr/bin/renderer"),
            )
            .unwrap();
            symlink("renderer", root.path().join("usr/bin/chafa")).unwrap();
        }
        let output = run(root.path(), false, None);
        assert_eq!(output.matches("RENDERED").count(), 1, "{output}");
        assert_eq!(audit(root.path()).len(), 1);
    }
}

#[test]
fn cli_renderer_discovery_skips_unsafe_alias_ancestors_and_nonexecutables() {
    use std::os::unix::fs::symlink;
    for unsafe_alias in [false, true] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(path, png()).unwrap();
        std::fs::create_dir(root.path().join("unsafe")).unwrap();
        let search = if unsafe_alias {
            std::fs::set_permissions(
                root.path().join("unsafe"),
                std::fs::Permissions::from_mode(0o777),
            )
            .unwrap();
            symlink("../bin", root.path().join("unsafe/alias")).unwrap();
            "unsafe/alias:bin".to_owned()
        } else {
            std::fs::set_permissions(
                root.path().join("unsafe"),
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap();
            std::fs::copy(
                root.path().join("bin/chafa"),
                root.path().join("unsafe/chafa"),
            )
            .unwrap();
            std::fs::set_permissions(
                root.path().join("unsafe/chafa"),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            "unsafe:bin".to_owned()
        };
        let output = run_with(root.path(), false, None, &[("PATH", search)]);
        assert_eq!(output.matches("RENDERED").count(), 1, "{output}");
        let record = audit(root.path());
        assert_eq!(record.len(), 1);
        assert_eq!(
            record[0]["executable"],
            root.path().join("bin/chafa").to_string_lossy().as_ref()
        );
    }
}

#[test]
fn cli_renderer_discovery_accepts_exact_path_limits() {
    for boundary in ["entries", "bytes"] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(path, png()).unwrap();
        let search = if boundary == "entries" {
            format!(
                "{}{}",
                "missing:".repeat(63),
                root.path().join("bin").display()
            )
        } else {
            let prefix = format!("{}:", root.path().join("bin").display());
            format!("{prefix}{}", "x".repeat(16 * 1024 - prefix.len()))
        };
        let output = run_with(root.path(), false, None, &[("PATH", search)]);
        assert_eq!(
            output.matches("RENDERED").count(),
            1,
            "{boundary}: {output}"
        );
        assert_eq!(audit(root.path()).len(), 1);
    }
}

#[test]
fn cli_renderer_discovery_bounds_literal_and_symlink_resolved_depth() {
    use std::os::unix::fs::symlink;
    for (depth, alias) in [(64, false), (65, false), (65, true)] {
        let root = root();
        let path = root.path().join("doclibrary_cache/report.png");
        setup(
            root.path(),
            json!([{ "updates":[text(&format!("Image file: {}\n", path.display()))], "stopReason":"end_turn" }]),
            true,
        );
        std::fs::write(&path, png()).unwrap();
        let mut directory = root.path().to_owned();
        while directory.components().count() + 1 < depth {
            directory.push("child");
            std::fs::create_dir(&directory).unwrap();
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::fs::rename(root.path().join("bin/chafa"), directory.join("chafa")).unwrap();
        let search = if alias {
            symlink(&directory, root.path().join("alias")).unwrap();
            root.path().join("alias")
        } else {
            directory
        };
        let output = run_with(
            root.path(),
            false,
            None,
            &[("PATH", search.to_string_lossy().into_owned())],
        );
        if depth == 64 {
            assert_eq!(output.matches("RENDERED").count(), 1, "{output}");
            assert_eq!(audit(root.path()).len(), 1);
        } else {
            assert!(
                output.contains(&format!("[Image: {}]", path.display())),
                "{output}"
            );
            assert!(audit(root.path()).is_empty());
        }
    }
}
