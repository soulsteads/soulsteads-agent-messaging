#![cfg(unix)]

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::fs::symlink,
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

struct Console(Child);

impl Drop for Console {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(root: &Path) -> (Console, String, String) {
    let mut child = Console(
        Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(root)
            .env_clear()
            .env("HOME", root)
            .env("WEB_PORT", "4444")
            .args(["--settings", "serve", "--config", "settings.json"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(BufReader::new(stdout).lines().next());
    });
    let url = rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .expect("settings serve must print a bootstrap URL")
        .unwrap();
    let (base, token) = url.split_once('#').unwrap();
    assert!(base.starts_with("http://127.0.0.1:"));
    assert!(token.len() >= 32);
    (
        child,
        base.trim_end_matches('/').to_owned(),
        token.to_owned(),
    )
}

#[tokio::test]
async fn settings_console_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("settings.json");
    let initial = json!({
        "web":{"enabled":true}, "botName":"{env:LABEL}",
        "telegram":{"token":"synthetic_private"},
        "extension":{"x":"synthetic_private"}
    });
    fs::write(&path, initial.to_string()).unwrap();
    let (child, base, token) = start(root.path());
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let shell = client.get(&base).send().await.unwrap();
    assert_eq!(shell.status(), StatusCode::OK);
    assert!(
        shell.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'")
    );
    assert!(!shell.text().await.unwrap().contains(&token));
    let url = format!("{base}/api/settings");
    let get = || client.get(&url).bearer_auth(&token);
    let post = |body: Value| {
        client
            .post(&url)
            .bearer_auth(&token)
            .header("origin", &base)
            .json(&body)
    };
    for request in [
        client.get(&url),
        client.post(&url).header("origin", &base),
        client.get(&url).bearer_auth("wrong"),
    ] {
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for request in [
        get().header("host", "localhost"),
        get().header("origin", "http://example.invalid"),
        get().header("forwarded", "host=localhost"),
    ] {
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    let response = get().send().await.unwrap().text().await.unwrap();
    for secret in ["synthetic_private", "{env:LABEL}", &token] {
        assert!(!response.contains(secret));
    }
    let snapshot: Value = serde_json::from_str(&response).unwrap();
    assert!(response.contains("env_override"));
    let revision = &snapshot["revision"];
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let write = json!({"revision":revision,"op":"set","path":"/rateLimitSeconds","value":7});
    assert_eq!(
        post(write.clone()).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        post(write).send().await.unwrap().status(),
        StatusCode::CONFLICT
    );
    let saved = fs::read(&path).unwrap();
    let mut expected = initial;
    expected["rateLimitSeconds"] = 7.into();
    assert_eq!(serde_json::from_slice::<Value>(&saved).unwrap(), expected);
    let snapshot: Value = get().send().await.unwrap().json().await.unwrap();
    let invalid =
        json!({"revision":snapshot["revision"],"op":"set","path":"/telegram","value":false});
    assert_eq!(
        post(invalid).send().await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(fs::read(&path).unwrap(), saved);
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);

    drop(child);
    let (_child, base, token) = start(root.path());
    let response = client
        .get(format!("{base}/api/settings"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("\"value\":7"));
}

#[tokio::test]
async fn symlink_config_is_denied() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target.json");
    fs::write(&target, "{}").unwrap();
    symlink(&target, root.path().join("settings.json")).unwrap();
    let (_child, base, token) = start(root.path());
    let response = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(format!("{base}/api/settings"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(fs::read_to_string(target).unwrap(), "{}");
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
}

#[tokio::test]
async fn concurrent_creation_and_write_only_reset() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("settings.json");
    let (_first, first, first_token) = start(root.path());
    let (_second, second, second_token) = start(root.path());
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let api = |base: &str| format!("{base}/api/settings");
    let get = |base: &str, token: &str| client.get(api(base)).bearer_auth(token);
    let post = |base: &str, token: &str, body: Value| {
        client
            .post(api(base))
            .bearer_auth(token)
            .header("origin", base)
            .json(&body)
    };
    let first_view: Value = get(&first, &first_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let second_view: Value = get(&second, &second_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!path.exists());
    let change = |view: &Value| {
        json!({
            "revision": view["revision"], "op":"set_json", "path":"/extension/a~1b~0c",
            "json":r#"{"keep":"synthetic_hidden","n":18446744073709551615,"fraction":0.12345678901234567890123456789}"#
        })
    };
    let (a, b) = tokio::join!(
        post(&first, &first_token, change(&first_view)).send(),
        post(&second, &second_token, change(&second_view)).send(),
    );
    let mut results = [a.unwrap().status(), b.unwrap().status()];
    results.sort();
    assert_eq!(results, [StatusCode::OK, StatusCode::CONFLICT]);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let response = get(&first, &first_token)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!response.contains("synthetic_hidden"));
    let current: Value = serde_json::from_str(&response).unwrap();
    let reset = |path| json!({"revision":current["revision"],"op":"reset","path":path});
    for pointer in [
        "",
        "/missing",
        "/extension/a~2b",
        "/extension/a~1b~0c/n/child",
    ] {
        assert_eq!(
            post(&first, &first_token, reset(pointer))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    let duplicate = json!({
        "revision":current["revision"],"op":"set_json","path":"/extension",
        "json":r#"{"a":1,"\u0061":2}"#
    });
    assert_eq!(
        post(&first, &first_token, duplicate)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        post(&first, &first_token, reset("/extension/a~1b~0c/keep"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let saved = fs::read_to_string(&path).unwrap();
    assert!(saved.contains("0.12345678901234567890123456789"));
    let document: Value = serde_json::from_str(&saved).unwrap();
    assert_eq!(
        document.pointer("/extension/a~1b~0c/n").unwrap().as_u64(),
        Some(u64::MAX)
    );
    assert!(document.pointer("/extension/a~1b~0c/keep").is_none());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn edited_file_port_is_validated_under_runtime_override() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("settings.json");
    let initial = r#"{"web":{"port":"invalid"}}"#;
    fs::write(&path, initial).unwrap();
    let (_child, base, token) = start(root.path());
    let url = format!("{base}/api/settings");
    let client = Client::builder().no_proxy().build().unwrap();
    let edit = |pointer, value: Value| {
        let request = client.get(&url).bearer_auth(&token);
        let post = client
            .post(&url)
            .bearer_auth(&token)
            .header("origin", &base);
        async move {
            let snapshot: Value = request.send().await.unwrap().json().await.unwrap();
            post.json(&json!({
                "revision": snapshot["revision"], "op": "set", "path": pointer, "value": value
            }))
            .send()
            .await
            .unwrap()
        }
    };

    for (pointer, value) in [
        ("/web/port", json!("5555")),
        ("/web/port", json!("invalid{env:WEB_PORT}")),
        ("/web/port", json!("{env:WEB_PORT}")),
        ("/web", json!({"port":"{env:WEB_PORT}"})),
        ("/web", json!({"port":"invalid", "enabled":false})),
        ("/web/port", json!(65536)),
        ("/web/port", json!(null)),
    ] {
        let response = edit(pointer, value).await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{pointer}"
        );
        assert_eq!(response.json::<Value>().await.unwrap()["error"], "web_port");
        assert_eq!(fs::read_to_string(&path).unwrap(), initial);
    }

    for (pointer, value) in [
        ("/rateLimitSeconds", json!(7)),
        ("/web/enabled", json!(false)),
        ("/web", json!({"port":5555})),
        ("/web/port", json!(0)),
        ("/web/port", json!(65535)),
    ] {
        assert_eq!(
            edit(pointer, value).await.status(),
            StatusCode::OK,
            "{pointer}"
        );
    }
}

#[tokio::test]
async fn legacy_stream_tools_lists_and_resets_the_file_pointer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("settings.json");
    fs::write(
        &path,
        r#"{"streamTools":["shell"],"toolMessages":{"mode":"off"}}"#,
    )
    .unwrap();
    let cli = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .env_clear()
        .args([
            "--settings",
            "get",
            "/toolMessages/showOutputFor",
            "--config",
            "settings.json",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(cli.status.success());
    let entry: Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(entry["source"], "configured");
    let (_child, base, token) = start(root.path());
    let url = format!("{base}/api/settings");
    let client = Client::builder().no_proxy().build().unwrap();
    let snapshot: Value = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = snapshot["settings"].as_array().unwrap();
    assert!(
        !entries
            .iter()
            .any(|entry| entry["path"] == "/toolMessages/showOutputFor")
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry["path"] == "/streamTools" && entry["source"] == "configured")
    );

    let response = client
        .post(&url)
        .bearer_auth(&token)
        .header("origin", &base)
        .json(&json!({"revision":snapshot["revision"],"op":"reset","path":"/streamTools"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let saved: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(saved, json!({"toolMessages":{"mode":"off"}}));
}
