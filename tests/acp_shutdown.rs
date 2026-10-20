use soulsteads_messaging::acp::{Client, Options};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

fn options(directory: &std::path::Path) -> Options {
    Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec![],
        cwd: directory.to_owned(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    }
}

#[tokio::test]
async fn shutdown_closes_stdin_and_allows_a_cooperative_peer_to_exit_promptly() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();

    let started = Instant::now();
    client.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "cooperative ACP peer waited for the forced-shutdown deadline"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_allows_a_cooperative_parent_to_reap_its_owned_native_child() {
    let directory = tempfile::tempdir().unwrap();
    let child_pid_path = directory.path().join("child.pid");
    let reap_audit = directory.path().join("child.reaped");
    let mut config = options(directory.path());
    config.env.insert(
        "ACP_FIXTURE_CHILD_PID".into(),
        child_pid_path.display().to_string(),
    );
    config.env.insert(
        "ACP_FIXTURE_COOPERATIVE_CHILD_REAP_AUDIT".into(),
        reap_audit.display().to_string(),
    );
    let client = Client::connect(config).await.unwrap();
    let child: libc::pid_t = std::fs::read_to_string(child_pid_path)
        .unwrap()
        .parse()
        .unwrap();

    client.shutdown().await.unwrap();
    assert_eq!(std::fs::read_to_string(reap_audit).unwrap(), "reaped");
    assert_eq!(
        unsafe { libc::kill(child, 0) },
        -1,
        "cooperative parent left its native child unreaped"
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[tokio::test]
async fn simultaneous_shutdown_calls_share_one_owned_process_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    let first = {
        let client = client.clone();
        tokio::spawn(async move { client.shutdown().await })
    };
    assert!(client.shutdown().await.is_ok());
    assert!(first.await.unwrap().is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_forces_an_uncooperative_owned_process_group_after_a_grace_period() {
    let directory = tempfile::tempdir().unwrap();
    let child_pid_path = directory.path().join("child.pid");
    let mut config = options(directory.path());
    config
        .env
        .insert("ACP_FIXTURE_IGNORE_STDIN_EOF".into(), "1".into());
    config.env.insert(
        "ACP_FIXTURE_CHILD_PID".into(),
        child_pid_path.display().to_string(),
    );
    let client = Client::connect(config).await.unwrap();
    let child: libc::pid_t = std::fs::read_to_string(child_pid_path)
        .unwrap()
        .parse()
        .unwrap();

    let started = Instant::now();
    client.shutdown().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "uncooperative owned process bypassed the bounded graceful shutdown period"
    );
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "forced shutdown of an uncooperative owned process was not bounded"
    );
    let reaped = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if unsafe { libc::kill(child, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(reaped.is_ok(), "forced shutdown left an owned child behind");
}

#[cfg(unix)]
#[tokio::test]
async fn fatal_frame_cleanup_kills_owned_group_while_closed_client_is_still_held() {
    let directory = tempfile::tempdir().unwrap();
    let pid_path = directory.path().join("child.pid");
    let mut config = options(directory.path());
    config.env.insert(
        "ACP_FIXTURE_CHILD_PID".into(),
        pid_path.display().to_string(),
    );
    let client = Client::connect(config).await.unwrap();
    let pid: libc::pid_t = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.request("fixture/oversized-frame", serde_json::json!({})),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let cleaned = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if unsafe { libc::kill(pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    // Always clean up the owned fixture before reporting a RED failure.
    client.shutdown().await.unwrap();
    assert!(
        cleaned,
        "fatal transport closure left its owned process group alive"
    );
}
