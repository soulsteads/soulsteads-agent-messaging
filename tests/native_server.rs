use soulsteads_messaging::acp::Options;
use soulsteads_messaging::native_server;
use std::{collections::BTreeMap, time::Duration};
fn options(script: &str) -> Options {
    Options {
        command: "python3".into(),
        args: vec!["-u".into(), "-c".into(), script.into()],
        cwd: std::env::temp_dir(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout: Duration::from_millis(500),
    }
}
#[tokio::test]
async fn native_server_refuses_legacy_acp_argument_before_launch() {
    let mut options = options(
        "import sys,json; print(json.dumps({'url':'http://127.0.0.1:4096'}),flush=True); sys.stdin.read()",
    );
    options.args.push("acp".into());
    if let Ok(server) = native_server::NativeServer::start(options).await {
        server.shutdown().await.unwrap();
        panic!("Legacy ACP argument must not reach native serve");
    }
}
#[cfg(unix)]
#[tokio::test]
async fn native_server_owns_a_distinct_process_group() {
    let server = native_server::NativeServer::start(options("import os,sys,json; assert os.getpgrp()==os.getpid(); print(json.dumps({'url':'http://127.0.0.1:4096'}),flush=True); sys.stdin.read()" )).await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn lease_closure_exits_owned_child() {
    let server = native_server::NativeServer::start(options("import os,sys,json; assert len(os.environ['OPENCODE_PASSWORD']) >= 32; assert '--stdio' in sys.argv; print(json.dumps({'url':'http://127.0.0.1:4096'}),flush=True); sys.stdin.read()" )).await.unwrap();
    let _client = server.client();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn public_readiness_endpoint_is_rejected() {
    assert!(
        native_server::NativeServer::start(options(
            "import json; print(json.dumps({'url':'http://192.0.2.1:4096'}),flush=True); input()"
        ))
        .await
        .is_err()
    );
}
#[tokio::test]
async fn oversized_readiness_is_rejected() {
    assert!(
        native_server::NativeServer::start(options("print('x'*20000,flush=True); input()"))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn readiness_timeout_is_bounded() {
    assert!(
        native_server::NativeServer::start(options("import time; time.sleep(10)"))
            .await
            .is_err()
    );
}
