use anyhow::{Context, Result, ensure};
use soulsteads_messaging::backend::{Client, Options, Transport};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments
        .next()
        .context("Provide the OpenCode executable")?;
    let root = PathBuf::from(
        arguments
            .next()
            .context("Provide the isolated fixture directory")?,
    );
    ensure!(root.is_absolute(), "Fixture directory must be absolute");
    ensure!(arguments.next().is_none(), "Unexpected probe argument");
    let allowed = [
        "PATH",
        "LANG",
        "TMPDIR",
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "OPENCODE_CONFIG_CONTENT",
    ];
    let env: BTreeMap<_, _> = std::env::vars()
        .filter(|(key, _)| allowed.contains(&key.as_str()))
        .collect();
    let client = Client::connect(
        Transport::OpenCodeNative,
        Options {
            command,
            args: vec![],
            cwd: root.clone(),
            env,
            inherit_env: false,
            timeout: Duration::from_secs(30),
        },
        vec![],
    )
    .await?;
    let result = async {
        let session = client.new_session().await?;
        client.set_option("model", "fixture/fake").await?;
        let first = client.clone();
        let mut pending = tokio::spawn(async move { first.prompt("ISOLATED FIXTURE 1", vec![]).await });
        tokio::time::timeout(Duration::from_secs(20), async {
            while !root.join("provider-started").exists() {
                if pending.is_finished() {
                    (&mut pending).await?.context("Native first prompt failed before provider dispatch")?;
                    anyhow::bail!("Native first prompt completed without provider dispatch");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await.context("Synthetic provider did not receive the first prompt")??;
        client.forward("ISOLATED FIXTURE 2").await?;
        ensure!(!pending.is_finished(), "Admission must not end the first collector");
        let Client::Native(native) = &client else { unreachable!() };
        let queued = native.queued_inputs().await?;
        ensure!(queued.len() == 1, "Second prompt was not observed in the busy session inbox");
        std::fs::write(root.join("release-provider"), "")?;
        let response = tokio::time::timeout(Duration::from_secs(35), pending).await???;
        ensure!(response.text == "FIXTURE_REPLY_1FIXTURE_REPLY_2", "Native collector lost or reordered replies");
        ensure!(response.output.collector_closed, "Collector did not reach terminal closure");
        ensure!(native.queued_inputs().await?.is_empty(), "Inbox is not empty after completion");
        let history = client.load_session(&session, true).await?;
        let transcript: Vec<_> = history.iter().map(|item| (item["sessionUpdate"].as_str().unwrap_or(""), item["content"]["text"].as_str().unwrap_or(""))).collect();
        ensure!(transcript == [
            ("user_message_chunk", "ISOLATED FIXTURE 1"), ("agent_message_chunk", "FIXTURE_REPLY_1"),
            ("user_message_chunk", "ISOLATED FIXTURE 2"), ("agent_message_chunk", "FIXTURE_REPLY_2"),
        ], "Projected transcript is not two ordered users and assistants");
        Ok::<_, anyhow::Error>(json!({"session":session,"two_replies_verified":true,"rust_runtime":true,"queued_before_release":queued.len(),"inbox_after":[],"output_lost":response.output.output_lost}))
    }.await;
    let shutdown = client.shutdown().await;
    let mut receipt = result?;
    shutdown.context("Owned native server lease did not exit successfully")?;
    receipt["server_exit"] = 0.into();
    println!("{receipt}");
    Ok(())
}
