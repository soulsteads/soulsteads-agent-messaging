//! Opt-in real Copilot inference through the compiled messaging CLI and native ACP.
//! Supply COPILOT_GITHUB_TOKEN or explicitly opt into an authenticated native home
//! with COPILOT_LIVE_HOME. Tokens are never written to the bridge config. Only
//! synthetic conversation data is sent; a supplied home retains the test sessions.
#[path = "support/copilot_environment.rs"]
mod copilot_environment;
use anyhow::{Context, Result, ensure};
use serde_json::json;
use soulsteads_messaging::store::Store;
use std::{path::Path, time::Duration};

async fn prompt(root: &Path, text: &str) -> Result<String> {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"));
    command
        .current_dir(root)
        .env("SESSION_BASE_DIR", root.join("sessions"))
        .args(["--no-images", "--config"])
        .arg(root.join("config.json"))
        .arg(text)
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(180), command.output())
        .await
        .context("Live Copilot CLI timed out")??;
    ensure!(
        output.status.success(),
        "Live messaging CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[tokio::test]
#[ignore = "real authenticated Copilot inference; requires COPILOT_GITHUB_TOKEN or COPILOT_LIVE_HOME"]
async fn live_copilot_cli_text_and_restart_resume() -> Result<()> {
    let token = std::env::var("COPILOT_GITHUB_TOKEN").is_ok_and(|token| !token.is_empty());
    let native_home = std::env::var_os("COPILOT_LIVE_HOME").map(std::path::PathBuf::from);
    ensure!(
        token || native_home.is_some(),
        "Set COPILOT_GITHUB_TOKEN or COPILOT_LIVE_HOME for a Copilot-enabled account"
    );
    let root = tempfile::tempdir()?;
    let native_home = native_home.unwrap_or_else(|| root.path().join("copilot"));
    let environment = copilot_environment::environment(&native_home, token);
    let result = async {
    std::fs::write(root.path().join("config.json"), json!({
        "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"defaultBackend":"copilot","agentHome":root.path(),"backends":{"copilot":{
            "args":["--acp","--stdio","--disable-builtin-mcps","--no-custom-instructions","--no-auto-update"],
            "env":environment
        }}}
    }).to_string())?;
    let word = format!("COPILOT_{}", uuid::Uuid::new_v4().simple());
    let first = prompt(root.path(), &format!("This is an isolated integration test. Remember the synthetic test word {word}. Reply exactly COPILOT_ACP_LIVE_OK. Do not use tools.")).await?;
    ensure!(first.contains("COPILOT_ACP_LIVE_OK"), "Missing first live response: {first}");
    let store = Store::new(root.path().join("store.json"));
    let session = store.get("web", "cli", "copilot")?.context("Missing Copilot binding")?;
    let second = prompt(root.path(), "Return only the synthetic test word I asked you to remember in the previous turn. Do not use tools.").await?;
    ensure!(second.contains(&word), "Restart did not retain conversation context: {second}");
    ensure!(store.get("web", "cli", "copilot")?.context("Missing resumed binding")?.session_id == session.session_id,
        "Restart created a different Copilot session");
    println!("LIVE Copilot PASS: compiled CLI → native ACP → real inference; restart resumed identical session and recalled a synthetic word; no connectors or blanket tool grants");
    Ok(())
    }.await;
    if result.is_err() {
        eprintln!(
            "Retained private live-test evidence: {}",
            root.keep().display()
        );
    }
    result
}
