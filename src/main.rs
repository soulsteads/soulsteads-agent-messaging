use anyhow::{Result, bail};
use clap::Parser;
use soulsteads_messaging::{
    config::{self, boolean, string},
    connectors::http::Platform,
    runtime::{Bridge, Event, SessionRetentionPolicy},
};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};

mod cli_images;
use cli_images::Console;

#[derive(Parser)]
#[command(
    version,
    about = "Native ACP chat bridge",
    after_help = "Read-only settings: soulsteads-messaging --settings <list|validate|get> --help"
)]
struct Arguments {
    #[arg(long, value_enum, conflicts_with_all = ["config", "check_config", "serve", "no_images", "prompt"])]
    acp_adapter: Option<soulsteads_messaging::adapters::Kind>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, visible_alias = "check")]
    check_config: bool,
    #[arg(long)]
    serve: bool,
    #[arg(long)]
    no_images: bool,
    #[arg(trailing_var_arg = true)]
    prompt: Vec<String>,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum SettingsOperation {
    List,
    Validate,
    Get,
}

#[derive(Parser)]
#[command(
    about = "Inspect and validate local settings",
    disable_help_subcommand = true
)]
struct SettingsArguments {
    #[arg(long, value_enum, required = true, num_args = 0..=1)]
    settings: Option<SettingsOperation>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    json: bool,
    #[arg(value_name = "JSON_POINTER", required_if_eq("settings", "get"))]
    path: Option<String>,
}

fn settings_requested(args: &[OsString]) -> bool {
    let marker = args
        .iter()
        .skip(1)
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "--settings" || arg.as_encoded_bytes().starts_with(b"--settings="));
    // The legacy trailing prompt can contain option-shaped words. The MCP
    // proxy has no prompt, so a mixed invocation always reaches Settings.
    marker
        && (args.get(1).is_some_and(|arg| arg == "--mcp-sse-proxy")
            || Arguments::try_parse_from(args).is_err())
}

fn run_settings(args: Vec<OsString>) -> Result<()> {
    const USAGE: &str = "settings_usage: Use --settings list, validate, or get JSON_POINTER with optional --config FILE and --json.";
    let arguments = match SettingsArguments::try_parse_from(args) {
        Ok(arguments) => arguments,
        Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => error.exit(),
        Err(_) => bail!(USAGE),
    };
    let Some(operation) = arguments.settings else {
        bail!(USAGE);
    };
    if arguments.path.is_some() && !matches!(operation, SettingsOperation::Get) {
        bail!("settings_usage: A JSON Pointer is accepted only with --settings get.");
    }
    let cwd = std::env::current_dir()
        .map_err(|_| anyhow::anyhow!("config_read: Cannot determine the current directory."))?;
    let ambient = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    let settings =
        soulsteads_messaging::settings::Settings::load(&cwd, arguments.config.as_deref(), ambient)?;
    match operation {
        SettingsOperation::List => {
            let entries = settings.list();
            if arguments.json {
                println!("{}", serde_json::json!({ "settings": entries }));
            } else {
                for entry in entries {
                    println!("{}\t{}", entry.path, entry.source.as_str());
                }
            }
        }
        SettingsOperation::Get => {
            let entry = settings.get(arguments.path.as_deref().unwrap_or_default())?;
            if arguments.json {
                println!("{}", serde_json::json!(entry));
            } else {
                println!("{}\t{}", entry.path, entry.source.as_str());
            }
        }
        SettingsOperation::Validate => {
            settings.validate()?;
            if arguments.json {
                println!(r#"{{"valid":true}}"#);
            } else {
                println!("Local settings validation passed.");
            }
        }
    }
    Ok(())
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if settings_requested(&args) {
        if let Err(error) = run_settings(args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
        return;
    }
    run_bridge();
}

#[tokio::main]
async fn run_bridge() {
    if let Err(_error) = start().await {
        eprintln!("Chat bridge failed; verify local configuration and connector availability.");
        std::process::exit(1);
    }
}

async fn start() -> Result<()> {
    if soulsteads_messaging::adapters::serve_mcp_transport_if_requested().await? {
        return Ok(());
    }
    let arguments = Arguments::parse();
    if let Some(adapter) = arguments.acp_adapter {
        return adapter.serve().await;
    }
    let cwd = std::env::current_dir()?;
    let environment =
        config::project_environment(&cwd, std::env::vars().collect::<BTreeMap<_, _>>())?;
    let mut configuration =
        config::Loader::default().load(&cwd, arguments.config.as_deref(), &environment)?;
    config::apply_runtime_overrides(&mut configuration, &environment)?;
    // Validate even for --check-config, before connecting any chat service.
    config::validate_connector_access(&configuration, &environment).map_err(|error| {
        eprintln!("Connector access configuration: {error}");
        error
    })?;
    let home = environment
        .get("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| cwd.clone());
    let sessions = environment
        .get("SESSION_BASE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".cache/soulsteads-messaging/sessions"));
    let bridge = Arc::new(Bridge::new(
        configuration.clone(),
        environment.clone(),
        sessions,
    )?);
    if arguments.check_config {
        println!("Configuration loaded successfully; no connectors started.");
        return Ok(());
    }
    bridge.prepare_agent_tools().await?;
    if !arguments.serve {
        let console = Console::new(!arguments.no_images);
        if !arguments.prompt.is_empty() {
            let event = Event::web("cli", &arguments.prompt.join(" "));
            let result = bridge.handle(event.clone(), &console).await;
            console.finish(&bridge.working_directory(&event)).await;
            let shutdown = bridge.shutdown().await;
            result?;
            return shutdown;
        } else {
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            println!("OpenCode chat bridge; /quit exits.");
            loop {
                let line =
                    tokio::select! {line=lines.next_line()=>line?,_=tokio::signal::ctrl_c()=>None};
                let Some(line) = line else {
                    break;
                };
                if matches!(line.trim(), "/quit" | "/exit") {
                    break;
                }
                let mut event = Event::web("cli", &line);
                event.platform = "cli".into();
                let _ = bridge.handle(event.clone(), &console).await;
                console.finish(&bridge.working_directory(&event)).await;
            }
        }
        return bridge.shutdown().await;
    }
    let web_port = config::enabled_web_port(&configuration)?;
    config::validate_platform_sdk_support(&configuration)?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    for platform in ["telegram", "mattermost", "slack", "discord"] {
        if !boolean(&configuration[platform], "enabled") {
            continue;
        }
        let (base, token) = match platform {
            "telegram" => (
                "https://api.telegram.org".into(),
                string(&configuration[platform], "token").to_owned(),
            ),
            "mattermost" => (
                string(&configuration[platform], "url")
                    .trim_end_matches('/')
                    .to_owned(),
                string(&configuration[platform], "token").to_owned(),
            ),
            "slack" => (
                "https://slack.com".into(),
                environment
                    .get("SLACK_BOT_TOKEN")
                    .cloned()
                    .unwrap_or_default(),
            ),
            _ => (
                "https://discord.com".into(),
                environment
                    .get("DISCORD_BOT_TOKEN")
                    .cloned()
                    .unwrap_or_default(),
            ),
        };
        if token.is_empty() {
            bail!("Enabled connector lacks credentials");
        }
        let platform = Platform::new(platform, &base, &token, "", "");
        let bridge = bridge.clone();
        let receiver = receiver.clone();
        tasks.spawn(async move { platform.run(bridge, receiver).await });
    }
    if let Some(port) = web_port {
        let address = format!("{}:{port}", string(&configuration["web"], "host"));
        let listener = tokio::net::TcpListener::bind(address).await?;
        let router =
            soulsteads_messaging::web::router(bridge.clone(), configuration["web"].clone());
        let mut receiver = receiver.clone();
        tasks.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = receiver.changed().await;
                })
                .await
                .map_err(Into::into)
        });
    }
    #[cfg(feature = "platform-sdks")]
    for platform in ["matrix", "whatsapp"] {
        if !boolean(&configuration[platform], "enabled") {
            continue;
        }
        let bridge = bridge.clone();
        let receiver = receiver.clone();
        if platform == "matrix" {
            let storage = environment
                .get("MATRIX_STORAGE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/share/opencode-matrix-bot"));
            let client = soulsteads_messaging::connectors::matrix::connect(
                &configuration[platform],
                &storage,
            )
            .await?;
            tasks.spawn(async move {
                soulsteads_messaging::connectors::matrix::run(client, bridge, receiver).await
            });
        } else {
            let bot =
                soulsteads_messaging::connectors::whatsapp::build(&configuration[platform], bridge)
                    .await?;
            tasks.spawn(async move {
                soulsteads_messaging::connectors::whatsapp::run(bot, receiver).await
            });
        }
    }
    if tasks.is_empty() {
        bail!("No connectors enabled");
    }
    let mirror_bridge = bridge.clone();
    let mut mirror_shutdown = receiver.clone();
    tasks.spawn(async move {
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _=mirror_shutdown.changed()=>break,
                _=poll.tick()=>{mirror_bridge.poll_mirrors().await?;}
            }
        }
        Ok(())
    });
    #[cfg(feature = "agent-network")]
    if configuration["agentMessaging"]["tools"]["reviewQueueDir"].is_string() {
        let review_bridge = bridge.clone();
        let mut review_shutdown = receiver.clone();
        tasks.spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(1));
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = review_shutdown.changed() => break,
                    _ = poll.tick() => {
                        tokio::select! {
                            _ = review_shutdown.changed() => break,
                            result = review_bridge.poll_agent_reviews() => result?,
                        }
                    }
                }
            }
            Ok(())
        });
    }
    let result = supervise(
        &bridge,
        &environment,
        &configuration,
        &mut tasks,
        shutdown_signal(),
        Instant::now,
    )
    .await;
    let _ = shutdown.send(true);
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
    }
    bridge.shutdown().await?;
    result
}

async fn supervise(
    bridge: &Bridge,
    environment: &BTreeMap<String, String>,
    configuration: &serde_json::Value,
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    stop: impl std::future::Future<Output = std::io::Result<()>>,
    sweep_time: impl Fn() -> Instant,
) -> Result<()> {
    let retention = SessionRetentionPolicy::from_environment(environment);
    let mut sweep = [
        "matrix",
        "discord",
        "mattermost",
        "telegram",
        "slack",
        "whatsapp",
        "web",
    ]
    .iter()
    .any(|platform| boolean(&configuration[platform], "enabled") && retention.is_enabled(platform))
    .then(|| tokio::time::interval(Duration::from_secs(60)));
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _=&mut stop=>break Ok(()),
            _=async {
                match &mut sweep {
                    Some(sweep) => { sweep.tick().await; },
                    None => std::future::pending::<()>().await,
                }
            }=>{bridge.expire_with_policy(&retention, sweep_time()).await?;},
            result=tasks.join_next()=>break match result{Some(Ok(result))=>result,Some(Err(error))=>Err(error.into()),None=>Ok(())},
        }
    }
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
#[path = "../tests/support/acp_fixture_path.rs"]
mod fixture_path;

#[cfg(test)]
mod retention_startup_tests {
    use super::*;
    use serde_json::{Value, json};
    use soulsteads_messaging::{runtime::Sink, store::Store};

    struct Recorder;
    #[async_trait::async_trait]
    impl Sink for Recorder {
        async fn emit(&self, _: Value) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn retention_policy_actual_startup_web_default_and_project_numeric_prefix() {
        for raw in [None, Some("0"), Some("-15"), Some("1minutes")] {
            let directory = tempfile::Builder::new()
                .prefix("retention-startup-")
                .tempdir()
                .unwrap();
            if let Some(raw) = raw {
                std::fs::write(
                    directory.path().join(".env"),
                    format!("SESSION_RETENTION_MINS={raw}\n"),
                )
                .unwrap();
            }
            let environment =
                config::project_environment(directory.path(), BTreeMap::new()).unwrap();
            let fixture = fixture_path::acp_fixture();
            let configuration = config::load(&json!({"rateLimitSeconds":0,"web":{"enabled":true},"sessionStorePath":directory.path().join("store.json"),"acp":{"command":fixture,"args":[]}}).to_string(), &environment).unwrap();
            let bridge = Bridge::new(
                configuration.clone(),
                environment.clone(),
                directory.path().join("sessions"),
            )
            .unwrap();
            bridge
                .handle(Event::web("startup", "hello"), &Recorder)
                .await
                .unwrap();
            let store = Store::new(directory.path().join("store.json"));
            let initial = store.get("web", "startup", "opencode").unwrap();
            assert!(initial.is_some());
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(std::future::pending());
            let stop = async {
                if raw != Some("1minutes") {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                } else {
                    let _ = tokio::time::timeout(Duration::from_secs(2), async {
                        while store.get("web", "startup", "opencode").unwrap().is_some() {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    })
                    .await;
                }
                Ok(())
            };
            let age = if raw != Some("1minutes") {
                31 * 60
            } else {
                120
            };
            let ticks = std::cell::Cell::new(0);
            supervise(
                &bridge,
                &environment,
                &configuration,
                &mut tasks,
                stop,
                || {
                    ticks.set(ticks.get() + 1);
                    Instant::now() + Duration::from_secs(age)
                },
            )
            .await
            .unwrap();
            let after = store.get("web", "startup", "opencode").unwrap();
            tasks.abort_all();
            bridge.shutdown().await.unwrap();
            assert_eq!(
                after.is_none(),
                raw == Some("1minutes"),
                "actual startup raw={raw:?}"
            );
            if raw != Some("1minutes") {
                assert_eq!(after, initial);
                assert_eq!(
                    ticks.get(),
                    0,
                    "all active platforms disabled: no sweep timer"
                );
            } else {
                assert!(ticks.get() > 0);
            }
        }
    }
}
