use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Parser, Subcommand};
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
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, visible_alias = "check")]
    check_config: bool,
    #[arg(long)]
    serve: bool,
    #[arg(long)]
    no_images: bool,
    #[arg(trailing_var_arg = true)]
    prompt: Vec<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
}

#[derive(Subcommand)]
enum ServiceCommand {
    #[command(group(ArgGroup::new("scope").required(true).args(["user", "system"])))]
    Install {
        #[arg(long)]
        user: bool,
        #[arg(long)]
        system: bool,
        #[arg(long)]
        run_as: Option<String>,
        #[arg(long)]
        dry_run: bool,
    },
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
    if let Some(command) = arguments.command {
        if arguments.acp_adapter.is_some()
            || arguments.check_config
            || arguments.serve
            || arguments.no_images
            || !arguments.prompt.is_empty()
        {
            bail!("service installation cannot be combined with gateway execution options");
        }
        return run_command(command, arguments.config);
    }
    if let Some(adapter) = arguments.acp_adapter {
        return adapter.serve().await;
    }
    let cwd = std::env::current_dir()?;
    let environment =
        config::project_environment(&cwd, std::env::vars().collect::<BTreeMap<_, _>>())?;
    let mut configuration =
        config::Loader::default().load(&cwd, arguments.config.as_deref(), &environment)?;
    if configuration.get("agents").is_some() {
        if !arguments.serve && !arguments.check_config
            || !arguments.prompt.is_empty()
            || arguments.no_images
        {
            bail!("Gateway manifests require --serve or --check-config without CLI prompt options");
        }
        return start_gateway(configuration, &environment, arguments.check_config).await;
    }
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
    serve_agents(
        &[ServiceAgent {
            bridge,
            #[cfg(feature = "platform-sdks")]
            matrix_storage: environment
                .get("MATRIX_STORAGE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/share/opencode-matrix-bot")),
        }],
        false,
    )
    .await
}

struct ServiceAgent {
    bridge: Arc<Bridge>,
    #[cfg(feature = "platform-sdks")]
    matrix_storage: PathBuf,
}

async fn start_gateway(
    configuration: serde_json::Value,
    environment: &BTreeMap<String, String>,
    check_only: bool,
) -> Result<()> {
    use soulsteads_messaging::gateway::{Gateway, agent_environment};
    let gateway = Gateway::load(configuration, environment).map_err(|error| {
        eprintln!("Gateway configuration: {error}");
        error
    })?;
    let env = agent_environment(environment);
    let mut agents = Vec::new();
    for agent in gateway.agents {
        let bridge =
            Bridge::new_for_gateway(agent.config, env.clone(), agent.state.join("sessions"))
                .with_context(|| format!("Invalid bridge configuration for agent {}", agent.id))?;
        let bridge = Arc::new(bridge);
        agents.push(ServiceAgent {
            bridge,
            #[cfg(feature = "platform-sdks")]
            matrix_storage: agent.state.join("matrix"),
        });
    }
    if check_only {
        println!("Configuration loaded successfully; no connectors started.");
        return Ok(());
    }
    // A second gateway must not poll bots or write stores under the same root.
    let lock = std::fs::File::open(&gateway.state)?;
    lock.try_lock().context("Gateway state is already in use")?;
    serve_agents(&agents, true).await
}

type ServiceTask = futures_util::future::BoxFuture<'static, Result<()>>;

async fn prepare_agent_tasks(
    agent: &ServiceAgent,
    multi_agent: bool,
    receiver: tokio::sync::watch::Receiver<bool>,
    identities: &mut std::collections::BTreeSet<(String, String, String)>,
) -> Result<Vec<ServiceTask>> {
    let bridge = &agent.bridge;
    let configuration = &bridge.config;
    let environment = &bridge.env;
    let web_port = config::enabled_web_port(configuration)?;
    config::validate_platform_sdk_support(configuration)?;
    let mut tasks: Vec<ServiceTask> = Vec::new();
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
        let connector = Platform::new(platform, &base, &token, "", "");
        let bridge = bridge.clone();
        let receiver = receiver.clone();
        if multi_agent {
            let connector = connector.identify().await?;
            let scope = url::Url::parse(&base)?.origin().ascii_serialization();
            if connector.bot.is_empty()
                || !identities.insert((platform.into(), scope, connector.bot.clone()))
            {
                bail!("Gateway connector account is missing or assigned more than once");
            }
            tasks.push(Box::pin(connector.run_identified(bridge, receiver)));
        } else {
            tasks.push(Box::pin(connector.run(bridge, receiver)));
        }
    }
    if let Some(port) = web_port {
        let address = format!("{}:{port}", string(&configuration["web"], "host"));
        let listener = tokio::net::TcpListener::bind(address).await?;
        let router =
            soulsteads_messaging::web::router(bridge.clone(), configuration["web"].clone());
        let mut receiver = receiver.clone();
        tasks.push(Box::pin(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = receiver.changed().await;
                })
                .await
                .map_err(Into::into)
        }));
    }
    #[cfg(feature = "platform-sdks")]
    for platform in ["matrix", "whatsapp"] {
        if !boolean(&configuration[platform], "enabled") {
            continue;
        }
        let bridge = bridge.clone();
        let receiver = receiver.clone();
        if platform == "matrix" {
            let client = soulsteads_messaging::connectors::matrix::connect(
                &configuration[platform],
                &agent.matrix_storage,
            )
            .await?;
            if multi_agent {
                let user = client
                    .user_id()
                    .context("Gateway Matrix account is missing")?;
                if !identities.insert((platform.into(), String::new(), user.to_string())) {
                    bail!("Gateway Matrix account assigned more than once");
                }
            }
            tasks.push(Box::pin(async move {
                soulsteads_messaging::connectors::matrix::run(client, bridge, receiver).await
            }));
        } else {
            let bot =
                soulsteads_messaging::connectors::whatsapp::build(&configuration[platform], bridge)
                    .await?;
            tasks.push(Box::pin(async move {
                soulsteads_messaging::connectors::whatsapp::run(bot, receiver).await
            }));
        }
    }
    if tasks.is_empty() {
        bail!("No connectors enabled");
    }
    let mirror_bridge = bridge.clone();
    let mut mirror_shutdown = receiver.clone();
    tasks.push(Box::pin(async move {
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _=mirror_shutdown.changed()=>break,
                _=poll.tick()=>{mirror_bridge.poll_mirrors().await?;}
            }
        }
        Ok(())
    }));
    #[cfg(feature = "agent-network")]
    if configuration["agentMessaging"]["tools"]["reviewQueueDir"].is_string() {
        let review_bridge = bridge.clone();
        let mut review_shutdown = receiver.clone();
        tasks.push(Box::pin(async move {
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
        }));
    }
    Ok(tasks)
}

async fn serve_agents(agents: &[ServiceAgent], multi_agent: bool) -> Result<()> {
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut stop = std::pin::pin!(shutdown_signal());
    let prepare = async {
        let mut pending = Vec::new();
        let mut identities = std::collections::BTreeSet::new();
        for agent in agents {
            pending.extend(
                prepare_agent_tasks(agent, multi_agent, receiver.clone(), &mut identities).await?,
            );
        }
        Ok::<_, anyhow::Error>(pending)
    };
    // Register signal handlers before transport startup, and do not dispatch
    // any prompts until all bindings have been prepared successfully.
    let pending = tokio::select! {
        biased;
        result = &mut stop => return result.map_err(Into::into),
        result = prepare => result?,
    };
    let mut tasks = tokio::task::JoinSet::new();
    for task in pending {
        tasks.spawn(task);
    }
    let result = supervise(
        agents.iter().map(|agent| agent.bridge.as_ref()),
        &mut tasks,
        stop,
        Instant::now,
    )
    .await;
    let _ = shutdown.send(true);
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.shutdown().await;
    }
    // Attempt every bridge cleanup even when one backend reports an error.
    let cleanup =
        futures_util::future::join_all(agents.iter().map(|agent| agent.bridge.shutdown())).await;
    result.and(cleanup.into_iter().collect::<Result<()>>())
}

fn run_command(command: Command, config: Option<PathBuf>) -> Result<()> {
    match command {
        Command::Service {
            command:
                ServiceCommand::Install {
                    user,
                    system,
                    run_as,
                    dry_run,
                },
        } => {
            let scope = match (user, system) {
                (true, false) => soulsteads_messaging::service::Scope::User,
                (false, true) => soulsteads_messaging::service::Scope::System,
                _ => bail!("choose exactly one of --user or --system"),
            };
            let config = config.context("service install requires --config PATH")?;
            let message = soulsteads_messaging::service::install(
                soulsteads_messaging::service::InstallRequest {
                    scope,
                    run_as,
                    dry_run,
                    config,
                },
            )?;
            println!("{message}");
            Ok(())
        }
    }
}

async fn supervise<'a>(
    bridges: impl IntoIterator<Item = &'a Bridge>,
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    stop: impl std::future::Future<Output = std::io::Result<()>>,
    sweep_time: impl Fn() -> Instant,
) -> Result<()> {
    let retention: Vec<_> = bridges
        .into_iter()
        .filter_map(|bridge| {
            let policy = SessionRetentionPolicy::from_environment(&bridge.env);
            soulsteads_messaging::gateway::PLATFORMS
                .iter()
                .any(|platform| {
                    boolean(&bridge.config[*platform], "enabled") && policy.is_enabled(platform)
                })
                .then_some((bridge, policy))
        })
        .collect();
    let mut sweep = (!retention.is_empty()).then(|| tokio::time::interval(Duration::from_secs(60)));
    tokio::pin!(stop);
    loop {
        tokio::select! {
            result=&mut stop=>break result.map_err(Into::into),
            _=async {
                match &mut sweep {
                    Some(sweep) => { sweep.tick().await; },
                    None => std::future::pending::<()>().await,
                }
            }=>{
                let now = sweep_time();
                for (bridge, policy) in &retention {
                    bridge.expire_with_policy(policy, now).await?;
                }
            },
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
            supervise([&bridge], &mut tasks, stop, || {
                ticks.set(ticks.get() + 1);
                Instant::now() + Duration::from_secs(age)
            })
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
