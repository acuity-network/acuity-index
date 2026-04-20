use byte_unit::Byte;
use clap::{ArgGroup, Parser, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use futures::StreamExt;
use signal_hook::{consts::TERM_SIGNALS, flag};
use signal_hook_tokio::Signals;
use std::{
    io::ErrorKind,
    path::PathBuf,
    process::exit,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods},
};
use tokio::{
    select, spawn,
    sync::{mpsc, watch},
    time::sleep,
};
use tracing::{error, info, warn};
use tracing_log::AsTrace;

mod config;
mod config_gen;
mod indexer;
mod pallets;
mod shared;
mod websockets;

use config::{IndexSpec, KUSAMA_TOML, OptionsConfig, PASEO_TOML, POLKADOT_TOML, WESTEND_TOML};
use config_gen::write_generated_index_spec;
use indexer::{process_sub_msg, run_indexer};
use shared::{RuntimeState, Trees, WsConfig};
use websockets::websockets_listen;

#[cfg(test)]
mod tests;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum Chain {
    Polkadot,
    Kusama,
    Westend,
    Paseo,
}

#[derive(Clone, ValueEnum, Debug)]
pub enum DbMode {
    LowSpace,
    HighThroughput,
}

impl From<DbMode> for sled::Mode {
    fn from(val: DbMode) -> Self {
        match val {
            DbMode::LowSpace => sled::Mode::LowSpace,
            DbMode::HighThroughput => sled::Mode::HighThroughput,
        }
    }
}

fn clap_styles() -> clap::builder::Styles {
    use clap::builder::styling::{AnsiColor, Effects, Styles};

    Styles::styled()
        .header(AnsiColor::Green.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Cyan.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Cyan.on_default())
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    disable_help_subcommand = true,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true,
    next_line_help = true,
    color = clap::ColorChoice::Always,
    styles = clap_styles()
)]
pub struct Args {
    #[command(flatten)]
    pub run: RunArgs,
    #[command(subcommand)]
    pub command: Option<Command>,
    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
    /// Delete the index database for the specified chain
    PurgeIndex {
        #[command(flatten)]
        args: PurgeIndexArgs,
    },
    /// Generate a starter index spec TOML from live node metadata
    GenerateIndexSpec {
        /// URL of Substrate node to connect to
        #[arg(short, long)]
        url: String,
        /// Path to write the generated index spec TOML file
        output: String,
    },
}

#[derive(Parser, Debug)]
#[command(group(ArgGroup::new("chain_source").required(true).args(["chain", "index_config"])))]
pub struct RunArgs {
    /// Chain to index
    #[arg(short, long, value_enum)]
    pub chain: Option<Chain>,
    /// Path to an index specification TOML file (overrides --chain)
    #[arg(long)]
    pub index_config: Option<String>,
    /// Path to an options TOML file
    #[arg(long)]
    pub options_config: Option<String>,
    /// Database path
    #[arg(short, long)]
    pub db_path: Option<String>,
    /// Database mode [default: low-space]
    #[arg(long, value_enum)]
    pub db_mode: Option<DbMode>,
    /// Maximum size for the system page cache [default: 1024.00 MiB]
    #[arg(long)]
    pub db_cache_capacity: Option<String>,
    /// URL of Substrate node to connect to
    #[arg(short, long)]
    pub url: Option<String>,
    /// Maximum number of concurrent block requests [default: 1]
    #[arg(long)]
    pub queue_depth: Option<u8>,
    /// Only index finalized blocks
    #[arg(short, long, default_value_t = false)]
    pub finalized: bool,
    /// Index event variants
    #[arg(short, long, default_value_t = false)]
    pub index_variant: bool,
    /// Store decoded events for immediate retrieval
    #[arg(short, long, default_value_t = false)]
    pub store_events: bool,
    /// WebSocket port [default: 8172]
    #[arg(short, long)]
    pub port: Option<u16>,
    /// Maximum concurrent WebSocket connections [default: 1024]
    #[arg(long)]
    pub max_connections: Option<usize>,
    /// Maximum total subscriptions across all connections [default: 65536]
    #[arg(long)]
    pub max_total_subscriptions: Option<usize>,
    /// Maximum subscriptions per connection [default: 128]
    #[arg(long)]
    pub max_subscriptions_per_connection: Option<usize>,
    /// Per-connection subscription notification buffer size [default: 256]
    #[arg(long)]
    pub subscription_buffer_size: Option<usize>,
    /// Subscription control channel buffer size [default: 1024]
    #[arg(long)]
    pub subscription_control_buffer_size: Option<usize>,
    /// Idle connection timeout in seconds [default: 300]
    #[arg(long)]
    pub idle_timeout_secs: Option<u64>,
    /// Maximum number of events returned per query [default: 1000]
    #[arg(long)]
    pub max_events_limit: Option<usize>,
}

#[derive(Parser, Debug)]
#[command(group(ArgGroup::new("chain_source").required(true).args(["chain", "index_config"])))]
pub struct PurgeIndexArgs {
    /// Chain whose index should be deleted
    #[arg(short, long, value_enum)]
    pub chain: Option<Chain>,
    /// Path to an index specification TOML file (overrides --chain)
    #[arg(long)]
    pub index_config: Option<String>,
    /// Database path
    #[arg(short, long)]
    pub db_path: Option<String>,
}

const DEFAULT_DB_MODE: DbMode = DbMode::LowSpace;
const DEFAULT_DB_CACHE_CAPACITY: &str = "1024.00 MiB";
const DEFAULT_QUEUE_DEPTH: u8 = 1;
const DEFAULT_PORT: u16 = 8172;

struct ResolvedArgs {
    db_path: Option<String>,
    db_mode: DbMode,
    db_cache_capacity: String,
    url: Option<String>,
    queue_depth: u8,
    finalized: bool,
    index_variant: bool,
    store_events: bool,
    port: u16,
    ws_config: WsConfig,
}

fn parse_db_mode(s: &str) -> Result<DbMode, String> {
    match s {
        "low_space" | "low-space" => Ok(DbMode::LowSpace),
        "high_throughput" | "high-throughput" => Ok(DbMode::HighThroughput),
        _ => Err(format!(
            "invalid db_mode '{}': expected 'low_space' or 'high_throughput'",
            s
        )),
    }
}

fn validate_ws_config(ws_config: &WsConfig) -> Result<(), String> {
    for (name, value) in [
        ("max_connections", ws_config.max_connections),
        ("max_total_subscriptions", ws_config.max_total_subscriptions),
        (
            "max_subscriptions_per_connection",
            ws_config.max_subscriptions_per_connection,
        ),
        (
            "subscription_buffer_size",
            ws_config.subscription_buffer_size,
        ),
        (
            "subscription_control_buffer_size",
            ws_config.subscription_control_buffer_size,
        ),
        ("max_events_limit", ws_config.max_events_limit),
    ] {
        if value == 0 {
            return Err(format!("{name} must be greater than 0"));
        }
    }

    Ok(())
}

fn resolve_args(
    cli: &RunArgs,
    spec: &IndexSpec,
    options: Option<&OptionsConfig>,
) -> Result<ResolvedArgs, String> {
    let opts = options.as_ref();

    let db_mode = if let Some(ref m) = cli.db_mode {
        m.clone()
    } else if let Some(ref s) = opts.and_then(|o| o.db_mode.as_ref()) {
        parse_db_mode(s)?
    } else {
        DEFAULT_DB_MODE.clone()
    };

    let db_cache_capacity = cli
        .db_cache_capacity
        .clone()
        .or_else(|| opts.and_then(|o| o.db_cache_capacity.clone()))
        .unwrap_or_else(|| DEFAULT_DB_CACHE_CAPACITY.to_string());

    let queue_depth = cli
        .queue_depth
        .or(opts.and_then(|o| o.queue_depth))
        .unwrap_or(DEFAULT_QUEUE_DEPTH);

    let finalized = cli.finalized || opts.and_then(|o| o.finalized).unwrap_or(false);
    let index_variant = cli.index_variant || spec.index_variant;
    let store_events = cli.store_events || spec.store_events;

    let port = cli
        .port
        .or(opts.and_then(|o| o.port))
        .unwrap_or(DEFAULT_PORT);

    let default_ws = WsConfig::default();
    let ws_config = WsConfig {
        max_connections: cli
            .max_connections
            .or(opts.and_then(|o| o.max_connections))
            .unwrap_or(default_ws.max_connections),
        max_total_subscriptions: cli
            .max_total_subscriptions
            .or(opts.and_then(|o| o.max_total_subscriptions))
            .unwrap_or(default_ws.max_total_subscriptions),
        max_subscriptions_per_connection: cli
            .max_subscriptions_per_connection
            .or(opts.and_then(|o| o.max_subscriptions_per_connection))
            .unwrap_or(default_ws.max_subscriptions_per_connection),
        subscription_buffer_size: cli
            .subscription_buffer_size
            .or(opts.and_then(|o| o.subscription_buffer_size))
            .unwrap_or(default_ws.subscription_buffer_size),
        subscription_control_buffer_size: cli
            .subscription_control_buffer_size
            .or(opts.and_then(|o| o.subscription_control_buffer_size))
            .unwrap_or(default_ws.subscription_control_buffer_size),
        idle_timeout_secs: cli
            .idle_timeout_secs
            .or(opts.and_then(|o| o.idle_timeout_secs))
            .unwrap_or(default_ws.idle_timeout_secs),
        max_events_limit: cli
            .max_events_limit
            .or(opts.and_then(|o| o.max_events_limit))
            .unwrap_or(default_ws.max_events_limit),
    };

    validate_ws_config(&ws_config)?;

    Ok(ResolvedArgs {
        db_path: cli
            .db_path
            .clone()
            .or_else(|| opts.and_then(|o| o.db_path.clone())),
        db_mode,
        db_cache_capacity,
        url: cli.url.clone().or_else(|| opts.and_then(|o| o.url.clone())),
        queue_depth,
        finalized,
        index_variant,
        store_events,
        port,
        ws_config,
    })
}

fn load_index_spec(chain: Option<Chain>, index_config_path: Option<&str>) -> IndexSpec {
    let spec: IndexSpec = if let Some(path) = index_config_path {
        let toml_str = std::fs::read_to_string(path).unwrap_or_else(|e| {
            error!("Cannot read index spec {path}: {e}");
            exit(1);
        });
        toml::from_str(&toml_str).unwrap_or_else(|e| {
            error!("Invalid index spec: {e}");
            exit(1);
        })
    } else {
        let toml_str = match chain {
            Some(Chain::Polkadot) => POLKADOT_TOML,
            Some(Chain::Kusama) => KUSAMA_TOML,
            Some(Chain::Westend) => WESTEND_TOML,
            Some(Chain::Paseo) => PASEO_TOML,
            None => unreachable!("clap requires --chain or --index-config"),
        };
        toml::from_str(toml_str).expect("Built-in TOML is valid")
    };

    spec.validate().unwrap_or_else(|e| {
        error!("Invalid index spec: {e}");
        exit(1);
    });

    spec
}

fn load_options_config(path: &str) -> OptionsConfig {
    let toml_str = std::fs::read_to_string(path).unwrap_or_else(|e| {
        error!("Cannot read options config {path}: {e}");
        exit(1);
    });
    toml::from_str(&toml_str).unwrap_or_else(|e| {
        error!("Invalid options config: {e}");
        exit(1);
    })
}

fn apply_resolved_indexing_flags(mut spec: IndexSpec, resolved: &ResolvedArgs) -> IndexSpec {
    spec.index_variant = resolved.index_variant;
    spec.store_events = resolved.store_events;
    spec
}

fn resolve_db_path(chain_name: &str, db_path: Option<&str>) -> PathBuf {
    match db_path {
        Some(path) => PathBuf::from(path),
        None => match home::home_dir() {
            Some(mut p) => {
                p.push(".local/share/acuity-index");
                p.push(chain_name);
                p.push("db");
                p
            }
            None => {
                error!("No home directory.");
                exit(1);
            }
        },
    }
}

fn purge_index(args: &PurgeIndexArgs) {
    let spec = load_index_spec(args.chain, args.index_config.as_deref());
    let db_path = resolve_db_path(&spec.name, args.db_path.as_deref());

    match std::fs::remove_dir_all(&db_path) {
        Ok(()) => {
            info!("Purged index at {}", db_path.display());
            exit(0);
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            info!("Index path does not exist: {}", db_path.display());
            exit(0);
        }
        Err(err) => {
            error!("Failed to purge index at {}: {err}", db_path.display());
            exit(1);
        }
    }
}

fn normalize_args<I>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    args.into_iter().collect()
}

#[cfg(test)]
fn describe_indexer_shutdown_result(
    result: Result<Result<(), shared::IndexError>, tokio::task::JoinError>,
) -> &'static str {
    match result {
        Ok(Ok(())) => {
            info!("Indexer stopped cleanly; beginning shutdown.");
        }
        Ok(Err(err)) => {
            error!("Indexer stopped with error: {err}");
        }
        Err(err) => {
            error!("Indexer task failed: {err}");
        }
    }
    "indexer stopped"
}

fn parse_db_cache_capacity(value: &str) -> Result<u64, shared::IndexError> {
    let bytes = Byte::parse_str(value, true).map_err(|err| {
        shared::internal_error(format!("invalid db cache capacity '{value}': {err}"))
    })?;
    bytes.as_u64_checked().ok_or_else(|| {
        shared::internal_error(format!("db cache capacity '{value}' does not fit in u64"))
    })
}

fn init_db_genesis(
    trees: &Trees,
    genesis_hash_config: &[u8],
) -> Result<Vec<u8>, shared::IndexError> {
    match trees.root.get("genesis_hash")? {
        Some(v) => Ok(v.to_vec()),
        None => {
            trees.root.insert("genesis_hash", genesis_hash_config)?;
            Ok(genesis_hash_config.to_vec())
        }
    }
}

const INITIAL_BACKOFF_SECS: u64 = 1;
const MAX_BACKOFF_SECS: u64 = 60;

async fn connect_rpc(
    url: &str,
) -> Result<
    (
        OnlineClient<PolkadotConfig>,
        LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    ),
    shared::IndexError,
> {
    let rpc_client = RpcClient::from_url(url).await?;
    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone()).await?;
    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client);
    Ok((api, rpc))
}

async fn run() -> Result<(), shared::IndexError> {
    let args = Args::parse_from(normalize_args(std::env::args()));
    let log_level = args.verbose.log_level_filter().as_trace();
    tracing_subscriber::fmt().with_max_level(log_level).init();

    match &args.command {
        Some(Command::PurgeIndex { args: purge_args }) => purge_index(purge_args),
        Some(Command::GenerateIndexSpec { url, output }) => {
            match write_generated_index_spec(url, PathBuf::from(output).as_path()).await {
                Ok(spec) => {
                    info!("Generated index spec for {} at {}", spec.name, output);
                    exit(0);
                }
                Err(err) => {
                    error!("Failed to generate index spec: {err}");
                    exit(1);
                }
            }
        }
        None => {}
    }

    let run_args = &args.run;
    let spec = load_index_spec(run_args.chain, run_args.index_config.as_deref());
    let options = run_args
        .options_config
        .as_deref()
        .map(|p| load_options_config(p));
    let resolved = resolve_args(run_args, &spec, options.as_ref()).unwrap_or_else(|e| {
        error!("{e}");
        exit(1);
    });
    let spec = apply_resolved_indexing_flags(spec, &resolved);

    info!("Indexing chain: {}", spec.name);

    let genesis_hash_config = spec
        .genesis_hash_bytes()
        .map_err(|e| shared::internal_error(format!("invalid genesis hash in config: {e}")))?;

    let db_path = resolve_db_path(&spec.name, resolved.db_path.as_deref());
    info!("Database path: {}", db_path.display());

    let db_cache_capacity = parse_db_cache_capacity(&resolved.db_cache_capacity)?;

    let db_config = sled::Config::new()
        .path(db_path)
        .mode(resolved.db_mode.clone().into())
        .cache_capacity(db_cache_capacity);

    let trees = Trees::open(db_config)?;

    let stored_genesis = init_db_genesis(&trees, genesis_hash_config.as_ref())?;
    if stored_genesis != genesis_hash_config {
        return Err(shared::internal_error(format!(
            "database genesis hash mismatch. expected 0x{}, stored 0x{}",
            hex::encode(genesis_hash_config),
            hex::encode(stored_genesis),
        )));
    }

    let url = resolved
        .url
        .clone()
        .unwrap_or_else(|| spec.default_url.clone());

    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        flag::register(*sig, Arc::clone(&term_now))?;
    }

    let runtime = Arc::new(RuntimeState::new(
        resolved.ws_config.max_total_subscriptions,
    ));
    let (exit_tx, exit_rx) = watch::channel(false);
    let (sub_tx, mut sub_rx) =
        mpsc::channel(resolved.ws_config.subscription_control_buffer_size.max(1));

    let subscriptions_runtime = runtime.clone();
    let _subscription_task = spawn(async move {
        while let Some(msg) = sub_rx.recv().await {
            if let Err(err) = process_sub_msg(subscriptions_runtime.as_ref(), msg) {
                error!("Subscription rejected: {err}");
            }
        }
    });

    let ws_task = spawn(websockets_listen(
        trees.clone(),
        runtime.clone(),
        resolved.port,
        exit_rx.clone(),
        sub_tx,
        resolved.ws_config.clone(),
    ));

    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut signals = Signals::new(TERM_SIGNALS)?;

    loop {
        if term_now.load(Ordering::Relaxed) {
            runtime.set_rpc(None);
            let _ = exit_tx.send(true);
            let _ = ws_task.await;
            let _ = trees.flush();
            info!("Shutdown requested; exiting.");
            return Ok(());
        }

        info!("Connecting to {url}");

        let (api, rpc) = match connect_rpc(&url).await {
            Ok(clients) => clients,
            Err(err) if err.is_recoverable() => {
                runtime.set_rpc(None);
                error!("RPC connection failed: {err}; retrying in {backoff_secs}s");
                select! {
                    _ = signals.next() => {
                        let _ = exit_tx.send(true);
                        let _ = ws_task.await;
                        let _ = trees.flush();
                        info!("Shutdown requested during reconnection backoff.");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_secs(backoff_secs)) => {}
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
            Err(err) => return Err(err),
        };

        let chain_genesis = api.genesis_hash().as_ref().to_vec();
        if chain_genesis != genesis_hash_config {
            return Err(shared::internal_error(format!(
                "chain genesis hash mismatch. expected 0x{}, chain 0x{}",
                hex::encode(genesis_hash_config),
                hex::encode(chain_genesis),
            )));
        }

        backoff_secs = INITIAL_BACKOFF_SECS;
        runtime.set_rpc(Some(rpc.clone()));

        let indexer_handle = spawn(run_indexer(
            trees.clone(),
            api,
            rpc,
            spec.clone(),
            resolved.finalized,
            resolved.queue_depth.into(),
            exit_rx.clone(),
            runtime.clone(),
        ));
        tokio::pin!(indexer_handle);

        let indexer_result = select! {
            _ = signals.next() => {
                runtime.set_rpc(None);
                let _ = exit_tx.send(true);
                let _ = indexer_handle.await;
                let _ = ws_task.await;
                let _ = trees.flush();
                info!("Shutdown complete.");
                return Ok(());
            }
            result = &mut indexer_handle => result,
        };

        let _ = trees.flush();

        match indexer_result {
            Ok(Ok(())) => {
                runtime.set_rpc(None);
                let _ = exit_tx.send(true);
                let _ = ws_task.await;
                info!("Indexer stopped cleanly.");
                return Ok(());
            }
            Ok(Err(err)) if err.is_recoverable() => {
                runtime.set_rpc(None);
                warn!("Indexer error (recoverable): {err}; reconnecting in {backoff_secs}s");
                select! {
                    _ = signals.next() => {
                        let _ = exit_tx.send(true);
                        let _ = ws_task.await;
                        let _ = trees.flush();
                        info!("Shutdown requested during reconnection backoff.");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_secs(backoff_secs)) => {}
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
            Ok(Err(err)) => {
                runtime.set_rpc(None);
                let _ = exit_tx.send(true);
                let _ = ws_task.await;
                error!("Indexer error (fatal): {err}");
                return Err(err);
            }
            Err(join_err) => {
                runtime.set_rpc(None);
                let _ = exit_tx.send(true);
                let _ = ws_task.await;
                error!("Indexer task failed: {join_err}");
                return Err(shared::internal_error(format!(
                    "indexer task panicked: {join_err}"
                )));
            }
        }
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        error!("{err}");
        exit(1);
    }
    exit(0);
}

#[cfg(test)]
mod main_tests {
    use super::*;

    fn test_spec() -> IndexSpec {
        IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            versions: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: Default::default(),
            pallets: vec![],
        }
    }

    #[test]
    fn db_mode_maps_to_sled_modes() {
        assert!(matches!(
            sled::Mode::from(DbMode::LowSpace),
            sled::Mode::LowSpace
        ));
        assert!(matches!(
            sled::Mode::from(DbMode::HighThroughput),
            sled::Mode::HighThroughput
        ));
    }

    #[test]
    fn clap_styles_builds_without_panicking() {
        let _ = clap_styles();
    }

    #[test]
    fn args_parse_defaults() {
        let args = Args::try_parse_from(normalize_args([
            "acuity-index".to_string(),
            "--chain".to_string(),
            "polkadot".to_string(),
        ]))
        .unwrap();
        assert!(args.command.is_none());
        assert!(matches!(args.run.chain, Some(Chain::Polkadot)));
        assert!(args.run.db_mode.is_none());
        assert!(args.run.db_cache_capacity.is_none());
        assert!(args.run.queue_depth.is_none());
        assert!(!args.run.finalized);
        assert!(!args.run.index_variant);
        assert!(!args.run.store_events);
        assert!(args.run.port.is_none());
    }

    #[test]
    fn resolve_args_uses_defaults_when_no_config() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let spec = test_spec();
        let resolved = resolve_args(&args, &spec, None).unwrap();
        assert!(matches!(resolved.db_mode, DbMode::LowSpace));
        assert_eq!(resolved.db_cache_capacity, "1024.00 MiB");
        assert_eq!(resolved.queue_depth, 1);
        assert!(!resolved.finalized);
        assert!(!resolved.index_variant);
        assert!(!resolved.store_events);
        assert_eq!(resolved.port, 8172);

        let default_ws = WsConfig::default();
        assert_eq!(
            resolved.ws_config.max_connections,
            default_ws.max_connections
        );
        assert_eq!(
            resolved.ws_config.max_total_subscriptions,
            default_ws.max_total_subscriptions
        );
        assert_eq!(
            resolved.ws_config.max_subscriptions_per_connection,
            default_ws.max_subscriptions_per_connection
        );
        assert_eq!(
            resolved.ws_config.subscription_buffer_size,
            default_ws.subscription_buffer_size
        );
        assert_eq!(
            resolved.ws_config.subscription_control_buffer_size,
            default_ws.subscription_control_buffer_size
        );
        assert_eq!(
            resolved.ws_config.idle_timeout_secs,
            default_ws.idle_timeout_secs
        );
        assert_eq!(
            resolved.ws_config.max_events_limit,
            default_ws.max_events_limit
        );
    }

    #[test]
    fn resolve_args_config_overrides_defaults() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            versions: vec![0],
            index_variant: true,
            store_events: true,
            custom_keys: Default::default(),
            pallets: vec![],
        };
        let opts = OptionsConfig {
            url: Some("ws://custom:9944".into()),
            db_path: Some("/data/db".into()),
            db_mode: Some("high_throughput".into()),
            db_cache_capacity: Some("2 GiB".into()),
            queue_depth: Some(4),
            finalized: Some(true),
            port: Some(9999),
            max_connections: None,
            max_total_subscriptions: None,
            max_subscriptions_per_connection: None,
            subscription_buffer_size: None,
            subscription_control_buffer_size: None,
            idle_timeout_secs: None,
            max_events_limit: None,
        };
        let resolved = resolve_args(&args, &spec, Some(&opts)).unwrap();
        assert!(matches!(resolved.db_mode, DbMode::HighThroughput));
        assert_eq!(resolved.db_cache_capacity, "2 GiB");
        assert_eq!(resolved.queue_depth, 4);
        assert!(resolved.finalized);
        assert!(resolved.index_variant);
        assert!(resolved.store_events);
        assert_eq!(resolved.port, 9999);
        assert_eq!(resolved.url.as_deref(), Some("ws://custom:9944"));
        assert_eq!(resolved.db_path.as_deref(), Some("/data/db"));
    }

    #[test]
    fn resolve_args_cli_overrides_config() {
        let args = RunArgs::try_parse_from([
            "acuity-index",
            "--chain",
            "polkadot",
            "--port",
            "1234",
            "--queue-depth",
            "8",
            "--finalized",
            "--db-mode",
            "high-throughput",
            "--store-events",
        ])
        .unwrap();
        let spec = test_spec();
        let opts = OptionsConfig {
            url: Some("ws://config:9944".into()),
            db_path: None,
            db_mode: Some("low_space".into()),
            db_cache_capacity: Some("512 MiB".into()),
            queue_depth: Some(2),
            finalized: Some(false),
            port: Some(9999),
            max_connections: None,
            max_total_subscriptions: None,
            max_subscriptions_per_connection: None,
            subscription_buffer_size: None,
            subscription_control_buffer_size: None,
            idle_timeout_secs: None,
            max_events_limit: None,
        };
        let resolved = resolve_args(&args, &spec, Some(&opts)).unwrap();
        assert!(matches!(resolved.db_mode, DbMode::HighThroughput));
        assert_eq!(resolved.queue_depth, 8);
        assert!(resolved.finalized);
        assert!(!resolved.index_variant);
        assert!(resolved.store_events);
        assert_eq!(resolved.port, 1234);
    }

    #[test]
    fn resolve_args_invalid_db_mode() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let spec = test_spec();
        let opts = OptionsConfig {
            db_mode: Some("invalid".into()),
            ..Default::default()
        };
        assert!(resolve_args(&args, &spec, Some(&opts)).is_err());
    }

    #[test]
    fn resolve_args_rejects_zero_websocket_limits() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let spec = test_spec();

        for (field, opts) in [
            (
                "max_connections",
                OptionsConfig {
                    max_connections: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_total_subscriptions",
                OptionsConfig {
                    max_total_subscriptions: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_subscriptions_per_connection",
                OptionsConfig {
                    max_subscriptions_per_connection: Some(0),
                    ..Default::default()
                },
            ),
            (
                "subscription_buffer_size",
                OptionsConfig {
                    subscription_buffer_size: Some(0),
                    ..Default::default()
                },
            ),
            (
                "subscription_control_buffer_size",
                OptionsConfig {
                    subscription_control_buffer_size: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_events_limit",
                OptionsConfig {
                    max_events_limit: Some(0),
                    ..Default::default()
                },
            ),
        ] {
            let err = resolve_args(&args, &spec, Some(&opts)).err().unwrap();
            assert!(err.contains(field), "unexpected error for {field}: {err}");
        }
    }

    #[test]
    fn resolve_args_allows_zero_idle_timeout_to_disable_it() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let spec = test_spec();
        let opts = OptionsConfig {
            idle_timeout_secs: Some(0),
            ..Default::default()
        };

        let resolved = resolve_args(&args, &spec, Some(&opts)).unwrap();

        assert_eq!(resolved.ws_config.idle_timeout_secs, 0);
    }

    #[test]
    fn resolve_args_spec_index_variant_or_with_cli() {
        let args = RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot"]).unwrap();
        let mut spec = test_spec();
        spec.index_variant = true;
        spec.store_events = true;
        let resolved = resolve_args(&args, &spec, None).unwrap();
        assert!(resolved.index_variant);
        assert!(resolved.store_events);
    }

    #[test]
    fn resolve_args_spec_and_cli_both_true() {
        let args =
            RunArgs::try_parse_from(["acuity-index", "--chain", "polkadot", "--index-variant"])
                .unwrap();
        let mut spec = test_spec();
        spec.index_variant = true;
        let resolved = resolve_args(&args, &spec, None).unwrap();
        assert!(resolved.index_variant);
        assert!(!resolved.store_events);
    }

    #[test]
    fn apply_resolved_indexing_flags_preserves_cli_overrides() {
        let args = RunArgs::try_parse_from([
            "acuity-index",
            "--chain",
            "polkadot",
            "--index-variant",
            "--store-events",
        ])
        .unwrap();
        let spec = test_spec();
        let resolved = resolve_args(&args, &spec, None).unwrap();
        let spec = apply_resolved_indexing_flags(spec, &resolved);

        assert!(spec.index_variant);
        assert!(spec.store_events);
    }

    #[test]
    fn load_options_config_parses_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("options.toml");
        std::fs::write(
            &path,
            r#"
url = "wss://rpc.example.com:443"
db_mode = "high_throughput"
queue_depth = 4
finalized = true
port = 9999
max_connections = 2048
max_total_subscriptions = 100000
max_subscriptions_per_connection = 64
subscription_buffer_size = 512
subscription_control_buffer_size = 2048
idle_timeout_secs = 600
max_events_limit = 500
"#,
        )
        .unwrap();
        let opts = load_options_config(path.to_str().unwrap());
        assert_eq!(opts.url.as_deref(), Some("wss://rpc.example.com:443"));
        assert_eq!(opts.db_mode.as_deref(), Some("high_throughput"));
        assert_eq!(opts.queue_depth, Some(4));
        assert_eq!(opts.finalized, Some(true));
        assert_eq!(opts.port, Some(9999));
        assert_eq!(opts.max_connections, Some(2048));
        assert_eq!(opts.max_total_subscriptions, Some(100000));
        assert_eq!(opts.max_subscriptions_per_connection, Some(64));
        assert_eq!(opts.subscription_buffer_size, Some(512));
        assert_eq!(opts.subscription_control_buffer_size, Some(2048));
        assert_eq!(opts.idle_timeout_secs, Some(600));
        assert_eq!(opts.max_events_limit, Some(500));
    }

    #[test]
    fn args_parse_store_events_flag() {
        let args = Args::try_parse_from(["acuity-index", "--chain", "polkadot", "--store-events"])
            .unwrap();

        assert!(args.run.store_events);
    }

    #[test]
    fn describe_indexer_shutdown_result_reports_clean_stop() {
        assert_eq!(
            describe_indexer_shutdown_result(Ok(Ok(()))),
            "indexer stopped"
        );
    }

    #[test]
    fn describe_indexer_shutdown_result_reports_indexer_error() {
        assert_eq!(
            describe_indexer_shutdown_result(Ok(Err(shared::IndexError::BlockStreamClosed))),
            "indexer stopped"
        );
    }

    #[tokio::test]
    async fn describe_indexer_shutdown_result_reports_join_error() {
        let join_err = tokio::spawn(async {
            panic!("boom");
        })
        .await
        .unwrap_err();

        assert_eq!(
            describe_indexer_shutdown_result(Err(join_err)),
            "indexer stopped"
        );
    }

    #[test]
    fn args_parse_purge_index_chain() {
        let args =
            Args::try_parse_from(["acuity-index", "purge-index", "--chain", "kusama"]).unwrap();

        match args.command {
            Some(Command::PurgeIndex { args: purge_args }) => {
                assert!(matches!(purge_args.chain, Some(Chain::Kusama)));
                assert!(purge_args.db_path.is_none());
            }
            _ => panic!("expected purge-index command"),
        }
    }

    #[test]
    fn args_parse_purge_index_db_path() {
        let args = Args::try_parse_from([
            "acuity-index",
            "purge-index",
            "--chain",
            "polkadot",
            "--db-path",
            "/tmp/test-db",
        ])
        .unwrap();

        match args.command {
            Some(Command::PurgeIndex { args: purge_args }) => {
                assert_eq!(purge_args.db_path.as_deref(), Some("/tmp/test-db"));
            }
            _ => panic!("expected purge-index command"),
        }
    }

    #[test]
    fn args_reject_help_subcommand() {
        let err = Args::try_parse_from(["acuity-index", "help"]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn resolve_db_path_uses_chain_name() {
        let path = resolve_db_path("kusama", None);

        assert!(path.ends_with(".local/share/acuity-index/kusama/db"));
    }

    #[test]
    fn purge_index_removes_existing_directory() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("data"), b"x").unwrap();

        let result = std::fs::remove_dir_all(&db_path);

        assert!(result.is_ok());
        assert!(!db_path.exists());
    }

    #[test]
    fn purge_index_missing_directory_is_ok() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("missing-db");

        let result = std::fs::remove_dir_all(&db_path);

        assert!(matches!(result, Err(err) if err.kind() == ErrorKind::NotFound));
    }
}
