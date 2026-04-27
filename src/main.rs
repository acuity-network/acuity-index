use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use futures::StreamExt;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use signal_hook::{consts::TERM_SIGNALS, flag};
use signal_hook_tokio::Signals;
use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io::ErrorKind,
    path::{Component, Path, PathBuf},
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
    net::TcpListener,
    select, spawn,
    sync::{mpsc, oneshot, watch},
    time::sleep,
};
use tracing::{error, info, warn};
use tracing_log::AsTrace;

mod config;
mod config_gen;
mod errors;
mod event_hydration;
mod indexer;
mod metrics;
mod protocol;
mod runtime_state;
mod ws_api;

use config::{IndexSpec, OptionsConfig};
use config_gen::write_generated_index_spec;
use errors::{IndexError, internal_error};
use indexer::{process_sub_msg, run_indexer};
use metrics::{Metrics, metrics_listen};
use protocol::{LiveWsConfig, Trees, WsConfig};
use runtime_state::RuntimeState;
use ws_api::websockets_listen;

#[cfg(test)]
mod tests;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Clone, ValueEnum, Debug, PartialEq, Eq)]
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
    subcommand_required = true,
    next_line_help = true,
    color = clap::ColorChoice::Always,
    styles = clap_styles()
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
    /// Run the indexer for the specified chain
    Run {
        #[command(flatten)]
        args: RunArgs,
    },
    /// Delete the index database for the specified chain
    PurgeIndex {
        #[command(flatten)]
        args: PurgeIndexArgs,
    },
    /// Generate a starter index spec TOML from live node metadata
    GenerateIndexSpec {
        /// Path to write the generated index spec TOML file
        index_spec: String,
        /// URL of Substrate node to connect to
        #[arg(short, long)]
        url: String,
        /// Overwrite the output file if it already exists
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Path to a hot-reloading index specification TOML file
    pub index_spec: String,
    /// Path to a hot-reloading options TOML file
    #[arg(long)]
    pub options_config: Option<String>,
    /// Database path
    #[arg(short, long, value_parser = parse_db_path_arg)]
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
    /// WebSocket port [default: 8172]
    #[arg(short, long)]
    pub port: Option<u16>,
    /// OpenMetrics HTTP port
    #[arg(long)]
    pub metrics_port: Option<u16>,
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
pub struct PurgeIndexArgs {
    /// Path to an index specification TOML file
    pub index_spec: String,
    /// Database path
    #[arg(short, long, value_parser = parse_db_path_arg)]
    pub db_path: Option<String>,
}

const DEFAULT_DB_MODE: DbMode = DbMode::LowSpace;
const DEFAULT_DB_CACHE_CAPACITY: &str = "1024.00 MiB";
const DEFAULT_QUEUE_DEPTH: u8 = 1;
const DEFAULT_PORT: u16 = 8172;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedArgs {
    db_path: Option<String>,
    db_mode: DbMode,
    db_cache_capacity: String,
    url: Option<String>,
    queue_depth: u8,
    finalized: bool,
    port: u16,
    metrics_port: Option<u16>,
    ws_config: WsConfig,
}

#[derive(Clone, Debug)]
struct ConfigSnapshot {
    spec: IndexSpec,
    source_hash: u64,
}

#[derive(Clone, Debug)]
struct OptionsSnapshot {
    options: OptionsConfig,
    source_hash: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpecUpdateAction {
    RestartIndexer,
    Unchanged,
}

#[derive(Clone, Debug)]
struct SpecUpdate {
    snapshot: ConfigSnapshot,
    action: SpecUpdateAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OptionsUpdateAction {
    RestartIndexer,
    Unchanged,
}

#[derive(Clone, Debug)]
struct OptionsRuntimeUpdate {
    resolved: ResolvedArgs,
    action: OptionsUpdateAction,
    ignored_fields: Vec<&'static str>,
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
    _spec: &IndexSpec,
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

    let port = cli
        .port
        .or(opts.and_then(|o| o.port))
        .unwrap_or(DEFAULT_PORT);
    let metrics_port = cli.metrics_port.or(opts.and_then(|o| o.metrics_port));

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
        port,
        metrics_port,
        ws_config,
    })
}

fn hash_content(content: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    hasher.finish()
}

fn load_config_source(path: &str, kind: &str) -> Result<(String, u64), String> {
    let toml_str =
        std::fs::read_to_string(path).map_err(|e| format!("Cannot read {kind} {path}: {e}"))?;
    let source_hash = hash_content(&toml_str);
    Ok((toml_str, source_hash))
}

fn load_index_spec_source(index_spec_path: &str) -> Result<(String, u64), String> {
    load_config_source(index_spec_path, "index spec")
}

fn load_options_config_source(path: &str) -> Result<(String, u64), String> {
    load_config_source(path, "options config")
}

fn parse_index_spec(toml_str: &str) -> Result<IndexSpec, String> {
    let spec: IndexSpec =
        toml::from_str(toml_str).map_err(|e| format!("Invalid index spec: {e}"))?;
    spec.validate()
        .map_err(|e| format!("Invalid index spec: {e}"))?;
    Ok(spec)
}

fn build_config_snapshot(index_spec_path: &str) -> Result<ConfigSnapshot, String> {
    let (toml_str, source_hash) = load_index_spec_source(index_spec_path)?;
    let spec = parse_index_spec(&toml_str)?;
    Ok(ConfigSnapshot { spec, source_hash })
}

fn parse_options_config(toml_str: &str) -> Result<OptionsConfig, String> {
    toml::from_str(toml_str).map_err(|e| format!("Invalid options config: {e}"))
}

fn build_options_snapshot(path: &str) -> Result<OptionsSnapshot, String> {
    let (toml_str, source_hash) = load_options_config_source(path)?;
    let options = parse_options_config(&toml_str)?;
    Ok(OptionsSnapshot {
        options,
        source_hash,
    })
}

fn load_index_spec(index_spec_path: &str) -> IndexSpec {
    build_config_snapshot(index_spec_path)
        .map(|snapshot| snapshot.spec)
        .unwrap_or_else(|e| {
            error!("{e}");
            exit(1);
        })
}

#[cfg(test)]
fn load_options_config(path: &str) -> OptionsConfig {
    build_options_snapshot(path)
        .map(|snapshot| snapshot.options)
        .unwrap_or_else(|e| {
            error!("{e}");
            exit(1);
        })
}

fn effective_url(url_override: Option<&str>, spec: &IndexSpec) -> String {
    url_override
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| spec.default_url.clone())
}

fn classify_spec_update(
    current: &ConfigSnapshot,
    candidate: &ConfigSnapshot,
    url_override: Option<&str>,
) -> Result<SpecUpdateAction, String> {
    if current.spec.name != candidate.spec.name {
        return Err("index spec name cannot change during hot reload".to_owned());
    }

    if current.spec.genesis_hash != candidate.spec.genesis_hash {
        return Err("index spec genesis_hash cannot change during hot reload".to_owned());
    }

    if current.spec == candidate.spec {
        return Ok(SpecUpdateAction::Unchanged);
    }

    let effective_url_changed =
        url_override.is_none() && current.spec.default_url != candidate.spec.default_url;

    let indexing_changed = current.spec.spec_change_blocks != candidate.spec.spec_change_blocks
        || current.spec.index_variant != candidate.spec.index_variant
        || current.spec.keys != candidate.spec.keys
        || current.spec.pallets != candidate.spec.pallets;

    if effective_url_changed || indexing_changed {
        Ok(SpecUpdateAction::RestartIndexer)
    } else {
        Ok(SpecUpdateAction::Unchanged)
    }
}

fn classify_options_update(
    current: &ResolvedArgs,
    candidate: &ResolvedArgs,
) -> OptionsRuntimeUpdate {
    let mut resolved = current.clone();
    let mut action = OptionsUpdateAction::Unchanged;
    let mut ignored_fields = Vec::new();

    if current.url != candidate.url {
        resolved.url = candidate.url.clone();
        action = OptionsUpdateAction::RestartIndexer;
    }

    if current.queue_depth != candidate.queue_depth {
        resolved.queue_depth = candidate.queue_depth;
        action = OptionsUpdateAction::RestartIndexer;
    }

    if current.finalized != candidate.finalized {
        resolved.finalized = candidate.finalized;
        action = OptionsUpdateAction::RestartIndexer;
    }

    resolved.ws_config.max_connections = candidate.ws_config.max_connections;
    resolved.ws_config.max_total_subscriptions = candidate.ws_config.max_total_subscriptions;
    resolved.ws_config.max_subscriptions_per_connection =
        candidate.ws_config.max_subscriptions_per_connection;
    resolved.ws_config.subscription_buffer_size = candidate.ws_config.subscription_buffer_size;
    resolved.ws_config.idle_timeout_secs = candidate.ws_config.idle_timeout_secs;
    resolved.ws_config.max_events_limit = candidate.ws_config.max_events_limit;

    for (field, changed) in [
        ("db_path", current.db_path != candidate.db_path),
        ("db_mode", current.db_mode != candidate.db_mode),
        (
            "db_cache_capacity",
            current.db_cache_capacity != candidate.db_cache_capacity,
        ),
        (
            "subscription_control_buffer_size",
            current.ws_config.subscription_control_buffer_size
                != candidate.ws_config.subscription_control_buffer_size,
        ),
        ("port", current.port != candidate.port),
        (
            "metrics_port",
            current.metrics_port != candidate.metrics_port,
        ),
    ] {
        if changed {
            ignored_fields.push(field);
        }
    }

    OptionsRuntimeUpdate {
        resolved,
        action,
        ignored_fields,
    }
}

fn resolve_options_runtime_update(
    run_args: &RunArgs,
    current_spec: &ConfigSnapshot,
    current_resolved: &ResolvedArgs,
    snapshot: &OptionsSnapshot,
) -> Result<OptionsRuntimeUpdate, String> {
    let candidate = resolve_args(run_args, &current_spec.spec, Some(&snapshot.options))?;
    Ok(classify_options_update(current_resolved, &candidate))
}

fn log_ignored_options_fields(fields: &[&'static str]) {
    if fields.is_empty() {
        return;
    }

    warn!(
        "Ignoring hot-reloaded options changes for startup-only fields: {}",
        fields.join(", ")
    );
}

fn event_targets_path(event: &Event, target_path: &Path) -> bool {
    let Some(target_name) = target_path.file_name() else {
        return false;
    };

    event.paths.iter().any(|path| {
        path == target_path
            || path
                .file_name()
                .is_some_and(|file_name| file_name == target_name)
    })
}

async fn watch_runtime_config(
    index_spec_path: PathBuf,
    initial_spec_snapshot: ConfigSnapshot,
    options_config_path: Option<PathBuf>,
    initial_options_snapshot: Option<OptionsSnapshot>,
    url_override: Option<String>,
    spec_snapshot_tx: watch::Sender<SpecUpdate>,
    options_snapshot_tx: watch::Sender<Option<OptionsSnapshot>>,
    ready_tx: Option<oneshot::Sender<()>>,
    mut exit_rx: watch::Receiver<bool>,
) -> Result<(), IndexError> {
    let mut watch_dirs = HashSet::new();
    watch_dirs.insert(
        index_spec_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(".")),
    );
    if let Some(options_config_path) = options_config_path.as_ref() {
        watch_dirs.insert(
            options_config_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
        );
    }
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let mut watcher = RecommendedWatcher::new(
        move |result| {
            let _ = event_tx.send(result);
        },
        notify::Config::default(),
    )
    .map_err(|e| internal_error(format!("failed to create config watcher: {e}")))?;
    for watch_dir in &watch_dirs {
        watcher
            .watch(watch_dir, RecursiveMode::NonRecursive)
            .map_err(|e| {
                internal_error(format!("failed to watch {}: {e}", watch_dir.display()))
            })?;
    }
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    let mut current_spec_snapshot = initial_spec_snapshot;
    let mut last_seen_spec_source_hash = Some(current_spec_snapshot.source_hash);
    let mut last_seen_options_source_hash = initial_options_snapshot
        .as_ref()
        .map(|snapshot| snapshot.source_hash);

    loop {
        tokio::select! {
            _ = exit_rx.changed() => return Ok(()),
            maybe_event = event_rx.recv() => {
                let Some(result) = maybe_event else {
                    return Err(internal_error("config watcher channel closed"));
                };

                let event = match result {
                    Ok(event) => event,
                    Err(err) => {
                        warn!("Config watcher error: {err}");
                        continue;
                    }
                };

                let mut spec_event_seen = event_targets_path(&event, &index_spec_path);
                let mut options_event_seen = options_config_path
                    .as_ref()
                    .is_some_and(|path| event_targets_path(&event, path));

                if !spec_event_seen && !options_event_seen {
                    continue;
                }

                sleep(Duration::from_millis(200)).await;
                while let Ok(result) = event_rx.try_recv() {
                    match result {
                        Ok(event) => {
                            spec_event_seen |= event_targets_path(&event, &index_spec_path);
                            options_event_seen |= options_config_path
                                .as_ref()
                                .is_some_and(|path| event_targets_path(&event, path));
                        }
                        Err(err) => warn!("Config watcher error: {err}"),
                    }
                }

                if spec_event_seen {
                    let (toml_str, source_hash) = match load_index_spec_source(index_spec_path.to_str().ok_or_else(|| {
                        internal_error("index spec path is not valid UTF-8")
                    })?) {
                        Ok(source) => source,
                        Err(err) => {
                            warn!("{err}");
                            continue;
                        }
                    };

                    if last_seen_spec_source_hash != Some(source_hash) {
                        last_seen_spec_source_hash = Some(source_hash);

                        let candidate = match parse_index_spec(&toml_str) {
                            Ok(spec) => ConfigSnapshot { spec, source_hash },
                            Err(err) => {
                                warn!("{err}");
                                continue;
                            }
                        };

                        match classify_spec_update(&current_spec_snapshot, &candidate, url_override.as_deref()) {
                            Ok(action) => {
                                current_spec_snapshot = candidate.clone();
                                if action == SpecUpdateAction::RestartIndexer {
                                    info!("Accepted index spec change; restarting indexer loop.");
                                }
                                let _ = spec_snapshot_tx.send(SpecUpdate {
                                    snapshot: candidate,
                                    action,
                                });
                            }
                            Err(reason) => {
                                warn!("Rejected index spec change: {reason}");
                            }
                        }
                    }
                }

                if options_event_seen {
                    let Some(options_config_path) = options_config_path.as_ref() else {
                        continue;
                    };
                    let snapshot = match build_options_snapshot(options_config_path.to_str().ok_or_else(|| {
                        internal_error("options config path is not valid UTF-8")
                    })?) {
                        Ok(snapshot) => snapshot,
                        Err(err) => {
                            warn!("{err}");
                            continue;
                        }
                    };

                    if last_seen_options_source_hash != Some(snapshot.source_hash) {
                        last_seen_options_source_hash = Some(snapshot.source_hash);
                        let _ = options_snapshot_tx.send(Some(snapshot));
                    }
                }
            }
        }
    }
}

fn validate_chain_name(chain_name: &str) -> Result<(), IndexError> {
    match Path::new(chain_name).components().next() {
        Some(Component::Normal(component))
            if Path::new(component) == Path::new(chain_name)
                && !component.is_empty() =>
        {
            Ok(())
        }
        _ => Err(internal_error(format!(
            "invalid chain name '{chain_name}': path separators and traversal are not allowed"
        ))),
    }
}

fn validate_db_path(path: &str) -> Result<(), IndexError> {
    if Path::new(path)
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(internal_error(format!(
            "invalid database path '{path}': parent directory traversal is not allowed"
        )));
    }

    Ok(())
}

fn parse_db_path_arg(path: &str) -> Result<String, String> {
    validate_db_path(path).map_err(|err| err.to_string())?;
    Ok(path.to_owned())
}

fn resolve_db_path(chain_name: &str, db_path: Option<&str>) -> Result<PathBuf, IndexError> {
    match db_path {
        Some(path) => {
            validate_db_path(path)?;
            Ok(PathBuf::from(path))
        }
        None => {
            validate_chain_name(chain_name)?;

            match home::home_dir() {
                Some(mut p) => {
                    p.push(".local/share/acuity-index");
                    p.push(chain_name);
                    p.push("db");
                    Ok(p)
                }
                None => {
                    error!("No home directory.");
                    exit(1);
                }
            }
        }
    }
}

fn purge_index(args: &PurgeIndexArgs) -> ! {
    let spec = load_index_spec(&args.index_spec);
    let db_path = resolve_db_path(&spec.name, args.db_path.as_deref()).unwrap_or_else(|err| {
        error!("{err}");
        exit(1);
    });

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
    result: Result<Result<(), IndexError>, tokio::task::JoinError>,
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

fn parse_db_cache_capacity(value: &str) -> Result<u64, IndexError> {
    let bytes = Byte::parse_str(value, true).map_err(|err| {
        internal_error(format!("invalid db cache capacity '{value}': {err}"))
    })?;
    bytes.as_u64_checked().ok_or_else(|| {
        internal_error(format!("db cache capacity '{value}' does not fit in u64"))
    })
}

fn init_db_genesis(
    trees: &Trees,
    genesis_hash_config: &[u8],
) -> Result<Vec<u8>, IndexError> {
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
    IndexError,
> {
    let rpc_client = RpcClient::from_url(url).await?;
    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone()).await?;
    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client);
    Ok((api, rpc))
}

async fn run() -> Result<(), IndexError> {
    let args = Args::parse_from(normalize_args(std::env::args()));
    let log_level = args.verbose.log_level_filter().as_trace();
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let run_args = match &args.command {
        Command::Run { args: run_args } => run_args,
        Command::PurgeIndex { args: purge_args } => purge_index(purge_args),
        Command::GenerateIndexSpec {
            index_spec,
            url,
            force,
        } => {
            match write_generated_index_spec(url, PathBuf::from(index_spec).as_path(), *force).await
            {
                Ok(spec) => {
                    info!("Generated index spec for {} at {}", spec.name, index_spec);
                    exit(0);
                }
                Err(err) => {
                    error!("Failed to generate index spec: {err}");
                    exit(1);
                }
            }
        }
    };

    let initial_snapshot = build_config_snapshot(&run_args.index_spec).unwrap_or_else(|e| {
        error!("{e}");
        exit(1);
    });
    let spec = initial_snapshot.spec.clone();
    let initial_options_snapshot = run_args.options_config.as_deref().map(|p| {
        build_options_snapshot(p).unwrap_or_else(|e| {
            error!("{e}");
            exit(1);
        })
    });
    let mut resolved = resolve_args(
        run_args,
        &spec,
        initial_options_snapshot
            .as_ref()
            .map(|snapshot| &snapshot.options),
    )
    .unwrap_or_else(|e| {
        error!("{e}");
        exit(1);
    });

    info!("Indexing chain: {}", spec.name);

    let genesis_hash_config = spec
        .genesis_hash_bytes()
        .map_err(|e| internal_error(format!("invalid genesis hash in config: {e}")))?;

    let db_path = resolve_db_path(&spec.name, resolved.db_path.as_deref())?;
    info!("Database path: {}", db_path.display());

    let db_cache_capacity = parse_db_cache_capacity(&resolved.db_cache_capacity)?;

    let db_config = sled::Config::new()
        .path(db_path)
        .mode(resolved.db_mode.clone().into())
        .cache_capacity(db_cache_capacity);

    let trees = Trees::open(db_config)?;

    let stored_genesis = init_db_genesis(&trees, genesis_hash_config.as_ref())?;
    if stored_genesis != genesis_hash_config {
        return Err(internal_error(format!(
            "database genesis hash mismatch. expected 0x{}, stored 0x{}",
            hex::encode(genesis_hash_config),
            hex::encode(stored_genesis),
        )));
    }

    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        flag::register(*sig, Arc::clone(&term_now))?;
    }

    let metrics = Arc::new(Metrics::new());
    let live_ws_config = LiveWsConfig::from(&resolved.ws_config);
    let (live_ws_config_tx, live_ws_config_rx) = watch::channel(live_ws_config);
    let mut current_live_ws_config = live_ws_config;
    let runtime = Arc::new(RuntimeState::with_metrics(
        resolved.ws_config.max_total_subscriptions,
        metrics.clone(),
    ));
    runtime.set_finalized_mode(resolved.finalized);
    let (process_exit_tx, process_exit_rx) = watch::channel(false);
    let (spec_update_tx, mut spec_update_rx) = watch::channel(SpecUpdate {
        snapshot: initial_snapshot.clone(),
        action: SpecUpdateAction::Unchanged,
    });
    let (options_update_tx, mut options_update_rx) =
        watch::channel(initial_options_snapshot.clone());
    let (sub_tx, mut sub_rx) =
        mpsc::channel(resolved.ws_config.subscription_control_buffer_size.max(1));

    let subscriptions_runtime = runtime.clone();
    let subscriptions_ws_config = live_ws_config_rx.clone();
    let _subscription_task = spawn(async move {
        while let Some(msg) = sub_rx.recv().await {
            if let Err(err) = process_sub_msg(
                subscriptions_runtime.as_ref(),
                &*subscriptions_ws_config.borrow(),
                msg,
            ) {
                error!("Subscription rejected: {err}");
            }
        }
    });

    let mut ws_task = Some(spawn(websockets_listen(
        trees.clone(),
        runtime.clone(),
        resolved.port,
        process_exit_rx.clone(),
        sub_tx,
        live_ws_config_rx.clone(),
    )));
    let mut metrics_task = match resolved.metrics_port {
        Some(metrics_port) => {
            let listener = TcpListener::bind(("0.0.0.0", metrics_port)).await?;
            Some(spawn(metrics_listen(
                listener,
                trees.clone(),
                metrics.clone(),
                process_exit_rx.clone(),
            )))
        }
        None => None,
    };
    let watcher_task = spawn(watch_runtime_config(
        PathBuf::from(&run_args.index_spec),
        initial_snapshot.clone(),
        run_args.options_config.as_ref().map(PathBuf::from),
        initial_options_snapshot.clone(),
        resolved.url.clone(),
        spec_update_tx,
        options_update_tx,
        None,
        process_exit_rx.clone(),
    ));
    tokio::pin!(watcher_task);

    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut signals = Signals::new(TERM_SIGNALS)?;
    let mut current_snapshot = initial_snapshot;
    let options_hot_reload_enabled = initial_options_snapshot.is_some();

    loop {
        if term_now.load(Ordering::Relaxed) {
            runtime.set_api(None);
            runtime.set_rpc(None);
            metrics.set_rpc_connected(false);
            let _ = process_exit_tx.send(true);
            if let Some(ws_task) = ws_task.take() {
                let _ = ws_task.await;
            }
            if let Some(metrics_task) = metrics_task.take() {
                let _ = metrics_task.await;
            }
            let _ = watcher_task.as_mut().await;
            let _ = trees.flush();
            info!("Shutdown requested; exiting.");
            return Ok(());
        }

        let spec = current_snapshot.spec.clone();
        let url = effective_url(resolved.url.as_deref(), &spec);
        info!("Connecting to {url}");

        let (api, rpc) = match select! {
            _ = signals.next() => {
                runtime.set_api(None);
                runtime.set_rpc(None);
                metrics.set_rpc_connected(false);
                let _ = process_exit_tx.send(true);
                if let Some(ws_task) = ws_task.take() {
                    let _ = ws_task.await;
                }
                if let Some(metrics_task) = metrics_task.take() {
                    let _ = metrics_task.await;
                }
                let _ = watcher_task.as_mut().await;
                let _ = trees.flush();
                info!("Shutdown complete.");
                return Ok(());
            }
            changed = spec_update_rx.changed() => {
                if changed.is_ok() {
                    current_snapshot = spec_update_rx.borrow_and_update().snapshot.clone();
                    continue;
                }
                return Err(internal_error("index spec update channel closed"));
            }
            changed = options_update_rx.changed(), if options_hot_reload_enabled => {
                if changed.is_ok() {
                    let Some(snapshot) = options_update_rx.borrow_and_update().clone() else {
                        continue;
                    };
                    match resolve_options_runtime_update(run_args, &current_snapshot, &resolved, &snapshot) {
                        Ok(update) => {
                            log_ignored_options_fields(&update.ignored_fields);
                            resolved = update.resolved;
                            runtime.set_finalized_mode(resolved.finalized);
                            let next_live_ws_config = LiveWsConfig::from(&resolved.ws_config);
                            if next_live_ws_config != current_live_ws_config {
                                current_live_ws_config = next_live_ws_config;
                                let _ = live_ws_config_tx.send(next_live_ws_config);
                            }
                            continue;
                        }
                        Err(err) => {
                            warn!("Rejected options config change: {err}");
                            continue;
                        }
                    }
                }
                return Err(internal_error("options config update channel closed"));
            }
            watcher_result = &mut watcher_task => {
                match watcher_result {
                    Ok(Ok(())) => {
                        return Err(internal_error("config watcher stopped unexpectedly"));
                    }
                    Ok(Err(err)) => return Err(err),
                    Err(join_err) => {
                        return Err(internal_error(format!(
                            "config watcher task panicked: {join_err}"
                        )));
                    }
                }
            }
            result = connect_rpc(&url) => result,
        } {
            Ok(clients) => clients,
            Err(err) if err.is_recoverable() => {
                runtime.set_api(None);
                runtime.set_rpc(None);
                metrics.set_rpc_connected(false);
                metrics.inc_reconnects();
                error!("RPC connection failed: {err}; retrying in {backoff_secs}s");
                select! {
                    _ = signals.next() => {
                        let _ = process_exit_tx.send(true);
                        if let Some(ws_task) = ws_task.take() {
                            let _ = ws_task.await;
                        }
                        if let Some(metrics_task) = metrics_task.take() {
                            let _ = metrics_task.await;
                        }
                        let _ = watcher_task.as_mut().await;
                        let _ = trees.flush();
                        info!("Shutdown requested during reconnection backoff.");
                        return Ok(());
                    }
                    changed = spec_update_rx.changed() => {
                        if changed.is_ok() {
                            current_snapshot = spec_update_rx.borrow_and_update().snapshot.clone();
                            continue;
                        }
                        return Err(internal_error("index spec update channel closed"));
                    }
                    changed = options_update_rx.changed(), if options_hot_reload_enabled => {
                        if changed.is_ok() {
                            let Some(snapshot) = options_update_rx.borrow_and_update().clone() else {
                                continue;
                            };
                            match resolve_options_runtime_update(run_args, &current_snapshot, &resolved, &snapshot) {
                                Ok(update) => {
                                    log_ignored_options_fields(&update.ignored_fields);
                                    resolved = update.resolved;
                                    runtime.set_finalized_mode(resolved.finalized);
                                    let next_live_ws_config = LiveWsConfig::from(&resolved.ws_config);
                                    if next_live_ws_config != current_live_ws_config {
                                        current_live_ws_config = next_live_ws_config;
                                        let _ = live_ws_config_tx.send(next_live_ws_config);
                                    }
                                    continue;
                                }
                                Err(err) => {
                                    warn!("Rejected options config change: {err}");
                                    continue;
                                }
                            }
                        }
                        return Err(internal_error("options config update channel closed"));
                    }
                    watcher_result = &mut watcher_task => {
                        match watcher_result {
                            Ok(Ok(())) => {
                                return Err(internal_error("config watcher stopped unexpectedly"));
                            }
                            Ok(Err(err)) => return Err(err),
                            Err(join_err) => {
                                return Err(internal_error(format!(
                                    "config watcher task panicked: {join_err}"
                                )));
                            }
                        }
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
            return Err(internal_error(format!(
                "chain genesis hash mismatch. expected 0x{}, chain 0x{}",
                hex::encode(genesis_hash_config),
                hex::encode(chain_genesis),
            )));
        }

        backoff_secs = INITIAL_BACKOFF_SECS;
        runtime.set_api(Some(api.clone()));
        runtime.set_rpc(Some(rpc.clone()));
        metrics.set_rpc_connected(true);
        let (indexer_exit_tx, indexer_exit_rx) = watch::channel(false);

        let indexer_handle = spawn(run_indexer(
            trees.clone(),
            api,
            rpc,
            spec.clone(),
            resolved.finalized,
            resolved.queue_depth.into(),
            indexer_exit_rx,
            runtime.clone(),
        ));
        tokio::pin!(indexer_handle);

        enum LoopControl {
            Shutdown,
            Continue,
            Restart,
            Indexer(Result<Result<(), IndexError>, tokio::task::JoinError>),
        }

        let loop_control = select! {
            _ = signals.next() => {
                runtime.set_api(None);
                runtime.set_rpc(None);
                metrics.set_rpc_connected(false);
                let _ = indexer_exit_tx.send(true);
                let _ = process_exit_tx.send(true);
                let _ = indexer_handle.as_mut().await;
                if let Some(ws_task) = ws_task.take() {
                    let _ = ws_task.await;
                }
                if let Some(metrics_task) = metrics_task.take() {
                    let _ = metrics_task.await;
                }
                let _ = watcher_task.as_mut().await;
                let _ = trees.flush();
                info!("Shutdown complete.");
                LoopControl::Shutdown
            }
            changed = spec_update_rx.changed() => {
                if changed.is_ok() {
                    let update = spec_update_rx.borrow_and_update().clone();
                    current_snapshot = update.snapshot.clone();
                    if update.action == SpecUpdateAction::RestartIndexer {
                        runtime.set_api(None);
                        runtime.set_rpc(None);
                        metrics.set_rpc_connected(false);
                        let _ = indexer_exit_tx.send(true);
                        LoopControl::Restart
                    } else {
                        LoopControl::Continue
                    }
                } else {
                    return Err(internal_error("index spec update channel closed"));
                }
            }
            changed = options_update_rx.changed(), if options_hot_reload_enabled => {
                if changed.is_ok() {
                    if let Some(snapshot) = options_update_rx.borrow_and_update().clone() {
                        match resolve_options_runtime_update(
                            run_args,
                            &current_snapshot,
                            &resolved,
                            &snapshot,
                        ) {
                            Ok(update) => {
                                log_ignored_options_fields(&update.ignored_fields);
                                resolved = update.resolved;
                                runtime.set_finalized_mode(resolved.finalized);
                                let next_live_ws_config = LiveWsConfig::from(&resolved.ws_config);
                                if next_live_ws_config != current_live_ws_config {
                                    current_live_ws_config = next_live_ws_config;
                                    let _ = live_ws_config_tx.send(next_live_ws_config);
                                }
                                if update.action == OptionsUpdateAction::RestartIndexer {
                                    runtime.set_api(None);
                                    runtime.set_rpc(None);
                                    metrics.set_rpc_connected(false);
                                    let _ = indexer_exit_tx.send(true);
                                    LoopControl::Restart
                                } else {
                                    LoopControl::Continue
                                }
                            }
                            Err(err) => {
                                warn!("Rejected options config change: {err}");
                                LoopControl::Continue
                            }
                        }
                    } else {
                        LoopControl::Continue
                    }
                } else {
                    return Err(internal_error("options config update channel closed"));
                }
            }
            watcher_result = &mut watcher_task => {
                match watcher_result {
                    Ok(Ok(())) => {
                        error!("Config watcher stopped unexpectedly.");
                        runtime.set_api(None);
                        runtime.set_rpc(None);
                        metrics.set_rpc_connected(false);
                        let _ = indexer_exit_tx.send(true);
                        let _ = process_exit_tx.send(true);
                        let _ = indexer_handle.as_mut().await;
                        if let Some(ws_task) = ws_task.take() {
                            let _ = ws_task.await;
                        }
                        if let Some(metrics_task) = metrics_task.take() {
                            let _ = metrics_task.await;
                        }
                        let _ = trees.flush();
                        return Err(internal_error("config watcher stopped unexpectedly"));
                    }
                    Ok(Err(err)) => {
                        runtime.set_api(None);
                        runtime.set_rpc(None);
                        metrics.set_rpc_connected(false);
                        let _ = indexer_exit_tx.send(true);
                        let _ = process_exit_tx.send(true);
                        let _ = indexer_handle.as_mut().await;
                        if let Some(ws_task) = ws_task.take() {
                            let _ = ws_task.await;
                        }
                        if let Some(metrics_task) = metrics_task.take() {
                            let _ = metrics_task.await;
                        }
                        let _ = trees.flush();
                        return Err(err);
                    }
                    Err(join_err) => {
                        runtime.set_api(None);
                        runtime.set_rpc(None);
                        metrics.set_rpc_connected(false);
                        let _ = indexer_exit_tx.send(true);
                        let _ = process_exit_tx.send(true);
                        let _ = indexer_handle.as_mut().await;
                        if let Some(ws_task) = ws_task.take() {
                            let _ = ws_task.await;
                        }
                        if let Some(metrics_task) = metrics_task.take() {
                            let _ = metrics_task.await;
                        }
                        let _ = trees.flush();
                        return Err(internal_error(format!(
                            "config watcher task panicked: {join_err}"
                        )));
                    }
                }
            }
            result = &mut indexer_handle => LoopControl::Indexer(result),
        };

        let _ = trees.flush();

        match loop_control {
            LoopControl::Shutdown => return Ok(()),
            LoopControl::Continue => continue,
            LoopControl::Restart => match indexer_handle.as_mut().await {
                Ok(Ok(())) => {
                    info!("Indexer stopped for index spec reload.");
                    continue;
                }
                Ok(Err(err)) if err.is_recoverable() => {
                    warn!("Indexer stopped during spec reload with recoverable error: {err}");
                    continue;
                }
                Ok(Err(err)) => {
                    let _ = process_exit_tx.send(true);
                    if let Some(ws_task) = ws_task.take() {
                        let _ = ws_task.await;
                    }
                    if let Some(metrics_task) = metrics_task.take() {
                        let _ = metrics_task.await;
                    }
                    error!("Indexer error (fatal): {err}");
                    return Err(err);
                }
                Err(join_err) => {
                    let _ = process_exit_tx.send(true);
                    if let Some(ws_task) = ws_task.take() {
                        let _ = ws_task.await;
                    }
                    if let Some(metrics_task) = metrics_task.take() {
                        let _ = metrics_task.await;
                    }
                    error!("Indexer task failed: {join_err}");
                    return Err(internal_error(format!(
                        "indexer task panicked: {join_err}"
                    )));
                }
            },
            LoopControl::Indexer(indexer_result) => match indexer_result {
                Ok(Ok(())) => {
                    runtime.set_api(None);
                    runtime.set_rpc(None);
                    metrics.set_rpc_connected(false);
                    let _ = process_exit_tx.send(true);
                    if let Some(ws_task) = ws_task.take() {
                        let _ = ws_task.await;
                    }
                    if let Some(metrics_task) = metrics_task.take() {
                        let _ = metrics_task.await;
                    }
                    let _ = watcher_task.as_mut().await;
                    info!("Indexer stopped cleanly.");
                    return Ok(());
                }
                Ok(Err(err)) if err.is_recoverable() => {
                    runtime.set_api(None);
                    runtime.set_rpc(None);
                    metrics.set_rpc_connected(false);
                    metrics.inc_reconnects();
                    warn!("Indexer error (recoverable): {err}; reconnecting in {backoff_secs}s");
                    select! {
                        _ = signals.next() => {
                            let _ = process_exit_tx.send(true);
                            if let Some(ws_task) = ws_task.take() {
                                let _ = ws_task.await;
                            }
                            if let Some(metrics_task) = metrics_task.take() {
                                let _ = metrics_task.await;
                            }
                            let _ = watcher_task.as_mut().await;
                            let _ = trees.flush();
                            info!("Shutdown requested during reconnection backoff.");
                            return Ok(());
                        }
                        changed = spec_update_rx.changed() => {
                            if changed.is_ok() {
                                current_snapshot = spec_update_rx.borrow_and_update().snapshot.clone();
                                continue;
                            }
                            return Err(internal_error("index spec update channel closed"));
                        }
                        changed = options_update_rx.changed(), if options_hot_reload_enabled => {
                            if changed.is_ok() {
                                let Some(snapshot) = options_update_rx.borrow_and_update().clone() else {
                                    continue;
                                };
                                match resolve_options_runtime_update(run_args, &current_snapshot, &resolved, &snapshot) {
                                    Ok(update) => {
                                        log_ignored_options_fields(&update.ignored_fields);
                                        resolved = update.resolved;
                                        runtime.set_finalized_mode(resolved.finalized);
                                        let next_live_ws_config = LiveWsConfig::from(&resolved.ws_config);
                                        if next_live_ws_config != current_live_ws_config {
                                            current_live_ws_config = next_live_ws_config;
                                            let _ = live_ws_config_tx.send(next_live_ws_config);
                                        }
                                        continue;
                                    }
                                    Err(err) => {
                                        warn!("Rejected options config change: {err}");
                                        continue;
                                    }
                                }
                            }
                            return Err(internal_error("options config update channel closed"));
                        }
                        watcher_result = &mut watcher_task => {
                            match watcher_result {
                                Ok(Ok(())) => {
                                    return Err(internal_error("config watcher stopped unexpectedly"));
                                }
                                Ok(Err(err)) => return Err(err),
                                Err(join_err) => {
                                    return Err(internal_error(format!(
                                        "config watcher task panicked: {join_err}"
                                    )));
                                }
                            }
                        }
                        _ = sleep(Duration::from_secs(backoff_secs)) => {}
                    }
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }
                Ok(Err(err)) => {
                    runtime.set_api(None);
                    runtime.set_rpc(None);
                    metrics.set_rpc_connected(false);
                    let _ = process_exit_tx.send(true);
                    if let Some(ws_task) = ws_task.take() {
                        let _ = ws_task.await;
                    }
                    if let Some(metrics_task) = metrics_task.take() {
                        let _ = metrics_task.await;
                    }
                    let _ = watcher_task.as_mut().await;
                    error!("Indexer error (fatal): {err}");
                    return Err(err);
                }
                Err(join_err) => {
                    runtime.set_api(None);
                    runtime.set_rpc(None);
                    metrics.set_rpc_connected(false);
                    let _ = process_exit_tx.send(true);
                    if let Some(ws_task) = ws_task.take() {
                        let _ = ws_task.await;
                    }
                    if let Some(metrics_task) = metrics_task.take() {
                        let _ = metrics_task.await;
                    }
                    let _ = watcher_task.as_mut().await;
                    error!("Indexer task failed: {join_err}");
                    return Err(internal_error(format!(
                        "indexer task panicked: {join_err}"
                    )));
                }
            },
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
    use tokio::time::timeout;

    const TEST_INDEX_CONFIG: &str = "/tmp/test-index.toml";

    fn test_spec() -> IndexSpec {
        IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            keys: Default::default(),
            pallets: vec![],
        }
    }

    fn test_run_args() -> RunArgs {
        RunArgs::try_parse_from(["acuity-index", TEST_INDEX_CONFIG]).unwrap()
    }

    fn test_snapshot(spec: IndexSpec) -> ConfigSnapshot {
        ConfigSnapshot {
            spec,
            source_hash: 1,
        }
    }

    fn write_spec(path: &Path, spec: &IndexSpec) {
        std::fs::write(path, toml::to_string(spec).unwrap()).unwrap();
    }

    fn write_options(path: &Path, options: &str) {
        std::fs::write(path, options).unwrap();
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
    fn args_parse_run_defaults() {
        let args = Args::try_parse_from(normalize_args([
            "acuity-index".to_string(),
            "run".to_string(),
            TEST_INDEX_CONFIG.to_string(),
        ]))
        .unwrap();
        match args.command {
            Command::Run { args: run_args } => {
                assert_eq!(run_args.index_spec, TEST_INDEX_CONFIG);
                assert!(run_args.db_mode.is_none());
                assert!(run_args.db_cache_capacity.is_none());
                assert!(run_args.queue_depth.is_none());
                assert!(!run_args.finalized);
                assert!(run_args.port.is_none());
                assert!(run_args.metrics_port.is_none());
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn resolve_args_uses_defaults_when_no_config() {
        let args = test_run_args();
        let spec = test_spec();
        let resolved = resolve_args(&args, &spec, None).unwrap();
        assert!(matches!(resolved.db_mode, DbMode::LowSpace));
        assert_eq!(resolved.db_cache_capacity, "1024.00 MiB");
        assert_eq!(resolved.queue_depth, 1);
        assert!(!resolved.finalized);
        assert_eq!(resolved.port, 8172);
        assert_eq!(resolved.metrics_port, None);

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
        let args = test_run_args();
        let spec = test_spec();
        let opts = OptionsConfig {
            url: Some("ws://custom:9944".into()),
            db_path: Some("/data/db".into()),
            db_mode: Some("high_throughput".into()),
            db_cache_capacity: Some("2 GiB".into()),
            queue_depth: Some(4),
            finalized: Some(true),
            port: Some(9999),
            metrics_port: Some(9998),
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
        assert_eq!(resolved.port, 9999);
        assert_eq!(resolved.metrics_port, Some(9998));
        assert_eq!(resolved.url.as_deref(), Some("ws://custom:9944"));
        assert_eq!(resolved.db_path.as_deref(), Some("/data/db"));
    }

    #[test]
    fn resolve_args_cli_overrides_config() {
        let args = RunArgs::try_parse_from([
            "acuity-index",
            TEST_INDEX_CONFIG,
            "--port",
            "1234",
            "--metrics-port",
            "4321",
            "--queue-depth",
            "8",
            "--finalized",
            "--db-mode",
            "high-throughput",
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
            metrics_port: Some(9998),
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
        assert_eq!(resolved.port, 1234);
        assert_eq!(resolved.metrics_port, Some(4321));
    }

    #[test]
    fn resolve_args_invalid_db_mode() {
        let args = test_run_args();
        let spec = test_spec();
        let opts = OptionsConfig {
            db_mode: Some("invalid".into()),
            ..Default::default()
        };
        assert!(resolve_args(&args, &spec, Some(&opts)).is_err());
    }

    #[test]
    fn resolve_args_rejects_zero_websocket_limits() {
        let args = test_run_args();
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
        let args = test_run_args();
        let spec = test_spec();
        let opts = OptionsConfig {
            idle_timeout_secs: Some(0),
            ..Default::default()
        };

        let resolved = resolve_args(&args, &spec, Some(&opts)).unwrap();

        assert_eq!(resolved.ws_config.idle_timeout_secs, 0);
    }

    #[test]
    fn resolve_args_ignores_spec_indexing_flags() {
        let args = test_run_args();
        let mut spec = test_spec();
        spec.index_variant = true;
        let _resolved = resolve_args(&args, &spec, None).unwrap();
    }

    #[test]
    fn classify_spec_update_rejects_name_change() {
        let current = test_snapshot(test_spec());
        let mut next_spec = test_spec();
        next_spec.name = "other".into();
        let next = test_snapshot(next_spec);

        let err = classify_spec_update(&current, &next, None).unwrap_err();

        assert!(err.contains("name"));
    }

    #[test]
    fn classify_spec_update_rejects_genesis_change() {
        let current = test_snapshot(test_spec());
        let mut next_spec = test_spec();
        next_spec.genesis_hash = "11".repeat(32);
        let next = test_snapshot(next_spec);

        let err = classify_spec_update(&current, &next, None).unwrap_err();

        assert!(err.contains("genesis_hash"));
    }

    #[test]
    fn classify_spec_update_ignores_default_url_when_override_present() {
        let current = test_snapshot(test_spec());
        let mut next_spec = test_spec();
        next_spec.default_url = "ws://127.0.0.1:9999".into();
        let next = test_snapshot(next_spec);

        let action = classify_spec_update(&current, &next, Some("ws://override:9944")).unwrap();

        assert_eq!(action, SpecUpdateAction::Unchanged);
    }

    #[test]
    fn classify_spec_update_restarts_for_indexing_changes() {
        let current = test_snapshot(test_spec());
        let mut next_spec = test_spec();
        next_spec.index_variant = true;
        let next = test_snapshot(next_spec);

        let action = classify_spec_update(&current, &next, None).unwrap();

        assert_eq!(action, SpecUpdateAction::RestartIndexer);
    }

    #[test]
    fn classify_options_update_restarts_for_indexer_settings() {
        let spec = test_spec();
        let current = resolve_args(&test_run_args(), &spec, None).unwrap();
        let candidate = resolve_args(
            &test_run_args(),
            &spec,
            Some(&OptionsConfig {
                url: Some("ws://override:9944".into()),
                queue_depth: Some(4),
                finalized: Some(true),
                ..Default::default()
            }),
        )
        .unwrap();

        let update = classify_options_update(&current, &candidate);

        assert_eq!(update.action, OptionsUpdateAction::RestartIndexer);
        assert_eq!(update.resolved.url.as_deref(), Some("ws://override:9944"));
        assert_eq!(update.resolved.queue_depth, 4);
        assert!(update.resolved.finalized);
        assert!(update.ignored_fields.is_empty());
    }

    #[test]
    fn classify_options_update_logs_and_ignores_startup_only_fields() {
        let spec = test_spec();
        let current = resolve_args(&test_run_args(), &spec, None).unwrap();
        let candidate = resolve_args(
            &test_run_args(),
            &spec,
            Some(&OptionsConfig {
                db_path: Some("/tmp/other-db".into()),
                db_mode: Some("high_throughput".into()),
                db_cache_capacity: Some("2 GiB".into()),
                port: Some(9999),
                metrics_port: Some(9998),
                subscription_control_buffer_size: Some(2048),
                ..Default::default()
            }),
        )
        .unwrap();

        let update = classify_options_update(&current, &candidate);

        assert_eq!(update.action, OptionsUpdateAction::Unchanged);
        assert_eq!(update.resolved, current);
        assert_eq!(
            update.ignored_fields,
            vec![
                "db_path",
                "db_mode",
                "db_cache_capacity",
                "subscription_control_buffer_size",
                "port",
                "metrics_port",
            ]
        );
    }

    #[tokio::test]
    async fn watch_runtime_config_detects_spec_replace_via_rename() {
        let dir = tempfile::tempdir().unwrap();
        let spec_path = dir.path().join("index.toml");
        let replacement_path = dir.path().join("index.tmp.toml");
        let initial_spec = test_spec();
        write_spec(&spec_path, &initial_spec);

        let initial_snapshot = build_config_snapshot(spec_path.to_str().unwrap()).unwrap();
        let (spec_tx, mut spec_rx) = watch::channel(SpecUpdate {
            snapshot: initial_snapshot.clone(),
            action: SpecUpdateAction::Unchanged,
        });
        let (options_tx, _options_rx) = watch::channel(None::<OptionsSnapshot>);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_runtime_config(
            spec_path.clone(),
            initial_snapshot,
            None,
            None,
            None,
            spec_tx,
            options_tx,
            Some(ready_tx),
            exit_rx,
        ));
        timeout(Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();

        let mut updated_spec = test_spec();
        updated_spec.index_variant = true;
        write_spec(&replacement_path, &updated_spec);
        std::fs::rename(&replacement_path, &spec_path).unwrap();

        timeout(Duration::from_secs(5), spec_rx.changed())
            .await
            .unwrap()
            .unwrap();
        let update = spec_rx.borrow_and_update().clone();

        assert_eq!(update.action, SpecUpdateAction::RestartIndexer);
        assert!(update.snapshot.spec.index_variant);

        let _ = exit_tx.send(true);
        assert!(watcher.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn watch_runtime_config_detects_options_replace_via_rename() {
        let dir = tempfile::tempdir().unwrap();
        let spec_path = dir.path().join("index.toml");
        let options_path = dir.path().join("options.toml");
        let replacement_path = dir.path().join("options.tmp.toml");
        let initial_spec = test_spec();
        write_spec(&spec_path, &initial_spec);
        write_options(&options_path, "queue_depth = 1\n");

        let initial_snapshot = build_config_snapshot(spec_path.to_str().unwrap()).unwrap();
        let initial_options = build_options_snapshot(options_path.to_str().unwrap()).unwrap();
        let (spec_tx, _spec_rx) = watch::channel(SpecUpdate {
            snapshot: initial_snapshot.clone(),
            action: SpecUpdateAction::Unchanged,
        });
        let (options_tx, mut options_rx) = watch::channel(Some(initial_options.clone()));
        let (ready_tx, ready_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_runtime_config(
            spec_path,
            initial_snapshot,
            Some(options_path.clone()),
            Some(initial_options),
            None,
            spec_tx,
            options_tx,
            Some(ready_tx),
            exit_rx,
        ));
        timeout(Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();

        write_options(&replacement_path, "queue_depth = 4\nmax_connections = 32\n");
        std::fs::rename(&replacement_path, &options_path).unwrap();

        timeout(Duration::from_secs(5), options_rx.changed())
            .await
            .unwrap()
            .unwrap();
        let update = options_rx.borrow_and_update().clone().unwrap();

        assert_eq!(update.options.queue_depth, Some(4));
        assert_eq!(update.options.max_connections, Some(32));

        let _ = exit_tx.send(true);
        assert!(watcher.await.unwrap().is_ok());
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
metrics_port = 9998
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
        assert_eq!(opts.metrics_port, Some(9998));
        assert_eq!(opts.max_connections, Some(2048));
        assert_eq!(opts.max_total_subscriptions, Some(100000));
        assert_eq!(opts.max_subscriptions_per_connection, Some(64));
        assert_eq!(opts.subscription_buffer_size, Some(512));
        assert_eq!(opts.subscription_control_buffer_size, Some(2048));
        assert_eq!(opts.idle_timeout_secs, Some(600));
        assert_eq!(opts.max_events_limit, Some(500));
    }

    #[test]
    fn args_reject_removed_indexing_flags() {
        assert!(
            Args::try_parse_from(["acuity-index", "run", TEST_INDEX_CONFIG, "--store-events",])
                .is_err()
        );
        assert!(
            Args::try_parse_from(["acuity-index", "run", TEST_INDEX_CONFIG, "--index-variant",])
                .is_err()
        );
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
            describe_indexer_shutdown_result(Ok(Err(IndexError::BlockStreamClosed))),
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
    fn args_parse_purge_index_index_spec() {
        let args =
            Args::try_parse_from(["acuity-index", "purge-index", TEST_INDEX_CONFIG]).unwrap();

        match args.command {
            Command::PurgeIndex { args: purge_args } => {
                assert_eq!(purge_args.index_spec, TEST_INDEX_CONFIG);
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
            TEST_INDEX_CONFIG,
            "--db-path",
            "/tmp/test-db",
        ])
        .unwrap();

        match args.command {
            Command::PurgeIndex { args: purge_args } => {
                assert_eq!(purge_args.db_path.as_deref(), Some("/tmp/test-db"));
            }
            _ => panic!("expected purge-index command"),
        }
    }

    #[test]
    fn args_reject_run_db_path_traversal() {
        let err = Args::try_parse_from([
            "acuity-index",
            "run",
            TEST_INDEX_CONFIG,
            "--db-path",
            "db/../other",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn args_reject_purge_index_db_path_traversal() {
        let err = Args::try_parse_from([
            "acuity-index",
            "purge-index",
            TEST_INDEX_CONFIG,
            "--db-path",
            "db/../other",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn args_accept_run_absolute_db_path() {
        let args = Args::try_parse_from([
            "acuity-index",
            "run",
            TEST_INDEX_CONFIG,
            "--db-path",
            "/tmp/test-db",
        ])
        .unwrap();

        match args.command {
            Command::Run { args: run_args } => {
                assert_eq!(run_args.db_path.as_deref(), Some("/tmp/test-db"));
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn args_parse_generate_index_spec_short_force() {
        let args = Args::try_parse_from([
            "acuity-index",
            "generate-index-spec",
            "/tmp/test.toml",
            "--url",
            "wss://rpc.example.com:443",
            "-f",
        ])
        .unwrap();

        match args.command {
            Command::GenerateIndexSpec {
                index_spec,
                url,
                force,
            } => {
                assert_eq!(index_spec, "/tmp/test.toml");
                assert_eq!(url, "wss://rpc.example.com:443");
                assert!(force);
            }
            _ => panic!("expected generate-index-spec command"),
        }
    }

    #[test]
    fn args_parse_generate_index_spec_long_force() {
        let args = Args::try_parse_from([
            "acuity-index",
            "generate-index-spec",
            "/tmp/test.toml",
            "--url",
            "wss://rpc.example.com:443",
            "--force",
        ])
        .unwrap();

        match args.command {
            Command::GenerateIndexSpec { force, .. } => {
                assert!(force);
            }
            _ => panic!("expected generate-index-spec command"),
        }
    }

    #[test]
    fn args_require_run_index_spec() {
        let err = Args::try_parse_from(["acuity-index", "run"]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn args_require_purge_index_spec() {
        let err = Args::try_parse_from(["acuity-index", "purge-index"]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn args_require_generate_index_spec_path() {
        let err = Args::try_parse_from([
            "acuity-index",
            "generate-index-spec",
            "--url",
            "ws://127.0.0.1:9944",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn args_reject_help_subcommand() {
        let err = Args::try_parse_from(["acuity-index", "help"]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn resolve_db_path_uses_chain_name() {
        let path = resolve_db_path("kusama", None).unwrap();

        assert!(path.ends_with(".local/share/acuity-index/kusama/db"));
    }

    #[test]
    fn resolve_db_path_rejects_chain_name_traversal() {
        let err = resolve_db_path("../kusama", None).unwrap_err();

        assert!(err.to_string().contains("invalid chain name"));
    }

    #[test]
    fn resolve_db_path_rejects_db_path_traversal() {
        let err = resolve_db_path("kusama", Some("db/../other")).unwrap_err();

        assert!(err.to_string().contains("invalid database path"));
    }

    #[test]
    fn resolve_db_path_allows_explicit_absolute_path() {
        let path = resolve_db_path("kusama", Some("/tmp/acuity-db")).unwrap();

        assert_eq!(path, PathBuf::from("/tmp/acuity-db"));
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
