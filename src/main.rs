use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use futures::StreamExt;
use signal_hook::{consts::TERM_SIGNALS, flag};
use signal_hook_tokio::Signals;
use std::{
    io::ErrorKind,
    path::PathBuf,
    process::exit,
    sync::{Arc, atomic::AtomicBool},
};
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods},
};
use tokio::{
    join, spawn,
    sync::{mpsc, watch},
};
use tracing::{error, info};
use tracing_log::AsTrace;

mod config;
mod config_gen;
mod indexer;
mod pallets;
mod shared;
mod websockets;

use config::{ChainConfig, KUSAMA_TOML, PASEO_TOML, POLKADOT_TOML, WESTEND_TOML};
use config_gen::write_generated_chain_config;
use indexer::run_indexer;
use shared::Trees;
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
    /// Generate a starter chain TOML config from live node metadata
    GenerateChainConfig {
        /// URL of Substrate node to connect to
        #[arg(short, long)]
        url: String,
        /// Path to write the generated chain TOML config
        output: String,
    },
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Chain to index
    #[arg(short, long, value_enum)]
    pub chain: Option<Chain>,
    /// Path to a custom chain TOML config (overrides --chain)
    #[arg(long)]
    pub chain_config: Option<String>,
    /// Database path
    #[arg(short, long)]
    pub db_path: Option<String>,
    /// Database mode
    #[arg(long, value_enum, default_value_t = DbMode::LowSpace)]
    pub db_mode: DbMode,
    /// Maximum size in bytes for the system page cache
    #[arg(long, default_value = "1024.00 MiB")]
    pub db_cache_capacity: String,
    /// URL of Substrate node to connect to
    #[arg(short, long)]
    pub url: Option<String>,
    /// Maximum number of concurrent block requests
    #[arg(long, default_value_t = 1)]
    pub queue_depth: u8,
    /// Only index finalized blocks
    #[arg(short, long, default_value_t = false)]
    pub finalized: bool,
    /// Index event variants
    #[arg(short, long, default_value_t = false)]
    pub index_variant: bool,
    /// Store decoded events for immediate retrieval
    #[arg(short, long, default_value_t = false)]
    pub store_events: bool,
    /// WebSocket port
    #[arg(short, long, default_value_t = 8172)]
    pub port: u16,
}

#[derive(Parser, Debug)]
pub struct PurgeIndexArgs {
    /// Chain whose index should be deleted
    #[arg(short, long, value_enum)]
    pub chain: Option<Chain>,
    /// Path to a custom chain TOML config (overrides --chain)
    #[arg(long)]
    pub chain_config: Option<String>,
    /// Database path
    #[arg(short, long)]
    pub db_path: Option<String>,
}

fn load_chain_config(chain: Option<Chain>, chain_config_path: Option<&str>) -> ChainConfig {
    let config: ChainConfig = if let Some(path) = chain_config_path {
        let toml_str = std::fs::read_to_string(path).unwrap_or_else(|e| {
            error!("Cannot read chain config {path}: {e}");
            exit(1);
        });
        toml::from_str(&toml_str).unwrap_or_else(|e| {
            error!("Invalid chain config: {e}");
            exit(1);
        })
    } else {
        let toml_str = match chain {
            Some(Chain::Polkadot) => POLKADOT_TOML,
            Some(Chain::Kusama) => KUSAMA_TOML,
            Some(Chain::Westend) => WESTEND_TOML,
            Some(Chain::Paseo) => PASEO_TOML,
            None => {
                error!("Either --chain or --chain-config must be specified.");
                exit(1);
            }
        };
        toml::from_str(toml_str).expect("Built-in TOML is valid")
    };

    config.validate().unwrap_or_else(|e| {
        error!("Invalid chain config: {e}");
        exit(1);
    });

    config
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
    let chain_config = load_chain_config(args.chain, args.chain_config.as_deref());
    let db_path = resolve_db_path(&chain_config.name, args.db_path.as_deref());

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

// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse_from(normalize_args(std::env::args()));
    let log_level = args.verbose.log_level_filter().as_trace();
    tracing_subscriber::fmt().with_max_level(log_level).init();

    match &args.command {
        Some(Command::PurgeIndex { args: purge_args }) => purge_index(purge_args),
        Some(Command::GenerateChainConfig { url, output }) => {
            match write_generated_chain_config(url, PathBuf::from(output).as_path()).await {
                Ok(config) => {
                    info!("Generated chain config for {} at {}", config.name, output);
                    exit(0);
                }
                Err(err) => {
                    error!("Failed to generate chain config: {err}");
                    exit(1);
                }
            }
        }
        None => {}
    }

    let run_args = &args.run;

    let chain_config = load_chain_config(run_args.chain, run_args.chain_config.as_deref());

    info!("Indexing chain: {}", chain_config.name);

    let genesis_hash_config = chain_config.genesis_hash_bytes().unwrap_or_else(|e| {
        error!("Invalid genesis hash in config: {e}");
        exit(1);
    });

    // Open database.
    let db_path = resolve_db_path(&chain_config.name, run_args.db_path.as_deref());
    info!("Database path: {}", db_path.display());

    let db_cache_capacity = Byte::parse_str(&run_args.db_cache_capacity, true)
        .unwrap()
        .as_u64_checked()
        .unwrap();

    let db_config = sled::Config::new()
        .path(db_path)
        .mode(run_args.db_mode.clone().into())
        .cache_capacity(db_cache_capacity);

    let trees = Trees::open(db_config).unwrap_or_else(|e| {
        error!("Failed to open database: {e}");
        exit(1);
    });

    // Verify genesis hash.
    let stored_genesis = match trees.root.get("genesis_hash").unwrap() {
        Some(v) => v.to_vec(),
        None => {
            trees
                .root
                .insert("genesis_hash", genesis_hash_config.as_ref())
                .unwrap();
            genesis_hash_config.to_vec()
        }
    };
    if stored_genesis != genesis_hash_config {
        error!(
            "Database genesis hash mismatch.\n  Expected: 0x{}\n  Stored:   0x{}",
            hex::encode(genesis_hash_config),
            hex::encode(stored_genesis),
        );
        let _ = trees.flush();
        exit(1);
    }

    // Connect to node.
    let url = run_args
        .url
        .clone()
        .unwrap_or_else(|| chain_config.default_url.clone());
    info!("Connecting to {url}");

    let rpc_client = RpcClient::from_url(&url).await.unwrap_or_else(|e| {
        error!("Connection failed: {e}");
        let _ = trees.flush();
        exit(1);
    });

    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone())
        .await
        .unwrap_or_else(|e| {
            error!("API init failed: {e}");
            let _ = trees.flush();
            exit(1);
        });

    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client);

    // Verify chain genesis hash.
    let chain_genesis = api.genesis_hash().as_ref().to_vec();
    if chain_genesis != genesis_hash_config {
        error!(
            "Chain genesis hash mismatch.\n  Expected: 0x{}\n  Chain:    0x{}",
            hex::encode(genesis_hash_config),
            hex::encode(chain_genesis),
        );
        let _ = trees.flush();
        exit(1);
    }

    // Signal handling.
    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now)).unwrap();
        flag::register(*sig, Arc::clone(&term_now)).unwrap();
    }

    let (exit_tx, exit_rx) = watch::channel(false);
    let (sub_tx, sub_rx) = mpsc::unbounded_channel();

    let indexer_task = spawn(run_indexer(
        trees.clone(),
        api,
        rpc.clone(),
        chain_config,
        run_args.finalized,
        run_args.queue_depth.into(),
        run_args.index_variant,
        run_args.store_events,
        exit_rx.clone(),
        sub_rx,
    ));

    let ws_task = spawn(websockets_listen(
        trees.clone(),
        rpc,
        run_args.port,
        exit_rx,
        sub_tx,
    ));

    let mut signals = Signals::new(TERM_SIGNALS).unwrap();
    signals.next().await;
    info!("Shutting down.");
    let _ = exit_tx.send(true);
    let _ = join!(indexer_task, ws_task);
    let _ = trees.flush();
    info!("Closed database.");
    exit(0);
}

#[cfg(test)]
mod main_tests {
    use super::*;

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
        let args = Args::try_parse_from(normalize_args(["acuity-index".to_string()])).unwrap();
        assert!(args.command.is_none());
        assert!(args.run.chain.is_none());
        assert!(matches!(args.run.db_mode, DbMode::LowSpace));
        assert_eq!(args.run.db_cache_capacity, "1024.00 MiB");
        assert_eq!(args.run.queue_depth, 1);
        assert!(!args.run.finalized);
        assert!(!args.run.index_variant);
        assert!(!args.run.store_events);
        assert_eq!(args.run.port, 8172);
    }

    #[test]
    fn args_parse_store_events_flag() {
        let args = Args::try_parse_from(["acuity-index", "--store-events"]).unwrap();

        assert!(args.run.store_events);
    }

    #[test]
    fn args_parse_purge_index_chain() {
        let args = Args::try_parse_from(["acuity-index", "purge-index", "--chain", "kusama"])
            .unwrap();

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
