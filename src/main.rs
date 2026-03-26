use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use futures::StreamExt;
use signal_hook::{consts::TERM_SIGNALS, flag};
use signal_hook_tokio::Signals;
use std::{
    path::PathBuf,
    process::exit,
    sync::{Arc, atomic::AtomicBool},
};
use subxt::{
    config::RpcConfigFor,
    OnlineClient, PolkadotConfig,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods},
};
use tokio::{
    join, spawn,
    sync::{mpsc, watch},
};
use tracing::{error, info};
use tracing_log::AsTrace;

mod config;
mod indexer;
mod pallets;
mod shared;
mod websockets;

use config::{ChainConfig, KUSAMA_TOML, PASEO_TOML, POLKADOT_TOML, WESTEND_TOML};
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

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Args {
    /// Chain to index
    #[arg(short, long, value_enum, default_value_t = Chain::Polkadot)]
    pub chain: Chain,
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
    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let log_level = args.verbose.log_level_filter().as_trace();
    tracing_subscriber::fmt().with_max_level(log_level).init();

    // Load chain config.
    let chain_config: ChainConfig = if let Some(path) = &args.chain_config {
        let toml_str = std::fs::read_to_string(path).unwrap_or_else(|e| {
            error!("Cannot read chain config {path}: {e}");
            exit(1);
        });
        toml::from_str(&toml_str).unwrap_or_else(|e| {
            error!("Invalid chain config: {e}");
            exit(1);
        })
    } else {
        let toml_str = match args.chain {
            Chain::Polkadot => POLKADOT_TOML,
            Chain::Kusama => KUSAMA_TOML,
            Chain::Westend => WESTEND_TOML,
            Chain::Paseo => PASEO_TOML,
        };
        toml::from_str(toml_str).expect("Built-in TOML is valid")
    };

    info!("Indexing chain: {}", chain_config.name);

    let genesis_hash_config = chain_config.genesis_hash_bytes().unwrap_or_else(|e| {
        error!("Invalid genesis hash in config: {e}");
        exit(1);
    });

    // Open database.
    let db_path = match &args.db_path {
        Some(p) => PathBuf::from(p),
        None => match home::home_dir() {
            Some(mut p) => {
                p.push(".local/share/acuity-index");
                p.push(&chain_config.name);
                p.push("db");
                p
            }
            None => {
                error!("No home directory.");
                exit(1);
            }
        },
    };
    info!("Database path: {}", db_path.display());

    let db_cache_capacity: u64 = Byte::from_str(&args.db_cache_capacity)
        .unwrap()
        .get_bytes()
        .try_into()
        .unwrap();

    let db_config = sled::Config::new()
        .path(db_path)
        .mode(args.db_mode.into())
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
    let url = args
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
        args.finalized,
        args.queue_depth.into(),
        args.index_variant,
        args.store_events,
        exit_rx.clone(),
        sub_rx,
    ));

    let ws_task = spawn(websockets_listen(
        trees.clone(),
        rpc,
        args.port,
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
