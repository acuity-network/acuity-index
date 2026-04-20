mod common;

use acuity_index::synthetic_devnet::{
    fetch_genesis_hash, get_events, pick_unused_port, validate_query_expectation,
    wait_for_indexed_tip, wait_for_node,
};
use std::{error::Error, time::Duration};
use tempfile::tempdir;

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn smoke_indexes_synthetic_runtime_events() -> Result<(), Box<dyn Error>> {
    common::build_runtime_release()?;

    let temp = tempdir()?;
    let chain_spec = temp.path().join("synthetic-dev-chain-spec.json");
    common::build_chain_spec(&chain_spec)?;

    let rpc_port = pick_unused_port()?;
    let indexer_port = pick_unused_port()?;
    let node_base = temp.path().join("node");
    let index_db = temp.path().join("db");
    let node_log = temp.path().join("node.log");
    let indexer_log = temp.path().join("indexer.log");
    let manifest_path = temp.path().join("seed-manifest.json");

    let mut node = common::start_node(&chain_spec, rpc_port, &node_base, &node_log)?;
    let node_url = format!("ws://127.0.0.1:{rpc_port}");
    wait_for_node(&node_url, 0, Duration::from_secs(30)).await?;

    let genesis_hash = fetch_genesis_hash(&node_url).await?;
    let config_path = temp.path().join("synthetic.toml");
    common::write_config(&config_path, &node_url, &genesis_hash)?;

    let mut indexer = common::start_indexer(
        &config_path,
        &node_url,
        &index_db,
        indexer_port,
        &indexer_log,
    )?;
    let indexer_url = format!("ws://127.0.0.1:{indexer_port}");

    let manifest = common::run_smoke_seeder(&node_url, &manifest_path)?;
    wait_for_indexed_tip(&indexer_url, manifest.end_block, Duration::from_secs(30)).await?;

    for query in &manifest.queries {
        let limit = u16::try_from(query.min_events.max(16))?;
        let response = get_events(&indexer_url, query.key.clone(), limit).await?;
        validate_query_expectation(query, &response).map_err(std::io::Error::other)?;
    }

    indexer.terminate();
    node.terminate();
    Ok(())
}
