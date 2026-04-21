mod common;

use acuity_index::synthetic_devnet::{
    QueryExpectation, events_len, fetch_genesis_hash, fetch_status, get_events, key_u32,
    pick_unused_port, size_on_disk, spans_cover_tip, validate_query_expectation,
    wait_for_indexed_tip, wait_for_node,
};
use serde_json::{Value, json};
use std::{
    error::Error,
    io,
    time::{Duration, Instant},
};

use common::{
    ConfigOverrides, IndexerOptions, SyntheticStack, WsClient, build_chain_spec,
    build_runtime_release, read_text, run_bulk_seeder, run_smoke_seeder, start_indexer, start_node,
    write_config_with_overrides,
};

fn response_events(response: &Value) -> Result<Vec<(u64, u64)>, Box<dyn Error>> {
    let events = response["data"]["events"]
        .as_array()
        .ok_or_else(|| io::Error::other(format!("missing events array in response: {response}")))?;

    events
        .iter()
        .map(|event| {
            let block_number = event["blockNumber"].as_u64().ok_or_else(|| {
                io::Error::other(format!("missing blockNumber in event: {event}"))
            })?;
            let event_index = event["eventIndex"]
                .as_u64()
                .ok_or_else(|| io::Error::other(format!("missing eventIndex in event: {event}")))?;
            Ok((block_number, event_index))
        })
        .collect()
}

fn find_query<'a>(
    manifest: &'a acuity_index::synthetic_devnet::SeedManifest,
    description: &str,
) -> Result<&'a QueryExpectation, Box<dyn Error>> {
    manifest
        .queries
        .iter()
        .find(|query| query.description == description)
        .ok_or_else(|| {
            io::Error::other(format!("missing query expectation '{description}'")).into()
        })
}

fn find_variant_indexes(
    variants_response: &Value,
    pallet_name: &str,
    event_name: &str,
) -> Result<(u64, u64), Box<dyn Error>> {
    let pallets = variants_response["data"]
        .as_array()
        .ok_or_else(|| io::Error::other(format!("missing variants array: {variants_response}")))?;

    for pallet in pallets {
        if pallet["name"].as_str() != Some(pallet_name) {
            continue;
        }

        let pallet_index = pallet["index"]
            .as_u64()
            .ok_or_else(|| io::Error::other(format!("missing pallet index: {pallet}")))?;
        let events = pallet["events"]
            .as_array()
            .ok_or_else(|| io::Error::other(format!("missing events array: {pallet}")))?;
        for event in events {
            if event["name"].as_str() == Some(event_name) {
                let event_index = event["index"]
                    .as_u64()
                    .ok_or_else(|| io::Error::other(format!("missing event index: {event}")))?;
                return Ok((pallet_index, event_index));
            }
        }
    }

    Err(io::Error::other(format!(
        "missing variant metadata for {pallet_name}::{event_name}"
    ))
    .into())
}

fn variant_key(pallet_index: u64, event_index: u64) -> Value {
    json!({"type": "Variant", "value": [pallet_index, event_index]})
}

async fn wait_for_variants_temporarily_unavailable(
    indexer_url: &str,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let response = acuity_index::synthetic_devnet::request_json_ws(
            indexer_url,
            json!({"id": 500, "type": "Variants"}),
        )
        .await?;

        if response["type"] == "error" && response["data"]["code"] == "temporarily_unavailable" {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for temporarily_unavailable response: {response}"
            ))
            .into());
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_variants_available(
    indexer_url: &str,
    timeout: Duration,
) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let response = acuity_index::synthetic_devnet::request_json_ws(
            indexer_url,
            json!({"id": 501, "type": "Variants"}),
        )
        .await?;

        if response["type"] == "variants" {
            return Ok(response);
        }

        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for variants response: {response}"
            ))
            .into());
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_query_expectation(
    indexer_url: &str,
    query: &QueryExpectation,
    timeout: Duration,
) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    let limit = u16::try_from(query.min_events.max(16))?;

    loop {
        let response = get_events(indexer_url, query.key.clone(), limit).await?;
        if validate_query_expectation(query, &response).is_ok() {
            return Ok(response);
        }

        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for query expectation '{}'",
                query.description
            ))
            .into());
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn different_genesis_hash(actual: &str) -> String {
    let replacement = if actual.starts_with("00") { "11" } else { "00" };
    format!("{replacement}{}", &actual[2..])
}

fn is_sorted_newest_first(events: &[(u64, u64)]) -> bool {
    events.windows(2).all(|window| window[0] >= window[1])
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn smoke_indexes_synthetic_runtime_events() -> Result<(), Box<dyn Error>> {
    let stack =
        SyntheticStack::start(ConfigOverrides::default(), IndexerOptions::default()).await?;
    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("smoke-manifest.json");

    let manifest = run_smoke_seeder(&stack.node_url, &manifest_path)?;
    wait_for_indexed_tip(
        &stack.indexer_url,
        manifest.end_block,
        Duration::from_secs(30),
    )
    .await?;

    for query in &manifest.queries {
        let limit = u16::try_from(query.min_events.max(16))?;
        let response = get_events(&stack.indexer_url, query.key.clone(), limit).await?;
        validate_query_expectation(query, &response).map_err(io::Error::other)?;
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn bulk_seeder_handles_long_instant_seal_runs() -> Result<(), Box<dyn Error>> {
    let stack =
        SyntheticStack::start(ConfigOverrides::default(), IndexerOptions::default()).await?;
    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("bulk-manifest.json");

    let manifest = run_bulk_seeder(&stack.node_url, &manifest_path, 9600, 100, 2)?;
    assert_eq!(manifest.transactions_submitted, 100);
    assert_eq!(manifest.synthetic_event_count, 200);

    wait_for_indexed_tip(
        &stack.indexer_url,
        manifest.end_block,
        Duration::from_secs(60),
    )
    .await?;

    for query in &manifest.queries {
        let limit = u16::try_from(query.min_events.max(16))?;
        let response = get_events(&stack.indexer_url, query.key.clone(), limit).await?;
        validate_query_expectation(query, &response).map_err(io::Error::other)?;
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn api_requests_cover_status_variants_size_and_cursor_pagination()
-> Result<(), Box<dyn Error>> {
    let stack =
        SyntheticStack::start(ConfigOverrides::default(), IndexerOptions::default()).await?;
    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("smoke-manifest.json");

    let manifest = run_smoke_seeder(&stack.node_url, &manifest_path)?;
    wait_for_indexed_tip(
        &stack.indexer_url,
        manifest.end_block,
        Duration::from_secs(30),
    )
    .await?;

    let status = fetch_status(&stack.indexer_url).await?;
    assert_eq!(status["type"], "status");
    assert!(spans_cover_tip(&status, manifest.end_block));

    let variants = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({"id": 2, "type": "Variants"}),
    )
    .await?;
    assert_eq!(variants["type"], "variants");
    let _ = find_variant_indexes(&variants, "Synthetic", "BurstEmitted")?;

    let size = size_on_disk(&stack.indexer_url).await?;
    assert!(size > 0);

    let batch_query = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({
            "id": 3,
            "type": "GetEvents",
            "key": key_u32("batch_id", 77),
        }),
    )
    .await?;
    assert_eq!(batch_query["type"], "events");
    let batch_events = response_events(&batch_query)?;
    assert_eq!(batch_events.len(), 4);
    assert!(is_sorted_newest_first(&batch_events));

    let cursor = &batch_query["data"]["events"][0];
    let before_response = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({
            "id": 4,
            "type": "GetEvents",
            "key": key_u32("batch_id", 77),
            "before": {
                "blockNumber": cursor["blockNumber"],
                "eventIndex": cursor["eventIndex"],
            },
            "limit": 100,
        }),
    )
    .await?;
    let before_events = response_events(&before_response)?;
    assert_eq!(before_events, batch_events[1..].to_vec());

    let burst_query = find_query(&manifest, "burst batch query returns many events")?;
    validate_query_expectation(burst_query, &batch_query).map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn subscriptions_deliver_status_and_event_notifications() -> Result<(), Box<dyn Error>> {
    let stack =
        SyntheticStack::start(ConfigOverrides::default(), IndexerOptions::default()).await?;

    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("subscriptions-bulk.json");
    let event_key = key_u32("batch_id", 8000);

    let mut status_client = WsClient::connect(&stack.indexer_url).await?;
    let status_subscribed = status_client
        .request(json!({"id": 10, "type": "SubscribeStatus"}))
        .await?;
    assert_eq!(status_subscribed["type"], "subscriptionStatus");
    assert_eq!(status_subscribed["data"]["action"], "subscribed");
    assert_eq!(status_subscribed["data"]["target"]["type"], "status");

    let mut event_client = WsClient::connect(&stack.indexer_url).await?;
    let event_subscribed = event_client
        .request(json!({"id": 12, "type": "SubscribeEvents", "key": event_key.clone()}))
        .await?;
    assert_eq!(event_subscribed["type"], "subscriptionStatus");
    assert_eq!(event_subscribed["data"]["action"], "subscribed");
    assert_eq!(event_subscribed["data"]["target"]["type"], "events");

    let manifest = run_bulk_seeder(&stack.node_url, &manifest_path, 8000, 1, 1)?;

    let status_notification = status_client
        .wait_for_message_where(Duration::from_secs(30), |message| {
            message["type"] == "status" && spans_cover_tip(message, manifest.end_block)
        })
        .await?;
    assert_eq!(status_notification["type"], "status");

    let event_notification = event_client
        .wait_for_message_where(Duration::from_secs(30), |message| {
            message["type"] == "eventNotification"
                && message["data"]["key"] == event_key
                && message["data"]["event"]["blockNumber"]
                    == Value::from(u64::from(manifest.end_block))
        })
        .await?;
    assert_eq!(event_notification["data"]["key"], event_key);

    let batch_query = find_query(
        &manifest,
        "first seeded batch becomes queryable after backfill",
    )?;
    let indexed_batch =
        wait_for_query_expectation(&stack.indexer_url, batch_query, Duration::from_secs(30))
            .await?;
    assert_eq!(
        indexed_batch["data"]["decodedEvents"][0]["event"]["eventName"],
        "BurstEmitted"
    );

    let status_unsubscribed = status_client
        .request(json!({"id": 11, "type": "UnsubscribeStatus"}))
        .await?;
    assert_eq!(status_unsubscribed["type"], "subscriptionStatus");
    assert_eq!(status_unsubscribed["data"]["action"], "unsubscribed");
    status_client
        .expect_no_message(Duration::from_millis(250))
        .await?;
    status_client.close().await?;

    let event_unsubscribed = event_client
        .request(json!({"id": 13, "type": "UnsubscribeEvents", "key": event_key.clone()}))
        .await?;
    assert_eq!(event_unsubscribed["type"], "subscriptionStatus");
    assert_eq!(event_unsubscribed["data"]["action"], "unsubscribed");
    event_client
        .expect_no_message(Duration::from_millis(250))
        .await?;
    event_client.close().await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn variant_queries_and_store_events_flag_follow_config() -> Result<(), Box<dyn Error>> {
    let stack = SyntheticStack::start(
        ConfigOverrides {
            store_events: Some(false),
            index_variant: Some(true),
        },
        IndexerOptions::default(),
    )
    .await?;
    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("bulk-manifest.json");

    let manifest = run_bulk_seeder(&stack.node_url, &manifest_path, 9100, 1, 3)?;
    wait_for_indexed_tip(
        &stack.indexer_url,
        manifest.end_block,
        Duration::from_secs(30),
    )
    .await?;

    let custom_response = get_events(&stack.indexer_url, key_u32("batch_id", 9100), 10).await?;
    assert_eq!(events_len(&custom_response), 3);
    assert!(
        custom_response["data"]["decodedEvents"]
            .as_array()
            .is_some_and(|events| events.is_empty())
    );

    let variants = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({"id": 20, "type": "Variants"}),
    )
    .await?;
    let (pallet_index, event_index) = find_variant_indexes(&variants, "Synthetic", "BurstEmitted")?;
    let variant_response = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({
            "id": 21,
            "type": "GetEvents",
            "key": variant_key(pallet_index, event_index),
            "limit": 10,
        }),
    )
    .await?;
    assert_eq!(variant_response["type"], "events");
    assert_eq!(response_events(&variant_response)?.len(), 3);
    assert!(
        variant_response["data"]["decodedEvents"]
            .as_array()
            .is_some_and(|events| events.is_empty())
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn limits_invalid_requests_and_idle_timeouts_are_enforced() -> Result<(), Box<dyn Error>> {
    let stack = SyntheticStack::start(
        ConfigOverrides::default(),
        IndexerOptions {
            max_events_limit: 2,
            max_total_subscriptions: Some(1),
            idle_timeout_secs: Some(1),
            ..IndexerOptions::default()
        },
    )
    .await?;
    let temp = tempfile::tempdir()?;
    let manifest_path = temp.path().join("limits-bulk.json");

    let manifest = run_bulk_seeder(&stack.node_url, &manifest_path, 9200, 1, 5)?;
    wait_for_indexed_tip(
        &stack.indexer_url,
        manifest.end_block,
        Duration::from_secs(30),
    )
    .await?;

    let clamped = acuity_index::synthetic_devnet::request_json_ws(
        &stack.indexer_url,
        json!({
            "id": 30,
            "type": "GetEvents",
            "key": key_u32("batch_id", 9200),
            "limit": 50,
        }),
    )
    .await?;
    assert_eq!(response_events(&clamped)?.len(), 2);

    let mut invalid_client = WsClient::connect(&stack.indexer_url).await?;
    let invalid_request = invalid_client
        .request(json!({
            "id": 31,
            "type": "GetEvents",
            "key": {
                "type": "Custom",
                "value": {
                    "name": "x".repeat(129),
                    "kind": "u32",
                    "value": 7,
                }
            },
            "limit": 10,
        }))
        .await?;
    assert_eq!(invalid_request["type"], "error");
    assert_eq!(invalid_request["data"]["code"], "invalid_request");
    invalid_client.close().await?;

    let mut first_subscriber = WsClient::connect(&stack.indexer_url).await?;
    let first_subscription = first_subscriber
        .request(json!({"id": 32, "type": "SubscribeStatus"}))
        .await?;
    assert_eq!(first_subscription["type"], "subscriptionStatus");

    let mut rejected_subscriber = WsClient::connect(&stack.indexer_url).await?;
    let rejected_subscription = rejected_subscriber
        .request(json!({
            "id": 33,
            "type": "SubscribeEvents",
            "key": key_u32("batch_id", 9201),
        }))
        .await?;
    assert_eq!(rejected_subscription["type"], "error");
    assert_eq!(rejected_subscription["data"]["code"], "subscription_limit");
    rejected_subscriber.close().await?;

    let mut idle_client = WsClient::connect(&stack.indexer_url).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        idle_client
            .request(json!({"id": 34, "type": "Status"}))
            .await
            .is_err()
    );

    first_subscriber.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn indexer_restart_and_rpc_reconnect_preserve_queryability() -> Result<(), Box<dyn Error>> {
    let mut stack =
        SyntheticStack::start(ConfigOverrides::default(), IndexerOptions::default()).await?;
    let temp = tempfile::tempdir()?;
    let smoke_manifest_path = temp.path().join("smoke-manifest.json");

    let smoke_manifest = run_smoke_seeder(&stack.node_url, &smoke_manifest_path)?;
    wait_for_indexed_tip(
        &stack.indexer_url,
        smoke_manifest.end_block,
        Duration::from_secs(30),
    )
    .await?;

    let baseline = get_events(&stack.indexer_url, key_u32("record_id", 100), 10).await?;
    assert_eq!(events_len(&baseline), 2);

    stack.restart_indexer().await?;
    let after_restart = get_events(&stack.indexer_url, key_u32("record_id", 100), 10).await?;
    assert_eq!(
        response_events(&after_restart)?,
        response_events(&baseline)?
    );
    let status_after_restart = fetch_status(&stack.indexer_url).await?;
    assert_eq!(status_after_restart["type"], "status");

    stack.stop_node();
    wait_for_variants_temporarily_unavailable(&stack.indexer_url, Duration::from_secs(30)).await?;

    let status_during_outage = fetch_status(&stack.indexer_url).await?;
    assert_eq!(status_during_outage["type"], "status");
    let query_during_outage = get_events(&stack.indexer_url, key_u32("record_id", 100), 10).await?;
    assert_eq!(
        response_events(&query_during_outage)?,
        response_events(&baseline)?
    );

    stack.restart_node().await?;
    let variants_after_restart =
        wait_for_variants_available(&stack.indexer_url, Duration::from_secs(60)).await?;
    let _ = find_variant_indexes(&variants_after_restart, "Synthetic", "BurstEmitted")?;

    let query_after_reconnect =
        get_events(&stack.indexer_url, key_u32("record_id", 100), 10).await?;
    assert_eq!(
        response_events(&query_after_reconnect)?,
        response_events(&baseline)?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires polkadot-omni-node and a release runtime build"]
async fn startup_fails_on_chain_genesis_hash_mismatch() -> Result<(), Box<dyn Error>> {
    build_runtime_release()?;

    let temp = tempfile::tempdir()?;
    let chain_spec = temp.path().join("synthetic-dev-chain-spec.json");
    let node_base = temp.path().join("node");
    let node_log = temp.path().join("node.log");
    let config_path = temp.path().join("synthetic.toml");
    let index_db = temp.path().join("db");
    let indexer_log = temp.path().join("indexer.log");
    let rpc_port = pick_unused_port()?;
    let indexer_port = pick_unused_port()?;

    build_chain_spec(&chain_spec)?;

    let _node = start_node(&chain_spec, rpc_port, &node_base, &node_log)?;
    let node_url = format!("ws://127.0.0.1:{rpc_port}");
    wait_for_node(&node_url, 0, Duration::from_secs(30)).await?;

    let actual_genesis_hash = fetch_genesis_hash(&node_url).await?;
    let wrong_genesis_hash = different_genesis_hash(&actual_genesis_hash);
    write_config_with_overrides(
        &config_path,
        &node_url,
        &wrong_genesis_hash,
        &ConfigOverrides::default(),
    )?;

    let mut indexer = start_indexer(
        &config_path,
        &node_url,
        &index_db,
        indexer_port,
        &indexer_log,
        &IndexerOptions::default(),
    )?;
    let status = indexer.wait_for_exit(Duration::from_secs(30)).await?;
    assert!(!status.success());

    let log = read_text(&indexer_log)?;
    assert!(
        log.contains("chain genesis hash mismatch"),
        "indexer log missing chain genesis mismatch: {log}"
    );

    Ok(())
}
