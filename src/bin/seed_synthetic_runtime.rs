use acuity_index::synthetic_devnet::{
    QueryExpectation, SeedManifest, key_account, key_bytes32, key_u32,
};
use clap::{Parser, ValueEnum};
use serde_json::to_string_pretty;
use std::{
    error::Error,
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use subxt::config::PolkadotExtrinsicParamsBuilder;
use subxt::tx::TransactionStatus;
use subxt::utils::AccountId32;
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    dynamic,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods, rpc_params},
};
use subxt_signer::sr25519::{Keypair, dev};
use tokio::time::sleep;
use tracing::{Level, info};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SeedMode {
    Smoke,
    Bulk,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:9944")]
    url: String,
    #[arg(long, value_enum, default_value_t = SeedMode::Smoke)]
    mode: SeedMode,
    #[arg(long, default_value_t = 1000)]
    batch_start: u32,
    #[arg(long, default_value_t = 100)]
    batches: u32,
    #[arg(long, default_value_t = 64)]
    burst_count: u32,
    #[arg(long)]
    output: Option<PathBuf>,
}

struct NodeClient {
    api: OnlineClient<PolkadotConfig>,
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    rpc_client: RpcClient,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(Level::INFO)
        .init();

    let args = Args::parse();
    info!(
        url = %args.url,
        mode = ?args.mode,
        batch_start = args.batch_start,
        batches = args.batches,
        burst_count = args.burst_count,
        "starting synthetic runtime seeder"
    );

    let client = connect_when_ready(&args.url, Duration::from_secs(30)).await?;
    let genesis_hash = hex::encode(client.api.genesis_hash().as_ref());
    let start_block = current_block_number(&client).await?;
    info!(genesis_hash = %genesis_hash, start_block, "connected to synthetic node");

    let manifest = match args.mode {
        SeedMode::Smoke => seed_smoke(&client, &genesis_hash, start_block).await?,
        SeedMode::Bulk => {
            seed_bulk(
                &client,
                &genesis_hash,
                start_block,
                args.batch_start,
                args.batches,
                args.burst_count,
            )
            .await?
        }
    };

    let rendered = to_string_pretty(&manifest)?;
    if let Some(path) = args.output {
        fs::write(&path, &rendered)?;
        info!(path = %path.display(), "wrote seed manifest");
    }

    info!(
        mode = %manifest.mode,
        start_block = manifest.start_block,
        end_block = manifest.end_block,
        transactions_submitted = manifest.transactions_submitted,
        synthetic_event_count = manifest.synthetic_event_count,
        "seeding complete"
    );
    println!("{rendered}");
    Ok(())
}

async fn connect_node(url: &str) -> Result<NodeClient, Box<dyn Error>> {
    let rpc_client = RpcClient::from_url(url).await?;
    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone()).await?;
    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client.clone());
    Ok(NodeClient {
        api,
        rpc,
        rpc_client,
    })
}

async fn connect_when_ready(url: &str, timeout: Duration) -> Result<NodeClient, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    info!(url, timeout = ?timeout, "waiting for node RPC");

    loop {
        match connect_node(url).await {
            Ok(client) => {
                info!(url, "node RPC is reachable");
                return Ok(client);
            }
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Err(err) => {
                return Err(std::io::Error::other(format!(
                    "node did not become reachable within {timeout:?}: {err}"
                ))
                .into());
            }
        }
    }
}

async fn current_block_number(client: &NodeClient) -> Result<u32, Box<dyn Error>> {
    let header = client
        .rpc
        .chain_get_header(None)
        .await?
        .ok_or_else(|| std::io::Error::other("best head header missing"))?;
    header
        .number
        .try_into()
        .map_err(|_| std::io::Error::other("block number exceeds u32").into())
}

async fn seed_smoke(
    client: &NodeClient,
    genesis_hash: &str,
    start_block: u32,
) -> Result<SeedManifest, Box<dyn Error>> {
    let alice = dev::alice();
    let bob = dev::bob();
    let charlie = dev::charlie();
    let mut alice_nonce = account_nonce(client, alice.public_key().0).await?;
    let mut charlie_nonce = account_nonce(client, charlie.public_key().0).await?;

    let record_id = 100u32;
    let record_digest = [0x11; 32];
    let link_digest_a = [0x21; 32];
    let link_digest_b = [0x22; 32];
    let batch_id = 77u32;
    let burst_count = 4u32;

    info!(
        start_block,
        record_id, batch_id, burst_count, "seeding smoke workload"
    );

    info!(to = %hex::encode(bob.public_key().0), amount = 123u128, "submitting balances transfer");
    let transfer_block = submit_call_with_retry(
        client,
        &dynamic::tx(
            "Balances",
            "transfer_allow_death",
            (bob.public_key().to_address::<()>(), 123u128),
        ),
        &alice,
        &mut alice_nonce,
    )
    .await?;
    info!(block_number = transfer_block, "balances transfer included");

    info!(record_id, digest = %hex::encode(record_digest), "submitting synthetic record");
    let record_block = submit_call_with_retry(
        client,
        &dynamic::tx(
            "Synthetic",
            "store_record",
            (record_id, record_digest, [7u32, 9u32]),
        ),
        &alice,
        &mut alice_nonce,
    )
    .await?;
    info!(
        block_number = record_block,
        record_id, "synthetic record included"
    );

    info!(record_id, link_count = 2, "submitting synthetic links");
    let links_block = submit_call_with_retry(
        client,
        &dynamic::tx(
            "Synthetic",
            "store_links",
            (record_id, [200u32, 201u32], [link_digest_a, link_digest_b]),
        ),
        &alice,
        &mut alice_nonce,
    )
    .await?;
    info!(
        block_number = links_block,
        record_id, "synthetic links included"
    );

    info!(batch_id, burst_count, "submitting synthetic burst");
    let burst_block = submit_call_with_retry(
        client,
        &dynamic::tx("Synthetic", "emit_burst", (batch_id, burst_count)),
        &charlie,
        &mut charlie_nonce,
    )
    .await?;
    info!(
        block_number = burst_block,
        batch_id, burst_count, "synthetic burst included"
    );
    let end_block = [transfer_block, record_block, links_block, burst_block]
        .into_iter()
        .max()
        .unwrap_or(start_block);

    Ok(SeedManifest {
        mode: "smoke".into(),
        genesis_hash: genesis_hash.into(),
        start_block,
        end_block,
        total_blocks: end_block.saturating_sub(start_block),
        transactions_submitted: 4,
        synthetic_event_count: 6,
        queries: vec![
            QueryExpectation {
                description: "record id aggregates multiple synthetic events".into(),
                key: key_u32("record_id", record_id),
                min_events: 2,
                expected_event_names: vec!["RecordStored".into(), "LinksStored".into()],
            },
            QueryExpectation {
                description: "topic multi-value lookup works".into(),
                key: key_u32("topic", 7),
                min_events: 1,
                expected_event_names: vec!["RecordStored".into()],
            },
            QueryExpectation {
                description: "bytes32 lookup works".into(),
                key: key_bytes32("digest", record_digest),
                min_events: 1,
                expected_event_names: vec!["RecordStored".into()],
            },
            QueryExpectation {
                description: "built-in balances transfer is indexed by account".into(),
                key: key_account(bob.public_key().0),
                min_events: 1,
                expected_event_names: vec!["Transfer".into()],
            },
            QueryExpectation {
                description: "burst batch query returns many events".into(),
                key: key_u32("batch_id", batch_id),
                min_events: burst_count as usize,
                expected_event_names: vec!["BurstEmitted".into()],
            },
        ],
    })
}

async fn seed_bulk(
    client: &NodeClient,
    genesis_hash: &str,
    start_block: u32,
    batch_start: u32,
    batches: u32,
    burst_count: u32,
) -> Result<SeedManifest, Box<dyn Error>> {
    let alice = dev::alice();
    let bob = dev::bob();
    let charlie = dev::charlie();
    let mut alice_nonce = account_nonce(client, alice.public_key().0).await?;
    let mut bob_nonce = account_nonce(client, bob.public_key().0).await?;
    let mut charlie_nonce = account_nonce(client, charlie.public_key().0).await?;

    let mut end_block = start_block;
    info!(
        start_block,
        batch_start, batches, burst_count, "seeding bulk workload"
    );

    for idx in 0..batches {
        let batch_id = batch_start + idx;
        match idx % 3 {
            0 => {
                end_block = end_block.max(
                    submit_call_with_retry(
                        client,
                        &dynamic::tx("Synthetic", "emit_burst", (batch_id, burst_count)),
                        &alice,
                        &mut alice_nonce,
                    )
                    .await?,
                );
            }
            1 => {
                end_block = end_block.max(
                    submit_call_with_retry(
                        client,
                        &dynamic::tx("Synthetic", "emit_burst", (batch_id, burst_count)),
                        &bob,
                        &mut bob_nonce,
                    )
                    .await?,
                );
            }
            _ => {
                end_block = end_block.max(
                    submit_call_with_retry(
                        client,
                        &dynamic::tx("Synthetic", "emit_burst", (batch_id, burst_count)),
                        &charlie,
                        &mut charlie_nonce,
                    )
                    .await?,
                );
            }
        }

        info!(
            batch_id,
            batch_index = idx + 1,
            total_batches = batches,
            burst_count,
            block_number = end_block,
            "seeded synthetic burst batch"
        );
    }

    let first_batch = batch_start;
    let last_batch = batch_start + batches.saturating_sub(1);

    Ok(SeedManifest {
        mode: "bulk".into(),
        genesis_hash: genesis_hash.into(),
        start_block,
        end_block,
        total_blocks: end_block.saturating_sub(start_block),
        transactions_submitted: batches,
        synthetic_event_count: batches.saturating_mul(burst_count),
        queries: vec![
            QueryExpectation {
                description: "first seeded batch becomes queryable after backfill".into(),
                key: key_u32("batch_id", first_batch),
                min_events: burst_count as usize,
                expected_event_names: vec!["BurstEmitted".into()],
            },
            QueryExpectation {
                description: "latest seeded batch remains queryable".into(),
                key: key_u32("batch_id", last_batch),
                min_events: burst_count as usize,
                expected_event_names: vec!["BurstEmitted".into()],
            },
        ],
    })
}

async fn submit_call<Call>(
    client: &NodeClient,
    call: &Call,
    signer: &Keypair,
    nonce: u64,
) -> Result<u32, Box<dyn Error>>
where
    Call: subxt::tx::Payload,
{
    let params = PolkadotExtrinsicParamsBuilder::<PolkadotConfig>::new()
        .nonce(nonce)
        .immortal()
        .build();
    let mut progress = client
        .api
        .tx()
        .await?
        .sign_and_submit_then_watch(call, signer, params)
        .await?;

    while let Some(status) = progress.next().await {
        match status? {
            TransactionStatus::InBestBlock(in_block)
            | TransactionStatus::InFinalizedBlock(in_block) => {
                in_block.wait_for_success().await?;
                let at = in_block.at().await?;
                return Ok(at
                    .block_number()
                    .try_into()
                    .map_err(|_| std::io::Error::other("block number exceeds u32"))?);
            }
            TransactionStatus::Error { message }
            | TransactionStatus::Invalid { message }
            | TransactionStatus::Dropped { message } => {
                return Err(std::io::Error::other(message).into());
            }
            TransactionStatus::Validated
            | TransactionStatus::Broadcasted
            | TransactionStatus::NoLongerInBestBlock => continue,
        }
    }

    Err(std::io::Error::other("transaction status stream ended before inclusion").into())
}

async fn submit_call_with_retry<Call>(
    client: &NodeClient,
    call: &Call,
    signer: &Keypair,
    nonce: &mut u64,
) -> Result<u32, Box<dyn Error>>
where
    Call: subxt::tx::Payload,
{
    for _ in 0..3 {
        let current_nonce = *nonce;
        match submit_call(client, call, signer, current_nonce).await {
            Ok(block_number) => {
                *nonce = current_nonce + 1;
                return Ok(block_number);
            }
            Err(err) if err.to_string().to_ascii_lowercase().contains("outdated") => {
                info!(
                    nonce = current_nonce,
                    account = %hex::encode(signer.public_key().0),
                    "transaction nonce outdated, refreshing account nonce"
                );
                *nonce = account_nonce(client, signer.public_key().0).await?;
                info!(nonce = *nonce, account = %hex::encode(signer.public_key().0), "refreshed account nonce");
            }
            Err(err) => return Err(err),
        }
    }

    Err(std::io::Error::other("transaction remained outdated after nonce refresh").into())
}

async fn account_nonce(client: &NodeClient, account_id: [u8; 32]) -> Result<u64, Box<dyn Error>> {
    // `system_accountNextIndex` reflects the transaction pool, which avoids stale finalized
    // nonces when `--instant-seal` advances blocks faster than finality catches up.
    client
        .rpc_client
        .request(
            "system_accountNextIndex",
            rpc_params![AccountId32(account_id)],
        )
        .await
        .map_err(Into::into)
}
