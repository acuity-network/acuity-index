# acuity-index

A configurable, schema-less event indexer for Substrate-based blockchains.
It connects to a node via WebSocket, decodes on-chain events, stores them in
an embedded [sled](https://github.com/spacejam/sled) database, and exposes the
indexed data through a WebSocket API.

For an implementation-level overview of how the indexer is structured, see
[ARCHITECTURE.md](./ARCHITECTURE.md).

For the current security review and deployment guidance for the public WebSocket
service, see [SECURITY.md](./SECURITY.md).

## Features

- **Config-driven** — indexing rules are defined in TOML files; no recompilation needed for new chains
- **Built-in SDK pallets** — first-class support for common Polkadot SDK pallets (Balances, Staking, Referenda, NominationPools, etc.)
- **Custom pallet rules** — map event fields to scalar or binary composite index keys via TOML
- **Resume-safe** — tracks indexed block spans and resumes after restart
- **Safe shutdown** — persists progress and exits cleanly on termination signals or when the upstream node disconnects
- **Backward indexing** — indexes from the chain tip backwards while simultaneously tracking new blocks
- **WebSocket API** — query events by key, subscribe to live updates, and inspect chain metadata
- **Concurrent block fetching** — configurable queue depth for parallel backfill and HEAD catch-up requests

## Supported Chains

| Chain     | Flag        |
|-----------|-------------|
| Polkadot  | `polkadot`  |
| Kusama    | `kusama`    |
| Westend   | `westend`   |
| Paseo     | `paseo`     |

Any Substrate chain can be supported by providing a custom index specification TOML with `--index-config`.

## Requirements

- Rust **stable** (see `rust-toolchain.toml`)
- A running Substrate node with WebSocket RPC enabled
- The node must be started with `--state-pruning archive-canonical`
- `polkadot-omni-node` if you want to use the in-repo synthetic runtime for local integration tests and benchmarks

## Installation

```bash
cargo install --path .
```

For Rust clients consuming the WebSocket API, see
[`acuity-index-api-rs` on crates.io](https://crates.io/crates/acuity-index-api-rs).

## Usage

```bash
acuity-index [OPTIONS]
```

### Options

| Option | Default | Description |
|---|---|---|
| `-c, --chain <CHAIN>` | — | Chain to index (`polkadot`, `kusama`, `westend`, `paseo`) |
| `--index-config <PATH>` | — | Path to an index specification TOML file (overrides `--chain`) |
| `--options-config <PATH>` | — | Path to a runtime options TOML file |
| `-d, --db-path <PATH>` | `~/.local/share/acuity-index/<chain>/db` | Database directory |
| `--db-mode <MODE>` | `low-space` | `low-space` or `high-throughput` |
| `--db-cache-capacity <SIZE>` | `1024.00 MiB` | Maximum sled page-cache size |
| `-u, --url <URL>` | Chain default | WebSocket URL of the Substrate node |
| `--queue-depth <N>` | `1` | Concurrent block requests during backfill and HEAD catch-up |
| `-f, --finalized` | `false` | Only index finalized blocks |
| `-i, --index-variant` | `false` | Also index events by pallet/variant index |
| `-s, --store-events` | `false` | Store decoded events per `(block_number, event_index)` for retrieval |
| `-p, --port <PORT>` | `8172` | WebSocket API port |
| `--metrics-port <PORT>` | — | Optional HTTP `/metrics` port for OpenMetrics scraping |
| `-v / -q` | — | Increase / decrease log verbosity |

Runtime option precedence: **CLI flags > `--options-config` file > built-in defaults**.
`--index-variant` and `--store-events` are enabled if either the CLI flag or the index spec field is `true`.

### Subcommands

| Subcommand | Description |
|---|---|
| `generate-index-spec --url <URL> <OUTPUT>` | Inspect live metadata and write a starter index specification TOML file |
| `purge-index [OPTIONS]` | Delete the index database for a built-in chain or custom index spec |

### Examples

Index Polkadot with default settings:

```bash
acuity-index
```

Index Kusama with event storage and 8 concurrent requests:

```bash
acuity-index --chain kusama --store-events --queue-depth 8
```

Use a custom index specification:

```bash
acuity-index --index-config ./chains/mychain.toml --url wss://mynode:443
```

Generate a starter index specification from a live node:

```bash
acuity-index generate-index-spec --url wss://mynode:443 ./chains/mychain.toml
```

## Local Synthetic Devnet

This repository now includes a minimal in-repo Polkadot SDK runtime under `runtime/` and a matching local index spec template in `chains/synthetic.toml`.

The synthetic runtime is intentionally small and deterministic:

- one custom `Synthetic` pallet emits searchable `u32`, `bytes32`, `account_id`, and multi-value event fields
- `Balances` and `TransactionPayment` remain present so built-in SDK event indexing is exercised too
- `just synthetic-node` runs `polkadot-omni-node --instant-seal --pool-type single-state` for ad hoc local experimentation and smoke-style seeding
- `just benchmark-indexing` starts its own disposable synthetic node with `--dev-block-time 100 --pool-type single-state` and does not use `--instant-seal`

Useful recipes:

```bash
# build the in-repo runtime, emit a chain spec, and run the synthetic dev node locally
just synthetic-node

# seed a small deterministic dataset for integration testing against the running node
just seed-smoke

# run the ignored node-backed integration suite
just test-integration

# auto-start a timed synthetic node, seed many event-dense blocks, and benchmark end-to-end indexing throughput
just benchmark-indexing
```

The benchmark recipe starts its own synthetic node on the selected RPC port. Its bulk seeder submits one transaction, waits until that transaction has been included in a block, and only then submits the next one. The reported event rate is based on the synthetic pallet events submitted by the seeder.

By default, `just benchmark-indexing` starts at `queue_depth=4`, seeds 5000 burst blocks with `burst_count=128`, waits up to 600 seconds for each benchmark run to become ready, prints the JSON report for each successful run, then prints a summary table and the first failing `queue_depth`.

The ignored synthetic integration suite exercises the real node-backed stack end
to end, including `Status`, `Variants`, `GetEvents`, `SizeOnDisk`, subscription
flows, live notifications, selected WebSocket limits/error behavior, and
restart/reconnect behavior.

`just` recipe overrides are positional here. `queue_depth` is the starting depth, and each successful run doubles it until a run fails. To change the benchmark inputs, use:

```bash
just benchmark-indexing <rpc_port> <queue_depth> <batch_start> <batches> <burst_count> <timeout_secs>
```

For example:

```bash
just benchmark-indexing 9944 8 1000 1000 128 600
```

`generate-index-spec` currently requires runtime metadata v14 or higher.
The generator uses `subxt` metadata decoding to inspect pallets and event fields,
and the current implementation only converts modern FRAME metadata layouts. Nodes
serving older metadata, such as v13 from early chain history before a runtime
upgrade, are rejected with an explicit error instead of a generic decode failure.

This most often happens when pointing at a node that is still syncing from an old
genesis/runtime snapshot. In that case, wait until the chain has synced past the
runtime upgrade that introduced v14+ metadata and try again.

## Syncing And Warp-Synced Nodes

`acuity-index` indexes backwards from the observed tip while also tracking new blocks at the head. When the node is syncing quickly, `--queue-depth` is used for both historical backfill and forward HEAD catch-up so the indexer can catch up instead of processing only one new head block at a time.

The indexer requires historical state to be available. If the node prunes historical state, indexing stops with an explicit error explaining that `--state-pruning` must be set to `archive-canonical`.

For `polkadot-omni-node`, a working example is:

```bash
polkadot-omni-node --chain target/dev-chain-spec.json --dev --dev-block-time 1000 --state-pruning archive-canonical
```

Without `--state-pruning archive-canonical`, the node may still serve recent heads, but `acuity-index` will fail once it needs historical state during backfill.

## Shutdown Behavior

`acuity-index` persists the active in-memory span before exit so it can resume safely on the next start.

Clean shutdown happens in two cases:

- the process receives a termination signal
- a fatal startup/runtime error forces the process to exit

If the upstream node closes the live block stream or the RPC connection drops, `acuity-index` saves the active span, keeps the WebSocket server running for sled-backed reads and existing subscriptions, and reconnects with exponential backoff instead of exiting. RPC-backed requests such as `Variants` return a temporary-unavailable error until the node comes back.

On actual shutdown, the process logs the shutdown reason, stops the WebSocket server, flushes sled, and exits cleanly instead of panicking.

Startup failures such as invalid cache-size configuration, genesis-hash mismatches, database open errors, RPC initialization failures, and signal-registration failures are also reported as structured errors and logged before the process exits.

## Index Specification

Each chain is described by an index specification TOML file. Built-in specs are embedded at
compile time; custom specs can be passed via `--index-config`.

If a runtime includes multiple instances of the same Substrate pallet under
different names, treat them as distinct pallets in the config. Each instance
has its own pallet name in metadata and its own independent storage, calls,
events, and errors, even when the runtime uses the same pallet code for both.
This only applies to instantiable pallets; a non-instantiable pallet cannot be
added to a runtime more than once. For example, a runtime may include
`pallet_collective` twice as `Council` and `TechnicalCommittee`; those should
be configured as two separate pallets because proposals, votes, and membership
are tracked independently for each instance.

```toml
name = "mychain"
genesis_hash = "abc123..."
default_url = "wss://my-node:443"
index_variant = false
store_events = false
spec_change_blocks = [0]

# Built-in SDK pallet — indexing rules handled internally
[[pallets]]
name = "Balances"
sdk = true

# Custom keys must be declared once at schema level
[custom_keys]
item_id = "bytes32"
item_revision = { fields = ["bytes32", "u32"] }

# Custom pallet — built-in and generic scalar mappings
[[pallets]]
name = "MyPallet"

[[pallets.events]]
name = "SomeEvent"

[[pallets.events.params]]
field = "who"       # field name, or "0" for positional
key = "account_id"  # built-in key

[[pallets.events.params]]
field = "item_id"
key = "item_id"     # custom query key name

[[pallets.events.params]]
fields = ["item_id", "revision_id"]
key = "item_revision" # binary composite query key
```

`spec_change_blocks` lists the block heights where a new index-spec revision starts.
It must start with `0` and be strictly increasing. When a new boundary is added in
the past, existing indexed spans are kept through the block before that boundary,
and the suffix starting at the earliest affected boundary is re-indexed.

### Runtime Options Config

Runtime options can be loaded from a separate TOML file via `--options-config`.
This is useful for deployment-specific settings (like a WebSocket URL, database path,
or listener port) that vary across environments.
`index_variant` and `store_events` live in the index spec, not in the runtime options file.

All fields are optional — omit any field to keep its built-in default:

| Field | Type | Default |
|---|---|---|
| `url` | string | index spec `default_url` |
| `db_path` | string | `~/.local/share/acuity-index/<chain>/db` |
| `db_mode` | string (`"low_space"` or `"high_throughput"`) | `low_space` |
| `db_cache_capacity` | string | `1024.00 MiB` |
| `queue_depth` | integer | `1` |
| `finalized` | boolean | `false` |
| `port` | integer | `8172` |
| `metrics_port` | integer | disabled |

Merge precedence: **CLI flags > `--options-config` file > built-in defaults**.
`finalized` is enabled if either the CLI flag or the options config field is `true`.

### Built-in Key Types

| Key type | Description |
|---|---|
| `account_id` | 32-byte account identifier |
| `account_index` | u32 account index |
| `bounty_index` | u32 bounty/child-bounty index |
| `era_index` | u32 era index |
| `message_id` | 32-byte message identifier |
| `pool_id` | u32 nomination pool identifier |
| `preimage_hash` | 32-byte preimage hash |
| `proposal_hash` | 32-byte proposal hash |
| `proposal_index` | u32 proposal index |
| `ref_index` | u32 referendum index |
| `registrar_index` | u32 registrar index |
| `session_index` | u32 session index |
| `spend_index` | u32 treasury spend index |
| `tip_hash` | 32-byte tip hash |

### Generic Custom Keys

Custom pallet fields no longer need Rust enum variants or dedicated sled trees.
Use any key name with one of these scalar kinds:

| Kind | Stored/query value |
|---|---|
| `bytes32` | 32-byte hex value |
| `u32` | 32-bit unsigned integer |
| `u64` | 64-bit unsigned integer |
| `u128` | 128-bit unsigned integer |
| `string` | UTF-8 string |
| `bool` | boolean |

Example:

```toml
[custom_keys]
para_id = "u32"
candidate_hash = "bytes32"

[[pallets.events.params]]
field = "para_id"
key = "para_id"

[[pallets.events.params]]
field = "candidate_hash"
key = "candidate_hash"
```

### Composite Custom Keys

Composite custom keys let the indexer build one binary query key from multiple
event fields. They are defined in `[custom_keys]` and referenced with
`fields = ["...", "..."]` in event params.

Example:

```toml
[custom_keys]
item_id = "bytes32"
revision_id = "u32"
item_revision = { fields = ["bytes32", "u32"] }

[[pallets]]
name = "ContentReactions"

[[pallets.events]]
name = "SetReactions"

[[pallets.events.params]]
fields = ["item_id", "revision_id"]
key = "item_revision"

[[pallets.events.params]]
field = "reactor"
key = "account_id"
```

Notes:

- Use `field = "..."` for built-in keys and scalar custom keys.
- Use `fields = [...]` only for composite custom keys.
- Composite key values are ordered and binary encoded, so field order matters.

## WebSocket API

The complete WebSocket API reference now lives in [`API.md`](API.md).

The public WebSocket service also enforces connection and request limits. See
[`SECURITY.md`](./SECURITY.md) for the current security review, deployment
guidance, and operational limits.

If you are integrating from Rust, the published client crate is
[`acuity-index-api-rs`](https://crates.io/crates/acuity-index-api-rs).

It covers:

- request and response envelopes
- all request types
- notification types
- key formats
- composite key request shapes
- pagination semantics
- error responses
- backpressure and subscription termination behavior
