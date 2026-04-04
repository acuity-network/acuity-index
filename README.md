# acuity-index

A configurable, schema-less event indexer for Substrate-based blockchains.
It connects to a node via WebSocket, decodes on-chain events, stores them in
an embedded [sled](https://github.com/spacejam/sled) database, and exposes the
indexed data through a WebSocket API.

For an implementation-level overview of how the indexer is structured, see
[ARCHITECTURE.md](./ARCHITECTURE.md).

## Features

- **Config-driven** — indexing rules are defined in TOML files; no recompilation needed for new chains
- **Built-in SDK pallets** — first-class support for common Polkadot SDK pallets (Balances, Staking, Referenda, NominationPools, etc.)
- **Custom pallet rules** — map any event field to an index key via TOML
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

Any Substrate chain can be supported by providing a custom TOML config with `--chain-config`.

## Requirements

- Rust **stable** (see `rust-toolchain.toml`)
- A running Substrate node with WebSocket RPC enabled

## Installation

```bash
cargo install --path .
```

## Usage

```bash
acuity-index [OPTIONS]
```

### Options

| Option | Default | Description |
|---|---|---|
| `-c, --chain <CHAIN>` | `polkadot` | Chain to index (`polkadot`, `kusama`, `westend`, `paseo`) |
| `--chain-config <PATH>` | — | Path to a custom chain TOML config (overrides `--chain`) |
| `--generate-chain-config <PATH>` | — | Inspect live metadata and write a starter TOML config |
| `-d, --db-path <PATH>` | `~/.local/share/acuity-index/<chain>/db` | Database directory |
| `--db-mode <MODE>` | `low-space` | `low-space` or `high-throughput` |
| `--db-cache-capacity <SIZE>` | `1024.00 MiB` | Maximum sled page-cache size |
| `-u, --url <URL>` | Chain default | WebSocket URL of the Substrate node |
| `--queue-depth <N>` | `1` | Concurrent block requests during backfill and HEAD catch-up |
| `-f, --finalized` | `false` | Only index finalized blocks |
| `-i, --index-variant` | `false` | Also index events by pallet/variant index |
| `-s, --store-events` | `false` | Store decoded events per `(block_number, event_index)` for retrieval |
| `-p, --port <PORT>` | `8172` | WebSocket API port |
| `-v / -q` | — | Increase / decrease log verbosity |

### Examples

Index Polkadot with default settings:

```bash
acuity-index
```

Index Kusama with event storage and 8 concurrent requests:

```bash
acuity-index --chain kusama --store-events --queue-depth 8
```

Use a custom chain config:

```bash
acuity-index --chain-config ./chains/mychain.toml --url wss://mynode:443
```

Generate a starter config from a live node:

```bash
acuity-index --url wss://mynode:443 --generate-chain-config ./chains/mychain.toml
```

## Syncing And Warp-Synced Nodes

`acuity-index` indexes backwards from the observed tip while also tracking new blocks at the head. When the node is syncing quickly, `--queue-depth` is used for both historical backfill and forward HEAD catch-up so the indexer can catch up instead of processing only one new head block at a time.

The indexer now treats missing historical block bodies as a hard error. If the node can return a block hash but cannot return the block body for that hash, indexing stops with a `HistoricalBlockDataUnavailable` error instead of retrying indefinitely.

This matters for nodes that are still syncing and for nodes started in modes like `warp` or with non-archive pruning, where headers may exist but historical block bodies are unavailable.

For full historical indexing, use a node that can serve historical block bodies for the range you expect to index.

## Shutdown Behavior

`acuity-index` persists the active in-memory span before exit so it can resume safely on the next start.

Clean shutdown happens in two cases:

- the process receives a termination signal
- the upstream node closes the live block stream, such as when the node exits

In both cases the process logs the shutdown reason, stops the WebSocket server, flushes sled, and exits cleanly instead of panicking.

## Chain Configuration

Each chain is described by a TOML file. Built-in configs are embedded at
compile time; custom configs can be passed via `--chain-config`.

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
versions = [0]

# Built-in SDK pallet — indexing rules handled internally
[[pallets]]
name = "Balances"
sdk = true

# Custom keys must be declared once at schema level
[custom_keys]
item_id = "bytes32"

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
```

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

## WebSocket API

The complete WebSocket API reference now lives in [`API.md`](API.md).

It covers:

- request and response envelopes
- all request types
- notification types
- key formats
- pagination semantics
- error responses
- backpressure and subscription termination behavior
