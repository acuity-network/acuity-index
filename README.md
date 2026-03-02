# acuity-index

A configurable, schema-less event indexer for Substrate-based blockchains.
It connects to a node via WebSocket, decodes on-chain events, stores them in
an embedded [sled](https://github.com/spacejam/sled) database, and exposes the
indexed data through a WebSocket API.

## Features

- **Config-driven** — indexing rules are defined in TOML files; no recompilation needed for new chains
- **Built-in SDK pallets** — first-class support for common Polkadot SDK pallets (Balances, Staking, Referenda, NominationPools, etc.)
- **Custom pallet rules** — map any event field to an index key via TOML
- **Resume-safe** — tracks indexed block spans and resumes after restart
- **Backward indexing** — indexes from the chain tip backwards while simultaneously tracking new blocks
- **WebSocket API** — query events by key, subscribe to live updates, and inspect chain metadata
- **Concurrent block fetching** — configurable queue depth for parallel block requests

## Supported Chains

| Chain     | Flag        |
|-----------|-------------|
| Polkadot  | `polkadot`  |
| Kusama    | `kusama`    |
| Westend   | `westend`   |
| Paseo     | `paseo`     |

Any Substrate chain can be supported by providing a custom TOML config with `--chain-config`.

## Requirements

- Rust **nightly** (see `rust-toolchain.toml`)
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
| `-d, --db-path <PATH>` | `~/.local/share/acuity-index/<chain>/db` | Database directory |
| `--db-mode <MODE>` | `low-space` | `low-space` or `high-throughput` |
| `--db-cache-capacity <SIZE>` | `1024.00 MiB` | Maximum sled page-cache size |
| `-u, --url <URL>` | Chain default | WebSocket URL of the Substrate node |
| `--queue-depth <N>` | `1` | Concurrent block requests during batch indexing |
| `-f, --finalized` | `false` | Only index finalized blocks |
| `-i, --index-variant` | `false` | Also index events by pallet/variant index |
| `-s, --store-events` | `false` | Store full decoded event JSON for retrieval |
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

## Chain Configuration

Each chain is described by a TOML file. Built-in configs are embedded at
compile time; custom configs can be passed via `--chain-config`.

```toml
name = "mychain"
genesis_hash = "abc123..."
default_url = "wss://my-node:443"
versions = [0]

# Built-in SDK pallet — indexing rules handled internally
[[pallets]]
name = "Balances"
sdk = true

# Custom pallet — explicit field → key mappings
[[pallets]]
name = "MyPallet"

[[pallets.events]]
name = "SomeEvent"

[[pallets.events.params]]
field = "who"       # field name, or "0" for positional
key = "account_id"  # key type (see below)
```

### Available Key Types

| Key type | Description |
|---|---|
| `account_id` | 32-byte account identifier |
| `account_index` | u32 account index |
| `auction_index` | u32 auction index |
| `bounty_index` | u32 bounty/child-bounty index |
| `candidate_hash` | 32-byte candidate hash |
| `era_index` | u32 era index |
| `message_id` | 32-byte message identifier |
| `para_id` | u32 parachain identifier |
| `pool_id` | u32 nomination pool identifier |
| `preimage_hash` | 32-byte preimage hash |
| `proposal_hash` | 32-byte proposal hash |
| `proposal_index` | u32 proposal index |
| `ref_index` | u32 referendum index |
| `registrar_index` | u32 registrar index |
| `session_index` | u32 session index |
| `spend_index` | u32 treasury spend index |
| `tip_hash` | 32-byte tip hash |

## WebSocket API

Connect to `ws://localhost:8172` and send/receive JSON messages.

### Request → Response

| Request | Response |
|---|---|
| `{"type":"Status"}` | Indexed block spans |
| `{"type":"Variants"}` | All pallet event variants from chain metadata |
| `{"type":"GetEvents","key":{...}}` | Matching event refs (+ decoded JSON if `--store-events`) |
| `{"type":"SizeOnDisk"}` | Database size in bytes |

### Subscriptions

```json
{"type":"SubscribeStatus"}
{"type":"UnsubscribeStatus"}

{"type":"SubscribeEvents","key":{"type":"AccountId","value":"0xabc..."}}
{"type":"UnsubscribeEvents","key":{"type":"AccountId","value":"0xabc..."}}
```

Subscribers receive push messages whenever a matching event is indexed.

### Key Format

Keys are JSON objects with a `type` discriminant:

```json
{"type":"AccountId",    "value":"0x1234..."}
{"type":"ParaId",       "value":1000}
{"type":"EraIndex",     "value":1500}
{"type":"Variant",      "value":[0, 3]}
```

## Architecture
