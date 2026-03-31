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
| `--queue-depth <N>` | `1` | Concurrent block requests during batch indexing |
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
kind = "bytes32"    # generic scalar storage kind
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
[[pallets.events.params]]
field = "para_id"
key = "para_id"
kind = "u32"

[[pallets.events.params]]
field = "candidate_hash"
key = "candidate_hash"
kind = "bytes32"
```

## WebSocket API

Connect to `ws://localhost:8172` and send/receive JSON messages.

### Request → Response

| Request | Response |
|---|---|
| `{"type":"Status"}` | Indexed block spans |
| `{"type":"Variants"}` | All pallet event variants from chain metadata |
| `{"type":"GetEvents","key":{...}}` | Matching event refs (+ matching decoded events if `--store-events`) |
| `{"type":"SizeOnDisk"}` | Database size in bytes |

When `--store-events` is enabled, decoded events are stored individually using
the composite key `(block_number, event_index)`. This lets the API return only
the matched decoded events instead of whole block-wide event arrays.

Each decoded event payload includes:

- `specVersion`
- `palletName`
- `eventName`
- `palletIndex`
- `variantIndex`
- `eventIndex`
- `fields`

Example `GetEvents` response with stored decoded events:

```json
{
  "type": "events",
  "data": {
    "key": {"type": "RefIndex", "value": 42},
    "events": [
      {"blockNumber": 50, "eventIndex": 3}
    ],
    "decodedEvents": [
      {
        "blockNumber": 50,
        "eventIndex": 3,
        "event": {
          "specVersion": 1234,
          "palletName": "Referenda",
          "eventName": "Submitted",
          "palletIndex": 42,
          "variantIndex": 0,
          "eventIndex": 3,
          "fields": {
            "index": 42
          }
        }
      }
    ]
  }
}
```

### Subscriptions

```json
{"type":"SubscribeStatus"}
{"type":"UnsubscribeStatus"}

{"type":"SubscribeEvents","key":{"type":"AccountId","value":"0xabc..."}}
{"type":"SubscribeEvents","key":{"type":"Custom","value":{"name":"item_id","kind":"bytes32","value":"0xabc..."}}}
{"type":"UnsubscribeEvents","key":{"type":"Custom","value":{"name":"item_id","kind":"bytes32","value":"0xabc..."}}}
```

Subscribers receive push messages whenever a matching event is indexed.

### Key Format

Keys are JSON objects with a `type` discriminant:

```json
{"type":"AccountId",    "value":"0x1234..."}
{"type":"EraIndex",     "value":1500}
{"type":"Variant",      "value":[0, 3]}
{"type":"Custom",       "value":{"name":"para_id","kind":"u32","value":1000}}
{"type":"Custom",       "value":{"name":"published","kind":"bool","value":true}}
{"type":"Custom",       "value":{"name":"revision","kind":"u128","value":"42"}}
```

## Architecture
