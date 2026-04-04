## Overview

`acuity-index` is a config-driven event indexer for Substrate chains. It connects to a node over WebSocket RPC, decodes runtime events with `subxt`, derives query keys from built-in pallet rules or TOML config, stores index entries in `sled`, and exposes read/query access over its own WebSocket API.

The design is intentionally schema-light:

- Chain-specific behavior lives mostly in TOML.
- Event payloads are decoded generically with `scale_value` instead of generated Rust types.
- The on-disk index is append-oriented and keyed by queryable event attributes.

This file is meant to help AI agents quickly find the right code and understand the main invariants before making changes.

## Runtime Components

- `src/main.rs`
  Parses CLI args, loads chain config, opens sled, verifies genesis hash, connects to the chain, and starts the indexer task plus the WebSocket server.
- `src/indexer.rs`
  Owns the indexing pipeline, span tracking, resume logic, live-head tailing, event key derivation, decoded event storage, and subscription fanout.
- `src/config.rs`
  Defines the TOML schema and resolves configured event params into runtime key-mapping rules.
- `src/pallets.rs`
  Contains built-in indexing rules for supported Polkadot SDK pallets and helpers for extracting scalar values from schema-less event fields.
- `src/websockets.rs`
  Implements the public WebSocket API, request/response handling, and connection lifecycle.
- `src/shared.rs`
  Holds shared wire types, on-disk key formats, database tree handles, and common enums like `Key`, `RequestMessage`, and `ResponseMessage`.
- `src/config_gen.rs`
  Builds starter chain TOML configs from live runtime metadata.

## Startup Sequence

The normal startup path lives in `src/main.rs`:

1. Parse CLI args.
2. Load a built-in chain TOML or a user-supplied `--chain-config`.
3. Validate the config.
4. Resolve the database path and open sled.
5. Verify or initialize the stored `genesis_hash` in the root database.
6. Connect to the target node with `subxt`.
7. Verify the connected chain genesis hash matches the config.
8. Spawn two long-running tasks:
   - `run_indexer(...)`
   - `websockets_listen(...)`
9. Wait for either a termination signal or indexer completion, then notify the remaining task to stop and flush sled.

Important invariant:

- A database directory is tied to a single chain genesis hash. If the config or connected node disagrees with the stored hash, the process exits instead of mixing data from different chains.

## Data Model

The sled layout is opened in `Trees::open` in `src/shared.rs`.

- `root`
  Holds the sled database handle and root-level keys like `genesis_hash`.
- `span`
  Stores indexed block spans for resume/reindex logic.
- `variant`
  Stores event references keyed by `(pallet_index, variant_index, block_number, event_index)`.
- `index`
  Stores custom and built-in query keys under a compact binary prefix plus `(block_number, event_index)` suffix.
- `events`
  Stores decoded event JSON keyed by `(block_number, event_index)`.

The two main query surfaces are:

- Variant queries via the `variant` tree.
- All other keys via the `index` tree.

`Key::Custom` covers both built-in semantic keys like `account_id` and user-defined keys from `[custom_keys]` in TOML. The distinction is logical, not stored in separate trees.

## Indexing Flow

The main loop is `run_indexer` in `src/indexer.rs`.

### High-level behavior

- Determine the starting head block.
  - If `--finalized`, use the finalized head.
  - Otherwise use the best head.
- Load previously indexed spans from the `span` tree.
- Either resume an existing tail span or index the current head block immediately.
- Start two concurrent streams of work:
  - Backfill backward toward genesis.
  - Track new head blocks forward as they arrive.

This is why the README describes the indexer as indexing backward while simultaneously tailing the head.

### Per-block indexing

`Indexer::index_block(block_number)` does the block-level work:

1. Fetch the block hash from RPC.
2. Confirm the block body is available.
3. Create a block-scoped `subxt` view with `api.at_block(hash)`.
4. Fetch and iterate decoded runtime events.
5. For each event:
   - Read pallet name, event name, pallet index, and variant index.
   - Optionally write a variant index record if `index_variant` is enabled.
   - Decode fields schema-lessly into `scale_value::Composite<()>`.
   - Derive indexing keys.
   - Write event references for each derived key.
   - Optionally store a JSON-encoded decoded event if `store_events` is enabled.

### Key derivation

`Indexer::keys_for_event(...)` uses this priority order:

1. If the pallet is marked `sdk = true`, try built-in pallet logic from `src/pallets.rs`.
2. Otherwise fall back to the resolved TOML config from `ChainConfig::build_custom_index()`.
3. If neither path yields keys, the event is ignored unless variant indexing causes it to be stored/queryable by variant.

### Why `HistoricalBlockDataUnavailable` exists

`Indexer::index_block` treats a missing block body as a hard error even if the block hash exists. This protects the process from looping forever against warp-synced or pruned nodes that cannot serve historical bodies.

## Concurrency Model

The indexer has one async loop that multiplexes several inputs with `tokio::select!`:

- exit notifications
- subscription control messages from WebSocket handlers
- new chain head notifications from `subxt`
- queued live-head indexing futures
- queued backfill indexing futures
- periodic stats logging

Two separate queues exist inside the loop:

- Backfill queue for descending historical blocks.
- Live-head queue for ascending new blocks.

`queue_depth` applies to both. The code intentionally allows multiple outstanding block indexing futures so the process can catch up faster when the node is moving ahead.

Because futures can complete out of order, the code uses orphan maps:

- `orphans` for backward backfill continuity
- `head_orphans` for forward live-head continuity

Blocks only extend the active span once continuity is satisfied.

## Span And Resume Semantics

Resume behavior is implemented by the span helpers in `src/indexer.rs`.

Each stored span means: blocks `start..=end` have been indexed with a specific indexing configuration.

Span values also persist:

- config version boundary info derived from `ChainConfig.versions`
- whether `index_variant` was enabled
- whether `store_events` was enabled

When loading spans, the indexer may discard or trim them if they are stale relative to the current run:

- If variant indexing is now enabled but was previously disabled, affected spans are removed for reindexing.
- If decoded event storage is now enabled but was previously disabled, affected spans are removed for reindexing.
- If `config.versions` indicates the chain config changed starting at some block, affected spans are removed or split so that only the stale portion is reindexed.

Important invariant:

- The active in-memory `current_span` is not always persisted immediately. On shutdown, `save_current_span(...)` persists the current progress back into the `span` tree.
- If the upstream `subxt` block stream closes because the node disconnects or exits, the indexer treats that as a graceful shutdown condition: it saves the current span, returns `Ok(())`, and lets `main` shut down the WebSocket task and flush sled.

## Chain Config Model

The TOML schema is defined in `src/config.rs`.

Top-level fields:

- `name`
- `genesis_hash`
- `default_url`
- `versions`
- `custom_keys`
- `pallets`

Pallet configuration supports two modes:

- `sdk = true`
  Use built-in Rust logic in `src/pallets.rs`.
- custom event mappings
  Explicit `field -> key` mappings in TOML.

`ParamConfig::resolve(...)` turns TOML key names into runtime `ParamKey` values:

- `ParamKey::BuiltIn(KeyTypeName)` for built-in semantic keys like `account_id` or `ref_index`
- `ParamKey::Custom { name, kind }` for user-defined keys declared in `[custom_keys]`

Important invariant:

- Unknown TOML key names are rejected at config validation time, not lazily during indexing.

## Built-in Pallet Rules

`src/pallets.rs` is a library of schema-less extractors and pallet-specific indexing rules.

The pattern is:

- Decode event fields into generic `scale_value::Value` trees.
- Extract common scalar shapes such as account IDs, u32/u64/u128 values, booleans, strings, byte arrays, or vectors.
- Emit `Key` values for the event.

If you need to add first-class support for another Polkadot SDK pallet, this is the place to start.

## Event Encoding

Decoded events are converted to JSON in `Indexer::encode_event(...)` and `composite_to_json(...)`.

The encoded payload includes:

- `specVersion`
- `palletName`
- `eventName`
- `palletIndex`
- `variantIndex`
- `eventIndex`
- `fields`

Notable encoding choices:

- `u128` and `i128` are serialized as strings to avoid JavaScript precision loss.
- Some byte-like unnamed composites are serialized as `0x...` hex strings.
- Variant values are represented as `{ "variant": ..., "fields": ... }`.

## WebSocket API Flow

The public API is implemented in `src/websockets.rs`.

### Request path

For each client connection:

1. Accept a TCP connection.
2. Upgrade it to WebSocket.
3. Read JSON requests into `RequestMessage`.
4. Either handle the request locally or forward subscription intent to the indexer loop.

### Local reads

These are answered directly from sled or RPC in `process_msg(...)`:

- `Status`
- `Variants`
- `GetEvents { key }`
- `SizeOnDisk`

`GetEvents` reads at most the most recent 100 matching event refs. If matching decoded event JSON exists in the `events` tree, it is attached as `decodedEvents`.

### Subscriptions

Subscription registration is split across tasks:

- WebSocket handlers send `SubscriptionMessage` values over an internal unbounded channel.
- The indexer loop owns the actual subscriber registries.
- When a newly indexed event matches a subscribed key, the indexer pushes a `ResponseMessage::Events` update to those subscribers.
- Status subscribers are notified when the live head advances the persisted span.

Important invariant:

- The WebSocket server does not own indexing state. It only performs read queries and forwards subscribe/unsubscribe requests to the indexer task.

## Generated Chain Configs

`src/config_gen.rs` inspects metadata from a live chain and builds a starter TOML config.

This is heuristic, not perfect. It tries to:

- detect account-like fields
- infer scalar key types
- recognize collection fields that should use `multi = true`
- mark known SDK pallets as `sdk = true`

Agents should treat generated configs as a starting point that may need cleanup for chain-specific semantics.

## Key Files For Common Changes

- Add or change CLI/runtime startup behavior:
  `src/main.rs`
- Change how spans resume or reindex:
  `src/indexer.rs`
- Add a new built-in key type or custom key storage shape:
  `src/config.rs`, `src/shared.rs`, `src/indexer.rs`
- Add built-in support for another SDK pallet:
  `src/pallets.rs`
- Change WebSocket request/response shapes:
  `src/shared.rs`, `src/websockets.rs`
- Change chain-config generation heuristics:
  `src/config_gen.rs`

## Gotchas

- Do not assume every event is stored in `events`. That only happens when `--store-events` is enabled and the event was considered indexable or variant-indexed.
- Do not assume every decoded field is named. Some event fields are positional and TOML may reference them by stringified index like `"0"`.
- Do not assume block indexing completes in numeric order. Both backfill and head processing can finish out of order and are stitched together afterward.
- Do not bypass genesis-hash checks when reusing an existing database path.
- Do not add chain-specific logic to the main loop if it can live in TOML or `src/pallets.rs` instead.
- `Key::Custom` is the main path for queryable keys, including many built-in semantic keys.

## Mental Model

The simplest correct mental model is:

1. Chain config says which event fields matter.
2. The indexer walks blocks and turns matching event fields into binary index entries.
3. Spans record which block ranges are already trustworthy for the current indexing mode.
4. The WebSocket server is a thin query/subscription layer on top of sled plus the indexer-owned subscriber lists.

If you are changing behavior, first decide which layer owns it:

- config/schema concern
- field extraction concern
- per-event indexing concern
- span/resume concern
- API/query concern
