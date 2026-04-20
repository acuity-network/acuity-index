## Overview

`acuity-index` is a config-driven event indexer for Substrate chains. It connects to a node over WebSocket RPC, decodes runtime events with `subxt`, derives query keys from built-in pallet rules or TOML config, stores index entries in `sled`, and exposes read/query access over its own WebSocket API.

The design is intentionally schema-light:

- Chain-specific behavior lives mostly in TOML.
- Event payloads are decoded generically with `scale_value` instead of generated Rust types.
- The on-disk index is append-oriented and keyed by queryable event attributes.

This file is meant to help AI agents quickly find the right code and understand the main invariants before making changes.

## Runtime Components

- `src/main.rs`
  Parses CLI args, loads index spec and options config, opens sled, verifies genesis hash, starts long-lived subscription/WebSocket tasks, and runs a reconnect loop that only recreates the RPC clients plus indexer task with exponential backoff on transient failures.
- `src/indexer.rs`
  Owns the indexing pipeline, span tracking, resume logic, live-head tailing, event key derivation, decoded event storage, and notification fanout into shared subscriber state. Saves the current span before returning on any error so the reconnect loop can resume without data loss.
- `src/config.rs`
  Defines the TOML schema and resolves configured event params into runtime key-mapping rules.
- `src/pallets.rs`
  Contains built-in indexing rules for supported Polkadot SDK pallets and helpers for extracting scalar values from schema-less event fields.
- `src/websockets.rs`
  Implements the public WebSocket API, request/response handling, and connection lifecycle.
- `src/shared.rs`
  Holds shared wire types, on-disk key formats, database tree handles, shared runtime state, and common enums like `Key`, `RequestMessage`, and `ResponseMessage`.
- `src/config_gen.rs`
  Builds starter index spec TOML files from live runtime metadata.
- `src/synthetic_devnet.rs`
  Shared support code for the in-repo synthetic devnet. It renders a synthetic index spec from a template, polls node/indexer readiness, provides small WebSocket API probe helpers, and defines the manifest/report types shared by the seeder, benchmark harness, and integration tests.
- `src/bin/seed_synthetic_runtime.rs`
  Deterministic chain seeder for the synthetic runtime. It submits known transactions in `smoke` or `bulk` mode and emits a `SeedManifest` describing the expected indexed queries that tests and benchmarks later validate through the public API.
- `src/bin/benchmark_synthetic_indexing.rs`
  End-to-end benchmark harness for the synthetic runtime. It starts `acuity-index` against a rendered synthetic config, waits until the manifest queries are observable through `GetEvents`, and emits a `BenchmarkReport` with throughput and on-disk size metrics.
- `runtime/`
  Separate in-repo Polkadot SDK runtime workspace used for local devnet testing. It builds a WASM runtime consumed by `polkadot-omni-node` and includes the custom `Synthetic` pallet used to exercise indexed key shapes.
- `tests/common/mod.rs`
  Process orchestration helpers for the synthetic integration test. It builds the runtime, generates a chain spec, starts the local node and indexer, runs the seeder, and manages child-process cleanup.
- `tests/synthetic_integration.rs`
  Ignored external integration test that validates the full synthetic stack end to end: runtime build, local node, indexer startup, deterministic seeding, and WebSocket API query verification.

## Synthetic Devnet Architecture

A self-contained local devnet stack lets the repository test and benchmark the real indexer against a deterministic Substrate chain without depending on external networks.

The stack has five layers:

1. `runtime/` builds a small Polkadot SDK runtime WASM with standard system pallets plus a custom `Synthetic` pallet.
2. `polkadot-omni-node` runs that WASM runtime locally from a generated chain spec.
3. `chains/synthetic.toml` defines the matching index specification template for the synthetic pallet events and built-in SDK pallets.
4. `src/bin/seed_synthetic_runtime.rs` writes deterministic on-chain data that should become queryable through the indexer.
5. `acuity-index` indexes that chain normally, and tests/benchmarks validate the result through the public WebSocket API.

This is intentionally close to production architecture. The synthetic setup does not bypass the normal indexer path; it exercises the same RPC, metadata decoding, indexing, sled persistence, and WebSocket query surfaces as a real deployment.

### Synthetic runtime

The in-repo runtime exists to make local validation deterministic and self-contained.

- It is built as a separate Cargo workspace under `runtime/`.
- `runtime/pallets/synthetic` defines a small custom pallet that emits predictable events covering the key shapes the indexer needs to exercise locally: `u32`, `bytes32`, `account_id`, and repeated multi-value fields.
- `chains/synthetic.toml` mirrors those event shapes as index rules so the indexer can query the seeded data through normal custom-key and built-in-key lookups.

The runtime keeps a small pallet set on purpose. It includes enough standard SDK pallets to validate built-in indexing paths such as `Balances` and `TransactionPayment`, while the custom `Synthetic` pallet provides deterministic event-heavy workloads for integration tests and benchmarks.

### Synthetic config generation

The synthetic index spec checked into `chains/synthetic.toml` is a template, not a directly usable chain config.

- The template contains a placeholder genesis hash and default local RPC URL.
- `src/synthetic_devnet.rs` loads that template and rewrites `genesis_hash` plus `default_url` for the currently running local node.
- Tests and benchmark tooling write that rendered config into a temporary working directory before launching `acuity-index`.

This keeps the checked-in config stable while still making the synthetic workflow safe for disposable local chains whose genesis hash is only known after the chain spec/runtime is materialized.

### Why `polkadot-omni-node --instant-seal`

The synthetic workflow uses `polkadot-omni-node` in dev mode with `--instant-seal`.

- Each submitted extrinsic is included promptly and deterministically.
- Seeder-generated manifests can refer to stable expected block ranges and query results.
- Benchmark runs can seed a known amount of work first, then measure how long `acuity-index` takes to index that fixed workload from an empty database.

The node is still required to run with archival pruning settings because the indexer's normal historical-state requirement remains in force even for the synthetic chain.

## Synthetic Seeding And Benchmark Flow

The synthetic benchmark path is organized around durable artifacts rather than ad hoc shell scripting.

### Seed manifest

`src/bin/seed_synthetic_runtime.rs` is the source of truth for the seeded workload.

- In `smoke` mode it submits a small set of hand-picked transactions that exercise several query shapes.
- In `bulk` mode it submits many `Synthetic::emit_burst` transactions to generate an event-dense benchmark workload.
- After submission it emits a `SeedManifest` JSON document containing:
  - chain identity (`genesis_hash`)
  - seeded block range
  - transaction and synthetic event counts
  - a list of `QueryExpectation` values describing which WebSocket `GetEvents` lookups must succeed

That manifest is the contract between the seeder and downstream validation code. Tests and benchmarks do not hardcode their own expectations independently; they consume the manifest produced by the actual seeding pass.

### Benchmark harness

`src/bin/benchmark_synthetic_indexing.rs` consumes a `SeedManifest` and measures the real indexer.

Its flow is:

1. Read the seeder manifest JSON.
2. Render a temporary synthetic index config whose `genesis_hash` and node URL match the seeded local chain.
3. Start `acuity-index` as a child process against an empty database.
4. Poll `GetEvents` over the indexer's public WebSocket API until every manifest query passes.
5. Query `SizeOnDisk` and compute elapsed-time throughput metrics.
6. Emit a `BenchmarkReport` JSON document.

This means benchmark success is defined by observable API behavior, not only by internal logs or process completion. The benchmark verifies that the indexer has actually made the seeded data queryable.

## Synthetic Integration Testing

The repository now has an external node-backed integration test in `tests/synthetic_integration.rs`.

This test is marked `#[ignore]` because it requires heavyweight local tooling (`polkadot-omni-node` and a release runtime build), but its role is architectural rather than incidental: it is the full end-to-end validation path for the synthetic stack.

The integration test uses helpers from `tests/common/mod.rs` to orchestrate this pipeline:

1. Build the synthetic runtime in release mode.
2. Generate a local chain spec from the runtime WASM.
3. Start `polkadot-omni-node` on random local ports.
4. Render a matching synthetic index config for that node's genesis hash.
5. Start `acuity-index` against a temporary database.
6. Run the smoke seeder and capture its `SeedManifest`.
7. Wait until the indexer reports spans covering the seeded tip.
8. Execute the manifest's `GetEvents` expectations against the public WebSocket API.

Because this path validates the actual WebSocket interface, it catches integration breakage across runtime metadata, config rendering, indexing, persistence, and query handling in one place.

## Developer Orchestration

The `justfile` is now part of the architecture for the synthetic workflow, not only a convenience wrapper.

- `runtime-build` and `runtime-chain-spec` materialize the local runtime artifacts.
- `synthetic-node` starts the local omni-node instance with the required archival settings.
- `seed-smoke` and `seed-bulk` produce deterministic seeded workloads.
- `test-integration` runs the ignored end-to-end synthetic integration test.
- `benchmark-indexing` orchestrates runtime build, node startup, bulk seeding, and benchmark execution in one reproducible command.

These recipes are the supported developer entrypoints for the synthetic stack. The binaries and helper module are structured so that the same underlying flow can be used interactively from `just`, from tests, or from future CI automation without duplicating the workflow logic.

## Startup Sequence

The normal startup path lives in `src/main.rs`:

1. Parse CLI args.
2. Load a built-in index spec TOML or a user-supplied `--index-config` index specification.
3. Validate the spec and resolve runtime options (CLI > `--options-config` > defaults) plus indexing flags (CLI OR spec).
4. Resolve the database path and open sled.
5. Verify or initialize the stored `genesis_hash` in the root database.
6. Create shared runtime state plus long-lived tasks:
   a. Spawn a bounded subscription dispatcher task that applies subscribe/unsubscribe messages to shared in-memory registries.
   b. Spawn `websockets_listen(...)` once for the process lifetime.
7. Enter the reconnect loop (see [RPC Reconnection](#rpc-reconnection) below). On each iteration:
   a. Connect to the target node with `subxt` (retry with exponential backoff on transient failures).
   b. Verify the connected chain genesis hash matches the config.
   c. Publish the current RPC handle into shared runtime state.
   d. Spawn `run_indexer(...)`.
   e. Wait for either a termination signal or indexer completion, then act accordingly.

Startup error handling is centralized in a fallible `run()` path in `src/main.rs`. Configuration, database, and signal-registration failures return structured errors that are logged once before the process exits.

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
2. Create a block-scoped `subxt` view with `api.at_block(hash)`.
3. Fetch and iterate decoded runtime events.
4. For each event:
    - Read pallet name, event name, pallet index, and variant index.
    - Optionally write a variant index record if `index_variant` is enabled.
    - Decode fields schema-lessly into `scale_value::Composite<()>`.
    - Derive indexing keys.
     - Write event references for each derived key.
     - Optionally store a JSON-encoded decoded event if `store_events` is enabled.

Operational detail:

- Runtime decoding of persisted sled keys/values is now defensive. Malformed span records or malformed index keys are skipped with error logging rather than panicking the process.

### Key derivation

`Indexer::keys_for_event(...)` uses this priority order:

1. If the pallet is marked `sdk = true`, try built-in pallet logic from `src/pallets.rs`.
2. Otherwise fall back to the resolved TOML config from `IndexSpec::build_custom_index()`.
3. If neither path yields keys, the event is ignored unless variant indexing causes it to be stored/queryable by variant.

### Historical state requirement

`Indexer::index_block` requires historical state to be available for `api.at_block(hash)`. If the connected node has pruned historical state, the indexer exits with `StatePruningMisconfigured` and instructs the operator to run the node with `--state-pruning archive-canonical`.

## Concurrency Model

The indexer has one async loop that multiplexes several inputs with `tokio::select!`:

- exit notifications
- new chain head notifications from `subxt`
- queued live-head indexing futures
- queued backfill indexing futures
- periodic stats logging

Subscription control messages are handled by a separate long-lived dispatcher task in `src/main.rs`, which updates shared subscriber registries that outlive individual indexer reconnects.

Two separate queues exist inside the loop:

- Backfill queue for descending historical blocks.
- Live-head queue for ascending new blocks.

`queue_depth` applies to both. The code intentionally allows multiple outstanding block indexing futures so the process can catch up faster when the node is moving ahead.

Because futures can complete out of order, the code uses orphan maps:

- `orphans` for backward backfill continuity
- `head_orphans` for forward live-head continuity

Blocks only extend the active span once continuity is satisfied.

## Catchup With a Rapidly Syncing Node

The indexer does not poll for "am I behind?". Instead the node pushes new block numbers over the WebSocket subscription and the indexer reacts immediately.

### Live-head gap filling

When a new head block is received (`src/indexer.rs:996-1022`), `latest_seen_head` is updated to `max(latest_seen_head, announced_block)`. Then `queue_head_blocks` (`src/indexer.rs:723-738`) fills the live-head queue:

```rust
while head_futures.len() < queue_depth && *next_head_to_queue <= latest_seen_head {
    // queue index_block(next_head_to_queue)
    *next_head_to_queue += 1;
}
```

If the node jumps ahead by N blocks (e.g. during warp sync or a burst of finalization), all N blocks are queued immediately, up to `queue_depth` concurrently. The queue is refilled each time a future completes, so the pipeline stays saturated until the gap is closed.

### Out-of-order completion

Concurrent futures may complete out of order. Two orphan maps hold results that arrived early:

- `head_orphans` (`src/indexer.rs:958`) — for live-head results. `process_queued_head_result` (`src/indexer.rs:797-844`) advances `current_span.end` only once the contiguous chain from the previous high-water mark is assembled.
- `orphans` (`src/indexer.rs:969`) — for backfill results. `advance_backfill_start` (`src/indexer.rs:767-795`) enforces the same contiguity invariant in the descending direction.

Blocks park in the orphan map until their predecessor is confirmed, then the map is drained contiguously.

### Concurrent backfill

While the live-head queue is catching up, the backfill queue descends from the starting head toward genesis at the same time, also running up to `queue_depth` concurrent futures. Already-indexed ranges are skipped by `check_next_batch_block` (`src/indexer.rs:640-651`). When the descending backfill reaches a previously indexed span, `check_span` (`src/indexer.rs:608-634`) merges them.

### Primary tuning lever

`--queue-depth` (default `1`, overridable via CLI or `--options-config`) controls how many concurrent block-indexing RPC calls are issued for both queues. Increasing it is the main way to speed up catchup against a node that is moving quickly.

## Span And Resume Semantics

Resume behavior is implemented by the span helpers in `src/indexer.rs`.

Each stored span means: blocks `start..=end` have been indexed with a specific indexing configuration.

Span values also persist:

- config version boundary info derived from `IndexSpec.versions`
- whether `index_variant` was enabled
- whether `store_events` was enabled

When loading spans, the indexer may discard or trim them if they are stale relative to the current run:

- If variant indexing is now enabled but was previously disabled, affected spans are removed for reindexing.
- If decoded event storage is now enabled but was previously disabled, affected spans are removed for reindexing.
- If `spec.versions` indicates the index spec changed starting at some block, affected spans are removed or split so that only the stale portion is reindexed.

Important invariants:

- The active in-memory `current_span` is not always persisted immediately. On shutdown or recoverable error, `save_current_span(...)` persists the current progress back into the `span` tree before the indexer task returns.
- If the upstream `subxt` block stream closes because the node disconnects or exits, the indexer saves the current span and returns `BlockStreamClosed` so the reconnect loop in `src/main.rs` can re-establish the connection and resume indexing.

## RPC Reconnection

The indexer is wrapped in a reconnect loop in `src/main.rs` that handles transient RPC failures without data loss.

### Error classification

`IndexError::is_recoverable()` (`src/shared.rs`) classifies each error variant:

- **Recoverable**: `Subxt`, `RpcError`, `BlocksError`, `BlockStreamClosed`, `EventsError`, `OnlineClientAtBlockError`, `OnlineClientError`, `BlockNotFound` — these indicate transient network or node issues and trigger reconnection.
- **Fatal**: `Sled`, `Tungstenite`, `Hex`, `StatePruningMisconfigured`, `CodecError`, `MetadataError`, `Json`, `Io`, `TomlSer`, `Internal` — these indicate configuration or data integrity problems and cause the process to exit.

### Reconnect loop behavior

On each iteration:

1. Check if SIGTERM was received between loops (via `term_now` atomic flag).
2. Attempt RPC connection via `connect_rpc()`. Transient failures log the error and sleep with exponential backoff before retrying. Fatal connection errors exit immediately.
3. Verify genesis hash. A mismatch is fatal (exits with an error).
4. Publish the fresh RPC handle into shared runtime state and spawn the indexer task.
5. `select!` on the signal handler vs the indexer task handle.
6. On SIGTERM: clear the shared RPC handle, send the exit signal, await the indexer and WebSocket listener tasks, flush sled, and return `Ok(())`.
7. On indexer completion:
   - `Ok(())` — clean shutdown (e.g. exit signal received), return `Ok(())`.
   - Recoverable error — clear the shared RPC handle, log the error, sleep with backoff, and `continue` the loop to reconnect.
   - Fatal error — log and return `Err`.
   - `JoinError` (panic) — log and return a fatal `Internal` error.

### Backoff timing

- Initial delay: 1 second (`INITIAL_BACKOFF_SECS`)
- Maximum delay: 60 seconds (`MAX_BACKOFF_SECS`)
- The delay doubles after each consecutive failure and resets to 1 second on a successful connection.

During backoff sleeps, SIGTERM is checked via `signals.next()` so the process exits promptly rather than waiting for the full backoff duration.

### Span state preservation

Because `run_indexer` saves the current span before returning on any error path, the reconnect loop preserves all committed indexing progress. On reconnection, `load_spans` reconstructs the span state from sled, and any blocks not covered by a span are re-indexed. This makes the reconnection path idempotent.

## Index Spec Model

The TOML schema for `IndexSpec` is defined in `src/config.rs`.

Top-level fields:

- `name`
- `genesis_hash`
- `default_url`
- `index_variant`
- `store_events`
- `versions`
- `custom_keys`
- `pallets`

Runtime options (`url`, `db_path`, `db_mode`, `db_cache_capacity`, `queue_depth`,
`finalized`, `port`, `max_connections`, `max_total_subscriptions`,
`max_subscriptions_per_connection`, `subscription_buffer_size`,
`subscription_control_buffer_size`, `idle_timeout_secs`, `max_events_limit`)
are loaded from a separate `OptionsConfig` TOML file via the
`--options-config` CLI flag. At startup, `resolve_args()` merges those values with
**CLI flags > `--options-config` file > built-in defaults** precedence. `finalized`
uses OR logic: `cli_flag || options_flag.unwrap_or(false)`. `index_variant` and
`store_events` come from `IndexSpec` and are enabled when either the CLI flag or
the spec field is `true`.

Pallet configuration supports two modes:

- `sdk = true`
  Use built-in Rust logic in `src/pallets.rs`.
- custom event mappings
  Explicit `field -> key` mappings in TOML.

`ParamConfig::resolve(...)` turns TOML key names into runtime `ParamKey` values:

- `ParamKey::BuiltIn(KeyTypeName)` for built-in semantic keys like `account_id` or `ref_index`
- `ParamKey::Custom { name, kind }` for scalar user-defined keys declared in `[custom_keys]`
- `ParamKey::CompositeCustom { name, fields }` for binary composite user-defined keys built from multiple event fields

Custom event params may use either `field = "..."` for scalar/built-in keys or
`fields = ["...", "..."]` for composite custom keys. Composite keys are encoded
as ordered binary tuples by the indexer and queried through the same `Key::Custom`
API surface.

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
2. Attempt to upgrade it to WebSocket.
3. Read JSON requests into `RequestMessage`.
4. Either handle the request locally or forward subscription intent to the shared subscription dispatcher.

Important operational details:

- The listener enforces WebSocket frame and message size limits during handshake/runtime.
- If the global connection cap is exhausted, the upgrade is rejected with HTTP `503 Service Unavailable`.
- Each accepted connection is subject to an idle timeout and a per-connection subscription cap. Setting `idle_timeout_secs = 0` disables the timeout.
- There is also a global total subscription cap across all connections. When exceeded, new subscriptions are rejected with a request-scoped `subscription_limit` error. Because the subscription was never established, no `subscriptionTerminated` notification is sent for that initial rejection.

### Local reads

These are answered directly from sled or RPC in `process_msg(...)`:

- `Status`
- `Variants`
- `GetEvents { key }`
- `SizeOnDisk`

`GetEvents` reads at most the most recent 100 matching event refs. If matching decoded event JSON exists in the `events` tree, it is attached as `decodedEvents`.

Operational detail:

- `Status`, `GetEvents`, and `SizeOnDisk` are sled-backed and continue working while the node RPC is unavailable.
- `Variants` requires a live RPC handle. During reconnect backoff it returns a request-scoped `temporarily_unavailable` error instead of disconnecting the client.

### Subscriptions

Subscription registration is split across tasks:

- WebSocket handlers send `SubscriptionMessage` values over an internal bounded channel and wait for the dispatcher to accept or reject the request before replying.
- A long-lived subscription dispatcher task owns updates to the actual subscriber registries.
- When a newly indexed event matches a subscribed key, the indexer pushes a `ResponseMessage::Events` update to those subscribers.
- Status subscribers are notified when the live head advances the persisted span.

Current WebSocket-side safeguards:

- subscribe/unsubscribe forwarding uses bounded backpressure rather than unbounded buffering
- per-connection subscriptions are capped (configurable: `--max-subscriptions-per-connection`)
- total subscriptions across all connections are capped (configurable: `--max-total-subscriptions`)
- invalid custom key payloads are rejected with request-scoped `invalid_request` responses before subscription registration or sled scans
- custom key names are limited to `128` bytes and string values to `1024` bytes
- composite values are limited to `64` elements, `8` nesting levels, and `16384` encoded bytes
- idle connections are disconnected automatically (configurable: `--idle-timeout-secs`)
- malformed persisted event/span index records are skipped during reads instead of crashing the server

All WebSocket operational parameters are configurable via CLI flags or `--options-config` TOML:

| Parameter | CLI flag | Default |
|---|---|---|
| max connections | `--max-connections` | 1024 |
| max total subscriptions | `--max-total-subscriptions` | 65536 |
| max subscriptions per connection | `--max-subscriptions-per-connection` | 128 |
| subscription buffer size | `--subscription-buffer-size` | 256 |
| subscription control buffer | `--subscription-control-buffer-size` | 1024 |
| idle timeout | `--idle-timeout-secs` | 300 |
| max events per query | `--max-events-limit` | 1000 |

Protocol-safety constants that remain compile-time:

- max WebSocket message size (`256 KiB`)
- max WebSocket frame size (`64 KiB`)
- max custom key name length (`128 bytes`)
- max custom string value length (`1024 bytes`)
- max composite depth (`8`) and elements (`64`)

Important invariant:

- The WebSocket server does not own indexing state. It performs read queries and forwards subscribe/unsubscribe requests to shared runtime state that survives indexer reconnects.

## Generated Chain Configs

`src/config_gen.rs` inspects metadata from a live chain and builds a starter TOML config.

It currently requires runtime metadata v14 or higher. The generator relies on
`subxt` to convert the raw `state_getMetadata` payload into the metadata model
used for pallet/event inspection, and that conversion path does not support the
legacy v13 payloads returned by older runtimes.

In practice this limitation usually appears when a node is still syncing early
chain history and its current best block is still running a pre-upgrade runtime.
The generator checks the raw metadata version first and returns a targeted error
that explains the likely sync/runtime-upgrade cause.

This is heuristic, not perfect. It tries to:

- detect account-like fields
- infer scalar key types
- recognize collection fields that should use `multi = true`
- mark known SDK pallets as `sdk = true`

Agents should treat generated configs as a starting point that may need cleanup for chain-specific semantics.

## Key Files For Common Changes

- Add or change CLI/runtime startup behavior or reconnection logic, or change how config options are resolved and merged:
  `src/main.rs`
- Change how spans resume or reindex:
  `src/indexer.rs`
- Add a new built-in key type or custom key storage shape:
  `src/config.rs`, `src/shared.rs`, `src/indexer.rs`
- Add built-in support for another SDK pallet:
  `src/pallets.rs`
- Change how operational/WebSocket parameters are resolved and merged, or add new CLI flags:
  `src/main.rs`, `src/config.rs`, `src/shared.rs` (for `WsConfig`)
- Change WebSocket request/response shapes or connection limits:
  `src/shared.rs`, `src/websockets.rs`
- Change index-spec generation heuristics:
  `src/config_gen.rs`

## Gotchas

- Do not assume every event is stored in `events`. That only happens when `--store-events` is enabled and the event was considered indexable or variant-indexed.
- Do not assume every decoded field is named. Some event fields are positional and TOML may reference them by stringified index like `"0"`.
- Do not assume block indexing completes in numeric order. Both backfill and head processing can finish out of order and are stitched together afterward.
- Do not bypass genesis-hash checks when reusing an existing database path.
- Do not add chain-specific logic to the main loop if it can live in TOML or `src/pallets.rs` instead.
- `Key::Custom` is the main path for queryable keys, including many built-in semantic keys.
- Recoverable RPC errors trigger reconnection, not process exit. If you need the process to stop on a specific error, classify it as fatal in `IndexError::is_recoverable()`.

## Mental Model

The simplest correct mental model is:

1. Chain config says which event fields matter.
2. The indexer walks blocks and turns matching event fields into binary index entries.
3. Spans record which block ranges are already trustworthy for the current indexing mode.
4. The reconnect loop in `src/main.rs` recreates RPC clients and the indexer task; transient RPC failures trigger reconnection with exponential backoff, preserving all span state through sled.
5. The WebSocket server is a thin query/subscription layer on top of sled plus shared subscriber lists that survive reconnects.

If you are changing behavior, first decide which layer owns it:

- config/schema concern
- field extraction concern
- per-event indexing concern
- span/resume concern
- reconnection concern
- API/query concern
