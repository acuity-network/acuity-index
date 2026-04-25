# Architecture

`acuity-index` is a config-driven event indexer for Substrate chains. It decodes
runtime events with `subxt`, derives query keys from TOML config, stores index
entries in `sled`, and serves query access over WebSocket.

## Main Components

- `src/main.rs`: startup, config loading, database initialization, watcher setup,
  metrics listener, and reconnect supervisor loop
- `src/indexer.rs`: indexing pipeline, resume logic, live-head tailing, event key
  derivation, notification fanout
- `src/config.rs`: TOML schema and mapping resolution
- `src/websockets.rs`: public API implementation, connection lifecycle, and optional finalized proof inclusion for `GetEvents`
- `src/shared.rs`: wire types, sled key layouts, shared runtime state, and finalized-mode gating for proof responses
- `src/event_hydration.rs`: decoded-event hydration plus finalized `System.Events` proof fetching
- `src/config_gen.rs`: live metadata to starter spec generation
- `src/metrics.rs`: metrics registry and HTTP export
- `src/synthetic_devnet.rs`: synthetic local chain helpers and shared test types

The repository also includes an in-repo synthetic runtime workspace under
`runtime/`, a checked-in example spec at [`acuity.toml`](../../acuity.toml), plus
node-backed integration and benchmarking paths.

## Startup Sequence

The normal startup path is:

1. parse CLI args
2. load the required index spec
3. validate config and resolve runtime options
4. open `sled`
5. verify or initialize `genesis_hash`
6. start long-lived tasks such as WebSocket serving and spec watching
7. enter the RPC reconnect and indexing supervisor loop

The long-lived tasks created before entering the supervisor loop are important:

- a bounded subscription dispatcher task
- a single process-lifetime WebSocket listener
- an optional metrics listener serving `/metrics`
- an index-spec watcher for accepted file changes

The shared runtime state also tracks whether the current run is indexing
finalized blocks so WebSocket proof responses stay aligned with the active mode.

## Data Model

The `sled` database is organized into trees opened by `Trees::open` in
`src/shared.rs`:

- `root`: database-level values such as `genesis_hash`
- `span`: indexed block spans for resume and reindex logic
- `variant`: event references keyed by pallet and variant indices
- `index`: custom and built-in query keys with `(block_number, event_index)` suffixes
- `events`: decoded event JSON keyed by `(block_number, event_index)`

The two main query surfaces are:

- variant queries via the `variant` tree
- all other key queries via the `index` tree

`Key::Custom` covers every declared query key from `[keys]` in the TOML spec.

## Indexing Flow

The main indexing loop lives in `run_indexer` in `src/indexer.rs`.

High-level behavior:

- determine the starting head block
- load previously indexed spans
- resume an existing tail span or index the current head immediately
- run backward backfill and live-head tracking concurrently

If finalized mode is enabled, the same finalized-only setting also governs
whether API callers can receive verifiable proof material for `GetEvents`.

Per-block indexing follows this shape:

1. fetch block hash from RPC
2. create a block-scoped `subxt` view with `api.at_block(hash)`
3. fetch and iterate decoded runtime events
4. for each event:
5. read pallet name, event name, pallet index, and variant index
6. optionally write a variant index record if `index_variant` is enabled
7. decode fields schema-lessly into `scale_value::Composite<()>`
8. derive indexing keys from explicit config
9. write event references for each derived key
10. store event refs locally and hydrate decoded event payloads from the node when queries or subscriptions need them

When `GetEvents` requests `includeProofs = true`, the WebSocket layer also asks
for one proof object per returned block. That proof is built from the block
header plus `state_get_read_proof` over the `System.Events` storage key.

Malformed persisted sled data is handled defensively during decode. Corrupt span
records or malformed index keys are skipped with logging instead of panicking.

## Historical State Requirement

`Indexer::index_block` requires historical state to be available for
`api.at_block(hash)`. If the node prunes historical state, the process exits with
an explicit misconfiguration error and instructs the operator to run the node
with `--state-pruning archive-canonical`.

## Concurrency Model

The indexer uses one async loop that multiplexes several inputs with
`tokio::select!`:

- exit notifications
- new chain head notifications from `subxt`
- queued live-head indexing futures
- queued backfill indexing futures
- periodic stats logging

Two queues are maintained inside the loop:

- backfill queue for descending historical blocks
- live-head queue for ascending new blocks

`queue_depth` applies to both queues. Multiple outstanding block-indexing futures
are allowed so the process can catch up against a fast-moving node.

Because futures can complete out of order, the code uses orphan maps:

- `orphans` for backfill continuity
- `head_orphans` for live-head continuity

Blocks only extend active spans once contiguity is satisfied.

## Catchup And Rapidly Syncing Nodes

When a node jumps ahead quickly, the indexer does not poll for lag. It reacts to
new announced head blocks, updates `latest_seen_head`, and fills the live-head
queue immediately up to `queue_depth`.

This is the main reason `--queue-depth` is the primary tuning lever for catchup.
The same mechanism keeps both head-following and backward backfill saturated.

## Span And Resume Semantics

Each stored span means a contiguous `start..=end` block range has been indexed
for a specific config revision.

Span values persist revision boundary information derived from
`IndexSpec.spec_change_blocks`.

When loading spans, the indexer may trim or discard stale sections if the active
spec introduces new historical revision boundaries.

Important invariants:

- the active in-memory span is not always persisted immediately
- on shutdown or recoverable failure, `save_current_span(...)` persists progress
- if the upstream block stream closes, the indexer returns a recoverable error so
  the supervisor loop can reconnect and resume

Changes to `index_variant` only trigger historical reindexing
when the spec revision advances via `spec_change_blocks`.

## RPC Reconnection And Spec Reload

`src/main.rs` wraps the indexer in a supervisor loop that handles transient RPC
failures and accepted spec reloads without data loss.

Recoverable errors trigger reconnect with exponential backoff. Fatal errors cause
process exit.

Supervisor behavior, in practice:

1. derive the effective RPC URL from the latest accepted config snapshot
2. attempt RPC connection
3. verify chain genesis hash
4. publish the fresh RPC handle into shared runtime state
5. spawn the indexer task
6. wait for signals, watcher updates, or indexer completion
7. on accepted spec reload, stop only the current indexer and restart it
8. on recoverable indexer failure, reconnect and resume
9. on fatal failure, log and exit

The spec watcher validates the entire updated file before publishing it and
rejects changes to `name` or `genesis_hash`.

## Synthetic Devnet Architecture

The local synthetic stack stays intentionally close to production architecture.

Layers:

1. `runtime/` builds a small Polkadot SDK runtime WASM
2. `polkadot-omni-node` runs that runtime locally
3. `src/synthetic_devnet.rs` renders the matching index spec
4. `seed_synthetic_runtime` writes deterministic chain data
5. tests and benchmarks validate the public WebSocket API against the real indexer

This is not a mocked shortcut. It exercises the normal RPC, metadata decoding,
indexing, persistence, and query surfaces end to end.

For proof-oriented tests, the local node is started in a libp2p-enabled mode
instead of instant-seal dev mode so finalized proof verification can run against a
more realistic finalized-chain setup.

## Invariants

- a database path belongs to exactly one chain identity
- accepted spec reloads should not take down the public service
- the synthetic test path should stay close to production architecture
- public API correctness is validated end to end, not only through unit tests
