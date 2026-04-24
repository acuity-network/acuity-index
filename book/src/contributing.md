# Contributing

## Development Entry Points

The repository uses `just` as the main developer command surface:

```bash
just build
just test
just runtime-build
just synthetic-node
just seed-smoke
just test-integration
just benchmark-indexing
```

## Code Areas

High-level module boundaries:

- `src/main.rs`: CLI, startup, supervisor loop, long-lived tasks
- `src/indexer.rs`: indexing pipeline and span tracking
- `src/config.rs`: TOML schema and runtime mapping resolution
- `src/websockets.rs`: public WebSocket API and connection lifecycle
- `src/shared.rs`: shared types, key encodings, sled trees, runtime state
- `src/metrics.rs`: Prometheus/OpenMetrics integration
- `src/config_gen.rs`: starter index spec generation from metadata
- `src/synthetic_devnet.rs`: synthetic local chain support

## Suggested Workflow

1. run unit tests with `just test`
2. use the synthetic devnet when your change affects observable runtime behavior
3. run ignored integration tests when changing indexing, API, or reconnect logic
4. benchmark with the synthetic harness when changing throughput-sensitive code

## Documentation Guidance

Keep user-facing behavior documented close to the change:

- update the book for narrative/operator guidance
- update `README.md` for the short entrypoint experience
- update `API.md`, `ARCHITECTURE.md`, or `SECURITY.md` when the detailed source
  references change

## Testing Philosophy

This repository has both unit-style code coverage and a heavier end-to-end path.
The synthetic runtime exists specifically to exercise the real indexing path
against a deterministic local chain.
