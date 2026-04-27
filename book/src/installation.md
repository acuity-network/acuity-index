# Installation

## Requirements

- Rust `stable` as pinned by `rust-toolchain.toml`
- a running Substrate node with WebSocket RPC enabled
- archival historical state on that node via `--state-pruning archive-canonical`
- `polkadot-omni-node` if you want the in-repo synthetic devnet workflows

## Install The Binary

```bash
cargo install --path .
```

For local development you can also run the binary directly from the repository:

```bash
cargo run -- run ./mychain.toml
```

## Build From Source

Common project entrypoints are exposed through `just`:

```bash
just build
just test
just build-release
```

## Related Tooling

- Main binary: Acuity Index (`acuity-index`)
- Synthetic workload seeder: `seed_synthetic_runtime`
- Synthetic benchmark harness: `benchmark_synthetic_indexing`

If you plan to work on the docs book itself, install `mdbook` too:

```bash
cargo install mdbook
```
