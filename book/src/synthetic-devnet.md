# Synthetic Devnet

This repository includes an in-repo Polkadot SDK runtime under `runtime/` and a
matching synthetic index-spec renderer in `src/synthetic_devnet.rs`.

The synthetic stack exists to test and benchmark the real indexer against a
deterministic local chain without depending on external networks.

## Stack Layers

1. `runtime/` builds a small runtime WASM
2. `polkadot-omni-node` runs that runtime locally
3. `src/synthetic_devnet.rs` renders the matching index spec
4. `seed_synthetic_runtime` writes deterministic on-chain data
5. `acuity-index` indexes the chain normally and is validated through its public API

The synthetic runtime now includes GRANDPA so finalized indexing and proof
responses can be exercised locally.

## Useful Commands

Start a local dev node:

```bash
just synthetic-node
```

Seed a small smoke dataset:

```bash
just seed-smoke
```

Run the external node-backed integration suite:

```bash
just test-integration
```

The ignored integration suite now covers both ordinary `GetEvents` queries and
proof-oriented requests using `includeProofs`.

- In default mode it verifies that proofs are reported as unavailable.
- In finalized mode it starts a libp2p-enabled local node, runs the indexer with
  `--finalized`, requests proofs, and verifies the returned header and storage
  proof against the block state root.

## Why This Matters

The synthetic setup does not bypass the normal indexing path. It exercises:

- RPC access
- runtime metadata decoding
- index key derivation
- `sled` persistence
- WebSocket request and notification behavior
- finalized event proof responses

That makes it the main contributor workflow for validating cross-module changes.
