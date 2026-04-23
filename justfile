set shell := ["bash", "-euo", "pipefail", "-c"]

runtime_manifest := "runtime/Cargo.toml"
runtime_wasm := "runtime/target/release/wbuild/synthetic-runtime/synthetic_runtime.wasm"
runtime_chain_spec := "runtime/target/synthetic-dev-chain-spec.json"

default:
    @just --list

build:
    cargo build

build-release:
    cargo build --release

test:
    cargo test

runtime-build:
    cargo build --release --manifest-path "{{runtime_manifest}}"

runtime-chain-spec: runtime-build
    command -v polkadot-omni-node >/dev/null
    polkadot-omni-node chain-spec-builder --chain-spec-path "{{runtime_chain_spec}}" create -t development --para-id 1000 --relay-chain rococo-local --runtime "{{runtime_wasm}}" named-preset development

synthetic-node rpc_port='9944': runtime-chain-spec
    command -v polkadot-omni-node >/dev/null
    polkadot-omni-node --chain "{{runtime_chain_spec}}" --dev --instant-seal --pool-type single-state --state-pruning archive-canonical --blocks-pruning archive-canonical --rpc-port "{{rpc_port}}" --prometheus-port 0 --no-prometheus --port 0

seed-smoke url='ws://127.0.0.1:9944':
    cargo run --bin seed_synthetic_runtime -- --url "{{url}}" --mode smoke

seed-bulk url='ws://127.0.0.1:9944' batch_start='1000' batches='100' burst_count='64':
    cargo run --bin seed_synthetic_runtime -- --url "{{url}}" --mode bulk --batch-start "{{batch_start}}" --batches "{{batches}}" --burst-count "{{burst_count}}"

test-integration:
    cargo test --test synthetic_integration -- --ignored --nocapture

benchmark-indexing rpc_port='9944' queue_depth='4' batch_start='1000' batches='1000' burst_count='128' timeout_secs='600': runtime-chain-spec
    #!/usr/bin/env bash
    set -euo pipefail
    command -v polkadot-omni-node >/dev/null
    cargo build --release --bins
    workdir="$(mktemp -d)"
    cleanup() {
      if [[ -n "${node_pid:-}" ]]; then
        kill "${node_pid}" 2>/dev/null || true
        wait "${node_pid}" 2>/dev/null || true
      fi
    }
    trap cleanup EXIT
    polkadot-omni-node --chain "{{runtime_chain_spec}}" --dev --dev-block-time 100 --pool-type single-state --state-pruning archive-canonical --blocks-pruning archive-canonical --rpc-port "{{rpc_port}}" --prometheus-port 0 --no-prometheus --port 0 >"$workdir/node.log" 2>&1 &
    node_pid=$!
    cargo run --release --bin seed_synthetic_runtime -- --url "ws://127.0.0.1:{{rpc_port}}" --mode bulk --batch-start "{{batch_start}}" --batches "{{batches}}" --burst-count "{{burst_count}}" --output "$workdir/seed-manifest.json"
    cargo run --release --bin benchmark_synthetic_indexing -- --node-url "ws://127.0.0.1:{{rpc_port}}" --manifest "$workdir/seed-manifest.json" --queue-depth "{{queue_depth}}" --timeout-secs "{{timeout_secs}}" --workdir "$workdir"
