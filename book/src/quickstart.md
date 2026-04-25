# Quickstart

This path is the fastest way to get `acuity-index` indexing a chain.

## 1. Generate A Starter Spec

```bash
acuity-index generate-index-spec ./mychain.toml --url wss://mynode:443
```

This inspects live runtime metadata and writes a starter TOML file. Metadata v14
or newer is required.

## 2. Review And Edit The Spec

At minimum, make sure the file contains the correct:

- `name`
- `genesis_hash`
- `default_url`
- declared `[keys]`
- explicit `[[pallets]]` and `[[pallets.events]]` mappings you want indexed

## 3. Run The Indexer

```bash
acuity-index run ./mychain.toml
```

Common overrides:

```bash
acuity-index run ./mychain.toml --url wss://mynode:443 --queue-depth 4 --port 8172
```

If clients need verifiable event proofs from `GetEvents`, run in finalized mode:

```bash
acuity-index run ./mychain.toml --finalized
```

## 4. Query The Service

By default, connect to:

```text
ws://localhost:8172
```

Minimal request:

```json
{"id":1,"type":"Status"}
```

## 5. Iterate Safely

When `run <INDEX_SPEC>` points to a file, `acuity-index` watches that file for
accepted changes. Valid changes restart only the RPC/indexer loop. The WebSocket
server and metrics listener stay up.

Rejected changes do not kill the running process.
