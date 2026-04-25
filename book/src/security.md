# Security And Deployment

`acuity-index` is designed to expose a public WebSocket service, so deployment
controls matter as much as application behavior.

## Current Hardening

The service currently includes:

- bounded subscription control queues
- WebSocket frame and message size limits
- bounded custom key sizes in request handling
- a global connection cap
- a per-connection subscription cap
- idle connection timeout
- graceful overload rejection
- reconnect behavior for transient upstream RPC failures

More concretely, the current server-side controls are:

- `max_message_size = 256 KiB`
- `max_frame_size = 64 KiB`
- `CustomKey.name` limited to `128` bytes
- `CustomValue::String` limited to `1024` bytes during request handling
- global concurrent WebSocket connection cap of `1024`
- per-connection subscription cap of `128`
- idle timeout of `300` seconds

Duplicate subscriptions do not consume extra quota.

The subscription control path now uses bounded channels, and queue saturation or
closure is treated as an error rather than a panic.

## Main Residual Risks

## Scope

The primary Internet-facing attack surface is:

- the public WebSocket listener on `0.0.0.0:<port>`
- JSON request parsing for `Status`, `Variants`, `GetEvents`, subscription
  requests, and `SizeOnDisk`
- the subscription dispatch path between connection handlers and the indexer

Reviewed components for the current security posture include:

- `src/websockets.rs`
- `src/main.rs`
- `src/indexer.rs`
- `src/shared.rs`
- dependency surface via `cargo audit`

## Residual Risks

### No Authentication Or Authorization

The service is intentionally Internet-accessible and does not authenticate
clients.

Impact:

- any reachable client can query indexed data
- any reachable client can subscribe to live updates
- any reachable client can call `SizeOnDisk`
- any reachable client can retrieve decoded events while the service has live node access

This is the largest remaining exposure and should be treated as a deliberate
product decision rather than an implicit default.

### No In-Process TLS

The service speaks plain WebSocket (`ws`), not `wss`.

Traffic confidentiality and integrity therefore depend on external deployment
infrastructure. Direct exposure without TLS termination allows observation and
tampering in transit.

### Expensive Public Endpoints

`Variants`, `GetEvents`, and live subscriptions remain public.

The configured limits reduce blast radius but do not provide true rate limiting
or fairness across clients or source IPs.

### Operational Metadata Exposure

`SizeOnDisk` returns database size to any client. When enabled, the metrics
listener also exposes connection state, subscription counts, span progress, and
database size.

This is lower severity than unauthenticated data access, but still relevant for
public deployments.

### Residual Panic And Dependency Risk

Production code no longer relies on `unwrap()` across the main request path for
persisted-data decoding or ordinary startup flow. Remaining fail-fast behavior is
mostly concentrated in tests or explicit startup assertions.

The dependency surface still carries maintenance and audit risk, especially in
the `sled` stack and a transitive `lru` advisory in the `subxt` light-client
dependency chain.

## Cargo Audit Notes

Recent advisories called out in the current review include:

- `RUSTSEC-2025-0057` via `sled -> fxhash`
- `RUSTSEC-2024-0384` via `sled -> parking_lot -> instant`
- `RUSTSEC-2026-0002` via `subxt -> subxt-lightclient -> smoldot-light -> lru`

These are dependency risks, not proof of a directly exploitable application bug,
but they should stay on the maintenance backlog.

## Deployment Guidance

For Internet-facing deployment, put the service behind infrastructure that
provides at least:

- TLS termination
- request logging
- connection or IP rate limiting
- firewalling or edge filtering
- overload monitoring and health checks
- internal-only exposure for the metrics listener when enabled

Without those controls, the service remains materially more exposed to abuse even
after the implemented application-level hardening.

## Recommended Next Steps

Highest-value remaining work:

1. add rate limiting
2. decide whether `SizeOnDisk` should remain public
3. decide whether decoded event retrieval should remain public
4. require TLS termination in all documented deployment paths
5. revisit authentication if the service needs differentiated access or abuse accountability
6. track or remediate the current dependency audit findings

## Metrics

If `--metrics-port` is configured, the process serves `/metrics` in OpenMetrics
text format on a separate HTTP listener.

Treat that listener as an internal observability surface rather than part of the
public API.

## Storage And Chain Identity

The most important operational safety guard is the stored `genesis_hash` check.
It prevents one database path from silently mixing data from multiple chains.
