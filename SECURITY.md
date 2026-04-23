# Security Review

This document summarizes the current security posture of the Internet-facing WebSocket service in this repository, the hardening that has been implemented, and the main residual risks that remain.

## Scope

Reviewed components:

- `src/websockets.rs`
- `src/main.rs`
- `src/indexer.rs`
- `src/shared.rs`
- dependency surface via `cargo audit`

Primary attack surface:

- public WebSocket listener on `0.0.0.0:<port>`
- JSON request parsing for `Status`, `Variants`, `GetEvents`, `SubscribeStatus`, `SubscribeEvents`, `UnsubscribeStatus`, `UnsubscribeEvents`, and `SizeOnDisk`
- subscription dispatch path between connection handlers and the indexer

## Current Hardening

The following protections are now implemented in the server:

### Resource exhaustion controls

- Bounded subscription control queue
  - The connection-to-subscription-dispatcher control path uses a bounded Tokio channel.
  - Saturation no longer causes unbounded memory growth.

- WebSocket message and frame size limits
  - `max_message_size = 256 KiB`
  - `max_frame_size = 64 KiB`
  - Oversized frames/messages are rejected by tungstenite during WebSocket handling.

- Bounded custom request key sizes
  - `CustomKey.name` is limited to `128` bytes.
  - `CustomValue::String` is limited to `1024` bytes for request handling.
  - Oversized key inputs are rejected before sled prefix scanning or subscription registration.

- Global connection cap
  - Concurrent WebSocket connections are capped at `1024` using a semaphore.

- Per-connection subscription cap
  - A single connection may hold at most `128` subscriptions total.
  - The cap counts `status` and distinct event-key subscriptions.
  - Duplicate subscriptions do not consume extra quota.

- Idle timeout
  - Idle connections are closed after `300` seconds without inbound traffic or outbound subscription notifications.

- Graceful overload rejection
  - When the connection cap is exhausted, the server rejects new upgrades with HTTP `503 Service Unavailable` instead of silently dropping the TCP connection.

### Crash resistance improvements

- The remotely reachable subscription control path no longer relies on unbounded buffering.
- Subscription queue saturation and closure are handled as errors rather than panics.
- Transient RPC failures (node restarts, network blips) no longer terminate the process. The indexer saves its span state to sled, reconnects with exponential backoff, and keeps existing WebSocket clients connected for sled-backed reads while RPC-backed requests return temporary unavailability.

## Residual Risks

The most important remaining security concerns are below.

### 1. No authentication or authorization

The service is intentionally Internet-accessible and currently does not authenticate clients.

Impact:

- Any reachable client can query indexed data.
- Any reachable client can subscribe to live updates.
- Any reachable client can call `SizeOnDisk`, which exposes operational metadata.
- If event storage is enabled, any reachable client can retrieve decoded events through `GetEvents`.

Assessment:

- This is the largest remaining exposure.
- It may be acceptable for a fully public data service, but it should be considered a deliberate product decision rather than an implicit default.

### 2. No transport-layer security in-process

The service speaks plain WebSocket (`ws`), not `wss`.

Impact:

- Traffic confidentiality and integrity depend on deployment infrastructure.
- Direct exposure without a TLS terminator would allow traffic observation and tampering in transit.

Assessment:

- Safe deployment requires a reverse proxy, load balancer, tunnel, or edge service that terminates TLS.

### 3. Expensive public endpoints remain available

`Variants`, `GetEvents`, and live subscriptions are still public and can be used repeatedly.

Impact:

- Attackers can still consume CPU, I/O, and database scan capacity within the configured limits.
- The current controls reduce blast radius, but they do not provide rate limiting or fairness across clients/IPs.

Assessment:

- The server is substantially more resilient than before, but it is still vulnerable to sustained abuse from many clients or many source IPs.

### 4. Some operational metadata is still exposed

`SizeOnDisk` returns on-disk database size to any client. If enabled, the separate
OpenMetrics endpoint also exposes operational signals such as connection state,
subscription counts, span progress, and database size.

Impact:

- This leaks storage growth and operational characteristics.
- The metrics listener widens the set of operational metadata visible to anything
  that can reach that port.

Assessment:

- Lower severity than unauthenticated data access, but worth reviewing if the service is public.
- The dedicated metrics port should be treated as an internal observability surface,
  not as part of the public WebSocket API.

### 5. Internal panic paths remain mostly limited to test-only code and explicit fatal startup assertions

The production runtime no longer relies on `unwrap()` for persisted data decoding, subscription lock handling, or normal startup error flow. Remaining `unwrap()` usage is primarily in Rust test code, with a small number of explicit fail-fast assertions still used for conditions like process startup that cannot be recovered meaningfully in place.

Impact:

- This is lower risk for direct Internet-triggered exploitation than the public WebSocket request path.
- Corrupt persisted span/index records are now skipped with logging instead of taking down the process.
- Residual fail-fast behavior is concentrated in startup-only paths and test helpers rather than steady-state request handling.

Assessment:

- This is now primarily a reliability and operability concern, not the top Internet-facing security issue.

## Cargo Audit

`cargo audit` reported the following advisories during review:

1. `RUSTSEC-2025-0057`
   - dependency chain: `sled -> fxhash 0.2.1`
   - status: unmaintained

2. `RUSTSEC-2024-0384`
   - dependency chain: `sled -> parking_lot 0.11.2 -> instant 0.1.13`
   - status: unmaintained

3. `RUSTSEC-2026-0002`
   - dependency chain: `subxt -> subxt-lightclient -> smoldot-light -> lru 0.12.5`
   - status: unsoundness in `IterMut`

Assessment:

- The `sled` advisories indicate maintenance risk in the storage stack.
- The `lru` advisory is more serious in principle, but practical exposure depends on whether the affected light-client path is exercised in this deployment model.
- These are dependency risks, not evidence of a directly exploitable application bug in this review.

## Recommended Next Steps

Highest-value remaining work:

1. Add rate limiting.
2. Decide whether `SizeOnDisk` should remain public.
3. Decide whether decoded event retrieval should remain public when `store_events = true` in the index spec.
4. Require TLS termination in all documented deployment paths.
5. Revisit authentication if the service needs differentiated access or abuse accountability.
6. Track or remediate the `cargo audit` findings, especially the transitive `lru` advisory and long-term `sled` maintenance risk.

## Deployment Guidance

For Internet exposure, deploy behind infrastructure that provides at least:

- TLS termination
- request logging
- connection/IP rate limiting
- firewalling or edge filtering
- health checks and overload monitoring
- internal-only exposure or equivalent protection for the metrics port when enabled

Without those controls, the service is still materially more exposed to abuse even after the application-level hardening implemented here.
