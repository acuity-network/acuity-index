# JSON-RPC 2.0 Migration Plan

Target contract:
- JSON-RPC 2.0 only
- Method names use the `acuity_` prefix
- `acuity_getEvents` always returns proofs
- Pagination includes `nextCursor` and `hasMore`
- Remove `SizeOnDisk` from the public API
- No capability discovery endpoint

## Method Surface

Use these methods:

- `acuity_indexStatus`
- `acuity_getEventMetadata`
- `acuity_getEvents`
- `acuity_subscribeStatus`
- `acuity_unsubscribeStatus`
- `acuity_subscribeEvents`
- `acuity_unsubscribeEvents`

Remove:
- `acuity_sizeOnDisk`
- legacy `SizeOnDisk`

## JSON-RPC Envelope

Requests:

```json
{"jsonrpc":"2.0","id":1,"method":"acuity_getEvents","params":{"key":{...},"limit":100}}
```

Success:

```json
{"jsonrpc":"2.0","id":1,"result":{...}}
```

Error:

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params","data":{"reason":"invalid_key"}}}
```

Subscription notification:

```json
{"jsonrpc":"2.0","method":"acuity_subscription","params":{"subscription":"sub_123","result":{...}}}
```

## Domain Semantics

### `acuity_indexStatus`

Replaces `Status`. Returns:

```json
{"spans":[{"start":1,"end":1000}]}
```

### `acuity_getEventMetadata`

Replaces `Variants`. Returns:

```json
{"pallets":[...]}
```

### `acuity_getEvents`

Replaces `GetEvents`. Params:

```json
{"key":{...},"limit":100,"before":{"blockNumber":50,"eventIndex":3}}
```

- Remove `includeProofs` param
- Always return proofs in a normalized object

Response:

```json
{
  "key": {...},
  "events": [...],
  "decodedEvents": [...],
  "proofs": {
    "available": true,
    "reason": "included",
    "message": "Finalized event proofs included.",
    "items": [...]
  },
  "page": {
    "nextCursor": {"blockNumber":50,"eventIndex":3},
    "hasMore": true
  }
}
```

Proof behavior:
- If proofs are unavailable, still return the same `proofs` object with `available: false`, a stable `reason`, and `items: []`.
- Do not fail the whole request just because proofs are unavailable.

Pagination behavior:
- `hasMore` is `true` when more matching rows exist beyond this page
- `nextCursor` is the last returned event when `hasMore` is true, otherwise `null`
- Internally fetch `limit + 1` to determine `hasMore`, return at most `limit`

### `acuity_subscribeStatus`

Returns a subscription ID string.

### `acuity_unsubscribeStatus`

Params:

```json
{"subscription":"sub_123"}
```

Returns `true`.

### `acuity_subscribeEvents`

Params:

```json
{"key":{...}}
```

Returns a subscription ID string.

### `acuity_unsubscribeEvents`

Params:

```json
{"subscription":"sub_456"}
```

Returns `true`.

## Notifications

Use a single notification method: `acuity_subscription`.

Status notification:

```json
{
  "jsonrpc":"2.0",
  "method":"acuity_subscription",
  "params":{
    "subscription":"sub_123",
    "result":{
      "type":"status",
      "spans":[...]
    }
  }
}
```

Event notification:

```json
{
  "jsonrpc":"2.0",
  "method":"acuity_subscription",
  "params":{
    "subscription":"sub_456",
    "result":{
      "type":"event",
      "key": {...},
      "event": {...},
      "decodedEvent": {...}
    }
  }
}
```

Termination notification for backpressure:

```json
{
  "jsonrpc":"2.0",
  "method":"acuity_subscription",
  "params":{
    "subscription":"sub_456",
    "result":{
      "type":"terminated",
      "reason":"backpressure",
      "message":"subscriber disconnected due to backpressure"
    }
  }
}
```

## Errors

Use JSON-RPC standard top-level codes:

- parse error -> `-32700`
- invalid request -> `-32600`
- method not found -> `-32601`
- invalid params -> `-32602`
- internal error -> `-32603`
- temporary upstream unavailability -> `-32001`

Put app-specific meaning in `error.data.reason`:

- `invalid_key`
- `invalid_cursor`
- `subscription_limit`
- `temporarily_unavailable`
- `proofs_unavailable`
- `upstream_rpc_unavailable`

Examples:
- malformed JSON -> `-32700`
- wrong envelope or missing `jsonrpc` -> `-32600`
- unknown `method` -> `-32601`
- bad key or bad cursor -> `-32602`
- RPC outage for `acuity_getEvents` / `acuity_getEventMetadata` -> `-32001`, `data.reason = "temporarily_unavailable"`
- true internal failures -> `-32603`

## Data Model Changes

1. Replace the current custom request/response envelope
   - Current `type`-tagged request body becomes JSON-RPC `method` plus `params`
   - Current response `type`/`data` becomes JSON-RPC `result`

2. Add explicit subscription registry
   Track:
   - `subscription_id -> sender`
   - `subscription_id -> kind` (status or events)
   - `subscription_id -> key` for event subscriptions
   - connection -> set of subscription IDs for cleanup on disconnect

3. Keep domain payload structs where they still fit
   Reuse existing:
   - `Span`
   - `EventRef`
   - `DecodedEvent`
   - proof structs
   - key structs
   
   Wrap them in JSON-RPC result objects.

## Implementation Sequence

### 1. Define new wire structs

- JSON-RPC request
- JSON-RPC success response
- JSON-RPC error response
- JSON-RPC subscription notification
- Typed params/result structs per method

### 2. Refactor dispatch

- Parse JSON-RPC envelope first
- Validate `jsonrpc == "2.0"`
- Route by `method`
- Decode params by method

### 3. Replace current request names with new methods

- Stop accepting legacy `type` requests
- Stop producing legacy `type` responses

### 4. Implement explicit subscription handles

- Generate unique subscription IDs
- Return IDs from subscribe methods
- Require IDs for unsubscribe methods

### 5. Rework notification emission

- Emit `acuity_subscription`
- Include subscription ID in every push
- Include typed `result.type`

### 6. Update `acuity_getEvents`

- Remove `includeProofs` input
- Always attempt proof inclusion
- Normalize proof result shape
- Add `page.nextCursor`
- Add `page.hasMore`

### 7. Remove public size-on-disk method

- Delete request parsing and handler support
- Remove documentation and tests for it

### 8. Normalize errors

- Map transport/protocol/domain failures to JSON-RPC codes consistently
- Keep stable app reasons in `error.data.reason`

### 9. Rewrite docs

Update:
- `API.md`
- `book/src/api.md`
- `ARCHITECTURE.md`
- `SECURITY.md`
- `README.md` references

### 10. Update tests

Replace legacy tests with JSON-RPC-focused coverage for:
- parse error
- invalid request envelope
- method not found
- invalid params
- `acuity_indexStatus`
- `acuity_getEventMetadata`
- `acuity_getEvents`
- pagination `nextCursor` / `hasMore`
- proofs always present
- subscribe/unsubscribe by handle
- backpressure termination notification
- disconnect cleanup

### 11. Update integration helpers

- Synthetic devnet websocket probe helpers
- Benchmark/test clients that currently send legacy request shapes

## Locked Decisions

1. Method names: `acuity_indexStatus`, `acuity_getEventMetadata`, `acuity_getEvents`, `acuity_subscribeStatus`, `acuity_unsubscribeStatus`, `acuity_subscribeEvents`, `acuity_unsubscribeEvents`

2. Subscription ID format: opaque string IDs with no semantic meaning.

3. Proof failure semantics: do not fail the whole request; return events and decoded events with `proofs.available = false`.

4. Pagination computation: fetch `limit + 1` internally, return at most `limit`, derive `hasMore` from presence of extra row, derive `nextCursor` from last returned row when `hasMore`.