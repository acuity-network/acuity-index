# API

## WebSocket API

Connect to `ws://localhost:8172` by default.

For Internet-facing deployment guidance and the current security review, see
[`SECURITY.md`](./SECURITY.md).

The public API is a JSON-RPC 2.0-over-WebSocket protocol with two message classes:

- request/response messages, which carry a numeric or string `id`
- server notifications, which never carry an `id`

## Protocol Overview

### Requests

Every client request must conform to the JSON-RPC 2.0 specification:

- `jsonrpc`: must be `"2.0"`
- `id`: unsigned integer or string chosen by the client
- `method`: the RPC method name
- `params`: optional method parameters

Example:

```json
{"jsonrpc":"2.0","id":1,"method":"acuity_indexStatus","params":{}}
```

### Responses

Every successful request receives a JSON-RPC 2.0 response with the same `id`.

- `jsonrpc`: `"2.0"`
- `id`: matches the request `id`
- `result`: the method return value

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {"start": 1, "end": 1000}
  ]
}
```

### Error Responses

Failed requests receive a JSON-RPC 2.0 error response with the same `id`.

- `jsonrpc`: `"2.0"`
- `id`: matches the request `id` (or `null` if the `id` could not be parsed)
- `error`: error object with:
  - `code`: integer error code
  - `message`: human-readable description
  - `data`: optional additional information

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "Invalid params",
    "data": {
      "reason": "missing field `key`"
    }
  }
}
```

### Notifications

Notifications are pushed by the server for active subscriptions. They use a consistent envelope:

- `jsonrpc`: `"2.0"`
- `method`: `"acuity_subscription"`
- `params`:
  - `subscription`: the subscription ID string
  - `result`: the notification payload, which always includes a `type` field

Example:

```json
{
  "jsonrpc": "2.0",
  "method": "acuity_subscription",
  "params": {
    "subscription": "sub_123",
    "result": {
      "type": "event",
      "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
      "event": {"blockNumber": 50, "eventIndex": 3},
      "decodedEvent": {
        "blockNumber": 50,
        "eventIndex": 3,
        "event": {
          "specVersion": 1234,
          "palletName": "Referenda",
          "eventName": "Submitted",
          "palletIndex": 42,
          "variantIndex": 0,
          "eventIndex": 3,
          "fields": {"index": 42}
        }
      }
    }
  }
}
```

## Methods

### `acuity_indexStatus`

Request:

```json
{"jsonrpc":"2.0","id":1,"method":"acuity_indexStatus","params":{}}
```

Response payload:

- array of indexed block spans
- each span has:
  - `start`: first indexed block
  - `end`: last indexed block

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {"start": 1, "end": 1000}
  ]
}
```

### `acuity_getEventMetadata`

Request:

```json
{"jsonrpc":"2.0","id":2,"method":"acuity_getEventMetadata","params":{}}
```

Response payload:

- array of pallets
- each pallet has:
  - `index`: pallet event index
  - `name`: pallet name
  - `events`: array of event variants
- each event variant has:
  - `index`: variant index
  - `name`: variant name

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": [
    {
      "index": 42,
      "name": "Referenda",
      "events": [
        {"index": 0, "name": "Submitted"}
      ]
    }
  ]
}
```

### `acuity_getEvents`

Request parameters:

- `key`: query key
- `limit`: optional `u16`, default `100`
- `before`: optional event cursor

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "acuity_getEvents",
  "params": {
    "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
    "limit": 100,
    "before": null
  }
}
```

Composite custom keys use an ordered array of typed values:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "acuity_getEvents",
  "params": {
    "key": {
      "type": "Custom",
      "value": {
        "name": "item_revision",
        "kind": "composite",
        "value": [
          {"kind": "bytes32", "value": "0xabc123..."},
          {"kind": "u32", "value": 7}
        ]
      }
    }
  }
}
```

Response payload:

- `key`: the queried key
- `events`: matching event references, newest first
- `decodedEvents`: decoded payloads for the returned event refs
- `proofs`: proof availability and items
- `page`: pagination cursor information

`proofs` object:

- `available`: boolean indicating whether proofs are included
- `reason`: stable machine-readable reason such as `included`, `rpc_proof_unavailable`, or `finalized_proofs_unavailable`
- `message`: human-readable explanation
- `items`: array of proof objects (present when `available` is `true`)

Each proof item contains:

- `blockNumber`: `u32`
- `blockHash`: hex-encoded block hash
- `header`: serialized block header JSON
- `storageKey`: hex-encoded `System.Events` storage key
- `storageValue`: hex-encoded SCALE-encoded `System.Events` bytes for that block
- `storageProof`: array of hex-encoded trie proof nodes

`page` object:

- `nextCursor`: cursor for the next page, or `null` if no more results
- `hasMore`: boolean indicating additional pages exist

Each `EventRef` contains:

- `blockNumber`: `u32`
- `eventIndex`: zero-based `u32` ordinal within the block

Each `DecodedEvent` contains:

- `blockNumber`: `u32`
- `eventIndex`: zero-based `u32` ordinal within the block
- `event`: decoded event JSON

Decoded event JSON currently contains:

- `specVersion`
- `palletName`
- `eventName`
- `palletIndex`
- `variantIndex`
- `eventIndex`: zero-based `u32` ordinal within the block
- `fields`

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
    "events": [
      {"blockNumber": 50, "eventIndex": 3}
    ],
    "decodedEvents": [
      {
        "blockNumber": 50,
        "eventIndex": 3,
        "event": {
          "specVersion": 1234,
          "palletName": "Referenda",
          "eventName": "Submitted",
          "palletIndex": 42,
          "variantIndex": 0,
          "eventIndex": 3,
          "fields": {
            "index": 42
          }
        }
      }
    ],
    "proofs": {
      "available": true,
      "reason": "included",
      "message": "Finalized event proofs included.",
      "items": [
        {
          "blockNumber": 50,
          "blockHash": "0xabc123...",
          "header": {
            "parent_hash": "0x...",
            "number": 50,
            "state_root": "0x...",
            "extrinsics_root": "0x...",
            "digest": {"logs": []}
          },
          "storageKey": "0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7",
          "storageValue": "0x...",
          "storageProof": ["0x..."]
        }
      ]
    },
    "page": {
      "nextCursor": {"blockNumber": 49, "eventIndex": 7},
      "hasMore": true
    }
  }
}
```

Example when proofs are unavailable (indexer not in finalized mode):

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
    "events": [{"blockNumber": 50, "eventIndex": 3}],
    "decodedEvents": [],
    "proofs": {
      "available": false,
      "reason": "finalized_proofs_unavailable",
      "message": "Finalized proofs are only available when the indexer is running with finalized indexing."
    },
    "page": {
      "nextCursor": null,
      "hasMore": false
    }
  }
}
```

### `acuity_subscribeStatus`

Request:

```json
{"jsonrpc":"2.0","id":5,"method":"acuity_subscribeStatus","params":{}}
```

Response payload: subscription ID string

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "result": "sub_abc123"
}
```

### `acuity_unsubscribeStatus`

Request:

```json
{"jsonrpc":"2.0","id":6,"method":"acuity_unsubscribeStatus","params":{"subscription":"sub_abc123"}}
```

Response payload: `true`

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "result": true
}
```

### `acuity_subscribeEvents`

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "acuity_subscribeEvents",
  "params": {
    "key": {"type": "Custom", "value": {"name": "account_id", "kind": "bytes32", "value": "0xabc..."}}
  }
}
```

Response payload: subscription ID string

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "result": "sub_def456"
}
```

### `acuity_unsubscribeEvents`

Request:

```json
{"jsonrpc":"2.0","id":8,"method":"acuity_unsubscribeEvents","params":{"subscription":"sub_def456"}}
```

Response payload: `true`

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 8,
  "result": true
}
```

## Notifications

All subscription notifications use the method `acuity_subscription` and include the subscription ID in `params.subscription`. The `params.result` object always contains a `type` field indicating the notification type.

### Status notifications

Sent to status subscribers whenever the persisted indexed span advances.

`params.result.type`: `"status"`

Payload (in `params.result`):

- same shape as the `acuity_indexStatus` response payload

Example:

```json
{
  "jsonrpc": "2.0",
  "method": "acuity_subscription",
  "params": {
    "subscription": "sub_abc123",
    "result": {
      "type": "status",
      "data": [
        {"start": 1, "end": 1001}
      ]
    }
  }
}
```

### Event notifications

Sent to event subscribers whenever a matching event is indexed.

`params.result.type`: `"event"`

Payload (in `params.result`):

- `key`: subscribed key that matched
- `event`: matching event reference
- `decodedEvent`: decoded event object hydrated from the node before delivery

Example:

```json
{
  "jsonrpc": "2.0",
  "method": "acuity_subscription",
  "params": {
    "subscription": "sub_def456",
    "result": {
      "type": "event",
      "key": {"type": "Custom", "value": {"name": "item_id", "kind": "bytes32", "value": "0xabc..."}},
      "event": {"blockNumber": 50, "eventIndex": 3},
      "decodedEvent": {
        "blockNumber": 50,
        "eventIndex": 3,
        "event": {
          "specVersion": 1234,
          "palletName": "Referenda",
          "eventName": "Submitted",
          "palletIndex": 42,
          "variantIndex": 0,
          "eventIndex": 3,
          "fields": {
            "index": 42
          }
        }
      }
    }
  }
}
```

### Subscription termination notifications

Sent best-effort before the server drops a subscriber because it cannot keep up.

`params.result.type`: `"terminated"`

Payload (in `params.result`):

- `reason`: currently `backpressure`
- `message`: human-readable explanation

Example:

```json
{
  "jsonrpc": "2.0",
  "method": "acuity_subscription",
  "params": {
    "subscription": "sub_def456",
    "result": {
      "type": "terminated",
      "reason": "backpressure",
      "message": "subscriber disconnected due to backpressure"
    }
  }
}
```

This notification is best-effort only. If the subscriber queue is already full, the termination notice may not be delivered before the connection is dropped.

## Errors

The server uses standard JSON-RPC 2.0 error codes:

| Code | Meaning |
|------|---------|
| `-32700` | Parse error - invalid JSON |
| `-32600` | Invalid request - not a valid JSON-RPC 2.0 object |
| `-32601` | Method not found |
| `-32602` | Invalid params |
| `-32603` | Internal error |
| `-32001` | Upstream unavailable - node RPC connection is down |

Application-specific error reasons are provided in `error.data.reason`:

- `subscription_limit`: connection exceeds the per-connection subscription cap or the global total subscription cap
- `upstream_unavailable`: an RPC-backed request (`acuity_getEventMetadata` or `acuity_getEvents`) is made while the node connection is down

Example when no request `id` could be recovered:

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32700,
    "message": "Parse error"
  }
}
```

Invalid custom key payloads are returned as `-32602` responses, including:

- custom key names longer than `128` bytes
- custom string values longer than `1024` bytes
- composite keys with more than `64` elements
- composite keys nested deeper than `8` composite levels
- custom values whose encoded key payload exceeds `16384` bytes

Operational failure modes that may also appear as `-32603` responses or connection drops include:

- oversized WebSocket frame or message rejected during protocol handling
- subscription control queue saturation
- connection idle timeout

During node outages, local requests such as `acuity_indexStatus` continue to
work. `acuity_getEventMetadata` and `acuity_getEvents` require live RPC access and return
`-32001` (upstream unavailable) until the node connection is re-established.

## Pagination Semantics

`acuity_getEvents` returns matches in descending order by `(blockNumber, eventIndex)`.

- default `limit` is `100`
- `limit` is clamped to `1..=max_events_limit` (default `1000`, configurable via `--max-events-limit`; invalid zero-valued configs are rejected at startup)
- `before` is exclusive

Example cursor:

```json
{"blockNumber": 50, "eventIndex": 3}
```

This returns only events strictly older than `(50, 3)`.

Use the `page` object in the response for cursor-based pagination:

```json
{
  "page": {
    "nextCursor": {"blockNumber": 49, "eventIndex": 7},
    "hasMore": true
  }
}
```

Pass `nextCursor` as the `before` parameter to fetch the next page.

## Key Format

Keys are JSON objects with a `type` discriminant.

Supported key shapes:

```json
{"type":"Variant","value":[0,3]}
{"type":"Custom","value":{"name":"para_id","kind":"u32","value":1000}}
{"type":"Custom","value":{"name":"account_id","kind":"bytes32","value":"0x1234..."}}
{"type":"Custom","value":{"name":"published","kind":"bool","value":true}}
{"type":"Custom","value":{"name":"revision","kind":"u128","value":"42"}}
{"type":"Custom","value":{"name":"slug","kind":"string","value":"hello"}}
{"type":"Custom","value":{"name":"balance","kind":"u64","value":"42"}}
```

### `Variant`

- array of `[pallet_index, variant_index]`

### `Custom`

Fields:

- `name`: key name
- `kind`: scalar type
- `value`: scalar value encoded according to `kind`

Supported scalar kinds:

- `bytes32`: 32-byte hex string, `0x` prefix accepted
- `u32`: JSON number
- `u64`: JSON integer or string on input, serialized as string on output
- `u128`: JSON integer or string on input, serialized as string on output
- `string`: JSON string
- `bool`: JSON boolean

Composite keys are encoded recursively and must satisfy these protocol-level limits:

- max composite elements per composite value: `64`
- max composite nesting depth: `8`
- max encoded custom value size: `16384` bytes

## Storage Notes

Index entries store event refs locally in sled.

Decoded event payloads are hydrated from the node on demand for:

- `acuity_getEvents` responses
- event subscription notifications

This keeps the JSON-RPC shape unchanged while making decoded payload availability depend on live node access.

## Delivery and Backpressure

Subscription delivery uses bounded internal queues.

- slow subscribers are removed instead of being buffered indefinitely
- the server attempts to send a `terminated` notification before removal
- if that best-effort notification cannot be queued, the client may observe only the disconnect

## Connection Limits

The server applies these connection-level limits (defaults; all configurable via CLI flags or `--options-config`):

- max concurrent WebSocket connections: `1024` (`--max-connections`)
- max total subscriptions across all connections: `65536` (`--max-total-subscriptions`)
- max subscriptions per connection: `128` (`--max-subscriptions-per-connection`)
- subscription notification buffer size: `256` (`--subscription-buffer-size`)
- subscription control channel buffer: `1024` (`--subscription-control-buffer-size`)
- idle timeout: `300s` (`--idle-timeout-secs`, `0` disables the timeout)
- max events per query: `1000` (`--max-events-limit`)

If the global connection cap is exhausted, new upgrade attempts are rejected with
HTTP `503 Service Unavailable`.

If the total subscription cap is reached, new subscription requests are rejected
with a `-32603` error response with `data.reason: "subscription_limit"`. Because the
subscription was never established, no termination notification is
sent for that initial rejection.

Protocol-level limits (not configurable at runtime):

- max WebSocket message size: `256 KiB`
- max WebSocket frame size: `64 KiB`
- max custom key name length: `128` bytes
- max custom string key length: `1024` bytes
- max composite elements per composite value: `64`
- max composite nesting depth: `8`
- max encoded custom value size: `16384` bytes
