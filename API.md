# API

## WebSocket API

Connect to `ws://localhost:8172` by default.

For Internet-facing deployment guidance and the current security review, see
[`SECURITY.md`](./SECURITY.md).

The public API is a JSON-over-WebSocket protocol with two message classes:

- request/response messages, which always carry a numeric `id`
- server notifications, which never carry an `id`

## Protocol Overview

### Requests

Every client request must include:

- `id`: unsigned integer chosen by the client
- `type`: request discriminator

Example:

```json
{"id":1,"type":"Status"}
```

### Responses

Every successful or failed request receives a response with the same `id`.

Example:

```json
{
  "id": 1,
  "type": "status",
  "data": []
}
```

### Notifications

Notifications are pushed by the server for active subscriptions. They do not include an `id`.

Example:

```json
{
  "type": "eventNotification",
  "data": {
    "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
    "event": {"blockNumber": 50, "eventIndex": 3},
    "decodedEvent": {
      "blockNumber": 50,
      "eventIndex": 3,
      "event": {"specVersion": 1234, "palletName": "Referenda", "eventName": "Submitted", "palletIndex": 42, "variantIndex": 0, "eventIndex": 3, "fields": {"index": 42}}
    }
  }
}
```

## Request Types

### `Status`

Request:

```json
{"id":1,"type":"Status"}
```

Response type: `status`

Response payload:

- array of indexed block spans
- each span has:
  - `start`: first indexed block
  - `end`: last indexed block

Example:

```json
{
  "id": 1,
  "type": "status",
  "data": [
    {"start": 1, "end": 1000}
  ]
}
```

### `Variants`

Request:

```json
{"id":2,"type":"Variants"}
```

Response type: `variants`

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
  "id": 2,
  "type": "variants",
  "data": [
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

### `GetEvents`

Request fields:

- `key`: query key
- `limit`: optional `u16`, default `100`
- `before`: optional event cursor

Request:

```json
{
  "id": 3,
  "type": "GetEvents",
  "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 42}},
  "limit": 100,
  "before": null
}
```

Composite custom keys use an ordered array of typed values:

```json
{
  "id": 3,
  "type": "GetEvents",
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
```

Response type: `events`

Response payload:

- `key`: the queried key
- `events`: matching event references, newest first
- `decodedEvents`: decoded payloads for the returned event refs

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
  "id": 3,
  "type": "events",
  "data": {
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
    ]
  }
}
```

### `SizeOnDisk`

Request:

```json
{"id":4,"type":"SizeOnDisk"}
```

Response type: `sizeOnDisk`

Response payload:

- total sled database size in bytes as `u64`

Example:

```json
{
  "id": 4,
  "type": "sizeOnDisk",
  "data": 123456
}
```

### `SubscribeStatus`

Request:

```json
{"id":5,"type":"SubscribeStatus"}
```

Response type: `subscriptionStatus`

Example:

```json
{
  "id": 5,
  "type": "subscriptionStatus",
  "data": {
    "action": "subscribed",
    "target": {
      "type": "status"
    }
  }
}
```

### `UnsubscribeStatus`

Request:

```json
{"id":6,"type":"UnsubscribeStatus"}
```

Response type: `subscriptionStatus`

Example:

```json
{
  "id": 6,
  "type": "subscriptionStatus",
  "data": {
    "action": "unsubscribed",
    "target": {
      "type": "status"
    }
  }
}
```

### `SubscribeEvents`

Request:

```json
{
  "id": 7,
  "type": "SubscribeEvents",
  "key": {"type": "Custom", "value": {"name": "account_id", "kind": "bytes32", "value": "0xabc..."}}
}
```

Response type: `subscriptionStatus`

Example:

```json
{
  "id": 7,
  "type": "subscriptionStatus",
  "data": {
    "action": "subscribed",
    "target": {
      "type": "events",
      "key": {"type": "Custom", "value": {"name": "account_id", "kind": "bytes32", "value": "0xabc..."}}
    }
  }
}
```

### `UnsubscribeEvents`

Request:

```json
{
  "id": 8,
  "type": "UnsubscribeEvents",
  "key": {"type": "Custom", "value": {"name": "account_id", "kind": "bytes32", "value": "0xabc..."}}
}
```

Response type: `subscriptionStatus`

Example:

```json
{
  "id": 8,
  "type": "subscriptionStatus",
  "data": {
    "action": "unsubscribed",
    "target": {
      "type": "events",
      "key": {"type": "Custom", "value": {"name": "account_id", "kind": "bytes32", "value": "0xabc..."}}
    }
  }
}
```

## Notifications

### `status`

Sent to status subscribers whenever the persisted indexed span advances.

Payload:

- same shape as the `Status` response payload

Example:

```json
{
  "type": "status",
  "data": [
    {"start": 1, "end": 1001}
  ]
}
```

### `eventNotification`

Sent to event subscribers whenever a matching event is indexed.

Payload:

- `key`: subscribed key that matched
- `event`: matching event reference
- `decodedEvent`: decoded event object hydrated from the node before delivery

Example:

```json
{
  "type": "eventNotification",
  "data": {
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
```

### `subscriptionTerminated`

Sent best-effort before the server drops a subscriber because it cannot keep up.

Payload:

- `reason`: currently `backpressure`
- `message`: human-readable explanation

Example:

```json
{
  "type": "subscriptionTerminated",
  "data": {
    "reason": "backpressure",
    "message": "subscriber disconnected due to backpressure"
  }
}
```

This notification is best-effort only. If the subscriber queue is already full, the termination notice may not be delivered before the connection is dropped.

## Errors

Invalid requests and handler failures are returned as normal responses with `type: "error"` and the original request `id` when available.

If the server cannot deserialize a request `id` at all, the error response omits the `id` field.

Payload:

- `code`: stable machine-readable string
- `message`: human-readable detail

Current error codes used by the WebSocket layer include:

- `invalid_request`
- `internal_error`

Example:

```json
{
  "id": 9,
  "type": "error",
  "data": {
    "code": "invalid_request",
    "message": "missing field `id`"
  }
}
```

Example when no request `id` could be recovered:

```json
{
  "type": "error",
  "data": {
    "code": "invalid_request",
    "message": "missing field `id`"
  }
}
```

Additional error codes currently returned by the WebSocket layer include:

- `subscription_limit` when a connection exceeds the per-connection subscription cap or the global total subscription cap
- `temporarily_unavailable` when an RPC-backed request (`Variants` or `GetEvents`) is made while the node connection is down and the server is reconnecting

Invalid custom key payloads are returned as `invalid_request` responses, including:

- custom key names longer than `128` bytes
- custom string values longer than `1024` bytes
- composite keys with more than `64` elements
- composite keys nested deeper than `8` composite levels
- custom values whose encoded key payload exceeds `16384` bytes

Operational failure modes that may also appear as request-scoped `internal_error`
responses or connection drops include:

- oversized WebSocket frame or message rejected during protocol handling
- subscription control queue saturation
- connection idle timeout

During node outages, local requests such as `Status` and `SizeOnDisk` continue to
work. `Variants` and `GetEvents` require live RPC access and return
`temporarily_unavailable` until the node connection is re-established.

## Pagination Semantics

`GetEvents` returns matches in descending order by `(blockNumber, eventIndex)`.

- default `limit` is `100`
- `limit` is clamped to `1..=max_events_limit` (default `1000`, configurable via `--max-events-limit`; invalid zero-valued configs are rejected at startup)
- `before` is exclusive

Example cursor:

```json
{"blockNumber": 50, "eventIndex": 3}
```

This returns only events strictly older than `(50, 3)`.

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

- `GetEvents` responses
- `eventNotification` subscription deliveries

This keeps the JSON-RPC shape unchanged while making decoded payload availability depend on live node access.

## Delivery and Backpressure

Subscription delivery uses bounded internal queues.

- slow subscribers are removed instead of being buffered indefinitely
- the server attempts to send `subscriptionTerminated` before removal
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
with a request-scoped `subscription_limit` error response. Because the
subscription was never established, no `subscriptionTerminated` notification is
sent for that initial rejection.

Protocol-level limits (not configurable at runtime):

- max WebSocket message size: `256 KiB`
- max WebSocket frame size: `64 KiB`
- max custom key name length: `128` bytes
- max custom string key length: `1024` bytes
- max composite elements per composite value: `64`
- max composite nesting depth: `8`
- max encoded custom value size: `16384` bytes
