# WebSocket API

The public API is a JSON-over-WebSocket protocol exposed on `ws://localhost:8172`
by default.

There are two message classes:

- request/response messages, which always carry an `id`
- notifications, which never carry an `id`

For Internet-facing deployment guidance, read [Security And Deployment](./security.md).

## Requests

Every request includes:

- `id`: client-selected unsigned integer
- `type`: request discriminator

Example:

```json
{"id":1,"type":"Status"}
```

## Responses

Every successful or failed request receives a response with the same `id`.

Example:

```json
{
  "id": 1,
  "type": "status",
  "data": []
}
```

## Main Request Types

- `Status`: returns indexed block spans
- `Variants`: returns pallet and event variant metadata
- `GetEvents`: queries indexed events for a key, optionally paginated with `before`
- `SubscribeStatus`: subscribes to status changes
- `SubscribeEvents`: subscribes to updates for one key
- `UnsubscribeStatus`: removes a status subscription
- `UnsubscribeEvents`: removes an event subscription
- `SizeOnDisk`: returns current database size

## `Status`

Request:

```json
{"id":1,"type":"Status"}
```

Response type: `status`

Response payload:

- array of indexed block spans
- each span has `start` and `end`

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

## `Variants`

Request:

```json
{"id":2,"type":"Variants"}
```

Response type: `variants`

Response payload:

- array of pallets
- each pallet has `index`, `name`, and `events`
- each event variant has `index` and `name`

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

## Example `GetEvents`

```json
{
  "id": 3,
  "type": "GetEvents",
  "key": {
    "type": "Custom",
    "value": {"name": "ref_index", "kind": "u32", "value": 42}
  },
  "limit": 100,
  "before": null
}
```

Request fields:

- `key`: query key
- `limit`: optional `u16`, default `100`
- `before`: optional event cursor

Response type: `events`

Response payload:

- `key`: the queried key
- `events`: matching event refs, newest first
- `decodedEvents`: decoded payloads when event storage is enabled

Each `EventRef` contains:

- `blockNumber`
- `eventIndex`

Each decoded event object contains:

- `specVersion`
- `palletName`
- `eventName`
- `palletIndex`
- `variantIndex`
- `eventIndex`
- `fields`

Example response:

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

## Composite Custom Keys

Composite keys use an ordered array of typed values:

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

## `SizeOnDisk`

Request:

```json
{"id":4,"type":"SizeOnDisk"}
```

Response type: `sizeOnDisk`

Response payload: total database size in bytes as `u64`.

Example:

```json
{
  "id": 4,
  "type": "sizeOnDisk",
  "data": 123456
}
```

## Subscription Requests

Supported subscription management requests:

- `SubscribeStatus`
- `UnsubscribeStatus`
- `SubscribeEvents`
- `UnsubscribeEvents`

Subscription responses use `type: "subscriptionStatus"` and return an action
such as `subscribed` or `unsubscribed` plus the subscription target.

Example `SubscribeEvents` response:

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

## Notifications

Notifications are emitted for active subscriptions. They do not include `id`.

Representative notification types:

- `status`
- `eventNotification`
- `subscriptionTerminated`

`status` carries the same payload shape as a `Status` response.

`eventNotification` includes:

- `key`: subscribed key that matched
- `event`: matching event reference
- `decodedEvent`: decoded event object hydrated from the node before delivery

`subscriptionTerminated` is sent best-effort before the server drops a subscriber
that cannot keep up. The current reason is `backpressure`.

Example termination notice:

```json
{
  "type": "subscriptionTerminated",
  "data": {
    "reason": "backpressure",
    "message": "subscriber disconnected due to backpressure"
  }
}
```

This notice is best-effort only. If the subscriber queue is already full, the
termination notice may not be delivered before the connection is dropped.

## Errors

Invalid requests and handler failures are returned as normal responses with
`type: "error"` and the original request `id` when available.

Payload fields:

- `code`: stable machine-readable string
- `message`: human-readable detail

Current error codes include:

- `invalid_request`
- `internal_error`
- `subscription_limit`
- `temporarily_unavailable`

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

Protocol-level invalid custom key inputs include:

- custom key names longer than `128` bytes
- custom string values longer than `1024` bytes
- composite keys with more than `64` elements
- composite keys nested deeper than `8` levels
- custom values whose encoded key payload exceeds `16384` bytes

During node outages, local requests such as `Status` and `SizeOnDisk` continue
to work. `Variants` and `GetEvents` require live RPC access and return
`temporarily_unavailable` until the node connection is restored.

## Pagination Semantics

`GetEvents` returns matches in descending order by `(blockNumber, eventIndex)`.

- default `limit` is `100`
- `before` is exclusive

Example cursor:

```json
{"blockNumber": 50, "eventIndex": 3}
```

This returns only events strictly older than `(50, 3)`.

## Key Format

Keys are JSON objects with a `type` discriminant.

Supported shapes:

```json
{"type":"Variant","value":[0,3]}
{"type":"Custom","value":{"name":"para_id","kind":"u32","value":1000}}
{"type":"Custom","value":{"name":"account_id","kind":"bytes32","value":"0x1234..."}}
{"type":"Custom","value":{"name":"published","kind":"bool","value":true}}
{"type":"Custom","value":{"name":"revision","kind":"u128","value":"42"}}
{"type":"Custom","value":{"name":"slug","kind":"string","value":"hello"}}
{"type":"Custom","value":{"name":"balance","kind":"u64","value":"42"}}
```

Scalar kinds:

- `bytes32`
- `u32`
- `u64`
- `u128`
- `string`
- `bool`

## Integration Notes

- `GetEvents` returns newest-first ordering
- `before` acts as a pagination cursor
- decoded events are hydrated from the node for `GetEvents` and `eventNotification`
- `Variants` and `GetEvents` depend on a live RPC connection
- local requests like `Status` and `SizeOnDisk` can continue to work during temporary RPC outages
