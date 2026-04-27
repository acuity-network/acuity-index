# Configuration

Acuity Index uses two configuration layers:

- an index specification TOML that describes chain identity and indexing rules
- an optional runtime options TOML passed via `--options-config`

Runtime precedence is:

```text
CLI flags > --options-config file > built-in defaults
```

## Index Specification

Each chain is described by an index specification passed as `<INDEX_SPEC>`.

The repository also includes a larger checked-in example spec in the
repository.

```toml
name = "mychain"
genesis_hash = "abc123..."
default_url = "wss://my-node:443"
index_variant = false
spec_change_blocks = [0]

[keys]
account_id = "bytes32"
item_id = "bytes32"
revision_id = "u32"
item_revision = { fields = ["bytes32", "u32"] }

[[pallets]]
name = "MyPallet"

[[pallets.events]]
name = "SomeEvent"

[[pallets.events.params]]
field = "who"
key = "account_id"

[[pallets.events.params]]
fields = ["item_id", "revision_id"]
key = "item_revision"
```

## Important Fields

- `name`: logical name for the spec and default local storage path
- `genesis_hash`: chain identity guard; a database is tied to one chain only
- `default_url`: fallback node URL when CLI and runtime options do not override it
- `index_variant`: whether to store pallet/variant lookups in the variant tree
- `spec_change_blocks`: revision boundaries for historical reindex behavior

## Declared Keys

Scalar key kinds:

- `bytes32`
- `u32`
- `u64`
- `u128`
- `string`
- `bool`

Composite keys are declared structurally:

```toml
[keys]
item_revision = { fields = ["bytes32", "u32"] }
```

Use:

- `field = "..."` for scalar keys
- `fields = ["...", "..."]` for composite keys

Composite key values are ordered and binary encoded, so field order matters.

## Runtime Options Config

Deployment-specific settings can live in a separate TOML file:

```toml
url = "wss://mynode:443"
db_path = "/var/lib/acuity-index/mychain"
db_mode = "low_space"
db_cache_capacity = "1024 MiB"
queue_depth = 4
finalized = false
port = 8172
metrics_port = 9000
```

Supported fields:

| Field | Type | Default |
|---|---|---|
| `url` | string | index spec `default_url` |
| `db_path` | string | `~/.local/share/acuity-index/<chain>/db` |
| `db_mode` | string | `low_space` |
| `db_cache_capacity` | string | `1024.00 MiB` |
| `queue_depth` | integer | `1` |
| `finalized` | boolean | `false` |
| `port` | integer | `8172` |
| `metrics_port` | integer | disabled |

`index_variant` belongs in the index spec, not the runtime options file.

When `finalized = true`, `GetEvents` can return finalized block proofs for
requests that set `includeProofs = true`. In non-finalized mode the same request
shape is accepted, but the response reports proofs as unavailable.
