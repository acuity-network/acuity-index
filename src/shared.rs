//! Shared data types for acuity-index.

use serde::{Deserialize, Serialize};
use sled::Tree;
use std::{
    fmt,
    sync::{Mutex, MutexGuard},
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;
use tracing::error;
use zerocopy::{
    byteorder::{U16, U32},
    BigEndian, FromBytes, IntoBytes,
};
use zerocopy_derive::*;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(thiserror::Error, Debug)]
pub enum IndexError {
    #[error("database error")]
    Sled(#[from] sled::Error),
    #[error("connection error")]
    Subxt(#[from] subxt::Error),
    #[error("websocket error")]
    Tungstenite(#[from] tungstenite::Error),
    #[error("parse error")]
    Hex(#[from] hex::FromHexError),
    #[error("block not found: {0}")]
    BlockNotFound(u32),
    #[error(
        "node is pruning historical state at #{block_number}; --state-pruning must be set to archive-canonical"
    )]
    StatePruningMisconfigured { block_number: u32 },
    #[error("RPC error: {0}")]
    RpcError(#[from] subxt::rpcs::Error),
    #[error("codec error")]
    CodecError(#[from] subxt::ext::codec::Error),
    #[error("metadata error")]
    MetadataError(#[from] subxt::error::MetadataTryFromError),
    #[error("block stream error")]
    BlocksError(#[from] subxt::error::BlocksError),
    #[error("block stream closed")]
    BlockStreamClosed,
    #[error("events error")]
    EventsError(#[from] subxt::error::EventsError),
    #[error("at-block error")]
    OnlineClientAtBlockError(#[from] subxt::error::OnlineClientAtBlockError),
    #[error("online client error")]
    OnlineClientError(#[from] subxt::error::OnlineClientError),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("TOML serialization error")]
    TomlSer(#[from] toml::ser::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

pub fn internal_error(message: impl Into<String>) -> IndexError {
    IndexError::Internal(message.into())
}

pub fn metadata_version(metadata_bytes: &[u8]) -> Option<u8> {
    if metadata_bytes.len() < 5 {
        return None;
    }

    if &metadata_bytes[..4] != b"meta" {
        return None;
    }

    Some(metadata_bytes[4])
}

pub fn unsupported_metadata_error(version: u8, spec_name: &str, spec_version: u64) -> IndexError {
    internal_error(format!(
        "unsupported metadata version v{version} from runtime {spec_name} specVersion {spec_version}; the node may still be syncing early chain history before a runtime upgrade"
    ))
}

pub fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!("Recovering poisoned mutex: {name}");
            poisoned.into_inner()
        }
    }
}

pub fn decode_u32_key(bytes: &[u8]) -> Option<u32> {
    let key: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(key))
}

pub fn decode_event_ref_suffix(bytes: &[u8]) -> Option<EventRef> {
    let suffix: [u8; 6] = bytes.try_into().ok()?;
    Some(EventRef {
        block_number: u32::from_be_bytes(suffix[..4].try_into().ok()?),
        event_index: u16::from_be_bytes(suffix[4..].try_into().ok()?),
    })
}

pub fn read_span_db_value(bytes: &[u8]) -> Option<SpanDbValue> {
    SpanDbValue::read_from_bytes(bytes).ok()
}

// ─── On-disk key formats ──────────────────────────────────────────────────────

/// On-disk format for variant keys.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct VariantKey {
    pub pallet_index: u8,
    pub variant_index: u8,
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for stored event keys.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct EventKey {
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for span values.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct SpanDbValue {
    pub start: U32<BigEndian>,
    pub version: U16<BigEndian>,
    pub index_variant: u8,
    pub store_events: u8,
}

// ─── Bytes32 helper ──────────────────────────────────────────────────────────

#[derive(Copy, Clone, Debug, PartialEq, Hash, Eq)]
pub struct Bytes32(pub [u8; 32]);

impl AsRef<[u8; 32]> for Bytes32 {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Bytes32 {
    fn from(x: [u8; 32]) -> Self {
        Bytes32(x)
    }
}

impl AsRef<[u8]> for Bytes32 {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl Serialize for Bytes32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("0x{}", hex::encode(self.0)))
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let hex_part = s.strip_prefix("0x").unwrap_or(&s);
        let bytes = hex::decode(hex_part).map_err(serde::de::Error::custom)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 32 bytes"))?;
        Ok(Bytes32(arr))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct U64Text(pub u64);

impl Serialize for U64Text {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for U64Text {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = U64Text;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a u64 string or integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U64Text(value))
            }

            fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U64Text(u64::try_from(value).map_err(E::custom)?))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U64Text(value.parse::<u64>().map_err(E::custom)?))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct U128Text(pub u128);

impl Serialize for U128Text {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for U128Text {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = U128Text;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a u128 string or integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U128Text(value.into()))
            }

            fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U128Text(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(U128Text(value.parse::<u128>().map_err(E::custom)?))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CustomValue {
    Bytes32(Bytes32),
    U32(u32),
    U64(U64Text),
    U128(U128Text),
    String(String),
    Bool(bool),
    Composite(Vec<CustomValue>),
}

impl CustomValue {
    fn tag(&self) -> u8 {
        match self {
            CustomValue::Bytes32(_) => 0,
            CustomValue::U32(_) => 1,
            CustomValue::U64(_) => 2,
            CustomValue::U128(_) => 3,
            CustomValue::String(_) => 4,
            CustomValue::Bool(_) => 5,
            CustomValue::Composite(_) => 6,
        }
    }

    fn db_bytes(&self) -> Vec<u8> {
        match self {
            CustomValue::Bytes32(v) => v.0.to_vec(),
            CustomValue::U32(v) => v.to_be_bytes().to_vec(),
            CustomValue::U64(v) => v.0.to_be_bytes().to_vec(),
            CustomValue::U128(v) => v.0.to_be_bytes().to_vec(),
            CustomValue::String(v) => v.as_bytes().to_vec(),
            CustomValue::Bool(v) => vec![u8::from(*v)],
            CustomValue::Composite(values) => {
                let count = u16::try_from(values.len()).expect("composite value too large");
                let mut encoded = Vec::new();
                encoded.extend_from_slice(&count.to_be_bytes());
                for value in values {
                    encoded.push(value.tag());
                    let value_bytes = value.db_bytes();
                    let value_len =
                        u32::try_from(value_bytes.len()).expect("composite component too large");
                    encoded.extend_from_slice(&value_len.to_be_bytes());
                    encoded.extend_from_slice(&value_bytes);
                }
                encoded
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct CustomKey {
    pub name: String,
    #[serde(flatten)]
    pub value: CustomValue,
}

impl CustomKey {
    pub(crate) fn db_prefix(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let name_len = u16::try_from(name_bytes.len()).expect("custom key name too long");
        let value_bytes = self.value.db_bytes();
        let value_len = u32::try_from(value_bytes.len()).expect("custom key value too large");

        let mut prefix = Vec::with_capacity(2 + name_bytes.len() + 1 + 4 + value_bytes.len());
        prefix.extend_from_slice(&name_len.to_be_bytes());
        prefix.extend_from_slice(name_bytes);
        prefix.push(self.value.tag());
        prefix.extend_from_slice(&value_len.to_be_bytes());
        prefix.extend_from_slice(&value_bytes);
        prefix
    }
}

// ─── Database trees ───────────────────────────────────────────────────────────

const INDEX_NAMESPACE_CUSTOM: u8 = 2;

/// All database trees.
#[derive(Clone)]
pub struct Trees {
    pub root: sled::Db,
    pub span: Tree,
    pub variant: Tree,
    pub index: Tree,
    /// Stores JSON-encoded decoded events keyed by block number and event index.
    pub events: Tree,
}

impl Trees {
    pub fn open(db_config: sled::Config) -> Result<Self, sled::Error> {
        let db = db_config.open()?;
        Ok(Trees {
            span: db.open_tree(b"span")?,
            variant: db.open_tree(b"variant")?,
            index: db.open_tree(b"index")?,
            events: db.open_tree(b"events")?,
            root: db,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.root.flush()?;
        self.span.flush()?;
        self.variant.flush()?;
        self.index.flush()?;
        self.events.flush()?;
        Ok(())
    }
}

// ─── Key enum ────────────────────────────────────────────────────────────────

/// All indexable key types across all chains.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(tag = "type", content = "value")]
pub enum Key {
    Variant(u8, u8),
    Custom(CustomKey),
}

impl Key {
    pub fn index_prefix(&self) -> Option<Vec<u8>> {
        match self {
            Key::Variant(_, _) => None,
            Key::Custom(custom) => {
                let mut prefix = Vec::with_capacity(1 + custom.db_prefix().len());
                prefix.push(INDEX_NAMESPACE_CUSTOM);
                prefix.extend_from_slice(&custom.db_prefix());
                Some(prefix)
            }
        }
    }

    pub fn write_db_key(
        &self,
        trees: &Trees,
        block_number: u32,
        event_index: u16,
    ) -> Result<(), sled::Error> {
        let bn: U32<BigEndian> = block_number.into();
        let ei: U16<BigEndian> = event_index.into();

        match self {
            Key::Variant(pi, vi) => {
                let key = VariantKey {
                    pallet_index: *pi,
                    variant_index: *vi,
                    block_number: bn,
                    event_index: ei,
                };
                trees.variant.insert(key.as_bytes(), &[])?;
            }
            Key::Custom(_) => {
                let mut key = self.index_prefix().ok_or_else(|| {
                    sled::Error::Io(std::io::Error::other("custom key missing index prefix"))
                })?;
                key.extend_from_slice(&block_number.to_be_bytes());
                key.extend_from_slice(&event_index.to_be_bytes());
                trees.index.insert(key, &[])?;
            }
        }
        Ok(())
    }

    pub fn get_events(
        &self,
        trees: &Trees,
        before: Option<&EventRef>,
        limit: usize,
    ) -> Vec<EventRef> {
        use crate::websockets::get_events_index;
        match self {
            Key::Variant(pi, vi) => get_events_variant(&trees.variant, *pi, *vi, before, limit),
            Key::Custom(_) => self
                .index_prefix()
                .map(|prefix| get_events_index(&trees.index, &prefix, before, limit))
                .unwrap_or_default(),
        }
    }
}

fn get_events_variant(
    tree: &sled::Tree,
    pallet_id: u8,
    variant_id: u8,
    before: Option<&EventRef>,
    limit: usize,
) -> Vec<EventRef> {
    use zerocopy::FromBytes;
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix([pallet_id, variant_id]).keys();
    while let Some(Ok(key)) = iter.next_back() {
        let Ok(key) = VariantKey::read_from_bytes(&key) else {
            error!("Skipping malformed variant index key");
            continue;
        };
        let event = EventRef {
            block_number: key.block_number.into(),
            event_index: key.event_index.into(),
        };
        if before.is_some_and(|cursor| {
            (event.block_number, event.event_index) >= (cursor.block_number, cursor.event_index)
        }) {
            continue;
        }
        events.push(event);
        if events.len() == limit {
            break;
        }
    }
    events
}

// ─── Wire types ───────────────────────────────────────────────────────────────

/// Pointer to a specific event.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventRef {
    pub block_number: u32,
    pub event_index: u16,
}

impl fmt::Display for EventRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "block_number: {}, event_index: {}",
            self.block_number, self.event_index
        )
    }
}

/// A decoded event payload associated with an event ref.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DecodedEvent {
    pub block_number: u32,
    pub event_index: u16,
    /// JSON object for the decoded event.
    pub event: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct EventMeta {
    pub index: u8,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PalletMeta {
    pub index: u8,
    pub name: String,
    pub events: Vec<EventMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "start: {}, end: {}", self.start, self.end)
    }
}

/// JSON request messages.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum RequestBody {
    Status,
    SubscribeStatus,
    UnsubscribeStatus,
    Variants,
    GetEvents {
        key: Key,
        #[serde(default = "default_get_events_limit")]
        limit: u16,
        before: Option<EventRef>,
    },
    SubscribeEvents {
        key: Key,
    },
    UnsubscribeEvents {
        key: Key,
    },
    SizeOnDisk,
}

fn default_get_events_limit() -> u16 {
    100
}

/// JSON request envelope.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct RequestMessage {
    pub id: u64,
    #[serde(flatten)]
    pub body: RequestBody,
}

/// JSON response messages.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum ResponseBody {
    Status(Vec<Span>),
    Variants(Vec<PalletMeta>),
    Events {
        key: Key,
        events: Vec<EventRef>,
        #[serde(rename = "decodedEvents")]
        decoded_events: Vec<DecodedEvent>,
    },
    SubscriptionStatus {
        action: SubscriptionAction,
        target: SubscriptionTarget,
    },
    SizeOnDisk(u64),
    Error(ApiError),
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ResponseMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(flatten)]
    pub body: ResponseBody,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionAction {
    Subscribed,
    Unsubscribed,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SubscriptionTarget {
    Status,
    Events { key: Key },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionTerminationReason {
    Backpressure,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct NotificationMessage {
    #[serde(flatten)]
    pub body: NotificationBody,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum NotificationBody {
    Status(Vec<Span>),
    EventNotification {
        key: Key,
        event: EventRef,
        #[serde(rename = "decodedEvent")]
        decoded_event: Option<DecodedEvent>,
    },
    SubscriptionTerminated {
        reason: SubscriptionTerminationReason,
        message: String,
    },
}

/// Subscription messages from WebSocket threads to the indexer thread.
#[derive(Debug)]
pub enum SubscriptionMessage {
    SubscribeStatus {
        tx: mpsc::Sender<NotificationMessage>,
    },
    UnsubscribeStatus {
        tx: mpsc::Sender<NotificationMessage>,
    },
    SubscribeEvents {
        key: Key,
        tx: mpsc::Sender<NotificationMessage>,
    },
    UnsubscribeEvents {
        key: Key,
        tx: mpsc::Sender<NotificationMessage>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScalarKind;
    use serde::de::value::{
        Error as ValueError, StrDeserializer, U128Deserializer, U64Deserializer,
    };
    use serde::Deserialize;

    #[test]
    fn u64_text_serializes_and_deserializes_from_multiple_input_shapes() {
        assert_eq!(serde_json::to_string(&U64Text(42)).unwrap(), "\"42\"");
        assert_eq!(
            U64Text::deserialize(U64Deserializer::<ValueError>::new(42)).unwrap(),
            U64Text(42)
        );
        assert_eq!(
            U64Text::deserialize(U128Deserializer::<ValueError>::new(42)).unwrap(),
            U64Text(42)
        );
        assert_eq!(
            U64Text::deserialize(StrDeserializer::<ValueError>::new("42")).unwrap(),
            U64Text(42)
        );
    }

    #[test]
    fn u128_text_serializes_and_deserializes_from_multiple_input_shapes() {
        assert_eq!(serde_json::to_string(&U128Text(42)).unwrap(), "\"42\"");
        assert_eq!(
            U128Text::deserialize(U64Deserializer::<ValueError>::new(42)).unwrap(),
            U128Text(42)
        );
        assert_eq!(
            U128Text::deserialize(U128Deserializer::<ValueError>::new(42)).unwrap(),
            U128Text(42)
        );
        assert_eq!(
            U128Text::deserialize(StrDeserializer::<ValueError>::new("42")).unwrap(),
            U128Text(42)
        );
    }

    #[test]
    fn custom_value_kind_and_db_prefix_cover_all_scalar_types() {
        let cases = [
            (
                CustomValue::Bytes32(Bytes32([0xAB; 32])),
                ScalarKind::Bytes32,
                0u8,
            ),
            (CustomValue::U32(7), ScalarKind::U32, 1u8),
            (CustomValue::U64(U64Text(8)), ScalarKind::U64, 2u8),
            (CustomValue::U128(U128Text(9)), ScalarKind::U128, 3u8),
            (CustomValue::String("slug".into()), ScalarKind::String, 4u8),
            (CustomValue::Bool(true), ScalarKind::Bool, 5u8),
        ];

        for (value, kind, tag) in cases {
            let key = CustomKey {
                name: "example".into(),
                value,
            };
            let prefix = key.db_prefix();
            assert_eq!(u16::from_be_bytes(prefix[0..2].try_into().unwrap()), 7);
            assert_eq!(&prefix[2..9], b"example");
            assert_eq!(prefix[9], tag);
            match &key.value {
                CustomValue::Bytes32(_) => assert_eq!(kind, ScalarKind::Bytes32),
                CustomValue::U32(_) => assert_eq!(kind, ScalarKind::U32),
                CustomValue::U64(_) => assert_eq!(kind, ScalarKind::U64),
                CustomValue::U128(_) => assert_eq!(kind, ScalarKind::U128),
                CustomValue::String(_) => assert_eq!(kind, ScalarKind::String),
                CustomValue::Bool(_) => assert_eq!(kind, ScalarKind::Bool),
                CustomValue::Composite(_) => panic!("unexpected composite value"),
            }
        }
    }

    #[test]
    fn custom_value_composite_db_prefix_and_serde_round_trip() {
        let value = CustomValue::Composite(vec![
            CustomValue::Bytes32(Bytes32([0xAA; 32])),
            CustomValue::U32(7),
        ]);

        let key = CustomKey {
            name: "item_revision".into(),
            value: value.clone(),
        };
        let prefix = key.db_prefix();
        assert_eq!(prefix[15], 6);

        let json = serde_json::to_string(&value).unwrap();
        assert!(json.contains("\"kind\":\"composite\""));
        let decoded: CustomValue = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn deserializers_report_errors_and_tree_helpers_open_and_flush() {
        let u64_err = U64Text::deserialize(serde::de::value::UnitDeserializer::<ValueError>::new())
            .unwrap_err()
            .to_string();
        let u128_err =
            U128Text::deserialize(serde::de::value::UnitDeserializer::<ValueError>::new())
                .unwrap_err()
                .to_string();
        assert!(u64_err.contains("u64 string or integer"));
        assert!(u128_err.contains("u128 string or integer"));

        let trees = Trees::open(sled::Config::new().temporary(true)).unwrap();
        trees.flush().unwrap();
    }

    #[test]
    fn variant_key_retrieval_limits_to_recent_100_events() {
        let trees = Trees::open(sled::Config::new().temporary(true)).unwrap();
        let key = Key::Variant(1, 2);

        for i in 0..150u32 {
            key.write_db_key(&trees, i, 0).unwrap();
        }

        let events = key.get_events(&trees, None, 100);
        assert_eq!(events.len(), 100);
        assert_eq!(events[0].block_number, 149);
        assert_eq!(events[99].block_number, 50);
    }
}
