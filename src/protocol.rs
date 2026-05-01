//! Protocol and storage data types for acuity-index.

use serde::{Deserialize, Serialize};
use sled::Tree;
use std::fmt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::error;
use zerocopy::{
    BigEndian, FromBytes, IntoBytes,
    byteorder::{U16, U32},
};
use zerocopy_derive::*;

use crate::errors::{IndexError, internal_error};

pub fn decode_u32_key(bytes: &[u8]) -> Option<u32> {
    let key: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(key))
}

pub const EVENT_REF_SUFFIX_LEN: usize = 8;

pub fn decode_event_ref_suffix(bytes: &[u8]) -> Option<EventRef> {
    let suffix: [u8; EVENT_REF_SUFFIX_LEN] = bytes.try_into().ok()?;
    Some(EventRef {
        block_number: u32::from_be_bytes(suffix[..4].try_into().ok()?),
        event_index: u32::from_be_bytes(suffix[4..].try_into().ok()?),
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
    pub event_index: U32<BigEndian>,
}

/// On-disk format for span values.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct SpanDbValue {
    pub start: U32<BigEndian>,
    pub version: U16<BigEndian>,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HexBytes(pub Vec<u8>);

impl Serialize for HexBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("0x{}", hex::encode(&self.0)))
    }
}

impl<'de> Deserialize<'de> for HexBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let hex_part = s.strip_prefix("0x").unwrap_or(&s);
        let bytes = hex::decode(hex_part).map_err(serde::de::Error::custom)?;
        Ok(HexBytes(bytes))
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

    pub(crate) fn db_bytes(&self) -> Result<Vec<u8>, IndexError> {
        match self {
            CustomValue::Bytes32(v) => Ok(v.0.to_vec()),
            CustomValue::U32(v) => Ok(v.to_be_bytes().to_vec()),
            CustomValue::U64(v) => Ok(v.0.to_be_bytes().to_vec()),
            CustomValue::U128(v) => Ok(v.0.to_be_bytes().to_vec()),
            CustomValue::String(v) => Ok(v.as_bytes().to_vec()),
            CustomValue::Bool(v) => Ok(vec![u8::from(*v)]),
            CustomValue::Composite(values) => {
                let count = u16::try_from(values.len())
                    .map_err(|_| internal_error("composite value too large"))?;
                let mut encoded = Vec::new();
                encoded.extend_from_slice(&count.to_be_bytes());
                for value in values {
                    encoded.push(value.tag());
                    let value_bytes = value.db_bytes()?;
                    let value_len = u32::try_from(value_bytes.len())
                        .map_err(|_| internal_error("composite component too large"))?;
                    encoded.extend_from_slice(&value_len.to_be_bytes());
                    encoded.extend_from_slice(&value_bytes);
                }
                Ok(encoded)
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
    pub(crate) fn db_prefix(&self) -> Result<Vec<u8>, IndexError> {
        let name_bytes = self.name.as_bytes();
        let name_len = u16::try_from(name_bytes.len())
            .map_err(|_| internal_error("custom key name too long"))?;
        let value_bytes = self.value.db_bytes()?;
        let value_len = u32::try_from(value_bytes.len())
            .map_err(|_| internal_error("custom key value too large"))?;

        let mut prefix = Vec::with_capacity(2 + name_bytes.len() + 1 + 4 + value_bytes.len());
        prefix.extend_from_slice(&name_len.to_be_bytes());
        prefix.extend_from_slice(name_bytes);
        prefix.push(self.value.tag());
        prefix.extend_from_slice(&value_len.to_be_bytes());
        prefix.extend_from_slice(&value_bytes);
        Ok(prefix)
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
}

impl Trees {
    pub fn open(db_config: sled::Config) -> Result<Self, sled::Error> {
        let db = db_config.open()?;
        Ok(Trees {
            span: db.open_tree(b"span")?,
            variant: db.open_tree(b"variant")?,
            index: db.open_tree(b"index")?,
            root: db,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.root.flush()?;
        self.span.flush()?;
        self.variant.flush()?;
        self.index.flush()?;
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
    pub fn index_prefix(&self) -> Result<Option<Vec<u8>>, IndexError> {
        match self {
            Key::Variant(_, _) => Ok(None),
            Key::Custom(custom) => {
                let db_prefix = custom.db_prefix()?;
                let mut prefix = Vec::with_capacity(1 + db_prefix.len());
                prefix.push(INDEX_NAMESPACE_CUSTOM);
                prefix.extend_from_slice(&db_prefix);
                Ok(Some(prefix))
            }
        }
    }

    pub fn write_db_key(
        &self,
        trees: &Trees,
        block_number: u32,
        event_index: u32,
    ) -> Result<(), IndexError> {
        let bn: U32<BigEndian> = block_number.into();
        let ei: U32<BigEndian> = event_index.into();

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
                let prefix = self
                    .index_prefix()?
                    .ok_or_else(|| internal_error("custom key missing index prefix"))?;
                let mut key = prefix;
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
    ) -> Result<Vec<EventRef>, IndexError> {
        match self {
            Key::Variant(pi, vi) => Ok(get_events_variant(&trees.variant, *pi, *vi, before, limit)),
            Key::Custom(_) => {
                let prefix = self
                    .index_prefix()?
                    .ok_or_else(|| internal_error("custom key missing index prefix"))?;
                Ok(get_events_index(&trees.index, &prefix, before, limit))
            }
        }
    }
}

pub(crate) fn get_events_index(
    tree: &Tree,
    prefix: &[u8],
    before: Option<&EventRef>,
    limit: usize,
) -> Vec<EventRef> {
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix(prefix).keys();
    while let Some(Ok(raw)) = iter.next_back() {
        if raw.len() < EVENT_REF_SUFFIX_LEN {
            continue;
        }
        let suffix = &raw[raw.len() - EVENT_REF_SUFFIX_LEN..];
        let Some(event) = decode_event_ref_suffix(suffix) else {
            error!("Skipping malformed event index key");
            continue;
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
    pub event_index: u32,
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
    pub event_index: u32,
    /// JSON object for the decoded event.
    pub event: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventBlockProof {
    pub block_number: u32,
    pub block_hash: HexBytes,
    pub header: serde_json::Value,
    pub storage_key: HexBytes,
    pub storage_value: HexBytes,
    pub storage_proof: Vec<HexBytes>,
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

// ─── JSON-RPC 2.0 envelope types ─────────────────────────────────────────────

/// JSON-RPC 2.0 request envelope.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// JSON-RPC 2.0 success response.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JsonRpcSuccessResponse {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub result: serde_json::Value,
}

/// JSON-RPC 2.0 error object.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// JSON-RPC 2.0 error response.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: &'static str,
    pub id: Option<u64>,
    pub error: JsonRpcError,
}

/// Either a success or error JSON-RPC response.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum JsonRpcResponse {
    Success(JsonRpcSuccessResponse),
    Error(JsonRpcErrorResponse),
}

// ─── JSON-RPC standard error codes ───────────────────────────────────────────

pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;
pub const UPSTREAM_UNAVAILABLE: i32 = -32001;

// ─── Error data reason codes ─────────────────────────────────────────────────

pub const REASON_INVALID_KEY: &str = "invalid_key";
pub const REASON_INVALID_CURSOR: &str = "invalid_cursor";
pub const REASON_SUBSCRIPTION_LIMIT: &str = "subscription_limit";
pub const REASON_TEMPORARILY_UNAVAILABLE: &str = "temporarily_unavailable";
pub const REASON_PROOFS_UNAVAILABLE: &str = "proofs_unavailable";

// ─── JSON-RPC response builders ──────────────────────────────────────────────

pub fn jsonrpc_success(id: u64, result: serde_json::Value) -> JsonRpcResponse {
    JsonRpcResponse::Success(JsonRpcSuccessResponse {
        jsonrpc: "2.0",
        id,
        result,
    })
}

pub fn jsonrpc_error_with_id(
    id: Option<u64>,
    code: i32,
    message: impl Into<String>,
    reason: Option<&str>,
) -> JsonRpcResponse {
    let data = reason.map(|r| serde_json::json!({ "reason": r }));
    JsonRpcResponse::Error(JsonRpcErrorResponse {
        jsonrpc: "2.0",
        id,
        error: JsonRpcError {
            code,
            message: message.into(),
            data,
        },
    })
}

pub fn jsonrpc_error(id: u64, code: i32, message: impl Into<String>, reason: Option<&str>) -> JsonRpcResponse {
    jsonrpc_error_with_id(Some(id), code, message, reason)
}

pub fn jsonrpc_parse_error(message: impl Into<String>) -> JsonRpcResponse {
    jsonrpc_error_with_id(None, PARSE_ERROR, message, None)
}

pub fn jsonrpc_invalid_request(message: impl Into<String>) -> JsonRpcResponse {
    jsonrpc_error_with_id(None, INVALID_REQUEST, message, None)
}

pub fn jsonrpc_method_not_found(id: u64, method: &str) -> JsonRpcResponse {
    jsonrpc_error(id, METHOD_NOT_FOUND, format!("method not found: {method}"), None)
}

pub fn jsonrpc_invalid_params(id: u64, message: impl Into<String>, reason: &str) -> JsonRpcResponse {
    jsonrpc_error(id, INVALID_PARAMS, message, Some(reason))
}

pub fn jsonrpc_internal_error(id: u64, message: impl Into<String>) -> JsonRpcResponse {
    jsonrpc_error(id, INTERNAL_ERROR, message, None)
}

pub fn jsonrpc_temporarily_unavailable(id: u64) -> JsonRpcResponse {
    jsonrpc_error(
        id,
        UPSTREAM_UNAVAILABLE,
        "node temporarily unavailable",
        Some(REASON_TEMPORARILY_UNAVAILABLE),
    )
}

pub fn jsonrpc_subscription_limit(id: u64, message: impl Into<String>) -> JsonRpcResponse {
    jsonrpc_error(id, INVALID_PARAMS, message, Some(REASON_SUBSCRIPTION_LIMIT))
}

// ─── Typed params structs ────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GetEventsParams {
    pub key: Key,
    #[serde(default = "default_get_events_limit")]
    pub limit: u16,
    pub before: Option<EventRef>,
}

fn default_get_events_limit() -> u16 {
    100
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SubscribeEventsParams {
    pub key: Key,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct UnsubscribeParams {
    pub subscription: String,
}

// ─── Typed result structs ────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IndexStatusResult {
    pub spans: Vec<Span>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct GetEventMetadataResult {
    pub pallets: Vec<PalletMeta>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProofsResult {
    pub available: bool,
    pub reason: String,
    pub message: String,
    pub items: Vec<EventBlockProof>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PageResult {
    pub next_cursor: Option<EventRef>,
    pub has_more: bool,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GetEventsResult {
    pub key: Key,
    pub events: Vec<EventRef>,
    pub decoded_events: Vec<DecodedEvent>,
    pub proofs: ProofsResult,
    pub page: PageResult,
}

// ─── Subscription notification types ─────────────────────────────────────────

/// JSON-RPC 2.0 subscription notification.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JsonRpcNotification {
    pub jsonrpc: &'static str,
    pub method: &'static str,
    pub params: NotificationParams,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct NotificationParams {
    pub subscription: String,
    pub result: NotificationResult,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NotificationResult {
    Status {
        spans: Vec<Span>,
    },
    Event {
        key: Key,
        event: EventRef,
        #[serde(rename = "decodedEvent")]
        decoded_event: Option<DecodedEvent>,
    },
    Terminated {
        reason: String,
        message: String,
    },
}

/// Subscription messages from WebSocket connections to the shared subscription dispatcher.
#[derive(Debug)]
pub enum SubscriptionMessage {
    SubscribeStatus {
        tx: mpsc::Sender<JsonRpcNotification>,
        response_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
    UnsubscribeStatus {
        tx: mpsc::Sender<JsonRpcNotification>,
        response_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
    SubscribeEvents {
        key: Key,
        tx: mpsc::Sender<JsonRpcNotification>,
        response_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
    UnsubscribeEvents {
        key: Key,
        tx: mpsc::Sender<JsonRpcNotification>,
        response_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
}

// ─── WebSocket configuration ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsConfig {
    pub max_connections: usize,
    pub max_total_subscriptions: usize,
    pub max_subscriptions_per_connection: usize,
    pub subscription_buffer_size: usize,
    pub subscription_control_buffer_size: usize,
    pub idle_timeout_secs: u64,
    pub max_events_limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveWsConfig {
    pub max_connections: usize,
    pub max_total_subscriptions: usize,
    pub max_subscriptions_per_connection: usize,
    pub subscription_buffer_size: usize,
    pub idle_timeout_secs: u64,
    pub max_events_limit: usize,
}

impl From<&WsConfig> for LiveWsConfig {
    fn from(config: &WsConfig) -> Self {
        Self {
            max_connections: config.max_connections,
            max_total_subscriptions: config.max_total_subscriptions,
            max_subscriptions_per_connection: config.max_subscriptions_per_connection,
            subscription_buffer_size: config.subscription_buffer_size,
            idle_timeout_secs: config.idle_timeout_secs,
            max_events_limit: config.max_events_limit,
        }
    }
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_total_subscriptions: 65536,
            max_subscriptions_per_connection: 128,
            subscription_buffer_size: 256,
            subscription_control_buffer_size: 1024,
            idle_timeout_secs: 300,
            max_events_limit: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScalarKind;
    use serde::Deserialize;
    use serde::de::value::{
        Error as ValueError, StrDeserializer, U64Deserializer, U128Deserializer,
    };

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
            let prefix = key.db_prefix().unwrap();
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
        let prefix = key.db_prefix().unwrap();
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
    fn db_bytes_returns_error_for_oversized_composite() {
        let values: Vec<CustomValue> = (0..=u16::MAX as usize)
            .map(|_| CustomValue::U32(1))
            .collect();
        let value = CustomValue::Composite(values);
        assert!(value.db_bytes().is_err());
    }

    #[test]
    fn db_prefix_returns_error_for_oversized_composite_value() {
        let values: Vec<CustomValue> = (0..=u16::MAX as usize)
            .map(|_| CustomValue::U32(1))
            .collect();
        let key = CustomKey {
            name: "big".into(),
            value: CustomValue::Composite(values),
        };
        assert!(key.db_prefix().is_err());
    }

    #[test]
    fn variant_key_retrieval_limits_to_recent_100_events() {
        let trees = Trees::open(sled::Config::new().temporary(true)).unwrap();
        let key = Key::Variant(1, 2);

        for i in 0..150u32 {
            key.write_db_key(&trees, i, 0).unwrap();
        }

        let events = key.get_events(&trees, None, 100).unwrap();
        assert_eq!(events.len(), 100);
        assert_eq!(events[0].block_number, 149);
        assert_eq!(events[99].block_number, 50);
    }
}
