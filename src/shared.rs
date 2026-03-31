//! Shared data types for acuity-index.

use crate::config::ScalarKind;
use scale_value::{Composite, Primitive, Value, ValueDef, Variant};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::fmt;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite;
use zerocopy::{
    byteorder::{U16, U32},
    BigEndian, IntoBytes,
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
    #[error("RPC error")]
    RpcError(#[from] subxt::rpcs::Error),
    #[error("codec error")]
    CodecError(#[from] subxt::ext::codec::Error),
    #[error("metadata error")]
    MetadataError(#[from] subxt::error::MetadataTryFromError),
    #[error("block stream error")]
    BlocksError(#[from] subxt::error::BlocksError),
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

/// On-disk format for 32-byte keys.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct Bytes32Key {
    pub key: [u8; 32],
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for u32 keys.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct U32Key {
    pub key: U32<BigEndian>,
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

/// Fixed-width header for stored event payloads.
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, PartialEq, Debug)]
#[repr(C)]
pub struct StoredEventHeader {
    pub spec_version: U32<BigEndian>,
    pub pallet_index: u8,
    pub variant_index: u8,
    pub event_index: U16<BigEndian>,
    pub pallet_name_len: U16<BigEndian>,
    pub event_name_len: U16<BigEndian>,
}

const STORED_VALUE_BOOL: u8 = 0;
const STORED_VALUE_CHAR: u8 = 1;
const STORED_VALUE_STRING: u8 = 2;
const STORED_VALUE_U128: u8 = 3;
const STORED_VALUE_I128: u8 = 4;
const STORED_VALUE_U256: u8 = 5;
const STORED_VALUE_I256: u8 = 6;
const STORED_VALUE_BIT_SEQUENCE: u8 = 7;
const STORED_VALUE_COMPOSITE_NAMED: u8 = 8;
const STORED_VALUE_COMPOSITE_UNNAMED: u8 = 9;
const STORED_VALUE_VARIANT: u8 = 10;

#[derive(Debug, Clone, PartialEq)]
pub struct StoredEvent {
    pub spec_version: u32,
    pub pallet_name: String,
    pub event_name: String,
    pub pallet_index: u8,
    pub variant_index: u8,
    pub event_index: u16,
    pub fields: StoredComposite,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoredComposite {
    Named(Vec<(String, StoredValue)>),
    Unnamed(Vec<StoredValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoredValue {
    Bool(bool),
    Char(char),
    String(String),
    U128(u128),
    I128(i128),
    U256([u8; 32]),
    I256([u8; 32]),
    BitSequence(Vec<u8>),
    Composite(StoredComposite),
    Variant {
        name: String,
        fields: StoredComposite,
    },
}

impl StoredEvent {
    pub fn from_scale(
        spec_version: u32,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        event_index: u16,
        fields: &Composite<()>,
    ) -> Self {
        StoredEvent {
            spec_version,
            pallet_name: pallet_name.to_owned(),
            event_name: event_name.to_owned(),
            pallet_index,
            variant_index,
            event_index,
            fields: StoredComposite::from_scale(fields),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let pallet_name = self.pallet_name.as_bytes();
        let event_name = self.event_name.as_bytes();
        let header = StoredEventHeader {
            spec_version: self.spec_version.into(),
            pallet_index: self.pallet_index,
            variant_index: self.variant_index,
            event_index: self.event_index.into(),
            pallet_name_len: u16::try_from(pallet_name.len())
                .expect("pallet name too long")
                .into(),
            event_name_len: u16::try_from(event_name.len())
                .expect("event name too long")
                .into(),
        };
        let mut bytes =
            Vec::with_capacity(header.as_bytes().len() + pallet_name.len() + event_name.len() + 64);
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(pallet_name);
        bytes.extend_from_slice(event_name);
        self.fields.encode_into(&mut bytes);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let header_len = 12;
        if bytes.len() < header_len {
            return None;
        }
        let spec_version = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let pallet_index = bytes[4];
        let variant_index = bytes[5];
        let event_index = u16::from_be_bytes(bytes[6..8].try_into().ok()?);
        let pallet_name_len = u16::from_be_bytes(bytes[8..10].try_into().ok()?) as usize;
        let event_name_len = u16::from_be_bytes(bytes[10..12].try_into().ok()?) as usize;
        let names_end = header_len
            .checked_add(pallet_name_len)?
            .checked_add(event_name_len)?;
        if bytes.len() < names_end {
            return None;
        }
        let pallet_name = std::str::from_utf8(&bytes[header_len..header_len + pallet_name_len])
            .ok()?
            .to_owned();
        let event_name = std::str::from_utf8(&bytes[header_len + pallet_name_len..names_end])
            .ok()?
            .to_owned();
        let (fields, consumed) = StoredComposite::decode(&bytes[names_end..])?;
        if consumed != bytes[names_end..].len() {
            return None;
        }
        Some(StoredEvent {
            spec_version,
            pallet_name,
            event_name,
            pallet_index,
            variant_index,
            event_index,
            fields,
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "specVersion": self.spec_version,
            "palletName": self.pallet_name,
            "eventName": self.event_name,
            "palletIndex": self.pallet_index,
            "variantIndex": self.variant_index,
            "eventIndex": self.event_index,
            "fields": self.fields.to_json(),
        })
    }
}

impl StoredComposite {
    pub fn from_scale(composite: &Composite<()>) -> Self {
        match composite {
            Composite::Named(fields) => StoredComposite::Named(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), StoredValue::from_scale(value)))
                    .collect(),
            ),
            Composite::Unnamed(fields) => {
                StoredComposite::Unnamed(fields.iter().map(StoredValue::from_scale).collect())
            }
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            StoredComposite::Named(fields) => {
                out.push(STORED_VALUE_COMPOSITE_NAMED);
                push_u32(out, fields.len());
                for (name, value) in fields {
                    push_string(out, name);
                    value.encode_into(out);
                }
            }
            StoredComposite::Unnamed(fields) => {
                out.push(STORED_VALUE_COMPOSITE_UNNAMED);
                push_u32(out, fields.len());
                for value in fields {
                    value.encode_into(out);
                }
            }
        }
    }

    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (&tag, rest) = bytes.split_first()?;
        let (len, offset) = read_u32(rest)?;
        let mut cursor = 1 + offset;
        match tag {
            STORED_VALUE_COMPOSITE_NAMED => {
                let mut fields = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let (name, name_len) = read_string(&bytes[cursor..])?;
                    cursor += name_len;
                    let (value, value_len) = StoredValue::decode(&bytes[cursor..])?;
                    cursor += value_len;
                    fields.push((name, value));
                }
                Some((StoredComposite::Named(fields), cursor))
            }
            STORED_VALUE_COMPOSITE_UNNAMED => {
                let mut fields = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let (value, value_len) = StoredValue::decode(&bytes[cursor..])?;
                    cursor += value_len;
                    fields.push(value);
                }
                Some((StoredComposite::Unnamed(fields), cursor))
            }
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            StoredComposite::Named(fields) => serde_json::Value::Object(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_json()))
                    .collect(),
            ),
            StoredComposite::Unnamed(fields) => {
                if fields.len() > 1 {
                    let as_bytes: Option<Vec<u8>> = fields
                        .iter()
                        .map(|value| match value {
                            StoredValue::U128(n) => u8::try_from(*n).ok(),
                            _ => None,
                        })
                        .collect();
                    if let Some(bytes) = as_bytes {
                        return serde_json::Value::String(format!("0x{}", hex::encode(bytes)));
                    }
                }
                if fields.len() == 1 {
                    fields[0].to_json()
                } else {
                    serde_json::Value::Array(fields.iter().map(StoredValue::to_json).collect())
                }
            }
        }
    }
}

impl StoredValue {
    pub fn from_scale(value: &Value<()>) -> Self {
        match &value.value {
            ValueDef::Composite(composite) => {
                StoredValue::Composite(StoredComposite::from_scale(composite))
            }
            ValueDef::Variant(Variant { name, values }) => StoredValue::Variant {
                name: name.clone(),
                fields: StoredComposite::from_scale(values),
            },
            ValueDef::BitSequence(bits) => {
                StoredValue::BitSequence(bits.iter().map(u8::from).collect())
            }
            ValueDef::Primitive(primitive) => match primitive {
                Primitive::Bool(value) => StoredValue::Bool(*value),
                Primitive::Char(value) => StoredValue::Char(*value),
                Primitive::String(value) => StoredValue::String(value.clone()),
                Primitive::U128(value) => StoredValue::U128(*value),
                Primitive::I128(value) => StoredValue::I128(*value),
                Primitive::U256(value) => StoredValue::U256(*value),
                Primitive::I256(value) => StoredValue::I256(*value),
            },
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            StoredValue::Bool(value) => {
                out.push(STORED_VALUE_BOOL);
                out.push(u8::from(*value));
            }
            StoredValue::Char(value) => {
                out.push(STORED_VALUE_CHAR);
                push_u32(out, *value as usize);
            }
            StoredValue::String(value) => {
                out.push(STORED_VALUE_STRING);
                push_string(out, value);
            }
            StoredValue::U128(value) => {
                out.push(STORED_VALUE_U128);
                out.extend_from_slice(&value.to_be_bytes());
            }
            StoredValue::I128(value) => {
                out.push(STORED_VALUE_I128);
                out.extend_from_slice(&value.to_be_bytes());
            }
            StoredValue::U256(value) => {
                out.push(STORED_VALUE_U256);
                out.extend_from_slice(value);
            }
            StoredValue::I256(value) => {
                out.push(STORED_VALUE_I256);
                out.extend_from_slice(value);
            }
            StoredValue::BitSequence(bits) => {
                out.push(STORED_VALUE_BIT_SEQUENCE);
                push_u32(out, bits.len());
                out.extend_from_slice(bits);
            }
            StoredValue::Composite(composite) => composite.encode_into(out),
            StoredValue::Variant { name, fields } => {
                out.push(STORED_VALUE_VARIANT);
                push_string(out, name);
                fields.encode_into(out);
            }
        }
    }

    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (&tag, rest) = bytes.split_first()?;
        match tag {
            STORED_VALUE_BOOL => Some((StoredValue::Bool(*rest.first()? != 0), 2)),
            STORED_VALUE_CHAR => {
                let (value, consumed) = read_u32(rest)?;
                Some((StoredValue::Char(char::from_u32(value)?), 1 + consumed))
            }
            STORED_VALUE_STRING => {
                let (value, consumed) = read_string(rest)?;
                Some((StoredValue::String(value), 1 + consumed))
            }
            STORED_VALUE_U128 => {
                let bytes: [u8; 16] = rest.get(..16)?.try_into().ok()?;
                Some((StoredValue::U128(u128::from_be_bytes(bytes)), 17))
            }
            STORED_VALUE_I128 => {
                let bytes: [u8; 16] = rest.get(..16)?.try_into().ok()?;
                Some((StoredValue::I128(i128::from_be_bytes(bytes)), 17))
            }
            STORED_VALUE_U256 => {
                let bytes: [u8; 32] = rest.get(..32)?.try_into().ok()?;
                Some((StoredValue::U256(bytes), 33))
            }
            STORED_VALUE_I256 => {
                let bytes: [u8; 32] = rest.get(..32)?.try_into().ok()?;
                Some((StoredValue::I256(bytes), 33))
            }
            STORED_VALUE_BIT_SEQUENCE => {
                let (len, consumed) = read_u32(rest)?;
                let len = len as usize;
                let bits = rest.get(consumed..consumed + len)?.to_vec();
                Some((StoredValue::BitSequence(bits), 1 + consumed + len))
            }
            STORED_VALUE_COMPOSITE_NAMED | STORED_VALUE_COMPOSITE_UNNAMED => {
                let (composite, consumed) = StoredComposite::decode(bytes)?;
                Some((StoredValue::Composite(composite), consumed))
            }
            STORED_VALUE_VARIANT => {
                let (name, name_len) = read_string(rest)?;
                let (fields, fields_len) = StoredComposite::decode(&rest[name_len..])?;
                Some((
                    StoredValue::Variant { name, fields },
                    1 + name_len + fields_len,
                ))
            }
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            StoredValue::Bool(value) => serde_json::json!(value),
            StoredValue::Char(value) => serde_json::json!(value.to_string()),
            StoredValue::String(value) => serde_json::json!(value),
            StoredValue::U128(value) => serde_json::json!(value.to_string()),
            StoredValue::I128(value) => serde_json::json!(value.to_string()),
            StoredValue::U256(value) => serde_json::json!(format!("0x{}", hex::encode(value))),
            StoredValue::I256(value) => serde_json::json!(format!("0x{}", hex::encode(value))),
            StoredValue::BitSequence(_) => serde_json::Value::String("<bitseq>".to_string()),
            StoredValue::Composite(composite) => composite.to_json(),
            StoredValue::Variant { name, fields } => serde_json::json!({
                "variant": name,
                "fields": fields.to_json(),
            }),
        }
    }
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u32::try_from(value).expect("value too large").to_be_bytes());
}

fn push_string(out: &mut Vec<u8>, value: &str) {
    push_u32(out, value.len());
    out.extend_from_slice(value.as_bytes());
}

fn read_u32(bytes: &[u8]) -> Option<(u32, usize)> {
    let value = u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?);
    Some((value, 4))
}

fn read_string(bytes: &[u8]) -> Option<(String, usize)> {
    let (len, consumed) = read_u32(bytes)?;
    let len = len as usize;
    let value = std::str::from_utf8(bytes.get(consumed..consumed + len)?).ok()?;
    Some((value.to_owned(), consumed + len))
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
}

impl CustomValue {
    pub fn kind(&self) -> ScalarKind {
        match self {
            CustomValue::Bytes32(_) => ScalarKind::Bytes32,
            CustomValue::U32(_) => ScalarKind::U32,
            CustomValue::U64(_) => ScalarKind::U64,
            CustomValue::U128(_) => ScalarKind::U128,
            CustomValue::String(_) => ScalarKind::String,
            CustomValue::Bool(_) => ScalarKind::Bool,
        }
    }

    fn tag(&self) -> u8 {
        match self.kind() {
            ScalarKind::Bytes32 => 0,
            ScalarKind::U32 => 1,
            ScalarKind::U64 => 2,
            ScalarKind::U128 => 3,
            ScalarKind::String => 4,
            ScalarKind::Bool => 5,
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

/// Substrate built-in index trees.
#[derive(Clone)]
pub struct SubstrateTrees {
    pub account_id: Tree,
    pub account_index: Tree,
    pub bounty_index: Tree,
    pub era_index: Tree,
    pub message_id: Tree,
    pub pool_id: Tree,
    pub preimage_hash: Tree,
    pub proposal_hash: Tree,
    pub proposal_index: Tree,
    pub ref_index: Tree,
    pub registrar_index: Tree,
    pub session_index: Tree,
    pub tip_hash: Tree,
    pub spend_index: Tree,
}

impl SubstrateTrees {
    pub fn open(db: &Db) -> Result<Self, sled::Error> {
        Ok(SubstrateTrees {
            account_id: db.open_tree(b"account_id")?,
            account_index: db.open_tree(b"account_index")?,
            bounty_index: db.open_tree(b"bounty_index")?,
            era_index: db.open_tree(b"era_index")?,
            message_id: db.open_tree(b"message_id")?,
            pool_id: db.open_tree(b"pool_id")?,
            preimage_hash: db.open_tree(b"preimage_hash")?,
            proposal_hash: db.open_tree(b"proposal_hash")?,
            proposal_index: db.open_tree(b"proposal_index")?,
            ref_index: db.open_tree(b"ref_index")?,
            registrar_index: db.open_tree(b"registrar_index")?,
            session_index: db.open_tree(b"session_index")?,
            tip_hash: db.open_tree(b"tip_hash")?,
            spend_index: db.open_tree(b"spend_index")?,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.account_id.flush()?;
        self.account_index.flush()?;
        self.bounty_index.flush()?;
        self.era_index.flush()?;
        self.message_id.flush()?;
        self.pool_id.flush()?;
        self.preimage_hash.flush()?;
        self.proposal_hash.flush()?;
        self.proposal_index.flush()?;
        self.ref_index.flush()?;
        self.registrar_index.flush()?;
        self.session_index.flush()?;
        self.tip_hash.flush()?;
        self.spend_index.flush()?;
        Ok(())
    }
}

/// All database trees.
#[derive(Clone)]
pub struct Trees {
    pub root: sled::Db,
    pub span: Tree,
    pub variant: Tree,
    pub substrate: SubstrateTrees,
    pub custom: Tree,
    /// Stores JSON-encoded decoded events keyed by block number and event index.
    pub events: Tree,
}

impl Trees {
    pub fn open(db_config: sled::Config) -> Result<Self, sled::Error> {
        let db = db_config.open()?;
        Ok(Trees {
            span: db.open_tree(b"span")?,
            variant: db.open_tree(b"variant")?,
            substrate: SubstrateTrees::open(&db)?,
            custom: db.open_tree(b"custom")?,
            events: db.open_tree(b"events")?,
            root: db,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.root.flush()?;
        self.span.flush()?;
        self.variant.flush()?;
        self.substrate.flush()?;
        self.custom.flush()?;
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
    // Substrate built-ins
    AccountId(Bytes32),
    AccountIndex(u32),
    BountyIndex(u32),
    EraIndex(u32),
    MessageId(Bytes32),
    PoolId(u32),
    PreimageHash(Bytes32),
    ProposalHash(Bytes32),
    ProposalIndex(u32),
    RefIndex(u32),
    RegistrarIndex(u32),
    SessionIndex(u32),
    TipHash(Bytes32),
    SpendIndex(u32),
    // Config-defined custom keys
    Custom(CustomKey),
}

impl Key {
    pub fn write_db_key(
        &self,
        trees: &Trees,
        block_number: u32,
        event_index: u16,
    ) -> Result<(), sled::Error> {
        let bn: U32<BigEndian> = block_number.into();
        let ei: U16<BigEndian> = event_index.into();

        macro_rules! insert_u32 {
            ($tree:expr, $v:expr) => {{
                let key = U32Key {
                    key: (*$v).into(),
                    block_number: bn,
                    event_index: ei,
                };
                $tree.insert(key.as_bytes(), &[])?;
            }};
        }
        macro_rules! insert_b32 {
            ($tree:expr, $v:expr) => {{
                let key = Bytes32Key {
                    key: $v.0,
                    block_number: bn,
                    event_index: ei,
                };
                $tree.insert(key.as_bytes(), &[])?;
            }};
        }

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
            Key::AccountId(v) => insert_b32!(trees.substrate.account_id, v),
            Key::AccountIndex(v) => {
                insert_u32!(trees.substrate.account_index, v)
            }
            Key::BountyIndex(v) => {
                insert_u32!(trees.substrate.bounty_index, v)
            }
            Key::EraIndex(v) => insert_u32!(trees.substrate.era_index, v),
            Key::MessageId(v) => {
                insert_b32!(trees.substrate.message_id, v)
            }
            Key::PoolId(v) => insert_u32!(trees.substrate.pool_id, v),
            Key::PreimageHash(v) => {
                insert_b32!(trees.substrate.preimage_hash, v)
            }
            Key::ProposalHash(v) => {
                insert_b32!(trees.substrate.proposal_hash, v)
            }
            Key::ProposalIndex(v) => {
                insert_u32!(trees.substrate.proposal_index, v)
            }
            Key::RefIndex(v) => insert_u32!(trees.substrate.ref_index, v),
            Key::RegistrarIndex(v) => {
                insert_u32!(trees.substrate.registrar_index, v)
            }
            Key::SessionIndex(v) => {
                insert_u32!(trees.substrate.session_index, v)
            }
            Key::TipHash(v) => insert_b32!(trees.substrate.tip_hash, v),
            Key::SpendIndex(v) => {
                insert_u32!(trees.substrate.spend_index, v)
            }
            Key::Custom(custom) => {
                let mut key = custom.db_prefix();
                key.extend_from_slice(&block_number.to_be_bytes());
                key.extend_from_slice(&event_index.to_be_bytes());
                trees.custom.insert(key, &[])?;
            }
        }
        Ok(())
    }

    pub fn get_events(&self, trees: &Trees) -> Vec<EventRef> {
        use crate::websockets::{get_events_bytes32, get_events_custom, get_events_u32};
        match self {
            Key::Variant(pi, vi) => get_events_variant(&trees.variant, *pi, *vi),
            Key::AccountId(v) => get_events_bytes32(&trees.substrate.account_id, v),
            Key::AccountIndex(v) => get_events_u32(&trees.substrate.account_index, *v),
            Key::BountyIndex(v) => get_events_u32(&trees.substrate.bounty_index, *v),
            Key::EraIndex(v) => get_events_u32(&trees.substrate.era_index, *v),
            Key::MessageId(v) => get_events_bytes32(&trees.substrate.message_id, v),
            Key::PoolId(v) => get_events_u32(&trees.substrate.pool_id, *v),
            Key::PreimageHash(v) => get_events_bytes32(&trees.substrate.preimage_hash, v),
            Key::ProposalHash(v) => get_events_bytes32(&trees.substrate.proposal_hash, v),
            Key::ProposalIndex(v) => get_events_u32(&trees.substrate.proposal_index, *v),
            Key::RefIndex(v) => get_events_u32(&trees.substrate.ref_index, *v),
            Key::RegistrarIndex(v) => get_events_u32(&trees.substrate.registrar_index, *v),
            Key::SessionIndex(v) => get_events_u32(&trees.substrate.session_index, *v),
            Key::TipHash(v) => get_events_bytes32(&trees.substrate.tip_hash, v),
            Key::SpendIndex(v) => get_events_u32(&trees.substrate.spend_index, *v),
            Key::Custom(custom) => get_events_custom(&trees.custom, custom),
        }
    }
}

fn get_events_variant(tree: &sled::Tree, pallet_id: u8, variant_id: u8) -> Vec<EventRef> {
    use zerocopy::FromBytes;
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix([pallet_id, variant_id]).keys();
    while let Some(Ok(key)) = iter.next_back() {
        let key = VariantKey::read_from_bytes(&key).unwrap();
        events.push(EventRef {
            block_number: key.block_number.into(),
            event_index: key.event_index.into(),
        });
        if events.len() == 100 {
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
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum RequestMessage {
    Status,
    SubscribeStatus,
    UnsubscribeStatus,
    Variants,
    GetEvents { key: Key },
    SubscribeEvents { key: Key },
    UnsubscribeEvents { key: Key },
    SizeOnDisk,
}

/// JSON response messages.
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum ResponseMessage {
    Status(Vec<Span>),
    Variants(Vec<PalletMeta>),
    Events {
        key: Key,
        events: Vec<EventRef>,
        #[serde(rename = "decodedEvents")]
        decoded_events: Vec<DecodedEvent>,
    },
    Subscribed,
    Unsubscribed,
    SizeOnDisk(u64),
}

/// Subscription messages from WebSocket threads to the indexer thread.
#[derive(Debug)]
pub enum SubscriptionMessage {
    SubscribeStatus {
        tx: UnboundedSender<ResponseMessage>,
    },
    UnsubscribeStatus {
        tx: UnboundedSender<ResponseMessage>,
    },
    SubscribeEvents {
        key: Key,
        tx: UnboundedSender<ResponseMessage>,
    },
    UnsubscribeEvents {
        key: Key,
        tx: UnboundedSender<ResponseMessage>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
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
            assert_eq!(value.kind(), kind);
            let key = CustomKey {
                name: "example".into(),
                value,
            };
            let prefix = key.db_prefix();
            assert_eq!(u16::from_be_bytes(prefix[0..2].try_into().unwrap()), 7);
            assert_eq!(&prefix[2..9], b"example");
            assert_eq!(prefix[9], tag);
        }
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

        let db = sled::Config::new().temporary(true).open().unwrap();
        let substrate = SubstrateTrees::open(&db).unwrap();
        substrate.flush().unwrap();

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

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 100);
        assert_eq!(events[0].block_number, 149);
        assert_eq!(events[99].block_number, 50);
    }
}
