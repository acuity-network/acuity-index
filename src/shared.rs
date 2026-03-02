//! Shared data types for acuity-index.

use byteorder::BigEndian;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::fmt;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite;
use zerocopy::{
    AsBytes,
    byteorder::{U16, U32},
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
    #[error("parse error")]
    ParseError,
    #[error("block not found: {0}")]
    BlockNotFound(u32),
    #[error("RPC error")]
    RpcError(#[from] subxt::ext::subxt_rpcs::Error),
    #[error("codec error")]
    CodecError(#[from] subxt::ext::codec::Error),
    #[error("metadata error")]
    MetadataError(#[from] subxt::error::MetadataTryFromError),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("config error: {0}")]
    Config(String),
}

// ─── On-disk key formats ──────────────────────────────────────────────────────

/// On-disk format for variant keys.
#[derive(
    FromZeroes, FromBytes, AsBytes, Unaligned, PartialEq, Debug,
)]
#[repr(C)]
pub struct VariantKey {
    pub pallet_index: u8,
    pub variant_index: u8,
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for 32-byte keys.
#[derive(
    FromZeroes, FromBytes, AsBytes, Unaligned, PartialEq, Debug,
)]
#[repr(C)]
pub struct Bytes32Key {
    pub key: [u8; 32],
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for u32 keys.
#[derive(
    FromZeroes, FromBytes, AsBytes, Unaligned, PartialEq, Debug,
)]
#[repr(C)]
pub struct U32Key {
    pub key: U32<BigEndian>,
    pub block_number: U32<BigEndian>,
    pub event_index: U16<BigEndian>,
}

/// On-disk format for span values.
#[derive(
    FromZeroes, FromBytes, AsBytes, Unaligned, PartialEq, Debug,
)]
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
        serializer
            .serialize_str(&format!("0x{}", hex::encode(self.0)))
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let hex_part = s.strip_prefix("0x").unwrap_or(&s);
        let bytes = hex::decode(hex_part)
            .map_err(serde::de::Error::custom)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 32 bytes"))?;
        Ok(Bytes32(arr))
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

/// Chain-specific index trees (Polkadot/Kusama extras).
#[derive(Clone)]
pub struct ChainTrees {
    pub auction_index: Tree,
    pub candidate_hash: Tree,
    pub para_id: Tree,
}

impl ChainTrees {
    pub fn open(db: &Db) -> Result<Self, sled::Error> {
        Ok(ChainTrees {
            auction_index: db.open_tree(b"auction_index")?,
            candidate_hash: db.open_tree(b"candidate_hash")?,
            para_id: db.open_tree(b"para_id")?,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.auction_index.flush()?;
        self.candidate_hash.flush()?;
        self.para_id.flush()?;
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
    pub chain: ChainTrees,
    /// Stores JSON-encoded decoded event fields keyed by block number.
    pub block_events: Tree,
}

impl Trees {
    pub fn open(db_config: sled::Config) -> Result<Self, sled::Error> {
        let db = db_config.open()?;
        Ok(Trees {
            span: db.open_tree(b"span")?,
            variant: db.open_tree(b"variant")?,
            substrate: SubstrateTrees::open(&db)?,
            chain: ChainTrees::open(&db)?,
            block_events: db.open_tree(b"block_events")?,
            root: db,
        })
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.root.flush()?;
        self.span.flush()?;
        self.variant.flush()?;
        self.substrate.flush()?;
        self.chain.flush()?;
        self.block_events.flush()?;
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
    // Chain-specific
    AuctionIndex(u32),
    CandidateHash(Bytes32),
    ParaId(u32),
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
            Key::AuctionIndex(v) => {
                insert_u32!(trees.chain.auction_index, v)
            }
            Key::CandidateHash(v) => {
                insert_b32!(trees.chain.candidate_hash, v)
            }
            Key::ParaId(v) => insert_u32!(trees.chain.para_id, v),
        }
        Ok(())
    }

    pub fn get_events(&self, trees: &Trees) -> Vec<EventRef> {
        use crate::websockets::{get_events_bytes32, get_events_u32};
        match self {
            Key::Variant(pi, vi) => {
                get_events_variant(&trees.variant, *pi, *vi)
            }
            Key::AccountId(v) => {
                get_events_bytes32(&trees.substrate.account_id, v)
            }
            Key::AccountIndex(v) => {
                get_events_u32(&trees.substrate.account_index, *v)
            }
            Key::BountyIndex(v) => {
                get_events_u32(&trees.substrate.bounty_index, *v)
            }
            Key::EraIndex(v) => {
                get_events_u32(&trees.substrate.era_index, *v)
            }
            Key::MessageId(v) => {
                get_events_bytes32(&trees.substrate.message_id, v)
            }
            Key::PoolId(v) => {
                get_events_u32(&trees.substrate.pool_id, *v)
            }
            Key::PreimageHash(v) => {
                get_events_bytes32(&trees.substrate.preimage_hash, v)
            }
            Key::ProposalHash(v) => {
                get_events_bytes32(&trees.substrate.proposal_hash, v)
            }
            Key::ProposalIndex(v) => {
                get_events_u32(&trees.substrate.proposal_index, *v)
            }
            Key::RefIndex(v) => {
                get_events_u32(&trees.substrate.ref_index, *v)
            }
            Key::RegistrarIndex(v) => {
                get_events_u32(&trees.substrate.registrar_index, *v)
            }
            Key::SessionIndex(v) => {
                get_events_u32(&trees.substrate.session_index, *v)
            }
            Key::TipHash(v) => {
                get_events_bytes32(&trees.substrate.tip_hash, v)
            }
            Key::SpendIndex(v) => {
                get_events_u32(&trees.substrate.spend_index, *v)
            }
            Key::AuctionIndex(v) => {
                get_events_u32(&trees.chain.auction_index, *v)
            }
            Key::CandidateHash(v) => {
                get_events_bytes32(&trees.chain.candidate_hash, v)
            }
            Key::ParaId(v) => {
                get_events_u32(&trees.chain.para_id, *v)
            }
        }
    }
}

fn get_events_variant(
    tree: &sled::Tree,
    pallet_id: u8,
    variant_id: u8,
) -> Vec<EventRef> {
    use zerocopy::FromBytes;
    let mut events = Vec::new();
    let mut iter =
        tree.scan_prefix([pallet_id, variant_id]).keys();
    while let Some(Ok(key)) = iter.next_back() {
        let key = VariantKey::read_from(&key).unwrap();
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

/// A block's decoded events as JSON.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BlockEvents {
    pub block_number: u32,
    /// JSON array of decoded event objects for this block.
    pub events: serde_json::Value,
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
        block_events: Vec<BlockEvents>,
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
