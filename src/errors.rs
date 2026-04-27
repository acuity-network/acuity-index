use tokio_tungstenite::tungstenite;

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

impl IndexError {
    pub fn is_recoverable(&self) -> bool {
        match self {
            IndexError::Subxt(_)
            | IndexError::RpcError(_)
            | IndexError::BlocksError(_)
            | IndexError::BlockStreamClosed
            | IndexError::EventsError(_)
            | IndexError::OnlineClientAtBlockError(_)
            | IndexError::OnlineClientError(_)
            | IndexError::BlockNotFound(_) => true,
            IndexError::Sled(_)
            | IndexError::Tungstenite(_)
            | IndexError::Hex(_)
            | IndexError::StatePruningMisconfigured { .. }
            | IndexError::CodecError(_)
            | IndexError::MetadataError(_)
            | IndexError::Json(_)
            | IndexError::Io(_)
            | IndexError::TomlSer(_)
            | IndexError::Internal(_) => false,
        }
    }
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
