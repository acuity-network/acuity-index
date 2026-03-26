//! TOML chain configuration parsing.

use serde::Deserialize;
use std::collections::HashMap;

/// Key type identifiers used in TOML configs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyTypeName {
    AccountId,
    AccountIndex,
    AuctionIndex,
    BountyIndex,
    CandidateHash,
    EraIndex,
    MessageId,
    ParaId,
    PoolId,
    PreimageHash,
    ProposalHash,
    ProposalIndex,
    RefIndex,
    RegistrarIndex,
    SessionIndex,
    SpendIndex,
    TipHash,
}

/// A single field→key mapping within an event.
#[derive(Debug, Clone, Deserialize)]
pub struct ParamConfig {
    /// Field name (string) or positional index (stringified number, e.g. "0").
    pub field: String,
    pub key: KeyTypeName,
}

/// Configuration for a single event variant.
#[derive(Debug, Clone, Deserialize)]
pub struct EventConfig {
    pub name: String,
    #[serde(default)]
    pub params: Vec<ParamConfig>,
}

/// Configuration for a single pallet.
#[derive(Debug, Clone, Deserialize)]
pub struct PalletConfig {
    pub name: String,
    /// If true, use built-in Polkadot SDK indexing rules for this pallet.
    #[serde(default)]
    pub sdk: bool,
    #[serde(default)]
    pub events: Vec<EventConfig>,
}

/// Top-level chain configuration loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub name: String,
    pub genesis_hash: String,
    pub default_url: String,
    pub versions: Vec<u32>,
    #[serde(default)]
    pub pallets: Vec<PalletConfig>,
}

impl ChainConfig {
    /// Build a fast lookup: pallet_name → event_name → Vec<ParamConfig>.
    pub fn build_custom_index(&self) -> HashMap<String, HashMap<String, Vec<ParamConfig>>> {
        let mut map: HashMap<String, HashMap<String, Vec<ParamConfig>>> = HashMap::new();
        for pallet in &self.pallets {
            if pallet.sdk {
                continue;
            }
            let event_map = map.entry(pallet.name.clone()).or_default();
            for event in &pallet.events {
                event_map.insert(event.name.clone(), event.params.clone());
            }
        }
        map
    }

    /// Returns the set of pallet names that use built-in SDK indexing.
    pub fn sdk_pallets(&self) -> std::collections::HashSet<String> {
        self.pallets
            .iter()
            .filter(|p| p.sdk)
            .map(|p| p.name.clone())
            .collect()
    }

    pub fn genesis_hash_bytes(&self) -> Result<[u8; 32], hex::FromHexError> {
        let bytes = hex::decode(&self.genesis_hash)?;
        bytes
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)
    }
}

pub const POLKADOT_TOML: &str = include_str!("../chains/polkadot.toml");
pub const KUSAMA_TOML: &str = include_str!("../chains/kusama.toml");
pub const WESTEND_TOML: &str = include_str!("../chains/westend.toml");
pub const PASEO_TOML: &str = include_str!("../chains/paseo.toml");
