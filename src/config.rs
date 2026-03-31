//! TOML chain configuration parsing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn is_false(v: &bool) -> bool {
    !*v
}

/// Key type identifiers used in TOML configs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyTypeName {
    AccountId,
    AccountIndex,
    BountyIndex,
    EraIndex,
    MessageId,
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

impl KeyTypeName {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "account_id" => Self::AccountId,
            "account_index" => Self::AccountIndex,
            "bounty_index" => Self::BountyIndex,
            "era_index" => Self::EraIndex,
            "message_id" => Self::MessageId,
            "pool_id" => Self::PoolId,
            "preimage_hash" => Self::PreimageHash,
            "proposal_hash" => Self::ProposalHash,
            "proposal_index" => Self::ProposalIndex,
            "ref_index" => Self::RefIndex,
            "registrar_index" => Self::RegistrarIndex,
            "session_index" => Self::SessionIndex,
            "spend_index" => Self::SpendIndex,
            "tip_hash" => Self::TipHash,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarKind {
    Bytes32,
    U32,
    U64,
    U128,
    String,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamKey {
    BuiltIn(KeyTypeName),
    Custom { name: String, kind: ScalarKind },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedParamConfig {
    pub field: String,
    pub key: ParamKey,
    pub multi: bool,
}

pub type CustomIndex = HashMap<String, HashMap<String, Vec<ResolvedParamConfig>>>;

/// A single field→key mapping within an event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParamConfig {
    /// Field name (string) or positional index (stringified number, e.g. "0").
    pub field: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub multi: bool,
}

impl ParamConfig {
    pub fn resolve(
        &self,
        custom_keys: &HashMap<String, ScalarKind>,
    ) -> Result<ResolvedParamConfig, String> {
        let key = match KeyTypeName::parse(&self.key) {
            Some(key) => ParamKey::BuiltIn(key),
            None => match custom_keys.get(&self.key) {
                Some(kind) => ParamKey::Custom {
                    name: self.key.clone(),
                    kind: kind.clone(),
                },
                None => {
                    return Err(format!(
                        "unknown key '{}' for field '{}' (define it in custom_keys)",
                        self.key, self.field
                    ));
                }
            },
        };

        Ok(ResolvedParamConfig {
            field: self.field.clone(),
            key,
            multi: self.multi,
        })
    }
}

/// Configuration for a single event variant.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamConfig>,
}

/// Configuration for a single pallet.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PalletConfig {
    pub name: String,
    /// If true, use built-in Polkadot SDK indexing rules for this pallet.
    #[serde(default, skip_serializing_if = "is_false")]
    pub sdk: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventConfig>,
}

/// Top-level chain configuration loaded from a TOML file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChainConfig {
    pub name: String,
    pub genesis_hash: String,
    pub default_url: String,
    pub versions: Vec<u32>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_keys: HashMap<String, ScalarKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pallets: Vec<PalletConfig>,
}

impl ChainConfig {
    /// Build a fast lookup: pallet_name → event_name → Vec<ParamConfig>.
    pub fn build_custom_index(&self) -> Result<CustomIndex, String> {
        let mut map: CustomIndex = HashMap::new();
        for pallet in &self.pallets {
            if pallet.sdk {
                continue;
            }
            let event_map = map.entry(pallet.name.clone()).or_default();
            for event in &pallet.events {
                let mut params = Vec::with_capacity(event.params.len());
                for param in &event.params {
                    params.push(param.resolve(&self.custom_keys)?);
                }
                event_map.insert(event.name.clone(), params);
            }
        }
        Ok(map)
    }

    pub fn validate(&self) -> Result<(), String> {
        let _ = self.build_custom_index()?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_type_name_parse_covers_all_variants() {
        for (name, expected) in [
            ("account_id", KeyTypeName::AccountId),
            ("account_index", KeyTypeName::AccountIndex),
            ("bounty_index", KeyTypeName::BountyIndex),
            ("era_index", KeyTypeName::EraIndex),
            ("message_id", KeyTypeName::MessageId),
            ("pool_id", KeyTypeName::PoolId),
            ("preimage_hash", KeyTypeName::PreimageHash),
            ("proposal_hash", KeyTypeName::ProposalHash),
            ("proposal_index", KeyTypeName::ProposalIndex),
            ("ref_index", KeyTypeName::RefIndex),
            ("registrar_index", KeyTypeName::RegistrarIndex),
            ("session_index", KeyTypeName::SessionIndex),
            ("spend_index", KeyTypeName::SpendIndex),
            ("tip_hash", KeyTypeName::TipHash),
        ] {
            assert_eq!(KeyTypeName::parse(name), Some(expected));
        }
        assert_eq!(KeyTypeName::parse("unknown"), None);
    }

    #[test]
    fn validate_returns_ok_for_valid_custom_config() {
        let config = ChainConfig {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            versions: vec![0],
            custom_keys: HashMap::from([("item_id".into(), ScalarKind::Bytes32)]),
            pallets: vec![PalletConfig {
                name: "Content".into(),
                sdk: false,
                events: vec![EventConfig {
                    name: "Published".into(),
                    params: vec![ParamConfig {
                        field: "item_id".into(),
                        key: "item_id".into(),
                        multi: false,
                    }],
                }],
            }],
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn serializing_non_sdk_pallet_omits_sdk_flag() {
        let pallet = PalletConfig {
            name: "Content".into(),
            sdk: false,
            events: vec![],
        };

        let toml = toml::to_string(&pallet).unwrap();

        assert!(!toml.contains("sdk"));
    }
}
