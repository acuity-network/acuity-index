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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CustomKeyConfig {
    Scalar(ScalarKind),
    Composite(CompositeKeyConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct CompositeKeyConfig {
    pub fields: Vec<ScalarKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamKey {
    BuiltIn(KeyTypeName),
    Custom {
        name: String,
        kind: ScalarKind,
    },
    CompositeCustom {
        name: String,
        fields: Vec<ScalarKind>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedParamConfig {
    pub fields: Vec<String>,
    pub key: ParamKey,
    pub multi: bool,
}

pub type CustomIndex = HashMap<String, HashMap<String, Vec<ResolvedParamConfig>>>;

/// A single field→key mapping within an event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParamConfig {
    /// Field name (string) or positional index (stringified number, e.g. "0").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    pub key: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub multi: bool,
}

impl ParamConfig {
    pub fn resolve(
        &self,
        custom_keys: &HashMap<String, CustomKeyConfig>,
    ) -> Result<ResolvedParamConfig, String> {
        let fields = match (&self.field, self.fields.is_empty()) {
            (Some(field), true) => vec![field.clone()],
            (None, false) => self.fields.clone(),
            (Some(_), false) => {
                return Err(format!(
                    "param for key '{}' must specify either field or fields, not both",
                    self.key
                ));
            }
            (None, true) => {
                return Err(format!(
                    "param for key '{}' must specify field or fields",
                    self.key
                ));
            }
        };

        let key = match KeyTypeName::parse(&self.key) {
            Some(key) => {
                if fields.len() != 1 {
                    return Err(format!(
                        "built-in key '{}' for field(s) {:?} requires exactly one source field",
                        self.key, fields
                    ));
                }
                ParamKey::BuiltIn(key)
            }
            None => match custom_keys.get(&self.key) {
                Some(CustomKeyConfig::Scalar(kind)) => {
                    if fields.len() != 1 {
                        return Err(format!(
                            "scalar key '{}' for field(s) {:?} requires exactly one source field",
                            self.key, fields
                        ));
                    }
                    ParamKey::Custom {
                        name: self.key.clone(),
                        kind: kind.clone(),
                    }
                }
                Some(CustomKeyConfig::Composite(config)) => {
                    if self.multi {
                        return Err(format!(
                            "composite key '{}' does not support multi=true",
                            self.key
                        ));
                    }
                    if fields.len() != config.fields.len() {
                        return Err(format!(
                            "composite key '{}' expects {} fields, got {}",
                            self.key,
                            config.fields.len(),
                            fields.len()
                        ));
                    }
                    ParamKey::CompositeCustom {
                        name: self.key.clone(),
                        fields: config.fields.clone(),
                    }
                }
                None => {
                    return Err(format!(
                        "unknown key '{}' for field(s) {:?} (define it in custom_keys)",
                        self.key, fields
                    ));
                }
            },
        };

        Ok(ResolvedParamConfig {
            fields,
            key,
            multi: self.multi,
        })
    }
}

/// Configuration for a single event variant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EventConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamConfig>,
}

/// Configuration for a single pallet.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PalletConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventConfig>,
}

/// Runtime options loaded from a separate options TOML file.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OptionsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_cache_capacity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_subscriptions: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subscriptions_per_connection: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_control_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events_limit: Option<usize>,
}

/// Index specification loaded from a TOML file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexSpec {
    pub name: String,
    pub genesis_hash: String,
    pub default_url: String,
    /// Block heights where a new index-spec revision starts.
    ///
    /// The first entry must be `0`, and each later entry must be strictly
    /// greater than the previous one. A new block number added in the past
    /// causes spans at or after that boundary to be re-indexed.
    pub spec_change_blocks: Vec<u32>,
    #[serde(default)]
    pub index_variant: bool,
    #[serde(default)]
    pub store_events: bool,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_keys: HashMap<String, CustomKeyConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pallets: Vec<PalletConfig>,
}

impl IndexSpec {
    /// Build a fast lookup: pallet_name → event_name → Vec<ParamConfig>.
    pub fn build_event_index(&self) -> Result<CustomIndex, String> {
        let mut map: CustomIndex = HashMap::new();
        for pallet in &self.pallets {
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
        if self.spec_change_blocks.is_empty() {
            return Err("spec_change_blocks must not be empty".to_owned());
        }
        if self.spec_change_blocks[0] != 0 {
            return Err("spec_change_blocks must start at block 0".to_owned());
        }
        for window in self.spec_change_blocks.windows(2) {
            if window[0] >= window[1] {
                return Err(
                    "spec_change_blocks must be strictly increasing with no duplicates".to_owned(),
                );
            }
        }
        let _ = self.build_event_index()?;
        Ok(())
    }

    pub fn genesis_hash_bytes(&self) -> Result<[u8; 32], hex::FromHexError> {
        let bytes = hex::decode(&self.genesis_hash)?;
        bytes
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)
    }
}

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
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: HashMap::from([(
                "item_id".into(),
                CustomKeyConfig::Scalar(ScalarKind::Bytes32),
            )]),
            pallets: vec![PalletConfig {
                name: "Content".into(),
                events: vec![EventConfig {
                    name: "Published".into(),
                    params: vec![ParamConfig {
                        field: Some("item_id".into()),
                        fields: vec![],
                        key: "item_id".into(),
                        multi: false,
                    }],
                }],
            }],
        };

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_spec_change_blocks() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![],
            index_variant: false,
            store_events: false,
            custom_keys: HashMap::new(),
            pallets: vec![],
        };

        assert_eq!(
            spec.validate().unwrap_err(),
            "spec_change_blocks must not be empty"
        );
    }

    #[test]
    fn validate_rejects_spec_change_blocks_not_starting_at_zero() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![10],
            index_variant: false,
            store_events: false,
            custom_keys: HashMap::new(),
            pallets: vec![],
        };

        assert_eq!(
            spec.validate().unwrap_err(),
            "spec_change_blocks must start at block 0"
        );
    }

    #[test]
    fn validate_rejects_unsorted_spec_change_blocks() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0, 10, 10],
            index_variant: false,
            store_events: false,
            custom_keys: HashMap::new(),
            pallets: vec![],
        };

        assert_eq!(
            spec.validate().unwrap_err(),
            "spec_change_blocks must be strictly increasing with no duplicates"
        );
    }

    #[test]
    fn serializing_pallet_omits_sdk_flag() {
        let pallet = PalletConfig {
            name: "Content".into(),
            events: vec![],
        };

        let toml = toml::to_string(&pallet).unwrap();

        assert!(!toml.contains("sdk"));
    }
}
