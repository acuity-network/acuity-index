//! TOML chain configuration parsing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn is_false(v: &bool) -> bool {
    !*v
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
    Scalar {
        name: String,
        kind: ScalarKind,
    },
    Composite {
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
        keys: &HashMap<String, CustomKeyConfig>,
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

        let key = match keys.get(&self.key) {
            Some(CustomKeyConfig::Scalar(kind)) => {
                if fields.len() != 1 {
                    return Err(format!(
                        "scalar key '{}' for field(s) {:?} requires exactly one source field",
                        self.key, fields
                    ));
                }
                ParamKey::Scalar {
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
                ParamKey::Composite {
                    name: self.key.clone(),
                    fields: config.fields.clone(),
                }
            }
            None => {
                return Err(format!(
                    "unknown key '{}' for field(s) {:?} (define it in keys)",
                    self.key, fields
                ));
            }
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
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub keys: HashMap<String, CustomKeyConfig>,
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
                    params.push(param.resolve(&self.keys)?);
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
    fn validate_returns_ok_for_valid_custom_config() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            keys: HashMap::from([(
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
            keys: HashMap::new(),
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
            keys: HashMap::new(),
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
            keys: HashMap::new(),
            pallets: vec![],
        };

        assert_eq!(
            spec.validate().unwrap_err(),
            "spec_change_blocks must be strictly increasing with no duplicates"
        );
    }

}
