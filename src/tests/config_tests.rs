#[cfg(test)]
mod config_tests {
    use crate::config::*;

    fn explicit_spec() -> IndexSpec {
        toml::from_str(
            r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

[custom_keys]
amount = "u128"
item_id = "bytes32"
item_revision = { fields = ["bytes32", "u32"] }

[[pallets]]
name = "System"
events = [
  { name = "NewAccount", params = [
    { field = "account", key = "account_id" },
  ]},
]

[[pallets]]
name = "Claims"
events = [
  { name = "Claimed", params = [
    { field = "who", key = "account_id" },
    { field = "amount", key = "amount" },
  ]},
]

[[pallets]]
name = "ContentReactions"
events = [
  { name = "SetReactions", params = [
    { fields = ["item_id", "revision_id"], key = "item_revision" },
  ]},
]
"#,
        )
        .unwrap()
    }

    #[test]
    fn genesis_hash_bytes_valid() {
        let bytes = explicit_spec().genesis_hash_bytes().unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(hex::encode(bytes), "00".repeat(31) + "01");
    }

    #[test]
    fn genesis_hash_bytes_invalid() {
        let cfg = IndexSpec {
            name: "bad".into(),
            genesis_hash: "zzzz".into(),
            default_url: "wss://x".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: Default::default(),
            pallets: vec![],
        };
        assert!(cfg.genesis_hash_bytes().is_err());
    }

    #[test]
    fn genesis_hash_bytes_wrong_length() {
        let cfg = IndexSpec {
            name: "bad".into(),
            genesis_hash: "aabb".into(),
            default_url: "wss://x".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: Default::default(),
            pallets: vec![],
        };
        assert!(cfg.genesis_hash_bytes().is_err());
    }

    #[test]
    fn build_event_index_includes_all_explicit_pallets() {
        let cfg = explicit_spec();
        let idx = cfg.build_event_index().unwrap();

        assert!(idx.contains_key("System"));
        assert!(idx.contains_key("Claims"));
        assert!(idx.contains_key("ContentReactions"));
    }

    #[test]
    fn build_event_index_resolves_event_params() {
        let cfg = explicit_spec();
        let idx = cfg.build_event_index().unwrap();

        let claims = idx.get("Claims").unwrap();
        let claimed_params = claims.get("Claimed").unwrap();
        assert_eq!(claimed_params.len(), 2);
        assert_eq!(claimed_params[0].fields, vec!["who".to_owned()]);
        assert_eq!(
            claimed_params[0].key,
            ParamKey::BuiltIn(KeyTypeName::AccountId)
        );
        assert_eq!(
            claimed_params[1].key,
            ParamKey::Custom {
                name: "amount".into(),
                kind: ScalarKind::U128,
            }
        );
    }

    #[test]
    fn build_event_index_resolves_composite_keys() {
        let cfg = explicit_spec();
        let idx = cfg.build_event_index().unwrap();
        let reactions = idx.get("ContentReactions").unwrap();
        let params = reactions.get("SetReactions").unwrap();

        assert_eq!(
            params[0].key,
            ParamKey::CompositeCustom {
                name: "item_revision".into(),
                fields: vec![ScalarKind::Bytes32, ScalarKind::U32],
            }
        );
    }

    #[test]
    fn custom_toml_round_trip_preserves_explicit_events() {
        let toml_str = r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "wss://test:443"
spec_change_blocks = [0]

[[pallets]]
name = "System"
events = [
  { name = "NewAccount", params = [
    { field = "account", key = "account_id" },
  ]},
]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        let idx = cfg.build_event_index().unwrap();
        assert!(idx.get("System").unwrap().contains_key("NewAccount"));
    }

    #[test]
    fn custom_toml_rejects_composite_key_with_wrong_number_of_fields() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

[custom_keys]
item_revision = { fields = ["bytes32", "u32"] }

[[pallets]]
name = "ContentReactions"
events = [
  { name = "SetReactions", params = [
    { field = "item_id", key = "item_revision" },
  ]},
]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(cfg.build_event_index().is_err());
    }

    #[test]
    fn custom_toml_rejects_field_and_fields_together() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

[custom_keys]
item_id = "bytes32"

[[pallets]]
name = "Content"
events = [
  { name = "PublishItem", params = [
    { field = "item_id", fields = ["item_id"], key = "item_id" },
  ]},
]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(cfg.build_event_index().is_err());
    }

    #[test]
    fn custom_toml_rejects_unknown_key_without_definition() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

[[pallets]]
name = "Content"
events = [
  { name = "PublishRevision", params = [
    { field = "item_id", key = "item_id" },
  ]},
]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(cfg.build_event_index().is_err());
    }

    #[test]
    fn index_spec_parses_spec_change_blocks() {
        let toml_str = r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0, 100, 250]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.spec_change_blocks, vec![0, 100, 250]);
    }
}
