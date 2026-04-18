#[cfg(test)]
mod config_tests {
    use crate::config::*;
    use std::collections::HashSet;

    fn load_polkadot() -> IndexSpec {
        toml::from_str(POLKADOT_TOML).unwrap()
    }

    fn load_kusama() -> IndexSpec {
        toml::from_str(KUSAMA_TOML).unwrap()
    }

    fn load_westend() -> IndexSpec {
        toml::from_str(WESTEND_TOML).unwrap()
    }

    fn load_paseo() -> IndexSpec {
        toml::from_str(PASEO_TOML).unwrap()
    }

    fn pallet<'a>(cfg: &'a IndexSpec, name: &str) -> &'a PalletConfig {
        cfg.pallets
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("missing pallet {name}"))
    }

    fn has_events(cfg: &IndexSpec, pallet_name: &str, required: &[&str]) {
        let pallet = pallet(cfg, pallet_name);
        let names: HashSet<&str> = pallet.events.iter().map(|e| e.name.as_str()).collect();
        for event_name in required {
            assert!(
                names.contains(event_name),
                "missing event {pallet_name}::{event_name}"
            );
        }
    }

    #[test]
    fn parse_all_builtin_configs() {
        let dot = load_polkadot();
        assert_eq!(dot.name, "polkadot");
        assert!(!dot.pallets.is_empty());

        let ksm = load_kusama();
        assert_eq!(ksm.name, "kusama");

        let wnd = load_westend();
        assert_eq!(wnd.name, "westend");

        let pas = load_paseo();
        assert_eq!(pas.name, "paseo");
    }

    #[test]
    fn genesis_hash_bytes_valid() {
        let cfg = load_polkadot();
        let bytes = cfg.genesis_hash_bytes().unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            hex::encode(bytes),
            "91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3"
        );
    }

    #[test]
    fn genesis_hash_bytes_invalid() {
        let cfg = IndexSpec {
            name: "bad".into(),
            genesis_hash: "zzzz".into(),
            default_url: "wss://x".into(),
            versions: vec![0],
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
            versions: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: Default::default(),
            pallets: vec![],
        };
        assert!(cfg.genesis_hash_bytes().is_err());
    }

    #[test]
    fn sdk_pallets_returns_only_sdk_flagged() {
        assert!(crate::pallets::is_supported_sdk_pallet("System"));
        assert!(crate::pallets::is_supported_sdk_pallet("Balances"));
        assert!(crate::pallets::is_supported_sdk_pallet("Staking"));
        assert!(crate::pallets::is_supported_sdk_pallet("StakingAhClient"));
        assert!(crate::pallets::is_supported_sdk_pallet("XcmPallet"));
        assert!(!crate::pallets::is_supported_sdk_pallet("Claims"));
        assert!(!crate::pallets::is_supported_sdk_pallet("RcMigrator"));
    }

    #[test]
    fn supported_sdk_pallets_include_stable2603_umbrella_exports() {
        for pallet_name in [
            "Alliance",
            "AssetConversion",
            "Assets",
            "AssignedSlots",
            "AuthorityDiscovery",
            "Auctions",
            "BagsList",
            "Beefy",
            "Broker",
            "Collective",
            "Contracts",
            "Coretime",
            "CoreFellowship",
            "Democracy",
            "ElectionProviderMultiBlock",
            "ElectionsPhragmen",
            "Grandpa",
            "Hrmp",
            "ImOnline",
            "Lottery",
            "Membership",
            "MessageQueue",
            "Migrations",
            "Mixnet",
            "Mmr",
            "Nfts",
            "Oracle",
            "OnDemand",
            "OnDemandAssignmentProvider",
            "ParaInclusion",
            "Paras",
            "ParasDisputes",
            "Parameters",
            "People",
            "RankedCollective",
            "Registrar",
            "Salary",
            "Scheduler",
            "Slots",
            "StakingAhClient",
            "Society",
            "StakingAsync",
            "Statement",
            "Timestamp",
            "Tips",
            "TransactionStorage",
            "Uniques",
            "Utility",
            "Whitelist",
            "Xcm",
            "XcmPallet",
        ] {
            assert!(
                crate::pallets::is_supported_sdk_pallet(pallet_name),
                "missing supported sdk pallet {pallet_name}"
            );
        }
    }

    #[test]
    fn build_custom_index_excludes_sdk() {
        let cfg: IndexSpec = toml::from_str(
            r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

[custom_keys]
amount = "u128"

[[pallets]]
name = "System"
sdk = true

[[pallets]]
name = "StakingAhClient"
sdk = true

[[pallets]]
name = "XcmPallet"
sdk = true

[[pallets]]
name = "Claims"
events = [
  { name = "Claimed", params = [
    { field = "who", key = "account_id" },
    { field = "amount", key = "amount" },
  ]},
]
"#,
        )
        .unwrap();
        let idx = cfg.build_custom_index().unwrap();
        assert!(!idx.contains_key("System"));
        assert!(!idx.contains_key("StakingAhClient"));
        assert!(!idx.contains_key("XcmPallet"));
        assert!(idx.contains_key("Claims"));
    }

    #[test]
    fn build_custom_index_event_params() {
        let cfg = load_polkadot();
        let idx = cfg.build_custom_index().unwrap();

        let claims = idx.get("Claims").unwrap();
        let claimed_params = claims.get("Claimed").unwrap();
        assert_eq!(claimed_params.len(), 2);
        assert_eq!(claimed_params[0].fields, vec!["who".to_owned()]);
        assert_eq!(
            claimed_params[0].key,
            ParamKey::BuiltIn(KeyTypeName::AccountId)
        );
        assert!(!claimed_params[0].multi);
        assert_eq!(
            claimed_params[1].key,
            ParamKey::Custom {
                name: "amount".into(),
                kind: ScalarKind::U128,
            }
        );
    }

    #[test]
    fn custom_toml_round_trip() {
        let toml_str = r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "wss://test:443"
versions = [0]

        [[pallets]]
        name = "Foo"
        sdk = true

        [[pallets]]
        name = "Bar"
        events = [
          { name = "Baz", params = [
            { field = "x", key = "pool_id" },
          ]},
        ]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.name, "test");
        assert_eq!(cfg.sdk_pallets().len(), 1);
        assert!(cfg.sdk_pallets().contains("Foo"));
        let idx = cfg.build_custom_index().unwrap();
        let bar = idx.get("Bar").unwrap();
        let baz = bar.get("Baz").unwrap();
        assert_eq!(baz[0].key, ParamKey::BuiltIn(KeyTypeName::PoolId));
        assert!(!baz[0].multi);
    }

    #[test]
    fn custom_toml_supports_generic_scalar_keys() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

[custom_keys]
item_id = "bytes32"
revision_id = "u32"

        [[pallets]]
        name = "Content"
        events = [
          { name = "PublishRevision", params = [
            { field = "item_id", key = "item_id" },
            { field = "revision_id", key = "revision_id" },
            { field = "links", key = "item_id", multi = true },
            { field = "mentions", key = "account_id", multi = true },
          ]},
        ]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        let idx = cfg.build_custom_index().unwrap();
        let content = idx.get("Content").unwrap();
        let publish_revision = content.get("PublishRevision").unwrap();
        assert_eq!(
            publish_revision[0].key,
            ParamKey::Custom {
                name: "item_id".into(),
                kind: ScalarKind::Bytes32,
            }
        );
        assert_eq!(
            publish_revision[1].key,
            ParamKey::Custom {
                name: "revision_id".into(),
                kind: ScalarKind::U32,
            }
        );
        assert!(!publish_revision[0].multi);
        assert!(!publish_revision[1].multi);
        assert_eq!(
            publish_revision[2].key,
            ParamKey::Custom {
                name: "item_id".into(),
                kind: ScalarKind::Bytes32,
            }
        );
        assert!(publish_revision[2].multi);
        assert_eq!(
            publish_revision[3].key,
            ParamKey::BuiltIn(KeyTypeName::AccountId)
        );
        assert!(publish_revision[3].multi);
    }

    #[test]
    fn custom_toml_supports_composite_keys() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

[custom_keys]
item_id = "bytes32"
revision_id = "u32"
item_revision = { fields = ["bytes32", "u32"] }

        [[pallets]]
        name = "ContentReactions"
        events = [
          { name = "SetReactions", params = [
            { fields = ["item_id", "revision_id"], key = "item_revision" },
            { field = "reactor", key = "account_id" },
          ]},
        ]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        let idx = cfg.build_custom_index().unwrap();
        let reactions = idx.get("ContentReactions").unwrap();
        let params = reactions.get("SetReactions").unwrap();

        assert_eq!(
            cfg.custom_keys.get("item_revision"),
            Some(&CustomKeyConfig::Composite(CompositeKeyConfig {
                fields: vec![ScalarKind::Bytes32, ScalarKind::U32],
            }))
        );
        assert_eq!(
            params[0].key,
            ParamKey::CompositeCustom {
                name: "item_revision".into(),
                fields: vec![ScalarKind::Bytes32, ScalarKind::U32],
            }
        );
        assert_eq!(
            params[0].fields,
            vec!["item_id".to_owned(), "revision_id".to_owned()]
        );
    }

    #[test]
    fn custom_toml_rejects_composite_key_with_wrong_number_of_fields() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

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
        assert!(cfg.build_custom_index().is_err());
    }

    #[test]
    fn custom_toml_rejects_field_and_fields_together() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

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
        assert!(cfg.build_custom_index().is_err());
    }

    #[test]
    fn custom_toml_rejects_unknown_key_without_definition() {
        let toml_str = r#"
name = "acuity"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

        [[pallets]]
        name = "Content"
        events = [
          { name = "PublishRevision", params = [
            { field = "item_id", key = "item_id" },
          ]},
        ]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(cfg.build_custom_index().is_err());
    }

    #[test]
    fn sdk_pallets_match_legacy_chain_modules() {
        let dot = load_polkadot();
        assert!(pallet(&dot, "StateTrieMigration").sdk);

        let ksm = load_kusama();
        assert!(pallet(&ksm, "DelegatedStaking").sdk);
        assert!(pallet(&ksm, "Recovery").sdk);

        let wnd = load_westend();
        for name in [
            "Preimage",
            "Identity",
            "ElectionProviderMultiPhase",
            "VoterList",
            "Sudo",
            "Treasury",
            "Recovery",
            "DelegatedStaking",
        ] {
            assert!(pallet(&wnd, name).sdk, "{name} should be sdk");
        }

        let pas = load_paseo();
        for name in [
            "Preimage",
            "Bounties",
            "ChildBounties",
            "Sudo",
            "Treasury",
            "ElectionProviderMultiPhase",
            "VoterList",
            "DelegatedStaking",
            "StateTrieMigration",
        ] {
            assert!(pallet(&pas, name).sdk, "{name} should be sdk");
        }
    }

    #[test]
    fn custom_pallet_events_match_legacy_chain_modules() {
        let ksm = load_kusama();
        assert!(pallet(&ksm, "Paras").sdk);
        assert!(pallet(&ksm, "Hrmp").sdk);
        assert!(pallet(&ksm, "OnDemandAssignmentProvider").sdk);
        assert!(pallet(&ksm, "XcmPallet").sdk);
        assert!(pallet(&ksm, "StakingAhClient").sdk);

        let wnd = load_westend();
        assert!(pallet(&wnd, "AssignedSlots").sdk);
        assert!(pallet(&wnd, "OnDemandAssignmentProvider").sdk);
        assert!(pallet(&wnd, "XcmPallet").sdk);
        assert!(pallet(&wnd, "StakingAhClient").sdk);

        let pas = load_paseo();
        has_events(&pas, "Claims", &["Claimed"]);
        assert!(pallet(&pas, "ParasDisputes").sdk);
        assert!(pallet(&pas, "XcmPallet").sdk);
        assert!(pallet(&pas, "StakingAhClient").sdk);
    }

    #[test]
    fn paseo_defaults_match_legacy_values() {
        let pas = load_paseo();
        assert_eq!(
            pas.genesis_hash,
            "77afd6190f1554ad45fd0d31aee62aacc33c6db0ea801129acb813f913e0764f"
        );
        assert_eq!(pas.default_url, "wss://paseo.ibp.network:443");
    }

    #[test]
    fn index_spec_parses_index_variant_and_store_events() {
        let toml_str = r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]
index_variant = true
store_events = true
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(cfg.index_variant);
        assert!(cfg.store_events);
    }

    #[test]
    fn index_variant_and_store_events_default_to_false() {
        let toml_str = r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]
"#;
        let cfg: IndexSpec = toml::from_str(toml_str).unwrap();
        assert!(!cfg.index_variant);
        assert!(!cfg.store_events);
    }

    #[test]
    fn options_config_parses_from_toml() {
        let toml_str = r#"
url = "ws://custom:9999"
db_path = "/data/acuity/db"
db_mode = "high_throughput"
db_cache_capacity = "2 GiB"
queue_depth = 4
finalized = true
port = 9999
"#;
        let opts: OptionsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.url.as_deref(), Some("ws://custom:9999"));
        assert_eq!(opts.db_path.as_deref(), Some("/data/acuity/db"));
        assert_eq!(opts.db_mode.as_deref(), Some("high_throughput"));
        assert_eq!(opts.db_cache_capacity.as_deref(), Some("2 GiB"));
        assert_eq!(opts.queue_depth, Some(4));
        assert_eq!(opts.finalized, Some(true));
        assert_eq!(opts.port, Some(9999));
    }

    #[test]
    fn options_config_defaults_to_none() {
        let toml_str = "";
        let opts: OptionsConfig = toml::from_str(toml_str).unwrap();
        assert!(opts.url.is_none());
        assert!(opts.db_path.is_none());
        assert!(opts.db_mode.is_none());
        assert!(opts.db_cache_capacity.is_none());
        assert!(opts.queue_depth.is_none());
        assert!(opts.finalized.is_none());
        assert!(opts.port.is_none());
    }

    #[test]
    fn options_config_partial_fields() {
        let toml_str = r#"
finalized = true
port = 4000
"#;
        let opts: OptionsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.finalized, Some(true));
        assert_eq!(opts.port, Some(4000));
        assert!(opts.url.is_none());
        assert!(opts.db_path.is_none());
        assert!(opts.db_mode.is_none());
        assert!(opts.db_cache_capacity.is_none());
        assert!(opts.queue_depth.is_none());
    }
}
