#[cfg(test)]
mod config_tests {
    use crate::config::*;

    fn load_polkadot() -> ChainConfig {
        toml::from_str(POLKADOT_TOML).unwrap()
    }

    fn load_kusama() -> ChainConfig {
        toml::from_str(KUSAMA_TOML).unwrap()
    }

    fn load_westend() -> ChainConfig {
        toml::from_str(WESTEND_TOML).unwrap()
    }

    fn load_paseo() -> ChainConfig {
        toml::from_str(PASEO_TOML).unwrap()
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
        let cfg = ChainConfig {
            name: "bad".into(),
            genesis_hash: "zzzz".into(),
            default_url: "wss://x".into(),
            versions: vec![0],
            pallets: vec![],
        };
        assert!(cfg.genesis_hash_bytes().is_err());
    }

    #[test]
    fn genesis_hash_bytes_wrong_length() {
        let cfg = ChainConfig {
            name: "bad".into(),
            genesis_hash: "aabb".into(),
            default_url: "wss://x".into(),
            versions: vec![0],
            pallets: vec![],
        };
        assert!(cfg.genesis_hash_bytes().is_err());
    }

    #[test]
    fn sdk_pallets_returns_only_sdk_flagged() {
        let cfg = load_polkadot();
        let sdk = cfg.sdk_pallets();
        assert!(sdk.contains("System"));
        assert!(sdk.contains("Balances"));
        assert!(sdk.contains("Staking"));
        // Custom pallets should NOT be in the SDK set.
        assert!(!sdk.contains("Claims"));
        assert!(!sdk.contains("Paras"));
        assert!(!sdk.contains("Crowdloan"));
    }

    #[test]
    fn build_custom_index_excludes_sdk() {
        let cfg = load_polkadot();
        let idx = cfg.build_custom_index();
        // SDK pallets must not appear in the custom index.
        assert!(!idx.contains_key("System"));
        assert!(!idx.contains_key("Balances"));
        // Custom pallets should appear.
        assert!(idx.contains_key("Claims"));
        assert!(idx.contains_key("Paras"));
    }

    #[test]
    fn build_custom_index_event_params() {
        let cfg = load_polkadot();
        let idx = cfg.build_custom_index();

        let claims = idx.get("Claims").unwrap();
        let claimed_params = claims.get("Claimed").unwrap();
        assert_eq!(claimed_params.len(), 1);
        assert_eq!(claimed_params[0].field, "who");
        assert_eq!(claimed_params[0].key, KeyTypeName::AccountId);
    }

    #[test]
    fn build_custom_index_multi_param_event() {
        let cfg = load_polkadot();
        let idx = cfg.build_custom_index();

        let registrar = idx.get("Registrar").unwrap();
        let registered = registrar.get("Registered").unwrap();
        assert_eq!(registered.len(), 2);
        assert_eq!(registered[0].field, "para_id");
        assert_eq!(registered[0].key, KeyTypeName::ParaId);
        assert_eq!(registered[1].field, "manager");
        assert_eq!(registered[1].key, KeyTypeName::AccountId);
    }

    #[test]
    fn build_custom_index_positional_field() {
        let cfg = load_polkadot();
        let idx = cfg.build_custom_index();

        let paras = idx.get("Paras").unwrap();
        let code_updated = paras.get("CurrentCodeUpdated").unwrap();
        assert_eq!(code_updated.len(), 1);
        assert_eq!(code_updated[0].field, "0");
        assert_eq!(code_updated[0].key, KeyTypeName::ParaId);
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

[[pallets.events]]
name = "Baz"
[[pallets.events.params]]
field = "x"
key = "pool_id"
"#;
        let cfg: ChainConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.name, "test");
        assert_eq!(cfg.sdk_pallets().len(), 1);
        assert!(cfg.sdk_pallets().contains("Foo"));
        let idx = cfg.build_custom_index();
        let bar = idx.get("Bar").unwrap();
        let baz = bar.get("Baz").unwrap();
        assert_eq!(baz[0].key, KeyTypeName::PoolId);
    }
}


