// src/tests.rs

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

#[cfg(test)]
mod pallets_tests {
    use crate::pallets::*;
    use crate::shared::{Bytes32, Key};
    use scale_value::{Composite, Primitive, Value, ValueDef};

    fn u128_val(n: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(n)),
            context: (),
        }
    }

    fn bytes32_val(b: [u8; 32]) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(
                b.iter()
                    .map(|&byte| Value {
                        value: ValueDef::Primitive(Primitive::U128(byte as u128)),
                        context: (),
                    })
                    .collect(),
            )),
            context: (),
        }
    }

    fn named(fields: Vec<(&str, Value<()>)>) -> Composite<()> {
        Composite::Named(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    fn unnamed(fields: Vec<Value<()>>) -> Composite<()> {
        Composite::Unnamed(fields)
    }

    // ─── extract_u32 ──────────────────────────────────────────────────────

    #[test]
    fn extract_u32_from_u128() {
        let v = u128_val(42);
        assert_eq!(extract_u32(&v), Some(42));
    }

    #[test]
    fn extract_u32_from_i128() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::I128(99)),
            context: (),
        };
        assert_eq!(extract_u32(&v), Some(99));
    }

    #[test]
    fn extract_u32_overflow() {
        let v = u128_val(u128::from(u32::MAX) + 1);
        assert_eq!(extract_u32(&v), None);
    }

    #[test]
    fn extract_u32_negative_i128() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::I128(-1)),
            context: (),
        };
        assert_eq!(extract_u32(&v), None);
    }

    #[test]
    fn extract_u32_wrapped_unnamed() {
        let inner = u128_val(7);
        let v = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![inner])),
            context: (),
        };
        assert_eq!(extract_u32(&v), Some(7));
    }

    #[test]
    fn extract_u32_string_returns_none() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::String("hello".into())),
            context: (),
        };
        assert_eq!(extract_u32(&v), None);
    }

    // ─── extract_bytes32 ──────────────────────────────────────────────────

    #[test]
    fn extract_bytes32_valid() {
        let mut arr = [0u8; 32];
        arr[0] = 0xAA;
        arr[31] = 0xFF;
        let v = bytes32_val(arr);
        assert_eq!(extract_bytes32(&v), Some(arr));
    }

    #[test]
    fn extract_bytes32_wrong_length() {
        let v = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![u128_val(1), u128_val(2)])),
            context: (),
        };
        assert_eq!(extract_bytes32(&v), None);
    }

    #[test]
    fn extract_bytes32_byte_overflow() {
        let mut fields: Vec<Value<()>> = (0..32).map(|_| u128_val(0)).collect();
        fields[0] = u128_val(256); // overflow for u8
        let v = Value {
            value: ValueDef::Composite(Composite::Unnamed(fields)),
            context: (),
        };
        assert_eq!(extract_bytes32(&v), None);
    }

    #[test]
    fn extract_bytes32_wrapped_single_unnamed() {
        let mut arr = [0u8; 32];
        arr[5] = 0x42;
        let inner = bytes32_val(arr);
        let v = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![inner])),
            context: (),
        };
        assert_eq!(extract_bytes32(&v), Some(arr));
    }

    #[test]
    fn extract_bytes32_wrapped_single_named() {
        let mut arr = [0u8; 32];
        arr[10] = 0xBB;
        let inner = bytes32_val(arr);
        let v = Value {
            value: ValueDef::Composite(Composite::Named(vec![("inner".to_string(), inner)])),
            context: (),
        };
        assert_eq!(extract_bytes32(&v), Some(arr));
    }

    // ─── get_field ────────────────────────────────────────────────────────

    #[test]
    fn get_field_by_name() {
        let c = named(vec![("who", u128_val(10)), ("amount", u128_val(99))]);
        let v = get_field(&c, "amount").unwrap();
        assert_eq!(extract_u32(v), Some(99));
    }

    #[test]
    fn get_field_by_name_missing() {
        let c = named(vec![("who", u128_val(10))]);
        assert!(get_field(&c, "missing").is_none());
    }

    #[test]
    fn get_field_by_positional_index_unnamed() {
        let c = unnamed(vec![u128_val(100), u128_val(200)]);
        let v = get_field(&c, "1").unwrap();
        assert_eq!(extract_u32(v), Some(200));
    }

    #[test]
    fn get_field_by_positional_index_named() {
        let c = named(vec![("a", u128_val(11)), ("b", u128_val(22))]);
        let v = get_field(&c, "0").unwrap();
        assert_eq!(extract_u32(v), Some(11));
    }

    #[test]
    fn get_field_positional_out_of_bounds() {
        let c = unnamed(vec![u128_val(1)]);
        assert!(get_field(&c, "5").is_none());
    }

    // ─── SDK pallet: System ───────────────────────────────────────────────

    #[test]
    fn system_new_account() {
        let acct = [1u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_system("NewAccount", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    #[test]
    fn system_killed_account() {
        let acct = [2u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_system("KilledAccount", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    #[test]
    fn system_remarked() {
        let sender = [3u8; 32];
        let fields = named(vec![("sender", bytes32_val(sender))]);
        let keys = index_system("Remarked", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(sender))]);
    }

    #[test]
    fn system_unknown_event_returns_empty() {
        let fields = named(vec![]);
        let keys = index_system("SomeFutureEvent", &fields);
        assert!(keys.is_empty());
    }

    // ─── SDK pallet: Balances ─────────────────────────────────────────────

    #[test]
    fn balances_transfer() {
        let from = [10u8; 32];
        let to = [20u8; 32];
        let fields = named(vec![
            ("from", bytes32_val(from)),
            ("to", bytes32_val(to)),
            ("amount", u128_val(1_000_000)),
        ]);
        let keys = index_balances("Transfer", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::AccountId(Bytes32(from))));
        assert!(keys.contains(&Key::AccountId(Bytes32(to))));
    }

    #[test]
    fn balances_endowed() {
        let acct = [11u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_balances("Endowed", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    #[test]
    fn balances_reserved() {
        let who = [12u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_balances("Reserved", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(who))]);
    }

    #[test]
    fn balances_reserve_repatriated() {
        let from = [13u8; 32];
        let to = [14u8; 32];
        let fields = named(vec![("from", bytes32_val(from)), ("to", bytes32_val(to))]);
        let keys = index_balances("ReserveRepatriated", &fields);
        assert_eq!(keys.len(), 2);
    }

    // ─── SDK pallet: Staking ──────────────────────────────────────────────

    #[test]
    fn staking_era_paid() {
        let fields = unnamed(vec![u128_val(100)]);
        let keys = index_staking("EraPaid", &fields);
        assert_eq!(keys, vec![Key::EraIndex(100)]);
    }

    #[test]
    fn staking_bonded() {
        let stash = [15u8; 32];
        let fields = named(vec![("stash", bytes32_val(stash))]);
        let keys = index_staking("Bonded", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(stash))]);
    }

    #[test]
    fn staking_payout_started() {
        let validator = [16u8; 32];
        let fields = named(vec![
            ("era_index", u128_val(50)),
            ("validator_stash", bytes32_val(validator)),
        ]);
        let keys = index_staking("PayoutStarted", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::EraIndex(50)));
        assert!(keys.contains(&Key::AccountId(Bytes32(validator))));
    }

    #[test]
    fn staking_stakers_elected_empty() {
        let fields = named(vec![]);
        let keys = index_staking("StakersElected", &fields);
        assert!(keys.is_empty());
    }

    // ─── SDK pallet: Session ──────────────────────────────────────────────

    #[test]
    fn session_new_session() {
        let fields = named(vec![("session_index", u128_val(777))]);
        let keys = index_session("NewSession", &fields);
        assert_eq!(keys, vec![Key::SessionIndex(777)]);
    }

    // ─── SDK pallet: Indices ──────────────────────────────────────────────

    #[test]
    fn indices_index_assigned() {
        let who = [17u8; 32];
        let fields = named(vec![("who", bytes32_val(who)), ("index", u128_val(42))]);
        let keys = index_indices("IndexAssigned", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::AccountId(Bytes32(who))));
        assert!(keys.contains(&Key::AccountIndex(42)));
    }

    // ─── SDK pallet: Preimage ─────────────────────────────────────────────

    #[test]
    fn preimage_noted() {
        let hash = [0xABu8; 32];
        let fields = named(vec![("hash", bytes32_val(hash))]);
        let keys = index_preimage("Noted", &fields);
        assert_eq!(keys, vec![Key::PreimageHash(Bytes32(hash))]);
    }

    // ─── SDK pallet: Treasury ─────────────────────────────────────────────

    #[test]
    fn treasury_proposed() {
        let fields = named(vec![("proposal_index", u128_val(5))]);
        let keys = index_treasury("Proposed", &fields);
        assert_eq!(keys, vec![Key::ProposalIndex(5)]);
    }

    #[test]
    fn treasury_spend_approved() {
        let beneficiary = [18u8; 32];
        let fields = named(vec![
            ("proposal_index", u128_val(10)),
            ("beneficiary", bytes32_val(beneficiary)),
        ]);
        let keys = index_treasury("SpendApproved", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::ProposalIndex(10)));
        assert!(keys.contains(&Key::AccountId(Bytes32(beneficiary))));
    }

    #[test]
    fn treasury_paid() {
        let fields = named(vec![("index", u128_val(3))]);
        let keys = index_treasury("Paid", &fields);
        assert_eq!(keys, vec![Key::SpendIndex(3)]);
    }

    // ─── SDK pallet: Bounties ─────────────────────────────────────────────

    #[test]
    fn bounties_proposed() {
        let fields = named(vec![("index", u128_val(7))]);
        let keys = index_bounties("BountyProposed", &fields);
        assert_eq!(keys, vec![Key::BountyIndex(7)]);
    }

    // ─── SDK pallet: ChildBounties ────────────────────────────────────────

    #[test]
    fn child_bounties_added() {
        let fields = named(vec![("index", u128_val(4))]);
        let keys = index_child_bounties("Added", &fields);
        assert_eq!(keys, vec![Key::BountyIndex(4)]);
    }

    // ─── SDK pallet: Vesting ──────────────────────────────────────────────

    #[test]
    fn vesting_updated() {
        let acct = [19u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_vesting("VestingUpdated", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    // ─── SDK pallet: Proxy ────────────────────────────────────────────────

    #[test]
    fn proxy_added() {
        let delegator = [20u8; 32];
        let delegatee = [21u8; 32];
        let fields = named(vec![
            ("delegator", bytes32_val(delegator)),
            ("delegatee", bytes32_val(delegatee)),
        ]);
        let keys = index_proxy("ProxyAdded", &fields);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn proxy_pure_created() {
        let pure = [22u8; 32];
        let who = [23u8; 32];
        let fields = named(vec![("pure", bytes32_val(pure)), ("who", bytes32_val(who))]);
        let keys = index_proxy("PureCreated", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::AccountId(Bytes32(pure))));
        assert!(keys.contains(&Key::AccountId(Bytes32(who))));
    }

    // ─── SDK pallet: Multisig ─────────────────────────────────────────────

    #[test]
    fn multisig_new() {
        let approving = [24u8; 32];
        let multisig = [25u8; 32];
        let fields = named(vec![
            ("approving", bytes32_val(approving)),
            ("multisig", bytes32_val(multisig)),
        ]);
        let keys = index_multisig("NewMultisig", &fields);
        assert_eq!(keys.len(), 2);
    }

    // ─── SDK pallet: NominationPools ──────────────────────────────────────

    #[test]
    fn nomination_pools_created() {
        let depositor = [26u8; 32];
        let fields = named(vec![
            ("depositor", bytes32_val(depositor)),
            ("pool_id", u128_val(1)),
        ]);
        let keys = index_nomination_pools("Created", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::AccountId(Bytes32(depositor))));
        assert!(keys.contains(&Key::PoolId(1)));
    }

    #[test]
    fn nomination_pools_destroyed() {
        let fields = named(vec![("pool_id", u128_val(99))]);
        let keys = index_nomination_pools("Destroyed", &fields);
        assert_eq!(keys, vec![Key::PoolId(99)]);
    }

    // ─── SDK pallet: FastUnstake ──────────────────────────────────────────

    #[test]
    fn fast_unstake_unstaked() {
        let stash = [27u8; 32];
        let fields = named(vec![("stash", bytes32_val(stash))]);
        let keys = index_fast_unstake("Unstaked", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(stash))]);
    }

    // ─── SDK pallet: ConvictionVoting ─────────────────────────────────────

    #[test]
    fn conviction_voting_delegated() {
        let a = [28u8; 32];
        let b = [29u8; 32];
        let fields = unnamed(vec![bytes32_val(a), bytes32_val(b)]);
        let keys = index_conviction_voting("Delegated", &fields);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn conviction_voting_undelegated() {
        let a = [30u8; 32];
        let fields = unnamed(vec![bytes32_val(a)]);
        let keys = index_conviction_voting("Undelegated", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(a))]);
    }

    // ─── SDK pallet: Referenda ────────────────────────────────────────────

    #[test]
    fn referenda_submitted() {
        let fields = named(vec![("index", u128_val(42))]);
        let keys = index_referenda("Submitted", &fields);
        assert_eq!(keys, vec![Key::RefIndex(42)]);
    }

    #[test]
    fn referenda_all_variants_use_ref_index() {
        for event in &[
            "Submitted",
            "DecisionDepositPlaced",
            "DecisionStarted",
            "Confirmed",
            "Approved",
            "Rejected",
            "TimedOut",
            "Cancelled",
            "Killed",
        ] {
            let fields = named(vec![("index", u128_val(1))]);
            let keys = index_referenda(event, &fields);
            assert_eq!(keys, vec![Key::RefIndex(1)], "failed for {event}");
        }
    }

    // ─── SDK pallet: TransactionPayment ───────────────────────────────────

    #[test]
    fn transaction_payment_fee_paid() {
        let who = [31u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_transaction_payment("TransactionFeePaid", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(who))]);
    }

    // ─── SDK pallet: DelegatedStaking ─────────────────────────────────────

    #[test]
    fn delegated_staking_delegated() {
        let agent = [32u8; 32];
        let delegator = [33u8; 32];
        let fields = named(vec![
            ("agent", bytes32_val(agent)),
            ("delegator", bytes32_val(delegator)),
        ]);
        let keys = index_delegated_staking("Delegated", &fields);
        assert_eq!(keys.len(), 2);
    }

    // ─── SDK dispatch ─────────────────────────────────────────────────────

    #[test]
    fn index_sdk_pallet_dispatch() {
        let acct = [40u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_sdk_pallet("System", "NewAccount", &fields).unwrap();
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    #[test]
    fn index_sdk_pallet_unknown_returns_none() {
        let fields = named(vec![]);
        assert!(index_sdk_pallet("NonExistentPallet", "Foo", &fields).is_none());
    }

    #[test]
    fn index_sdk_pallet_all_known_pallets() {
        let fields = named(vec![]);
        let known = [
            "System",
            "Balances",
            "Staking",
            "Session",
            "Indices",
            "Preimage",
            "Treasury",
            "Bounties",
            "ChildBounties",
            "Vesting",
            "Proxy",
            "Multisig",
            "ElectionProviderMultiPhase",
            "VoterList",
            "NominationPools",
            "FastUnstake",
            "ConvictionVoting",
            "Referenda",
            "TransactionPayment",
            "DelegatedStaking",
        ];
        for name in known {
            assert!(
                index_sdk_pallet(name, "UnknownEvent", &fields).is_some(),
                "dispatch should recognise {name}"
            );
        }
    }
}

#[cfg(test)]
mod shared_tests {
    use crate::shared::*;
    use zerocopy::AsBytes;

    // ─── Bytes32 ──────────────────────────────────────────────────────────

    #[test]
    fn bytes32_serialize_json() {
        let b = Bytes32([0xAB; 32]);
        let json = serde_json::to_string(&b).unwrap();
        assert_eq!(json, format!("\"0x{}\"", hex::encode([0xAB; 32])));
    }

    #[test]
    fn bytes32_deserialize_json_with_prefix() {
        let hex_str = format!("\"0x{}\"", hex::encode([0xCD; 32]));
        let b: Bytes32 = serde_json::from_str(&hex_str).unwrap();
        assert_eq!(b.0, [0xCD; 32]);
    }

    #[test]
    fn bytes32_deserialize_json_without_prefix() {
        let hex_str = format!("\"{}\"", hex::encode([0xEF; 32]));
        let b: Bytes32 = serde_json::from_str(&hex_str).unwrap();
        assert_eq!(b.0, [0xEF; 32]);
    }

    #[test]
    fn bytes32_from_array() {
        let arr = [42u8; 32];
        let b: Bytes32 = arr.into();
        assert_eq!(b.0, arr);
    }

    #[test]
    fn bytes32_as_ref_array() {
        let b = Bytes32([1u8; 32]);
        let r: &[u8; 32] = b.as_ref();
        assert_eq!(r, &[1u8; 32]);
    }

    #[test]
    fn bytes32_as_ref_slice() {
        let b = Bytes32([2u8; 32]);
        let r: &[u8] = b.as_ref();
        assert_eq!(r.len(), 32);
    }

    // ─── On-disk key formats ──────────────────────────────────────────────

    #[test]
    fn variant_key_layout() {
        let key = VariantKey {
            pallet_index: 5,
            variant_index: 3,
            block_number: 1000u32.into(),
            event_index: 7u16.into(),
        };
        let bytes = key.as_bytes();
        // 1 + 1 + 4 + 2 = 8 bytes
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes[0], 5);
        assert_eq!(bytes[1], 3);
    }

    #[test]
    fn bytes32_key_layout() {
        let key = Bytes32Key {
            key: [0xAA; 32],
            block_number: 500u32.into(),
            event_index: 2u16.into(),
        };
        let bytes = key.as_bytes();
        // 32 + 4 + 2 = 38 bytes
        assert_eq!(bytes.len(), 38);
    }

    #[test]
    fn u32_key_layout() {
        let key = U32Key {
            key: 42u32.into(),
            block_number: 100u32.into(),
            event_index: 1u16.into(),
        };
        let bytes = key.as_bytes();
        // 4 + 4 + 2 = 10 bytes
        assert_eq!(bytes.len(), 10);
    }

    // ─── Key serialization ────────────────────────────────────────────────

    #[test]
    fn key_json_round_trip_account_id() {
        let k = Key::AccountId(Bytes32([0x11; 32]));
        let json = serde_json::to_string(&k).unwrap();
        let k2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn key_json_round_trip_u32_types() {
        for key in [
            Key::AccountIndex(1),
            Key::AuctionIndex(2),
            Key::BountyIndex(3),
            Key::EraIndex(4),
            Key::ParaId(1000),
            Key::PoolId(5),
            Key::ProposalIndex(6),
            Key::RefIndex(7),
            Key::RegistrarIndex(8),
            Key::SessionIndex(9),
            Key::SpendIndex(10),
        ] {
            let json = serde_json::to_string(&key).unwrap();
            let k2: Key = serde_json::from_str(&json).unwrap();
            assert_eq!(key, k2);
        }
    }

    #[test]
    fn key_json_round_trip_bytes32_types() {
        let b = Bytes32([0xFF; 32]);
        for key in [
            Key::CandidateHash(b),
            Key::MessageId(b),
            Key::PreimageHash(b),
            Key::ProposalHash(b),
            Key::TipHash(b),
        ] {
            let json = serde_json::to_string(&key).unwrap();
            let k2: Key = serde_json::from_str(&json).unwrap();
            assert_eq!(key, k2);
        }
    }

    #[test]
    fn key_json_variant() {
        let k = Key::Variant(5, 3);
        let json = serde_json::to_string(&k).unwrap();
        assert!(json.contains("Variant"));
        let k2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(k, k2);
    }

    // ─── RequestMessage deserialization ───────────────────────────────────

    #[test]
    fn request_status() {
        let msg: RequestMessage = serde_json::from_str(r#"{"type":"Status"}"#).unwrap();
        assert!(matches!(msg, RequestMessage::Status));
    }

    #[test]
    fn request_subscribe_status() {
        let msg: RequestMessage = serde_json::from_str(r#"{"type":"SubscribeStatus"}"#).unwrap();
        assert!(matches!(msg, RequestMessage::SubscribeStatus));
    }

    #[test]
    fn request_get_events_para_id() {
        let json = r#"{"type":"GetEvents","key":{"type":"ParaId","value":2000}}"#;
        let msg: RequestMessage = serde_json::from_str(json).unwrap();
        match msg {
            RequestMessage::GetEvents {
                key: Key::ParaId(id),
            } => {
                assert_eq!(id, 2000)
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_get_events_account_id() {
        let hex = hex::encode([0xAA; 32]);
        let json =
            format!(r#"{{"type":"GetEvents","key":{{"type":"AccountId","value":"0x{hex}"}}}}"#);
        let msg: RequestMessage = serde_json::from_str(&json).unwrap();
        match msg {
            RequestMessage::GetEvents {
                key: Key::AccountId(b),
            } => assert_eq!(b.0, [0xAA; 32]),
            _ => panic!("wrong variant"),
        }
    }

    // ─── EventRef / Span Display ──────────────────────────────────────────

    #[test]
    fn event_ref_display() {
        let e = EventRef {
            block_number: 100,
            event_index: 5,
        };
        let s = format!("{e}");
        assert!(s.contains("100"));
        assert!(s.contains("5"));
    }

    #[test]
    fn span_display() {
        let s = Span { start: 10, end: 20 };
        let out = format!("{s}");
        assert!(out.contains("10"));
        assert!(out.contains("20"));
    }
}

#[cfg(test)]
mod indexer_tests {
    use crate::config::ChainConfig;
    use crate::indexer::*;
    use crate::shared::*;
    use scale_value::{Composite, Primitive, Value, ValueDef};

    fn u128_val(n: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(n)),
            context: (),
        }
    }

    fn bytes32_val(b: [u8; 32]) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(
                b.iter()
                    .map(|&byte| Value {
                        value: ValueDef::Primitive(Primitive::U128(byte as u128)),
                        context: (),
                    })
                    .collect(),
            )),
            context: (),
        }
    }

    fn named(fields: Vec<(&str, Value<()>)>) -> Composite<()> {
        Composite::Named(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    fn unnamed(fields: Vec<Value<()>>) -> Composite<()> {
        Composite::Unnamed(fields)
    }

    fn test_config() -> ChainConfig {
        toml::from_str(crate::config::POLKADOT_TOML).unwrap()
    }

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    // ─── keys_for_event: SDK pallet ───────────────────────────────────────

    #[test]
    fn keys_for_event_sdk_pallet() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let acct = [0xAAu8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = indexer.keys_for_event("System", "NewAccount", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
    }

    // ─── keys_for_event: custom pallet ────────────────────────────────────

    #[test]
    fn keys_for_event_custom_pallet_claims() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let who = [0xBBu8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = indexer.keys_for_event("Claims", "Claimed", &fields);
        assert_eq!(keys, vec![Key::AccountId(Bytes32(who))]);
    }

    #[test]
    fn keys_for_event_custom_pallet_paras_positional() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = unnamed(vec![u128_val(2000)]);
        let keys = indexer.keys_for_event("Paras", "CurrentCodeUpdated", &fields);
        assert_eq!(keys, vec![Key::ParaId(2000)]);
    }

    #[test]
    fn keys_for_event_custom_pallet_registrar_multi() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let manager = [0xCCu8; 32];
        let fields = named(vec![
            ("para_id", u128_val(1000)),
            ("manager", bytes32_val(manager)),
        ]);
        let keys = indexer.keys_for_event("Registrar", "Registered", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::ParaId(1000)));
        assert!(keys.contains(&Key::AccountId(Bytes32(manager))));
    }

    // ─── keys_for_event: unknown pallet/event ─────────────────────────────

    #[test]
    fn keys_for_event_unknown_pallet_returns_empty() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![]);
        let keys = indexer.keys_for_event("UnknownPallet", "Foo", &fields);
        assert!(keys.is_empty());
    }

    #[test]
    fn keys_for_event_unknown_event_in_custom_pallet() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![]);
        let keys = indexer.keys_for_event("Claims", "NonExistent", &fields);
        assert!(keys.is_empty());
    }

    // ─── DB write & read round trip ───────────────────────────────────────

    #[test]
    fn index_and_retrieve_account_id() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let acct = Bytes32([0xDD; 32]);
        let key = Key::AccountId(acct);
        indexer.index_event_key(key.clone(), 100, 3).unwrap();
        indexer.index_event_key(key.clone(), 200, 1).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 2);
        // Results come in reverse order (newest first).
        assert_eq!(events[0].block_number, 200);
        assert_eq!(events[1].block_number, 100);
    }

    #[test]
    fn index_and_retrieve_para_id() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::ParaId(2000);
        indexer.index_event_key(key.clone(), 50, 0).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 50);
        assert_eq!(events[0].event_index, 0);
    }

    #[test]
    fn index_variant_key() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::Variant(5, 3);
        indexer.index_event_key(key.clone(), 10, 2).unwrap();
        indexer.index_event_key(key.clone(), 20, 4).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 20);
        assert_eq!(events[1].block_number, 10);
    }

    #[test]
    fn retrieve_max_100_events() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::EraIndex(1);
        for i in 0..150u32 {
            indexer.index_event_key(key.clone(), i, 0).unwrap();
        }

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 100);
        // Should be the most recent 100.
        assert_eq!(events[0].block_number, 149);
        assert_eq!(events[99].block_number, 50);
    }

    // ─── encode_event ─────────────────────────────────────────────────────

    #[test]
    fn encode_event_structure() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![("amount", u128_val(999))]);
        let json = indexer.encode_event("Balances", "Deposit", 5, 2, 7, &fields);

        assert_eq!(json["palletName"], "Balances");
        assert_eq!(json["eventName"], "Deposit");
        assert_eq!(json["palletIndex"], 5);
        assert_eq!(json["variantIndex"], 2);
        assert_eq!(json["eventIndex"], 7);
        assert_eq!(json["fields"]["amount"], "999");
    }

    // ─── composite_to_json ────────────────────────────────────────────────

    #[test]
    fn composite_to_json_named() {
        let c = named(vec![("x", u128_val(1)), ("y", u128_val(2))]);
        let j = composite_to_json(&c);
        assert_eq!(j["x"], "1");
        assert_eq!(j["y"], "2");
    }

    #[test]
    fn composite_to_json_unnamed_single() {
        let c = unnamed(vec![u128_val(42)]);
        let j = composite_to_json(&c);
        assert_eq!(j, "42");
    }

    #[test]
    fn composite_to_json_unnamed_multi() {
        let c = unnamed(vec![u128_val(1), u128_val(2), u128_val(3)]);
        let j = composite_to_json(&c);
        // 3 U128 values ≤ 255 → treated as byte array
        assert_eq!(j, "0x010203");
    }

    #[test]
    fn composite_to_json_byte_array_32() {
        let bytes: Vec<Value<()>> = (0..32).map(|i| u128_val(i as u128)).collect();
        let c = unnamed(bytes);
        let j = composite_to_json(&c);
        // Should be hex string
        assert!(j.as_str().unwrap().starts_with("0x"));
        assert_eq!(j.as_str().unwrap().len(), 66); // "0x" + 64 hex chars
    }

    #[test]
    fn composite_to_json_non_byte_unnamed() {
        // Values > 255 prevent byte-array detection
        let c = unnamed(vec![u128_val(1000), u128_val(2000)]);
        let j = composite_to_json(&c);
        assert!(j.is_array());
        assert_eq!(j.as_array().unwrap().len(), 2);
    }

    #[test]
    fn value_to_json_bool() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::Bool(true)),
            context: (),
        };
        let j = super::indexer_tests_helpers::value_to_json_pub(&v);
        assert_eq!(j, true);
    }

    #[test]
    fn value_to_json_string() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::String("hello".into())),
            context: (),
        };
        let j = super::indexer_tests_helpers::value_to_json_pub(&v);
        assert_eq!(j, "hello");
    }

    // ─── Span helpers ─────────────────────────────────────────────────────

    #[test]
    fn check_next_batch_block_skips_spans() {
        let spans = vec![Span { start: 10, end: 20 }, Span { start: 30, end: 40 }];
        let mut next = 35u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 29);
    }

    #[test]
    fn check_next_batch_block_no_overlap() {
        let spans = vec![Span { start: 10, end: 20 }];
        let mut next = 25u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 25); // unchanged
    }

    #[test]
    fn check_next_batch_block_multiple_overlaps() {
        let spans = vec![Span { start: 5, end: 10 }, Span { start: 11, end: 20 }];
        let mut next = 15u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 4);
    }

    #[test]
    fn check_span_merges_adjacent() {
        let trees = temp_trees();
        let mut spans = vec![Span { start: 10, end: 20 }];

        // Write the old span to the DB.
        let sv = SpanDbValue {
            start: 10u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(20u32.to_be_bytes(), zerocopy::AsBytes::as_bytes(&sv))
            .unwrap();

        let mut current = Span { start: 21, end: 30 };

        check_span(&trees.span, &mut spans, &mut current).unwrap();
        assert_eq!(current.start, 10);
        assert!(spans.is_empty());
    }

    #[test]
    fn check_span_no_merge_when_gap() {
        let trees = temp_trees();
        let mut spans = vec![Span { start: 10, end: 20 }];
        let mut current = Span { start: 25, end: 30 };

        check_span(&trees.span, &mut spans, &mut current).unwrap();
        assert_eq!(current.start, 25);
        assert_eq!(spans.len(), 1);
    }

    // ─── Trees flush ──────────────────────────────────────────────────────

    #[test]
    fn trees_flush_succeeds() {
        let trees = temp_trees();
        trees.flush().unwrap();
    }
}

// Helper module to expose private functions for testing.
#[cfg(test)]
mod indexer_tests_helpers {
    use scale_value::Value;

    // Re-export the private value_to_json for tests.
    pub fn value_to_json_pub(v: &Value<()>) -> serde_json::Value {
        crate::indexer::composite_to_json(&scale_value::Composite::Unnamed(vec![v.clone()]))
    }
}

#[cfg(test)]
mod websocket_tests {
    use crate::shared::*;
    use crate::websockets::*;
    use zerocopy::AsBytes;

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    #[test]
    fn process_msg_status_empty() {
        let trees = temp_trees();
        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseMessage::Status(spans) => assert!(spans.is_empty()),
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_status_with_spans() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 100u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(200u32.to_be_bytes(), sv.as_bytes())
            .unwrap();

        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseMessage::Status(spans) => {
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].start, 100);
                assert_eq!(spans[0].end, 200);
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_get_events_empty() {
        let trees = temp_trees();
        let key = Key::AccountId(Bytes32([0; 32]));
        let msg = process_msg_get_events(&trees, key.clone());
        match msg {
            ResponseMessage::Events {
                key: k,
                events,
                block_events,
            } => {
                assert_eq!(k, key);
                assert!(events.is_empty());
                assert!(block_events.is_empty());
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_get_events_with_data() {
        let trees = temp_trees();

        // Insert an index entry.
        let key = Key::ParaId(1000);
        key.write_db_key(&trees, 50, 3).unwrap();

        // Insert block events JSON.
        let bn_key: zerocopy::byteorder::U32<zerocopy::BigEndian> = 50u32.into();
        let json = serde_json::to_vec(&serde_json::json!([
            {"palletName": "Paras", "eventName": "Test"}
        ]))
        .unwrap();
        trees
            .block_events
            .insert(bn_key.as_bytes(), json.as_slice())
            .unwrap();

        let msg = process_msg_get_events(&trees, key);
        match msg {
            ResponseMessage::Events {
                events,
                block_events,
                ..
            } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].block_number, 50);
                assert_eq!(events[0].event_index, 3);
                assert_eq!(block_events.len(), 1);
                assert_eq!(block_events[0].block_number, 50);
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn get_events_u32_empty_tree() {
        let trees = temp_trees();
        let events = get_events_u32(&trees.chain.para_id, 999);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_bytes32_empty_tree() {
        let trees = temp_trees();
        let b = Bytes32([0; 32]);
        let events = get_events_bytes32(&trees.substrate.account_id, &b);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_u32_multiple() {
        let trees = temp_trees();
        let key = Key::PoolId(7);
        key.write_db_key(&trees, 10, 0).unwrap();
        key.write_db_key(&trees, 20, 1).unwrap();
        key.write_db_key(&trees, 30, 2).unwrap();

        let events = get_events_u32(&trees.substrate.pool_id, 7);
        assert_eq!(events.len(), 3);
        // Reverse order (newest first).
        assert_eq!(events[0].block_number, 30);
        assert_eq!(events[2].block_number, 10);
    }

    #[test]
    fn get_events_bytes32_multiple() {
        let trees = temp_trees();
        let b = Bytes32([0x11; 32]);
        let key = Key::AccountId(b);
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let events = get_events_bytes32(&trees.substrate.account_id, &b);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    // ─── ResponseMessage serialization ────────────────────────────────────

    #[test]
    fn response_status_serializes() {
        let msg = ResponseMessage::Status(vec![Span { start: 1, end: 100 }]);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Status"));
        assert!(json.contains("100"));
    }

    #[test]
    fn response_subscribed_serializes() {
        let msg = ResponseMessage::Subscribed;
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Subscribed"));
    }

    #[test]
    fn response_size_on_disk_serializes() {
        let msg = ResponseMessage::SizeOnDisk(123456);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("123456"));
    }

    #[test]
    fn response_events_serializes() {
        let msg = ResponseMessage::Events {
            key: Key::RefIndex(42),
            events: vec![EventRef {
                block_number: 10,
                event_index: 2,
            }],
            block_events: vec![],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("RefIndex"));
        assert!(json.contains("42"));
    }
}
