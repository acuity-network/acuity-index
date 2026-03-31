#[cfg(test)]
mod pallets_tests {
    use crate::pallets::*;
    use crate::shared::{Bytes32, CustomKey, CustomValue, Key};

    fn key_u32(name: &str, value: u32) -> Key {
        Key::Custom(CustomKey {
            name: name.to_owned(),
            value: CustomValue::U32(value),
        })
    }

    fn key_bytes32(name: &str, value: [u8; 32]) -> Key {
        Key::Custom(CustomKey {
            name: name.to_owned(),
            value: CustomValue::Bytes32(Bytes32(value)),
        })
    }
    use scale_value::{Composite, Primitive, Value, ValueDef, Variant};

    fn u128_val(n: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(n)),
            context: (),
        }
    }

    fn bool_value(value: bool) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::Bool(value)),
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

    fn variant(name: &str, values: Composite<()>) -> Value<()> {
        Value {
            value: ValueDef::Variant(Variant {
                name: name.to_string(),
                values,
            }),
            context: (),
        }
    }

    fn composite_value(values: Composite<()>) -> Value<()> {
        Value {
            value: ValueDef::Composite(values),
            context: (),
        }
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

    #[test]
    fn extract_u64_from_wrapped_named() {
        let v = Value {
            value: ValueDef::Composite(Composite::Named(vec![("inner".into(), u128_val(123))])),
            context: (),
        };
        assert_eq!(extract_u64(&v), Some(123));
    }

    #[test]
    fn extract_u32_from_wrapped_named_and_invalid_bool() {
        let named_value = Value {
            value: ValueDef::Composite(Composite::Named(vec![("inner".into(), u128_val(12))])),
            context: (),
        };
        let bool_value = Value {
            value: ValueDef::Primitive(Primitive::Bool(true)),
            context: (),
        };

        assert_eq!(extract_u32(&named_value), Some(12));
        assert_eq!(extract_u32(&bool_value), None);
    }

    #[test]
    fn extract_u128_from_i128() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::I128(456)),
            context: (),
        };
        assert_eq!(extract_u128(&v), Some(456));
    }

    #[test]
    fn extract_u128_from_wrapped_values_and_invalid_string() {
        let unnamed = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![u128_val(321)])),
            context: (),
        };
        let named = Value {
            value: ValueDef::Composite(Composite::Named(vec![("inner".into(), u128_val(654))])),
            context: (),
        };
        let invalid = Value {
            value: ValueDef::Primitive(Primitive::String("bad".into())),
            context: (),
        };

        assert_eq!(extract_u128(&unnamed), Some(321));
        assert_eq!(extract_u128(&named), Some(654));
        assert_eq!(extract_u128(&invalid), None);
    }

    #[test]
    fn extract_string_and_bool_from_wrapped_values() {
        let string_value = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![Value {
                value: ValueDef::Primitive(Primitive::String("hello".into())),
                context: (),
            }])),
            context: (),
        };
        let bool_value = Value {
            value: ValueDef::Composite(Composite::Named(vec![(
                "inner".into(),
                Value {
                    value: ValueDef::Primitive(Primitive::Bool(true)),
                    context: (),
                },
            )])),
            context: (),
        };

        assert_eq!(extract_string(&string_value), Some("hello".into()));
        assert_eq!(extract_bool(&bool_value), Some(true));
    }

    #[test]
    fn extract_string_and_bool_invalid_paths_return_none() {
        let invalid_string = Value {
            value: ValueDef::Primitive(Primitive::Bool(false)),
            context: (),
        };
        let invalid_bool = Value {
            value: ValueDef::Primitive(Primitive::String("nope".into())),
            context: (),
        };

        assert_eq!(extract_string(&invalid_string), None);
        assert_eq!(extract_bool(&invalid_bool), None);
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
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(acct))
            })]
        );
    }

    #[test]
    fn system_killed_account() {
        let acct = [2u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_system("KilledAccount", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(acct))
            })]
        );
    }

    #[test]
    fn system_remarked() {
        let sender = [3u8; 32];
        let fields = named(vec![("sender", bytes32_val(sender))]);
        let keys = index_system("Remarked", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(sender))
            })]
        );
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
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(from))
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(to))
        })));
    }

    #[test]
    fn balances_endowed() {
        let acct = [11u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_balances("Endowed", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(acct))
            })]
        );
    }

    #[test]
    fn balances_reserved() {
        let who = [12u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_balances("Reserved", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(who))
            })]
        );
    }

    #[test]
    fn balances_locked_indexes_account() {
        let who = [42u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_balances("Locked", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(who))
            })]
        );
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
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "era_index".into(),
                value: CustomValue::U32(100)
            })]
        );
    }

    #[test]
    fn staking_bonded() {
        let stash = [15u8; 32];
        let fields = named(vec![("stash", bytes32_val(stash))]);
        let keys = index_staking("Bonded", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32(stash))
            })]
        );
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
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "era_index".into(),
            value: CustomValue::U32(50)
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(validator))
        })));
    }

    #[test]
    fn staking_slash_reported_indexes_validator_and_era() {
        let validator = [77u8; 32];
        let fields = named(vec![
            ("validator", bytes32_val(validator)),
            ("slash_era", u128_val(9)),
        ]);
        let keys = index_staking("SlashReported", &fields);
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(validator))
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "era_index".into(),
            value: CustomValue::U32(9)
        })));
    }

    #[test]
    fn staking_kicked_indexes_nominator_and_stash() {
        let nominator = [55u8; 32];
        let stash = [56u8; 32];
        let fields = named(vec![
            ("nominator", bytes32_val(nominator)),
            ("stash", bytes32_val(stash)),
        ]);
        let keys = index_staking("Kicked", &fields);
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(nominator))
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(stash))
        })));
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
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "session_index".into(),
                value: CustomValue::U32(777)
            })]
        );
    }

    // ─── SDK pallet: Indices ──────────────────────────────────────────────

    #[test]
    fn indices_index_assigned() {
        let who = [17u8; 32];
        let fields = named(vec![("who", bytes32_val(who)), ("index", u128_val(42))]);
        let keys = index_indices("IndexAssigned", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(who))
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_index".into(),
            value: CustomValue::U32(42)
        })));
    }

    // ─── SDK pallet: Preimage ─────────────────────────────────────────────

    #[test]
    fn preimage_noted() {
        let hash = [0xABu8; 32];
        let fields = named(vec![("hash", bytes32_val(hash))]);
        let keys = index_preimage("Noted", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "preimage_hash".into(),
                value: CustomValue::Bytes32(Bytes32(hash))
            })]
        );
    }

    // ─── SDK pallet: Treasury ─────────────────────────────────────────────

    #[test]
    fn treasury_proposed() {
        let fields = named(vec![("proposal_index", u128_val(5))]);
        let keys = index_treasury("Proposed", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "proposal_index".into(),
                value: CustomValue::U32(5)
            })]
        );
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
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "proposal_index".into(),
            value: CustomValue::U32(10)
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32(beneficiary))
        })));
    }

    #[test]
    fn treasury_paid() {
        let fields = named(vec![("index", u128_val(3))]);
        let keys = index_treasury("Paid", &fields);
        assert_eq!(
            keys,
            vec![Key::Custom(CustomKey {
                name: "spend_index".into(),
                value: CustomValue::U32(3)
            })]
        );
    }

    #[test]
    fn treasury_awarded_indexes_account() {
        let account = [66u8; 32];
        let fields = named(vec![
            ("proposal_index", u128_val(10)),
            ("account", bytes32_val(account)),
        ]);
        let keys = index_treasury("Awarded", &fields);
        assert!(keys.contains(&key_u32("proposal_index", 10)));
        assert!(keys.contains(&key_bytes32("account_id", account)));
    }

    #[test]
    fn treasury_asset_spend_approved_indexes_spend_index() {
        let fields = named(vec![("index", u128_val(88))]);
        let keys = index_treasury("AssetSpendApproved", &fields);
        assert_eq!(keys, vec![key_u32("spend_index", 88)]);
    }

    // ─── SDK pallet: Bounties ─────────────────────────────────────────────

    #[test]
    fn bounties_proposed() {
        let fields = named(vec![("index", u128_val(7))]);
        let keys = index_bounties("BountyProposed", &fields);
        assert_eq!(keys, vec![key_u32("bounty_index", 7)]);
    }

    #[test]
    fn bounties_curator_proposed_indexes_bounty_and_curator() {
        let curator = [67u8; 32];
        let fields = named(vec![
            ("bounty_id", u128_val(9)),
            ("curator", bytes32_val(curator)),
        ]);
        let keys = index_bounties("CuratorProposed", &fields);
        assert!(keys.contains(&key_u32("bounty_index", 9)));
        assert!(keys.contains(&key_bytes32("account_id", curator)));
    }

    #[test]
    fn bounties_awarded_indexes_beneficiary() {
        let beneficiary = [68u8; 32];
        let fields = named(vec![
            ("index", u128_val(13)),
            ("beneficiary", bytes32_val(beneficiary)),
        ]);
        let keys = index_bounties("BountyAwarded", &fields);
        assert!(keys.contains(&key_u32("bounty_index", 13)));
        assert!(keys.contains(&key_bytes32("account_id", beneficiary)));
    }

    // ─── SDK pallet: ChildBounties ────────────────────────────────────────

    #[test]
    fn child_bounties_added() {
        let fields = named(vec![("index", u128_val(4)), ("child_index", u128_val(5))]);
        let keys = index_child_bounties("Added", &fields);
        assert!(keys.contains(&key_u32("bounty_index", 4)));
        assert!(keys.contains(&key_u32("bounty_index", 5)));
    }

    #[test]
    fn child_bounties_awarded_indexes_child_and_beneficiary() {
        let beneficiary = [69u8; 32];
        let fields = named(vec![
            ("index", u128_val(4)),
            ("child_index", u128_val(6)),
            ("beneficiary", bytes32_val(beneficiary)),
        ]);
        let keys = index_child_bounties("Awarded", &fields);
        assert!(keys.contains(&key_u32("bounty_index", 4)));
        assert!(keys.contains(&key_u32("bounty_index", 6)));
        assert!(keys.contains(&key_bytes32("account_id", beneficiary)));
    }

    // ─── SDK pallet: Vesting ──────────────────────────────────────────────

    #[test]
    fn vesting_updated() {
        let acct = [19u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_vesting("VestingUpdated", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", acct)]);
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
        assert!(keys.contains(&key_bytes32("account_id", pure)));
        assert!(keys.contains(&key_bytes32("account_id", who)));
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

    #[test]
    fn multisig_cancelled_uses_cancelling_field() {
        let cancelling = [70u8; 32];
        let multisig = [71u8; 32];
        let fields = named(vec![
            ("cancelling", bytes32_val(cancelling)),
            ("multisig", bytes32_val(multisig)),
        ]);
        let keys = index_multisig("MultisigCancelled", &fields);
        assert!(keys.contains(&key_bytes32("account_id", cancelling)));
        assert!(keys.contains(&key_bytes32("account_id", multisig)));
    }

    #[test]
    fn election_provider_solution_stored_indexes_origin() {
        let origin = [81u8; 32];
        let fields = named(vec![(
            "origin",
            variant("Some", unnamed(vec![bytes32_val(origin)])),
        )]);
        let keys = index_election_provider_multi_phase("SolutionStored", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", origin)]);
    }

    #[test]
    fn election_provider_solution_stored_none_indexes_nothing() {
        let fields = named(vec![("origin", variant("None", unnamed(vec![])))]);
        let keys = index_election_provider_multi_phase("SolutionStored", &fields);
        assert!(keys.is_empty());
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
        assert!(keys.contains(&key_bytes32("account_id", depositor)));
        assert!(keys.contains(&key_u32("pool_id", 1)));
    }

    #[test]
    fn nomination_pools_destroyed() {
        let fields = named(vec![("pool_id", u128_val(99))]);
        let keys = index_nomination_pools("Destroyed", &fields);
        assert_eq!(keys, vec![key_u32("pool_id", 99)]);
    }

    #[test]
    fn nomination_pools_unbonded_indexes_era() {
        let member = [72u8; 32];
        let fields = named(vec![
            ("member", bytes32_val(member)),
            ("pool_id", u128_val(10)),
            ("era", u128_val(99)),
        ]);
        let keys = index_nomination_pools("Unbonded", &fields);
        assert!(keys.contains(&key_bytes32("account_id", member)));
        assert!(keys.contains(&key_u32("pool_id", 10)));
        assert!(keys.contains(&key_u32("era_index", 99)));
    }

    #[test]
    fn nomination_pools_roles_updated_indexes_optional_accounts() {
        let root = [73u8; 32];
        let nominator = [74u8; 32];
        let fields = named(vec![
            ("root", variant("Some", unnamed(vec![bytes32_val(root)]))),
            ("bouncer", variant("None", unnamed(vec![]))),
            (
                "nominator",
                variant("Some", unnamed(vec![bytes32_val(nominator)])),
            ),
        ]);
        let keys = index_nomination_pools("RolesUpdated", &fields);
        assert!(keys.contains(&key_bytes32("account_id", root)));
        assert!(keys.contains(&key_bytes32("account_id", nominator)));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn nomination_pools_pool_commission_updated_indexes_current_account() {
        let account = [75u8; 32];
        let current = variant(
            "Some",
            unnamed(vec![composite_value(unnamed(vec![
                u128_val(1),
                bytes32_val(account),
            ]))]),
        );
        let fields = named(vec![("pool_id", u128_val(7)), ("current", current)]);
        let keys = index_nomination_pools("PoolCommissionUpdated", &fields);
        assert!(keys.contains(&key_u32("pool_id", 7)));
        assert!(keys.contains(&key_bytes32("account_id", account)));
    }

    // ─── SDK pallet: FastUnstake ──────────────────────────────────────────

    #[test]
    fn fast_unstake_unstaked() {
        let stash = [27u8; 32];
        let fields = named(vec![("stash", bytes32_val(stash))]);
        let keys = index_fast_unstake("Unstaked", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", stash)]);
    }

    #[test]
    fn fast_unstake_batch_checked_indexes_eras() {
        let fields = named(vec![(
            "eras",
            composite_value(unnamed(vec![u128_val(5), u128_val(6), u128_val(7)])),
        )]);
        let keys = index_fast_unstake("BatchChecked", &fields);
        assert_eq!(
            keys,
            vec![
                key_u32("era_index", 5),
                key_u32("era_index", 6),
                key_u32("era_index", 7)
            ]
        );
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
        assert_eq!(keys, vec![key_bytes32("account_id", a)]);
    }

    #[test]
    fn conviction_voting_vote_removed_indexes_who() {
        let who = [76u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_conviction_voting("VoteRemoved", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", who)]);
    }

    // ─── SDK pallet: Referenda ────────────────────────────────────────────

    #[test]
    fn referenda_submitted() {
        let fields = named(vec![("index", u128_val(42))]);
        let keys = index_referenda("Submitted", &fields);
        assert_eq!(keys, vec![key_u32("ref_index", 42)]);
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
            assert_eq!(keys, vec![key_u32("ref_index", 1)], "failed for {event}");
        }
    }

    #[test]
    fn referenda_decision_deposit_placed_indexes_who_and_ref() {
        let who = [77u8; 32];
        let fields = named(vec![("index", u128_val(11)), ("who", bytes32_val(who))]);
        let keys = index_referenda("DecisionDepositPlaced", &fields);
        assert!(keys.contains(&key_u32("ref_index", 11)));
        assert!(keys.contains(&key_bytes32("account_id", who)));
    }

    #[test]
    fn referenda_deposit_slashed_indexes_only_who() {
        let who = [78u8; 32];
        let fields = named(vec![("index", u128_val(12)), ("who", bytes32_val(who))]);
        let keys = index_referenda("DepositSlashed", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", who)]);
    }

    // ─── SDK pallet: TransactionPayment ───────────────────────────────────

    #[test]
    fn transaction_payment_fee_paid() {
        let who = [31u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_transaction_payment("TransactionFeePaid", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", who)]);
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

    #[test]
    fn delegated_staking_released() {
        let agent = [79u8; 32];
        let delegator = [80u8; 32];
        let fields = named(vec![
            ("agent", bytes32_val(agent)),
            ("delegator", bytes32_val(delegator)),
        ]);
        let keys = index_delegated_staking("Released", &fields);
        assert!(keys.contains(&key_bytes32("account_id", agent)));
        assert!(keys.contains(&key_bytes32("account_id", delegator)));
    }

    #[test]
    fn identity_judgement_given_indexes_target_and_registrar() {
        let target = [82u8; 32];
        let fields = named(vec![
            ("target", bytes32_val(target)),
            ("registrar_index", u128_val(5)),
        ]);
        let keys = index_identity("JudgementGiven", &fields);
        assert!(keys.contains(&key_bytes32("account_id", target)));
        assert!(keys.contains(&key_u32("registrar_index", 5)));
    }

    #[test]
    fn recovery_vouched_indexes_three_accounts() {
        let lost = [83u8; 32];
        let rescuer = [84u8; 32];
        let sender = [85u8; 32];
        let fields = named(vec![
            ("lost_account", bytes32_val(lost)),
            ("rescuer_account", bytes32_val(rescuer)),
            ("sender", bytes32_val(sender)),
        ]);
        let keys = index_recovery("RecoveryVouched", &fields);
        assert!(keys.contains(&key_bytes32("account_id", lost)));
        assert!(keys.contains(&key_bytes32("account_id", rescuer)));
        assert!(keys.contains(&key_bytes32("account_id", sender)));
    }

    #[test]
    fn sudo_key_changed_indexes_new_and_old_if_present() {
        let old = [86u8; 32];
        let new = [87u8; 32];
        let fields = named(vec![
            ("old", variant("Some", unnamed(vec![bytes32_val(old)]))),
            ("new", bytes32_val(new)),
        ]);
        let keys = index_sudo("KeyChanged", &fields);
        assert!(keys.contains(&key_bytes32("account_id", old)));
        assert!(keys.contains(&key_bytes32("account_id", new)));
    }

    #[test]
    fn state_trie_migration_slashed_indexes_who() {
        let who = [88u8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = index_state_trie_migration("Slashed", &fields);
        assert_eq!(keys, vec![key_bytes32("account_id", who)]);
    }

    #[test]
    fn para_inclusion_candidate_backed_indexes_core_and_group() {
        let fields = unnamed(vec![u128_val(0), u128_val(1), u128_val(9), u128_val(12)]);
        let keys = index_para_inclusion("CandidateBacked", &fields);
        assert!(keys.contains(&key_u32("core_index", 9)));
        assert!(keys.contains(&key_u32("group_index", 12)));
    }

    #[test]
    fn paras_pvf_check_started_indexes_hash_and_id() {
        let code_hash = [0x41; 32];
        let para_id = [0x42; 32];
        let fields = unnamed(vec![bytes32_val(code_hash), bytes32_val(para_id)]);
        let keys = index_paras("PvfCheckStarted", &fields);
        assert!(keys.contains(&key_bytes32("validation_code_hash", code_hash)));
        assert!(keys.contains(&key_bytes32("id", para_id)));
    }

    #[test]
    fn paras_code_authorized_indexes_para_id_code_hash_and_expiry() {
        let code_hash = [0x43; 32];
        let fields = named(vec![
            ("para_id", u128_val(1000)),
            ("code_hash", bytes32_val(code_hash)),
            ("expire_at", u128_val(55)),
        ]);
        let keys = index_paras("CodeAuthorized", &fields);
        assert!(keys.contains(&key_u32("para_id", 1000)));
        assert!(keys.contains(&key_bytes32("code_hash", code_hash)));
        assert!(keys.contains(&key_u32("expire_at", 55)));
    }

    #[test]
    fn hrmp_open_channel_requested_indexes_sender_and_limits() {
        let sender = [0x44; 32];
        let fields = named(vec![
            ("sender", bytes32_val(sender)),
            ("recipient", u128_val(2000)),
            ("proposed_max_capacity", u128_val(8)),
            ("proposed_max_message_size", u128_val(16)),
        ]);
        let keys = index_hrmp("OpenChannelRequested", &fields);
        assert!(keys.contains(&key_bytes32("account_id", sender)));
        assert!(keys.contains(&key_u32("recipient", 2000)));
        assert!(keys.contains(&key_u32("proposed_max_capacity", 8)));
        assert!(keys.contains(&key_u32("proposed_max_message_size", 16)));
    }

    #[test]
    fn paras_disputes_revert_indexes_block_number() {
        let fields = unnamed(vec![u128_val(321)]);
        let keys = index_paras_disputes("Revert", &fields);
        assert_eq!(keys, vec![key_u32("block_number_for", 321)]);
    }

    #[test]
    fn on_demand_order_placed_indexes_para_price_and_account() {
        let who = [0x45; 32];
        let fields = named(vec![
            ("para_id", u128_val(77)),
            ("spot_price", u128_val(999)),
            ("ordered_by", bytes32_val(who)),
        ]);
        let keys = index_on_demand("OnDemandOrderPlaced", &fields);
        assert!(keys.contains(&key_u32("para_id", 77)));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "spot_price".into(),
            value: CustomValue::U128(crate::shared::U128Text(999))
        })));
        assert!(keys.contains(&key_bytes32("account_id", who)));
    }

    #[test]
    fn registrar_registered_indexes_para_and_manager() {
        let manager = [0x46; 32];
        let fields = named(vec![
            ("para_id", u128_val(88)),
            ("manager", bytes32_val(manager)),
        ]);
        let keys = index_registrar("Registered", &fields);
        assert!(keys.contains(&key_u32("para_id", 88)));
        assert!(keys.contains(&key_bytes32("account_id", manager)));
    }

    #[test]
    fn slots_leased_indexes_all_stable_fields() {
        let leaser = [0x47; 32];
        let fields = named(vec![
            ("para_id", u128_val(89)),
            ("leaser", bytes32_val(leaser)),
            ("period_begin", u128_val(4)),
            ("period_count", u128_val(2)),
            ("extra_reserved", u128_val(123)),
            ("total_amount", u128_val(456)),
        ]);
        let keys = index_slots("Leased", &fields);
        assert!(keys.contains(&key_u32("para_id", 89)));
        assert!(keys.contains(&key_bytes32("account_id", leaser)));
        assert!(keys.contains(&key_u32("period_begin", 4)));
        assert!(keys.contains(&key_u32("period_count", 2)));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "extra_reserved".into(),
            value: CustomValue::U128(crate::shared::U128Text(123))
        })));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "total_amount".into(),
            value: CustomValue::U128(crate::shared::U128Text(456))
        })));
    }

    #[test]
    fn auctions_bid_accepted_indexes_bidder_para_amount_and_slots() {
        let bidder = [0x48; 32];
        let fields = named(vec![
            ("bidder", bytes32_val(bidder)),
            ("para_id", u128_val(90)),
            ("amount", u128_val(500)),
            ("first_slot", u128_val(10)),
            ("last_slot", u128_val(20)),
        ]);
        let keys = index_auctions("BidAccepted", &fields);
        assert!(keys.contains(&key_bytes32("account_id", bidder)));
        assert!(keys.contains(&key_u32("para_id", 90)));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "amount".into(),
            value: CustomValue::U128(crate::shared::U128Text(500))
        })));
        assert!(keys.contains(&key_u32("first_slot", 10)));
        assert!(keys.contains(&key_u32("last_slot", 20)));
    }

    #[test]
    fn crowdloan_contributed_indexes_account_fund_and_amount() {
        let who = [0x49; 32];
        let fields = named(vec![
            ("who", bytes32_val(who)),
            ("fund_index", u128_val(7)),
            ("amount", u128_val(333)),
        ]);
        let keys = index_crowdloan("Contributed", &fields);
        assert!(keys.contains(&key_bytes32("account_id", who)));
        assert!(keys.contains(&key_u32("fund_index", 7)));
        assert!(keys.contains(&Key::Custom(CustomKey {
            name: "amount".into(),
            value: CustomValue::U128(crate::shared::U128Text(333))
        })));
    }

    #[test]
    fn assigned_slots_temporary_slot_assigned_indexes_id() {
        let id = [0x4A; 32];
        let fields = unnamed(vec![bytes32_val(id)]);
        let keys = index_assigned_slots("TemporarySlotAssigned", &fields);
        assert_eq!(keys, vec![key_bytes32("id", id)]);
    }

    #[test]
    fn coretime_events_index_when_and_core() {
        let revenue = index_coretime("RevenueInfoRequested", &named(vec![("when", u128_val(50))]));
        assert_eq!(revenue, vec![key_u32("when", 50)]);

        let assigned = index_coretime("CoreAssigned", &named(vec![("core", u128_val(6))]));
        assert_eq!(assigned, vec![key_u32("core", 6)]);
    }

    #[test]
    fn xcm_events_index_message_query_hash_and_version_keys() {
        let message_id = [0x4B; 32];
        let hash = [0x4C; 32];

        assert_eq!(
            index_xcm(
                "Sent",
                &named(vec![("message_id", bytes32_val(message_id))])
            ),
            vec![key_bytes32("message_id", message_id)]
        );

        assert_eq!(
            index_xcm(
                "UnexpectedResponse",
                &named(vec![("query_id", u128_val(44))])
            ),
            vec![Key::Custom(CustomKey {
                name: "query_id".into(),
                value: CustomValue::U64(crate::shared::U64Text(44))
            })]
        );

        assert_eq!(
            index_xcm("AssetsTrapped", &named(vec![("hash", bytes32_val(hash))])),
            vec![key_bytes32("hash", hash)]
        );

        let version_changed = index_xcm(
            "VersionChangeNotified",
            &named(vec![
                ("result", u128_val(2)),
                ("message_id", bytes32_val(message_id)),
            ]),
        );
        assert!(version_changed.contains(&key_u32("result", 2)));
        assert!(version_changed.contains(&key_bytes32("message_id", message_id)));

        assert_eq!(
            index_xcm(
                "SupportedVersionChanged",
                &named(vec![("version", u128_val(5))])
            ),
            vec![key_u32("version", 5)]
        );
    }

    #[test]
    fn staking_ah_client_indexes_validator_set_and_stash_events() {
        let stash = [0x4D; 32];
        let received = index_staking_ah_client(
            "ValidatorSetReceived",
            &named(vec![
                ("id", u128_val(3)),
                ("new_validator_set_count", u128_val(11)),
                ("leftover", bool_value(true)),
            ]),
        );
        assert!(received.contains(&key_u32("id", 3)));
        assert!(received.contains(&key_u32("new_validator_set_count", 11)));
        assert!(received.contains(&Key::Custom(CustomKey {
            name: "leftover".into(),
            value: CustomValue::Bool(true)
        })));

        let updated = index_staking_ah_client(
            "SessionKeysUpdated",
            &named(vec![("stash", bytes32_val(stash))]),
        );
        assert_eq!(updated, vec![key_bytes32("account_id", stash)]);
    }

    // ─── SDK dispatch ─────────────────────────────────────────────────────

    #[test]
    fn index_sdk_pallet_dispatch() {
        let acct = [40u8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = index_sdk_pallet("System", "NewAccount", &fields).unwrap();
        assert_eq!(keys, vec![key_bytes32("account_id", acct)]);
    }

    #[test]
    fn index_sdk_pallet_unknown_returns_none() {
        let fields = named(vec![]);
        assert!(index_sdk_pallet("NonExistentPallet", "Foo", &fields).is_none());
    }

    #[test]
    fn supported_sdk_pallet_list_matches_dispatch_expectations() {
        assert!(is_supported_sdk_pallet("System"));
        assert!(is_supported_sdk_pallet("Recovery"));
        assert!(is_supported_sdk_pallet("OnDemandAssignmentProvider"));
        assert!(is_supported_sdk_pallet("StakingAhClient"));
        assert!(is_supported_sdk_pallet("XcmPallet"));
        assert!(!is_supported_sdk_pallet("Claims"));
    }

    #[test]
    fn additional_pallet_event_branches_are_indexed() {
        let account = [90u8; 32];
        let other = [91u8; 32];
        let fields = named(vec![
            ("account", bytes32_val(account)),
            ("who", bytes32_val(account)),
            ("stash", bytes32_val(account)),
            ("staker", bytes32_val(account)),
            ("real", bytes32_val(account)),
            ("proxy", bytes32_val(other)),
            ("member", bytes32_val(account)),
            ("sub", bytes32_val(account)),
            ("main", bytes32_val(other)),
            ("lost_account", bytes32_val(account)),
            ("rescuer_account", bytes32_val(other)),
            ("beneficiary", bytes32_val(other)),
            ("index", u128_val(4)),
            ("proposal_index", u128_val(5)),
            ("registrar_index", u128_val(6)),
            ("pool_id", u128_val(7)),
            ("era", u128_val(8)),
            ("bounty_id", u128_val(9)),
        ]);

        for event in ["DustLost", "BalanceSet", "Deposit"] {
            assert_eq!(
                index_balances(event, &fields),
                vec![key_bytes32("account_id", account)]
            );
        }
        assert_eq!(
            index_staking("Rewarded", &fields),
            vec![key_bytes32("account_id", account)]
        );
        assert_eq!(
            index_staking("Slashed", &fields),
            vec![key_bytes32("account_id", account)]
        );
        assert_eq!(
            index_staking("OldSlashingReportDiscarded", &unnamed(vec![u128_val(6)])),
            vec![key_u32("session_index", 6)]
        );
        assert_eq!(
            index_indices("IndexFreed", &fields),
            vec![key_u32("account_index", 4)]
        );
        assert!(index_indices("IndexFrozen", &fields).contains(&key_bytes32("account_id", account)));
        let treasury_fallback_fields = named(vec![
            ("proposal_index", u128_val(5)),
            ("beneficiary", bytes32_val(other)),
        ]);
        assert!(index_treasury("Awarded", &treasury_fallback_fields)
            .contains(&key_bytes32("account_id", other)));
        assert_eq!(
            index_treasury("Deposit", &fields),
            vec![key_bytes32("account_id", account)]
        );
        assert_eq!(
            index_bounties("CuratorUnassigned", &fields),
            vec![key_u32("bounty_index", 9)]
        );
        assert!(index_proxy("Announced", &fields).contains(&key_bytes32("account_id", other)));
        assert!(index_proxy("ProxyExecuted", &fields).is_empty());
        assert_eq!(
            index_election_provider_multi_phase("Rewarded", &fields),
            vec![key_bytes32("account_id", account)]
        );
        assert_eq!(
            index_bags_list("Rebagged", &fields),
            vec![key_bytes32("account_id", account)]
        );
        assert!(index_nomination_pools("Bonded", &fields).contains(&key_u32("pool_id", 7)));
        assert!(index_nomination_pools("PaidOut", &fields).contains(&key_u32("pool_id", 7)));
        assert!(index_nomination_pools("Withdrawn", &fields).contains(&key_u32("pool_id", 7)));
        assert!(index_nomination_pools("MemberRemoved", &fields)
            .contains(&key_bytes32("account_id", account)));
        assert!(index_nomination_pools("UnbondingPoolSlashed", &fields)
            .contains(&key_u32("era_index", 8)));
        assert!(
            index_identity("JudgementRequested", &fields).contains(&key_u32("registrar_index", 6))
        );
        assert_eq!(
            index_identity("RegistrarAdded", &fields),
            vec![key_u32("registrar_index", 6)]
        );
        assert!(
            index_identity("SubIdentityAdded", &fields).contains(&key_bytes32("account_id", other))
        );
        assert!(index_recovery("RecoveryCreated", &fields)
            .contains(&key_bytes32("account_id", account)));
        assert!(index_recovery("RecoveryInitiated", &fields)
            .contains(&key_bytes32("account_id", other)));
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
            "Identity",
            "Recovery",
            "Sudo",
            "StateTrieMigration",
            "ParaInclusion",
            "Paras",
            "Hrmp",
            "ParasDisputes",
            "OnDemand",
            "OnDemandAssignmentProvider",
            "Registrar",
            "Slots",
            "Auctions",
            "Crowdloan",
            "AssignedSlots",
            "Coretime",
            "Xcm",
            "XcmPallet",
        ];
        for name in known {
            assert!(
                index_sdk_pallet(name, "UnknownEvent", &fields).is_some(),
                "dispatch should recognise {name}"
            );
        }
    }

    #[test]
    fn fast_unstake_batch_checked_uses_positional_fallback() {
        let fields = unnamed(vec![composite_value(unnamed(vec![
            u128_val(8),
            u128_val(9),
        ]))]);
        let keys = index_fast_unstake("BatchChecked", &fields);
        assert_eq!(keys, vec![key_u32("era_index", 8), key_u32("era_index", 9)]);
    }
}
