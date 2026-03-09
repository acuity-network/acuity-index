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


