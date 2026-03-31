//! Schema-less indexing rules for Polkadot SDK built-in pallets.
//!
//! Each function receives the decoded field values for one event and
//! returns a list of [`Key`]s that should be indexed for that event.

use scale_value::{Composite, Value, ValueDef};

use crate::shared::{Bytes32, CustomKey, CustomValue, Key, U128Text, U64Text};

pub const SUPPORTED_SDK_PALLETS: &[&str] = &[
    "Alliance",
    "AssetConversion",
    "AssetConversionOps",
    "AssetConversionTxPayment",
    "AssetRate",
    "AssetRewards",
    "AssetTxPayment",
    "Assets",
    "AssetsFreezer",
    "AssetsHolder",
    "AssignedSlots",
    "AtomicSwap",
    "Aura",
    "AuthorityDiscovery",
    "Authorship",
    "Auctions",
    "Babe",
    "BagsList",
    "System",
    "Balances",
    "Beefy",
    "BeefyMmr",
    "Staking",
    "BridgeGrandpa",
    "BridgeMessages",
    "BridgeParachains",
    "BridgeRelayers",
    "Broker",
    "Session",
    "CollatorSelection",
    "Collective",
    "CollectiveContent",
    "Contracts",
    "Coretime",
    "Crowdloan",
    "Indices",
    "CoreFellowship",
    "Dap",
    "DapSatellite",
    "Democracy",
    "Derivatives",
    "DummyDim",
    "ElectionProviderMultiBlock",
    "Preimage",
    "ElectionsPhragmen",
    "Treasury",
    "Grandpa",
    "Hrmp",
    "ImOnline",
    "Bounties",
    "ChildBounties",
    "Lottery",
    "Membership",
    "MessageQueue",
    "MetaTx",
    "Migrations",
    "Mixnet",
    "Mmr",
    "MultiAssetBounties",
    "Vesting",
    "Proxy",
    "Multisig",
    "NftFractionalization",
    "Nfts",
    "Nis",
    "NodeAuthorization",
    "ElectionProviderMultiPhase",
    "VoterList",
    "NominationPools",
    "Oracle",
    "OriginRestriction",
    "PagedList",
    "Parameters",
    "People",
    "ParaInclusion",
    "Paras",
    "ParasDisputes",
    "FastUnstake",
    "ConvictionVoting",
    "RankedCollective",
    "Referenda",
    "Remark",
    "RootOffences",
    "SafeMode",
    "Salary",
    "Scheduler",
    "ScoredPool",
    "Society",
    "TransactionPayment",
    "DelegatedStaking",
    "Identity",
    "OnDemand",
    "OnDemandAssignmentProvider",
    "Recovery",
    "Registrar",
    "SkipFeelessPayment",
    "Slots",
    "StakingAsync",
    "StakingAhClient",
    "Sudo",
    "StateTrieMigration",
    "Statement",
    "Timestamp",
    "Tips",
    "TransactionStorage",
    "TxPause",
    "Uniques",
    "Utility",
    "VerifySignature",
    "Whitelist",
    "Xcm",
    "XcmPallet",
];

pub fn is_supported_sdk_pallet(pallet_name: &str) -> bool {
    SUPPORTED_SDK_PALLETS.contains(&pallet_name)
}

// ─── Value extraction helpers ─────────────────────────────────────────────────

/// Try to extract a `u32` from a scale_value `Value`.
/// Handles `Primitive::U128` (most Substrate u32 fields decode this way)
/// and single-element `Composite::Unnamed` wrappers.
pub fn extract_u32(v: &Value<()>) -> Option<u32> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => u32::try_from(*n).ok(),
                Primitive::I128(n) => u32::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u32(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u32(&fields[0].1)
        }
        _ => None,
    }
}

pub fn extract_u64(v: &Value<()>) -> Option<u64> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => u64::try_from(*n).ok(),
                Primitive::I128(n) => u64::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u64(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u64(&fields[0].1)
        }
        _ => None,
    }
}

pub fn extract_u128(v: &Value<()>) -> Option<u128> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => Some(*n),
                Primitive::I128(n) => u128::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u128(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u128(&fields[0].1)
        }
        _ => None,
    }
}

pub fn extract_string(v: &Value<()>) -> Option<String> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::String(s)) => Some(s.clone()),
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_string(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_string(&fields[0].1)
        }
        _ => None,
    }
}

pub fn extract_bool(v: &Value<()>) -> Option<bool> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::Bool(value)) => Some(*value),
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_bool(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_bool(&fields[0].1)
        }
        _ => None,
    }
}

/// Try to extract a `[u8; 32]` from a scale_value `Value`.
///
/// Substrate encodes fixed-size byte arrays as `Composite::Unnamed` where
/// every element is a `Primitive::U128` holding a single byte.
pub fn extract_bytes32(v: &Value<()>) -> Option<[u8; 32]> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 32 => {
            let mut out = [0u8; 32];
            for (i, f) in fields.iter().enumerate() {
                match &f.value {
                    ValueDef::Primitive(scale_value::Primitive::U128(b)) => {
                        out[i] = u8::try_from(*b).ok()?;
                    }
                    _ => return None,
                }
            }
            Some(out)
        }
        // Some types wrap the array in one more layer.
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_bytes32(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_bytes32(&fields[0].1)
        }
        _ => None,
    }
}

/// Try to extract `Vec<u32>` from a scale_value `Value`.
pub fn extract_vec_u32(v: &Value<()>) -> Option<Vec<u32>> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(fields)) => {
            if fields.is_empty() {
                return Some(vec![]);
            }
            let mut out = Vec::with_capacity(fields.len());
            for field in fields {
                if let Some(value) = extract_u32(field) {
                    out.push(value);
                } else if fields.len() == 1 {
                    return extract_vec_u32(&fields[0]);
                } else {
                    return None;
                }
            }
            Some(out)
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_vec_u32(&fields[0].1)
        }
        _ => None,
    }
}

fn composite_single_value(composite: &Composite<()>) -> Option<&Value<()>> {
    match composite {
        Composite::Unnamed(values) if values.len() == 1 => Some(&values[0]),
        Composite::Named(values) if values.len() == 1 => Some(&values[0].1),
        _ => None,
    }
}

fn extract_option_bytes32(v: &Value<()>) -> Option<Option<[u8; 32]>> {
    if let Some(bytes) = extract_bytes32(v) {
        return Some(Some(bytes));
    }
    match &v.value {
        ValueDef::Variant(var) => match var.name.as_str() {
            "Some" => {
                let payload = composite_single_value(&var.values)?;
                extract_bytes32(payload).map(Some)
            }
            "None" => Some(None),
            _ => None,
        },
        _ => None,
    }
}

fn extract_tuple_second_bytes32(v: &Value<()>) -> Option<[u8; 32]> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(values)) if values.len() >= 2 => {
            extract_bytes32(&values[1])
        }
        ValueDef::Composite(Composite::Named(values)) if values.len() == 1 => {
            extract_tuple_second_bytes32(&values[0].1)
        }
        ValueDef::Composite(Composite::Unnamed(values)) if values.len() == 1 => {
            extract_tuple_second_bytes32(&values[0])
        }
        _ => None,
    }
}

fn extract_option_tuple_second_bytes32(v: &Value<()>) -> Option<Option<[u8; 32]>> {
    if let Some(bytes) = extract_tuple_second_bytes32(v) {
        return Some(Some(bytes));
    }
    match &v.value {
        ValueDef::Variant(var) => match var.name.as_str() {
            "Some" => {
                let payload = composite_single_value(&var.values)?;
                extract_tuple_second_bytes32(payload).map(Some)
            }
            "None" => Some(None),
            _ => None,
        },
        _ => None,
    }
}

/// Look up a field from a `Composite` by name or by positional index string.
pub fn get_field<'a>(composite: &'a Composite<()>, field: &str) -> Option<&'a Value<()>> {
    // Try positional index first.
    if let Ok(idx) = field.parse::<usize>() {
        match composite {
            Composite::Unnamed(fields) => return fields.get(idx),
            Composite::Named(fields) => return fields.get(idx).map(|(_, v)| v),
        }
    }
    // Try by name.
    match composite {
        Composite::Named(fields) => fields.iter().find(|(k, _)| k == field).map(|(_, v)| v),
        Composite::Unnamed(_) => None,
    }
}

// ─── SDK pallet rules ─────────────────────────────────────────────────────────

/// Index events from `pallet_system`.
pub fn index_system(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "NewAccount" | "KilledAccount" => named_account_id(fields, "account"),
        "Remarked" => named_account_id(fields, "sender"),
        _ => vec![],
    }
}

/// Index events from `pallet_balances`.
pub fn index_balances(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Endowed" => named_account_id(fields, "account"),
        "DustLost" => named_account_id(fields, "account"),
        "Transfer" => {
            let mut keys = named_account_id(fields, "from");
            keys.extend(named_account_id(fields, "to"));
            keys
        }
        "BalanceSet" => named_account_id(fields, "who"),
        "Reserved" | "Unreserved" | "Withdraw" | "Slashed" | "Minted" | "Burned" | "Suspended"
        | "Restored" | "Upgraded" | "Locked" | "Unlocked" => named_account_id(fields, "who"),
        "ReserveRepatriated" => {
            let mut keys = named_account_id(fields, "from");
            keys.extend(named_account_id(fields, "to"));
            keys
        }
        "Deposit" | "Frozen" | "Thawed" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_staking`.
pub fn index_staking(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "EraPaid" => positional_u32_key(fields, 0, "era_index"),
        "Rewarded" => named_account_id(fields, "stash"),
        "Slashed" => named_account_id(fields, "staker"),
        "SlashReported" => {
            let mut keys = named_account_id(fields, "validator");
            keys.extend(named_u32_key(fields, "slash_era", "era_index"));
            keys
        }
        "OldSlashingReportDiscarded" => positional_u32_key(fields, 0, "session_index"),
        "StakersElected" | "ForceEra" | "ChillThreshold" | "StakingElectionFailed" => vec![],
        "Kicked" => {
            let mut keys = named_account_id(fields, "nominator");
            keys.extend(named_account_id(fields, "stash"));
            keys
        }
        "Bonded" | "Unbonded" | "Withdrawn" | "Chilled" => named_account_id(fields, "stash"),
        "PayoutStarted" => {
            let mut keys = named_u32_key(fields, "era_index", "era_index");
            keys.extend(named_account_id(fields, "validator_stash"));
            keys
        }
        "ValidatorPrefsSet" => named_account_id(fields, "stash"),
        _ => vec![],
    }
}

/// Index events from `pallet_session`.
pub fn index_session(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "NewSession" => named_u32_key(fields, "session_index", "session_index"),
        _ => vec![],
    }
}

/// Index events from `pallet_indices`.
pub fn index_indices(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "IndexAssigned" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u32_key(fields, "index", "account_index"));
            keys
        }
        "IndexFreed" => named_u32_key(fields, "index", "account_index"),
        "IndexFrozen" => {
            let mut keys = named_u32_key(fields, "index", "account_index");
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_preimage`.
pub fn index_preimage(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Noted" | "Requested" | "Cleared" => named_bytes32_key(fields, "hash", "preimage_hash"),
        _ => vec![],
    }
}

/// Index events from `pallet_treasury`.
pub fn index_treasury(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Proposed" | "Rejected" => named_u32_key(fields, "proposal_index", "proposal_index"),
        "Awarded" => {
            let mut keys = named_u32_key(fields, "proposal_index", "proposal_index");
            let mut account_keys = named_account_id(fields, "account");
            if account_keys.is_empty() {
                account_keys = named_account_id(fields, "beneficiary");
            }
            keys.extend(account_keys);
            keys
        }
        "SpendApproved" => {
            let mut keys = named_u32_key(fields, "proposal_index", "proposal_index");
            keys.extend(named_account_id(fields, "beneficiary"));
            keys
        }
        "AssetSpendApproved" | "AssetSpendVoided" => named_u32_key(fields, "index", "spend_index"),
        "Paid" | "PaymentFailed" | "SpendProcessed" => {
            named_u32_key(fields, "index", "spend_index")
        }
        "Deposit" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_bounties`.
pub fn index_bounties(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "BountyProposed" | "BountyRejected" | "BountyBecameActive" | "BountyCanceled"
        | "BountyExtended" | "BountyApproved" => named_u32_key(fields, "index", "bounty_index"),
        "BountyAwarded" | "BountyClaimed" => {
            let mut keys = named_u32_key(fields, "index", "bounty_index");
            keys.extend(named_account_id(fields, "beneficiary"));
            keys
        }
        "CuratorProposed" | "CuratorAccepted" => {
            let mut keys = named_u32_key(fields, "bounty_id", "bounty_index");
            keys.extend(named_account_id(fields, "curator"));
            keys
        }
        "CuratorUnassigned" => named_u32_key(fields, "bounty_id", "bounty_index"),
        _ => vec![],
    }
}

/// Index events from `pallet_child_bounties`.
pub fn index_child_bounties(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Added" | "Canceled" => {
            let mut keys = named_u32_key(fields, "index", "bounty_index");
            keys.extend(named_u32_key(fields, "child_index", "bounty_index"));
            keys
        }
        "Awarded" | "Claimed" => {
            let mut keys = named_u32_key(fields, "index", "bounty_index");
            keys.extend(named_u32_key(fields, "child_index", "bounty_index"));
            keys.extend(named_account_id(fields, "beneficiary"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_vesting`.
pub fn index_vesting(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "VestingUpdated" | "VestingCompleted" => named_account_id(fields, "account"),
        _ => vec![],
    }
}

/// Index events from `pallet_proxy`.
pub fn index_proxy(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "ProxyAdded" | "ProxyRemoved" => {
            let mut keys = named_account_id(fields, "delegator");
            keys.extend(named_account_id(fields, "delegatee"));
            keys
        }
        "ProxyExecuted" => vec![],
        "PureCreated" => {
            let mut keys = named_account_id(fields, "pure");
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        "Announced" => {
            let mut keys = named_account_id(fields, "real");
            keys.extend(named_account_id(fields, "proxy"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_multisig`.
pub fn index_multisig(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "NewMultisig" | "MultisigApproval" | "MultisigExecuted" => {
            let mut keys = named_account_id(fields, "approving");
            keys.extend(named_account_id(fields, "multisig"));
            keys
        }
        "MultisigCancelled" => {
            let mut keys = named_account_id(fields, "cancelling");
            keys.extend(named_account_id(fields, "multisig"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_election_provider_multi_phase`.
pub fn index_election_provider_multi_phase(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "SolutionStored" => named_optional_account_id(fields, "origin"),
        "Rewarded" | "Slashed" => named_account_id(fields, "account"),
        _ => vec![],
    }
}

/// Index events from `pallet_bags_list` (VoterList).
pub fn index_bags_list(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Rebagged" => named_account_id(fields, "who"),
        "ScoreUpdated" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_nomination_pools`.
pub fn index_nomination_pools(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    let pool_key = |f: &Composite<()>| -> Vec<Key> { named_u32_key(f, "pool_id", "pool_id") };
    match event_name {
        "Created" => {
            let mut keys = named_account_id(fields, "depositor");
            keys.extend(pool_key(fields));
            keys
        }
        "Bonded" => {
            let mut keys = named_account_id(fields, "member");
            keys.extend(pool_key(fields));
            keys
        }
        "PaidOut" => {
            let mut keys = named_account_id(fields, "member");
            keys.extend(pool_key(fields));
            keys
        }
        "Unbonded" => {
            let mut keys = named_account_id(fields, "member");
            keys.extend(pool_key(fields));
            keys.extend(named_u32_key(fields, "era", "era_index"));
            keys
        }
        "Withdrawn" => {
            let mut keys = named_account_id(fields, "member");
            keys.extend(pool_key(fields));
            keys
        }
        "MemberRemoved" => {
            let mut keys = pool_key(fields);
            keys.extend(named_account_id(fields, "member"));
            keys
        }
        "RolesUpdated" => {
            let mut keys = named_optional_account_id(fields, "root");
            keys.extend(named_optional_account_id(fields, "bouncer"));
            keys.extend(named_optional_account_id(fields, "nominator"));
            keys
        }
        "UnbondingPoolSlashed" => {
            let mut keys = pool_key(fields);
            keys.extend(named_u32_key(fields, "era", "era_index"));
            keys
        }
        "PoolCommissionUpdated" => {
            let mut keys = pool_key(fields);
            keys.extend(named_option_tuple_second_account_id(fields, "current"));
            keys
        }
        "Destroyed"
        | "StateChanged"
        | "PoolSlashed"
        | "PoolMaxCommissionUpdated"
        | "PoolCommissionChangeRateUpdated"
        | "PoolCommissionClaimPermissionUpdated"
        | "PoolCommissionClaimed"
        | "MinBalanceDeficitAdjusted"
        | "MinBalanceExcessAdjusted" => pool_key(fields),
        _ => vec![],
    }
}

/// Index events from `pallet_fast_unstake`.
pub fn index_fast_unstake(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Unstaked" | "Slashed" => named_account_id(fields, "stash"),
        "BatchChecked" => get_field(fields, "eras")
            .or_else(|| get_field(fields, "0"))
            .and_then(extract_vec_u32)
            .map(|eras| {
                eras.into_iter()
                    .map(|era| custom_u32_key("era_index", era))
                    .collect()
            })
            .unwrap_or_default(),
        _ => vec![],
    }
}

/// Index events from `pallet_conviction_voting`.
pub fn index_conviction_voting(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Delegated" => {
            let mut keys = positional_account_id(fields, 0);
            keys.extend(positional_account_id(fields, 1));
            keys
        }
        "Undelegated" => positional_account_id(fields, 0),
        "Voted" | "VoteRemoved" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_referenda`.
pub fn index_referenda(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "DecisionDepositPlaced" | "DecisionDepositRefunded" | "SubmissionDepositRefunded" => {
            let mut keys = named_u32_key(fields, "index", "ref_index");
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        "DepositSlashed" => named_account_id(fields, "who"),
        "Submitted" | "DecisionStarted" | "ConfirmStarted" | "ConfirmAborted" | "Confirmed"
        | "Approved" | "Rejected" | "TimedOut" | "Cancelled" | "Killed" | "MetadataSet"
        | "MetadataCleared" => named_u32_key(fields, "index", "ref_index"),
        _ => vec![],
    }
}

/// Index events from `pallet_transaction_payment`.
pub fn index_transaction_payment(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "TransactionFeePaid" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_delegated_staking`.
pub fn index_delegated_staking(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Delegated" | "Released" | "Slashed" | "MigratedDelegation" | "Withdrawn" => {
            let mut keys = named_account_id(fields, "agent");
            keys.extend(named_account_id(fields, "delegator"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_identity`.
pub fn index_identity(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "IdentitySet" | "IdentityCleared" | "IdentityKilled" => named_account_id(fields, "who"),
        "JudgementRequested" | "JudgementUnrequested" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u32_key(fields, "registrar_index", "registrar_index"));
            keys
        }
        "JudgementGiven" => {
            let mut keys = named_account_id(fields, "target");
            keys.extend(named_u32_key(fields, "registrar_index", "registrar_index"));
            keys
        }
        "RegistrarAdded" => named_u32_key(fields, "registrar_index", "registrar_index"),
        "SubIdentityAdded" | "SubIdentityRemoved" | "SubIdentityRevoked" => {
            let mut keys = named_account_id(fields, "sub");
            keys.extend(named_account_id(fields, "main"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_recovery`.
pub fn index_recovery(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "RecoveryCreated" | "RecoveryRemoved" => named_account_id(fields, "account")
            .into_iter()
            .chain(named_account_id(fields, "lost_account"))
            .collect(),
        "RecoveryInitiated" | "RecoveryClosed" | "AccountRecovered" => {
            let mut keys = named_account_id(fields, "lost_account");
            keys.extend(named_account_id(fields, "rescuer_account"));
            keys
        }
        "RecoveryVouched" => {
            let mut keys = named_account_id(fields, "lost_account");
            keys.extend(named_account_id(fields, "rescuer_account"));
            keys.extend(named_account_id(fields, "sender"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_sudo`.
pub fn index_sudo(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "KeyChanged" => {
            let mut keys = named_account_id(fields, "new");
            keys.extend(named_optional_account_id(fields, "old"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_state_trie_migration`.
pub fn index_state_trie_migration(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Slashed" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `ParaInclusion`.
pub fn index_para_inclusion(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "CandidateBacked" | "CandidateIncluded" => {
            let mut keys = positional_u32_key(fields, 2, "core_index");
            keys.extend(positional_u32_key(fields, 3, "group_index"));
            keys
        }
        "CandidateTimedOut" => positional_u32_key(fields, 2, "core_index"),
        "UpwardMessagesReceived" => {
            let mut keys = named_account_id(fields, "from");
            keys.extend(named_u32_key(fields, "count", "count"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Paras`.
pub fn index_paras(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "CurrentCodeUpdated" | "CurrentHeadUpdated" | "CodeUpgradeScheduled" | "NewHeadNoted" => {
            positional_bytes32_key(fields, 0, "id")
        }
        "ActionQueued" => {
            let mut keys = positional_bytes32_key(fields, 0, "id");
            keys.extend(positional_u32_key(fields, 1, "session_index"));
            keys
        }
        "PvfCheckStarted" | "PvfCheckAccepted" | "PvfCheckRejected" => {
            let mut keys = positional_bytes32_key(fields, 0, "validation_code_hash");
            keys.extend(positional_bytes32_key(fields, 1, "id"));
            keys
        }
        "UpgradeCooldownRemoved" => named_u32_key(fields, "para_id", "para_id"),
        "CodeAuthorized" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_bytes32_key(fields, "code_hash", "code_hash"));
            keys.extend(named_u32_key(fields, "expire_at", "expire_at"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Hrmp`.
pub fn index_hrmp(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "OpenChannelRequested" | "HrmpChannelForceOpened" | "HrmpSystemChannelOpened" => {
            let mut keys = named_account_id(fields, "sender");
            keys.extend(named_u32_key(fields, "recipient", "recipient"));
            keys.extend(named_u32_key(
                fields,
                "proposed_max_capacity",
                "proposed_max_capacity",
            ));
            keys.extend(named_u32_key(
                fields,
                "proposed_max_message_size",
                "proposed_max_message_size",
            ));
            keys
        }
        "OpenChannelAccepted" | "OpenChannelDepositsUpdated" => {
            let mut keys = named_account_id(fields, "sender");
            keys.extend(named_u32_key(fields, "recipient", "recipient"));
            keys
        }
        "OpenChannelCanceled" | "ChannelClosed" => {
            named_u32_key(fields, "by_parachain", "by_parachain")
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `ParasDisputes`.
pub fn index_paras_disputes(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "DisputeInitiated" | "DisputeConcluded" => {
            positional_bytes32_key(fields, 0, "candidate_hash")
        }
        "Revert" => positional_u32_key(fields, 0, "block_number_for"),
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `OnDemand` and `OnDemandAssignmentProvider`.
pub fn index_on_demand(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "OnDemandOrderPlaced" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_u128_key(fields, "spot_price", "spot_price"));
            keys.extend(named_account_id(fields, "ordered_by"));
            keys
        }
        "SpotPriceSet" => named_u128_key(fields, "spot_price", "spot_price"),
        "AccountCredited" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u128_key(fields, "amount", "amount"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Registrar`.
pub fn index_registrar(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Registered" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_account_id(fields, "manager"));
            keys
        }
        "Reserved" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        "Deregistered" => named_u32_key(fields, "para_id", "para_id"),
        "Swapped" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_u32_key(fields, "other_id", "other_id"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Slots`.
pub fn index_slots(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "NewLeasePeriod" => named_u32_key(fields, "lease_period", "lease_period"),
        "Leased" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_account_id(fields, "leaser"));
            keys.extend(named_u32_key(fields, "period_begin", "period_begin"));
            keys.extend(named_u32_key(fields, "period_count", "period_count"));
            keys.extend(named_u128_key(fields, "extra_reserved", "extra_reserved"));
            keys.extend(named_u128_key(fields, "total_amount", "total_amount"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Auctions`.
pub fn index_auctions(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "AuctionStarted" => {
            let mut keys = named_u32_key(fields, "auction_index", "auction_index");
            keys.extend(named_u32_key(fields, "lease_period", "lease_period"));
            keys.extend(named_u32_key(fields, "ending", "ending"));
            keys
        }
        "AuctionClosed" => named_u32_key(fields, "auction_index", "auction_index"),
        "Reserved" => {
            let mut keys = named_account_id(fields, "bidder");
            keys.extend(named_u128_key(fields, "extra_reserved", "extra_reserved"));
            keys.extend(named_u128_key(fields, "total_amount", "total_amount"));
            keys
        }
        "Unreserved" => {
            let mut keys = named_account_id(fields, "bidder");
            keys.extend(named_u128_key(fields, "amount", "amount"));
            keys
        }
        "ReserveConfiscated" => {
            let mut keys = named_u32_key(fields, "para_id", "para_id");
            keys.extend(named_account_id(fields, "leaser"));
            keys.extend(named_u128_key(fields, "amount", "amount"));
            keys
        }
        "BidAccepted" => {
            let mut keys = named_account_id(fields, "bidder");
            keys.extend(named_u32_key(fields, "para_id", "para_id"));
            keys.extend(named_u128_key(fields, "amount", "amount"));
            keys.extend(named_u32_key(fields, "first_slot", "first_slot"));
            keys.extend(named_u32_key(fields, "last_slot", "last_slot"));
            keys
        }
        "WinningOffset" => {
            let mut keys = named_u32_key(fields, "auction_index", "auction_index");
            keys.extend(named_u32_key(fields, "block_number", "block_number"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Crowdloan`.
pub fn index_crowdloan(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Created" | "PartiallyRefunded" | "AllRefunded" | "Dissolved" | "HandleBidResult"
        | "Edited" | "AddedToNewRaise" => named_u32_key(fields, "para_id", "para_id"),
        "Contributed" | "Withdrew" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u32_key(fields, "fund_index", "fund_index"));
            keys.extend(named_u128_key(fields, "amount", "amount"));
            keys
        }
        "MemoUpdated" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u32_key(fields, "para_id", "para_id"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `AssignedSlots`.
pub fn index_assigned_slots(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "PermanentSlotAssigned" | "TemporarySlotAssigned" => {
            positional_bytes32_key(fields, 0, "id")
        }
        _ => vec![],
    }
}

/// Index events from Polkadot relay-chain `Coretime`.
pub fn index_coretime(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "RevenueInfoRequested" => named_u32_key(fields, "when", "when"),
        "CoreAssigned" => named_u32_key(fields, "core", "core"),
        _ => vec![],
    }
}

/// Index events from `pallet_xcm` runtime aliases like `XcmPallet`.
pub fn index_xcm(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Sent"
        | "SendFailed"
        | "ProcessXcmError"
        | "VersionNotifyStarted"
        | "VersionNotifyRequested"
        | "VersionNotifyUnrequested" => named_bytes32_key(fields, "message_id", "message_id"),
        "UnexpectedResponse"
        | "ResponseReady"
        | "Notified"
        | "NotifyOverweight"
        | "NotifyDispatchError"
        | "NotifyDecodeFailed"
        | "InvalidResponder"
        | "InvalidResponderVersion"
        | "ResponseTaken"
        | "NotifyTargetSendFail"
        | "NotifyTargetMigrationFail"
        | "InvalidQuerierVersion"
        | "InvalidQuerier" => named_u64_key(fields, "query_id", "query_id"),
        "AssetsTrapped" | "AssetsClaimed" => named_bytes32_key(fields, "hash", "hash"),
        "VersionChangeNotified" => {
            let mut keys = named_u32_key(fields, "result", "result");
            keys.extend(named_bytes32_key(fields, "message_id", "message_id"));
            keys
        }
        "SupportedVersionChanged" | "VersionMigrationFinished" => {
            named_u32_key(fields, "version", "version")
        }
        _ => vec![],
    }
}

/// Index events from staking async client runtime aliases like `StakingAhClient`.
pub fn index_staking_ah_client(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "ValidatorSetReceived" => {
            let mut keys = named_u32_key(fields, "id", "id");
            keys.extend(named_u32_key(
                fields,
                "new_validator_set_count",
                "new_validator_set_count",
            ));
            keys.extend(named_bool_key(fields, "leftover", "leftover"));
            keys
        }
        "SessionKeysUpdated" | "SessionKeysUpdateFailed" => named_account_id(fields, "stash"),
        _ => vec![],
    }
}

// ─── Private helpers ──────────────────────────────────────────────────────────

fn custom_u32_key(name: &str, value: u32) -> Key {
    Key::Custom(CustomKey {
        name: name.to_owned(),
        value: CustomValue::U32(value),
    })
}

fn custom_u128_key(name: &str, value: u128) -> Key {
    Key::Custom(CustomKey {
        name: name.to_owned(),
        value: CustomValue::U128(U128Text(value)),
    })
}

fn custom_u64_key(name: &str, value: u64) -> Key {
    Key::Custom(CustomKey {
        name: name.to_owned(),
        value: CustomValue::U64(U64Text(value)),
    })
}

fn custom_bool_key(name: &str, value: bool) -> Key {
    Key::Custom(CustomKey {
        name: name.to_owned(),
        value: CustomValue::Bool(value),
    })
}

fn custom_bytes32_key(name: &str, value: Bytes32) -> Key {
    Key::Custom(CustomKey {
        name: name.to_owned(),
        value: CustomValue::Bytes32(value),
    })
}

fn named_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_bytes32)
        .map(|b| vec![custom_bytes32_key("account_id", Bytes32(b))])
        .unwrap_or_default()
}

fn named_optional_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_option_bytes32)
        .and_then(|value| value.map(|bytes| vec![custom_bytes32_key("account_id", Bytes32(bytes))]))
        .unwrap_or_default()
}

fn named_option_tuple_second_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_option_tuple_second_bytes32)
        .and_then(|value| value.map(|bytes| vec![custom_bytes32_key("account_id", Bytes32(bytes))]))
        .unwrap_or_default()
}

fn positional_account_id(fields: &Composite<()>, idx: usize) -> Vec<Key> {
    named_account_id(fields, &idx.to_string())
}

fn named_u32_key(fields: &Composite<()>, field_name: &str, key_name: &str) -> Vec<Key> {
    get_field(fields, field_name)
        .and_then(extract_u32)
        .map(|v| vec![custom_u32_key(key_name, v)])
        .unwrap_or_default()
}

fn named_u128_key(fields: &Composite<()>, field_name: &str, key_name: &str) -> Vec<Key> {
    get_field(fields, field_name)
        .and_then(extract_u128)
        .map(|v| vec![custom_u128_key(key_name, v)])
        .unwrap_or_default()
}

fn named_u64_key(fields: &Composite<()>, field_name: &str, key_name: &str) -> Vec<Key> {
    get_field(fields, field_name)
        .and_then(extract_u64)
        .map(|v| vec![custom_u64_key(key_name, v)])
        .unwrap_or_default()
}

fn named_bool_key(fields: &Composite<()>, field_name: &str, key_name: &str) -> Vec<Key> {
    get_field(fields, field_name)
        .and_then(extract_bool)
        .map(|v| vec![custom_bool_key(key_name, v)])
        .unwrap_or_default()
}

fn positional_u32_key(fields: &Composite<()>, idx: usize, key_name: &str) -> Vec<Key> {
    named_u32_key(fields, &idx.to_string(), key_name)
}

fn positional_bytes32_key(fields: &Composite<()>, idx: usize, key_name: &str) -> Vec<Key> {
    named_bytes32_key(fields, &idx.to_string(), key_name)
}

fn named_bytes32_key(fields: &Composite<()>, field_name: &str, key_name: &str) -> Vec<Key> {
    get_field(fields, field_name)
        .and_then(extract_bytes32)
        .map(|b| vec![custom_bytes32_key(key_name, Bytes32(b))])
        .unwrap_or_default()
}

// ─── Dispatch ────────────────────────────────────────────────────────────────

/// Dispatch a built-in SDK pallet event to the appropriate indexing function.
/// Returns `None` if the pallet name is not recognised as a built-in.
pub fn index_sdk_pallet(
    pallet_name: &str,
    event_name: &str,
    fields: &Composite<()>,
) -> Option<Vec<Key>> {
    Some(match pallet_name {
        "System" => index_system(event_name, fields),
        "Balances" => index_balances(event_name, fields),
        "Staking" => index_staking(event_name, fields),
        "Session" => index_session(event_name, fields),
        "Indices" => index_indices(event_name, fields),
        "Preimage" => index_preimage(event_name, fields),
        "Treasury" => index_treasury(event_name, fields),
        "Bounties" => index_bounties(event_name, fields),
        "ChildBounties" => index_child_bounties(event_name, fields),
        "Vesting" => index_vesting(event_name, fields),
        "Proxy" => index_proxy(event_name, fields),
        "Multisig" => index_multisig(event_name, fields),
        "ElectionProviderMultiPhase" => index_election_provider_multi_phase(event_name, fields),
        "VoterList" => index_bags_list(event_name, fields),
        "NominationPools" => index_nomination_pools(event_name, fields),
        "FastUnstake" => index_fast_unstake(event_name, fields),
        "ConvictionVoting" => index_conviction_voting(event_name, fields),
        "Referenda" => index_referenda(event_name, fields),
        "TransactionPayment" => index_transaction_payment(event_name, fields),
        "DelegatedStaking" => index_delegated_staking(event_name, fields),
        "Identity" => index_identity(event_name, fields),
        "Recovery" => index_recovery(event_name, fields),
        "Sudo" => index_sudo(event_name, fields),
        "StateTrieMigration" => index_state_trie_migration(event_name, fields),
        "ParaInclusion" => index_para_inclusion(event_name, fields),
        "Paras" => index_paras(event_name, fields),
        "Hrmp" => index_hrmp(event_name, fields),
        "ParasDisputes" => index_paras_disputes(event_name, fields),
        "OnDemand" | "OnDemandAssignmentProvider" => index_on_demand(event_name, fields),
        "Registrar" => index_registrar(event_name, fields),
        "Slots" => index_slots(event_name, fields),
        "Auctions" => index_auctions(event_name, fields),
        "Crowdloan" => index_crowdloan(event_name, fields),
        "AssignedSlots" => index_assigned_slots(event_name, fields),
        "Coretime" => index_coretime(event_name, fields),
        "StakingAhClient" => index_staking_ah_client(event_name, fields),
        "Xcm" | "XcmPallet" => index_xcm(event_name, fields),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use scale_value::{Primitive, Variant};

    fn u128_value(value: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(value)),
            context: (),
        }
    }

    fn bytes32_value(byte: u8) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![u128_value(byte.into()); 32])),
            context: (),
        }
    }

    fn variant_value(name: &str, values: Composite<()>) -> Value<()> {
        Value {
            value: ValueDef::Variant(Variant {
                name: name.into(),
                values,
            }),
            context: (),
        }
    }

    #[test]
    fn helper_extractors_cover_remaining_edge_cases() {
        let named_u64 = Value {
            value: ValueDef::Composite(Composite::Named(vec![("inner".into(), u128_value(9))])),
            context: (),
        };
        let invalid = Value {
            value: ValueDef::Primitive(Primitive::Bool(true)),
            context: (),
        };
        let invalid_bytes = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![
                Value {
                    value: ValueDef::Primitive(Primitive::Bool(true)),
                    context: (),
                };
                32
            ])),
            context: (),
        };

        assert_eq!(extract_u64(&named_u64), Some(9));
        assert_eq!(extract_u64(&invalid), None);
        assert_eq!(extract_u128(&invalid), None);
        assert_eq!(extract_string(&invalid), None);
        assert_eq!(
            extract_bool(&Value {
                value: ValueDef::Primitive(Primitive::String("x".into())),
                context: ()
            }),
            None
        );
        assert_eq!(extract_bytes32(&invalid_bytes), None);
        assert_eq!(
            extract_vec_u32(&Value {
                value: ValueDef::Composite(Composite::Unnamed(vec![])),
                context: ()
            }),
            Some(vec![])
        );
        assert_eq!(
            extract_vec_u32(&Value {
                value: ValueDef::Composite(Composite::Named(vec![(
                    "inner".into(),
                    Value {
                        value: ValueDef::Composite(Composite::Unnamed(vec![
                            u128_value(1),
                            u128_value(2)
                        ])),
                        context: ()
                    }
                )])),
                context: ()
            }),
            Some(vec![1, 2])
        );
        assert_eq!(
            extract_vec_u32(&Value {
                value: ValueDef::Composite(Composite::Unnamed(vec![
                    Value {
                        value: ValueDef::Primitive(Primitive::Bool(true)),
                        context: ()
                    },
                    Value {
                        value: ValueDef::Primitive(Primitive::Bool(false)),
                        context: ()
                    }
                ])),
                context: ()
            }),
            None
        );
    }

    #[test]
    fn option_extractors_cover_variant_and_wrapper_paths() {
        let direct = bytes32_value(0xAB);
        let some = variant_value("Some", Composite::Unnamed(vec![bytes32_value(0xCD)]));
        let none = variant_value("None", Composite::Unnamed(vec![]));
        let unknown = variant_value("Unknown", Composite::Unnamed(vec![]));
        let tuple = Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![
                u128_value(1),
                bytes32_value(0xEF),
            ])),
            context: (),
        };
        let wrapped_tuple = Value {
            value: ValueDef::Composite(Composite::Named(vec![(
                "current".into(),
                Value {
                    value: ValueDef::Composite(Composite::Unnamed(vec![tuple.clone()])),
                    context: (),
                },
            )])),
            context: (),
        };

        assert_eq!(extract_option_bytes32(&direct), Some(Some([0xAB; 32])));
        assert_eq!(extract_option_bytes32(&some), Some(Some([0xCD; 32])));
        assert_eq!(extract_option_bytes32(&none), Some(None));
        assert_eq!(extract_option_bytes32(&unknown), None);
        assert_eq!(extract_tuple_second_bytes32(&tuple), Some([0xEF; 32]));
        assert_eq!(
            extract_tuple_second_bytes32(&wrapped_tuple),
            Some([0xEF; 32])
        );
        assert_eq!(
            extract_option_tuple_second_bytes32(&variant_value(
                "Some",
                Composite::Unnamed(vec![tuple])
            )),
            Some(Some([0xEF; 32]))
        );
        assert_eq!(
            extract_option_tuple_second_bytes32(&variant_value("None", Composite::Unnamed(vec![]))),
            Some(None)
        );
    }

    #[test]
    fn additional_public_event_branches_are_covered() {
        let who = [0x11; 32];
        let main = [0x22; 32];
        let fields = Composite::Named(vec![
            ("who".into(), bytes32_value(0x11)),
            ("sub".into(), bytes32_value(0x11)),
            ("main".into(), bytes32_value(0x22)),
            ("stash".into(), bytes32_value(0x11)),
            ("registrar_index".into(), u128_value(7)),
        ]);

        assert_eq!(
            index_staking("ValidatorPrefsSet", &fields),
            vec![custom_bytes32_key("account_id", Bytes32(who))]
        );
        assert_eq!(
            index_bags_list("ScoreUpdated", &fields),
            vec![custom_bytes32_key("account_id", Bytes32(who))]
        );
        assert_eq!(
            index_identity("IdentitySet", &fields),
            vec![custom_bytes32_key("account_id", Bytes32(who))]
        );
        assert!(index_identity("SubIdentityRemoved", &fields)
            .contains(&custom_bytes32_key("account_id", Bytes32(main))));
    }
}
