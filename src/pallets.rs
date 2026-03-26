//! Schema-less indexing rules for Polkadot SDK built-in pallets.
//!
//! Each function receives the decoded field values for one event and
//! returns a list of [`Key`]s that should be indexed for that event.

use scale_value::{Composite, Value, ValueDef};

use crate::shared::{Bytes32, Key};

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
        "EraPaid" => positional_u32_key(fields, 0, Key::EraIndex),
        "Rewarded" => named_account_id(fields, "stash"),
        "Slashed" => named_account_id(fields, "staker"),
        "SlashReported" => {
            let mut keys = named_account_id(fields, "validator");
            keys.extend(named_u32_key(fields, "slash_era", Key::EraIndex));
            keys
        }
        "OldSlashingReportDiscarded" => positional_u32_key(fields, 0, Key::SessionIndex),
        "StakersElected" | "ForceEra" | "ChillThreshold" | "StakingElectionFailed" => vec![],
        "Kicked" => {
            let mut keys = named_account_id(fields, "nominator");
            keys.extend(named_account_id(fields, "stash"));
            keys
        }
        "Bonded" | "Unbonded" | "Withdrawn" | "Chilled" => named_account_id(fields, "stash"),
        "PayoutStarted" => {
            let mut keys = named_u32_key(fields, "era_index", Key::EraIndex);
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
        "NewSession" => named_u32_key(fields, "session_index", Key::SessionIndex),
        _ => vec![],
    }
}

/// Index events from `pallet_indices`.
pub fn index_indices(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "IndexAssigned" => {
            let mut keys = named_account_id(fields, "who");
            keys.extend(named_u32_key(fields, "index", Key::AccountIndex));
            keys
        }
        "IndexFreed" => named_u32_key(fields, "index", Key::AccountIndex),
        "IndexFrozen" => {
            let mut keys = named_u32_key(fields, "index", Key::AccountIndex);
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        _ => vec![],
    }
}

/// Index events from `pallet_preimage`.
pub fn index_preimage(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Noted" | "Requested" | "Cleared" => named_bytes32_key(fields, "hash", Key::PreimageHash),
        _ => vec![],
    }
}

/// Index events from `pallet_treasury`.
pub fn index_treasury(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Proposed" | "Rejected" => named_u32_key(fields, "proposal_index", Key::ProposalIndex),
        "Awarded" => {
            let mut keys = named_u32_key(fields, "proposal_index", Key::ProposalIndex);
            let mut account_keys = named_account_id(fields, "account");
            if account_keys.is_empty() {
                account_keys = named_account_id(fields, "beneficiary");
            }
            keys.extend(account_keys);
            keys
        }
        "SpendApproved" => {
            let mut keys = named_u32_key(fields, "proposal_index", Key::ProposalIndex);
            keys.extend(named_account_id(fields, "beneficiary"));
            keys
        }
        "AssetSpendApproved" | "AssetSpendVoided" => {
            named_u32_key(fields, "index", Key::SpendIndex)
        }
        "Paid" | "PaymentFailed" | "SpendProcessed" => {
            named_u32_key(fields, "index", Key::SpendIndex)
        }
        "Deposit" => named_account_id(fields, "who"),
        _ => vec![],
    }
}

/// Index events from `pallet_bounties`.
pub fn index_bounties(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "BountyProposed" | "BountyRejected" | "BountyBecameActive" | "BountyCanceled"
        | "BountyExtended" | "BountyApproved" => named_u32_key(fields, "index", Key::BountyIndex),
        "BountyAwarded" | "BountyClaimed" => {
            let mut keys = named_u32_key(fields, "index", Key::BountyIndex);
            keys.extend(named_account_id(fields, "beneficiary"));
            keys
        }
        "CuratorProposed" | "CuratorAccepted" => {
            let mut keys = named_u32_key(fields, "bounty_id", Key::BountyIndex);
            keys.extend(named_account_id(fields, "curator"));
            keys
        }
        "CuratorUnassigned" => named_u32_key(fields, "bounty_id", Key::BountyIndex),
        _ => vec![],
    }
}

/// Index events from `pallet_child_bounties`.
pub fn index_child_bounties(event_name: &str, fields: &Composite<()>) -> Vec<Key> {
    match event_name {
        "Added" | "Canceled" => {
            let mut keys = named_u32_key(fields, "index", Key::BountyIndex);
            keys.extend(named_u32_key(fields, "child_index", Key::BountyIndex));
            keys
        }
        "Awarded" | "Claimed" => {
            let mut keys = named_u32_key(fields, "index", Key::BountyIndex);
            keys.extend(named_u32_key(fields, "child_index", Key::BountyIndex));
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
    let pool_key = |f: &Composite<()>| -> Vec<Key> { named_u32_key(f, "pool_id", Key::PoolId) };
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
            keys.extend(named_u32_key(fields, "era", Key::EraIndex));
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
            keys.extend(named_u32_key(fields, "era", Key::EraIndex));
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
            .map(|eras| eras.into_iter().map(Key::EraIndex).collect())
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
            let mut keys = named_u32_key(fields, "index", Key::RefIndex);
            keys.extend(named_account_id(fields, "who"));
            keys
        }
        "DepositSlashed" => named_account_id(fields, "who"),
        "Submitted" | "DecisionStarted" | "ConfirmStarted" | "ConfirmAborted" | "Confirmed"
        | "Approved" | "Rejected" | "TimedOut" | "Cancelled" | "Killed" | "MetadataSet"
        | "MetadataCleared" => named_u32_key(fields, "index", Key::RefIndex),
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
            keys.extend(named_u32_key(
                fields,
                "registrar_index",
                Key::RegistrarIndex,
            ));
            keys
        }
        "JudgementGiven" => {
            let mut keys = named_account_id(fields, "target");
            keys.extend(named_u32_key(
                fields,
                "registrar_index",
                Key::RegistrarIndex,
            ));
            keys
        }
        "RegistrarAdded" => named_u32_key(fields, "registrar_index", Key::RegistrarIndex),
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

// ─── Private helpers ──────────────────────────────────────────────────────────

fn named_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_bytes32)
        .map(|b| vec![Key::AccountId(Bytes32(b))])
        .unwrap_or_default()
}

fn named_optional_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_option_bytes32)
        .and_then(|value| value.map(|bytes| vec![Key::AccountId(Bytes32(bytes))]))
        .unwrap_or_default()
}

fn named_option_tuple_second_account_id(fields: &Composite<()>, name: &str) -> Vec<Key> {
    get_field(fields, name)
        .and_then(extract_option_tuple_second_bytes32)
        .and_then(|value| value.map(|bytes| vec![Key::AccountId(Bytes32(bytes))]))
        .unwrap_or_default()
}

fn positional_account_id(fields: &Composite<()>, idx: usize) -> Vec<Key> {
    named_account_id(fields, &idx.to_string())
}

fn named_u32_key<F>(fields: &Composite<()>, name: &str, ctor: F) -> Vec<Key>
where
    F: Fn(u32) -> Key,
{
    get_field(fields, name)
        .and_then(extract_u32)
        .map(|v| vec![ctor(v)])
        .unwrap_or_default()
}

fn positional_u32_key<F>(fields: &Composite<()>, idx: usize, ctor: F) -> Vec<Key>
where
    F: Fn(u32) -> Key,
{
    named_u32_key(fields, &idx.to_string(), ctor)
}

fn named_bytes32_key<F>(fields: &Composite<()>, name: &str, ctor: F) -> Vec<Key>
where
    F: Fn(Bytes32) -> Key,
{
    get_field(fields, name)
        .and_then(extract_bytes32)
        .map(|b| vec![ctor(Bytes32(b))])
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
        _ => return None,
    })
}
