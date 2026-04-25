use ahash::{AHashMap, AHashSet};
use scale_value::Composite;
use serde_json::json;
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    error::{BackendError, OnlineClientAtBlockError, RpcError},
    events::{Event, Events},
    rpcs::methods::legacy::LegacyRpcMethods,
};

use crate::shared::{DecodedEvent, EventRef, IndexError, internal_error};

pub(crate) struct FetchedBlock {
    pub(crate) block_number: u32,
    pub(crate) spec_version: u32,
    pub(crate) events: Events<PolkadotConfig>,
}

pub(crate) async fn fetch_block_events(
    api: &OnlineClient<PolkadotConfig>,
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    block_number: u32,
) -> Result<FetchedBlock, IndexError> {
    let block_hash = rpc
        .chain_get_block_hash(Some(block_number.into()))
        .await?
        .ok_or(IndexError::BlockNotFound(block_number))?;

    let at_block = api.at_block(block_hash).await.map_err(|err| {
        if is_state_pruned_error(&err) {
            IndexError::StatePruningMisconfigured { block_number }
        } else {
            err.into()
        }
    })?;
    let spec_version = at_block.spec_version();
    let events = at_block.events().fetch().await?;

    Ok(FetchedBlock {
        block_number,
        spec_version,
        events,
    })
}

pub(crate) async fn hydrate_event_refs(
    api: &OnlineClient<PolkadotConfig>,
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    event_refs: &[EventRef],
) -> Result<Vec<DecodedEvent>, IndexError> {
    if event_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut requested_by_block = AHashMap::<u32, AHashSet<u32>>::new();
    for event_ref in event_refs {
        requested_by_block
            .entry(event_ref.block_number)
            .or_default()
            .insert(event_ref.event_index);
    }

    let mut decoded_by_ref = AHashMap::<(u32, u32), DecodedEvent>::new();
    for block_number in requested_by_block.keys().copied() {
        let fetched = fetch_block_events(api, rpc, block_number).await?;
        decode_requested_block_events(&fetched, requested_by_block.get(&block_number).unwrap(), &mut decoded_by_ref)?;
    }

    let mut decoded_events = Vec::with_capacity(event_refs.len());
    for event_ref in event_refs {
        let decoded_event = decoded_by_ref
            .remove(&(event_ref.block_number, event_ref.event_index))
            .ok_or_else(|| {
                internal_error(format!(
                    "event not found on node for block {} event {}",
                    event_ref.block_number, event_ref.event_index
                ))
            })?;
        decoded_events.push(decoded_event);
    }

    Ok(decoded_events)
}

fn decode_requested_block_events(
    fetched: &FetchedBlock,
    requested_indexes: &AHashSet<u32>,
    decoded_by_ref: &mut AHashMap<(u32, u32), DecodedEvent>,
) -> Result<(), IndexError> {
    if requested_indexes.is_empty() {
        return Ok(());
    }

    for event_result in fetched.events.iter() {
        let event = event_result?;
        let event_index = event.index();
        if !requested_indexes.contains(&event_index) {
            continue;
        }

        let decoded_event = decode_event_details(fetched.block_number, fetched.spec_version, &event)?;
        decoded_by_ref.insert((fetched.block_number, event_index), decoded_event);
    }

    Ok(())
}

fn decode_event_details(
    block_number: u32,
    spec_version: u32,
    event: &Event<PolkadotConfig>,
) -> Result<DecodedEvent, IndexError> {
    let event_index = event.index();
    let fields: Composite<()> = event.decode_fields_unchecked_as::<Composite<()>>()?;

    Ok(DecodedEvent {
        block_number,
        event_index,
        event: encode_event_value(
            spec_version,
            event.pallet_name(),
            event.event_name(),
            event.pallet_index(),
            event.event_index(),
            event_index,
            &fields,
        ),
    })
}

pub(crate) fn encode_event_value(
    spec_version: u32,
    pallet_name: &str,
    event_name: &str,
    pallet_index: u8,
    variant_index: u8,
    event_index: u32,
    fields: &Composite<()>,
) -> serde_json::Value {
    json!({
        "specVersion": spec_version,
        "palletName": pallet_name,
        "eventName": event_name,
        "palletIndex": pallet_index,
        "variantIndex": variant_index,
        "eventIndex": event_index,
        "fields": crate::indexer::composite_to_json(fields),
    })
}

fn is_state_pruned_error(err: &OnlineClientAtBlockError) -> bool {
    match err {
        OnlineClientAtBlockError::CannotGetSpecVersion {
            reason: BackendError::Rpc(RpcError::ClientError(subxt::rpcs::Error::User(user_err))),
            ..
        } => user_err.code == 4003 && user_err.message.contains("State already discarded"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scale_value::{Primitive, Value, ValueDef};

    #[test]
    fn encode_event_value_preserves_existing_wire_shape() {
        let fields = Composite::Named(vec![(
            "amount".into(),
            Value {
                value: ValueDef::Primitive(Primitive::U128(999)),
                context: (),
            },
        )]);

        assert_eq!(
            encode_event_value(1234, "Balances", "Deposit", 5, 2, 7, &fields),
            json!({
                "specVersion": 1234,
                "palletName": "Balances",
                "eventName": "Deposit",
                "palletIndex": 5,
                "variantIndex": 2,
                "eventIndex": 7,
                "fields": {"amount": "999"},
            })
        );
    }
}
