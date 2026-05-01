use crate::{
    errors::IndexError,
    event_hydration::{fetch_event_block_proofs, hydrate_event_refs},
    protocol::*,
    runtime_state::RuntimeState,
};

use sled::Tree;
use subxt::{
    Metadata, OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::sync::{mpsc, mpsc::Sender, oneshot};
use tracing::error;

use super::{disconnect_error, validation::{clamp_events_limit, validate_key}};

pub(crate) fn enqueue_subscription_message(
    sub_tx: &Sender<SubscriptionMessage>,
    msg: SubscriptionMessage,
) -> Result<(), IndexError> {
    sub_tx.try_send(msg).map_err(|err| match err {
        mpsc::error::TrySendError::Full(_) => disconnect_error("subscription control queue full"),
        mpsc::error::TrySendError::Closed(_) => disconnect_error("subscription dispatcher closed"),
    })
}

pub fn build_index_status_result(span_db: &Tree) -> IndexStatusResult {
    let mut spans = vec![];
    for (key, value) in span_db.into_iter().flatten() {
        let Some(sv) = read_span_db_value(&value) else {
            error!("Skipping malformed span value");
            continue;
        };
        let start: u32 = sv.start.into();
        let Some(end) = decode_u32_key(key.as_ref()) else {
            error!("Skipping malformed span key");
            continue;
        };
        spans.push(Span { start, end });
    }
    IndexStatusResult { spans }
}

pub(crate) async fn build_get_event_metadata_result(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
) -> Result<GetEventMetadataResult, IndexError> {
    let metadata: Metadata = rpc
        .state_get_metadata(None)
        .await?
        .to_frame_metadata()?
        .try_into()?;

    let mut pallets = Vec::new();
    for pallet in metadata.pallets() {
        if let Some(variants) = pallet.event_variants() {
            let mut meta = PalletMeta {
                index: pallet.event_index(),
                name: pallet.name().to_owned(),
                events: vec![],
            };
            for v in variants {
                meta.events.push(EventMeta {
                    index: v.index,
                    name: v.name.clone(),
                });
            }
            pallets.push(meta);
        }
    }
    Ok(GetEventMetadataResult { pallets })
}

pub(crate) async fn build_get_events_result(
    runtime: &RuntimeState,
    trees: &Trees,
    api: &OnlineClient<PolkadotConfig>,
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    key: Key,
    before: Option<EventRef>,
    limit: u16,
    max_events_limit: usize,
) -> Result<GetEventsResult, IndexError> {
    let effective_limit = clamp_events_limit(limit, max_events_limit);
    // Fetch limit + 1 to determine hasMore
    let mut event_refs = key.get_events(
        trees,
        before.as_ref(),
        effective_limit + 1,
    )?;
    let has_more = event_refs.len() > effective_limit;
    if has_more {
        event_refs.truncate(effective_limit);
    }
    let next_cursor = if has_more {
        event_refs.last().cloned()
    } else {
        None
    };

    let decoded_events = hydrate_event_refs(api, rpc, &event_refs).await?;

    // Always attempt proof inclusion
    let proofs = if !runtime.finalized_mode() {
        ProofsResult {
            available: false,
            reason: REASON_PROOFS_UNAVAILABLE.into(),
            message: "Finalized proofs are only available when the indexer is running with finalized indexing.".into(),
            items: vec![],
        }
    } else {
        match fetch_event_block_proofs(rpc, &event_refs).await {
            Ok(proofs) => ProofsResult {
                available: true,
                reason: "included".into(),
                message: "Finalized event proofs included.".into(),
                items: proofs,
            },
            Err(err) => ProofsResult {
                available: false,
                reason: if err.is_recoverable() {
                    "rpc_proof_unavailable"
                } else {
                    REASON_PROOFS_UNAVAILABLE
                }
                .into(),
                message: format!("Unable to include finalized proofs: {err}"),
                items: vec![],
            },
        }
    };

    Ok(GetEventsResult {
        key,
        events: event_refs,
        decoded_events,
        proofs,
        page: PageResult {
            next_cursor,
            has_more,
        },
    })
}

pub(crate) fn process_local_method(
    trees: &Trees,
    id: u64,
    method: &str,
    params: &serde_json::Value,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<JsonRpcNotification>,
) -> Option<Result<JsonRpcResponse, IndexError>> {
    match method {
        "acuity_indexStatus" => {
            let result = build_index_status_result(&trees.span);
            Some(Ok(jsonrpc_success(id, serde_json::to_value(result).unwrap())))
        }
        "acuity_subscribeStatus" => {
            let subscription_id = generate_subscription_id();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            Some(Ok(jsonrpc_success(id, serde_json::json!(subscription_id))))
        }
        "acuity_unsubscribeStatus" => {
            let _params = match serde_json::from_value::<UnsubscribeParams>(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_CURSOR))),
            };
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            Some(Ok(jsonrpc_success(id, serde_json::json!(true))))
        }
        "acuity_getEventMetadata" => return None,
        "acuity_getEvents" => {
            let params: GetEventsParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_KEY))),
            };
            if let Err(err) = validate_key(&params.key) {
                return Some(Ok(jsonrpc_invalid_params(id, err, REASON_INVALID_KEY)));
            }
            return None;
        }
        "acuity_subscribeEvents" => {
            let params: SubscribeEventsParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_KEY))),
            };
            if let Err(err) = validate_key(&params.key) {
                return Some(Ok(jsonrpc_invalid_params(id, err, REASON_INVALID_KEY)));
            }
            let subscription_id = generate_subscription_id();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeEvents {
                    key: params.key.clone(),
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            Some(Ok(jsonrpc_success(id, serde_json::json!(subscription_id))))
        }
        "acuity_unsubscribeEvents" => {
            let _params: UnsubscribeParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_CURSOR))),
            };
            // Unsubscribe by subscription ID is handled via the subscription dispatcher
            return None;
        }
        _ => return None,
    }
}

async fn process_subscription_method(
    id: u64,
    method: &str,
    params: &serde_json::Value,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<JsonRpcNotification>,
) -> Option<Result<JsonRpcResponse, IndexError>> {
    match method {
        "acuity_subscribeStatus" => {
            let (response_tx, response_rx) = oneshot::channel();
            let subscription_id = generate_subscription_id();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: Some(response_tx),
                },
            ) {
                return Some(Err(err));
            }
            match response_rx.await {
                Ok(Ok(())) => Some(Ok(jsonrpc_success(id, serde_json::json!(subscription_id)))),
                Ok(Err(message)) => Some(Ok(jsonrpc_subscription_limit(id, message))),
                Err(_) => {
                    return Some(Err(disconnect_error(
                        "subscription dispatcher dropped response",
                    )));
                }
            }
        }
        "acuity_unsubscribeStatus" => {
            let _params: UnsubscribeParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_CURSOR))),
            };
            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: Some(response_tx),
                },
            ) {
                return Some(Err(err));
            }
            if response_rx.await.is_err() {
                return Some(Err(disconnect_error(
                    "subscription dispatcher dropped response",
                )));
            }
            Some(Ok(jsonrpc_success(id, serde_json::json!(true))))
        }
        "acuity_subscribeEvents" => {
            let params: SubscribeEventsParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_KEY))),
            };
            if let Err(err) = validate_key(&params.key) {
                return Some(Ok(jsonrpc_invalid_params(id, err, REASON_INVALID_KEY)));
            }

            let (response_tx, response_rx) = oneshot::channel();
            let subscription_id = generate_subscription_id();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeEvents {
                    key: params.key.clone(),
                    tx: sub_response_tx.clone(),
                    response_tx: Some(response_tx),
                },
            ) {
                return Some(Err(err));
            }
            match response_rx.await {
                Ok(Ok(())) => Some(Ok(jsonrpc_success(id, serde_json::json!(subscription_id)))),
                Ok(Err(message)) => Some(Ok(jsonrpc_subscription_limit(id, message))),
                Err(_) => {
                    return Some(Err(disconnect_error(
                        "subscription dispatcher dropped response",
                    )));
                }
            }
        }
        "acuity_unsubscribeEvents" => {
            let _params: UnsubscribeParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Some(Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_CURSOR))),
            };
            // For unsubscribe by subscription ID, we send to the dispatcher
            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeEvents {
                    key: Key::Variant(0, 0), // Placeholder - dispatcher uses channel identity
                    tx: sub_response_tx.clone(),
                    response_tx: Some(response_tx),
                },
            ) {
                return Some(Err(err));
            }
            if response_rx.await.is_err() {
                return Some(Err(disconnect_error(
                    "subscription dispatcher dropped response",
                )));
            }
            Some(Ok(jsonrpc_success(id, serde_json::json!(true))))
        }
        _ => None,
    }
}

pub(crate) async fn process_msg(
    runtime: &RuntimeState,
    trees: &Trees,
    request: JsonRpcRequest,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<JsonRpcNotification>,
    max_events_limit: usize,
) -> Result<JsonRpcResponse, IndexError> {
    let id = request.id;
    let method = request.method.as_str();
    let params = &request.params;

    // Try subscription methods first (need oneshot channels)
    if let Some(response) = process_subscription_method(id, method, params, sub_tx, sub_response_tx).await {
        return response;
    }

    // Try local methods (no RPC needed)
    if let Some(response) = process_local_method(trees, id, method, params, sub_tx, sub_response_tx) {
        return response;
    }

    // Check if this is a known method that requires RPC
    let needs_rpc = matches!(method, "acuity_getEventMetadata" | "acuity_getEvents");
    if !needs_rpc {
        return Ok(jsonrpc_method_not_found(id, method));
    }

    // Methods that need RPC
    let Some((api, rpc)) = runtime.clients() else {
        return Ok(jsonrpc_temporarily_unavailable(id));
    };

    let result = match method {
        "acuity_getEventMetadata" => build_get_event_metadata_result(&rpc).await
            .map(|r| jsonrpc_success(id, serde_json::to_value(r).unwrap())),
        "acuity_getEvents" => {
            let params: GetEventsParams = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(err) => return Ok(jsonrpc_invalid_params(id, err.to_string(), REASON_INVALID_KEY)),
            };
            build_get_events_result(
                runtime,
                trees,
                &api,
                &rpc,
                params.key,
                params.before,
                params.limit,
                max_events_limit,
            )
            .await
            .map(|r| jsonrpc_success(id, serde_json::to_value(r).unwrap()))
        }
        _ => unreachable!(), // Already checked for valid methods above
    };

    match result {
        Ok(response) => Ok(response),
        Err(err) if err.is_recoverable() => Ok(jsonrpc_temporarily_unavailable(id)),
        Err(err) => Err(err),
    }
}

/// Generate a unique subscription ID.
pub(crate) fn generate_subscription_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("sub_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::IndexError;
    use crate::protocol::get_events_index;
    use crate::ws_api::tests_support::{
        DEFAULT_WS_CONFIG, disconnected_runtime, temp_trees,
    };
    use tokio::sync::mpsc;
    use zerocopy::IntoBytes;

    #[test]
    fn build_index_status_result_empty() {
        let trees = temp_trees();
        let result = build_index_status_result(&trees.span);
        assert!(result.spans.is_empty());
    }

    #[test]
    fn build_index_status_result_with_spans() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 100u32.into(),
            version: 0u16.into(),
        };
        trees.span.insert(200u32.to_be_bytes(), sv.as_bytes()).unwrap();

        let result = build_index_status_result(&trees.span);
        assert_eq!(result.spans.len(), 1);
        assert_eq!(result.spans[0].start, 100);
        assert_eq!(result.spans[0].end, 200);
    }

    #[test]
    fn get_events_custom_empty_tree() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "para_id".into(),
            value: CustomValue::U32(999),
        });
        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_bytes32_empty_tree() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32([0; 32])),
        });
        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_u32_multiple() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        key.write_db_key(&trees, 10, 0).unwrap();
        key.write_db_key(&trees, 20, 1).unwrap();
        key.write_db_key(&trees, 30, 2).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].block_number, 30);
        assert_eq!(events[2].block_number, 10);
    }

    #[test]
    fn get_events_bytes32_multiple() {
        let trees = temp_trees();
        let b = Bytes32([0x11; 32]);
        let key = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(b),
        });
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    #[test]
    fn get_events_composite_multiple() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "item_revision".into(),
            value: CustomValue::Composite(vec![
                CustomValue::Bytes32(Bytes32([0x11; 32])),
                CustomValue::U32(7),
            ]),
        });
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    #[test]
    fn get_events_before_cursor_filters_newer_results() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        for (block_number, event_index) in [(10, 0), (20, 1), (20, 2), (30, 0)] {
            key.write_db_key(&trees, block_number, event_index).unwrap();
        }

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(
            &trees.index,
            &prefix,
            Some(&EventRef {
                block_number: 20,
                event_index: 2,
            }),
            100,
        );
        assert_eq!(
            events,
            vec![
                EventRef { block_number: 20, event_index: 1 },
                EventRef { block_number: 10, event_index: 0 }
            ]
        );
    }

    #[test]
    fn get_events_index_honors_cursor_and_limit_clamping_inputs() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });
        for i in 0..5u32 {
            key.write_db_key(&trees, i + 1, i).unwrap();
        }

        let prefix = key.index_prefix().unwrap().unwrap();
        let before = EventRef { block_number: 4, event_index: 3 };
        let events = get_events_index(&trees.index, &prefix, Some(&before), 1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], EventRef { block_number: 3, event_index: 2 });

        let events = get_events_index(&trees.index, &prefix, None, 1000);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn jsonrpc_error_serializes_correctly() {
        let response = jsonrpc_error(9, INVALID_PARAMS, "missing field `id`", Some(REASON_INVALID_KEY));
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"code\":-32602"));
        assert!(json.contains("\"reason\":\"invalid_key\""));
    }

    #[test]
    fn jsonrpc_success_serializes_correctly() {
        let result = IndexStatusResult { spans: vec![Span { start: 1, end: 100 }] };
        let response = jsonrpc_success(1, serde_json::to_value(result).unwrap());
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("100"));
    }

    #[test]
    fn generate_subscription_id_is_unique() {
        let id1 = generate_subscription_id();
        let id2 = generate_subscription_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("sub_"));
    }

    #[tokio::test]
    async fn process_msg_returns_temporarily_unavailable_when_rpc_is_missing() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: 16,
            method: "acuity_getEventMetadata".into(),
            params: serde_json::Value::Null,
        };
        let response = process_msg(runtime.as_ref(), &trees, request, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).await.unwrap();
        match &response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, UPSTREAM_UNAVAILABLE);
                assert_eq!(err.id, Some(16));
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_handles_index_status() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let response = process_local_method(&trees, 1, "acuity_indexStatus", &serde_json::Value::Null, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Success(success) => {
                assert_eq!(success.id, 1);
                let result: IndexStatusResult = serde_json::from_value(success.result).unwrap();
                assert!(result.spans.is_empty());
            }
            _ => panic!("expected success response"),
        }
    }

    #[test]
    fn process_local_method_rejects_invalid_key() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "x".repeat(crate::ws_api::validation::MAX_CUSTOM_KEY_NAME_BYTES + 1), value: CustomValue::U32(7) });
        let params = serde_json::to_value(GetEventsParams { key, limit: 100, before: None }).unwrap();
        let response = process_local_method(&trees, 10, "acuity_getEvents", &params, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, INVALID_PARAMS);
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_rejects_oversized_custom_string_value() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "slug".into(), value: CustomValue::String("x".repeat(crate::ws_api::validation::MAX_CUSTOM_STRING_VALUE_BYTES + 1)) });
        let params = serde_json::to_value(SubscribeEventsParams { key }).unwrap();
        let response = process_local_method(&trees, 11, "acuity_subscribeEvents", &params, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, INVALID_PARAMS);
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_rejects_composite_with_too_many_elements() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "too_many".into(), value: CustomValue::Composite((0..=crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS).map(|_| CustomValue::U32(1)).collect()) });
        let params = serde_json::to_value(GetEventsParams { key, limit: 100, before: None }).unwrap();
        let response = process_local_method(&trees, 12, "acuity_getEvents", &params, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, INVALID_PARAMS);
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_rejects_deeply_nested_composite() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let mut value = CustomValue::U32(1);
        for _ in 0..=crate::ws_api::validation::MAX_COMPOSITE_DEPTH {
            value = CustomValue::Composite(vec![value]);
        }
        let key = Key::Custom(CustomKey { name: "deep".into(), value });
        let params = serde_json::to_value(GetEventsParams { key, limit: 100, before: None }).unwrap();
        let response = process_local_method(&trees, 13, "acuity_getEvents", &params, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, INVALID_PARAMS);
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_rejects_oversized_encoded_composite_value() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let string_len = 252usize;
        let key = Key::Custom(CustomKey { name: "too_big".into(), value: CustomValue::Composite((0..crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS).map(|_| CustomValue::String("x".repeat(string_len))).collect()) });
        let params = serde_json::to_value(GetEventsParams { key, limit: 100, before: None }).unwrap();
        let response = process_local_method(&trees, 14, "acuity_getEvents", &params, &sub_tx, &response_tx).unwrap().unwrap();
        match response {
            JsonRpcResponse::Error(err) => {
                assert_eq!(err.error.code, INVALID_PARAMS);
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn process_local_method_rejects_subscription_when_control_queue_is_full() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        sub_tx.try_send(SubscriptionMessage::SubscribeStatus { tx: response_tx.clone(), response_tx: None }).unwrap();
        let result = process_local_method(&trees, 9, "acuity_subscribeStatus", &serde_json::Value::Null, &sub_tx, &response_tx).unwrap();
        assert!(matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted));
    }
}
