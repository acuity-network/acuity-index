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

pub(crate) fn request_id_response(id: u64, body: ResponseBody) -> ResponseMessage {
    ResponseMessage { id: Some(id), body }
}

pub(crate) fn invalid_request_response(id: u64, message: impl Into<String>) -> ResponseMessage {
    error_response(id, "invalid_request", message)
}

pub(crate) fn subscription_limit_response(
    id: u64,
    message: impl Into<String>,
) -> ResponseMessage {
    error_response(id, "subscription_limit", message)
}

pub(crate) fn temporarily_unavailable_response(id: u64) -> ResponseMessage {
    error_response(
        id,
        "temporarily_unavailable",
        "node temporarily unavailable",
    )
}

pub(crate) fn uncorrelated_error_response(
    code: &'static str,
    message: impl Into<String>,
) -> ResponseMessage {
    ResponseMessage {
        id: None,
        body: ResponseBody::Error(ApiError {
            code,
            message: message.into(),
        }),
    }
}

pub(crate) fn error_response(
    id: u64,
    code: &'static str,
    message: impl Into<String>,
) -> ResponseMessage {
    request_id_response(
        id,
        ResponseBody::Error(ApiError {
            code,
            message: message.into(),
        }),
    )
}

pub(crate) fn enqueue_subscription_message(
    sub_tx: &Sender<SubscriptionMessage>,
    msg: SubscriptionMessage,
) -> Result<(), IndexError> {
    sub_tx.try_send(msg).map_err(|err| match err {
        mpsc::error::TrySendError::Full(_) => disconnect_error("subscription control queue full"),
        mpsc::error::TrySendError::Closed(_) => disconnect_error("subscription dispatcher closed"),
    })
}

pub fn process_msg_status(span_db: &Tree) -> ResponseBody {
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
    ResponseBody::Status(spans)
}

pub(crate) async fn process_msg_variants(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
) -> Result<ResponseBody, IndexError> {
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
    Ok(ResponseBody::Variants(pallets))
}

pub(crate) async fn process_msg_get_events(
    runtime: &RuntimeState,
    trees: &Trees,
    api: &OnlineClient<PolkadotConfig>,
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    key: Key,
    before: Option<EventRef>,
    limit: u16,
    include_proofs: bool,
    max_events_limit: usize,
) -> Result<ResponseBody, IndexError> {
    let event_refs = key.get_events(
        trees,
        before.as_ref(),
        clamp_events_limit(limit, max_events_limit),
    )?;
    let decoded_events = hydrate_event_refs(api, rpc, &event_refs).await?;

    let (proofs_by_block, proofs_status) = if include_proofs {
        if !runtime.finalized_mode() {
            (
                Some(None),
                Some(ProofsStatus {
                    available: false,
                    reason: "finalized_proofs_unavailable".into(),
                    message: "Finalized proofs are only available when the indexer is running with finalized indexing.".into(),
                }),
            )
        } else {
            match fetch_event_block_proofs(rpc, &event_refs).await {
                Ok(proofs) => (
                    Some(Some(proofs)),
                    Some(ProofsStatus {
                        available: true,
                        reason: "included".into(),
                        message: "Finalized event proofs included.".into(),
                    }),
                ),
                Err(err) => (
                    Some(None),
                    Some(ProofsStatus {
                        available: false,
                        reason: if err.is_recoverable() {
                            "rpc_proof_unavailable"
                        } else {
                            "finalized_proofs_unavailable"
                        }
                        .into(),
                        message: format!("Unable to include finalized proofs: {err}"),
                    }),
                ),
            }
        }
    } else {
        (None, None)
    };

    Ok(ResponseBody::Events {
        key,
        events: event_refs,
        decoded_events,
        proofs_by_block,
        proofs_status,
    })
}

fn size_on_disk_response(trees: &Trees) -> Result<ResponseBody, IndexError> {
    Ok(ResponseBody::SizeOnDisk(trees.root.size_on_disk()?))
}

pub(crate) fn process_local_msg(
    trees: &Trees,
    msg: &RequestMessage,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
    max_events_limit: usize,
) -> Option<Result<ResponseMessage, IndexError>> {
    let response = match &msg.body {
        RequestBody::Status => request_id_response(msg.id, process_msg_status(&trees.span)),
        RequestBody::SubscribeStatus => {
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Status,
                },
            )
        }
        RequestBody::UnsubscribeStatus => {
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeStatus {
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Status,
                },
            )
        }
        RequestBody::Variants => return None,
        RequestBody::GetEvents {
            key,
            limit,
            before,
            include_proofs,
        } => {
            if let Err(err) = validate_key(key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }
            let _ = (before, limit, include_proofs, max_events_limit);
            return None;
        }
        RequestBody::SubscribeEvents { key } => {
            if let Err(err) = validate_key(key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Events { key: key.clone() },
                },
            )
        }
        RequestBody::UnsubscribeEvents { key } => {
            if let Err(err) = validate_key(key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                    response_tx: None,
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Events { key: key.clone() },
                },
            )
        }
        RequestBody::SizeOnDisk => {
            return Some(size_on_disk_response(trees).map(|body| request_id_response(msg.id, body)));
        }
    };

    Some(Ok(response))
}

async fn process_subscription_request(
    msg: &RequestMessage,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
) -> Option<Result<ResponseMessage, IndexError>> {
    let response = match &msg.body {
        RequestBody::SubscribeStatus => {
            let (response_tx, response_rx) = oneshot::channel();
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
                Ok(Ok(())) => request_id_response(
                    msg.id,
                    ResponseBody::SubscriptionStatus {
                        action: SubscriptionAction::Subscribed,
                        target: SubscriptionTarget::Status,
                    },
                ),
                Ok(Err(message)) => subscription_limit_response(msg.id, message),
                Err(_) => {
                    return Some(Err(disconnect_error(
                        "subscription dispatcher dropped response",
                    )));
                }
            }
        }
        RequestBody::UnsubscribeStatus => {
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
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Status,
                },
            )
        }
        RequestBody::SubscribeEvents { key } => {
            if let Err(err) = validate_key(key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }

            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                    response_tx: Some(response_tx),
                },
            ) {
                return Some(Err(err));
            }
            match response_rx.await {
                Ok(Ok(())) => request_id_response(
                    msg.id,
                    ResponseBody::SubscriptionStatus {
                        action: SubscriptionAction::Subscribed,
                        target: SubscriptionTarget::Events { key: key.clone() },
                    },
                ),
                Ok(Err(message)) => subscription_limit_response(msg.id, message),
                Err(_) => {
                    return Some(Err(disconnect_error(
                        "subscription dispatcher dropped response",
                    )));
                }
            }
        }
        RequestBody::UnsubscribeEvents { key } => {
            if let Err(err) = validate_key(key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }

            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeEvents {
                    key: key.clone(),
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
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Events { key: key.clone() },
                },
            )
        }
        _ => return None,
    };

    Some(Ok(response))
}

pub(crate) async fn process_msg(
    runtime: &RuntimeState,
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
    max_events_limit: usize,
) -> Result<ResponseMessage, IndexError> {
    let id = msg.id;
    if let Some(response) = process_subscription_request(&msg, sub_tx, sub_response_tx).await {
        return response;
    }

    if let Some(response) = process_local_msg(trees, &msg, sub_tx, sub_response_tx, max_events_limit)
    {
        return response;
    }

    let Some((api, rpc)) = runtime.clients() else {
        return Ok(temporarily_unavailable_response(id));
    };

    let body = match msg.body {
        RequestBody::Variants => process_msg_variants(&rpc).await,
        RequestBody::GetEvents {
            key,
            limit,
            before,
            include_proofs,
        } => {
            process_msg_get_events(
                runtime,
                trees,
                &api,
                &rpc,
                key,
                before,
                limit,
                include_proofs,
                max_events_limit,
            )
            .await
        }
        _ => unreachable!(),
    };
    let body = match body {
        Ok(body) => body,
        Err(err) if err.is_recoverable() => return Ok(temporarily_unavailable_response(id)),
        Err(err) => return Err(err),
    };
    Ok(request_id_response(id, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::IndexError;
    use crate::protocol::get_events_index;
    use crate::ws_api::tests_support::{
        DEFAULT_WS_CONFIG, DEFAULT_LIVE_WS_CONFIG, disconnected_runtime,
        spawn_subscription_dispatcher, temp_trees,
    };
    use tokio::sync::{mpsc, watch};
    use zerocopy::IntoBytes;

    #[test]
    fn process_msg_status_empty() {
        let trees = temp_trees();
        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseBody::Status(spans) => assert!(spans.is_empty()),
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_status_with_spans() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 100u32.into(),
            version: 0u16.into(),
        };
        trees.span.insert(200u32.to_be_bytes(), sv.as_bytes()).unwrap();

        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseBody::Status(spans) => {
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].start, 100);
                assert_eq!(spans[0].end, 200);
            }
            _ => panic!("wrong response type"),
        }
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
    fn error_response_serializes_structured_error() {
        let msg = ResponseMessage {
            id: Some(9),
            body: ResponseBody::Error(ApiError {
                code: "invalid_request",
                message: "missing field `id`".into(),
            }),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("invalid_request"));
        assert!(json.contains("missing field `id`"));
    }

    #[test]
    fn response_status_serializes() {
        let msg = ResponseMessage {
            id: Some(1),
            body: ResponseBody::Status(vec![Span { start: 1, end: 100 }]),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("status"));
        assert!(json.contains("100"));
    }

    #[test]
    fn response_subscription_status_serializes() {
        let msg = ResponseMessage {
            id: Some(2),
            body: ResponseBody::SubscriptionStatus {
                action: SubscriptionAction::Subscribed,
                target: SubscriptionTarget::Status,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("subscribed"));
    }

    #[test]
    fn response_size_on_disk_serializes() {
        let msg = ResponseMessage {
            id: Some(3),
            body: ResponseBody::SizeOnDisk(123456),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("123456"));
    }

    #[test]
    fn response_events_serializes() {
        let msg = ResponseMessage {
            id: Some(4),
            body: ResponseBody::Events {
                key: Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(42),
                }),
                events: vec![EventRef { block_number: 10, event_index: 2 }],
                decoded_events: vec![],
                proofs_by_block: None,
                proofs_status: None,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("ref_index"));
        assert!(json.contains("42"));
        assert!(json.contains("decodedEvents"));
    }

    #[test]
    fn response_subscription_events_target_serializes_key() {
        let msg = ResponseMessage {
            id: Some(10),
            body: ResponseBody::SubscriptionStatus {
                action: SubscriptionAction::Subscribed,
                target: SubscriptionTarget::Events {
                    key: Key::Custom(CustomKey {
                        name: "item_id".into(),
                        value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                    }),
                },
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("subscriptionStatus"));
        assert!(json.contains("item_id"));
        assert!(json.contains("events"));
    }

    #[test]
    fn process_local_msg_handles_subscribe_and_unsubscribe_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "pool_id".into(), value: CustomValue::U32(7) });

        let subscribed = process_local_msg(
            &trees,
            &RequestMessage { id: 1, body: RequestBody::SubscribeEvents { key: key.clone() } },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        ).unwrap().unwrap();
        assert_eq!(subscribed, ResponseMessage { id: Some(1), body: ResponseBody::SubscriptionStatus { action: SubscriptionAction::Subscribed, target: SubscriptionTarget::Events { key: key.clone() }}});
        assert!(matches!(sub_rx.try_recv().unwrap(), SubscriptionMessage::SubscribeEvents { key: received, .. } if received == key));

        let unsubscribed = process_local_msg(
            &trees,
            &RequestMessage { id: 2, body: RequestBody::UnsubscribeEvents { key: key.clone() } },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        ).unwrap().unwrap();
        assert_eq!(unsubscribed, ResponseMessage { id: Some(2), body: ResponseBody::SubscriptionStatus { action: SubscriptionAction::Unsubscribed, target: SubscriptionTarget::Events { key: key.clone() }}});
        assert!(matches!(sub_rx.try_recv().unwrap(), SubscriptionMessage::UnsubscribeEvents { key: received, .. } if received == key));
    }

    #[test]
    fn process_local_msg_handles_status_and_size_requests() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
        let (response_tx, _) = mpsc::channel(1);
        let status = process_local_msg(&trees, &RequestMessage { id: 1, body: RequestBody::SubscribeStatus }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap().unwrap();
        assert!(matches!(status, ResponseMessage { id: Some(1), body: ResponseBody::SubscriptionStatus { action: SubscriptionAction::Subscribed, target: SubscriptionTarget::Status, }, }));
        assert!(matches!(sub_rx.try_recv().unwrap(), SubscriptionMessage::SubscribeStatus { .. }));

        let size = process_local_msg(&trees, &RequestMessage { id: 2, body: RequestBody::SizeOnDisk }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap().unwrap();
        assert!(matches!(size, ResponseMessage { id: Some(2), body: ResponseBody::SizeOnDisk(_), }));
    }

    #[test]
    fn process_local_msg_handles_status_unsubscribe_and_get_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "ref_index".into(), value: CustomValue::U32(12) });
        key.write_db_key(&trees, 22, 1).unwrap();

        let status = process_local_msg(&trees, &RequestMessage { id: 1, body: RequestBody::Status }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap().unwrap();
        assert!(matches!(status, ResponseMessage { id: Some(1), body: ResponseBody::Status(_), }));

        let unsubscribed = process_local_msg(&trees, &RequestMessage { id: 2, body: RequestBody::UnsubscribeStatus }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap().unwrap();
        assert!(matches!(unsubscribed, ResponseMessage { id: Some(2), body: ResponseBody::SubscriptionStatus { action: SubscriptionAction::Unsubscribed, target: SubscriptionTarget::Status, }, }));
        assert!(matches!(sub_rx.try_recv().unwrap(), SubscriptionMessage::UnsubscribeStatus { .. }));

        let events = process_local_msg(&trees, &RequestMessage { id: 3, body: RequestBody::GetEvents { key, limit: 100, before: None, include_proofs: false, } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit);
        assert!(events.is_none());
    }

    #[test]
    fn process_local_msg_returns_none_for_variants() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        assert!(process_local_msg(&trees, &RequestMessage { id: 1, body: RequestBody::Variants }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).is_none());
    }

    #[tokio::test]
    async fn process_msg_returns_temporarily_unavailable_when_rpc_is_missing() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);

        let response = process_msg(runtime.as_ref(), &trees, RequestMessage { id: 16, body: RequestBody::Variants }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).await.unwrap();
        assert_eq!(response, ResponseMessage { id: Some(16), body: ResponseBody::Error(ApiError { code: "temporarily_unavailable", message: "node temporarily unavailable".into(), }), });
    }

    #[tokio::test]
    async fn process_msg_returns_request_error_only_when_global_cap_is_reached() {
        let trees = temp_trees();
        let runtime = std::sync::Arc::new(crate::runtime_state::RuntimeState::new(1));
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        crate::indexer::process_sub_msg(runtime.as_ref(), &LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG }, SubscriptionMessage::SubscribeStatus { tx: existing_tx, response_tx: None, }).unwrap();
        let (sub_tx, sub_rx) = mpsc::channel(4);
        let (_live_ws_tx, live_ws_rx) = watch::channel(LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG });
        let dispatcher = spawn_subscription_dispatcher(runtime.clone(), sub_rx, live_ws_rx);
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let response = process_msg(runtime.as_ref(), &trees, RequestMessage { id: 17, body: RequestBody::SubscribeStatus }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).await.unwrap();
        assert_eq!(response, ResponseMessage { id: Some(17), body: ResponseBody::Error(ApiError { code: "subscription_limit", message: "total subscription limit exceeded: max 1".into(), }), });
        assert!(response_rx.try_recv().is_err());
        drop(sub_tx);
        dispatcher.await.unwrap();
    }

    #[test]
    fn uncorrelated_error_response_omits_id() {
        let response = uncorrelated_error_response("invalid_request", "missing field `id`");
        assert_eq!(response.id, None);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(!json.contains("\"id\""));
    }

    #[test]
    fn process_local_msg_rejects_subscription_when_control_queue_is_full() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        sub_tx.try_send(SubscriptionMessage::SubscribeStatus { tx: response_tx.clone(), response_tx: None }).unwrap();
        let result = process_local_msg(&trees, &RequestMessage { id: 9, body: RequestBody::SubscribeStatus }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert!(matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted));
    }

    #[test]
    fn process_local_msg_rejects_oversized_custom_key_name() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "x".repeat(crate::ws_api::validation::MAX_CUSTOM_KEY_NAME_BYTES + 1), value: CustomValue::U32(7) });
        let result = process_local_msg(&trees, &RequestMessage { id: 10, body: RequestBody::GetEvents { key, limit: 100, before: None, include_proofs: false } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert_eq!(result.unwrap().id, Some(10));
    }

    #[test]
    fn process_local_msg_rejects_oversized_custom_string_value() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "slug".into(), value: CustomValue::String("x".repeat(crate::ws_api::validation::MAX_CUSTOM_STRING_VALUE_BYTES + 1)) });
        let result = process_local_msg(&trees, &RequestMessage { id: 11, body: RequestBody::SubscribeEvents { key } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert_eq!(result.unwrap().id, Some(11));
    }

    #[test]
    fn process_local_msg_rejects_composite_with_too_many_elements() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey { name: "too_many".into(), value: CustomValue::Composite((0..=crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS).map(|_| CustomValue::U32(1)).collect()) });
        let result = process_local_msg(&trees, &RequestMessage { id: 12, body: RequestBody::GetEvents { key, limit: 100, before: None, include_proofs: false } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert_eq!(result.unwrap().id, Some(12));
    }

    #[test]
    fn process_local_msg_rejects_deeply_nested_composite() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let mut value = CustomValue::U32(1);
        for _ in 0..=crate::ws_api::validation::MAX_COMPOSITE_DEPTH {
            value = CustomValue::Composite(vec![value]);
        }
        let key = Key::Custom(CustomKey { name: "deep".into(), value });
        let result = process_local_msg(&trees, &RequestMessage { id: 13, body: RequestBody::GetEvents { key, limit: 100, before: None, include_proofs: false } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert_eq!(result.unwrap().id, Some(13));
    }

    #[test]
    fn process_local_msg_rejects_oversized_encoded_composite_value() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let string_len = 252usize;
        let key = Key::Custom(CustomKey { name: "too_big".into(), value: CustomValue::Composite((0..crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS).map(|_| CustomValue::String("x".repeat(string_len))).collect()) });
        let result = process_local_msg(&trees, &RequestMessage { id: 14, body: RequestBody::GetEvents { key, limit: 100, before: None, include_proofs: false } }, &sub_tx, &response_tx, DEFAULT_WS_CONFIG.max_events_limit).unwrap();
        assert_eq!(result.unwrap().id, Some(14));
    }
}
