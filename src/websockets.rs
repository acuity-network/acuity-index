//! WebSocket server — schema-less edition.

use crate::shared::*;
use futures::{SinkExt, StreamExt};
use sled::Tree;
use std::{collections::HashSet, net::SocketAddr};
use subxt::{
    Metadata, PolkadotConfig, config::RpcConfigFor, rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    watch::Receiver,
};
use tokio_tungstenite::tungstenite;
use tracing::{error, info};
use zerocopy::{FromBytes, IntoBytes};

const MAX_EVENTS_LIMIT: usize = 1000;
const SUBSCRIPTION_BUFFER_SIZE: usize = 256;

// ─── Tree scan helpers (pub so shared.rs can call them) ───────────────────────

pub fn get_events_index(
    tree: &Tree,
    prefix: &[u8],
    before: Option<&EventRef>,
    limit: usize,
) -> Vec<EventRef> {
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix(prefix).keys();
    while let Some(Ok(raw)) = iter.next_back() {
        if raw.len() < 6 {
            continue;
        }
        let suffix = &raw[raw.len() - 6..];
        let event = EventRef {
            block_number: u32::from_be_bytes(suffix[..4].try_into().unwrap()),
            event_index: u16::from_be_bytes(suffix[4..].try_into().unwrap()),
        };
        if before.is_some_and(|cursor| !event_is_before(&event, cursor)) {
            continue;
        }
        events.push(event);
        if events.len() == limit {
            break;
        }
    }
    events
}

fn event_is_before(event: &EventRef, cursor: &EventRef) -> bool {
    (event.block_number, event.event_index) < (cursor.block_number, cursor.event_index)
}

fn clamp_events_limit(limit: u16) -> usize {
    usize::from(limit).clamp(1, MAX_EVENTS_LIMIT)
}

fn request_id_response(id: u64, body: ResponseBody) -> ResponseMessage {
    ResponseMessage { id, body }
}

fn error_response(id: u64, code: &'static str, message: impl Into<String>) -> ResponseMessage {
    request_id_response(
        id,
        ResponseBody::Error(ApiError {
            code,
            message: message.into(),
        }),
    )
}

// ─── Message handlers ─────────────────────────────────────────────────────────

pub fn process_msg_status(span_db: &Tree) -> ResponseBody {
    let mut spans = vec![];
    for (key, value) in span_db.into_iter().flatten() {
        let sv = SpanDbValue::read_from_bytes(&value).unwrap();
        let start: u32 = sv.start.into();
        let end: u32 = u32::from_be_bytes(key.as_ref().try_into().unwrap());
        spans.push(Span { start, end });
    }
    ResponseBody::Status(spans)
}

pub async fn process_msg_variants(
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

pub fn process_msg_get_events(
    trees: &Trees,
    key: Key,
    before: Option<EventRef>,
    limit: u16,
) -> ResponseBody {
    let event_refs = key.get_events(trees, before.as_ref(), clamp_events_limit(limit));

    let mut decoded_events = Vec::new();
    for event_ref in &event_refs {
        let db_key = EventKey {
            block_number: event_ref.block_number.into(),
            event_index: event_ref.event_index.into(),
        };
        if let Ok(Some(bytes)) = trees.events.get(db_key.as_bytes()) {
            if let Ok(json) = serde_json::from_slice(&bytes) {
                decoded_events.push(DecodedEvent {
                    block_number: event_ref.block_number,
                    event_index: event_ref.event_index,
                    event: json,
                });
            }
        }
    }

    ResponseBody::Events {
        key,
        events: event_refs,
        decoded_events,
    }
}

fn size_on_disk_response(trees: &Trees) -> Result<ResponseBody, IndexError> {
    Ok(ResponseBody::SizeOnDisk(trees.root.size_on_disk()?))
}

fn process_local_msg(
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &UnboundedSender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
) -> Option<Result<ResponseMessage, IndexError>> {
    Some(Ok(match msg.body {
        RequestBody::Status => request_id_response(msg.id, process_msg_status(&trees.span)),
        RequestBody::SubscribeStatus => {
            sub_tx
                .send(SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Status,
                },
            )
        }
        RequestBody::UnsubscribeStatus => {
            sub_tx
                .send(SubscriptionMessage::UnsubscribeStatus {
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Status,
                },
            )
        }
        RequestBody::Variants => return None,
        RequestBody::GetEvents { key, limit, before } => {
            request_id_response(msg.id, process_msg_get_events(trees, key, before, limit))
        }
        RequestBody::SubscribeEvents { key } => {
            sub_tx
                .send(SubscriptionMessage::SubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Events { key },
                },
            )
        }
        RequestBody::UnsubscribeEvents { key } => {
            sub_tx
                .send(SubscriptionMessage::UnsubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Events { key },
                },
            )
        }
        RequestBody::SizeOnDisk => return Some(size_on_disk_response(trees).map(|body| request_id_response(msg.id, body))),
    }))
}

pub async fn process_msg(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &UnboundedSender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
) -> Result<ResponseMessage, IndexError> {
    let id = msg.id;
    if let Some(response) = process_local_msg(trees, msg, sub_tx, sub_response_tx) {
        return response;
    }

    let body = process_msg_variants(rpc).await?;
    Ok(request_id_response(id, body))
}

async fn send_json_message<T: serde::Serialize>(
    ws_sender: &mut futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, tungstenite::Message>,
    msg: &T,
) -> Result<(), IndexError> {
    let json = serde_json::to_string(msg)?;
    ws_sender
        .send(tungstenite::Message::Text(json.into()))
        .await?;
    Ok(())
}

fn disconnect_error(message: impl Into<String>) -> IndexError {
    IndexError::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, message.into()))
}

// ─── Connection handler ───────────────────────────────────────────────────────

async fn handle_connection(
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    raw_stream: TcpStream,
    addr: SocketAddr,
    trees: Trees,
    sub_tx: UnboundedSender<SubscriptionMessage>,
) -> Result<(), IndexError> {
    info!("TCP connection from {addr}");
    let ws_stream = tokio_tungstenite::accept_async(raw_stream).await?;
    info!("WebSocket established: {addr}");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (sub_events_tx, mut sub_events_rx) = mpsc::channel(SUBSCRIPTION_BUFFER_SIZE);
    let mut status_subscribed = false;
    let mut event_subscriptions = HashSet::new();

    loop {
        tokio::select! {
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(msg)) if msg.is_text() || msg.is_binary() => {
                        match serde_json::from_str::<RequestMessage>(msg.to_text()?) {
                            Ok(request) => {
                                match &request.body {
                                    RequestBody::SubscribeStatus => status_subscribed = true,
                                    RequestBody::UnsubscribeStatus => status_subscribed = false,
                                    RequestBody::SubscribeEvents { key } => {
                                        event_subscriptions.insert(key.clone());
                                    }
                                    RequestBody::UnsubscribeEvents { key } => {
                                        event_subscriptions.remove(key);
                                    }
                                    _ => {}
                                }
                                let response = match process_msg(
                                    &rpc,
                                    &trees,
                                    request.clone(),
                                    &sub_tx,
                                    &sub_events_tx,
                                ).await {
                                    Ok(response) => response,
                                    Err(err) => error_response(request.id, "internal_error", err.to_string()),
                                };
                                send_json_message(&mut ws_sender, &response).await?;
                            }
                            Err(err) => {
                                error!("Parse error: {err}");
                                let response = error_response(0, "invalid_request", err.to_string());
                                send_json_message(&mut ws_sender, &response).await?;
                            }
                        }
                    }
                    Some(Ok(msg)) if msg.is_close() => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => break,
                }
            }
            notification = sub_events_rx.recv() => {
                match notification {
                    Some(msg) => send_json_message(&mut ws_sender, &msg).await?,
                    None => return Err(disconnect_error("subscription dispatcher closed")),
                }
            }
        }
    }

    if status_subscribed {
        let _ = sub_tx.send(SubscriptionMessage::UnsubscribeStatus {
            tx: sub_events_tx.clone(),
        });
    }
    for key in event_subscriptions {
        let _ = sub_tx.send(SubscriptionMessage::UnsubscribeEvents {
            key,
            tx: sub_events_tx.clone(),
        });
    }

    Ok(())
}

// ─── Listener ────────────────────────────────────────────────────────────────

pub async fn websockets_listen(
    trees: Trees,
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    port: u16,
    mut exit_rx: Receiver<bool>,
    sub_tx: UnboundedSender<SubscriptionMessage>,
) {
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind WebSocket port");
    info!("Listening on {addr}");

    loop {
        tokio::select! {
            biased;
            _ = exit_rx.changed() => break,
            Ok((stream, addr)) = listener.accept() => {
                tokio::spawn(handle_connection(
                    rpc.clone(),
                    stream,
                    addr,
                    trees.clone(),
                    sub_tx.clone(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    fn temp_trees() -> Trees {
        let db_config = sled::Config::new().temporary(true);
        Trees::open(db_config).unwrap()
    }

    #[test]
    fn process_local_msg_handles_subscribe_and_unsubscribe_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });

        let subscribed = process_local_msg(
            &trees,
            RequestMessage {
                id: 1,
                body: RequestBody::SubscribeEvents { key: key.clone() },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            subscribed,
            ResponseMessage {
                id: 1,
                body: ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Events { key: key.clone() },
                },
            }
        );
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::SubscribeEvents { key: received, .. } if received == key
        ));

        let unsubscribed = process_local_msg(
            &trees,
            RequestMessage {
                id: 2,
                body: RequestBody::UnsubscribeEvents { key: key.clone() },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            unsubscribed,
            ResponseMessage {
                id: 2,
                body: ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Events { key: key.clone() },
                },
            }
        );
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::UnsubscribeEvents { key: received, .. } if received == key
        ));
    }

    #[test]
    fn process_local_msg_handles_status_and_size_requests() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = mpsc::channel(1);

        let status = process_local_msg(
            &trees,
            RequestMessage {
                id: 1,
                body: RequestBody::SubscribeStatus,
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            status,
            ResponseMessage {
                id: 1,
                body: ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Status,
                },
            }
        ));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::SubscribeStatus { .. }
        ));

        let size = process_local_msg(
            &trees,
            RequestMessage {
                id: 2,
                body: RequestBody::SizeOnDisk,
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            size,
            ResponseMessage {
                id: 2,
                body: ResponseBody::SizeOnDisk(_),
            }
        ));
    }

    #[test]
    fn tree_scan_helpers_limit_results_and_skip_short_custom_keys() {
        let trees = temp_trees();
        let bytes_key = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32([0xAA; 32])),
        });
        let u32_key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(5),
        });
        let custom_key = CustomKey {
            name: "slug".into(),
            value: CustomValue::String("hello".into()),
        };

        let mut short_custom_prefix = vec![2u8];
        short_custom_prefix.extend_from_slice(&custom_key.db_prefix());
        trees.index.insert(short_custom_prefix, &[]).unwrap();
        for i in 0..150u32 {
            bytes_key.write_db_key(&trees, i, 0).unwrap();
            u32_key.write_db_key(&trees, i, 0).unwrap();
            Key::Custom(custom_key.clone())
                .write_db_key(&trees, i, 0)
                .unwrap();
        }

        assert_eq!(
            get_events_index(&trees.index, &bytes_key.index_prefix().unwrap(), None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(&trees.index, &u32_key.index_prefix().unwrap(), None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(&trees.index, &Key::Custom(custom_key.clone()).index_prefix().unwrap(), None, 100)
                .len(),
            100
        );

        trees.index.insert(b"x", &[]).unwrap();
        assert_eq!(
            get_events_index(&trees.index, &Key::Custom(custom_key).index_prefix().unwrap(), None, 100).len(),
            100
        );
    }

    #[test]
    fn process_msg_get_events_ignores_invalid_stored_event_json() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(3),
        });
        key.write_db_key(&trees, 10, 1).unwrap();
        let db_key = EventKey {
            block_number: 10u32.into(),
            event_index: 1u16.into(),
        };
        trees
            .events
            .insert(db_key.as_bytes(), b"not-json".as_slice())
            .unwrap();

        let ResponseBody::Events { decoded_events, .. } = process_msg_get_events(&trees, key, None, 100)
        else {
            panic!("expected events response");
        };
        assert!(decoded_events.is_empty());
    }

    #[test]
    fn process_local_msg_handles_status_unsubscribe_and_get_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(12),
        });
        key.write_db_key(&trees, 22, 1).unwrap();

        let status = process_local_msg(
            &trees,
            RequestMessage {
                id: 1,
                body: RequestBody::Status,
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            status,
            ResponseMessage {
                id: 1,
                body: ResponseBody::Status(_),
            }
        ));

        let unsubscribed = process_local_msg(
            &trees,
            RequestMessage {
                id: 2,
                body: RequestBody::UnsubscribeStatus,
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            unsubscribed,
            ResponseMessage {
                id: 2,
                body: ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Status,
                },
            }
        ));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::UnsubscribeStatus { .. }
        ));

        let events = process_local_msg(
            &trees,
            RequestMessage {
                id: 3,
                body: RequestBody::GetEvents {
                    key,
                    limit: 100,
                    before: None,
                },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            events,
            ResponseMessage {
                id: 3,
                body: ResponseBody::Events { .. },
            }
        ));
    }

    #[test]
    fn process_local_msg_returns_none_for_variants() {
        let trees = temp_trees();
        let (sub_tx, _) = unbounded_channel();
        let (response_tx, _) = mpsc::channel(1);

        assert!(
            process_local_msg(
                &trees,
                RequestMessage {
                    id: 1,
                    body: RequestBody::Variants,
                },
                &sub_tx,
                &response_tx,
            )
            .is_none()
        );
    }
}
