//! WebSocket server — schema-less edition.

use crate::shared::*;
use futures::{SinkExt, StreamExt};
use sled::Tree;
use std::net::SocketAddr;
use subxt::{
    Metadata, PolkadotConfig, config::RpcConfigFor, rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    mpsc::{UnboundedSender, unbounded_channel},
    watch::Receiver,
};
use tokio_tungstenite::tungstenite;
use tracing::{error, info};
use zerocopy::{FromBytes, IntoBytes};

// ─── Tree scan helpers (pub so shared.rs can call them) ───────────────────────

pub fn get_events_index(tree: &Tree, prefix: &[u8]) -> Vec<EventRef> {
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix(prefix).keys();
    while let Some(Ok(raw)) = iter.next_back() {
        let suffix = &raw[raw.len() - 6..];
        events.push(EventRef {
            block_number: u32::from_be_bytes(suffix[..4].try_into().unwrap()),
            event_index: u16::from_be_bytes(suffix[4..].try_into().unwrap()),
        });
        if events.len() == 100 {
            break;
        }
    }
    events
}

// ─── Message handlers ─────────────────────────────────────────────────────────

pub fn process_msg_status(span_db: &Tree) -> ResponseMessage {
    let mut spans = vec![];
    for (key, value) in span_db.into_iter().flatten() {
        let sv = SpanDbValue::read_from_bytes(&value).unwrap();
        let start: u32 = sv.start.into();
        let end: u32 = u32::from_be_bytes(key.as_ref().try_into().unwrap());
        spans.push(Span { start, end });
    }
    ResponseMessage::Status(spans)
}

pub async fn process_msg_variants(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
) -> Result<ResponseMessage, IndexError> {
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
    Ok(ResponseMessage::Variants(pallets))
}

pub fn process_msg_get_events(trees: &Trees, key: Key) -> ResponseMessage {
    let event_refs = key.get_events(trees);

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

    ResponseMessage::Events {
        key,
        events: event_refs,
        decoded_events,
    }
}

fn size_on_disk_response(trees: &Trees) -> Result<ResponseMessage, IndexError> {
    Ok(ResponseMessage::SizeOnDisk(trees.root.size_on_disk()?))
}

fn process_local_msg(
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &UnboundedSender<SubscriptionMessage>,
    sub_response_tx: &UnboundedSender<ResponseMessage>,
) -> Option<Result<ResponseMessage, IndexError>> {
    Some(Ok(match msg {
        RequestMessage::Status => process_msg_status(&trees.span),
        RequestMessage::SubscribeStatus => {
            sub_tx
                .send(SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            ResponseMessage::Subscribed
        }
        RequestMessage::UnsubscribeStatus => {
            sub_tx
                .send(SubscriptionMessage::UnsubscribeStatus {
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            ResponseMessage::Unsubscribed
        }
        RequestMessage::Variants => return None,
        RequestMessage::GetEvents { key } => process_msg_get_events(trees, key),
        RequestMessage::SubscribeEvents { key } => {
            sub_tx
                .send(SubscriptionMessage::SubscribeEvents {
                    key,
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            ResponseMessage::Subscribed
        }
        RequestMessage::UnsubscribeEvents { key } => {
            sub_tx
                .send(SubscriptionMessage::UnsubscribeEvents {
                    key,
                    tx: sub_response_tx.clone(),
                })
                .unwrap();
            ResponseMessage::Unsubscribed
        }
        RequestMessage::SizeOnDisk => return Some(size_on_disk_response(trees)),
    }))
}

pub async fn process_msg(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &UnboundedSender<SubscriptionMessage>,
    sub_response_tx: &UnboundedSender<ResponseMessage>,
) -> Result<ResponseMessage, IndexError> {
    if let Some(response) = process_local_msg(trees, msg, sub_tx, sub_response_tx) {
        return response;
    }

    process_msg_variants(rpc).await
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
    let (sub_events_tx, mut sub_events_rx) = unbounded_channel();

    loop {
        tokio::select! {
            Some(Ok(msg)) = ws_receiver.next() => {
                if msg.is_text() || msg.is_binary() {
                    match serde_json::from_str(msg.to_text()?) {
                        Ok(request) => {
                            let response = process_msg(
                                &rpc,
                                &trees,
                                request,
                                &sub_tx,
                                &sub_events_tx,
                            )
                            .await?;
                            let json =
                                serde_json::to_string(&response)?;
                            ws_sender
                                .send(tungstenite::Message::Text(json.into()))
                                .await?;
                        }
                        Err(err) => error!("Parse error: {err}"),
                    }
                }
            }
            Some(msg) = sub_events_rx.recv() => {
                let json = serde_json::to_string(&msg)?;
                ws_sender
                    .send(tungstenite::Message::Text(json.into()))
                    .await?;
            }
        }
    }
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

    fn temp_trees() -> Trees {
        let db_config = sled::Config::new().temporary(true);
        Trees::open(db_config).unwrap()
    }

    #[test]
    fn process_local_msg_handles_subscribe_and_unsubscribe_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = unbounded_channel();
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });

        let subscribed = process_local_msg(
            &trees,
            RequestMessage::SubscribeEvents { key: key.clone() },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(subscribed, ResponseMessage::Subscribed));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::SubscribeEvents { key: received, .. } if received == key
        ));

        let unsubscribed = process_local_msg(
            &trees,
            RequestMessage::UnsubscribeEvents { key: key.clone() },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(unsubscribed, ResponseMessage::Unsubscribed));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::UnsubscribeEvents { key: received, .. } if received == key
        ));
    }

    #[test]
    fn process_local_msg_handles_status_and_size_requests() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = unbounded_channel();

        let status = process_local_msg(
            &trees,
            RequestMessage::SubscribeStatus,
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(status, ResponseMessage::Subscribed));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::SubscribeStatus { .. }
        ));

        let size = process_local_msg(&trees, RequestMessage::SizeOnDisk, &sub_tx, &response_tx)
            .unwrap()
            .unwrap();
        assert!(matches!(size, ResponseMessage::SizeOnDisk(_)));
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
            get_events_index(&trees.index, &bytes_key.index_prefix().unwrap()).len(),
            100
        );
        assert_eq!(get_events_index(&trees.index, &u32_key.index_prefix().unwrap()).len(), 100);
        assert_eq!(
            get_events_index(&trees.index, &Key::Custom(custom_key.clone()).index_prefix().unwrap())
                .len(),
            100
        );

        trees.index.insert(b"x", &[]).unwrap();
        assert_eq!(
            get_events_index(&trees.index, &Key::Custom(custom_key).index_prefix().unwrap()).len(),
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

        let ResponseMessage::Events { decoded_events, .. } = process_msg_get_events(&trees, key)
        else {
            panic!("expected events response");
        };
        assert!(decoded_events.is_empty());
    }

    #[test]
    fn process_local_msg_handles_status_unsubscribe_and_get_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = unbounded_channel();
        let (response_tx, _) = unbounded_channel();
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(12),
        });
        key.write_db_key(&trees, 22, 1).unwrap();

        let status = process_local_msg(&trees, RequestMessage::Status, &sub_tx, &response_tx)
            .unwrap()
            .unwrap();
        assert!(matches!(status, ResponseMessage::Status(_)));

        let unsubscribed = process_local_msg(
            &trees,
            RequestMessage::UnsubscribeStatus,
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(unsubscribed, ResponseMessage::Unsubscribed));
        assert!(matches!(
            sub_rx.try_recv().unwrap(),
            SubscriptionMessage::UnsubscribeStatus { .. }
        ));

        let events = process_local_msg(
            &trees,
            RequestMessage::GetEvents { key },
            &sub_tx,
            &response_tx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(events, ResponseMessage::Events { .. }));
    }

    #[test]
    fn process_local_msg_returns_none_for_variants() {
        let trees = temp_trees();
        let (sub_tx, _) = unbounded_channel();
        let (response_tx, _) = unbounded_channel();

        assert!(
            process_local_msg(&trees, RequestMessage::Variants, &sub_tx, &response_tx).is_none()
        );
    }
}
