//! WebSocket server — schema-less edition.

use crate::shared::*;
use futures::{SinkExt, StreamExt};
use sled::Tree;
use std::net::SocketAddr;
use subxt::{
    Metadata, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    mpsc::{UnboundedSender, unbounded_channel},
    watch::Receiver,
};
use tokio_tungstenite::tungstenite;
use tracing::{error, info};
use zerocopy::{BigEndian, FromBytes, IntoBytes, byteorder::U32};

// ─── Tree scan helpers (pub so shared.rs can call them) ───────────────────────

pub fn get_events_bytes32(tree: &Tree, key: &Bytes32) -> Vec<EventRef> {
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix(key.as_ref() as &[u8]).keys();
    while let Some(Ok(raw)) = iter.next_back() {
        let k = Bytes32Key::read_from_bytes(&raw).unwrap();
        events.push(EventRef {
            block_number: k.block_number.into(),
            event_index: k.event_index.into(),
        });
        if events.len() == 100 {
            break;
        }
    }
    events
}

pub fn get_events_u32(tree: &Tree, key: u32) -> Vec<EventRef> {
    let mut events = Vec::new();
    let mut iter = tree.scan_prefix(key.to_be_bytes()).keys();
    while let Some(Ok(raw)) = iter.next_back() {
        let k = U32Key::read_from_bytes(&raw).unwrap();
        events.push(EventRef {
            block_number: k.block_number.into(),
            event_index: k.event_index.into(),
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

    let mut block_numbers: Vec<u32> = event_refs.iter().map(|e| e.block_number).collect();
    block_numbers.sort_unstable();
    block_numbers.dedup();

    let mut block_events = Vec::new();
    for bn in &block_numbers {
        let db_key: U32<BigEndian> = (*bn).into();
        if let Ok(Some(bytes)) = trees.block_events.get(db_key.as_bytes()) {
            if let Ok(json) = serde_json::from_slice(&bytes) {
                block_events.push(BlockEvents {
                    block_number: *bn,
                    events: json,
                });
            }
        }
    }

    ResponseMessage::Events {
        key,
        events: event_refs,
        block_events,
    }
}

pub async fn process_msg(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &UnboundedSender<SubscriptionMessage>,
    sub_response_tx: &UnboundedSender<ResponseMessage>,
) -> Result<ResponseMessage, IndexError> {
    Ok(match msg {
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
        RequestMessage::Variants => process_msg_variants(rpc).await?,
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
        RequestMessage::SizeOnDisk => ResponseMessage::SizeOnDisk(trees.root.size_on_disk()?),
    })
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
