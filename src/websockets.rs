//! WebSocket server — schema-less edition.

use crate::shared::*;
use futures::{SinkExt, StreamExt};
use sled::Tree;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use subxt::{
    Metadata, PolkadotConfig, config::RpcConfigFor, rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore,
    mpsc::{self, Sender},
    watch::Receiver,
};
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::{
    self,
    handshake::server::{Request, Response},
    http::{Response as HttpResponse, StatusCode},
    protocol::WebSocketConfig,
};
use tracing::{error, info};
use zerocopy::IntoBytes;

const MAX_EVENTS_LIMIT: usize = 1000;
const SUBSCRIPTION_BUFFER_SIZE: usize = 256;
pub const SUBSCRIPTION_CONTROL_BUFFER_SIZE: usize = 1024;
const MAX_WS_MESSAGE_SIZE_BYTES: usize = 256 * 1024;
const MAX_WS_FRAME_SIZE_BYTES: usize = 64 * 1024;
const MAX_CUSTOM_KEY_NAME_BYTES: usize = 128;
const MAX_CUSTOM_STRING_VALUE_BYTES: usize = 1024;
const MAX_COMPOSITE_ELEMENTS: usize = 64;
const MAX_COMPOSITE_DEPTH: usize = 8;
const MAX_CUSTOM_VALUE_BYTES: usize = 16384;
const MAX_CONNECTIONS: usize = 1024;
const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 128;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(MAX_WS_MESSAGE_SIZE_BYTES);
    config.max_frame_size = Some(MAX_WS_FRAME_SIZE_BYTES);
    config
}

fn enqueue_subscription_message(
    sub_tx: &Sender<SubscriptionMessage>,
    msg: SubscriptionMessage,
) -> Result<(), IndexError> {
    sub_tx.try_send(msg).map_err(|err| match err {
        mpsc::error::TrySendError::Full(_) => disconnect_error("subscription control queue full"),
        mpsc::error::TrySendError::Closed(_) => disconnect_error("subscription dispatcher closed"),
    })
}

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
        let Some(event) = decode_event_ref_suffix(suffix) else {
            error!("Skipping malformed event index key");
            continue;
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
    ResponseMessage { id: Some(id), body }
}

fn validate_key(key: &Key) -> Result<(), IndexError> {
    let Key::Custom(custom) = key else {
        return Ok(());
    };

    let name_len = custom.name.len();
    if name_len > MAX_CUSTOM_KEY_NAME_BYTES {
        return Err(disconnect_error(format!(
            "custom key name exceeds {MAX_CUSTOM_KEY_NAME_BYTES} bytes: {name_len}"
        )));
    }

    if let CustomValue::String(value) = &custom.value {
        let value_len = value.len();
        if value_len > MAX_CUSTOM_STRING_VALUE_BYTES {
            return Err(disconnect_error(format!(
                "custom string key value exceeds {MAX_CUSTOM_STRING_VALUE_BYTES} bytes: {value_len}"
            )));
        }
    }

    validate_custom_value(&custom.value, 0)?;

    Ok(())
}

fn validate_custom_value(value: &CustomValue, depth: usize) -> Result<(), IndexError> {
    if depth > MAX_COMPOSITE_DEPTH {
        return Err(disconnect_error(format!(
            "composite key nesting exceeds max depth {MAX_COMPOSITE_DEPTH}"
        )));
    }

    match value {
        CustomValue::Composite(values) => {
            if values.len() > MAX_COMPOSITE_ELEMENTS {
                return Err(disconnect_error(format!(
                    "composite key has {} elements, max is {MAX_COMPOSITE_ELEMENTS}",
                    values.len()
                )));
            }
            for v in values {
                validate_custom_value(v, depth + 1)?;
            }
        }
        _ => {}
    }

    match value.db_bytes() {
        Ok(bytes) if bytes.len() > MAX_CUSTOM_VALUE_BYTES => {
            Err(disconnect_error(format!(
                "custom key encoded value exceeds {MAX_CUSTOM_VALUE_BYTES} bytes: {} bytes",
                bytes.len()
            )))
        }
        Err(err) => Err(err),
        Ok(_) => Ok(()),
    }
}

fn validate_subscription_request(
    status_subscribed: bool,
    event_subscriptions: &HashSet<Key>,
    body: &RequestBody,
) -> Result<(), IndexError> {
    let next_count = match body {
        RequestBody::SubscribeStatus if !status_subscribed => event_subscriptions.len() + 1,
        RequestBody::SubscribeEvents { key } if !event_subscriptions.contains(key) => {
            event_subscriptions.len() + usize::from(!status_subscribed)
        }
        _ => return Ok(()),
    };

    if next_count > MAX_SUBSCRIPTIONS_PER_CONNECTION {
        return Err(disconnect_error(format!(
            "subscription limit exceeded: max {MAX_SUBSCRIPTIONS_PER_CONNECTION} per connection"
        )));
    }

    Ok(())
}

fn uncorrelated_error_response(code: &'static str, message: impl Into<String>) -> ResponseMessage {
    ResponseMessage {
        id: None,
        body: ResponseBody::Error(ApiError {
            code,
            message: message.into(),
        }),
    }
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
) -> Result<ResponseBody, IndexError> {
    let event_refs = key.get_events(trees, before.as_ref(), clamp_events_limit(limit))?;

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

    Ok(ResponseBody::Events {
        key,
        events: event_refs,
        decoded_events,
    })
}

fn size_on_disk_response(trees: &Trees) -> Result<ResponseBody, IndexError> {
    Ok(ResponseBody::SizeOnDisk(trees.root.size_on_disk()?))
}

fn process_local_msg(
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &Sender<SubscriptionMessage>,
    sub_response_tx: &mpsc::Sender<NotificationMessage>,
) -> Option<Result<ResponseMessage, IndexError>> {
    let response = match msg.body {
        RequestBody::Status => request_id_response(msg.id, process_msg_status(&trees.span)),
        RequestBody::SubscribeStatus => {
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeStatus {
                    tx: sub_response_tx.clone(),
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
        RequestBody::GetEvents { key, limit, before } => {
            if let Err(err) = validate_key(&key) {
                return Some(Err(err));
            }
            match process_msg_get_events(trees, key, before, limit) {
                Ok(body) => request_id_response(msg.id, body),
                Err(err) => return Some(Err(err)),
            }
        }
        RequestBody::SubscribeEvents { key } => {
            if let Err(err) = validate_key(&key) {
                return Some(Err(err));
            }
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::SubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Subscribed,
                    target: SubscriptionTarget::Events { key },
                },
            )
        }
        RequestBody::UnsubscribeEvents { key } => {
            if let Err(err) = validate_key(&key) {
                return Some(Err(err));
            }
            if let Err(err) = enqueue_subscription_message(
                sub_tx,
                SubscriptionMessage::UnsubscribeEvents {
                    key: key.clone(),
                    tx: sub_response_tx.clone(),
                },
            ) {
                return Some(Err(err));
            }
            request_id_response(
                msg.id,
                ResponseBody::SubscriptionStatus {
                    action: SubscriptionAction::Unsubscribed,
                    target: SubscriptionTarget::Events { key },
                },
            )
        }
        RequestBody::SizeOnDisk => {
            return Some(
                size_on_disk_response(trees).map(|body| request_id_response(msg.id, body)),
            );
        }
    };

    Some(Ok(response))
}

pub async fn process_msg(
    rpc: &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    trees: &Trees,
    msg: RequestMessage,
    sub_tx: &Sender<SubscriptionMessage>,
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
    ws_sender: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<TcpStream>,
        tungstenite::Message,
    >,
    msg: &T,
) -> Result<(), IndexError> {
    let json = serde_json::to_string(msg)?;
    ws_sender
        .send(tungstenite::Message::Text(json.into()))
        .await?;
    Ok(())
}

fn disconnect_error(message: impl Into<String>) -> IndexError {
    IndexError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        message.into(),
    ))
}

fn service_unavailable_response(message: &str) -> HttpResponse<Option<String>> {
    HttpResponse::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Some(message.to_owned()))
        .expect("service unavailable response should be valid")
}

// ─── Connection handler ───────────────────────────────────────────────────────

async fn handle_connection(
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    raw_stream: TcpStream,
    addr: SocketAddr,
    trees: Trees,
    sub_tx: Sender<SubscriptionMessage>,
    _connection_permit: OwnedSemaphorePermit,
) -> Result<(), IndexError> {
    info!("TCP connection from {addr}");
    let ws_stream =
        tokio_tungstenite::accept_async_with_config(raw_stream, Some(websocket_config())).await?;
    info!("WebSocket established: {addr}");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (sub_events_tx, mut sub_events_rx) = mpsc::channel(SUBSCRIPTION_BUFFER_SIZE);
    let mut status_subscribed = false;
    let mut event_subscriptions = HashSet::new();
    let idle_timer = time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle_timer);

    loop {
        tokio::select! {
            _ = &mut idle_timer => return Err(disconnect_error("connection idle timeout exceeded")),
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(msg)) if msg.is_text() || msg.is_binary() => {
                        idle_timer.as_mut().reset(time::Instant::now() + IDLE_TIMEOUT);
                        match serde_json::from_str::<RequestMessage>(msg.to_text()?) {
                            Ok(request) => {
                                if let Err(err) = validate_subscription_request(
                                    status_subscribed,
                                    &event_subscriptions,
                                    &request.body,
                                ) {
                                    let response = error_response(request.id, "subscription_limit", err.to_string());
                                    send_json_message(&mut ws_sender, &response).await?;
                                    continue;
                                }
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
                                let response =
                                    uncorrelated_error_response("invalid_request", err.to_string());
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
                    Some(msg) => {
                        idle_timer.as_mut().reset(time::Instant::now() + IDLE_TIMEOUT);
                        send_json_message(&mut ws_sender, &msg).await?
                    }
                    None => return Err(disconnect_error("subscription dispatcher closed")),
                }
            }
        }
    }

    if status_subscribed {
        let _ = enqueue_subscription_message(
            &sub_tx,
            SubscriptionMessage::UnsubscribeStatus {
                tx: sub_events_tx.clone(),
            },
        );
    }
    for key in event_subscriptions {
        let _ = enqueue_subscription_message(
            &sub_tx,
            SubscriptionMessage::UnsubscribeEvents {
                key,
                tx: sub_events_tx.clone(),
            },
        );
    }

    Ok(())
}

async fn reject_connection_limit_exceeded(raw_stream: TcpStream) {
    let callback = |_req: &Request, _response: Response| {
        Err(service_unavailable_response(
            "websocket connection limit reached; try again later",
        ))
    };

    if let Err(err) = tokio_tungstenite::accept_hdr_async_with_config(
        raw_stream,
        callback,
        Some(websocket_config()),
    )
    .await
    {
        error!("Failed to send overload handshake rejection: {err}");
    }
}

// ─── Listener ────────────────────────────────────────────────────────────────

pub async fn websockets_listen(
    trees: Trees,
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    port: u16,
    mut exit_rx: Receiver<bool>,
    sub_tx: Sender<SubscriptionMessage>,
) {
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
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
                let Ok(connection_permit) = connection_limit.clone().try_acquire_owned() else {
                    error!("Rejecting {addr}: connection limit reached");
                    tokio::spawn(reject_connection_limit_exceeded(stream));
                    continue;
                };
                tokio::spawn(handle_connection(
                    rpc.clone(),
                    stream,
                    addr,
                    trees.clone(),
                    sub_tx.clone(),
                    connection_permit,
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
    fn websocket_config_sets_explicit_message_and_frame_limits() {
        let config = websocket_config();

        assert_eq!(config.max_message_size, Some(MAX_WS_MESSAGE_SIZE_BYTES));
        assert_eq!(config.max_frame_size, Some(MAX_WS_FRAME_SIZE_BYTES));
    }

    #[test]
    fn validate_subscription_request_enforces_connection_limit() {
        let event_subscriptions = (0..MAX_SUBSCRIPTIONS_PER_CONNECTION)
            .map(|i| {
                Key::Custom(CustomKey {
                    name: format!("key_{i}"),
                    value: CustomValue::U32(i as u32),
                })
            })
            .collect();

        let result = validate_subscription_request(
            false,
            &event_subscriptions,
            &RequestBody::SubscribeStatus,
        );

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn validate_subscription_request_allows_duplicate_subscriptions() {
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        let event_subscriptions = HashSet::from([key.clone()]);

        assert!(
            validate_subscription_request(
                false,
                &event_subscriptions,
                &RequestBody::SubscribeEvents { key }
            )
            .is_ok()
        );
    }

    #[test]
    fn service_unavailable_response_is_http_503() {
        let response = service_unavailable_response("busy");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.body().as_deref(), Some("busy"));
    }

    #[test]
    fn process_local_msg_handles_subscribe_and_unsubscribe_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
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
                id: Some(1),
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
                id: Some(2),
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
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
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
                id: Some(1),
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
                id: Some(2),
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

        let custom_prefix_bytes = custom_key.db_prefix().unwrap();
        let mut short_custom_prefix = vec![2u8];
        short_custom_prefix.extend_from_slice(&custom_prefix_bytes);
        trees.index.insert(short_custom_prefix, &[]).unwrap();
        for i in 0..150u32 {
            bytes_key.write_db_key(&trees, i, 0).unwrap();
            u32_key.write_db_key(&trees, i, 0).unwrap();
            Key::Custom(custom_key.clone())
                .write_db_key(&trees, i, 0)
                .unwrap();
        }

        let bytes_prefix = bytes_key.index_prefix().unwrap().unwrap();
        let u32_prefix = u32_key.index_prefix().unwrap().unwrap();
        let custom_prefix = Key::Custom(custom_key.clone()).index_prefix().unwrap().unwrap();

        assert_eq!(
            get_events_index(&trees.index, &bytes_prefix, None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(&trees.index, &u32_prefix, None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(
                &trees.index,
                &custom_prefix,
                None,
                100
            )
            .len(),
            100
        );

        trees.index.insert(b"x", &[]).unwrap();
        assert_eq!(
            get_events_index(
                &trees.index,
                &custom_prefix,
                None,
                100
            )
            .len(),
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

        let ResponseBody::Events { decoded_events, .. } =
            process_msg_get_events(&trees, key, None, 100).unwrap()
        else {
            panic!("expected events response");
        };
        assert!(decoded_events.is_empty());
    }

    #[test]
    fn process_local_msg_handles_status_unsubscribe_and_get_events() {
        let trees = temp_trees();
        let (sub_tx, mut sub_rx) = mpsc::channel(4);
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
                id: Some(1),
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
                id: Some(2),
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
                id: Some(3),
                body: ResponseBody::Events { .. },
            }
        ));
    }

    #[test]
    fn process_local_msg_returns_none_for_variants() {
        let trees = temp_trees();
        let (sub_tx, _) = mpsc::channel(1);
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

        sub_tx
            .try_send(SubscriptionMessage::SubscribeStatus {
                tx: response_tx.clone(),
            })
            .unwrap();

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 9,
                body: RequestBody::SubscribeStatus,
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap();

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn process_local_msg_rejects_oversized_custom_key_name() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey {
            name: "x".repeat(MAX_CUSTOM_KEY_NAME_BYTES + 1),
            value: CustomValue::U32(7),
        });

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 10,
                body: RequestBody::GetEvents {
                    key,
                    limit: 100,
                    before: None,
                },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap();

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn process_local_msg_rejects_oversized_custom_string_value() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey {
            name: "slug".into(),
            value: CustomValue::String("x".repeat(MAX_CUSTOM_STRING_VALUE_BYTES + 1)),
        });

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 11,
                body: RequestBody::SubscribeEvents { key },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap();

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn process_local_msg_rejects_composite_with_too_many_elements() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let key = Key::Custom(CustomKey {
            name: "too_many".into(),
            value: CustomValue::Composite(
                (0..=MAX_COMPOSITE_ELEMENTS)
                    .map(|_| CustomValue::U32(1))
                    .collect(),
            ),
        });

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 12,
                body: RequestBody::GetEvents { key, limit: 100, before: None },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap();

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn process_local_msg_rejects_deeply_nested_composite() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);

        let mut value = CustomValue::U32(1);
        for _ in 0..=MAX_COMPOSITE_DEPTH {
            value = CustomValue::Composite(vec![value]);
        }

        let key = Key::Custom(CustomKey {
            name: "deep".into(),
            value,
        });

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 13,
                body: RequestBody::GetEvents { key, limit: 100, before: None },
            },
            &sub_tx,
            &response_tx,
        )
        .unwrap();

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }
}
