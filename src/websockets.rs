//! WebSocket server — schema-less edition.

use crate::shared::*;
use futures::{SinkExt, StreamExt};
use sled::Tree;
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use subxt::{
    Metadata, PolkadotConfig, config::RpcConfigFor, rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot, watch,
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

const MAX_WS_MESSAGE_SIZE_BYTES: usize = 256 * 1024;
const MAX_WS_FRAME_SIZE_BYTES: usize = 64 * 1024;
const MAX_CUSTOM_KEY_NAME_BYTES: usize = 128;
const MAX_CUSTOM_STRING_VALUE_BYTES: usize = 1024;
const MAX_COMPOSITE_ELEMENTS: usize = 64;
const MAX_COMPOSITE_DEPTH: usize = 8;
const MAX_CUSTOM_VALUE_BYTES: usize = 16384;

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
        if raw.len() < EVENT_REF_SUFFIX_LEN {
            continue;
        }
        let suffix = &raw[raw.len() - EVENT_REF_SUFFIX_LEN..];
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

fn clamp_events_limit(limit: u16, max_events_limit: usize) -> usize {
    usize::from(limit).clamp(1, max_events_limit.max(1))
}

fn idle_deadline_for(
    last_activity: time::Instant,
    idle_timeout_secs: u64,
) -> Option<time::Instant> {
    (idle_timeout_secs != 0).then(|| last_activity + Duration::from_secs(idle_timeout_secs))
}

fn request_id_response(id: u64, body: ResponseBody) -> ResponseMessage {
    ResponseMessage { id: Some(id), body }
}

fn invalid_request_response(id: u64, message: impl Into<String>) -> ResponseMessage {
    error_response(id, "invalid_request", message)
}

fn subscription_limit_response(id: u64, message: impl Into<String>) -> ResponseMessage {
    error_response(id, "subscription_limit", message)
}

fn temporarily_unavailable_response(id: u64) -> ResponseMessage {
    error_response(
        id,
        "temporarily_unavailable",
        "node temporarily unavailable",
    )
}

fn validate_key(key: &Key) -> Result<(), String> {
    let Key::Custom(custom) = key else {
        return Ok(());
    };

    let name_len = custom.name.len();
    if name_len > MAX_CUSTOM_KEY_NAME_BYTES {
        return Err(format!(
            "custom key name exceeds {MAX_CUSTOM_KEY_NAME_BYTES} bytes: {name_len}"
        ));
    }

    let encoded_len = validate_custom_value(&custom.value, 0)?;
    if encoded_len > MAX_CUSTOM_VALUE_BYTES {
        return Err(format!(
            "custom key encoded value exceeds {MAX_CUSTOM_VALUE_BYTES} bytes: {encoded_len} bytes"
        ));
    }

    Ok(())
}

fn validate_custom_value(value: &CustomValue, depth: usize) -> Result<usize, String> {
    if depth > MAX_COMPOSITE_DEPTH {
        return Err(format!(
            "composite key nesting exceeds max depth {MAX_COMPOSITE_DEPTH}"
        ));
    }

    match value {
        CustomValue::Composite(values) => {
            if values.len() > MAX_COMPOSITE_ELEMENTS {
                return Err(format!(
                    "composite key has {} elements, max is {MAX_COMPOSITE_ELEMENTS}",
                    values.len()
                ));
            }

            let mut encoded_len = 2usize;
            for v in values {
                let value_len = validate_custom_value(v, depth + 1)?;
                encoded_len += 1 + 4 + value_len;
            }

            Ok(encoded_len)
        }
        CustomValue::Bytes32(_) => Ok(32),
        CustomValue::U32(_) => Ok(4),
        CustomValue::U64(_) => Ok(8),
        CustomValue::U128(_) => Ok(16),
        CustomValue::String(value) => {
            let value_len = value.len();
            if value_len > MAX_CUSTOM_STRING_VALUE_BYTES {
                return Err(format!(
                    "custom string key value exceeds {MAX_CUSTOM_STRING_VALUE_BYTES} bytes: {value_len}"
                ));
            }
            Ok(value_len)
        }
        CustomValue::Bool(_) => Ok(1),
    }
}

fn validate_subscription_request(
    status_subscribed: bool,
    event_subscriptions: &HashSet<Key>,
    body: &RequestBody,
    max_subscriptions_per_connection: usize,
) -> Result<(), IndexError> {
    let next_count = match body {
        RequestBody::SubscribeStatus if !status_subscribed => event_subscriptions.len() + 1,
        RequestBody::SubscribeEvents { key } if !event_subscriptions.contains(key) => {
            event_subscriptions.len() + 1 + usize::from(status_subscribed)
        }
        _ => return Ok(()),
    };

    if next_count > max_subscriptions_per_connection {
        return Err(disconnect_error(format!(
            "subscription limit exceeded: max {max_subscriptions_per_connection} per connection"
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
    max_events_limit: usize,
) -> Result<ResponseBody, IndexError> {
    let event_refs = key.get_events(
        trees,
        before.as_ref(),
        clamp_events_limit(limit, max_events_limit),
    )?;

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
    max_events_limit: usize,
) -> Option<Result<ResponseMessage, IndexError>> {
    let response = match msg.body {
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
        RequestBody::GetEvents { key, limit, before } => {
            if let Err(err) = validate_key(&key) {
                return Some(Ok(invalid_request_response(msg.id, err)));
            }
            match process_msg_get_events(trees, key, before, limit, max_events_limit) {
                Ok(body) => request_id_response(msg.id, body),
                Err(err) => return Some(Err(err)),
            }
        }
        RequestBody::SubscribeEvents { key } => {
            if let Err(err) = validate_key(&key) {
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
                    target: SubscriptionTarget::Events { key },
                },
            )
        }
        RequestBody::UnsubscribeEvents { key } => {
            if let Err(err) = validate_key(&key) {
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

pub async fn process_msg(
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

    if let Some(response) = process_local_msg(trees, msg, sub_tx, sub_response_tx, max_events_limit)
    {
        return response;
    }

    let Some(rpc) = runtime.rpc() else {
        return Ok(temporarily_unavailable_response(id));
    };

    let body = match process_msg_variants(&rpc).await {
        Ok(body) => body,
        Err(err) if err.is_recoverable() => return Ok(temporarily_unavailable_response(id)),
        Err(err) => return Err(err),
    };
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
    let mut response = HttpResponse::new(Some(message.to_owned()));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::CONTENT_TYPE,
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static(
            "text/plain; charset=utf-8",
        ),
    );
    response
}

fn apply_subscription_response(
    status_subscribed: &mut bool,
    event_subscriptions: &mut HashSet<Key>,
    response: &ResponseMessage,
) {
    let ResponseBody::SubscriptionStatus { action, target } = &response.body else {
        return;
    };

    match (action, target) {
        (SubscriptionAction::Subscribed, SubscriptionTarget::Status) => {
            *status_subscribed = true;
        }
        (SubscriptionAction::Unsubscribed, SubscriptionTarget::Status) => {
            *status_subscribed = false;
        }
        (SubscriptionAction::Subscribed, SubscriptionTarget::Events { key }) => {
            event_subscriptions.insert(key.clone());
        }
        (SubscriptionAction::Unsubscribed, SubscriptionTarget::Events { key }) => {
            event_subscriptions.remove(key);
        }
    }
}

struct ConnectionMetricGuard {
    runtime: Arc<RuntimeState>,
}

impl ConnectionMetricGuard {
    fn new(runtime: Arc<RuntimeState>) -> Self {
        runtime.metrics.inc_ws_connections();
        Self { runtime }
    }
}

struct ConnectionSlotGuard {
    connection_count: Arc<AtomicUsize>,
}

impl ConnectionSlotGuard {
    fn new(connection_count: Arc<AtomicUsize>) -> Self {
        Self { connection_count }
    }
}

impl Drop for ConnectionSlotGuard {
    fn drop(&mut self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ConnectionMetricGuard {
    fn drop(&mut self) {
        self.runtime.metrics.dec_ws_connections();
    }
}

// ─── Connection handler ───────────────────────────────────────────────────────

async fn handle_connection(
    runtime: Arc<RuntimeState>,
    raw_stream: TcpStream,
    addr: SocketAddr,
    trees: Trees,
    sub_tx: Sender<SubscriptionMessage>,
    _connection_slot: ConnectionSlotGuard,
    subscription_buffer_size: usize,
    mut live_ws_config_rx: watch::Receiver<LiveWsConfig>,
) -> Result<(), IndexError> {
    info!("TCP connection from {addr}");
    let ws_stream =
        tokio_tungstenite::accept_async_with_config(raw_stream, Some(websocket_config())).await?;
    info!("WebSocket established: {addr}");
    let _connection_metrics = ConnectionMetricGuard::new(runtime.clone());

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (sub_events_tx, mut sub_events_rx) = mpsc::channel(subscription_buffer_size.max(1));
    let mut status_subscribed = false;
    let mut event_subscriptions = HashSet::new();
    let mut live_ws_config = *live_ws_config_rx.borrow();
    let mut last_activity = time::Instant::now();
    let mut idle_deadline = idle_deadline_for(last_activity, live_ws_config.idle_timeout_secs);

    loop {
        tokio::select! {
            _ = async {
                if let Some(deadline) = idle_deadline {
                    time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return Err(disconnect_error("connection idle timeout exceeded")),
            changed = live_ws_config_rx.changed() => {
                if changed.is_ok() {
                    live_ws_config = *live_ws_config_rx.borrow_and_update();
                    idle_deadline = idle_deadline_for(last_activity, live_ws_config.idle_timeout_secs);
                }
            }
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(msg)) if msg.is_text() || msg.is_binary() => {
                        last_activity = time::Instant::now();
                        idle_deadline = idle_deadline_for(last_activity, live_ws_config.idle_timeout_secs);
                        match serde_json::from_str::<RequestMessage>(msg.to_text()?) {
                            Ok(request) => {
                                if let Err(err) = validate_subscription_request(
                                    status_subscribed,
                                    &event_subscriptions,
                                    &request.body,
                                    live_ws_config.max_subscriptions_per_connection,
                                ) {
                                    let response = subscription_limit_response(request.id, err.to_string());
                                    send_json_message(&mut ws_sender, &response).await?;
                                    continue;
                                }
                                let response = match process_msg(
                                    runtime.as_ref(),
                                    &trees,
                                    request.clone(),
                                    &sub_tx,
                                    &sub_events_tx,
                                    live_ws_config.max_events_limit,
                                ).await {
                                    Ok(response) => response,
                                    Err(err) => error_response(request.id, "internal_error", err.to_string()),
                                };
                                apply_subscription_response(
                                    &mut status_subscribed,
                                    &mut event_subscriptions,
                                    &response,
                                );
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
                        last_activity = time::Instant::now();
                        idle_deadline = idle_deadline_for(last_activity, live_ws_config.idle_timeout_secs);
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
                response_tx: None,
            },
        );
    }
    for key in event_subscriptions {
        let _ = enqueue_subscription_message(
            &sub_tx,
            SubscriptionMessage::UnsubscribeEvents {
                key,
                tx: sub_events_tx.clone(),
                response_tx: None,
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
    runtime: Arc<RuntimeState>,
    port: u16,
    mut exit_rx: Receiver<bool>,
    sub_tx: Sender<SubscriptionMessage>,
    mut live_ws_config_rx: watch::Receiver<LiveWsConfig>,
) -> Result<(), IndexError> {
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on {addr}");
    let active_connections = Arc::new(AtomicUsize::new(0));
    let mut live_ws_config = *live_ws_config_rx.borrow();

    loop {
        tokio::select! {
            biased;
            _ = exit_rx.changed() => break,
            changed = live_ws_config_rx.changed() => {
                if changed.is_ok() {
                    live_ws_config = *live_ws_config_rx.borrow_and_update();
                }
            }
            Ok((stream, addr)) = listener.accept() => {
                if active_connections.load(Ordering::Relaxed) >= live_ws_config.max_connections {
                    error!("Rejecting {addr}: connection limit reached");
                    tokio::spawn(reject_connection_limit_exceeded(stream));
                    continue;
                }
                active_connections.fetch_add(1, Ordering::Relaxed);
                let connection_slot = ConnectionSlotGuard::new(active_connections.clone());
                tokio::spawn(handle_connection(
                    runtime.clone(),
                    stream,
                    addr,
                    trees.clone(),
                    sub_tx.clone(),
                    connection_slot,
                    live_ws_config.subscription_buffer_size,
                    live_ws_config_rx.clone(),
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::process_sub_msg;
    use futures::{SinkExt, StreamExt};
    use std::sync::Arc;
    use subxt::rpcs::client::{RawRpcFuture, RawRpcSubscription, RawValue, RpcClient, RpcClientT};
    use tokio::time::{Duration, sleep, timeout};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    const DEFAULT_WS_CONFIG: WsConfig = WsConfig {
        max_connections: 1024,
        max_total_subscriptions: 65536,
        max_subscriptions_per_connection: 128,
        subscription_buffer_size: 256,
        subscription_control_buffer_size: 1024,
        idle_timeout_secs: 300,
        max_events_limit: 1000,
    };

    const DEFAULT_LIVE_WS_CONFIG: LiveWsConfig = LiveWsConfig {
        max_connections: DEFAULT_WS_CONFIG.max_connections,
        max_total_subscriptions: DEFAULT_WS_CONFIG.max_total_subscriptions,
        max_subscriptions_per_connection: DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
        subscription_buffer_size: DEFAULT_WS_CONFIG.subscription_buffer_size,
        idle_timeout_secs: DEFAULT_WS_CONFIG.idle_timeout_secs,
        max_events_limit: DEFAULT_WS_CONFIG.max_events_limit,
    };

    fn temp_trees() -> Trees {
        let db_config = sled::Config::new().temporary(true);
        Trees::open(db_config).unwrap()
    }

    struct UnusedRpcClient;

    impl RpcClientT for UnusedRpcClient {
        fn request_raw<'a>(
            &'a self,
            method: &'a str,
            _params: Option<Box<RawValue>>,
        ) -> RawRpcFuture<'a, Box<RawValue>> {
            Box::pin(async move { panic!("unexpected RPC request in websocket test: {method}") })
        }

        fn subscribe_raw<'a>(
            &'a self,
            sub: &'a str,
            _params: Option<Box<RawValue>>,
            _unsub: &'a str,
        ) -> RawRpcFuture<'a, RawRpcSubscription> {
            Box::pin(async move { panic!("unexpected RPC subscription in websocket test: {sub}") })
        }
    }

    fn mock_rpc_methods() -> LegacyRpcMethods<RpcConfigFor<PolkadotConfig>> {
        LegacyRpcMethods::new(RpcClient::new(UnusedRpcClient))
    }

    fn runtime_with_rpc() -> Arc<RuntimeState> {
        let runtime = Arc::new(RuntimeState::new(DEFAULT_WS_CONFIG.max_total_subscriptions));
        runtime.set_rpc(Some(mock_rpc_methods()));
        runtime
    }

    fn disconnected_runtime() -> Arc<RuntimeState> {
        Arc::new(RuntimeState::new(DEFAULT_WS_CONFIG.max_total_subscriptions))
    }

    fn spawn_subscription_dispatcher(
        runtime: Arc<RuntimeState>,
        mut sub_rx: mpsc::Receiver<SubscriptionMessage>,
        live_ws_config_rx: watch::Receiver<LiveWsConfig>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let live_ws_config_rx = live_ws_config_rx;
            while let Some(msg) = sub_rx.recv().await {
                let _ = process_sub_msg(runtime.as_ref(), &*live_ws_config_rx.borrow(), msg);
            }
        })
    }

    #[test]
    fn websocket_config_sets_explicit_message_and_frame_limits() {
        let config = websocket_config();

        assert_eq!(config.max_message_size, Some(MAX_WS_MESSAGE_SIZE_BYTES));
        assert_eq!(config.max_frame_size, Some(MAX_WS_FRAME_SIZE_BYTES));
    }

    #[test]
    fn validate_subscription_request_enforces_connection_limit() {
        let event_subscriptions = (0..DEFAULT_WS_CONFIG.max_subscriptions_per_connection)
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
            DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
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
                &RequestBody::SubscribeEvents { key },
                DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_subscription_request_counts_status_and_event_subscriptions_together() {
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(8),
        });

        let result = validate_subscription_request(
            true,
            &HashSet::new(),
            &RequestBody::SubscribeEvents { key },
            1,
        );

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn clamp_events_limit_handles_zero_max_without_panicking() {
        assert_eq!(clamp_events_limit(0, 0), 1);
        assert_eq!(clamp_events_limit(500, 0), 1);
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
        let custom_prefix = Key::Custom(custom_key.clone())
            .index_prefix()
            .unwrap()
            .unwrap();

        assert_eq!(
            get_events_index(&trees.index, &bytes_prefix, None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(&trees.index, &u32_prefix, None, 100).len(),
            100
        );
        assert_eq!(
            get_events_index(&trees.index, &custom_prefix, None, 100).len(),
            100
        );

        trees.index.insert(b"x", &[]).unwrap();
        assert_eq!(
            get_events_index(&trees.index, &custom_prefix, None, 100).len(),
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
            event_index: 1u32.into(),
        };
        trees
            .events
            .insert(db_key.as_bytes(), b"not-json".as_slice())
            .unwrap();

        let ResponseBody::Events { decoded_events, .. } =
            process_msg_get_events(&trees, key, None, 100, DEFAULT_WS_CONFIG.max_events_limit)
                .unwrap()
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
                DEFAULT_WS_CONFIG.max_events_limit,
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn process_msg_returns_temporarily_unavailable_when_rpc_is_missing() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);

        let response = process_msg(
            runtime.as_ref(),
            &trees,
            RequestMessage {
                id: 16,
                body: RequestBody::Variants,
            },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .await
        .unwrap();

        assert_eq!(
            response,
            ResponseMessage {
                id: Some(16),
                body: ResponseBody::Error(ApiError {
                    code: "temporarily_unavailable",
                    message: "node temporarily unavailable".into(),
                }),
            }
        );
    }

    #[tokio::test]
    async fn process_msg_returns_request_error_only_when_global_cap_is_reached() {
        let trees = temp_trees();
        let runtime = Arc::new(RuntimeState::new(1));
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        process_sub_msg(
            runtime.as_ref(),
            &LiveWsConfig {
                max_total_subscriptions: 1,
                ..DEFAULT_LIVE_WS_CONFIG
            },
            SubscriptionMessage::SubscribeStatus {
                tx: existing_tx,
                response_tx: None,
            },
        )
        .unwrap();

        let (sub_tx, sub_rx) = mpsc::channel(4);
        let (_live_ws_tx, live_ws_rx) = watch::channel(LiveWsConfig {
            max_total_subscriptions: 1,
            ..DEFAULT_LIVE_WS_CONFIG
        });
        let dispatcher = spawn_subscription_dispatcher(runtime.clone(), sub_rx, live_ws_rx);
        let (response_tx, mut response_rx) = mpsc::channel(1);

        let response = process_msg(
            runtime.as_ref(),
            &trees,
            RequestMessage {
                id: 17,
                body: RequestBody::SubscribeStatus,
            },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .await
        .unwrap();

        assert_eq!(
            response,
            ResponseMessage {
                id: Some(17),
                body: ResponseBody::Error(ApiError {
                    code: "subscription_limit",
                    message: "total subscription limit exceeded: max 1".into(),
                }),
            }
        );
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

        sub_tx
            .try_send(SubscriptionMessage::SubscribeStatus {
                tx: response_tx.clone(),
                response_tx: None,
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
            DEFAULT_WS_CONFIG.max_events_limit,
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
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .unwrap();

        assert_eq!(
            result.unwrap(),
            ResponseMessage {
                id: Some(10),
                body: ResponseBody::Error(ApiError {
                    code: "invalid_request",
                    message: format!(
                        "custom key name exceeds {MAX_CUSTOM_KEY_NAME_BYTES} bytes: {}",
                        MAX_CUSTOM_KEY_NAME_BYTES + 1
                    ),
                }),
            }
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
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .unwrap();

        assert_eq!(
            result.unwrap(),
            ResponseMessage {
                id: Some(11),
                body: ResponseBody::Error(ApiError {
                    code: "invalid_request",
                    message: format!(
                        "custom string key value exceeds {MAX_CUSTOM_STRING_VALUE_BYTES} bytes: {}",
                        MAX_CUSTOM_STRING_VALUE_BYTES + 1
                    ),
                }),
            }
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
                body: RequestBody::GetEvents {
                    key,
                    limit: 100,
                    before: None,
                },
            },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .unwrap();

        assert_eq!(
            result.unwrap(),
            ResponseMessage {
                id: Some(12),
                body: ResponseBody::Error(ApiError {
                    code: "invalid_request",
                    message: format!(
                        "composite key has {} elements, max is {MAX_COMPOSITE_ELEMENTS}",
                        MAX_COMPOSITE_ELEMENTS + 1
                    ),
                }),
            }
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
                body: RequestBody::GetEvents {
                    key,
                    limit: 100,
                    before: None,
                },
            },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .unwrap();

        assert_eq!(
            result.unwrap(),
            ResponseMessage {
                id: Some(13),
                body: ResponseBody::Error(ApiError {
                    code: "invalid_request",
                    message: format!(
                        "composite key nesting exceeds max depth {MAX_COMPOSITE_DEPTH}"
                    ),
                }),
            }
        );
    }

    #[test]
    fn process_local_msg_rejects_oversized_encoded_composite_value() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(1);
        let (response_tx, _) = mpsc::channel(1);
        let string_len = 252usize;
        let encoded_len = 2 + MAX_COMPOSITE_ELEMENTS * (1 + 4 + string_len);
        let key = Key::Custom(CustomKey {
            name: "too_big".into(),
            value: CustomValue::Composite(
                (0..MAX_COMPOSITE_ELEMENTS)
                    .map(|_| CustomValue::String("x".repeat(string_len)))
                    .collect(),
            ),
        });

        let result = process_local_msg(
            &trees,
            RequestMessage {
                id: 14,
                body: RequestBody::GetEvents {
                    key,
                    limit: 100,
                    before: None,
                },
            },
            &sub_tx,
            &response_tx,
            DEFAULT_WS_CONFIG.max_events_limit,
        )
        .unwrap();

        assert_eq!(
            result.unwrap(),
            ResponseMessage {
                id: Some(14),
                body: ResponseBody::Error(ApiError {
                    code: "invalid_request",
                    message: format!(
                        "custom key encoded value exceeds {MAX_CUSTOM_VALUE_BYTES} bytes: {encoded_len} bytes"
                    ),
                }),
            }
        );
    }

    #[tokio::test]
    async fn websocket_request_returns_invalid_request_for_malicious_composite() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = runtime_with_rpc();
        let (_live_ws_tx, live_ws_rx) = watch::channel(DEFAULT_LIVE_WS_CONFIG);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(
                runtime,
                stream,
                peer_addr,
                trees,
                sub_tx,
                ConnectionSlotGuard::new(connection_count),
                DEFAULT_LIVE_WS_CONFIG.subscription_buffer_size,
                live_ws_rx,
            )
            .await
        });

        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let request = format!(
            r#"{{"id":15,"type":"GetEvents","key":{{"type":"Custom","value":{{"name":"too_many","kind":"composite","value":[{}]}}}},"limit":100}}"#,
            std::iter::repeat_n(r#"{"kind":"u32","value":1}"#, MAX_COMPOSITE_ELEMENTS + 1)
                .collect::<Vec<_>>()
                .join(",")
        );

        client.send(Message::Text(request.into())).await.unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();

        assert_eq!(response["id"], 15);
        assert_eq!(response["type"], "error");
        assert_eq!(response["data"]["code"], "invalid_request");
        assert_eq!(
            response["data"]["message"],
            format!(
                "composite key has {} elements, max is {MAX_COMPOSITE_ELEMENTS}",
                MAX_COMPOSITE_ELEMENTS + 1
            )
        );

        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn rejected_subscribe_does_not_consume_connection_slot() {
        let trees = temp_trees();
        let runtime = Arc::new(RuntimeState::new(1));
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        process_sub_msg(
            runtime.as_ref(),
            &LiveWsConfig {
                max_total_subscriptions: 1,
                ..DEFAULT_LIVE_WS_CONFIG
            },
            SubscriptionMessage::SubscribeStatus {
                tx: existing_tx.clone(),
                response_tx: None,
            },
        )
        .unwrap();

        let (sub_tx, sub_rx) = mpsc::channel(4);
        let (_dispatcher_live_ws_tx, dispatcher_live_ws_rx) = watch::channel(LiveWsConfig {
            max_total_subscriptions: 1,
            ..DEFAULT_LIVE_WS_CONFIG
        });
        let dispatcher =
            spawn_subscription_dispatcher(runtime.clone(), sub_rx, dispatcher_live_ws_rx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ws_config = LiveWsConfig {
            max_subscriptions_per_connection: 1,
            idle_timeout_secs: 0,
            ..DEFAULT_LIVE_WS_CONFIG
        };
        let (_live_ws_tx, live_ws_rx) = watch::channel(ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({
            let runtime = runtime.clone();
            let trees = trees.clone();
            let sub_tx = sub_tx.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                handle_connection(
                    runtime,
                    stream,
                    peer_addr,
                    trees,
                    sub_tx,
                    ConnectionSlotGuard::new(connection_count),
                    ws_config.subscription_buffer_size,
                    live_ws_rx,
                )
                .await
            }
        });

        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let first_request = serde_json::json!({
            "id": 18,
            "type": "SubscribeEvents",
            "key": {"type": "Custom", "value": {"name": "pool_id", "kind": "u32", "value": 1}}
        });
        client
            .send(Message::Text(first_request.to_string().into()))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 18);
        assert_eq!(response["type"], "error");
        assert_eq!(response["data"]["code"], "subscription_limit");
        assert!(
            timeout(Duration::from_millis(50), client.next())
                .await
                .is_err()
        );

        process_sub_msg(
            runtime.as_ref(),
            &LiveWsConfig {
                max_total_subscriptions: 1,
                ..DEFAULT_LIVE_WS_CONFIG
            },
            SubscriptionMessage::UnsubscribeStatus {
                tx: existing_tx,
                response_tx: None,
            },
        )
        .unwrap();

        let second_request = serde_json::json!({
            "id": 19,
            "type": "SubscribeEvents",
            "key": {"type": "Custom", "value": {"name": "pool_id", "kind": "u32", "value": 2}}
        });
        client
            .send(Message::Text(second_request.to_string().into()))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 19);
        assert_eq!(response["type"], "subscriptionStatus");
        assert_eq!(response["data"]["action"], "subscribed");

        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
        drop(sub_tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn idle_timeout_zero_disables_connection_timeout() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = disconnected_runtime();
        let ws_config = LiveWsConfig {
            idle_timeout_secs: 0,
            ..DEFAULT_LIVE_WS_CONFIG
        };
        let (_live_ws_tx, live_ws_rx) = watch::channel(ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(
                runtime,
                stream,
                peer_addr,
                trees,
                sub_tx,
                ConnectionSlotGuard::new(connection_count),
                ws_config.subscription_buffer_size,
                live_ws_rx,
            )
            .await
        });

        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();

        sleep(Duration::from_millis(25)).await;
        client
            .send(Message::Text(
                serde_json::json!({"id": 20, "type": "Status"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let response = timeout(Duration::from_millis(100), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 20);
        assert_eq!(response["type"], "status");

        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn existing_connection_observes_live_subscription_limit_changes() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let (sub_tx, sub_rx) = mpsc::channel(4);
        let (live_ws_tx, live_ws_rx) = watch::channel(DEFAULT_LIVE_WS_CONFIG);
        let dispatcher = spawn_subscription_dispatcher(runtime.clone(), sub_rx, live_ws_rx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let initial_ws_config = LiveWsConfig {
            max_subscriptions_per_connection: 2,
            idle_timeout_secs: 0,
            ..DEFAULT_LIVE_WS_CONFIG
        };
        live_ws_tx.send(initial_ws_config).unwrap();
        let live_ws_rx = live_ws_tx.subscribe();
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({
            let runtime = runtime.clone();
            let trees = trees.clone();
            let sub_tx = sub_tx.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                handle_connection(
                    runtime,
                    stream,
                    peer_addr,
                    trees,
                    sub_tx,
                    ConnectionSlotGuard::new(connection_count),
                    initial_ws_config.subscription_buffer_size,
                    live_ws_rx,
                )
                .await
            }
        });

        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let first_request = serde_json::json!({
            "id": 21,
            "type": "SubscribeEvents",
            "key": {"type": "Custom", "value": {"name": "pool_id", "kind": "u32", "value": 1}}
        });
        client
            .send(Message::Text(first_request.to_string().into()))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 21);
        assert_eq!(response["type"], "subscriptionStatus");

        live_ws_tx
            .send(LiveWsConfig {
                max_subscriptions_per_connection: 1,
                ..initial_ws_config
            })
            .unwrap();
        sleep(Duration::from_millis(25)).await;

        let second_request = serde_json::json!({
            "id": 22,
            "type": "SubscribeStatus"
        });
        client
            .send(Message::Text(second_request.to_string().into()))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 22);
        assert_eq!(response["type"], "error");
        assert_eq!(response["data"]["code"], "subscription_limit");

        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
        drop(sub_tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn existing_connection_observes_live_max_events_limit_changes() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(7),
        });
        key.write_db_key(&trees, 10, 1).unwrap();
        key.write_db_key(&trees, 11, 1).unwrap();

        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let initial_ws_config = LiveWsConfig {
            idle_timeout_secs: 0,
            max_events_limit: 100,
            ..DEFAULT_LIVE_WS_CONFIG
        };
        let (live_ws_tx, live_ws_rx) = watch::channel(initial_ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({
            let runtime = runtime.clone();
            let trees = trees.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                handle_connection(
                    runtime,
                    stream,
                    peer_addr,
                    trees,
                    sub_tx,
                    ConnectionSlotGuard::new(connection_count),
                    initial_ws_config.subscription_buffer_size,
                    live_ws_rx,
                )
                .await
            }
        });

        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        live_ws_tx
            .send(LiveWsConfig {
                max_events_limit: 1,
                ..initial_ws_config
            })
            .unwrap();
        sleep(Duration::from_millis(25)).await;

        let request = serde_json::json!({
            "id": 23,
            "type": "GetEvents",
            "key": {"type": "Custom", "value": {"name": "ref_index", "kind": "u32", "value": 7}},
            "limit": 100
        });
        client
            .send(Message::Text(request.to_string().into()))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value =
            serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 23);
        assert_eq!(response["type"], "events");
        assert_eq!(response["data"]["events"].as_array().unwrap().len(), 1);

        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }
}
