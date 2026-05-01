use crate::{errors::IndexError, protocol::*, runtime_state::RuntimeState};

use futures::{SinkExt, StreamExt};
use std::{collections::HashMap, net::SocketAddr, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tokio::net::{TcpStream};
use tokio::sync::{mpsc, mpsc::Sender, watch};
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::{self};
use tracing::{error, info};

use super::{
    disconnect_error,
    listener::websocket_config,
    requests::{
        enqueue_subscription_message, process_msg,
    },
    validation::validate_subscription_request,
};

fn idle_deadline_for(
    last_activity: time::Instant,
    idle_timeout_secs: u64,
) -> Option<time::Instant> {
    (idle_timeout_secs != 0).then(|| last_activity + Duration::from_secs(idle_timeout_secs))
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

struct ConnectionMetricGuard {
    runtime: Arc<RuntimeState>,
}

impl ConnectionMetricGuard {
    fn new(runtime: Arc<RuntimeState>) -> Self {
        runtime.metrics.inc_ws_connections();
        Self { runtime }
    }
}

pub(crate) struct ConnectionSlotGuard {
    connection_count: Arc<AtomicUsize>,
}

impl ConnectionSlotGuard {
    pub(crate) fn new(connection_count: Arc<AtomicUsize>) -> Self {
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

pub(crate) async fn handle_connection(
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
    // Track subscriptions by subscription ID
    let mut status_subscription: Option<String> = None;
    let mut event_subscriptions: HashMap<String, Key> = HashMap::new();
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
                        match serde_json::from_str::<JsonRpcRequest>(msg.to_text()?) {
                            Ok(request) => {
                                // Validate JSON-RPC envelope
                                if request.jsonrpc != "2.0" {
                                    let response = jsonrpc_invalid_request("jsonrpc must be \"2.0\"");
                                    send_json_message(&mut ws_sender, &response).await?;
                                    continue;
                                }

                                // Validate subscription limits for subscribe methods
                                if let Err(err) = validate_subscription_request(
                                    status_subscription.is_some(),
                                    &event_subscriptions.values().cloned().collect(),
                                    &request.method,
                                    live_ws_config.max_subscriptions_per_connection,
                                ) {
                                    let response = jsonrpc_subscription_limit(request.id, err.to_string());
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
                                    Err(err) => jsonrpc_internal_error(request.id, err.to_string()),
                                };

                                // Track subscription state changes
                                update_subscription_state(
                                    &mut status_subscription,
                                    &mut event_subscriptions,
                                    &request.method,
                                    &request.params,
                                    &response,
                                );

                                send_json_message(&mut ws_sender, &response).await?;
                            }
                            Err(err) => {
                                error!("Parse error: {err}");
                                let response = jsonrpc_parse_error(err.to_string());
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

    // Cleanup subscriptions on disconnect
    if status_subscription.is_some() {
        let _ = enqueue_subscription_message(
            &sub_tx,
            SubscriptionMessage::UnsubscribeStatus {
                tx: sub_events_tx.clone(),
                response_tx: None,
            },
        );
    }
    for (_sub_id, _key) in event_subscriptions {
        let _ = enqueue_subscription_message(
            &sub_tx,
            SubscriptionMessage::UnsubscribeEvents {
                key: Key::Variant(0, 0), // Placeholder - dispatcher uses channel identity
                tx: sub_events_tx.clone(),
                response_tx: None,
            },
        );
    }

    Ok(())
}

/// Update local subscription tracking state based on method and response.
fn update_subscription_state(
    status_subscription: &mut Option<String>,
    event_subscriptions: &mut HashMap<String, Key>,
    method: &str,
    params: &serde_json::Value,
    response: &JsonRpcResponse,
) {
    // Only track successful subscribe/unsubscribe responses
    let JsonRpcResponse::Success(success) = response else { return };

    match method {
        "acuity_subscribeStatus" => {
            if let Some(sub_id) = success.result.as_str() {
                *status_subscription = Some(sub_id.to_string());
            }
        }
        "acuity_unsubscribeStatus" => {
            *status_subscription = None;
        }
        "acuity_subscribeEvents" => {
            if let Some(sub_id) = success.result.as_str() {
                if let Ok(p) = serde_json::from_value::<SubscribeEventsParams>(params.clone()) {
                    event_subscriptions.insert(sub_id.to_string(), p.key);
                }
            }
        }
        "acuity_unsubscribeEvents" => {
            if let Some(sub_id) = params.get("subscription").and_then(|v| v.as_str()) {
                event_subscriptions.remove(sub_id);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws_api::tests_support::{
        DEFAULT_LIVE_WS_CONFIG, disconnected_runtime, runtime_with_rpc,
        spawn_subscription_dispatcher, temp_trees,
    };
    use futures::{SinkExt, StreamExt};
    use std::sync::{Arc, atomic::AtomicUsize};
    use tokio::{net::TcpListener, sync::{mpsc, watch}, time::{Duration, sleep, timeout}};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    #[tokio::test]
    async fn websocket_request_returns_invalid_params_for_malicious_composite() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = runtime_with_rpc();
        let (_live_ws_tx, live_ws_rx) = watch::channel(DEFAULT_LIVE_WS_CONFIG);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), DEFAULT_LIVE_WS_CONFIG.subscription_buffer_size, live_ws_rx).await
        });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let request = format!(r#"{{"jsonrpc":"2.0","id":15,"method":"acuity_getEvents","params":{{"key":{{"type":"Custom","value":{{"name":"too_many","kind":"composite","value":[{}]}}}},"limit":100}}}}"#, std::iter::repeat_n(r#"{"kind":"u32","value":1}"#, crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS + 1).collect::<Vec<_>>().join(","));
        client.send(Message::Text(request.into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 15);
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["error"]["data"]["reason"], "invalid_key");
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn rejected_subscribe_does_not_consume_connection_slot() {
        let trees = temp_trees();
        let runtime = Arc::new(RuntimeState::new(1));
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        crate::indexer::process_sub_msg(runtime.as_ref(), &LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG }, SubscriptionMessage::SubscribeStatus { tx: existing_tx.clone(), response_tx: None }).unwrap();
        let (sub_tx, sub_rx) = mpsc::channel(4);
        let (_dispatcher_live_ws_tx, dispatcher_live_ws_rx) = watch::channel(LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG });
        let dispatcher = spawn_subscription_dispatcher(runtime.clone(), sub_rx, dispatcher_live_ws_rx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ws_config = LiveWsConfig { max_subscriptions_per_connection: 1, idle_timeout_secs: 0, ..DEFAULT_LIVE_WS_CONFIG };
        let (_live_ws_tx, live_ws_rx) = watch::channel(ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({ let runtime = runtime.clone(); let trees = trees.clone(); let sub_tx = sub_tx.clone(); async move { let (stream, peer_addr) = listener.accept().await.unwrap(); handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), ws_config.subscription_buffer_size, live_ws_rx).await } });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let first_request = serde_json::json!({"jsonrpc":"2.0","id":18,"method":"acuity_subscribeEvents","params":{"key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":1}}}});
        client.send(Message::Text(first_request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert!(response.get("error").is_some());
        crate::indexer::process_sub_msg(runtime.as_ref(), &LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG }, SubscriptionMessage::UnsubscribeStatus { tx: existing_tx, response_tx: None }).unwrap();
        let second_request = serde_json::json!({"jsonrpc":"2.0","id":19,"method":"acuity_subscribeEvents","params":{"key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":2}}}});
        client.send(Message::Text(second_request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert!(response.get("result").is_some());
        assert!(response["result"].as_str().unwrap().starts_with("sub_"));
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
        let ws_config = LiveWsConfig { idle_timeout_secs: 0, ..DEFAULT_LIVE_WS_CONFIG };
        let (_live_ws_tx, live_ws_rx) = watch::channel(ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move { let (stream, peer_addr) = listener.accept().await.unwrap(); handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), ws_config.subscription_buffer_size, live_ws_rx).await });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        sleep(Duration::from_millis(25)).await;
        client.send(Message::Text(serde_json::json!({"jsonrpc":"2.0","id":20,"method":"acuity_indexStatus"}).to_string().into())).await.unwrap();
        let response = timeout(Duration::from_millis(100), client.next()).await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.unwrap().to_text().unwrap()).unwrap();
        assert!(response.get("result").is_some());
        assert!(response["result"]["spans"].is_array());
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
        let initial_ws_config = LiveWsConfig { max_subscriptions_per_connection: 2, idle_timeout_secs: 0, ..DEFAULT_LIVE_WS_CONFIG };
        live_ws_tx.send(initial_ws_config).unwrap();
        let live_ws_rx = live_ws_tx.subscribe();
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({ let runtime = runtime.clone(); let trees = trees.clone(); let sub_tx = sub_tx.clone(); async move { let (stream, peer_addr) = listener.accept().await.unwrap(); handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), initial_ws_config.subscription_buffer_size, live_ws_rx).await } });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        client.send(Message::Text(serde_json::json!({"jsonrpc":"2.0","id":21,"method":"acuity_subscribeEvents","params":{"key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":1}}}}).to_string().into())).await.unwrap();
        let _ = client.next().await.unwrap().unwrap();
        live_ws_tx.send(LiveWsConfig { max_subscriptions_per_connection: 1, ..initial_ws_config }).unwrap();
        sleep(Duration::from_millis(25)).await;
        client.send(Message::Text(serde_json::json!({"jsonrpc":"2.0","id":22,"method":"acuity_subscribeStatus"}).to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert!(response.get("error").is_some());
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
        drop(sub_tx);
        dispatcher.await.unwrap();
    }

    #[tokio::test]
    async fn get_events_returns_temporarily_unavailable_without_runtime_clients() {
        let trees = temp_trees();
        let runtime = disconnected_runtime();
        let key = Key::Custom(CustomKey { name: "ref_index".into(), value: CustomValue::U32(7) });
        key.write_db_key(&trees, 10, 1).unwrap();
        key.write_db_key(&trees, 11, 1).unwrap();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let initial_ws_config = LiveWsConfig { idle_timeout_secs: 0, max_events_limit: 100, ..DEFAULT_LIVE_WS_CONFIG };
        let (live_ws_tx, live_ws_rx) = watch::channel(initial_ws_config);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({ let runtime = runtime.clone(); let trees = trees.clone(); async move { let (stream, peer_addr) = listener.accept().await.unwrap(); handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), initial_ws_config.subscription_buffer_size, live_ws_rx).await } });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        live_ws_tx.send(LiveWsConfig { max_events_limit: 1, ..initial_ws_config }).unwrap();
        sleep(Duration::from_millis(25)).await;
        let request = serde_json::json!({"jsonrpc":"2.0","id":23,"method":"acuity_getEvents","params":{"key":{"type":"Custom","value":{"name":"ref_index","kind":"u32","value":7}},"limit":100}});
        client.send(Message::Text(request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["error"]["code"], -32001);
        assert_eq!(response["error"]["data"]["reason"], "temporarily_unavailable");
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn invalid_jsonrpc_version_returns_error() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = disconnected_runtime();
        let (_live_ws_tx, live_ws_rx) = watch::channel(DEFAULT_LIVE_WS_CONFIG);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), DEFAULT_LIVE_WS_CONFIG.subscription_buffer_size, live_ws_rx).await
        });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let request = serde_json::json!({"jsonrpc":"1.0","id":1,"method":"acuity_indexStatus"});
        client.send(Message::Text(request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["error"]["code"], -32600);
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn method_not_found_returns_error() {
        let trees = temp_trees();
        let (sub_tx, _sub_rx) = mpsc::channel(4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = disconnected_runtime();
        let (_live_ws_tx, live_ws_rx) = watch::channel(DEFAULT_LIVE_WS_CONFIG);
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), DEFAULT_LIVE_WS_CONFIG.subscription_buffer_size, live_ws_rx).await
        });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let request = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"acuity_nonexistent"});
        client.send(Message::Text(request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["error"]["code"], -32601);
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }
}
