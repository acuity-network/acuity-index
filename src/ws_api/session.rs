use crate::{errors::IndexError, protocol::*, runtime_state::RuntimeState};

use futures::{SinkExt, StreamExt};
use std::{collections::HashSet, net::SocketAddr, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tokio::net::{TcpStream};
use tokio::sync::{mpsc, mpsc::Sender, watch};
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::{self};
use tracing::{error, info};

use super::{
    disconnect_error,
    listener::websocket_config,
    requests::{
        error_response, enqueue_subscription_message, process_msg, subscription_limit_response,
        uncorrelated_error_response,
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
            handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), DEFAULT_LIVE_WS_CONFIG.subscription_buffer_size, live_ws_rx).await
        });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        let request = format!(r#"{{"id":15,"type":"GetEvents","key":{{"type":"Custom","value":{{"name":"too_many","kind":"composite","value":[{}]}}}},"limit":100}}"#, std::iter::repeat_n(r#"{"kind":"u32","value":1}"#, crate::ws_api::validation::MAX_COMPOSITE_ELEMENTS + 1).collect::<Vec<_>>().join(","));
        client.send(Message::Text(request.into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], 15);
        assert_eq!(response["type"], "error");
        assert_eq!(response["data"]["code"], "invalid_request");
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
        let first_request = serde_json::json!({"id":18,"type":"SubscribeEvents","key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":1}}});
        client.send(Message::Text(first_request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["type"], "error");
        crate::indexer::process_sub_msg(runtime.as_ref(), &LiveWsConfig { max_total_subscriptions: 1, ..DEFAULT_LIVE_WS_CONFIG }, SubscriptionMessage::UnsubscribeStatus { tx: existing_tx, response_tx: None }).unwrap();
        let second_request = serde_json::json!({"id":19,"type":"SubscribeEvents","key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":2}}});
        client.send(Message::Text(second_request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["type"], "subscriptionStatus");
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
        client.send(Message::Text(serde_json::json!({"id":20,"type":"Status"}).to_string().into())).await.unwrap();
        let response = timeout(Duration::from_millis(100), client.next()).await.unwrap().unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
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
        let initial_ws_config = LiveWsConfig { max_subscriptions_per_connection: 2, idle_timeout_secs: 0, ..DEFAULT_LIVE_WS_CONFIG };
        live_ws_tx.send(initial_ws_config).unwrap();
        let live_ws_rx = live_ws_tx.subscribe();
        let connection_count = Arc::new(AtomicUsize::new(1));
        let server = tokio::spawn({ let runtime = runtime.clone(); let trees = trees.clone(); let sub_tx = sub_tx.clone(); async move { let (stream, peer_addr) = listener.accept().await.unwrap(); handle_connection(runtime, stream, peer_addr, trees, sub_tx, ConnectionSlotGuard::new(connection_count), initial_ws_config.subscription_buffer_size, live_ws_rx).await } });
        let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
        client.send(Message::Text(serde_json::json!({"id":21,"type":"SubscribeEvents","key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":1}}}).to_string().into())).await.unwrap();
        let _ = client.next().await.unwrap().unwrap();
        live_ws_tx.send(LiveWsConfig { max_subscriptions_per_connection: 1, ..initial_ws_config }).unwrap();
        sleep(Duration::from_millis(25)).await;
        client.send(Message::Text(serde_json::json!({"id":22,"type":"SubscribeStatus"}).to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["type"], "error");
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
        let request = serde_json::json!({"id":23,"type":"GetEvents","key":{"type":"Custom","value":{"name":"ref_index","kind":"u32","value":7}},"limit":100});
        client.send(Message::Text(request.to_string().into())).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(response["data"]["code"], "temporarily_unavailable");
        client.close(None).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }
}
