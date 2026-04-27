use crate::{errors::IndexError, protocol::*, runtime_state::RuntimeState};

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc::Sender, watch, watch::Receiver};
use tokio_tungstenite::tungstenite::{
    handshake::server::{Request, Response},
    http::{Response as HttpResponse, StatusCode},
    protocol::WebSocketConfig,
};
use tracing::{error, info};

use super::{session::{ConnectionSlotGuard, handle_connection}, validation::{MAX_WS_FRAME_SIZE_BYTES, MAX_WS_MESSAGE_SIZE_BYTES}};

pub(crate) fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(MAX_WS_MESSAGE_SIZE_BYTES);
    config.max_frame_size = Some(MAX_WS_FRAME_SIZE_BYTES);
    config
}

pub(crate) fn service_unavailable_response(message: &str) -> HttpResponse<Option<String>> {
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
    use crate::ws_api::tests_support::{DEFAULT_LIVE_WS_CONFIG, DEFAULT_WS_CONFIG};

    #[test]
    fn websocket_config_sets_explicit_message_and_frame_limits() {
        let config = websocket_config();

        assert_eq!(config.max_message_size, Some(super::MAX_WS_MESSAGE_SIZE_BYTES));
        assert_eq!(config.max_frame_size, Some(super::MAX_WS_FRAME_SIZE_BYTES));
        let _ = (DEFAULT_WS_CONFIG, DEFAULT_LIVE_WS_CONFIG);
    }

    #[test]
    fn service_unavailable_response_is_http_503() {
        let response = service_unavailable_response("busy");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.body().as_deref(), Some("busy"));
    }
}
