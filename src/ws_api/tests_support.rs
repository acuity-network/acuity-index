use crate::indexer::process_sub_msg;
use crate::protocol::*;
use crate::runtime_state::RuntimeState;

use std::sync::Arc;
use subxt::{
    PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{
        client::{RawRpcFuture, RawRpcSubscription, RawValue, RpcClient, RpcClientT},
        methods::legacy::LegacyRpcMethods,
    },
};
use tokio::sync::{mpsc, watch};

pub(crate) const DEFAULT_WS_CONFIG: WsConfig = WsConfig {
    max_connections: 1024,
    max_total_subscriptions: 65536,
    max_subscriptions_per_connection: 128,
    subscription_buffer_size: 256,
    subscription_control_buffer_size: 1024,
    idle_timeout_secs: 300,
    max_events_limit: 1000,
};

pub(crate) const DEFAULT_LIVE_WS_CONFIG: LiveWsConfig = LiveWsConfig {
    max_connections: DEFAULT_WS_CONFIG.max_connections,
    max_total_subscriptions: DEFAULT_WS_CONFIG.max_total_subscriptions,
    max_subscriptions_per_connection: DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
    subscription_buffer_size: DEFAULT_WS_CONFIG.subscription_buffer_size,
    idle_timeout_secs: DEFAULT_WS_CONFIG.idle_timeout_secs,
    max_events_limit: DEFAULT_WS_CONFIG.max_events_limit,
};

pub(crate) fn temp_trees() -> Trees {
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

pub(crate) fn runtime_with_rpc() -> Arc<RuntimeState> {
    let runtime = Arc::new(RuntimeState::new(DEFAULT_WS_CONFIG.max_total_subscriptions));
    runtime.set_rpc(Some(mock_rpc_methods()));
    runtime
}

pub(crate) fn disconnected_runtime() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::new(DEFAULT_WS_CONFIG.max_total_subscriptions))
}

pub(crate) fn spawn_subscription_dispatcher(
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
