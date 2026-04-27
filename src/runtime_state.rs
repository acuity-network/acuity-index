use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};

use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::sync::mpsc;
use tracing::error;

use crate::metrics::Metrics;
use crate::protocol::{Key, NotificationMessage};

pub fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!("Recovering poisoned mutex: {name}");
            poisoned.into_inner()
        }
    }
}

pub struct RuntimeState {
    pub(crate) status_subs: Mutex<Vec<mpsc::Sender<NotificationMessage>>>,
    pub(crate) events_subs: Mutex<HashMap<Key, Vec<mpsc::Sender<NotificationMessage>>>>,
    pub(crate) metrics: Arc<Metrics>,
    api: Mutex<Option<OnlineClient<PolkadotConfig>>>,
    rpc: Mutex<Option<LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>>>,
    finalized_mode: AtomicBool,
}

impl RuntimeState {
    #[cfg(test)]
    pub fn new(max_total_subscriptions: usize) -> Self {
        Self::with_metrics(max_total_subscriptions, Arc::new(Metrics::new()))
    }

    pub fn with_metrics(_max_total_subscriptions: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            status_subs: Mutex::new(Vec::new()),
            events_subs: Mutex::new(HashMap::new()),
            metrics,
            api: Mutex::new(None),
            rpc: Mutex::new(None),
            finalized_mode: AtomicBool::new(false),
        }
    }

    pub fn set_api(&self, api: Option<OnlineClient<PolkadotConfig>>) {
        *lock_or_recover(&self.api, "runtime_api") = api;
    }

    pub fn api(&self) -> Option<OnlineClient<PolkadotConfig>> {
        lock_or_recover(&self.api, "runtime_api").clone()
    }

    pub fn set_rpc(&self, rpc: Option<LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>>) {
        *lock_or_recover(&self.rpc, "runtime_rpc") = rpc;
    }

    pub fn rpc(&self) -> Option<LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>> {
        lock_or_recover(&self.rpc, "runtime_rpc").clone()
    }

    pub fn clients(
        &self,
    ) -> Option<(
        OnlineClient<PolkadotConfig>,
        LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    )> {
        Some((self.api()?, self.rpc()?))
    }

    pub fn set_finalized_mode(&self, finalized_mode: bool) {
        self.finalized_mode.store(finalized_mode, Ordering::Relaxed);
    }

    pub fn finalized_mode(&self) -> bool {
        self.finalized_mode.load(Ordering::Relaxed)
    }
}
