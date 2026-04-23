//! Core block indexer — schema-less, config-driven.

use ahash::AHashMap;
use futures::future;
use num_format::{Locale, ToFormattedString};
use scale_value::{Composite, Value, ValueDef};
use serde_json::json;
use sled::{Batch, Tree};
use std::{collections::HashMap, sync::Arc};
use subxt::{
    OnlineClient, PolkadotConfig,
    client::Block,
    config::RpcConfigFor,
    error::{BackendError, OnlineClientAtBlockError, RpcError},
    events::Events,
    rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::{
    sync::{mpsc, watch},
    task,
    time::{self, Duration, Instant, MissedTickBehavior},
};
use tracing::{debug, error, info};
use zerocopy::IntoBytes;

use crate::{
    config::{IndexSpec, ParamKey, ResolvedParamConfig, ScalarKind},
    shared::*,
    websockets::process_msg_status,
};

// ─── Indexer struct ───────────────────────────────────────────────────────────

pub struct Indexer {
    pub trees: Trees,
    api: Option<OnlineClient<PolkadotConfig>>,
    rpc: Option<LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>>,
    runtime: Arc<RuntimeState>,
    processing_ctx: Arc<BlockProcessingContext>,
}

#[derive(Clone)]
struct BlockProcessingContext {
    index_variant: bool,
    store_events: bool,
    event_index: HashMap<String, HashMap<String, Vec<ResolvedParamConfig>>>,
}

struct FetchedBlock {
    block_number: u32,
    spec_version: u32,
    events: Events<PolkadotConfig>,
}

struct ProcessedBlock {
    block_number: u32,
    event_count: u32,
    key_count: u32,
    events: Vec<ProcessedEvent>,
}

#[derive(Debug, Clone)]
struct ProcessedEvent {
    event_index: u32,
    variant_key: Option<Key>,
    keys: Vec<Key>,
    stored_event_json: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
struct DerivedEvent {
    variant_key: Option<Key>,
    keys: Vec<Key>,
    stored_event_json: Option<Vec<u8>>,
}

impl BlockProcessingContext {
    fn new(config: &IndexSpec) -> Self {
        Self {
            index_variant: config.index_variant,
            store_events: config.store_events,
            event_index: config.build_event_index().expect("validated index spec"),
        }
    }

    fn keys_for_event(
        &self,
        pallet_name: &str,
        event_name: &str,
        fields: &Composite<()>,
    ) -> Vec<Key> {
        if let Some(event_map) = self.event_index.get(pallet_name) {
            if let Some(params) = event_map.get(event_name) {
                return self.keys_from_params(params, fields);
            }
        }
        vec![]
    }

    fn keys_from_params(&self, params: &[ResolvedParamConfig], fields: &Composite<()>) -> Vec<Key> {
        let mut keys = Vec::new();
        for param in params {
            if param.multi {
                let Some(field) = param.fields.first() else {
                    continue;
                };
                let value = match get_field(fields, field) {
                    Some(v) => v,
                    None => continue,
                };
                keys.extend(values_to_keys(value, &param.key));
            } else if let Some(key) = values_to_key(fields, &param.fields, &param.key) {
                keys.push(key);
            }
        }
        keys
    }

    fn derive_event(
        &self,
        spec_version: u32,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        event_index: u32,
        fields: &Composite<()>,
    ) -> Result<DerivedEvent, IndexError> {
        let variant_key = self
            .index_variant
            .then_some(Key::Variant(pallet_index, variant_index));
        let keys = self.keys_for_event(pallet_name, event_name, fields);
        let should_store_event = self.index_variant || !keys.is_empty();
        let stored_event_json = if self.store_events && should_store_event {
            Some(encode_event_json(
                spec_version,
                pallet_name,
                event_name,
                pallet_index,
                variant_index,
                event_index,
                fields,
            )?)
        } else {
            None
        };

        Ok(DerivedEvent {
            variant_key,
            keys,
            stored_event_json,
        })
    }
}

fn extract_u32(v: &Value<()>) -> Option<u32> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => u32::try_from(*n).ok(),
                Primitive::I128(n) => u32::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u32(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u32(&fields[0].1)
        }
        _ => None,
    }
}

fn extract_u64(v: &Value<()>) -> Option<u64> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => u64::try_from(*n).ok(),
                Primitive::I128(n) => u64::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u64(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u64(&fields[0].1)
        }
        _ => None,
    }
}

fn extract_u128(v: &Value<()>) -> Option<u128> {
    match &v.value {
        ValueDef::Primitive(p) => {
            use scale_value::Primitive;
            match p {
                Primitive::U128(n) => Some(*n),
                Primitive::I128(n) => u128::try_from(*n).ok(),
                _ => None,
            }
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_u128(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_u128(&fields[0].1)
        }
        _ => None,
    }
}

fn extract_string(v: &Value<()>) -> Option<String> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::String(s)) => Some(s.clone()),
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_string(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_string(&fields[0].1)
        }
        _ => None,
    }
}

fn extract_bool(v: &Value<()>) -> Option<bool> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::Bool(value)) => Some(*value),
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_bool(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_bool(&fields[0].1)
        }
        _ => None,
    }
}

fn extract_bytes32(v: &Value<()>) -> Option<[u8; 32]> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 32 => {
            let mut out = [0u8; 32];
            for (index, field) in fields.iter().enumerate() {
                match &field.value {
                    ValueDef::Primitive(scale_value::Primitive::U128(byte)) => {
                        out[index] = u8::try_from(*byte).ok()?;
                    }
                    _ => return None,
                }
            }
            Some(out)
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() == 1 => {
            extract_bytes32(&fields[0])
        }
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            extract_bytes32(&fields[0].1)
        }
        _ => None,
    }
}

fn get_field<'a>(composite: &'a Composite<()>, field: &str) -> Option<&'a Value<()>> {
    if let Ok(index) = field.parse::<usize>() {
        return match composite {
            Composite::Unnamed(fields) => fields.get(index),
            Composite::Named(fields) => fields.get(index).map(|(_, value)| value),
        };
    }

    match composite {
        Composite::Named(fields) => fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value),
        Composite::Unnamed(_) => None,
    }
}

impl Indexer {
    pub fn new(
        trees: Trees,
        api: OnlineClient<PolkadotConfig>,
        rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
        config: &IndexSpec,
        runtime: Arc<RuntimeState>,
    ) -> Self {
        Indexer {
            trees,
            api: Some(api),
            rpc: Some(rpc),
            runtime,
            processing_ctx: Arc::new(BlockProcessingContext::new(config)),
        }
    }

    #[cfg(test)]
    pub fn new_test_with_max_subs(
        trees: Trees,
        config: &IndexSpec,
        max_total_subscriptions: usize,
    ) -> Self {
        Indexer {
            trees,
            api: None,
            rpc: None,
            runtime: Arc::new(RuntimeState::new(max_total_subscriptions)),
            processing_ctx: Arc::new(BlockProcessingContext::new(config)),
        }
    }

    #[cfg(test)]
    pub fn new_test(trees: Trees, config: &IndexSpec) -> Self {
        Indexer {
            trees,
            api: None,
            rpc: None,
            runtime: Arc::new(RuntimeState::new(
                WsConfig::default().max_total_subscriptions,
            )),
            processing_ctx: Arc::new(BlockProcessingContext::new(config)),
        }
    }

    #[cfg(test)]
    pub fn runtime_state(&self) -> &RuntimeState {
        self.runtime.as_ref()
    }

    // ─── Block indexing ───────────────────────────────────────────────────────

    pub async fn next_head_block_number(
        next: impl Future<Output = Option<Result<Block<PolkadotConfig>, subxt::error::BlocksError>>>,
    ) -> Result<u32, IndexError> {
        let block = next.await.ok_or(IndexError::BlockStreamClosed)??;
        block
            .number()
            .try_into()
            .map_err(|_| internal_error("block number exceeds u32"))
    }

    fn runtime_clients(
        &self,
    ) -> Result<
        (
            &OnlineClient<PolkadotConfig>,
            &LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
        ),
        IndexError,
    > {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| internal_error("indexer API client not initialized"))?;
        let rpc = self
            .rpc
            .as_ref()
            .ok_or_else(|| internal_error("indexer RPC client not initialized"))?;
        Ok((api, rpc))
    }

    pub async fn index_block(&self, block_number: u32) -> Result<(u32, u32, u32), IndexError> {
        let fetch_started = Instant::now();
        let fetched = self.fetch_block_events(block_number).await?;
        let fetch_elapsed = fetch_started.elapsed();
        self.runtime.metrics.observe_block_fetch(fetch_elapsed);

        let process_started = Instant::now();
        let processing_ctx = Arc::clone(&self.processing_ctx);
        let processed =
            task::spawn_blocking(move || Self::process_block_cpu(processing_ctx, fetched))
                .await
                .map_err(|err| {
                    internal_error(format!("blocking event processor panicked: {err}"))
                })??;
        let process_elapsed = process_started.elapsed();
        self.runtime.metrics.observe_block_process(process_elapsed);

        let commit_started = Instant::now();
        let summary = self.commit_processed_block(processed)?;
        let commit_elapsed = commit_started.elapsed();
        self.runtime.metrics.observe_block_commit(commit_elapsed);
        self.runtime.metrics.add_indexed_block(summary.1, summary.2);

        debug!(
            "Block {block_number} stage timings: fetch={}ms cpu={}ms commit={}ms",
            fetch_elapsed.as_millis(),
            process_elapsed.as_millis(),
            commit_elapsed.as_millis(),
        );

        Ok(summary)
    }

    async fn fetch_block_events(&self, block_number: u32) -> Result<FetchedBlock, IndexError> {
        let (api, rpc) = self.runtime_clients()?;

        let block_hash = rpc
            .chain_get_block_hash(Some(block_number.into()))
            .await?
            .ok_or(IndexError::BlockNotFound(block_number))?;

        let at_block = api.at_block(block_hash).await.map_err(|err| {
            if is_state_pruned_error(&err) {
                IndexError::StatePruningMisconfigured { block_number }
            } else {
                err.into()
            }
        })?;
        let spec_version = at_block.spec_version();
        let events = at_block.events().fetch().await?;

        Ok(FetchedBlock {
            block_number,
            spec_version,
            events,
        })
    }

    fn process_block_cpu(
        processing_ctx: Arc<BlockProcessingContext>,
        fetched: FetchedBlock,
    ) -> Result<ProcessedBlock, IndexError> {
        let FetchedBlock {
            block_number,
            spec_version,
            events,
        } = fetched;

        let mut key_count = 0u32;
        let mut processed_events = Vec::new();

        for (i, event_result) in events.iter().enumerate() {
            let event = match event_result {
                Ok(e) => e,
                Err(err) => {
                    error!("Block {block_number}, event {i}: {err}");
                    continue;
                }
            };

            let event_index: u32 = i.try_into().map_err(|_| {
                internal_error(format!("event index exceeds u32 for block {block_number}"))
            })?;
            let pallet_name = event.pallet_name();
            let event_name = event.event_name();
            let pallet_index = event.pallet_index();
            let variant_index = event.event_index();

            let field_values: Composite<()> =
                match event.decode_fields_unchecked_as::<Composite<()>>() {
                    Ok(fv) => fv,
                    Err(err) => {
                        error!(
                            "Block {block_number} {pallet_name}::{event_name} \
                         field_values error: {err}"
                        );
                        continue;
                    }
                };

            let derived = processing_ctx.derive_event(
                spec_version,
                pallet_name,
                event_name,
                pallet_index,
                variant_index,
                event_index,
                &field_values,
            )?;

            key_count += derived.keys.len() as u32;
            if derived.variant_key.is_some() {
                key_count += 1;
            }
            processed_events.push(ProcessedEvent {
                event_index,
                variant_key: derived.variant_key,
                keys: derived.keys,
                stored_event_json: derived.stored_event_json,
            });
        }

        Ok(ProcessedBlock {
            block_number,
            event_count: events.len(),
            key_count,
            events: processed_events,
        })
    }

    fn commit_processed_block(
        &self,
        processed: ProcessedBlock,
    ) -> Result<(u32, u32, u32), IndexError> {
        let ProcessedBlock {
            block_number,
            event_count,
            key_count,
            events,
        } = processed;

        for event in events {
            if let Some(variant_key) = event.variant_key {
                self.index_event_key(variant_key, block_number, event.event_index)?;
            }

            for key in event.keys {
                self.index_event_key(key, block_number, event.event_index)?;
            }

            if let Some(json_bytes) = event.stored_event_json {
                self.store_encoded_event(block_number, event.event_index, &json_bytes)?;
            }
        }

        Ok((block_number, event_count, key_count))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn store_event(
        &self,
        block_number: u32,
        event_index: u32,
        spec_version: u32,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        fields: &Composite<()>,
    ) -> Result<(), IndexError> {
        if !self.processing_ctx.store_events {
            return Ok(());
        }

        let db_key = EventKey {
            block_number: block_number.into(),
            event_index: event_index.into(),
        };
        let json_bytes = encode_event_json(
            spec_version,
            pallet_name,
            event_name,
            pallet_index,
            variant_index,
            event_index,
            fields,
        )?;
        self.trees
            .events
            .insert(db_key.as_bytes(), json_bytes.as_slice())?;
        Ok(())
    }

    fn store_encoded_event(
        &self,
        block_number: u32,
        event_index: u32,
        json_bytes: &[u8],
    ) -> Result<(), IndexError> {
        let db_key = EventKey {
            block_number: block_number.into(),
            event_index: event_index.into(),
        };
        self.trees.events.insert(db_key.as_bytes(), json_bytes)?;
        Ok(())
    }

    // ─── Key derivation ───────────────────────────────────────────────────────

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn keys_for_event(
        &self,
        pallet_name: &str,
        event_name: &str,
        fields: &Composite<()>,
    ) -> Vec<Key> {
        self.processing_ctx
            .keys_for_event(pallet_name, event_name, fields)
    }

    // ─── Event encoding ───────────────────────────────────────────────────────

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn encode_event(
        &self,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        event_index: u32,
        fields: &Composite<()>,
    ) -> serde_json::Value {
        encode_event_value(
            pallet_name,
            event_name,
            pallet_index,
            variant_index,
            event_index,
            fields,
        )
    }
}

fn encode_event_value(
    pallet_name: &str,
    event_name: &str,
    pallet_index: u8,
    variant_index: u8,
    event_index: u32,
    fields: &Composite<()>,
) -> serde_json::Value {
    json!({
        "palletName": pallet_name,
        "eventName": event_name,
        "palletIndex": pallet_index,
        "variantIndex": variant_index,
        "eventIndex": event_index,
        "fields": composite_to_json(fields),
    })
}

fn encode_event_json(
    spec_version: u32,
    pallet_name: &str,
    event_name: &str,
    pallet_index: u8,
    variant_index: u8,
    event_index: u32,
    fields: &Composite<()>,
) -> Result<Vec<u8>, IndexError> {
    let mut encoded = encode_event_value(
        pallet_name,
        event_name,
        pallet_index,
        variant_index,
        event_index,
        fields,
    );
    encoded["specVersion"] = json!(spec_version);
    Ok(serde_json::to_vec(&encoded)?)
}

impl Indexer {
    // ─── DB write & notification ──────────────────────────────────────────────

    pub fn index_event_key(
        &self,
        key: Key,
        block_number: u32,
        event_index: u32,
    ) -> Result<(), IndexError> {
        key.write_db_key(&self.trees, block_number, event_index)?;
        self.notify_event_subscribers(
            key,
            EventRef {
                block_number,
                event_index,
            },
        );
        Ok(())
    }

    pub fn notify_status_subscribers(&self) {
        let msg = NotificationMessage {
            body: NotificationBody::Status(match process_msg_status(&self.trees.span) {
                ResponseBody::Status(spans) => spans,
                _ => unreachable!(),
            }),
        };
        let mut subs = lock_or_recover(&self.runtime.status_subs, "status_subs");
        subs.retain(|tx| keep_subscriber(tx, &msg));
    }

    fn notify_event_subscribers(&self, key: Key, event_ref: EventRef) {
        let mut subs = lock_or_recover(&self.runtime.events_subs, "events_subs");
        if let Some(txs) = subs.get_mut(&key) {
            let event_key = EventKey {
                block_number: event_ref.block_number.into(),
                event_index: event_ref.event_index.into(),
            };
            let decoded_events = self
                .trees
                .events
                .get(event_key.as_bytes())
                .ok()
                .flatten()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .map(|events| {
                    vec![DecodedEvent {
                        block_number: event_ref.block_number,
                        event_index: event_ref.event_index,
                        event: events,
                    }]
                })
                .unwrap_or_default();

            let notification = NotificationMessage {
                body: NotificationBody::EventNotification {
                    key: key.clone(),
                    event: event_ref,
                    decoded_event: decoded_events.into_iter().next(),
                },
            };
            txs.retain(|tx| keep_subscriber(tx, &notification));
        }
        if subs.get(&key).is_some_and(Vec::is_empty) {
            subs.remove(&key);
        }
    }
}

fn is_state_pruned_error(err: &OnlineClientAtBlockError) -> bool {
    match err {
        OnlineClientAtBlockError::CannotGetSpecVersion {
            reason: BackendError::Rpc(RpcError::ClientError(subxt::rpcs::Error::User(user_err))),
            ..
        } => user_err.code == 4003 && user_err.message.contains("State already discarded"),
        _ => false,
    }
}

fn keep_subscriber(tx: &mpsc::Sender<NotificationMessage>, msg: &NotificationMessage) -> bool {
    if tx.try_send(msg.clone()).is_ok() {
        return true;
    }

    error!("disconnecting slow WebSocket subscriber");
    let _ = tx.try_send(NotificationMessage {
        body: NotificationBody::SubscriptionTerminated {
            reason: SubscriptionTerminationReason::Backpressure,
            message: "subscriber disconnected due to backpressure".into(),
        },
    });
    false
}

// ─── scale_value → Key conversion ────────────────────────────────────────────

fn value_to_custom_value(value: &Value<()>, kind: &ScalarKind) -> Option<CustomValue> {
    match kind {
        ScalarKind::Bytes32 => extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ScalarKind::U32 => extract_u32(value).map(CustomValue::U32),
        ScalarKind::U64 => extract_u64(value).map(|v| CustomValue::U64(U64Text(v))),
        ScalarKind::U128 => extract_u128(value).map(|v| CustomValue::U128(U128Text(v))),
        ScalarKind::String => extract_string(value).map(CustomValue::String),
        ScalarKind::Bool => extract_bool(value).map(CustomValue::Bool),
    }
}

fn value_to_custom_key(value: &Value<()>, name: &str, kind: &ScalarKind) -> Option<Key> {
    let value = value_to_custom_value(value, kind)?;

    Some(Key::Custom(CustomKey {
        name: name.to_owned(),
        value,
    }))
}

fn values_to_key(fields: &Composite<()>, field_names: &[String], key: &ParamKey) -> Option<Key> {
    match key {
        ParamKey::Scalar { .. } => {
            let field = field_names.first()?;
            let value = get_field(fields, field)?;
            value_to_key(value, key)
        }
        ParamKey::Composite {
            name,
            fields: kinds,
        } => {
            let mut values = Vec::with_capacity(field_names.len());
            for (field_name, kind) in field_names.iter().zip(kinds) {
                let value = get_field(fields, field_name)?;
                values.push(value_to_custom_value(value, kind)?);
            }
            Some(Key::Custom(CustomKey {
                name: name.clone(),
                value: CustomValue::Composite(values),
            }))
        }
    }
}

fn value_to_key(value: &Value<()>, key: &ParamKey) -> Option<Key> {
    match key {
        ParamKey::Scalar { name, kind } => value_to_custom_key(value, name, kind),
        ParamKey::Composite { .. } => None,
    }
}

fn values_to_keys(value: &Value<()>, key: &ParamKey) -> Vec<Key> {
    match &value.value {
        ValueDef::Composite(Composite::Unnamed(values)) => values
            .iter()
            .filter_map(|value| value_to_key(value, key))
            .collect(),
        ValueDef::Composite(Composite::Named(values)) if values.len() == 1 => {
            values_to_keys(&values[0].1, key)
        }
        _ => value_to_key(value, key).into_iter().collect(),
    }
}

// ─── scale_value → serde_json ─────────────────────────────────────────────────

fn value_to_json(v: &Value<()>) -> serde_json::Value {
    use scale_value::{Primitive, ValueDef};
    match &v.value {
        ValueDef::Composite(c) => composite_to_json(c),
        ValueDef::Variant(var) => json!({
            "variant": var.name,
            "fields": composite_to_json(&var.values),
        }),
        ValueDef::BitSequence(_) => serde_json::Value::String("<bitseq>".to_string()),
        ValueDef::Primitive(p) => match p {
            Primitive::Bool(b) => json!(b),
            Primitive::Char(c) => {
                json!(c.to_string())
            }
            Primitive::String(s) => json!(s),
            Primitive::U128(n) => {
                // Represent as string to avoid JS precision loss.
                json!(n.to_string())
            }
            Primitive::I128(n) => json!(n.to_string()),
            Primitive::U256(n) => {
                json!(format!("0x{}", hex::encode(n)))
            }
            Primitive::I256(n) => {
                json!(format!("0x{}", hex::encode(n)))
            }
        },
    }
}

pub fn composite_to_json(c: &Composite<()>) -> serde_json::Value {
    match c {
        Composite::Named(fields) => {
            let map: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Composite::Unnamed(fields) => {
            // Detect byte arrays: all elements are U128 values ≤ 255
            if fields.len() > 1 {
                let as_bytes: Option<Vec<u8>> = fields
                    .iter()
                    .map(|v| match &v.value {
                        ValueDef::Primitive(scale_value::Primitive::U128(n)) => {
                            u8::try_from(*n).ok()
                        }
                        _ => None,
                    })
                    .collect();
                if let Some(bytes) = as_bytes {
                    return serde_json::Value::String(format!("0x{}", hex::encode(&bytes)));
                }
            }
            if fields.len() == 1 {
                value_to_json(&fields[0])
            } else {
                serde_json::Value::Array(fields.iter().map(value_to_json).collect())
            }
        }
    }
}

// ─── Subscription message handler ────────────────────────────────────────────

pub fn process_sub_msg(
    runtime: &RuntimeState,
    ws_config: &LiveWsConfig,
    msg: SubscriptionMessage,
) -> Result<(), IndexError> {
    match msg {
        SubscriptionMessage::SubscribeStatus { tx, response_tx } => {
            let mut status_subs = lock_or_recover(&runtime.status_subs, "status_subs");
            let events_count: usize = lock_or_recover(&runtime.events_subs, "events_subs")
                .values()
                .map(Vec::len)
                .sum();
            if status_subs.len() + events_count >= ws_config.max_total_subscriptions {
                let message = format!(
                    "total subscription limit exceeded: max {}",
                    ws_config.max_total_subscriptions
                );
                if let Some(response_tx) = response_tx {
                    let _ = response_tx.send(Err(message.clone()));
                }
                return Err(IndexError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    message,
                )));
            }
            if let Some(response_tx) = response_tx {
                let _ = response_tx.send(Ok(()));
            }
            status_subs.push(tx);
        }
        SubscriptionMessage::UnsubscribeStatus { tx, response_tx } => {
            lock_or_recover(&runtime.status_subs, "status_subs").retain(|t| !tx.same_channel(t));
            if let Some(response_tx) = response_tx {
                let _ = response_tx.send(Ok(()));
            }
        }
        SubscriptionMessage::SubscribeEvents {
            key,
            tx,
            response_tx,
        } => {
            let mut subs = lock_or_recover(&runtime.events_subs, "events_subs");
            let status_count = lock_or_recover(&runtime.status_subs, "status_subs").len();
            let current_events_count: usize = subs.values().map(Vec::len).sum();
            if status_count + current_events_count >= ws_config.max_total_subscriptions {
                let message = format!(
                    "total subscription limit exceeded: max {}",
                    ws_config.max_total_subscriptions
                );
                if let Some(response_tx) = response_tx {
                    let _ = response_tx.send(Err(message.clone()));
                }
                return Err(IndexError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    message,
                )));
            }
            if let Some(response_tx) = response_tx {
                let _ = response_tx.send(Ok(()));
            }
            subs.entry(key).or_default().push(tx);
        }
        SubscriptionMessage::UnsubscribeEvents {
            key,
            tx,
            response_tx,
        } => {
            let mut subs = lock_or_recover(&runtime.events_subs, "events_subs");
            if let Some(txs) = subs.get_mut(&key) {
                txs.retain(|t| !tx.same_channel(t));
            }
            if let Some(response_tx) = response_tx {
                let _ = response_tx.send(Ok(()));
            }
        }
    }
    update_subscription_metrics(runtime);
    Ok(())
}

fn update_subscription_metrics(runtime: &RuntimeState) {
    let status_subscriptions = lock_or_recover(&runtime.status_subs, "status_subs").len();
    let event_subscriptions: usize = lock_or_recover(&runtime.events_subs, "events_subs")
        .values()
        .map(Vec::len)
        .sum();
    runtime
        .metrics
        .set_status_subscriptions(status_subscriptions);
    runtime.metrics.set_event_subscriptions(event_subscriptions);
}

// ─── Span helpers ─────────────────────────────────────────────────────────────

pub fn load_spans(span_db: &Tree, spec_change_blocks: &[u32]) -> Result<Vec<Span>, IndexError> {
    let mut spans: Vec<Span> = vec![];
    'span: for (key, value) in span_db.into_iter().flatten() {
        let Some(span_value) = read_span_db_value(&value) else {
            error!("Skipping malformed span value during span load");
            continue;
        };
        let start: u32 = span_value.start.into();
        let Some(mut end) = decode_u32_key(key.as_ref()) else {
            error!("Skipping malformed span key during span load");
            continue;
        };
        let span_version: u16 = span_value.version.into();
        for (version, block_number) in spec_change_blocks.iter().enumerate() {
            let version_u16: u16 = version
                .try_into()
                .map_err(|_| internal_error("version index exceeds u16"))?;
            if span_version < version_u16 && end >= *block_number {
                let mut batch = Batch::default();
                batch.remove(&key);
                if start >= *block_number {
                    span_db.apply_batch(batch)?;
                    info!(
                        "📚 Re-indexing #{} to #{}.",
                        start.to_formatted_string(&Locale::en),
                        end.to_formatted_string(&Locale::en)
                    );
                    continue 'span;
                }
                info!(
                    "📚 Re-indexing #{} to #{}.",
                    block_number.to_formatted_string(&Locale::en),
                    end.to_formatted_string(&Locale::en)
                );
                end = block_number - 1;
                batch.insert(&end.to_be_bytes(), value.as_bytes());
                span_db.apply_batch(batch)?;
                break;
            }
        }
        let span = Span { start, end };
        if let Some(previous) = spans.last_mut() {
            if span.start <= previous.end.saturating_add(1) {
                previous.end = previous.end.max(span.end);
                continue;
            }
        }
        spans.push(span);
    }

    for span in &spans {
        info!(
            "📚 Previous indexed span #{} to #{}.",
            span.start.to_formatted_string(&Locale::en),
            span.end.to_formatted_string(&Locale::en)
        );
    }

    Ok(spans)
}

pub fn check_span(
    span_db: &Tree,
    spans: &mut Vec<Span>,
    current_span: &mut Span,
) -> Result<(), IndexError> {
    while let Some(span) = spans.last() {
        if let Some(prev_start) = prev_block(current_span.start) {
            if current_span.start > span.start && prev_start <= span.end {
                let skipped = span.end - span.start + 1;
                info!(
                    "📚 Skipping {} blocks #{} to #{}",
                    skipped.to_formatted_string(&Locale::en),
                    span.start.to_formatted_string(&Locale::en),
                    span.end.to_formatted_string(&Locale::en),
                );
                current_span.start = span.start;
                span_db.remove(span.end.to_be_bytes())?;
                spans.pop();
            } else {
                break;
            }
        } else {
            break;
        }
    }
    Ok(())
}

fn prev_block(block_number: u32) -> Option<u32> {
    block_number.checked_sub(1)
}

pub fn check_next_batch_block(spans: &[Span], next: &mut Option<u32>) {
    let mut i = spans.len();
    while i != 0 {
        i -= 1;
        let Some(block_number) = *next else {
            break;
        };
        if block_number >= spans[i].start && block_number <= spans[i].end {
            *next = prev_block(spans[i].start);
        }
    }
}

fn save_span(span_db: &Tree, span: &Span, spec_change_blocks_len: usize) -> Result<(), IndexError> {
    let value = SpanDbValue {
        start: span.start.into(),
        version: ((spec_change_blocks_len.saturating_sub(1)) as u16).into(),
    };
    span_db.insert(span.end.to_be_bytes(), value.as_bytes())?;
    Ok(())
}

fn save_current_span(
    trees: &Trees,
    current_span: &Span,
    spec_change_blocks_len: usize,
) -> Result<(), IndexError> {
    if current_span.start != current_span.end {
        let persisted_start = current_span.start.max(1);
        let persisted_span = Span {
            start: persisted_start,
            end: current_span.end,
        };
        save_span(&trees.span, &persisted_span, spec_change_blocks_len)?;
        info!(
            "📚 Saved span #{} to #{}",
            persisted_span.start.to_formatted_string(&Locale::en),
            persisted_span.end.to_formatted_string(&Locale::en),
        );
    }
    Ok(())
}

type BlockIndexFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(u32, u32, u32), IndexError>> + Send + 'a>,
>;

fn advance_span_end(
    trees: &Trees,
    current_span: &mut Span,
    next_block: u32,
    spec_change_blocks_len: usize,
) -> Result<(), IndexError> {
    let old_key = current_span.end.to_be_bytes();
    current_span.end = next_block;
    let value = SpanDbValue {
        start: current_span.start.into(),
        version: ((spec_change_blocks_len.saturating_sub(1)) as u16).into(),
    };
    let mut batch = Batch::default();
    batch.remove(&old_key);
    batch.insert(&current_span.end.to_be_bytes(), value.as_bytes());
    trees.span.apply_batch(batch)?;
    Ok(())
}

fn queue_head_blocks<'a>(
    head_futures: &mut Vec<BlockIndexFuture<'a>>,
    queue_depth: u32,
    next_head_to_queue: &mut u32,
    latest_seen_head: u32,
    indexer: &'a Indexer,
) {
    while head_futures.len() < queue_depth as usize && *next_head_to_queue <= latest_seen_head {
        let block_number = *next_head_to_queue;
        head_futures.push(Box::pin(indexer.index_block(block_number)));
        debug!(
            "⬆️  Queued live head #{}",
            block_number.to_formatted_string(&Locale::en)
        );
        *next_head_to_queue += 1;
    }
}

fn queue_next_backfill_block<'a>(
    futures: &mut Vec<BlockIndexFuture<'a>>,
    spans: &[Span],
    next_batch_block: &mut Option<u32>,
    indexer: &'a Indexer,
) {
    check_next_batch_block(spans, next_batch_block);
    let Some(block_number) = *next_batch_block else {
        return;
    };
    if block_number == 0 {
        *next_batch_block = None;
        return;
    }
    futures.push(Box::pin(indexer.index_block(block_number)));
    debug!(
        "⬇️  Queued backfill #{}",
        block_number.to_formatted_string(&Locale::en)
    );
    *next_batch_block = prev_block(block_number);
}

fn next_live_head_to_queue(current_span: &Span) -> u32 {
    current_span.end.saturating_add(1)
}

fn advance_backfill_start(
    current_span: &mut Span,
    spans: &mut Vec<Span>,
    span_db: &Tree,
    orphans: &mut AHashMap<u32, ()>,
    block_number: u32,
) -> Result<bool, IndexError> {
    if prev_block(current_span.start) != Some(block_number) {
        if block_number == 0 {
            return Ok(true);
        }
        orphans.insert(block_number, ());
        return Ok(false);
    }

    current_span.start = block_number;
    check_span(span_db, spans, current_span)?;

    while let Some(prev_start) = prev_block(current_span.start) {
        if !orphans.contains_key(&prev_start) {
            break;
        }
        current_span.start = prev_start;
        orphans.remove(&current_span.start);
        check_span(span_db, spans, current_span)?;
    }

    Ok(current_span.start == 1)
}

fn process_queued_head_result(
    trees: &Trees,
    current_span: &mut Span,
    spec_change_blocks_len: usize,
    indexer: &Indexer,
    head_orphans: &mut AHashMap<u32, (u32, u32)>,
    result: Result<(u32, u32, u32), IndexError>,
) -> Result<(), IndexError> {
    match result {
        Ok((block_number, event_count, key_count)) => {
            head_orphans.insert(block_number, (event_count, key_count));
            let mut advanced = false;
            while let Some(next_block) = current_span.end.checked_add(1) {
                let Some((events, keys)) = head_orphans.remove(&next_block) else {
                    break;
                };
                advance_span_end(trees, current_span, next_block, spec_change_blocks_len)?;
                info!(
                    "✨ Indexed head #{}: {} events, {} keys",
                    next_block.to_formatted_string(&Locale::en),
                    events.to_formatted_string(&Locale::en),
                    keys.to_formatted_string(&Locale::en),
                );
                advanced = true;
            }
            if advanced {
                indexer.notify_status_subscribers();
            }
            Ok(())
        }
        Err(IndexError::StatePruningMisconfigured { block_number }) => {
            Err(IndexError::StatePruningMisconfigured { block_number })
        }
        Err(err) => {
            error!("✨ Head indexing error: {err}");
            Ok(())
        }
    }
}

// ─── Main indexer loop ────────────────────────────────────────────────────────

pub async fn run_indexer(
    trees: Trees,
    api: OnlineClient<PolkadotConfig>,
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    spec: IndexSpec,
    finalized: bool,
    queue_depth: u32,
    mut exit_rx: watch::Receiver<bool>,
    runtime: Arc<RuntimeState>,
) -> Result<(), IndexError> {
    let index_variant = spec.index_variant;
    let store_events = spec.store_events;

    info!(
        "📇 Finalized only: {}",
        if finalized { "yes" } else { "no" }
    );
    info!("📇 Queue depth: {queue_depth}");
    info!(
        "📇 Variant indexing: {}",
        if index_variant { "yes" } else { "no" }
    );
    info!(
        "📇 Event storage: {}",
        if store_events { "yes" } else { "no" }
    );

    let spec_change_blocks_len = spec.spec_change_blocks.len();

    let mut next_batch_block: Option<u32> = Some(if finalized {
        let finalized_hash = rpc.chain_get_finalized_head().await?;
        rpc.chain_get_header(Some(finalized_hash))
            .await?
            .ok_or(IndexError::BlockNotFound(0))?
            .number
            .try_into()
            .map_err(|_| internal_error("finalized head number exceeds u32"))?
    } else {
        rpc.chain_get_header(None)
            .await?
            .ok_or(IndexError::BlockNotFound(0))?
            .number
            .try_into()
            .map_err(|_| internal_error("best head number exceeds u32"))?
    });

    let mut blocks_sub = if finalized {
        api.stream_blocks().await
    } else {
        api.stream_best_blocks().await
    }?;

    let mut spans = load_spans(&trees.span, &spec.spec_change_blocks)?;

    let indexer = Indexer::new(trees.clone(), api, rpc, &spec, runtime);

    let mut current_span =
        if let Some(span) = spans.last().filter(|s| Some(s.end) == next_batch_block) {
            let span = span.clone();
            trees.span.remove(span.end.to_be_bytes())?;
            spans.pop();
            next_batch_block = prev_block(span.start);
            info!(
                "📚 Resuming span #{} to #{}; backfilling from #{}, tailing new blocks from #{}",
                span.start.to_formatted_string(&Locale::en),
                span.end.to_formatted_string(&Locale::en),
                next_batch_block
                    .map(|n| n.to_formatted_string(&Locale::en))
                    .unwrap_or_else(|| "none".to_string()),
                next_live_head_to_queue(&span).to_formatted_string(&Locale::en),
            );
            span
        } else {
            let Some(chain_head) = next_batch_block else {
                return Err(internal_error("missing chain head during startup"));
            };
            indexer.index_block(chain_head).await?;
            let span = Span {
                start: chain_head,
                end: chain_head,
            };
            next_batch_block = prev_block(chain_head);
            info!(
                "📚 Indexed chain head #{}; backfilling to genesis, tailing new blocks from #{}",
                chain_head.to_formatted_string(&Locale::en),
                next_live_head_to_queue(&span).to_formatted_string(&Locale::en),
            );
            span
        };

    debug!(
        "📚 Startup state: current span #{} to #{}, next backfill #{}, next live head #{}, previous spans {}",
        current_span.start.to_formatted_string(&Locale::en),
        current_span.end.to_formatted_string(&Locale::en),
        next_batch_block
            .map(|n| n.to_formatted_string(&Locale::en).to_string())
            .unwrap_or_else(|| "none".to_string()),
        next_live_head_to_queue(&current_span).to_formatted_string(&Locale::en),
        spans.len().to_formatted_string(&Locale::en),
    );
    indexer
        .runtime
        .metrics
        .set_current_span(current_span.start, current_span.end);

    let mut latest_seen_head = current_span.end;
    indexer
        .runtime
        .metrics
        .set_latest_seen_head(latest_seen_head);
    let mut next_head_to_queue = next_live_head_to_queue(&current_span);
    let mut head_sub_future = Box::pin(Indexer::next_head_block_number(blocks_sub.next()));
    let mut head_futures = Vec::with_capacity(queue_depth as usize);
    let mut head_orphans: AHashMap<u32, (u32, u32)> = AHashMap::new();

    let mut futures = Vec::with_capacity(queue_depth as usize);
    for _ in 0..queue_depth {
        let before = futures.len();
        queue_next_backfill_block(&mut futures, &spans, &mut next_batch_block, &indexer);
        if futures.len() == before {
            break;
        }
    }

    let mut orphans: AHashMap<u32, ()> = AHashMap::new();
    let mut stats_blocks = 0u32;
    let mut stats_events = 0u32;
    let mut stats_keys = 0u32;
    let mut stats_start = Instant::now();
    let interval_dur = Duration::from_millis(2000);
    let mut interval = time::interval_at(Instant::now() + interval_dur, interval_dur);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
                    biased;

                    _ = exit_rx.changed() => {
                        save_current_span(
                            &trees,
                            &current_span,
                            spec_change_blocks_len,
                        )?;
                        return Ok(());
                    }

                    result = &mut head_sub_future => {
                        let block_number = match result {
                            Ok(block_number) => block_number,
        Err(IndexError::BlockStreamClosed) => {
                            info!("Node block stream closed; will attempt reconnection.");
                            save_current_span(
                                &trees,
                                &current_span,
                                spec_change_blocks_len,
                            )?;
                            return Err(IndexError::BlockStreamClosed);
                        }
                        Err(err) => {
                            error!("Head subscription error: {err}");
                            save_current_span(
                                &trees,
                                &current_span,
                                spec_change_blocks_len,
                            )?;
                            return Err(err);
                        }
                        };
                        latest_seen_head = latest_seen_head.max(block_number);
                        indexer.runtime.metrics.set_latest_seen_head(latest_seen_head);
                        queue_head_blocks(
                            &mut head_futures,
                            queue_depth,
                            &mut next_head_to_queue,
                            latest_seen_head,
                            &indexer,
                        );
                        drop(head_sub_future);
                        head_sub_future = Box::pin(Indexer::next_head_block_number(blocks_sub.next()));
                    }

                    (result, idx, _) = async { future::select_all(&mut head_futures).await }, if !head_futures.is_empty() => {
                        if let Err(err) = process_queued_head_result(
                            &trees,
                            &mut current_span,
                            spec_change_blocks_len,
                            &indexer,
                            &mut head_orphans,
                            result,
                        ) {
                            save_current_span(
                                &trees,
                                &current_span,
                                spec_change_blocks_len,
                            )?;
                            return Err(err);
                        }
                        indexer
                            .runtime
                            .metrics
                            .set_current_span(current_span.start, current_span.end);
                        drop(head_futures.swap_remove(idx));
                        queue_head_blocks(
                            &mut head_futures,
                            queue_depth,
                            &mut next_head_to_queue,
                            latest_seen_head,
                            &indexer,
                        );
                    }

                    _ = interval.tick() => {
                        let now = Instant::now();
                        let micros = now.duration_since(stats_start).as_micros();
                        if micros != 0 && next_batch_block.is_some() {
                            let rate = |n: u32| -> u128 {
                                u128::from(n) * 1_000_000 / micros
                            };
                            info!(
                                "📚 Backfilled to #{}: {} blocks/s, {} events/s, {} keys/s",
                                current_span.start.to_formatted_string(&Locale::en),
                                rate(stats_blocks).to_formatted_string(&Locale::en),
                                rate(stats_events).to_formatted_string(&Locale::en),
                                rate(stats_keys).to_formatted_string(&Locale::en),
                            );
                        }
                        stats_blocks = 0;
                        stats_events = 0;
                        stats_keys = 0;
                        stats_start = now;
                    }

                    (result, idx, _) = async { future::select_all(&mut futures).await }, if !futures.is_empty() => {
                        match result {
                            Ok((block_number, event_count, key_count)) => {
                                if block_number == 0 {
                                    next_batch_block = None;
                                    continue;
                                }
                                let reached_genesis = advance_backfill_start(
                                    &mut current_span,
                                    &mut spans,
                                    &trees.span,
                                    &mut orphans,
                                    block_number,
                                )?;
                                if reached_genesis {
                                    next_batch_block = None;
                                    info!(
                                        "📚 Genesis reached: indexed span #{} to #{}",
                                        current_span.start.to_formatted_string(&Locale::en),
                                        current_span.end.to_formatted_string(&Locale::en),
                                    );
                                }
                                indexer
                                    .runtime
                                    .metrics
                                    .set_current_span(current_span.start, current_span.end);
                                debug!(
                                    "📚 Backfill #{}: {} events, {} keys",
                                    block_number.to_formatted_string(&Locale::en),
                                    event_count.to_formatted_string(&Locale::en),
                                    key_count.to_formatted_string(&Locale::en)
                                );
                                stats_blocks += 1;
                                stats_events += event_count;
                                stats_keys += key_count;
                            }
                            Err(IndexError::BlockNotFound(n)) => {
                                error!("📚 Block not found #{n}");
                                futures.clear();
                                continue;
                            }
                            Err(IndexError::StatePruningMisconfigured { block_number }) => {
                                save_current_span(
                                    &trees,
                                    &current_span,
                                    spec_change_blocks_len,
                                )?;
                                return Err(IndexError::StatePruningMisconfigured { block_number });
                            }
                            Err(err) => {
                                error!("📚 Batch error: {err:?}");
                                futures.clear();
                                continue;
                            }
                        }
                        drop(futures.swap_remove(idx));
                        queue_next_backfill_block(&mut futures, &spans, &mut next_batch_block, &indexer);
                    }
                }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scale_value::{Composite, Primitive, Value, ValueDef, Variant};
    use serde_json::json;
    use zerocopy::FromBytes;
    use zerocopy::IntoBytes;

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    fn test_config() -> IndexSpec {
        toml::from_str(
            r#"
name = "test-runtime"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

[keys]
account_id = "bytes32"
para_id = "u32"
id = "bytes32"

[[pallets]]
name = "System"
events = [
  { name = "NewAccount", params = [
    { field = "account", key = "account_id" },
  ]},
]

[[pallets]]
name = "Claims"
events = [
  { name = "Claimed", params = [
    { field = "who", key = "account_id" },
  ]},
]

[[pallets]]
name = "Paras"
events = [
  { name = "CurrentCodeUpdated", params = [
    { field = "0", key = "id" },
  ]},
]

[[pallets]]
name = "Registrar"
events = [
  { name = "Registered", params = [
    { field = "para_id", key = "para_id" },
    { field = "manager", key = "account_id" },
  ]},
]
"#,
        )
        .unwrap()
    }

    fn test_runtime(max_total_subscriptions: usize) -> Arc<RuntimeState> {
        Arc::new(RuntimeState::new(max_total_subscriptions))
    }

    fn live_ws_config(max_total_subscriptions: usize) -> LiveWsConfig {
        LiveWsConfig {
            max_connections: WsConfig::default().max_connections,
            max_total_subscriptions,
            max_subscriptions_per_connection: WsConfig::default().max_subscriptions_per_connection,
            subscription_buffer_size: WsConfig::default().subscription_buffer_size,
            idle_timeout_secs: WsConfig::default().idle_timeout_secs,
            max_events_limit: WsConfig::default().max_events_limit,
        }
    }

    fn test_indexer_with_runtime(
        trees: Trees,
        store_events: bool,
        runtime: Arc<RuntimeState>,
    ) -> Indexer {
        let config = test_config();
        let mut processing_ctx = BlockProcessingContext::new(&config);
        processing_ctx.store_events = store_events;
        Indexer {
            trees,
            api: None,
            rpc: None,
            runtime,
            processing_ctx: Arc::new(processing_ctx),
        }
    }

    fn test_indexer(trees: Trees, store_events: bool) -> Indexer {
        test_indexer_with_runtime(
            trees,
            store_events,
            test_runtime(WsConfig::default().max_total_subscriptions),
        )
    }

    fn u128_value(value: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(value)),
            context: (),
        }
    }

    fn string_value(value: &str) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::String(value.into())),
            context: (),
        }
    }

    fn bool_value(value: bool) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::Bool(value)),
            context: (),
        }
    }

    #[tokio::test]
    async fn guarded_empty_backfill_queue_skips_select_all() {
        let mut futures: Vec<
            std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(u32, u32, u32), IndexError>> + Send>,
            >,
        > = Vec::new();

        let branch_selected = tokio::select! {
            (..)= async {
                debug_assert!(!futures.is_empty());
                future::select_all(&mut futures).await
            }, if !futures.is_empty() => true,
            else => false,
        };

        assert!(!branch_selected);
    }

    fn bytes32_value(byte: u8) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![u128_value(byte.into()); 32])),
            context: (),
        }
    }

    #[test]
    fn value_to_custom_key_supports_all_scalar_kinds() {
        assert_eq!(
            value_to_custom_key(
                &bytes32_value(0xCD),
                "item_id",
                &crate::config::ScalarKind::Bytes32
            ),
            Some(Key::Custom(CustomKey {
                name: "item_id".into(),
                value: CustomValue::Bytes32(Bytes32([0xCD; 32])),
            }))
        );
        assert_eq!(
            value_to_custom_key(
                &u128_value(7),
                "revision_id",
                &crate::config::ScalarKind::U32
            ),
            Some(Key::Custom(CustomKey {
                name: "revision_id".into(),
                value: CustomValue::U32(7),
            }))
        );
        assert_eq!(
            value_to_custom_key(&u128_value(8), "era", &crate::config::ScalarKind::U64),
            Some(Key::Custom(CustomKey {
                name: "era".into(),
                value: CustomValue::U64(U64Text(8)),
            }))
        );
        assert_eq!(
            value_to_custom_key(&u128_value(9), "stake", &crate::config::ScalarKind::U128),
            Some(Key::Custom(CustomKey {
                name: "stake".into(),
                value: CustomValue::U128(U128Text(9)),
            }))
        );
        assert_eq!(
            value_to_custom_key(
                &string_value("slug"),
                "slug",
                &crate::config::ScalarKind::String
            ),
            Some(Key::Custom(CustomKey {
                name: "slug".into(),
                value: CustomValue::String("slug".into()),
            }))
        );
        assert_eq!(
            value_to_custom_key(
                &bool_value(true),
                "published",
                &crate::config::ScalarKind::Bool
            ),
            Some(Key::Custom(CustomKey {
                name: "published".into(),
                value: CustomValue::Bool(true),
            }))
        );
    }

    #[test]
    fn value_to_json_handles_char_and_variant_values() {
        let char_value = Value {
            value: ValueDef::Primitive(Primitive::Char('x')),
            context: (),
        };
        let variant_value = Value {
            value: ValueDef::Variant(Variant {
                name: "Some".into(),
                values: Composite::Unnamed(vec![u128_value(5)]),
            }),
            context: (),
        };

        assert_eq!(value_to_json(&char_value), serde_json::json!("x"));
        assert_eq!(
            value_to_json(&variant_value),
            serde_json::json!({"variant": "Some", "fields": "5"})
        );
    }

    #[test]
    fn save_span_persists_start_and_version() {
        let trees = temp_trees();
        let span = Span { start: 10, end: 25 };

        save_span(&trees.span, &span, 3).unwrap();

        let bytes = trees.span.get(25u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 10);
        assert_eq!(u16::from(saved.version), 2);
    }

    #[test]
    fn save_current_span_normalizes_zero_start_to_one() {
        let trees = temp_trees();
        let current = Span { start: 0, end: 25 };

        save_current_span(&trees, &current, 2).unwrap();

        let bytes = trees.span.get(25u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 1);
    }

    #[test]
    fn advance_span_end_replaces_old_span_key() {
        let trees = temp_trees();
        let mut current = Span { start: 10, end: 25 };

        save_span(&trees.span, &current, 2).unwrap();
        advance_span_end(&trees, &mut current, 26, 2).unwrap();

        assert_eq!(current.end, 26);
        assert!(trees.span.get(25u32.to_be_bytes()).unwrap().is_none());

        let bytes = trees.span.get(26u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 10);
        assert_eq!(u16::from(saved.version), 1);
    }

    #[test]
    fn next_live_head_to_queue_starts_after_current_end() {
        let current = Span {
            start: 100,
            end: 250,
        };

        assert_eq!(next_live_head_to_queue(&current), 251);
    }

    #[test]
    fn process_queued_head_result_advances_contiguously() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), true);
        let initial = Span { start: 10, end: 10 };
        save_span(&trees.span, &initial, 1).unwrap();

        let mut current_span = initial;
        let mut head_orphans = AHashMap::new();
        process_queued_head_result(
            &trees,
            &mut current_span,
            1,
            &indexer,
            &mut head_orphans,
            Ok((11, 2, 3)),
        )
        .unwrap();

        assert_eq!(current_span.end, 11);
        assert!(head_orphans.is_empty());

        let bytes = trees.span.get(11u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 10);
    }

    #[test]
    fn process_queued_head_result_buffers_out_of_order_blocks_until_gap_closes() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), true);
        let initial = Span { start: 20, end: 20 };
        save_span(&trees.span, &initial, 1).unwrap();

        let mut current_span = initial;
        let mut head_orphans = AHashMap::new();

        process_queued_head_result(
            &trees,
            &mut current_span,
            1,
            &indexer,
            &mut head_orphans,
            Ok((22, 1, 1)),
        )
        .unwrap();

        assert_eq!(current_span.end, 20);
        assert_eq!(head_orphans.get(&22), Some(&(1, 1)));

        process_queued_head_result(
            &trees,
            &mut current_span,
            1,
            &indexer,
            &mut head_orphans,
            Ok((21, 5, 8)),
        )
        .unwrap();

        assert_eq!(current_span.end, 22);
        assert!(head_orphans.is_empty());

        let bytes = trees.span.get(22u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 20);
    }

    #[test]
    fn process_queued_head_result_propagates_state_pruning_errors() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), true);
        let initial = Span { start: 30, end: 30 };
        save_span(&trees.span, &initial, 1).unwrap();

        let mut current_span = initial;
        let mut head_orphans = AHashMap::new();
        let err = process_queued_head_result(
            &trees,
            &mut current_span,
            1,
            &indexer,
            &mut head_orphans,
            Err(IndexError::StatePruningMisconfigured { block_number: 31 }),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            IndexError::StatePruningMisconfigured { block_number: 31 }
        ));
        assert_eq!(current_span.end, 30);
        assert!(head_orphans.is_empty());
    }

    #[test]
    fn advance_backfill_start_marks_completion_at_block_one() {
        let trees = temp_trees();
        let mut spans = Vec::new();
        let mut current_span = Span { start: 2, end: 10 };
        let mut orphans = AHashMap::new();

        let reached_genesis =
            advance_backfill_start(&mut current_span, &mut spans, &trees.span, &mut orphans, 1)
                .unwrap();

        assert!(reached_genesis);
        assert_eq!(current_span.start, 1);
        assert!(orphans.is_empty());
    }

    #[test]
    fn queue_next_backfill_block_skips_block_zero() {
        let trees = temp_trees();
        let indexer = test_indexer(trees, true);
        let mut futures = Vec::new();
        let spans = Vec::new();
        let mut next_batch_block = Some(0);

        queue_next_backfill_block(&mut futures, &spans, &mut next_batch_block, &indexer);

        assert!(futures.is_empty());
        assert_eq!(next_batch_block, None);
    }

    #[test]
    fn derive_event_builds_custom_key_and_stored_event_json() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0],
            index_variant: false,
            store_events: true,
            keys: HashMap::from([(
                "amount".into(),
                crate::config::CustomKeyConfig::Scalar(ScalarKind::U128),
            )]),
            pallets: vec![crate::config::PalletConfig {
                name: "MyPallet".into(),
                events: vec![crate::config::EventConfig {
                    name: "Stored".into(),
                    params: vec![crate::config::ParamConfig {
                        field: Some("amount".into()),
                        fields: vec![],
                        key: "amount".into(),
                        multi: false,
                    }],
                }],
            }],
        };
        let ctx = BlockProcessingContext::new(&spec);

        let derived = ctx
            .derive_event(
                1234,
                "MyPallet",
                "Stored",
                9,
                2,
                7,
                &Composite::Named(vec![("amount".into(), u128_value(999))]),
            )
            .unwrap();

        assert_eq!(derived.variant_key, None);
        assert_eq!(
            derived.keys,
            vec![Key::Custom(CustomKey {
                name: "amount".into(),
                value: CustomValue::U128(U128Text(999)),
            })]
        );
        let stored: serde_json::Value =
            serde_json::from_slice(&derived.stored_event_json.expect("expected stored event"))
                .unwrap();
        assert_eq!(stored["specVersion"], json!(1234));
        assert_eq!(stored["eventName"], json!("Stored"));
        assert_eq!(stored["fields"], json!({"amount": "999"}));
    }

    #[test]
    fn derive_event_stores_variant_only_event_when_variant_indexing_enabled() {
        let spec = IndexSpec {
            name: "test".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            spec_change_blocks: vec![0],
            index_variant: true,
            store_events: true,
            keys: HashMap::new(),
            pallets: vec![],
        };
        let ctx = BlockProcessingContext::new(&spec);

        let derived = ctx
            .derive_event(
                55,
                "Unindexed",
                "OnlyVariant",
                4,
                1,
                3,
                &Composite::Named(vec![]),
            )
            .unwrap();

        assert_eq!(derived.variant_key, Some(Key::Variant(4, 1)));
        assert!(derived.keys.is_empty());
        let stored: serde_json::Value =
            serde_json::from_slice(&derived.stored_event_json.expect("expected stored event"))
                .unwrap();
        assert_eq!(stored["specVersion"], json!(55));
        assert_eq!(stored["palletIndex"], json!(4));
        assert_eq!(stored["variantIndex"], json!(1));
    }

    #[test]
    fn store_event_persists_decoded_event() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), true);

        indexer
            .store_event(
                42,
                70_000,
                1234,
                "Balances",
                "Deposit",
                5,
                2,
                &Composite::Named(vec![("amount".into(), u128_value(999))]),
            )
            .unwrap();

        let db_key = EventKey {
            block_number: 42u32.into(),
            event_index: 70_000u32.into(),
        };
        let bytes = trees.events.get(db_key.as_bytes()).unwrap().unwrap();
        let stored: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(stored["specVersion"], json!(1234));
        assert_eq!(stored["palletName"], json!("Balances"));
        assert_eq!(stored["eventName"], json!("Deposit"));
        assert_eq!(stored["eventIndex"], json!(70_000));
        assert_eq!(stored["fields"], json!({"amount": "999"}));
    }

    #[test]
    fn store_event_skips_writes_when_disabled() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), false);

        indexer
            .store_event(
                42,
                7,
                1234,
                "Balances",
                "Ignored",
                5,
                2,
                &Composite::Named(vec![]),
            )
            .unwrap();

        let db_key = EventKey {
            block_number: 42u32.into(),
            event_index: 7u32.into(),
        };
        assert!(trees.events.get(db_key.as_bytes()).unwrap().is_none());
    }

    #[tokio::test]
    async fn notify_event_subscribers_includes_decoded_events() {
        let trees = temp_trees();
        let indexer = Indexer::new_test(trees.clone(), &test_config());
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });
        let db_key = EventKey {
            block_number: 7u32.into(),
            event_index: 3u32.into(),
        };
        trees
            .events
            .insert(
                db_key.as_bytes(),
                serde_json::to_vec(&serde_json::json!({"specVersion": 1234, "eventName": "Ready"}))
                    .unwrap(),
            )
            .unwrap();

        let (tx, mut rx) = mpsc::channel(1);
        process_sub_msg(
            indexer.runtime.as_ref(),
            &live_ws_config(WsConfig::default().max_total_subscriptions),
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx,
                response_tx: None,
            },
        )
        .unwrap();

        indexer.index_event_key(key.clone(), 7, 3).unwrap();

        let NotificationBody::EventNotification { decoded_event, .. } =
            rx.recv().await.unwrap().body
        else {
            panic!("expected events response");
        };
        let decoded_event = decoded_event.expect("expected decoded event");
        assert_eq!(decoded_event.block_number, 7);
        assert_eq!(decoded_event.event_index, 3);
        assert_eq!(
            decoded_event.event,
            serde_json::json!({"specVersion": 1234, "eventName": "Ready"})
        );
    }

    #[tokio::test]
    async fn notify_event_subscribers_omits_decoded_events_when_not_stored() {
        let trees = temp_trees();
        let indexer = test_indexer(trees, false);
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });

        let (tx, mut rx) = mpsc::channel(1);
        process_sub_msg(
            indexer.runtime.as_ref(),
            &live_ws_config(WsConfig::default().max_total_subscriptions),
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx,
                response_tx: None,
            },
        )
        .unwrap();

        indexer.index_event_key(key.clone(), 7, 3).unwrap();

        let NotificationBody::EventNotification {
            event,
            decoded_event,
            ..
        } = rx.recv().await.unwrap().body
        else {
            panic!("expected events response");
        };
        assert_eq!(
            event,
            EventRef {
                block_number: 7,
                event_index: 3,
            }
        );
        assert!(decoded_event.is_none());
    }

    #[tokio::test]
    async fn subscriptions_survive_replacing_the_indexer_instance() {
        let trees = temp_trees();
        let runtime = test_runtime(WsConfig::default().max_total_subscriptions);
        let first_indexer = test_indexer_with_runtime(trees.clone(), true, runtime.clone());
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });

        let (tx, mut rx) = mpsc::channel(1);
        process_sub_msg(
            first_indexer.runtime.as_ref(),
            &live_ws_config(WsConfig::default().max_total_subscriptions),
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx,
                response_tx: None,
            },
        )
        .unwrap();

        let replacement_indexer = test_indexer_with_runtime(trees, true, runtime);
        replacement_indexer.index_event_key(key, 9, 2).unwrap();

        let NotificationBody::EventNotification { event, .. } = rx.recv().await.unwrap().body
        else {
            panic!("expected event notification");
        };
        assert_eq!(
            event,
            EventRef {
                block_number: 9,
                event_index: 2,
            }
        );
    }

    #[test]
    fn load_spans_keeps_span_when_flags_change_without_version_bump() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 5u32.into(),
            version: 0u16.into(),
        };
        trees
            .span
            .insert(15u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0]).unwrap();
        assert_eq!(spans, vec![Span { start: 5, end: 15 }]);
        assert!(trees.span.get(15u32.to_be_bytes()).unwrap().is_some());
    }

    #[test]
    fn keys_for_event_handles_unknown_pallet_and_param_failures() {
        let config: IndexSpec = toml::from_str(
            r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
spec_change_blocks = [0]

        [keys]
        account_id = "bytes32"
        count = "u32"

        [[pallets]]
        name = "Custom"
        events = [
          { name = "Created", params = [
            { field = "missing", key = "account_id" },
            { field = "flag", key = "count" },
          ]},
        ]
"#,
        )
        .unwrap();
        let indexer = Indexer::new_test(temp_trees(), &config);
        let fields = Composite::Named(vec![("flag".into(), bool_value(true))]);

        assert!(
            indexer
                .keys_for_event("UnknownPallet", "Created", &fields)
                .is_empty()
        );
        assert!(
            indexer
                .keys_for_event("Custom", "Created", &fields)
                .is_empty()
        );
    }

    #[test]
    fn composite_to_json_handles_mixed_unnamed_values() {
        let mixed = Composite::Unnamed(vec![u128_value(1), string_value("two")]);
        let json = composite_to_json(&mixed);
        assert_eq!(json, serde_json::json!(["1", "two"]));
    }

    #[tokio::test]
    async fn process_sub_msg_ignores_missing_event_subscription_bucket() {
        let indexer = Indexer::new_test(temp_trees(), &test_config());
        let (tx, _rx) = mpsc::channel(1);

        process_sub_msg(
            indexer.runtime.as_ref(),
            &live_ws_config(WsConfig::default().max_total_subscriptions),
            SubscriptionMessage::UnsubscribeEvents {
                key: Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(999),
                }),
                tx,
                response_tx: None,
            },
        )
        .unwrap();
    }
}
