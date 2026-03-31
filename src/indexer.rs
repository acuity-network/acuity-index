//! Core block indexer — schema-less, config-driven.

use ahash::AHashMap;
use futures::future;
use num_format::{Locale, ToFormattedString};
use scale_value::{Composite, Value, ValueDef};
use serde_json::json;
use sled::Tree;
use std::{collections::HashMap, future::Future, sync::Mutex};
use subxt::{
    OnlineClient, PolkadotConfig, client::Block, config::RpcConfigFor,
    rpcs::methods::legacy::LegacyRpcMethods,
};
use tokio::{
    sync::{mpsc, watch},
    time::{self, Duration, Instant, MissedTickBehavior},
};
use tracing::{debug, error, info};
use zerocopy::{FromBytes, IntoBytes};

use crate::{
    config::{ChainConfig, KeyTypeName, ParamKey, ResolvedParamConfig},
    pallets::{
        extract_bool, extract_bytes32, extract_string, extract_u32, extract_u64, extract_u128,
        get_field, index_sdk_pallet,
    },
    shared::*,
    websockets::process_msg_status,
};

// ─── Indexer struct ───────────────────────────────────────────────────────────

pub struct Indexer {
    pub trees: Trees,
    api: Option<OnlineClient<PolkadotConfig>>,
    rpc: Option<LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>>,
    index_variant: bool,
    store_events: bool,
    status_subs: Mutex<Vec<mpsc::UnboundedSender<ResponseMessage>>>,
    events_subs: Mutex<HashMap<Key, Vec<mpsc::UnboundedSender<ResponseMessage>>>>,
    /// sdk_pallets: set of pallet names using built-in SDK rules.
    sdk_pallets: std::collections::HashSet<String>,
    /// custom_index: pallet → event → params mapping from TOML.
    custom_index: HashMap<String, HashMap<String, Vec<ResolvedParamConfig>>>,
}

impl Indexer {
    pub fn new(
        trees: Trees,
        api: OnlineClient<PolkadotConfig>,
        rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
        index_variant: bool,
        store_events: bool,
        config: &ChainConfig,
    ) -> Self {
        Indexer {
            trees,
            api: Some(api),
            rpc: Some(rpc),
            index_variant,
            store_events,
            status_subs: Mutex::new(Vec::new()),
            events_subs: Mutex::new(HashMap::new()),
            sdk_pallets: config.sdk_pallets(),
            custom_index: config.build_custom_index().expect("validated chain config"),
        }
    }

    #[cfg(test)]
    pub fn new_test(trees: Trees, config: &ChainConfig) -> Self {
        Indexer {
            trees,
            api: None,
            rpc: None,
            index_variant: true,
            store_events: true,
            status_subs: Mutex::new(Vec::new()),
            events_subs: Mutex::new(HashMap::new()),
            sdk_pallets: config.sdk_pallets(),
            custom_index: config.build_custom_index().expect("validated chain config"),
        }
    }

    // ─── Block indexing ───────────────────────────────────────────────────────

    pub async fn index_head(
        &self,
        next: impl Future<Output = Option<Result<Block<PolkadotConfig>, subxt::error::BlocksError>>>,
    ) -> Result<(u32, u32, u32), IndexError> {
        let block = next.await.unwrap()?;
        let block_number: u32 = block.number().try_into().unwrap();
        self.index_block(block_number).await
    }

    pub async fn index_block(&self, block_number: u32) -> Result<(u32, u32, u32), IndexError> {
        let mut key_count = 0u32;
        let api = self.api.as_ref().unwrap();
        let rpc = self.rpc.as_ref().unwrap();

        let block_hash = rpc
            .chain_get_block_hash(Some(block_number.into()))
            .await?
            .ok_or(IndexError::BlockNotFound(block_number))?;

        let at_block = api.at_block(block_hash).await?;
        let spec = at_block.spec_version();
        let events = at_block.events().fetch().await?;

        for (i, event_result) in events.iter().enumerate() {
            let event = match event_result {
                Ok(e) => e,
                Err(err) => {
                    error!("Block {block_number}, event {i}: {err}");
                    continue;
                }
            };

            let event_index: u16 = i.try_into().unwrap();
            let pallet_name = event.pallet_name();
            let event_name = event.event_name();
            let pallet_index = event.pallet_index();
            let variant_index = event.event_index();

            // Index the variant key if enabled.
            if self.index_variant {
                self.index_event_key(
                    Key::Variant(pallet_index, variant_index),
                    block_number,
                    event_index,
                )?;
                key_count += 1;
            }

            // Decode field values schema-lessly.
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

            // Determine indexing keys from config.
            let keys = self.keys_for_event(pallet_name, event_name, &field_values);
            let should_store_event = self.index_variant || !keys.is_empty();

            for key in &keys {
                self.index_event_key(key.clone(), block_number, event_index)?;
                key_count += 1;
            }

            // Persist the decoded event row for direct retrieval.
            if self.store_events && should_store_event {
                self.store_event(
                    block_number,
                    event_index,
                    spec,
                    pallet_name,
                    event_name,
                    pallet_index,
                    variant_index,
                    &field_values,
                )?;
            }
        }

        Ok((block_number, events.len() as u32, key_count))
    }

    fn store_event(
        &self,
        block_number: u32,
        event_index: u16,
        spec_version: u32,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        fields: &Composite<()>,
    ) -> Result<(), IndexError> {
        if !self.store_events {
            return Ok(());
        }

        let db_key = EventKey {
            block_number: block_number.into(),
            event_index: event_index.into(),
        };
        let mut encoded = self.encode_event(
            pallet_name,
            event_name,
            pallet_index,
            variant_index,
            event_index,
            fields,
        );
        encoded["specVersion"] = json!(spec_version);
        let json_bytes = serde_json::to_vec(&encoded)?;
        self.trees
            .events
            .insert(db_key.as_bytes(), json_bytes.as_slice())?;
        Ok(())
    }

    // ─── Key derivation ───────────────────────────────────────────────────────

    pub fn keys_for_event(
        &self,
        pallet_name: &str,
        event_name: &str,
        fields: &Composite<()>,
    ) -> Vec<Key> {
        // Try SDK built-in first.
        if self.sdk_pallets.contains(pallet_name) {
            if let Some(keys) = index_sdk_pallet(pallet_name, event_name, fields) {
                return keys;
            }
        }
        // Fall back to TOML custom config.
        if let Some(event_map) = self.custom_index.get(pallet_name) {
            if let Some(params) = event_map.get(event_name) {
                return self.keys_from_params(params, fields);
            }
        }
        vec![]
    }

    fn keys_from_params(&self, params: &[ResolvedParamConfig], fields: &Composite<()>) -> Vec<Key> {
        let mut keys = Vec::new();
        for param in params {
            let value = match get_field(fields, &param.field) {
                Some(v) => v,
                None => continue,
            };
            if param.multi {
                keys.extend(values_to_keys(value, &param.key));
            } else if let Some(key) = value_to_key(value, &param.key) {
                keys.push(key);
            }
        }
        keys
    }

    // ─── Event encoding ───────────────────────────────────────────────────────

    pub fn encode_event(
        &self,
        pallet_name: &str,
        event_name: &str,
        pallet_index: u8,
        variant_index: u8,
        event_index: u16,
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

    // ─── DB write & notification ──────────────────────────────────────────────

    pub fn index_event_key(
        &self,
        key: Key,
        block_number: u32,
        event_index: u16,
    ) -> Result<(), sled::Error> {
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
        let msg = process_msg_status(&self.trees.span);
        for tx in self.status_subs.lock().unwrap().iter() {
            let _ = tx.send(msg.clone());
        }
    }

    fn notify_event_subscribers(&self, key: Key, event_ref: EventRef) {
        let subs = self.events_subs.lock().unwrap();
        if let Some(txs) = subs.get(&key) {
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

            let msg = ResponseMessage::Events {
                key,
                events: vec![event_ref],
                decoded_events,
            };
            for tx in txs {
                let _ = tx.send(msg.clone());
            }
        }
    }
}

// ─── scale_value → Key conversion ────────────────────────────────────────────

fn value_to_builtin_key(value: &Value<()>, key_type: &KeyTypeName) -> Option<Key> {
    let (name, value) = match key_type {
        KeyTypeName::AccountId => (
            "account_id",
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ),
        KeyTypeName::AccountIndex => ("account_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::BountyIndex => ("bounty_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::EraIndex => ("era_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::MessageId => (
            "message_id",
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ),
        KeyTypeName::PoolId => ("pool_id", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::PreimageHash => (
            "preimage_hash",
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ),
        KeyTypeName::ProposalHash => (
            "proposal_hash",
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ),
        KeyTypeName::ProposalIndex => ("proposal_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::RefIndex => ("ref_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::RegistrarIndex => {
            ("registrar_index", extract_u32(value).map(CustomValue::U32))
        }
        KeyTypeName::SessionIndex => ("session_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::SpendIndex => ("spend_index", extract_u32(value).map(CustomValue::U32)),
        KeyTypeName::TipHash => (
            "tip_hash",
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b))),
        ),
    };

    Some(Key::Custom(CustomKey {
        name: name.to_owned(),
        value: value?,
    }))
}

fn value_to_custom_key(
    value: &Value<()>,
    name: &str,
    kind: &crate::config::ScalarKind,
) -> Option<Key> {
    let value = match kind {
        crate::config::ScalarKind::Bytes32 => {
            extract_bytes32(value).map(|b| CustomValue::Bytes32(Bytes32(b)))
        }
        crate::config::ScalarKind::U32 => extract_u32(value).map(CustomValue::U32),
        crate::config::ScalarKind::U64 => extract_u64(value).map(|v| CustomValue::U64(U64Text(v))),
        crate::config::ScalarKind::U128 => {
            extract_u128(value).map(|v| CustomValue::U128(U128Text(v)))
        }
        crate::config::ScalarKind::String => extract_string(value).map(CustomValue::String),
        crate::config::ScalarKind::Bool => extract_bool(value).map(CustomValue::Bool),
    }?;

    Some(Key::Custom(CustomKey {
        name: name.to_owned(),
        value,
    }))
}

fn value_to_key(value: &Value<()>, key: &ParamKey) -> Option<Key> {
    match key {
        ParamKey::BuiltIn(key_type) => value_to_builtin_key(value, key_type),
        ParamKey::Custom { name, kind } => value_to_custom_key(value, name, kind),
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

pub fn process_sub_msg(indexer: &Indexer, msg: SubscriptionMessage) {
    match msg {
        SubscriptionMessage::SubscribeStatus { tx } => {
            indexer.status_subs.lock().unwrap().push(tx);
        }
        SubscriptionMessage::UnsubscribeStatus { tx } => {
            indexer
                .status_subs
                .lock()
                .unwrap()
                .retain(|t| !tx.same_channel(t));
        }
        SubscriptionMessage::SubscribeEvents { key, tx } => {
            let mut subs = indexer.events_subs.lock().unwrap();
            subs.entry(key).or_default().push(tx);
        }
        SubscriptionMessage::UnsubscribeEvents { key, tx } => {
            let mut subs = indexer.events_subs.lock().unwrap();
            if let Some(txs) = subs.get_mut(&key) {
                txs.retain(|t| !tx.same_channel(t));
            }
        }
    }
}

// ─── Span helpers ─────────────────────────────────────────────────────────────

pub fn load_spans(
    span_db: &Tree,
    versions: &[u32],
    index_variant: bool,
    store_events: bool,
) -> Result<Vec<Span>, IndexError> {
    let mut spans = vec![];
    'span: for (key, value) in span_db.into_iter().flatten() {
        let span_value = SpanDbValue::read_from_bytes(&value).unwrap();
        let start: u32 = span_value.start.into();
        let mut end: u32 = u32::from_be_bytes(key.as_ref().try_into().unwrap());
        if index_variant && span_value.index_variant != 1 {
            span_db.remove(&key)?;
            info!(
                "📚 Re-indexing #{} to #{}: event variants not indexed.",
                start.to_formatted_string(&Locale::en),
                end.to_formatted_string(&Locale::en)
            );
            continue;
        }
        if store_events && span_value.store_events != 1 {
            span_db.remove(&key)?;
            info!(
                "📚 Re-indexing #{} to #{}: events not stored.",
                start.to_formatted_string(&Locale::en),
                end.to_formatted_string(&Locale::en)
            );
            continue;
        }
        let span_version: u16 = span_value.version.into();
        for (version, block_number) in versions.iter().enumerate() {
            if span_version < version.try_into().unwrap() && end >= *block_number {
                span_db.remove(&key)?;
                if start >= *block_number {
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
                span_db.insert(end.to_be_bytes(), value)?;
                break;
            }
        }
        let span = Span { start, end };
        info!(
            "📚 Previous indexed span #{} to #{}.",
            start.to_formatted_string(&Locale::en),
            end.to_formatted_string(&Locale::en)
        );
        spans.push(span);
    }
    Ok(spans)
}

pub fn check_span(
    span_db: &Tree,
    spans: &mut Vec<Span>,
    current_span: &mut Span,
) -> Result<(), IndexError> {
    while let Some(span) = spans.last() {
        if current_span.start > span.start && current_span.start - 1 <= span.end {
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
    }
    Ok(())
}

pub fn check_next_batch_block(spans: &[Span], next: &mut u32) {
    let mut i = spans.len();
    while i != 0 {
        i -= 1;
        if *next >= spans[i].start && *next <= spans[i].end {
            *next = spans[i].start - 1;
        }
    }
}

fn save_span(
    span_db: &Tree,
    span: &Span,
    versions_len: usize,
    index_variant: bool,
    store_events: bool,
) -> Result<(), IndexError> {
    let value = SpanDbValue {
        start: span.start.into(),
        version: ((versions_len.saturating_sub(1)) as u16).into(),
        index_variant: u8::from(index_variant),
        store_events: u8::from(store_events),
    };
    span_db.insert(span.end.to_be_bytes(), value.as_bytes())?;
    Ok(())
}

// ─── Main indexer loop ────────────────────────────────────────────────────────

pub async fn run_indexer(
    trees: Trees,
    api: OnlineClient<PolkadotConfig>,
    rpc: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    config: ChainConfig,
    finalized: bool,
    queue_depth: u32,
    index_variant: bool,
    store_events: bool,
    mut exit_rx: watch::Receiver<bool>,
    mut sub_rx: mpsc::UnboundedReceiver<SubscriptionMessage>,
) -> Result<(), IndexError> {
    info!(
        "📇 Finalized only: {}",
        if finalized { "yes" } else { "no" }
    );
    info!(
        "📇 Variant indexing: {}",
        if index_variant { "yes" } else { "no" }
    );
    info!(
        "📇 Event storage: {}",
        if store_events { "yes" } else { "no" }
    );

    let versions_len = config.versions.len();

    let mut blocks_sub = if finalized {
        api.stream_blocks().await
    } else {
        api.stream_best_blocks().await
    }?;

    let mut next_batch_block: u32 = blocks_sub
        .next()
        .await
        .ok_or(IndexError::BlockNotFound(0))??
        .number()
        .try_into()
        .unwrap();

    info!(
        "📚 Indexing backwards from #{}",
        next_batch_block.to_formatted_string(&Locale::en)
    );

    let mut spans = load_spans(&trees.span, &config.versions, index_variant, store_events)?;

    let mut current_span = if let Some(span) = spans.last().filter(|s| s.end == next_batch_block) {
        let span = span.clone();
        info!(
            "📚 Resuming span #{} to #{}",
            span.start.to_formatted_string(&Locale::en),
            span.end.to_formatted_string(&Locale::en)
        );
        trees.span.remove(span.end.to_be_bytes())?;
        spans.pop();
        next_batch_block = span.start - 1;
        span
    } else {
        Span {
            start: next_batch_block + 1,
            end: next_batch_block + 1,
        }
    };

    let indexer = Indexer::new(
        trees.clone(),
        api,
        rpc,
        index_variant,
        store_events,
        &config,
    );

    let mut head_future = Box::pin(indexer.index_head(blocks_sub.next()));

    let mut futures = Vec::with_capacity(queue_depth as usize);
    for _ in 0..queue_depth {
        check_next_batch_block(&spans, &mut next_batch_block);
        futures.push(Box::pin(indexer.index_block(next_batch_block)));
        debug!(
            "⬆️  Queued #{}",
            next_batch_block.to_formatted_string(&Locale::en)
        );
        next_batch_block -= 1;
    }

    let mut orphans: AHashMap<u32, ()> = AHashMap::new();
    let mut stats_blocks = 0u32;
    let mut stats_events = 0u32;
    let mut stats_keys = 0u32;
    let mut stats_start = Instant::now();
    let interval_dur = Duration::from_millis(2000);
    let mut interval = time::interval_at(Instant::now() + interval_dur, interval_dur);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut is_batching = true;

    loop {
        tokio::select! {
            biased;

            _ = exit_rx.changed() => {
                if current_span.start != current_span.end {
                    save_span(
                        &trees.span,
                        &current_span,
                        versions_len,
                        index_variant,
                        store_events,
                    )?;
                    info!(
                        "📚 Saved span #{} to #{}",
                        current_span.start.to_formatted_string(&Locale::en),
                        current_span.end.to_formatted_string(&Locale::en),
                    );
                }
                return Ok(());
            }

            Some(msg) = sub_rx.recv() => {
                process_sub_msg(&indexer, msg);
            }

            result = &mut head_future => {
                match result {
                    Ok((block_number, event_count, key_count)) => {
                        trees.span.remove(current_span.end.to_be_bytes())?;
                        current_span.end = block_number;
                        save_span(
                            &trees.span,
                            &current_span,
                            versions_len,
                            index_variant,
                            store_events,
                        )?;
                        info!(
                            "✨ #{}: {} events, {} keys",
                            block_number.to_formatted_string(&Locale::en),
                            event_count.to_formatted_string(&Locale::en),
                            key_count.to_formatted_string(&Locale::en),
                        );
                        indexer.notify_status_subscribers();
                        drop(head_future);
                        head_future = Box::pin(
                            indexer.index_head(blocks_sub.next())
                        );
                    }
                    Err(err) => {
                        error!("✨ Head indexing error: {err}");
                        drop(head_future);
                        head_future = Box::pin(
                            indexer.index_head(blocks_sub.next())
                        );
                    }
                }
            }

            _ = interval.tick(), if is_batching => {
                let now = Instant::now();
                let micros = now.duration_since(stats_start).as_micros();
                if micros != 0 {
                    let rate = |n: u32| -> u128 {
                        u128::from(n) * 1_000_000 / micros
                    };
                    info!(
                        "📚 #{}: {}/s blocks, {}/s events, {}/s keys",
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

            (result, idx, _) = future::select_all(&mut futures),
                if is_batching =>
            {
                match result {
                    Ok((block_number, event_count, key_count)) => {
                        if block_number == current_span.start - 1 {
                            current_span.start = block_number;
                            debug!(
                                "⬇️  #{} indexed",
                                block_number.to_formatted_string(&Locale::en)
                            );
                            check_span(
                                &trees.span,
                                &mut spans,
                                &mut current_span,
                            )?;
                            while orphans
                                .contains_key(&(current_span.start - 1))
                            {
                                current_span.start -= 1;
                                orphans.remove(&current_span.start);
                                check_span(
                                    &trees.span,
                                    &mut spans,
                                    &mut current_span,
                                )?;
                            }
                        } else {
                            orphans.insert(block_number, ());
                        }
                        stats_blocks += 1;
                        stats_events += event_count;
                        stats_keys += key_count;
                    }
                    Err(IndexError::BlockNotFound(n)) => {
                        error!("📚 Block not found #{n}");
                        is_batching = false;
                    }
                    Err(err) => {
                        error!("📚 Batch error: {err:?}");
                        is_batching = false;
                    }
                }
                check_next_batch_block(&spans, &mut next_batch_block);
                futures[idx] =
                    Box::pin(indexer.index_block(next_batch_block));
                next_batch_block -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scale_value::{Composite, Primitive, Value, ValueDef, Variant};
    use serde_json::json;
    use zerocopy::IntoBytes;

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    fn test_config() -> ChainConfig {
        toml::from_str(crate::config::POLKADOT_TOML).unwrap()
    }

    fn test_indexer(trees: Trees, store_events: bool) -> Indexer {
        let config = test_config();
        Indexer {
            trees,
            api: None,
            rpc: None,
            index_variant: true,
            store_events,
            status_subs: Mutex::new(Vec::new()),
            events_subs: Mutex::new(HashMap::new()),
            sdk_pallets: config.sdk_pallets(),
            custom_index: config.build_custom_index().expect("validated chain config"),
        }
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

    fn bytes32_value(byte: u8) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(vec![u128_value(byte.into()); 32])),
            context: (),
        }
    }

    #[test]
    fn value_to_builtin_key_supports_every_builtin_key_type() {
        let number = u128_value(9);
        let bytes = bytes32_value(0xAB);

        for (key_type, expected) in [
            (
                KeyTypeName::AccountId,
                Key::Custom(CustomKey {
                    name: "account_id".into(),
                    value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                }),
            ),
            (
                KeyTypeName::AccountIndex,
                Key::Custom(CustomKey {
                    name: "account_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::BountyIndex,
                Key::Custom(CustomKey {
                    name: "bounty_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::EraIndex,
                Key::Custom(CustomKey {
                    name: "era_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::MessageId,
                Key::Custom(CustomKey {
                    name: "message_id".into(),
                    value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                }),
            ),
            (
                KeyTypeName::PoolId,
                Key::Custom(CustomKey {
                    name: "pool_id".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::PreimageHash,
                Key::Custom(CustomKey {
                    name: "preimage_hash".into(),
                    value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                }),
            ),
            (
                KeyTypeName::ProposalHash,
                Key::Custom(CustomKey {
                    name: "proposal_hash".into(),
                    value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                }),
            ),
            (
                KeyTypeName::ProposalIndex,
                Key::Custom(CustomKey {
                    name: "proposal_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::RefIndex,
                Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::RegistrarIndex,
                Key::Custom(CustomKey {
                    name: "registrar_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::SessionIndex,
                Key::Custom(CustomKey {
                    name: "session_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::SpendIndex,
                Key::Custom(CustomKey {
                    name: "spend_index".into(),
                    value: CustomValue::U32(9),
                }),
            ),
            (
                KeyTypeName::TipHash,
                Key::Custom(CustomKey {
                    name: "tip_hash".into(),
                    value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                }),
            ),
        ] {
            let value = match key_type {
                KeyTypeName::AccountId
                | KeyTypeName::MessageId
                | KeyTypeName::PreimageHash
                | KeyTypeName::ProposalHash
                | KeyTypeName::TipHash => &bytes,
                _ => &number,
            };
            assert_eq!(value_to_builtin_key(value, &key_type), Some(expected));
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
    fn save_span_persists_flags_and_version() {
        let trees = temp_trees();
        let span = Span { start: 10, end: 25 };

        save_span(&trees.span, &span, 3, true, false).unwrap();

        let bytes = trees.span.get(25u32.to_be_bytes()).unwrap().unwrap();
        let saved = SpanDbValue::read_from_bytes(&bytes).unwrap();
        assert_eq!(u32::from(saved.start), 10);
        assert_eq!(u16::from(saved.version), 2);
        assert_eq!(saved.index_variant, 1);
        assert_eq!(saved.store_events, 0);
    }

    #[test]
    fn store_event_persists_decoded_event() {
        let trees = temp_trees();
        let indexer = test_indexer(trees.clone(), true);

        indexer
            .store_event(
                42,
                7,
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
            event_index: 7u16.into(),
        };
        let bytes = trees.events.get(db_key.as_bytes()).unwrap().unwrap();
        let stored: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(stored["specVersion"], json!(1234));
        assert_eq!(stored["palletName"], json!("Balances"));
        assert_eq!(stored["eventName"], json!("Deposit"));
        assert_eq!(stored["eventIndex"], json!(7));
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
            event_index: 7u16.into(),
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
            event_index: 3u16.into(),
        };
        trees
            .events
            .insert(
                db_key.as_bytes(),
                serde_json::to_vec(&serde_json::json!({"specVersion": 1234, "eventName": "Ready"})).unwrap(),
            )
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        process_sub_msg(
            &indexer,
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx,
            },
        );

        indexer.index_event_key(key.clone(), 7, 3).unwrap();

        let ResponseMessage::Events { decoded_events, .. } = rx.recv().await.unwrap() else {
            panic!("expected events response");
        };
        assert_eq!(decoded_events.len(), 1);
        assert_eq!(decoded_events[0].block_number, 7);
        assert_eq!(decoded_events[0].event_index, 3);
        assert_eq!(decoded_events[0].event, serde_json::json!({"specVersion": 1234, "eventName": "Ready"}));
    }

    #[tokio::test]
    async fn notify_event_subscribers_omits_decoded_events_when_not_stored() {
        let trees = temp_trees();
        let indexer = test_indexer(trees, false);
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        process_sub_msg(
            &indexer,
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx,
            },
        );

        indexer.index_event_key(key.clone(), 7, 3).unwrap();

        let ResponseMessage::Events {
            events,
            decoded_events,
            ..
        } = rx.recv().await.unwrap()
        else {
            panic!("expected events response");
        };
        assert_eq!(
            events,
            vec![EventRef {
                block_number: 7,
                event_index: 3,
            }]
        );
        assert!(decoded_events.is_empty());
    }

    #[test]
    fn load_spans_keeps_span_when_event_storage_remains_disabled() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 5u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 0,
        };
        trees
            .span
            .insert(15u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0], true, false).unwrap();
        assert_eq!(spans, vec![Span { start: 5, end: 15 }]);
        assert!(trees.span.get(15u32.to_be_bytes()).unwrap().is_some());
    }

    #[test]
    fn keys_for_event_handles_unknown_sdk_pallet_and_param_failures() {
        let config: ChainConfig = toml::from_str(
            r#"
name = "test"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

[custom_keys]
count = "u32"

        [[pallets]]
        name = "UnknownSdk"
        sdk = true

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
                .keys_for_event("UnknownSdk", "Created", &fields)
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
        let (tx, _rx) = mpsc::unbounded_channel();

        process_sub_msg(
            &indexer,
            SubscriptionMessage::UnsubscribeEvents {
                key: Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(999),
                }),
                tx,
            },
        );
    }
}
