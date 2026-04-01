#[cfg(test)]
mod indexer_tests {
    use crate::config::ChainConfig;
    use crate::indexer::*;
    use crate::shared::*;
    use scale_value::{Composite, Primitive, Value, ValueDef};
    use tokio::sync::mpsc;

    fn u128_val(n: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(n)),
            context: (),
        }
    }

    fn bytes32_val(b: [u8; 32]) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(
                b.iter()
                    .map(|&byte| Value {
                        value: ValueDef::Primitive(Primitive::U128(byte as u128)),
                        context: (),
                    })
                    .collect(),
            )),
            context: (),
        }
    }

    fn vec_val(values: Vec<Value<()>>) -> Value<()> {
        Value {
            value: ValueDef::Composite(Composite::Unnamed(values)),
            context: (),
        }
    }

    fn named(fields: Vec<(&str, Value<()>)>) -> Composite<()> {
        Composite::Named(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    fn unnamed(fields: Vec<Value<()>>) -> Composite<()> {
        Composite::Unnamed(fields)
    }

    fn custom_u32_key(name: &str, value: u32) -> Key {
        Key::Custom(CustomKey {
            name: name.to_owned(),
            value: CustomValue::U32(value),
        })
    }

    fn custom_bytes32_key(name: &str, value: [u8; 32]) -> Key {
        Key::Custom(CustomKey {
            name: name.to_owned(),
            value: CustomValue::Bytes32(Bytes32(value)),
        })
    }

    fn builtin_u32_key(name: &str, value: u32) -> Key {
        custom_u32_key(name, value)
    }

    fn builtin_bytes32_key(name: &str, value: [u8; 32]) -> Key {
        custom_bytes32_key(name, value)
    }

    fn test_config() -> ChainConfig {
        toml::from_str(crate::config::POLKADOT_TOML).unwrap()
    }

    fn acuity_config() -> ChainConfig {
        toml::from_str(
            r#"
name = "acuity-runtime"
genesis_hash = "0000000000000000000000000000000000000000000000000000000000000001"
default_url = "ws://127.0.0.1:9944"
versions = [0]

[custom_keys]
item_id = "bytes32"
revision_id = "u32"

            [[pallets]]
            name = "Content"
            events = [
              { name = "PublishItem", params = [
                { field = "item_id", key = "item_id" },
                { field = "owner", key = "account_id" },
                { field = "parents", key = "item_id", multi = true },
              ]},
              { name = "PublishRevision", params = [
                { field = "item_id", key = "item_id" },
                { field = "owner", key = "account_id" },
                { field = "revision_id", key = "revision_id" },
                { field = "links", key = "item_id", multi = true },
                { field = "mentions", key = "account_id", multi = true },
              ]},
            ]
"#,
        )
        .unwrap()
    }

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    fn should_store_event(index_variant: bool, keys: &[Key]) -> bool {
        index_variant || !keys.is_empty()
    }

    // ─── keys_for_event: SDK pallet ───────────────────────────────────────

    #[test]
    fn keys_for_event_sdk_pallet() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let acct = [0xAAu8; 32];
        let fields = named(vec![("account", bytes32_val(acct))]);
        let keys = indexer.keys_for_event("System", "NewAccount", &fields);
        assert_eq!(keys, vec![builtin_bytes32_key("account_id", acct)]);
    }

    // ─── keys_for_event: custom pallet ────────────────────────────────────

    #[test]
    fn keys_for_event_custom_pallet_claims() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let who = [0xBBu8; 32];
        let fields = named(vec![("who", bytes32_val(who))]);
        let keys = indexer.keys_for_event("Claims", "Claimed", &fields);
        assert_eq!(keys, vec![builtin_bytes32_key("account_id", who)]);
    }

    #[test]
    fn keys_for_event_sdk_pallet_paras_positional() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = unnamed(vec![bytes32_val([0x77; 32])]);
        let keys = indexer.keys_for_event("Paras", "CurrentCodeUpdated", &fields);
        assert_eq!(keys, vec![custom_bytes32_key("id", [0x77; 32])]);
    }

    #[test]
    fn keys_for_event_sdk_pallet_registrar_multi() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let manager = [0xCCu8; 32];
        let fields = named(vec![
            ("para_id", u128_val(1000)),
            ("manager", bytes32_val(manager)),
        ]);
        let keys = indexer.keys_for_event("Registrar", "Registered", &fields);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&custom_u32_key("para_id", 1000)));
        assert!(keys.contains(&builtin_bytes32_key("account_id", manager)));
    }

    #[test]
    fn keys_for_event_custom_pallet_item_and_revision_keys() {
        let trees = temp_trees();
        let config = acuity_config();
        let indexer = Indexer::new_test(trees, &config);

        let owner = [0xABu8; 32];
        let item_id = [0xCDu8; 32];
        let fields = named(vec![
            ("item_id", bytes32_val(item_id)),
            ("owner", bytes32_val(owner)),
            ("revision_id", u128_val(7)),
        ]);
        let keys = indexer.keys_for_event("Content", "PublishRevision", &fields);
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&custom_bytes32_key("item_id", item_id)));
        assert!(keys.contains(&builtin_bytes32_key("account_id", owner)));
        assert!(keys.contains(&custom_u32_key("revision_id", 7)));
    }

    #[test]
    fn keys_for_event_custom_pallet_multi_value_item_and_account_keys() {
        let trees = temp_trees();
        let config = acuity_config();
        let indexer = Indexer::new_test(trees, &config);

        let owner = [0xABu8; 32];
        let item_id = [0xCDu8; 32];
        let parent_a = [0x11u8; 32];
        let parent_b = [0x12u8; 32];
        let link_a = [0x21u8; 32];
        let link_b = [0x22u8; 32];
        let mention_a = [0x31u8; 32];
        let mention_b = [0x32u8; 32];

        let publish_item_fields = named(vec![
            ("item_id", bytes32_val(item_id)),
            ("owner", bytes32_val(owner)),
            (
                "parents",
                vec_val(vec![bytes32_val(parent_a), bytes32_val(parent_b)]),
            ),
        ]);
        let item_keys = indexer.keys_for_event("Content", "PublishItem", &publish_item_fields);
        assert_eq!(item_keys.len(), 4);
        assert!(item_keys.contains(&custom_bytes32_key("item_id", item_id)));
        assert!(item_keys.contains(&builtin_bytes32_key("account_id", owner)));
        assert!(item_keys.contains(&custom_bytes32_key("item_id", parent_a)));
        assert!(item_keys.contains(&custom_bytes32_key("item_id", parent_b)));

        let publish_revision_fields = named(vec![
            ("item_id", bytes32_val(item_id)),
            ("owner", bytes32_val(owner)),
            ("revision_id", u128_val(7)),
            (
                "links",
                vec_val(vec![bytes32_val(link_a), bytes32_val(link_b)]),
            ),
            (
                "mentions",
                vec_val(vec![bytes32_val(mention_a), bytes32_val(mention_b)]),
            ),
        ]);
        let revision_keys =
            indexer.keys_for_event("Content", "PublishRevision", &publish_revision_fields);
        assert_eq!(revision_keys.len(), 7);
        assert!(revision_keys.contains(&custom_bytes32_key("item_id", item_id)));
        assert!(revision_keys.contains(&builtin_bytes32_key("account_id", owner)));
        assert!(revision_keys.contains(&custom_u32_key("revision_id", 7)));
        assert!(revision_keys.contains(&custom_bytes32_key("item_id", link_a)));
        assert!(revision_keys.contains(&custom_bytes32_key("item_id", link_b)));
        assert!(revision_keys.contains(&builtin_bytes32_key("account_id", mention_a)));
        assert!(revision_keys.contains(&builtin_bytes32_key("account_id", mention_b)));
    }

    // ─── keys_for_event: unknown pallet/event ─────────────────────────────

    #[test]
    fn keys_for_event_unknown_pallet_returns_empty() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![]);
        let keys = indexer.keys_for_event("UnknownPallet", "Foo", &fields);
        assert!(keys.is_empty());
    }

    #[test]
    fn keys_for_event_unknown_event_in_custom_pallet() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![]);
        let keys = indexer.keys_for_event("Claims", "NonExistent", &fields);
        assert!(keys.is_empty());
    }

    // ─── DB write & read round trip ───────────────────────────────────────

    #[test]
    fn index_and_retrieve_account_id() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let acct = Bytes32([0xDD; 32]);
        let key = builtin_bytes32_key("account_id", acct.0);
        indexer.index_event_key(key.clone(), 100, 3).unwrap();
        indexer.index_event_key(key.clone(), 200, 1).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 2);
        // Results come in reverse order (newest first).
        assert_eq!(events[0].block_number, 200);
        assert_eq!(events[1].block_number, 100);
    }

    #[test]
    fn index_and_retrieve_custom_u32_key() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = custom_u32_key("para_id", 2000);
        indexer.index_event_key(key.clone(), 50, 0).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 50);
        assert_eq!(events[0].event_index, 0);
    }

    #[test]
    fn should_store_event_when_configured_keys_exist() {
        let keys = vec![builtin_u32_key("ref_index", 7)];
        assert!(should_store_event(false, &keys));
    }

    #[test]
    fn should_store_event_when_variant_indexing_enabled() {
        assert!(should_store_event(true, &[]));
    }

    #[test]
    fn should_not_store_unindexed_event() {
        assert!(!should_store_event(false, &[]));
    }

    #[test]
    fn index_and_retrieve_custom_bytes32_key() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = custom_bytes32_key("item_id", [0x21; 32]);
        indexer.index_event_key(key.clone(), 75, 2).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 75);
        assert_eq!(events[0].event_index, 2);
    }

    #[test]
    fn index_and_retrieve_custom_u128_key() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::Custom(CustomKey {
            name: "revision_id".into(),
            value: CustomValue::U128(U128Text(42)),
        });
        indexer.index_event_key(key.clone(), 88, 1).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 88);
        assert_eq!(events[0].event_index, 1);
    }

    #[test]
    fn index_variant_key() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::Variant(5, 3);
        indexer.index_event_key(key.clone(), 10, 2).unwrap();
        indexer.index_event_key(key.clone(), 20, 4).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 20);
        assert_eq!(events[1].block_number, 10);
    }

    #[test]
    fn retrieve_max_100_events() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = builtin_u32_key("era_index", 1);
        for i in 0..150u32 {
            indexer.index_event_key(key.clone(), i, 0).unwrap();
        }

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 100);
        // Should be the most recent 100.
        assert_eq!(events[0].block_number, 149);
        assert_eq!(events[99].block_number, 50);
    }

    // ─── encode_event ─────────────────────────────────────────────────────

    #[test]
    fn encode_event_structure() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = named(vec![("amount", u128_val(999))]);
        let json = indexer.encode_event("Balances", "Deposit", 5, 2, 7, &fields);

        assert_eq!(json["palletName"], "Balances");
        assert_eq!(json["eventName"], "Deposit");
        assert_eq!(json["palletIndex"], 5);
        assert_eq!(json["variantIndex"], 2);
        assert_eq!(json["eventIndex"], 7);
        assert_eq!(json["fields"]["amount"], "999");
    }

    // ─── composite_to_json ────────────────────────────────────────────────

    #[test]
    fn composite_to_json_named() {
        let c = named(vec![("x", u128_val(1)), ("y", u128_val(2))]);
        let j = composite_to_json(&c);
        assert_eq!(j["x"], "1");
        assert_eq!(j["y"], "2");
    }

    #[test]
    fn composite_to_json_unnamed_single() {
        let c = unnamed(vec![u128_val(42)]);
        let j = composite_to_json(&c);
        assert_eq!(j, "42");
    }

    #[test]
    fn composite_to_json_unnamed_multi() {
        let c = unnamed(vec![u128_val(1), u128_val(2), u128_val(3)]);
        let j = composite_to_json(&c);
        // 3 U128 values ≤ 255 → treated as byte array
        assert_eq!(j, "0x010203");
    }

    #[test]
    fn composite_to_json_byte_array_32() {
        let bytes: Vec<Value<()>> = (0..32).map(|i| u128_val(i as u128)).collect();
        let c = unnamed(bytes);
        let j = composite_to_json(&c);
        // Should be hex string
        assert!(j.as_str().unwrap().starts_with("0x"));
        assert_eq!(j.as_str().unwrap().len(), 66); // "0x" + 64 hex chars
    }

    #[test]
    fn composite_to_json_non_byte_unnamed() {
        // Values > 255 prevent byte-array detection
        let c = unnamed(vec![u128_val(1000), u128_val(2000)]);
        let j = composite_to_json(&c);
        assert!(j.is_array());
        assert_eq!(j.as_array().unwrap().len(), 2);
    }

    #[test]
    fn value_to_json_bool() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::Bool(true)),
            context: (),
        };
        let j = super::indexer_tests_helpers::value_to_json_pub(&v);
        assert_eq!(j, true);
    }

    #[test]
    fn value_to_json_string() {
        let v = Value {
            value: ValueDef::Primitive(Primitive::String("hello".into())),
            context: (),
        };
        let j = super::indexer_tests_helpers::value_to_json_pub(&v);
        assert_eq!(j, "hello");
    }

    // ─── Span helpers ─────────────────────────────────────────────────────

    #[test]
    fn check_next_batch_block_skips_spans() {
        let spans = vec![Span { start: 10, end: 20 }, Span { start: 30, end: 40 }];
        let mut next = Some(35u32);
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, Some(29));
    }

    #[test]
    fn check_next_batch_block_no_overlap() {
        let spans = vec![Span { start: 10, end: 20 }];
        let mut next = Some(25u32);
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, Some(25)); // unchanged
    }

    #[test]
    fn check_next_batch_block_multiple_overlaps() {
        let spans = vec![Span { start: 5, end: 10 }, Span { start: 11, end: 20 }];
        let mut next = Some(15u32);
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, Some(4));
    }

    #[test]
    fn check_next_batch_block_skips_to_none_at_zero() {
        let spans = vec![Span { start: 0, end: 4 }];
        let mut next = Some(3u32);
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, None);
    }

    #[test]
    fn check_next_batch_block_preserves_none() {
        let spans = vec![Span { start: 0, end: 4 }];
        let mut next = None;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, None);
    }

    #[test]
    fn check_span_merges_adjacent() {
        let trees = temp_trees();
        let mut spans = vec![Span { start: 10, end: 20 }];

        // Write the old span to the DB.
        let sv = SpanDbValue {
            start: 10u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(20u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let mut current = Span { start: 21, end: 30 };

        check_span(&trees.span, &mut spans, &mut current).unwrap();
        assert_eq!(current.start, 10);
        assert!(spans.is_empty());
    }

    #[test]
    fn check_span_no_merge_when_gap() {
        let trees = temp_trees();
        let mut spans = vec![Span { start: 10, end: 20 }];
        let mut current = Span { start: 25, end: 30 };

        check_span(&trees.span, &mut spans, &mut current).unwrap();
        assert_eq!(current.start, 25);
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn check_span_does_not_underflow_at_zero() {
        let trees = temp_trees();
        let mut spans = vec![Span { start: 0, end: 0 }];
        let mut current = Span { start: 0, end: 5 };

        check_span(&trees.span, &mut spans, &mut current).unwrap();
        assert_eq!(current.start, 0);
        assert_eq!(spans.len(), 1);
    }

    // ─── Trees flush ──────────────────────────────────────────────────────

    #[test]
    fn trees_flush_succeeds() {
        let trees = temp_trees();
        trees.flush().unwrap();
    }

    #[test]
    fn load_spans_reads_existing_span() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 5u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(15u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0], true, true).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 5);
        assert_eq!(spans[0].end, 15);
    }

    #[test]
    fn load_spans_drops_when_variant_indexing_enabled() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 5u32.into(),
            version: 0u16.into(),
            index_variant: 0,
            store_events: 1,
        };
        trees
            .span
            .insert(15u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0], true, true).unwrap();
        assert!(spans.is_empty());
        assert!(trees.span.get(15u32.to_be_bytes()).unwrap().is_none());
    }

    #[test]
    fn load_spans_drops_when_event_storage_enabled() {
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

        let spans = load_spans(&trees.span, &[0], true, true).unwrap();
        assert!(spans.is_empty());
        assert!(trees.span.get(15u32.to_be_bytes()).unwrap().is_none());
    }

    #[test]
    fn load_spans_reindexes_full_span_when_version_boundary_inside_span() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 120u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(160u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0, 100], true, true).unwrap();
        assert!(spans.is_empty());
        assert!(trees.span.get(160u32.to_be_bytes()).unwrap().is_none());
    }

    #[test]
    fn load_spans_truncates_span_when_version_boundary_splits_span() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 80u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(160u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0, 100], true, true).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 80);
        assert_eq!(spans[0].end, 99);
        assert!(trees.span.get(160u32.to_be_bytes()).unwrap().is_none());
        assert!(trees.span.get(99u32.to_be_bytes()).unwrap().is_some());
    }

    #[test]
    fn load_spans_keeps_span_when_it_ends_before_version_boundary() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 10u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(90u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        let spans = load_spans(&trees.span, &[0, 100], true, true).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 10);
        assert_eq!(spans[0].end, 90);
        assert!(trees.span.get(90u32.to_be_bytes()).unwrap().is_some());
    }

    #[test]
    fn load_spans_uses_earliest_applicable_version_boundary() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 50u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(260u32.to_be_bytes(), zerocopy::IntoBytes::as_bytes(&sv))
            .unwrap();

        // Version 0 spans should reindex from block 100 onward (not 200),
        // because 100 is the earliest boundary newer than span_version.
        let spans = load_spans(&trees.span, &[0, 100, 200], true, true).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 50);
        assert_eq!(spans[0].end, 99);
        assert!(trees.span.get(260u32.to_be_bytes()).unwrap().is_none());
        assert!(trees.span.get(99u32.to_be_bytes()).unwrap().is_some());
    }

    #[tokio::test]
    async fn process_sub_msg_status_subscribe_and_unsubscribe() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let (tx, mut rx) = mpsc::unbounded_channel();
        process_sub_msg(
            &indexer,
            SubscriptionMessage::SubscribeStatus { tx: tx.clone() },
        );
        indexer.notify_status_subscribers();
        assert!(rx.recv().await.is_some());

        process_sub_msg(&indexer, SubscriptionMessage::UnsubscribeStatus { tx });
        indexer.notify_status_subscribers();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn process_sub_msg_event_subscribe_and_unsubscribe() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let key = builtin_u32_key("ref_index", 42);
        let (tx, mut rx) = mpsc::unbounded_channel();

        process_sub_msg(
            &indexer,
            SubscriptionMessage::SubscribeEvents {
                key: key.clone(),
                tx: tx.clone(),
            },
        );

        indexer.index_event_key(key.clone(), 7, 1).unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ResponseMessage::Events { .. })
        ));

        process_sub_msg(&indexer, SubscriptionMessage::UnsubscribeEvents { key, tx });

        indexer
            .index_event_key(builtin_u32_key("ref_index", 42), 8, 2)
            .unwrap();
        assert!(rx.try_recv().is_err());
    }
}

// Helper module to expose private functions for testing.
#[cfg(test)]
mod indexer_tests_helpers {
    use scale_value::Value;

    // Re-export the private value_to_json for tests.
    pub fn value_to_json_pub(v: &Value<()>) -> serde_json::Value {
        crate::indexer::composite_to_json(&scale_value::Composite::Unnamed(vec![v.clone()]))
    }
}
