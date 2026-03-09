#[cfg(test)]
mod indexer_tests {
    use crate::config::ChainConfig;
    use crate::indexer::*;
    use crate::shared::*;
    use scale_value::{Composite, Primitive, Value, ValueDef};

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

    fn test_config() -> ChainConfig {
        toml::from_str(crate::config::POLKADOT_TOML).unwrap()
    }

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
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
        assert_eq!(keys, vec![Key::AccountId(Bytes32(acct))]);
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
        assert_eq!(keys, vec![Key::AccountId(Bytes32(who))]);
    }

    #[test]
    fn keys_for_event_custom_pallet_paras_positional() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees, &config);

        let fields = unnamed(vec![u128_val(2000)]);
        let keys = indexer.keys_for_event("Paras", "CurrentCodeUpdated", &fields);
        assert_eq!(keys, vec![Key::ParaId(2000)]);
    }

    #[test]
    fn keys_for_event_custom_pallet_registrar_multi() {
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
        assert!(keys.contains(&Key::ParaId(1000)));
        assert!(keys.contains(&Key::AccountId(Bytes32(manager))));
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
        let key = Key::AccountId(acct);
        indexer.index_event_key(key.clone(), 100, 3).unwrap();
        indexer.index_event_key(key.clone(), 200, 1).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 2);
        // Results come in reverse order (newest first).
        assert_eq!(events[0].block_number, 200);
        assert_eq!(events[1].block_number, 100);
    }

    #[test]
    fn index_and_retrieve_para_id() {
        let trees = temp_trees();
        let config = test_config();
        let indexer = Indexer::new_test(trees.clone(), &config);

        let key = Key::ParaId(2000);
        indexer.index_event_key(key.clone(), 50, 0).unwrap();

        let events = key.get_events(&trees);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 50);
        assert_eq!(events[0].event_index, 0);
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

        let key = Key::EraIndex(1);
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
        let mut next = 35u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 29);
    }

    #[test]
    fn check_next_batch_block_no_overlap() {
        let spans = vec![Span { start: 10, end: 20 }];
        let mut next = 25u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 25); // unchanged
    }

    #[test]
    fn check_next_batch_block_multiple_overlaps() {
        let spans = vec![Span { start: 5, end: 10 }, Span { start: 11, end: 20 }];
        let mut next = 15u32;
        check_next_batch_block(&spans, &mut next);
        assert_eq!(next, 4);
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
            .insert(20u32.to_be_bytes(), zerocopy::AsBytes::as_bytes(&sv))
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

    // ─── Trees flush ──────────────────────────────────────────────────────

    #[test]
    fn trees_flush_succeeds() {
        let trees = temp_trees();
        trees.flush().unwrap();
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
