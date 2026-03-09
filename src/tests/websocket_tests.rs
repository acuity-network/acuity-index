#[cfg(test)]
mod websocket_tests {
    use crate::shared::*;
    use crate::websockets::*;
    use zerocopy::AsBytes;

    fn temp_trees() -> Trees {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        Trees::open(db_config).unwrap()
    }

    #[test]
    fn process_msg_status_empty() {
        let trees = temp_trees();
        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseMessage::Status(spans) => assert!(spans.is_empty()),
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_status_with_spans() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 100u32.into(),
            version: 0u16.into(),
            index_variant: 1,
            store_events: 1,
        };
        trees
            .span
            .insert(200u32.to_be_bytes(), sv.as_bytes())
            .unwrap();

        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseMessage::Status(spans) => {
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].start, 100);
                assert_eq!(spans[0].end, 200);
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_get_events_empty() {
        let trees = temp_trees();
        let key = Key::AccountId(Bytes32([0; 32]));
        let msg = process_msg_get_events(&trees, key.clone());
        match msg {
            ResponseMessage::Events {
                key: k,
                events,
                block_events,
            } => {
                assert_eq!(k, key);
                assert!(events.is_empty());
                assert!(block_events.is_empty());
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_get_events_with_data() {
        let trees = temp_trees();

        // Insert an index entry.
        let key = Key::ParaId(1000);
        key.write_db_key(&trees, 50, 3).unwrap();

        // Insert block events JSON.
        let bn_key: zerocopy::byteorder::U32<zerocopy::BigEndian> = 50u32.into();
        let json = serde_json::to_vec(&serde_json::json!([
            {"palletName": "Paras", "eventName": "Test"}
        ]))
        .unwrap();
        trees
            .block_events
            .insert(bn_key.as_bytes(), json.as_slice())
            .unwrap();

        let msg = process_msg_get_events(&trees, key);
        match msg {
            ResponseMessage::Events {
                events,
                block_events,
                ..
            } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].block_number, 50);
                assert_eq!(events[0].event_index, 3);
                assert_eq!(block_events.len(), 1);
                assert_eq!(block_events[0].block_number, 50);
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn get_events_u32_empty_tree() {
        let trees = temp_trees();
        let events = get_events_u32(&trees.chain.para_id, 999);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_bytes32_empty_tree() {
        let trees = temp_trees();
        let b = Bytes32([0; 32]);
        let events = get_events_bytes32(&trees.substrate.account_id, &b);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_u32_multiple() {
        let trees = temp_trees();
        let key = Key::PoolId(7);
        key.write_db_key(&trees, 10, 0).unwrap();
        key.write_db_key(&trees, 20, 1).unwrap();
        key.write_db_key(&trees, 30, 2).unwrap();

        let events = get_events_u32(&trees.substrate.pool_id, 7);
        assert_eq!(events.len(), 3);
        // Reverse order (newest first).
        assert_eq!(events[0].block_number, 30);
        assert_eq!(events[2].block_number, 10);
    }

    #[test]
    fn get_events_bytes32_multiple() {
        let trees = temp_trees();
        let b = Bytes32([0x11; 32]);
        let key = Key::AccountId(b);
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let events = get_events_bytes32(&trees.substrate.account_id, &b);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    // ─── ResponseMessage serialization ────────────────────────────────────

    #[test]
    fn response_status_serializes() {
        let msg = ResponseMessage::Status(vec![Span { start: 1, end: 100 }]);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Status"));
        assert!(json.contains("100"));
    }

    #[test]
    fn response_subscribed_serializes() {
        let msg = ResponseMessage::Subscribed;
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Subscribed"));
    }

    #[test]
    fn response_size_on_disk_serializes() {
        let msg = ResponseMessage::SizeOnDisk(123456);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("123456"));
    }

    #[test]
    fn response_events_serializes() {
        let msg = ResponseMessage::Events {
            key: Key::RefIndex(42),
            events: vec![EventRef {
                block_number: 10,
                event_index: 2,
            }],
            block_events: vec![],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("RefIndex"));
        assert!(json.contains("42"));
    }
}

