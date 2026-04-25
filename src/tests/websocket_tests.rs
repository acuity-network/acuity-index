#[cfg(test)]
mod websocket_tests {
    use crate::shared::*;
    use crate::websockets::*;
    use zerocopy::IntoBytes;

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
            ResponseBody::Status(spans) => assert!(spans.is_empty()),
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn process_msg_status_with_spans() {
        let trees = temp_trees();
        let sv = SpanDbValue {
            start: 100u32.into(),
            version: 0u16.into(),
        };
        trees
            .span
            .insert(200u32.to_be_bytes(), sv.as_bytes())
            .unwrap();

        let msg = process_msg_status(&trees.span);
        match msg {
            ResponseBody::Status(spans) => {
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].start, 100);
                assert_eq!(spans[0].end, 200);
            }
            _ => panic!("wrong response type"),
        }
    }

    #[test]
    fn get_events_custom_empty_tree() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "para_id".into(),
            value: CustomValue::U32(999),
        });
        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_bytes32_empty_tree() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32([0; 32])),
        });
        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert!(events.is_empty());
    }

    #[test]
    fn get_events_u32_multiple() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        key.write_db_key(&trees, 10, 0).unwrap();
        key.write_db_key(&trees, 20, 1).unwrap();
        key.write_db_key(&trees, 30, 2).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 3);
        // Reverse order (newest first).
        assert_eq!(events[0].block_number, 30);
        assert_eq!(events[2].block_number, 10);
    }

    #[test]
    fn get_events_bytes32_multiple() {
        let trees = temp_trees();
        let b = Bytes32([0x11; 32]);
        let key = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(b),
        });
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    #[test]
    fn get_events_composite_multiple() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "item_revision".into(),
            value: CustomValue::Composite(vec![
                CustomValue::Bytes32(Bytes32([0x11; 32])),
                CustomValue::U32(7),
            ]),
        });
        key.write_db_key(&trees, 5, 0).unwrap();
        key.write_db_key(&trees, 15, 1).unwrap();

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(&trees.index, &prefix, None, 100);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].block_number, 15);
        assert_eq!(events[1].block_number, 5);
    }

    #[test]
    fn get_events_before_cursor_filters_newer_results() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        for (block_number, event_index) in [(10, 0), (20, 1), (20, 2), (30, 0)] {
            key.write_db_key(&trees, block_number, event_index).unwrap();
        }

        let prefix = key.index_prefix().unwrap().unwrap();
        let events = get_events_index(
            &trees.index,
            &prefix,
            Some(&EventRef {
                block_number: 20,
                event_index: 2,
            }),
            100,
        );
        assert_eq!(
            events,
            vec![
                EventRef {
                    block_number: 20,
                    event_index: 1,
                },
                EventRef {
                    block_number: 10,
                    event_index: 0,
                }
            ]
        );
    }

    #[test]
    fn get_events_index_honors_cursor_and_limit_clamping_inputs() {
        let trees = temp_trees();
        let key = Key::Custom(CustomKey {
            name: "ref_index".into(),
            value: CustomValue::U32(42),
        });

        for i in 0..5u32 {
            key.write_db_key(&trees, i + 1, i).unwrap();
        }

        let prefix = key.index_prefix().unwrap().unwrap();
        let before = EventRef {
            block_number: 4,
            event_index: 3,
        };
        let events = get_events_index(
            &trees.index,
            &prefix,
            Some(&before),
            1,
        );
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            EventRef {
                block_number: 3,
                event_index: 2,
            }
        );

        let events = get_events_index(&trees.index, &prefix, None, 1000);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn error_response_serializes_structured_error() {
        let msg = ResponseMessage {
            id: Some(9),
            body: ResponseBody::Error(ApiError {
                code: "invalid_request",
                message: "missing field `id`".into(),
            }),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("invalid_request"));
        assert!(json.contains("missing field `id`"));
    }

    // ─── ResponseMessage serialization ────────────────────────────────────

    #[test]
    fn response_status_serializes() {
        let msg = ResponseMessage {
            id: Some(1),
            body: ResponseBody::Status(vec![Span { start: 1, end: 100 }]),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("status"));
        assert!(json.contains("100"));
    }

    #[test]
    fn response_subscription_status_serializes() {
        let msg = ResponseMessage {
            id: Some(2),
            body: ResponseBody::SubscriptionStatus {
                action: SubscriptionAction::Subscribed,
                target: SubscriptionTarget::Status,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("subscribed"));
    }

    #[test]
    fn response_size_on_disk_serializes() {
        let msg = ResponseMessage {
            id: Some(3),
            body: ResponseBody::SizeOnDisk(123456),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("123456"));
    }

    #[test]
    fn response_events_serializes() {
        let msg = ResponseMessage {
            id: Some(4),
            body: ResponseBody::Events {
                key: Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(42),
                }),
                events: vec![EventRef {
                    block_number: 10,
                    event_index: 2,
                }],
                decoded_events: vec![],
                proofs_by_block: None,
                proofs_status: None,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("ref_index"));
        assert!(json.contains("42"));
        assert!(json.contains("decodedEvents"));
    }

    #[test]
    fn response_subscription_events_target_serializes_key() {
        let msg = ResponseMessage {
            id: Some(10),
            body: ResponseBody::SubscriptionStatus {
                action: SubscriptionAction::Subscribed,
                target: SubscriptionTarget::Events {
                    key: Key::Custom(CustomKey {
                        name: "item_id".into(),
                        value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
                    }),
                },
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("subscriptionStatus"));
        assert!(json.contains("item_id"));
        assert!(json.contains("events"));
    }
}
