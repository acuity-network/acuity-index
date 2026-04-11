#[cfg(test)]
mod shared_tests {
    use crate::shared::*;
    use zerocopy::IntoBytes;

    // ─── Bytes32 ──────────────────────────────────────────────────────────

    #[test]
    fn bytes32_serialize_json() {
        let b = Bytes32([0xAB; 32]);
        let json = serde_json::to_string(&b).unwrap();
        assert_eq!(json, format!("\"0x{}\"", hex::encode([0xAB; 32])));
    }

    #[test]
    fn bytes32_deserialize_json_with_prefix() {
        let hex_str = format!("\"0x{}\"", hex::encode([0xCD; 32]));
        let b: Bytes32 = serde_json::from_str(&hex_str).unwrap();
        assert_eq!(b.0, [0xCD; 32]);
    }

    #[test]
    fn bytes32_deserialize_json_without_prefix() {
        let hex_str = format!("\"{}\"", hex::encode([0xEF; 32]));
        let b: Bytes32 = serde_json::from_str(&hex_str).unwrap();
        assert_eq!(b.0, [0xEF; 32]);
    }

    #[test]
    fn bytes32_deserialize_json_invalid_hex() {
        let bad = "\"0xzz\"";
        let result: Result<Bytes32, _> = serde_json::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn bytes32_deserialize_json_wrong_length() {
        let bad = "\"0xaaaa\"";
        let result: Result<Bytes32, _> = serde_json::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn bytes32_from_array() {
        let arr = [42u8; 32];
        let b: Bytes32 = arr.into();
        assert_eq!(b.0, arr);
    }

    #[test]
    fn bytes32_as_ref_array() {
        let b = Bytes32([1u8; 32]);
        let r: &[u8; 32] = b.as_ref();
        assert_eq!(r, &[1u8; 32]);
    }

    #[test]
    fn bytes32_as_ref_slice() {
        let b = Bytes32([2u8; 32]);
        let r: &[u8] = b.as_ref();
        assert_eq!(r.len(), 32);
    }

    // ─── On-disk key formats ──────────────────────────────────────────────

    #[test]
    fn variant_key_layout() {
        let key = VariantKey {
            pallet_index: 5,
            variant_index: 3,
            block_number: 1000u32.into(),
            event_index: 7u16.into(),
        };
        let bytes = key.as_bytes();
        // 1 + 1 + 4 + 2 = 8 bytes
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes[0], 5);
        assert_eq!(bytes[1], 3);
    }

    #[test]
    fn custom_key_prefix_layout() {
        let bytes32_key = CustomKey {
            name: "item_id".into(),
            value: CustomValue::Bytes32(Bytes32([0xAA; 32])),
        };
        let u32_key = CustomKey {
            name: "index".into(),
            value: CustomValue::U32(42),
        };

        let bytes32_prefix = bytes32_key.db_prefix();
        let u32_prefix = u32_key.db_prefix();

        assert_eq!(&bytes32_prefix[..2], &(7u16).to_be_bytes());
        assert_eq!(&bytes32_prefix[2..9], b"item_id");
        assert_eq!(bytes32_prefix[9], 0);
        assert_eq!(&bytes32_prefix[10..14], &(32u32).to_be_bytes());
        assert_eq!(bytes32_prefix.len(), 46);

        assert_eq!(&u32_prefix[..2], &(5u16).to_be_bytes());
        assert_eq!(&u32_prefix[2..7], b"index");
        assert_eq!(u32_prefix[7], 1);
        assert_eq!(&u32_prefix[8..12], &(4u32).to_be_bytes());
        assert_eq!(&u32_prefix[12..16], &42u32.to_be_bytes());
    }

    #[test]
    fn event_key_layout() {
        let key = EventKey {
            block_number: 123u32.into(),
            event_index: 9u16.into(),
        };
        let bytes = key.as_bytes();
        // 4 + 2 = 6 bytes
        assert_eq!(bytes.len(), 6);
        assert_eq!(&bytes[..4], &123u32.to_be_bytes());
        assert_eq!(&bytes[4..], &9u16.to_be_bytes());
    }

    // ─── Key serialization ────────────────────────────────────────────────

    #[test]
    fn key_json_round_trip_account_id() {
        let k = Key::Custom(CustomKey {
            name: "account_id".into(),
            value: CustomValue::Bytes32(Bytes32([0x11; 32])),
        });
        let json = serde_json::to_string(&k).unwrap();
        let k2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn key_json_round_trip_u32_types() {
        for key in [
            Key::Custom(CustomKey {
                name: "account_index".into(),
                value: CustomValue::U32(1),
            }),
            Key::Custom(CustomKey {
                name: "bounty_index".into(),
                value: CustomValue::U32(3),
            }),
            Key::Custom(CustomKey {
                name: "era_index".into(),
                value: CustomValue::U32(4),
            }),
            Key::Custom(CustomKey {
                name: "pool_id".into(),
                value: CustomValue::U32(5),
            }),
            Key::Custom(CustomKey {
                name: "proposal_index".into(),
                value: CustomValue::U32(6),
            }),
            Key::Custom(CustomKey {
                name: "ref_index".into(),
                value: CustomValue::U32(7),
            }),
            Key::Custom(CustomKey {
                name: "registrar_index".into(),
                value: CustomValue::U32(8),
            }),
            Key::Custom(CustomKey {
                name: "session_index".into(),
                value: CustomValue::U32(9),
            }),
            Key::Custom(CustomKey {
                name: "spend_index".into(),
                value: CustomValue::U32(10),
            }),
        ] {
            let json = serde_json::to_string(&key).unwrap();
            let k2: Key = serde_json::from_str(&json).unwrap();
            assert_eq!(key, k2);
        }
    }

    #[test]
    fn key_json_round_trip_bytes32_types() {
        let b = Bytes32([0xFF; 32]);
        for key in [
            Key::Custom(CustomKey {
                name: "message_id".into(),
                value: CustomValue::Bytes32(b),
            }),
            Key::Custom(CustomKey {
                name: "preimage_hash".into(),
                value: CustomValue::Bytes32(b),
            }),
            Key::Custom(CustomKey {
                name: "proposal_hash".into(),
                value: CustomValue::Bytes32(b),
            }),
            Key::Custom(CustomKey {
                name: "tip_hash".into(),
                value: CustomValue::Bytes32(b),
            }),
        ] {
            let json = serde_json::to_string(&key).unwrap();
            let k2: Key = serde_json::from_str(&json).unwrap();
            assert_eq!(key, k2);
        }
    }

    #[test]
    fn key_json_variant() {
        let k = Key::Variant(5, 3);
        let json = serde_json::to_string(&k).unwrap();
        assert!(json.contains("Variant"));
        let k2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn key_json_round_trip_custom_types() {
        let bytes = Key::Custom(CustomKey {
            name: "item_id".into(),
            value: CustomValue::Bytes32(Bytes32([0xAB; 32])),
        });
        let string = Key::Custom(CustomKey {
            name: "slug".into(),
            value: CustomValue::String("hello-world".into()),
        });
        let big = Key::Custom(CustomKey {
            name: "revision".into(),
            value: CustomValue::U128(U128Text(12345678901234567890u128)),
        });

        for key in [bytes, string, big] {
            let json = serde_json::to_string(&key).unwrap();
            let k2: Key = serde_json::from_str(&json).unwrap();
            assert_eq!(key, k2);
        }
    }

    #[test]
    fn key_write_and_get_events_all_variants() {
        let dir = tempfile::tempdir().unwrap();
        let db_config = sled::Config::new().path(dir.path()).temporary(true);
        let trees = Trees::open(db_config).unwrap();

        let keys = vec![
            Key::Variant(1, 2),
            Key::Custom(CustomKey {
                name: "account_id".into(),
                value: CustomValue::Bytes32(Bytes32([0x01; 32])),
            }),
            Key::Custom(CustomKey {
                name: "account_index".into(),
                value: CustomValue::U32(11),
            }),
            Key::Custom(CustomKey {
                name: "bounty_index".into(),
                value: CustomValue::U32(12),
            }),
            Key::Custom(CustomKey {
                name: "era_index".into(),
                value: CustomValue::U32(13),
            }),
            Key::Custom(CustomKey {
                name: "message_id".into(),
                value: CustomValue::Bytes32(Bytes32([0x02; 32])),
            }),
            Key::Custom(CustomKey {
                name: "pool_id".into(),
                value: CustomValue::U32(14),
            }),
            Key::Custom(CustomKey {
                name: "preimage_hash".into(),
                value: CustomValue::Bytes32(Bytes32([0x03; 32])),
            }),
            Key::Custom(CustomKey {
                name: "proposal_hash".into(),
                value: CustomValue::Bytes32(Bytes32([0x04; 32])),
            }),
            Key::Custom(CustomKey {
                name: "proposal_index".into(),
                value: CustomValue::U32(15),
            }),
            Key::Custom(CustomKey {
                name: "ref_index".into(),
                value: CustomValue::U32(16),
            }),
            Key::Custom(CustomKey {
                name: "registrar_index".into(),
                value: CustomValue::U32(17),
            }),
            Key::Custom(CustomKey {
                name: "session_index".into(),
                value: CustomValue::U32(18),
            }),
            Key::Custom(CustomKey {
                name: "tip_hash".into(),
                value: CustomValue::Bytes32(Bytes32([0x05; 32])),
            }),
            Key::Custom(CustomKey {
                name: "spend_index".into(),
                value: CustomValue::U32(19),
            }),
            Key::Custom(CustomKey {
                name: "auction_index".into(),
                value: CustomValue::U32(20),
            }),
            Key::Custom(CustomKey {
                name: "candidate_hash".into(),
                value: CustomValue::Bytes32(Bytes32([0x06; 32])),
            }),
            Key::Custom(CustomKey {
                name: "para_id".into(),
                value: CustomValue::U32(21),
            }),
            Key::Custom(CustomKey {
                name: "item_id".into(),
                value: CustomValue::Bytes32(Bytes32([0x07; 32])),
            }),
            Key::Custom(CustomKey {
                name: "revision_id".into(),
                value: CustomValue::U128(U128Text(22)),
            }),
        ];

        for (i, key) in keys.into_iter().enumerate() {
            key.write_db_key(&trees, 1000 + i as u32, i as u16).unwrap();
            let events = key.get_events(&trees, None, 100);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].block_number, 1000 + i as u32);
            assert_eq!(events[0].event_index, i as u16);
        }
    }

    // ─── RequestMessage deserialization ───────────────────────────────────

    #[test]
    fn request_status() {
        let msg: RequestMessage = serde_json::from_str(r#"{"id":1,"type":"Status"}"#).unwrap();
        assert_eq!(msg.id, 1);
        assert!(matches!(msg.body, RequestBody::Status));
    }

    #[test]
    fn request_subscribe_status() {
        let msg: RequestMessage =
            serde_json::from_str(r#"{"id":2,"type":"SubscribeStatus"}"#).unwrap();
        assert_eq!(msg.id, 2);
        assert!(matches!(msg.body, RequestBody::SubscribeStatus));
    }

    #[test]
    fn request_get_events_custom_u32() {
        let json = r#"{"id":3,"type":"GetEvents","key":{"type":"Custom","value":{"name":"para_id","kind":"u32","value":2000}},"limit":25}"#;
        let msg: RequestMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.id, 3);
        match msg.body {
            RequestBody::GetEvents {
                key:
                    Key::Custom(CustomKey {
                        name,
                        value: CustomValue::U32(id),
                    }),
                limit,
                before,
            } => {
                assert_eq!(name, "para_id");
                assert_eq!(id, 2000);
                assert_eq!(limit, 25);
                assert!(before.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_get_events_composite_key() {
        let hex = hex::encode([0xAB; 32]);
        let json = format!(
            r#"{{"id":6,"type":"GetEvents","key":{{"type":"Custom","value":{{"name":"item_revision","kind":"composite","value":[{{"kind":"bytes32","value":"0x{hex}"}},{{"kind":"u32","value":7}}]}}}}}}"#
        );
        let msg: RequestMessage = serde_json::from_str(&json).unwrap();

        match msg.body {
            RequestBody::GetEvents {
                key:
                    Key::Custom(CustomKey {
                        name,
                        value: CustomValue::Composite(values),
                    }),
                ..
            } => {
                assert_eq!(name, "item_revision");
                assert_eq!(
                    values,
                    vec![
                        CustomValue::Bytes32(Bytes32([0xAB; 32])),
                        CustomValue::U32(7)
                    ]
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_get_events_account_id() {
        let hex = hex::encode([0xAA; 32]);
        let json = format!(
            r#"{{"id":4,"type":"GetEvents","key":{{"type":"Custom","value":{{"name":"account_id","kind":"bytes32","value":"0x{hex}"}}}},"before":{{"blockNumber":10,"eventIndex":2}}}}"#
        );
        let msg: RequestMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.id, 4);
        match msg.body {
            RequestBody::GetEvents {
                key:
                    Key::Custom(CustomKey {
                        name,
                        value: CustomValue::Bytes32(b),
                    }),
                limit,
                before,
            } => {
                assert_eq!(name, "account_id");
                assert_eq!(b.0, [0xAA; 32]);
                assert_eq!(limit, 100);
                assert_eq!(
                    before,
                    Some(EventRef {
                        block_number: 10,
                        event_index: 2,
                    })
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_get_events_defaults_limit_when_omitted() {
        let json = r#"{"id":5,"type":"GetEvents","key":{"type":"Custom","value":{"name":"pool_id","kind":"u32","value":9}}}"#;
        let msg: RequestMessage = serde_json::from_str(json).unwrap();

        match msg.body {
            RequestBody::GetEvents { limit, before, .. } => {
                assert_eq!(limit, 100);
                assert!(before.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    // ─── EventRef / Span Display ──────────────────────────────────────────

    #[test]
    fn event_ref_display() {
        let e = EventRef {
            block_number: 100,
            event_index: 5,
        };
        let s = format!("{e}");
        assert!(s.contains("100"));
        assert!(s.contains("5"));
    }

    #[test]
    fn span_display() {
        let s = Span { start: 10, end: 20 };
        let out = format!("{s}");
        assert!(out.contains("10"));
        assert!(out.contains("20"));
    }

    #[test]
    fn state_pruning_misconfigured_error_mentions_required_flags() {
        let err = IndexError::StatePruningMisconfigured { block_number: 42 };
        let text = err.to_string();

        assert!(text.contains("#42"));
        assert!(text.contains("--state-pruning must be set to archive-canonical"));
    }

    #[test]
    fn response_events_with_decoded_events_serializes() {
        let msg = ResponseMessage {
            id: Some(7),
            body: ResponseBody::Events {
                key: Key::Custom(CustomKey {
                    name: "ref_index".into(),
                    value: CustomValue::U32(42),
                }),
                events: vec![EventRef {
                    block_number: 10,
                    event_index: 2,
                }],
                decoded_events: vec![DecodedEvent {
                    block_number: 10,
                    event_index: 2,
                    event: serde_json::json!({
                        "specVersion": 1234,
                        "eventName": "Deposit"
                    }),
                }],
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("decodedEvents"));
        assert!(json.contains("specVersion"));
        assert!(json.contains("Deposit"));
    }

    #[test]
    fn response_error_without_id_omits_id_field() {
        let msg = ResponseMessage {
            id: None,
            body: ResponseBody::Error(ApiError {
                code: "invalid_request",
                message: "missing field `id`".into(),
            }),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(!json.contains("\"id\""));
    }

    #[test]
    fn notification_subscription_terminated_serializes() {
        let msg = NotificationMessage {
            body: NotificationBody::SubscriptionTerminated {
                reason: SubscriptionTerminationReason::Backpressure,
                message: "subscriber disconnected due to backpressure".into(),
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("subscriptionTerminated"));
        assert!(json.contains("backpressure"));
    }
}
