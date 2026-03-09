#[cfg(test)]
mod shared_tests {
    use crate::shared::*;
    use zerocopy::AsBytes;

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
    fn bytes32_key_layout() {
        let key = Bytes32Key {
            key: [0xAA; 32],
            block_number: 500u32.into(),
            event_index: 2u16.into(),
        };
        let bytes = key.as_bytes();
        // 32 + 4 + 2 = 38 bytes
        assert_eq!(bytes.len(), 38);
    }

    #[test]
    fn u32_key_layout() {
        let key = U32Key {
            key: 42u32.into(),
            block_number: 100u32.into(),
            event_index: 1u16.into(),
        };
        let bytes = key.as_bytes();
        // 4 + 4 + 2 = 10 bytes
        assert_eq!(bytes.len(), 10);
    }

    // ─── Key serialization ────────────────────────────────────────────────

    #[test]
    fn key_json_round_trip_account_id() {
        let k = Key::AccountId(Bytes32([0x11; 32]));
        let json = serde_json::to_string(&k).unwrap();
        let k2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn key_json_round_trip_u32_types() {
        for key in [
            Key::AccountIndex(1),
            Key::AuctionIndex(2),
            Key::BountyIndex(3),
            Key::EraIndex(4),
            Key::ParaId(1000),
            Key::PoolId(5),
            Key::ProposalIndex(6),
            Key::RefIndex(7),
            Key::RegistrarIndex(8),
            Key::SessionIndex(9),
            Key::SpendIndex(10),
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
            Key::CandidateHash(b),
            Key::MessageId(b),
            Key::PreimageHash(b),
            Key::ProposalHash(b),
            Key::TipHash(b),
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

    // ─── RequestMessage deserialization ───────────────────────────────────

    #[test]
    fn request_status() {
        let msg: RequestMessage = serde_json::from_str(r#"{"type":"Status"}"#).unwrap();
        assert!(matches!(msg, RequestMessage::Status));
    }

    #[test]
    fn request_subscribe_status() {
        let msg: RequestMessage = serde_json::from_str(r#"{"type":"SubscribeStatus"}"#).unwrap();
        assert!(matches!(msg, RequestMessage::SubscribeStatus));
    }

    #[test]
    fn request_get_events_para_id() {
        let json = r#"{"type":"GetEvents","key":{"type":"ParaId","value":2000}}"#;
        let msg: RequestMessage = serde_json::from_str(json).unwrap();
        match msg {
            RequestMessage::GetEvents {
                key: Key::ParaId(id),
            } => {
                assert_eq!(id, 2000)
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_get_events_account_id() {
        let hex = hex::encode([0xAA; 32]);
        let json =
            format!(r#"{{"type":"GetEvents","key":{{"type":"AccountId","value":"0x{hex}"}}}}"#);
        let msg: RequestMessage = serde_json::from_str(&json).unwrap();
        match msg {
            RequestMessage::GetEvents {
                key: Key::AccountId(b),
            } => assert_eq!(b.0, [0xAA; 32]),
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
}


