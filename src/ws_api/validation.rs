use crate::{errors::IndexError, protocol::*};

use std::collections::HashSet;

use super::disconnect_error;

pub(crate) const MAX_WS_MESSAGE_SIZE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_WS_FRAME_SIZE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_CUSTOM_KEY_NAME_BYTES: usize = 128;
pub(crate) const MAX_CUSTOM_STRING_VALUE_BYTES: usize = 1024;
pub(crate) const MAX_COMPOSITE_ELEMENTS: usize = 64;
pub(crate) const MAX_COMPOSITE_DEPTH: usize = 8;
pub(crate) const MAX_CUSTOM_VALUE_BYTES: usize = 16384;

pub(crate) fn clamp_events_limit(limit: u16, max_events_limit: usize) -> usize {
    usize::from(limit).clamp(1, max_events_limit.max(1))
}

pub(crate) fn validate_key(key: &Key) -> Result<(), String> {
    let Key::Custom(custom) = key else {
        return Ok(());
    };

    let name_len = custom.name.len();
    if name_len > MAX_CUSTOM_KEY_NAME_BYTES {
        return Err(format!(
            "custom key name exceeds {MAX_CUSTOM_KEY_NAME_BYTES} bytes: {name_len}"
        ));
    }

    let encoded_len = validate_custom_value(&custom.value, 0)?;
    if encoded_len > MAX_CUSTOM_VALUE_BYTES {
        return Err(format!(
            "custom key encoded value exceeds {MAX_CUSTOM_VALUE_BYTES} bytes: {encoded_len} bytes"
        ));
    }

    Ok(())
}

fn validate_custom_value(value: &CustomValue, depth: usize) -> Result<usize, String> {
    if depth > MAX_COMPOSITE_DEPTH {
        return Err(format!(
            "composite key nesting exceeds max depth {MAX_COMPOSITE_DEPTH}"
        ));
    }

    match value {
        CustomValue::Composite(values) => {
            if values.len() > MAX_COMPOSITE_ELEMENTS {
                return Err(format!(
                    "composite key has {} elements, max is {MAX_COMPOSITE_ELEMENTS}",
                    values.len()
                ));
            }

            let mut encoded_len = 2usize;
            for v in values {
                let value_len = validate_custom_value(v, depth + 1)?;
                encoded_len += 1 + 4 + value_len;
            }

            Ok(encoded_len)
        }
        CustomValue::Bytes32(_) => Ok(32),
        CustomValue::U32(_) => Ok(4),
        CustomValue::U64(_) => Ok(8),
        CustomValue::U128(_) => Ok(16),
        CustomValue::String(value) => {
            let value_len = value.len();
            if value_len > MAX_CUSTOM_STRING_VALUE_BYTES {
                return Err(format!(
                    "custom string key value exceeds {MAX_CUSTOM_STRING_VALUE_BYTES} bytes: {value_len}"
                ));
            }
            Ok(value_len)
        }
        CustomValue::Bool(_) => Ok(1),
    }
}

pub(crate) fn validate_subscription_request(
    status_subscribed: bool,
    event_subscriptions: &HashSet<Key>,
    method: &str,
    max_subscriptions_per_connection: usize,
) -> Result<(), IndexError> {
    let next_count = match method {
        "acuity_subscribeStatus" if !status_subscribed => event_subscriptions.len() + 1,
        "acuity_subscribeEvents" => {
            // We can't check the key here without params, so we count optimistically
            event_subscriptions.len() + 1 + usize::from(status_subscribed)
        }
        _ => return Ok(()),
    };

    if next_count > max_subscriptions_per_connection {
        return Err(disconnect_error(format!(
            "subscription limit exceeded: max {max_subscriptions_per_connection} per connection"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::IndexError;
    use crate::ws_api::tests_support::DEFAULT_WS_CONFIG;

    #[test]
    fn validate_subscription_request_enforces_connection_limit() {
        let event_subscriptions = (0..DEFAULT_WS_CONFIG.max_subscriptions_per_connection)
            .map(|i| {
                Key::Custom(CustomKey {
                    name: format!("key_{i}"),
                    value: CustomValue::U32(i as u32),
                })
            })
            .collect();

        let result = validate_subscription_request(
            false,
            &event_subscriptions,
            "acuity_subscribeStatus",
            DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
        );

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn validate_subscription_request_allows_duplicate_subscriptions() {
        let key = Key::Custom(CustomKey {
            name: "pool_id".into(),
            value: CustomValue::U32(7),
        });
        let event_subscriptions = HashSet::from([key.clone()]);

        assert!(
            validate_subscription_request(
                false,
                &event_subscriptions,
                "acuity_subscribeEvents",
                DEFAULT_WS_CONFIG.max_subscriptions_per_connection,
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_subscription_request_counts_status_and_event_subscriptions_together() {
        let result = validate_subscription_request(
            true,
            &HashSet::new(),
            "acuity_subscribeEvents",
            1,
        );

        assert!(
            matches!(result, Err(IndexError::Io(err)) if err.kind() == std::io::ErrorKind::ConnectionAborted)
        );
    }

    #[test]
    fn clamp_events_limit_handles_zero_max_without_panicking() {
        assert_eq!(clamp_events_limit(0, 0), 1);
        assert_eq!(clamp_events_limit(500, 0), 1);
    }
}
