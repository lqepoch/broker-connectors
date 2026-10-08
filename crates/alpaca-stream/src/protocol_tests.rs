use super::*;
use crate::diagnostics::TimestampDecodeDiagnostic;
use std::time::{Duration, UNIX_EPOCH};

fn message(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Map(
        fields
            .into_iter()
            .map(|(key, value)| (Value::from(key), value))
            .collect(),
    )
}

fn encode_frame(values: Vec<Value>) -> Vec<u8> {
    let mut encoded = Vec::new();
    rmpv::encode::write_value(&mut encoded, &Value::Array(values)).expect("encode fixture");
    encoded
}

fn quote(timestamp: &str) -> Value {
    message([
        ("T", Value::from("q")),
        ("S", Value::from("AAPL260123C00150000")),
        ("t", Value::from(timestamp)),
        ("bx", Value::from("X")),
        ("bp", Value::F64(1.25)),
        ("bs", Value::from(2u64)),
        ("ax", Value::from("Y")),
        ("ap", Value::from(1.5)),
        ("as", Value::from(3u64)),
        ("c", Value::Array(vec![Value::from("A")])),
    ])
}

fn with_timestamp(message: &mut Value, timestamp: Value) {
    let Value::Map(fields) = message else {
        panic!("synthetic option message is a map");
    };
    let field = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("t"))
        .map(|(_, value)| value)
        .expect("synthetic option message has a timestamp");
    *field = timestamp;
}

fn timestamp_64_payload(seconds: u64, nanosecond: u32) -> Vec<u8> {
    ((u64::from(nanosecond) << 34) | seconds)
        .to_be_bytes()
        .to_vec()
}

fn timestamp_96_payload(seconds: i64, nanosecond: u32) -> Vec<u8> {
    let mut payload = [0; 12];
    payload[..4].copy_from_slice(&nanosecond.to_be_bytes());
    payload[4..].copy_from_slice(&seconds.to_be_bytes());
    payload.to_vec()
}

fn trade(timestamp: &str) -> Value {
    message([
        ("T", Value::from("t")),
        ("S", Value::from("AAPL260123C00150000")),
        ("t", Value::from(timestamp)),
        ("p", Value::from(1.25)),
        ("s", Value::from(1u64)),
        ("x", Value::from("X")),
        ("c", Value::Nil),
    ])
}

#[test]
fn decodes_control_quote_trade_and_multiple_messages() {
    let frame = encode_frame(vec![
        message([
            ("T", Value::from("success")),
            ("msg", Value::from("authenticated")),
        ]),
        message([
            ("T", Value::from("subscription")),
            (
                "quotes",
                Value::Array(vec![Value::from("AAPL260123C00150000")]),
            ),
            (
                "trades",
                Value::Array(vec![Value::from("AAPL260123C00150000")]),
            ),
        ]),
        quote("2026-09-30T12:34:56.123456789Z"),
        trade("2026-09-30T12:34:56.987654321Z"),
    ]);

    let decoded = decode_frame(&frame).expect("decode protocol golden");
    let frame_hash = sha256_hex(&frame);
    assert_eq!(decoded.len(), 4);
    assert_eq!(
        decoded[0],
        ProviderMessage::Success(SuccessMessage::Authenticated)
    );
    let ProviderMessage::Subscription(acknowledgement) = &decoded[1] else {
        panic!("expected complete subscription acknowledgement");
    };
    assert_eq!(acknowledgement.quotes.len(), 1);
    assert_eq!(acknowledgement.trades.len(), 1);
    let ProviderMessage::Quote(quote) = &decoded[2] else {
        panic!("expected quote");
    };
    assert_eq!(quote.timestamp.nanosecond(), 123_456_789);
    assert_eq!(quote.bid_price, MarketNumber::Float64(1.25));
    assert_eq!(quote.conditions, ["A"]);
    assert_eq!(quote.raw_frame_sha256, frame_hash);
    let ProviderMessage::Trade(trade) = &decoded[3] else {
        panic!("expected trade");
    };
    assert_eq!(trade.timestamp.nanosecond(), 987_654_321);
    assert_eq!(trade.size, 1);
    assert_eq!(trade.raw_frame_sha256, frame_hash);
}

#[test]
fn frame_sha256_uses_lowercase_hex_over_exact_wire_bytes() {
    assert_eq!(
        sha256_hex(b"hello"),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

#[test]
fn decodes_standard_timestamp_extension_into_numeric_time() {
    let seconds = 1_700_000_000_u64;
    let nanosecond = 123_456_789_u32;
    let payload = timestamp_64_payload(seconds, nanosecond);
    let mut quote_message = quote("2026-09-30T12:34:56.123456789Z");
    with_timestamp(&mut quote_message, Value::Ext(-1, payload));
    let frame = encode_frame(vec![quote_message]);

    let detailed =
        decode_frame_with_diagnostics(&frame).expect("valid timestamp extension should decode");
    let public = decode_frame(&frame).expect("public decoder accepts valid timestamp extension");
    assert_eq!(public, detailed);
    let [ProviderMessage::Quote(quote)] = public.as_slice() else {
        panic!("decoded message must be an option quote");
    };
    assert_eq!(
        quote.timestamp.unix_seconds(),
        i64::try_from(seconds).expect("synthetic seconds fit i64")
    );
    assert_eq!(quote.timestamp.nanosecond(), nanosecond);
    assert!(quote.timestamp.as_rfc3339().is_empty());
}

#[test]
fn quote_and_trade_timestamp_widths_preserve_nanoseconds_and_freshness() {
    let seconds = 1_700_000_000_u64;
    let expected_seconds = i64::try_from(seconds).expect("synthetic seconds fit i64");
    let cases = [
        (
            u32::try_from(seconds)
                .expect("synthetic seconds fit timestamp32")
                .to_be_bytes()
                .to_vec(),
            expected_seconds,
            0_u32,
        ),
        (
            timestamp_64_payload(seconds, 123_456_789),
            expected_seconds,
            123_456_789_u32,
        ),
        (
            timestamp_96_payload(expected_seconds, 987_654_321),
            expected_seconds,
            987_654_321_u32,
        ),
    ];

    for (payload, expected_seconds, expected_nanosecond) in cases {
        for mut message in [
            quote("2026-09-30T12:34:56.123456789Z"),
            trade("2026-09-30T12:34:56.987654321Z"),
        ] {
            with_timestamp(&mut message, Value::Ext(-1, payload.clone()));
            let decoded = decode_frame(&encode_frame(vec![message]))
                .expect("standard timestamp width should decode");
            let timestamp = match decoded.as_slice() {
                [ProviderMessage::Quote(quote)] => &quote.timestamp,
                [ProviderMessage::Trade(trade)] => &trade.timestamp,
                _ => panic!("message must decode to one quote or trade"),
            };
            assert_eq!(timestamp.unix_seconds(), expected_seconds);
            assert_eq!(timestamp.nanosecond(), expected_nanosecond);
            assert!(timestamp.as_rfc3339().is_empty());

            let source_seconds =
                u64::try_from(expected_seconds).expect("synthetic source seconds are nonnegative");
            let source_time = UNIX_EPOCH
                .checked_add(Duration::new(source_seconds, expected_nanosecond))
                .expect("synthetic source time is representable");
            let age_limit = Duration::from_millis(500);
            let at_age_limit = source_time
                .checked_add(age_limit)
                .expect("synthetic age boundary is representable");
            assert_eq!(
                crate::model::classify_freshness(
                    timestamp,
                    at_age_limit,
                    age_limit,
                    Duration::ZERO,
                ),
                crate::model::DataFreshness::Fresh
            );
            assert_eq!(
                crate::model::classify_freshness(
                    timestamp,
                    at_age_limit + Duration::from_micros(1),
                    age_limit,
                    Duration::ZERO,
                ),
                crate::model::DataFreshness::Stale
            );
        }
    }
}

#[test]
fn malformed_timestamp_extension_has_a_fixed_schema_diagnostic() {
    let invalid_nanoseconds = timestamp_64_payload(0, 1_000_000_000);
    let mut quote_message = quote("2026-09-30T12:34:56.123456789Z");
    with_timestamp(&mut quote_message, Value::Ext(-1, invalid_nanoseconds));
    let frame = encode_frame(vec![quote_message]);

    let failure = decode_frame_with_diagnostics(&frame)
        .expect_err("invalid timestamp nanoseconds must fail closed");
    assert_eq!(failure.error, DecodeError::InvalidMessageSchema);
    assert_eq!(
        failure.timestamp,
        Some(TimestampDecodeDiagnostic {
            message: TimestampMessageKind::Quote,
            wire_kind: TimestampWireKind::TimestampExtension,
            problem: TimestampDiagnosticProblem::InvalidTimestampExtension,
        })
    );
    assert_eq!(
        decode_frame(&frame),
        Err(DecodeError::InvalidMessageSchema),
        "the public schema error remains stable"
    );
}

#[test]
fn oversized_timestamp_string_preserves_the_required_text_bound() {
    let timestamp = "x".repeat(MAX_LABEL_BYTES + 1);
    let frame = encode_frame(vec![quote(&timestamp)]);
    let detailed =
        decode_frame_with_diagnostics(&frame).expect_err("overlong timestamp text is rejected");

    assert_eq!(detailed.error, DecodeError::InvalidMessageSchema);
    assert_eq!(
        detailed.timestamp,
        Some(TimestampDecodeDiagnostic {
            message: TimestampMessageKind::Quote,
            wire_kind: TimestampWireKind::String,
            problem: TimestampDiagnosticProblem::InvalidTimestampString,
        })
    );
    assert_eq!(
        decode_frame(&frame),
        Err(DecodeError::InvalidMessageSchema),
        "the existing public schema error remains unchanged"
    );
}

#[test]
fn timestamp_diagnostic_classifies_only_fixed_messagepack_value_kinds() {
    let cases = [
        (Value::Nil, TimestampWireKind::Nil),
        (Value::Boolean(true), TimestampWireKind::Boolean),
        (Value::from(1i64), TimestampWireKind::Integer),
        (Value::F64(1.0), TimestampWireKind::Float),
        (Value::from("synthetic"), TimestampWireKind::String),
        (Value::Binary(vec![1]), TimestampWireKind::Binary),
        (Value::Array(Vec::new()), TimestampWireKind::Array),
        (Value::Map(Vec::new()), TimestampWireKind::Map),
        (
            Value::Ext(-1, Vec::new()),
            TimestampWireKind::TimestampExtension,
        ),
        (
            Value::Ext(-2, Vec::new()),
            TimestampWireKind::OtherExtension,
        ),
    ];
    for (value, expected) in cases {
        assert_eq!(messagepack_value_kind(&value), expected);
    }
}

#[test]
fn missing_timestamp_is_distinguished_without_exposing_frame_content() {
    let mut quote_message = quote("2026-09-30T12:34:56.123456789Z");
    let Value::Map(fields) = &mut quote_message else {
        panic!("synthetic quote is a map");
    };
    fields.retain(|(key, _)| key.as_str() != Some("t"));
    let failure = decode_frame_with_diagnostics(&encode_frame(vec![quote_message]))
        .expect_err("missing timestamp is rejected");
    assert_eq!(failure.error, DecodeError::InvalidMessageSchema);
    assert_eq!(
        failure.timestamp,
        Some(TimestampDecodeDiagnostic {
            message: TimestampMessageKind::Quote,
            wire_kind: TimestampWireKind::Missing,
            problem: TimestampDiagnosticProblem::MissingRequiredField,
        })
    );
}

#[test]
fn authentication_request_is_binary_messagepack_and_contains_expected_fields() {
    let credentials = AlpacaCredentials::new("synthetic-key", "synthetic-secret")
        .expect("synthetic credentials are valid");
    let encoded = encode_auth(&credentials).expect("encode auth frame");
    let decoded =
        rmpv::decode::read_value(&mut Cursor::new(encoded.as_slice())).expect("decode auth frame");
    let Value::Map(fields) = decoded else {
        panic!("auth must be a MessagePack map");
    };
    assert_eq!(fields.len(), 3);
    assert!(
        fields.iter().any(|(key, value)| {
            key.as_str() == Some("action") && value.as_str() == Some("auth")
        })
    );
    assert!(fields.iter().any(|(key, _)| key.as_str() == Some("key")));
    assert!(fields.iter().any(|(key, _)| key.as_str() == Some("secret")));
}

#[test]
fn unknown_message_drops_its_body() {
    let frame = encode_frame(vec![message([
        ("T", Value::from("future")),
        ("msg", Value::from("synthetic private payload")),
    ])]);
    assert_eq!(
        decode_frame(&frame).expect("unknown messages are forward compatible"),
        [ProviderMessage::Unknown("future".to_owned())]
    );
}

#[test]
fn rejects_empty_oversized_truncated_and_invalid_marker_frames() {
    assert_eq!(decode_frame(&[]), Err(DecodeError::FrameSizeExceeded));
    assert_eq!(
        decode_frame(&vec![0; MAX_FRAME_BYTES + 1]),
        Err(DecodeError::FrameSizeExceeded)
    );
    assert_eq!(
        decode_frame(&[0x91, 0xdb, 0x00, 0x00, 0x00, 0x04, b'x']),
        Err(DecodeError::InvalidMessagePack)
    );
    assert_eq!(
        decode_frame(&[0x91, 0xc1]),
        Err(DecodeError::InvalidMessagePack)
    );
}

#[test]
fn preflight_bounds_array_map_string_and_nesting_cost() {
    let many = vec![message([("T", Value::from("z"))]); MAX_ARRAY_ITEMS + 1];
    assert_eq!(
        decode_frame(&encode_frame(many)),
        Err(DecodeError::DecodeLimitExceeded)
    );

    let wide_map = Value::Map(
        (0..=MAX_MAP_ENTRIES)
            .map(|index| {
                (
                    Value::from(i64::try_from(index).expect("synthetic map index fits i64")),
                    Value::Nil,
                )
            })
            .collect(),
    );
    assert_eq!(
        decode_frame(&encode_frame(vec![wide_map])),
        Err(DecodeError::DecodeLimitExceeded)
    );

    let long = "x".repeat(MAX_STRING_BYTES + 1);
    assert_eq!(
        decode_frame(&encode_frame(vec![message([("T", Value::from(long))])])),
        Err(DecodeError::DecodeLimitExceeded)
    );

    let mut nested = Value::Nil;
    for _ in 0..=MAX_DECODE_DEPTH {
        nested = Value::Array(vec![nested]);
    }
    assert_eq!(
        decode_frame(&encode_frame(vec![nested])),
        Err(DecodeError::DecodeLimitExceeded)
    );
}

#[test]
fn rejects_duplicate_known_fields_and_trailing_bytes() {
    let duplicate = Value::Map(vec![
        (Value::from("T"), Value::from("success")),
        (Value::from("msg"), Value::from("connected")),
        (Value::from("msg"), Value::from("authenticated")),
    ]);
    assert_eq!(
        decode_frame(&encode_frame(vec![duplicate])),
        Err(DecodeError::DuplicateField)
    );

    let mut trailing = encode_frame(vec![message([("T", Value::from("x"))])]);
    trailing.push(0xc0);
    assert_eq!(decode_frame(&trailing), Err(DecodeError::InvalidFrameShape));
}

#[test]
fn arbitrary_fragment_boundaries_never_panic_or_accept_partial_messages() {
    let payload = encode_frame(vec![quote("2026-09-30T12:34:56.123456789Z")]);
    for split in 1..payload.len() {
        assert!(decode_frame(&payload[..split]).is_err());
        assert!(decode_frame(&payload[split..]).is_err());
    }
    assert!(decode_frame(&payload).is_ok());
}

#[test]
fn bounded_pseudofuzz_inputs_never_panic_or_return_unbounded_messages() {
    let mut seed = 0x2360_5eed_u64;
    for case_index in 0..2_000usize {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let length = usize::try_from(seed % 4_097).expect("bounded fuzz length fits usize");
        let mut bytes = Vec::with_capacity(length);
        let mut byte_seed = seed;
        for _ in 0..length {
            byte_seed = byte_seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            bytes.push(
                u8::try_from((byte_seed >> 32) & 0xff).expect("masked pseudorandom byte fits u8"),
            );
        }
        let decoded = std::panic::catch_unwind(|| decode_frame(&bytes));
        assert!(
            decoded.is_ok(),
            "parser panicked for synthetic case {case_index}"
        );
        if let Ok(Ok(messages)) = decoded {
            assert!(!messages.is_empty());
            assert!(messages.len() <= MAX_FRAME_MESSAGES);
        }
    }
}
