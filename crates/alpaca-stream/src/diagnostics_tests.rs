use super::{
    DetailedDecodeFailure, ProtocolViolationReason, TimestampDecodeDiagnostic,
    TimestampDiagnosticProblem, TimestampMessageKind, TimestampWireKind, format_lane_failure,
    format_protocol_violation,
};
use crate::lane::LaneFailure;
use crate::{DecodeError, OptionFeed, SessionGeneration, SessionPhase};

#[test]
fn lane_failure_diagnostics_use_fixed_reason_codes() {
    let cases = [
        (
            LaneFailure::QuoteCapacityExceeded,
            "quote_capacity_exceeded",
        ),
        (LaneFailure::QuoteReceiverClosed, "quote_receiver_closed"),
        (LaneFailure::TradeOverloaded, "trade_overloaded"),
        (LaneFailure::TradeReceiverClosed, "trade_receiver_closed"),
        (LaneFailure::ControlOverloaded, "control_overloaded"),
        (
            LaneFailure::ControlReceiverClosed,
            "control_receiver_closed",
        ),
    ];

    for (failure, reason) in cases {
        assert_eq!(
            format_lane_failure(failure),
            format!("ALPACA_OPTIONS_STREAM_LANE_FAILURE reason={reason}")
        );
    }
}

#[test]
fn decode_errors_have_stable_safe_reason_codes() {
    let cases = [
        (DecodeError::FrameSizeExceeded, "decode_frame_size_exceeded"),
        (
            DecodeError::InvalidMessagePack,
            "decode_invalid_messagepack",
        ),
        (DecodeError::DecodeLimitExceeded, "decode_limit_exceeded"),
        (DecodeError::InvalidFrameShape, "decode_invalid_frame_shape"),
        (
            DecodeError::InvalidMessageSchema,
            "decode_invalid_message_schema",
        ),
        (DecodeError::DuplicateField, "decode_duplicate_field"),
    ];

    for (error, expected_reason) in cases {
        assert_eq!(
            format_protocol_violation(
                OptionFeed::Opra,
                SessionGeneration::new(1),
                SessionPhase::AwaitingFreshData,
                ProtocolViolationReason::Decode(error.into()),
            ),
            format!(
                "ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION feed=opra generation=1 phase=awaiting_fresh_data reason={expected_reason}"
            )
        );
    }
}

#[test]
fn invalid_timestamp_extension_diagnostic_uses_only_fixed_safe_values() {
    let line = format_protocol_violation(
        OptionFeed::Opra,
        SessionGeneration::new(1),
        SessionPhase::AwaitingFreshData,
        ProtocolViolationReason::Decode(DetailedDecodeFailure {
            error: DecodeError::InvalidMessageSchema,
            timestamp: Some(TimestampDecodeDiagnostic {
                message: TimestampMessageKind::Quote,
                wire_kind: TimestampWireKind::TimestampExtension,
                problem: TimestampDiagnosticProblem::InvalidTimestampExtension,
            }),
        }),
    );
    assert_eq!(
        line,
        "ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION feed=opra generation=1 phase=awaiting_fresh_data reason=decode_invalid_message_schema message=quote field=t wire_kind=timestamp_ext problem=invalid_timestamp_extension"
    );
    assert!(!line.contains("AAPL"));
    assert!(!line.contains("synthetic-private-value"));
}

#[test]
fn protocol_reasons_are_fixed_and_do_not_include_payload_values() {
    let cases = [
        (ProtocolViolationReason::TextFrame, "text_frame"),
        (
            ProtocolViolationReason::RawFrameOversized,
            "raw_frame_oversized",
        ),
        (
            ProtocolViolationReason::UnexpectedSuccess,
            "unexpected_success_message",
        ),
        (
            ProtocolViolationReason::UnexpectedSubscription,
            "unexpected_subscription_ack",
        ),
        (
            ProtocolViolationReason::QuoteSymbolNotSubscribed,
            "quote_symbol_not_subscribed",
        ),
        (
            ProtocolViolationReason::TradeSymbolNotSubscribed,
            "trade_symbol_not_subscribed",
        ),
        (
            ProtocolViolationReason::UnexpectedMarketMessage,
            "unexpected_market_message",
        ),
    ];

    for (reason, expected_reason) in cases {
        let line = format_protocol_violation(
            OptionFeed::Indicative,
            SessionGeneration::new(9),
            SessionPhase::Ready,
            reason,
        );
        assert_eq!(
            line,
            format!(
                "ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION feed=indicative generation=9 phase=ready reason={expected_reason}"
            )
        );
        assert!(!line.contains("synthetic-secret"));
        assert!(!line.contains("AAPL260123C00150000"));
    }
}
