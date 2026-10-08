//! Fixed low-cardinality protocol rejection diagnostics.
//!
//! 固定且低基数的协议拒绝诊断。

use crate::lane::LaneFailure;
use crate::protocol::DecodeError;
use crate::{OptionFeed, SessionGeneration, SessionPhase};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimestampMessageKind {
    Quote,
    Trade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimestampWireKind {
    Missing,
    Nil,
    Boolean,
    Integer,
    Float,
    String,
    Binary,
    Array,
    Map,
    TimestampExtension,
    OtherExtension,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimestampDiagnosticProblem {
    MissingRequiredField,
    WrongWireType,
    InvalidTimestampString,
    InvalidTimestampExtension,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimestampDecodeDiagnostic {
    pub(crate) message: TimestampMessageKind,
    pub(crate) wire_kind: TimestampWireKind,
    pub(crate) problem: TimestampDiagnosticProblem,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetailedDecodeFailure {
    pub(crate) error: DecodeError,
    pub(crate) timestamp: Option<TimestampDecodeDiagnostic>,
}

impl DetailedDecodeFailure {
    pub(crate) fn into_public_error(self) -> DecodeError {
        self.error
    }

    pub(crate) fn timestamp(
        message: TimestampMessageKind,
        wire_kind: TimestampWireKind,
        problem: TimestampDiagnosticProblem,
    ) -> Self {
        Self {
            error: DecodeError::InvalidMessageSchema,
            timestamp: Some(TimestampDecodeDiagnostic {
                message,
                wire_kind,
                problem,
            }),
        }
    }
}

impl From<DecodeError> for DetailedDecodeFailure {
    fn from(error: DecodeError) -> Self {
        Self {
            error,
            timestamp: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProtocolViolationReason {
    TextFrame,
    Decode(DetailedDecodeFailure),
    RawFrameOversized,
    UnexpectedSuccess,
    UnexpectedSubscription,
    QuoteSymbolNotSubscribed,
    TradeSymbolNotSubscribed,
    UnexpectedMarketMessage,
}

pub(crate) fn log_protocol_violation(
    feed: OptionFeed,
    generation: SessionGeneration,
    phase: SessionPhase,
    reason: ProtocolViolationReason,
) {
    eprintln!(
        "{}",
        format_protocol_violation(feed, generation, phase, reason)
    );
}

pub(crate) fn log_lane_failure(failure: LaneFailure) {
    eprintln!("{}", format_lane_failure(failure));
}

fn format_lane_failure(failure: LaneFailure) -> String {
    format!(
        "ALPACA_OPTIONS_STREAM_LANE_FAILURE reason={}",
        lane_failure_reason(failure)
    )
}

fn lane_failure_reason(failure: LaneFailure) -> &'static str {
    match failure {
        LaneFailure::QuoteCapacityExceeded => "quote_capacity_exceeded",
        LaneFailure::QuoteReceiverClosed => "quote_receiver_closed",
        LaneFailure::TradeOverloaded => "trade_overloaded",
        LaneFailure::TradeReceiverClosed => "trade_receiver_closed",
        LaneFailure::RawFrameOverloaded => "raw_frame_budget_exceeded",
        LaneFailure::ControlOverloaded => "control_overloaded",
        LaneFailure::ControlReceiverClosed => "control_receiver_closed",
    }
}

fn format_protocol_violation(
    feed: OptionFeed,
    generation: SessionGeneration,
    phase: SessionPhase,
    reason: ProtocolViolationReason,
) -> String {
    let schema_detail = match reason {
        ProtocolViolationReason::Decode(failure) => failure
            .timestamp
            .map(timestamp_diagnostic_suffix)
            .unwrap_or_default(),
        _ => String::new(),
    };
    format!(
        "ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION feed={} generation={} phase={} reason={}{}",
        feed_name(feed),
        generation.get(),
        phase_name(phase),
        reason_name(reason),
        schema_detail,
    )
}

fn reason_name(reason: ProtocolViolationReason) -> &'static str {
    match reason {
        ProtocolViolationReason::TextFrame => "text_frame",
        ProtocolViolationReason::Decode(failure) => decode_error_name(failure.error),
        ProtocolViolationReason::RawFrameOversized => "raw_frame_oversized",
        ProtocolViolationReason::UnexpectedSuccess => "unexpected_success_message",
        ProtocolViolationReason::UnexpectedSubscription => "unexpected_subscription_ack",
        ProtocolViolationReason::QuoteSymbolNotSubscribed => "quote_symbol_not_subscribed",
        ProtocolViolationReason::TradeSymbolNotSubscribed => "trade_symbol_not_subscribed",
        ProtocolViolationReason::UnexpectedMarketMessage => "unexpected_market_message",
    }
}

fn timestamp_diagnostic_suffix(diagnostic: TimestampDecodeDiagnostic) -> String {
    format!(
        " message={} field=t wire_kind={} problem={}",
        timestamp_message_name(diagnostic.message),
        timestamp_wire_kind_name(diagnostic.wire_kind),
        timestamp_problem_name(diagnostic.problem)
    )
}

fn timestamp_message_name(message: TimestampMessageKind) -> &'static str {
    match message {
        TimestampMessageKind::Quote => "quote",
        TimestampMessageKind::Trade => "trade",
    }
}

fn timestamp_wire_kind_name(kind: TimestampWireKind) -> &'static str {
    match kind {
        TimestampWireKind::Missing => "missing",
        TimestampWireKind::Nil => "nil",
        TimestampWireKind::Boolean => "bool",
        TimestampWireKind::Integer => "int",
        TimestampWireKind::Float => "float",
        TimestampWireKind::String => "string",
        TimestampWireKind::Binary => "binary",
        TimestampWireKind::Array => "array",
        TimestampWireKind::Map => "map",
        TimestampWireKind::TimestampExtension => "timestamp_ext",
        TimestampWireKind::OtherExtension => "extension",
    }
}

fn timestamp_problem_name(problem: TimestampDiagnosticProblem) -> &'static str {
    match problem {
        TimestampDiagnosticProblem::MissingRequiredField => "missing_required_field",
        TimestampDiagnosticProblem::WrongWireType => "wrong_wire_type",
        TimestampDiagnosticProblem::InvalidTimestampString => "invalid_timestamp_string",
        TimestampDiagnosticProblem::InvalidTimestampExtension => "invalid_timestamp_extension",
    }
}

fn decode_error_name(error: DecodeError) -> &'static str {
    match error {
        DecodeError::FrameSizeExceeded => "decode_frame_size_exceeded",
        DecodeError::InvalidMessagePack => "decode_invalid_messagepack",
        DecodeError::DecodeLimitExceeded => "decode_limit_exceeded",
        DecodeError::InvalidFrameShape => "decode_invalid_frame_shape",
        DecodeError::InvalidMessageSchema => "decode_invalid_message_schema",
        DecodeError::DuplicateField => "decode_duplicate_field",
    }
}

fn phase_name(phase: SessionPhase) -> &'static str {
    match phase {
        SessionPhase::Connecting => "connecting",
        SessionPhase::AwaitingConnected => "awaiting_connected",
        SessionPhase::Authenticating => "authenticating",
        SessionPhase::AwaitingAuthentication => "awaiting_authentication",
        SessionPhase::Subscribing => "subscribing",
        SessionPhase::AwaitingSubscriptionAcknowledgement => {
            "awaiting_subscription_acknowledgement"
        }
        SessionPhase::AwaitingFreshData => "awaiting_fresh_data",
        SessionPhase::Ready => "ready",
        SessionPhase::SessionLost => "session_lost",
        SessionPhase::Closed => "closed",
    }
}

fn feed_name(feed: OptionFeed) -> &'static str {
    match feed {
        OptionFeed::Opra => "opra",
        OptionFeed::Indicative => "indicative",
        OptionFeed::Delayed => "delayed",
    }
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
