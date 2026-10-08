//! Bounded `MessagePack` preflight, provider control parsing, and option data decoding.
//!
//! 有界 `MessagePack` 预检、provider 控制消息解析与期权行情解码。

use std::collections::BTreeSet;
use std::io::Cursor;

use rmpv::Value;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::config::{OptionContractSymbol, StreamConfigError};
use crate::credentials::AlpacaCredentials;
use crate::diagnostics::{
    DetailedDecodeFailure, TimestampDiagnosticProblem, TimestampMessageKind, TimestampWireKind,
};
use crate::model::{
    MarketNumber, OptionQuote, OptionTrade, ProviderTimestamp, SubscriptionAcknowledgement,
};
use crate::msgpack_timestamp::decode_timestamp_extension;

/// Maximum accepted complete WebSocket application message size.
/// 接受的完整 WebSocket 应用消息最大字节数。
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum top-level `MessagePack` messages accepted in one provider array.
/// 单个 provider 数组接受的顶层 `MessagePack` 消息最大数量。
pub const MAX_FRAME_MESSAGES: usize = 512;
/// Maximum total `MessagePack` values traversed before decode.
/// 解码前允许遍历的 `MessagePack` 值总数上限。
pub const MAX_DECODE_NODES: usize = 16_384;
/// Maximum nesting depth allowed by both preflight and the `MessagePack` decoder.
/// 预检与 `MessagePack` decoder 同时实施的最大嵌套深度。
pub const MAX_DECODE_DEPTH: usize = 16;
/// Maximum number of entries in one decoded `MessagePack` map.
/// 单个 `MessagePack` map 允许的最大条目数。
pub const MAX_MAP_ENTRIES: usize = 64;
/// Maximum number of items in one decoded `MessagePack` array.
/// 单个 `MessagePack` array 允许的最大元素数。
pub const MAX_ARRAY_ITEMS: usize = 512;
/// Maximum UTF-8 string or opaque binary extension payload size accepted by preflight.
/// 预检接受的 UTF-8 字符串或 opaque binary/extension 最大字节数。
pub const MAX_STRING_BYTES: usize = 4_096;
/// Maximum number of option conditions retained from one message.
/// 单条消息最多保留的期权交易条件数量。
pub const MAX_CONDITIONS: usize = 32;
/// Maximum UTF-8 bytes retained for an exchange or condition value.
/// 交易所代码或条件值保留的最大 UTF-8 字节数。
pub const MAX_LABEL_BYTES: usize = 64;

/// Safe, stable `MessagePack` or schema failure category.
/// `MessagePack` 或数据结构校验失败的固定安全类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    /// The complete WebSocket message is empty or exceeds the local frame limit.
    /// 完整 WebSocket 消息为空或超过本地 frame 上限。
    FrameSizeExceeded,
    /// The frame contains an invalid or truncated `MessagePack` value.
    /// frame 中包含无效或被截断的 `MessagePack` 值。
    InvalidMessagePack,
    /// A container, string, binary value, depth, or total node count exceeds its limit.
    /// 容器、字符串、binary 值、嵌套深度或节点总数超过上限。
    DecodeLimitExceeded,
    /// The frame does not contain exactly one top-level array with messages.
    /// frame 不包含且仅包含一个消息顶层数组。
    InvalidFrameShape,
    /// A recognized provider message is missing a required field or has a wrong field type.
    /// 已识别的 provider 消息缺少必需字段或字段类型错误。
    InvalidMessageSchema,
    /// A recognized field occurs more than once in one provider map.
    /// provider map 中同一个已识别字段重复出现。
    DuplicateField,
}

/// Typed provider success acknowledgement with no raw provider text.
/// 不保留 provider 原始文本的类型化成功回执。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessMessage {
    /// Initial WebSocket connection acknowledgement.
    /// WebSocket 初始连接回执。
    Connected,
    /// Successful authentication acknowledgement.
    /// 认证成功回执。
    Authenticated,
    /// A success control outside the connected/authenticated acknowledgements.
    /// `msg` is discarded because provider text is not part of stable diagnostics.
    /// connected/authenticated 回执之外的成功控制消息；丢弃 `msg`，避免 provider 文本进入稳定诊断。
    Other,
}

/// Sanitized provider error code and protocol classification.
/// 已脱敏的 provider 错误码与协议分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderError {
    /// Provider numeric error code; provider message text is discarded.
    /// Provider 数字错误码；provider 错误文本会被丢弃。
    pub code: i64,
    /// Stable local classification of the provider code.
    /// Provider 错误码对应的本地固定分类。
    pub kind: ProviderErrorKind,
}

/// Provider error classification used to decide whether a session may retry.
/// 用于决定会话是否可以重试的 provider 错误分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderErrorKind {
    /// Authentication was not accepted or its deadline expired.
    /// 认证未被接受或已超时。
    Authentication,
    /// The account's provider-side connection limit was reached.
    /// 已达到账户在 provider 侧的连接数量限制。
    ConnectionLimit,
    /// Subscription content, feed entitlement, or channel was rejected.
    /// 订阅内容、feed entitlement 或 channel 被拒绝。
    SubscriptionRejected,
    /// The server requires a `MessagePack` content type.
    /// 服务端要求使用 `MessagePack` content type。
    MessagePackRequired,
    /// The server reported a slow client.
    /// 服务端报告客户端消费过慢。
    SlowClient,
    /// The provider returned an error outside the known code set.
    /// Provider 返回了当前未识别的错误码。
    Other,
}

/// One decoded option provider message.
/// 一条已经解码的期权 provider 消息。
#[derive(Clone, Debug, PartialEq)]
pub enum ProviderMessage {
    /// Connection or authentication success control.
    /// 连接或认证成功控制消息。
    Success(SuccessMessage),
    /// Sanitized provider error control.
    /// 已脱敏的 provider 错误控制消息。
    Error(ProviderError),
    /// Full current quote/trade subscription acknowledgement.
    /// 完整的当前报价/成交订阅回执。
    Subscription(SubscriptionAcknowledgement),
    /// Decoded option quote.
    /// 已解码期权报价。
    Quote(OptionQuote),
    /// Decoded option trade.
    /// 已解码期权成交。
    Trade(OptionTrade),
    /// Unknown provider message type; the message body is intentionally discarded.
    /// 未知 provider 消息类型；有意丢弃消息正文。
    Unknown(String),
}

/// Decodes one complete provider `MessagePack` array after a non-allocating bounds preflight.
/// 对一条完整 provider `MessagePack` 数组执行无分配边界预检后解码。
///
/// # Errors
///
/// Returns a fixed error category for oversized, malformed, truncated, or unsupported frames.
pub fn decode_frame(input: &[u8]) -> Result<Vec<ProviderMessage>, DecodeError> {
    decode_frame_with_diagnostics(input).map_err(DetailedDecodeFailure::into_public_error)
}

pub(crate) fn decode_frame_with_diagnostics(
    input: &[u8],
) -> Result<Vec<ProviderMessage>, DetailedDecodeFailure> {
    if input.is_empty() || input.len() > MAX_FRAME_BYTES {
        return Err(DecodeError::FrameSizeExceeded.into());
    }
    preflight(input)?;
    let mut cursor = Cursor::new(input);
    let decoded = rmpv::decode::read_value_with_max_depth(&mut cursor, MAX_DECODE_DEPTH + 1)
        .map_err(|_| DecodeError::InvalidMessagePack)?;
    let expected_position =
        u64::try_from(input.len()).map_err(|_| DecodeError::InvalidFrameShape)?;
    if cursor.position() != expected_position {
        return Err(DecodeError::InvalidFrameShape.into());
    }
    let Value::Array(messages) = decoded else {
        return Err(DecodeError::InvalidFrameShape.into());
    };
    if messages.is_empty() || messages.len() > MAX_FRAME_MESSAGES {
        return Err(DecodeError::InvalidFrameShape.into());
    }
    let mut decoded = messages
        .iter()
        .map(decode_message)
        .collect::<Result<Vec<_>, _>>()?;
    let raw_frame_sha256 = sha256_hex(input);
    for message in &mut decoded {
        match message {
            ProviderMessage::Quote(quote) => {
                quote.raw_frame_sha256.clone_from(&raw_frame_sha256);
            }
            ProviderMessage::Trade(trade) => {
                trade.raw_frame_sha256.clone_from(&raw_frame_sha256);
            }
            _ => {}
        }
    }
    Ok(decoded)
}

fn sha256_hex(input: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(input);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) fn encode_auth(credentials: &AlpacaCredentials) -> Result<Zeroizing<Vec<u8>>, ()> {
    let mut encoded = Zeroizing::new(Vec::with_capacity(2_200));
    encoded.push(0x83);
    write_text(&mut encoded, "action")?;
    write_text(&mut encoded, "auth")?;
    write_text(&mut encoded, "key")?;
    write_text(&mut encoded, credentials.key_id())?;
    write_text(&mut encoded, "secret")?;
    write_text(&mut encoded, credentials.secret())?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(());
    }
    Ok(encoded)
}

pub(crate) fn encode_subscriptions(
    subscriptions: &crate::config::DesiredSubscriptions,
) -> Result<Vec<u8>, ()> {
    let mut fields = vec![(Value::from("action"), Value::from("subscribe"))];
    if !subscriptions.quotes.is_empty() {
        fields.push((
            Value::from("quotes"),
            Value::Array(
                subscriptions
                    .quotes
                    .iter()
                    .map(|symbol| Value::from(symbol.as_str()))
                    .collect(),
            ),
        ));
    }
    if !subscriptions.trades.is_empty() {
        fields.push((
            Value::from("trades"),
            Value::Array(
                subscriptions
                    .trades
                    .iter()
                    .map(|symbol| Value::from(symbol.as_str()))
                    .collect(),
            ),
        ));
    }
    encode_value(&Value::Map(fields))
}

fn encode_value(value: &Value) -> Result<Vec<u8>, ()> {
    let mut encoded = Vec::with_capacity(4 * 1024);
    rmpv::encode::write_value(&mut encoded, value).map_err(|_| ())?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(());
    }
    Ok(encoded)
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), ()> {
    let length = value.len();
    if length <= 31 {
        output.push(0xa0 | u8::try_from(length).map_err(|_| ())?);
    } else if let Ok(encoded_length) = u8::try_from(length) {
        output.push(0xd9);
        output.push(encoded_length);
    } else if let Ok(encoded_length) = u16::try_from(length) {
        output.push(0xda);
        output.extend_from_slice(&encoded_length.to_be_bytes());
    } else {
        return Err(());
    }
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

#[allow(clippy::too_many_lines)] // Keep the single-pass allocation-free MessagePack bound checks together.
fn preflight(input: &[u8]) -> Result<(), DecodeError> {
    let mut offset = 0usize;
    let mut nodes = 0usize;
    let mut frames = vec![(1usize, 0usize)];
    while let Some((remaining, _)) = frames.last_mut() {
        if *remaining == 0 {
            frames.pop();
            continue;
        }
        *remaining -= 1;
        let depth = frames.last().map_or(0, |(_, depth)| *depth);
        nodes = nodes
            .checked_add(1)
            .filter(|count| *count <= MAX_DECODE_NODES)
            .ok_or(DecodeError::DecodeLimitExceeded)?;
        let marker = take(input, &mut offset, 1)?[0];
        let mut child_count = None;
        match marker {
            0x00..=0x7f
            | 0xc0
            | 0xc2
            | 0xc3
            | 0xca
            | 0xcb
            | 0xcc
            | 0xcd
            | 0xce
            | 0xcf
            | 0xd0
            | 0xd1
            | 0xd2
            | 0xd3
            | 0xc1 => {
                let payload_len = match marker {
                    0xca | 0xce | 0xd2 => 4,
                    0xcb | 0xcf | 0xd3 => 8,
                    0xcc | 0xd0 => 1,
                    0xcd | 0xd1 => 2,
                    _ => 0,
                };
                take(input, &mut offset, payload_len)?;
                if marker == 0xc1 {
                    return Err(DecodeError::InvalidMessagePack);
                }
            }
            0x80..=0x8f => child_count = Some(usize::from(marker & 0x0f) * 2),
            0x90..=0x9f => child_count = Some(usize::from(marker & 0x0f)),
            0xa0..=0xbf => skip_bounded(input, &mut offset, usize::from(marker & 0x1f))?,
            0xc4..=0xc6 => {
                let len = read_length(input, &mut offset, marker)?;
                skip_blob(input, &mut offset, len)?;
            }
            0xc7..=0xc9 => {
                let len = read_length(input, &mut offset, marker)?;
                if len > MAX_STRING_BYTES {
                    return Err(DecodeError::DecodeLimitExceeded);
                }
                skip_bounded(input, &mut offset, len.saturating_add(1))?;
            }
            0xd4..=0xd8 => {
                let len = match marker {
                    0xd4 => 1,
                    0xd5 => 2,
                    0xd6 => 4,
                    0xd7 => 8,
                    _ => 16,
                };
                skip_bounded(input, &mut offset, len + 1)?;
            }
            0xd9..=0xdb => {
                let len = read_length(input, &mut offset, marker)?;
                if len > MAX_STRING_BYTES {
                    return Err(DecodeError::DecodeLimitExceeded);
                }
                skip_bounded(input, &mut offset, len)?;
            }
            0xdc | 0xdd => {
                let len = read_length(input, &mut offset, marker)?;
                if len > MAX_ARRAY_ITEMS {
                    return Err(DecodeError::DecodeLimitExceeded);
                }
                child_count = Some(len);
            }
            0xde | 0xdf => {
                let len = read_length(input, &mut offset, marker)?;
                if len > MAX_MAP_ENTRIES {
                    return Err(DecodeError::DecodeLimitExceeded);
                }
                child_count = Some(len.checked_mul(2).ok_or(DecodeError::DecodeLimitExceeded)?);
            }
            _ => return Err(DecodeError::InvalidMessagePack),
        }
        if let Some(child_count) = child_count.filter(|count| *count > 0) {
            if depth >= MAX_DECODE_DEPTH || child_count > MAX_DECODE_NODES.saturating_sub(nodes) {
                return Err(DecodeError::DecodeLimitExceeded);
            }
            frames.push((child_count, depth + 1));
        }
    }
    if offset != input.len() {
        return Err(DecodeError::InvalidFrameShape);
    }
    Ok(())
}

fn take<'a>(input: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], DecodeError> {
    let end = offset
        .checked_add(len)
        .filter(|end| *end <= input.len())
        .ok_or(DecodeError::InvalidMessagePack)?;
    let result = &input[*offset..end];
    *offset = end;
    Ok(result)
}

fn skip_bounded(input: &[u8], offset: &mut usize, len: usize) -> Result<(), DecodeError> {
    take(input, offset, len).map(|_| ())
}

fn skip_blob(input: &[u8], offset: &mut usize, len: usize) -> Result<(), DecodeError> {
    if len > MAX_STRING_BYTES {
        return Err(DecodeError::DecodeLimitExceeded);
    }
    skip_bounded(input, offset, len)
}

fn read_length(input: &[u8], offset: &mut usize, marker: u8) -> Result<usize, DecodeError> {
    let width = match marker {
        0xc4 | 0xc7 | 0xd9 => 1,
        0xc5 | 0xc8 | 0xda | 0xdc | 0xde => 2,
        0xc6 | 0xc9 | 0xdb | 0xdd | 0xdf => 4,
        _ => return Err(DecodeError::InvalidMessagePack),
    };
    let bytes = take(input, offset, width)?;
    let length = match width {
        1 => usize::from(bytes[0]),
        2 => usize::from(u16::from_be_bytes([bytes[0], bytes[1]])),
        4 => usize::try_from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .map_err(|_| DecodeError::DecodeLimitExceeded)?,
        _ => return Err(DecodeError::InvalidMessagePack),
    };
    Ok(length)
}

fn decode_message(value: &Value) -> Result<ProviderMessage, DetailedDecodeFailure> {
    let Value::Map(fields) = value else {
        return Err(DecodeError::InvalidMessageSchema.into());
    };
    if fields.len() > MAX_MAP_ENTRIES {
        return Err(DecodeError::DecodeLimitExceeded.into());
    }
    let tag = required_text(fields, "T", 16)?;
    match tag {
        "success" => match required_text(fields, "msg", MAX_LABEL_BYTES)? {
            "connected" => Ok(ProviderMessage::Success(SuccessMessage::Connected)),
            "authenticated" => Ok(ProviderMessage::Success(SuccessMessage::Authenticated)),
            _ => Ok(ProviderMessage::Success(SuccessMessage::Other)),
        },
        "error" => {
            let code = required_integer(fields, "code")?;
            Ok(ProviderMessage::Error(ProviderError {
                code,
                kind: classify_provider_error(code),
            }))
        }
        "subscription" => Ok(ProviderMessage::Subscription(parse_subscription(fields)?)),
        "q" => Ok(ProviderMessage::Quote(parse_quote(fields)?)),
        "t" => Ok(ProviderMessage::Trade(parse_trade(fields)?)),
        other => Ok(ProviderMessage::Unknown(other.to_owned())),
    }
}

fn parse_quote(fields: &[(Value, Value)]) -> Result<OptionQuote, DetailedDecodeFailure> {
    validate_unique_known(
        fields,
        &["T", "S", "t", "bx", "bp", "bs", "ax", "ap", "as", "c"],
    )?;
    Ok(OptionQuote {
        symbol: symbol(required_text(
            fields,
            "S",
            crate::config::MAX_OPTION_SYMBOL_BYTES,
        )?)?,
        timestamp: required_timestamp_field(fields, TimestampMessageKind::Quote)?,
        bid_exchange: label(required_text(fields, "bx", MAX_LABEL_BYTES)?)?,
        bid_price: nonnegative_number(required_value(fields, "bp")?)?,
        bid_size: nonnegative_u64(required_value(fields, "bs")?)?,
        ask_exchange: label(required_text(fields, "ax", MAX_LABEL_BYTES)?)?,
        ask_price: nonnegative_number(required_value(fields, "ap")?)?,
        ask_size: nonnegative_u64(required_value(fields, "as")?)?,
        conditions: optional_conditions(fields, "c")?,
        raw_frame_sha256: String::new(),
    })
}

fn parse_trade(fields: &[(Value, Value)]) -> Result<OptionTrade, DetailedDecodeFailure> {
    validate_unique_known(fields, &["T", "S", "t", "p", "s", "x", "c"])?;
    Ok(OptionTrade {
        symbol: symbol(required_text(
            fields,
            "S",
            crate::config::MAX_OPTION_SYMBOL_BYTES,
        )?)?,
        timestamp: required_timestamp_field(fields, TimestampMessageKind::Trade)?,
        price: nonnegative_number(required_value(fields, "p")?)?,
        size: nonnegative_u64(required_value(fields, "s")?)?,
        exchange: label(required_text(fields, "x", MAX_LABEL_BYTES)?)?,
        conditions: optional_conditions(fields, "c")?,
        raw_frame_sha256: String::new(),
    })
}

fn parse_subscription(
    fields: &[(Value, Value)],
) -> Result<SubscriptionAcknowledgement, DecodeError> {
    validate_unique_known(fields, &["T", "quotes", "trades"])?;
    Ok(SubscriptionAcknowledgement {
        quotes: optional_symbols(fields, "quotes")?,
        trades: optional_symbols(fields, "trades")?,
    })
}

fn optional_symbols(
    fields: &[(Value, Value)],
    key: &str,
) -> Result<std::collections::BTreeSet<OptionContractSymbol>, DecodeError> {
    let Some(value) = optional_value(fields, key)? else {
        return Ok(BTreeSet::default());
    };
    let Value::Array(items) = value else {
        return Err(DecodeError::InvalidMessageSchema);
    };
    if items.len() > crate::config::MAX_DESIRED_SYMBOLS_PER_CHANNEL {
        return Err(DecodeError::DecodeLimitExceeded);
    }
    let mut result = std::collections::BTreeSet::new();
    for item in items {
        let Value::String(value) = item else {
            return Err(DecodeError::InvalidMessageSchema);
        };
        let value = value.as_str().ok_or(DecodeError::InvalidMessageSchema)?;
        let symbol = symbol(value)?;
        if !result.insert(symbol) {
            return Err(DecodeError::InvalidMessageSchema);
        }
    }
    Ok(result)
}

fn optional_conditions(fields: &[(Value, Value)], key: &str) -> Result<Vec<String>, DecodeError> {
    let Some(value) = optional_value(fields, key)? else {
        return Ok(Vec::new());
    };
    match value {
        Value::Nil => Ok(Vec::new()),
        Value::String(value) => Ok(vec![label(
            value.as_str().ok_or(DecodeError::InvalidMessageSchema)?,
        )?]),
        Value::Array(items) => {
            if items.len() > MAX_CONDITIONS {
                return Err(DecodeError::DecodeLimitExceeded);
            }
            items
                .iter()
                .map(|item| {
                    let Value::String(value) = item else {
                        return Err(DecodeError::InvalidMessageSchema);
                    };
                    label(value.as_str().ok_or(DecodeError::InvalidMessageSchema)?)
                })
                .collect()
        }
        _ => Err(DecodeError::InvalidMessageSchema),
    }
}

fn symbol(value: &str) -> Result<OptionContractSymbol, DecodeError> {
    OptionContractSymbol::new(value.to_owned()).map_err(config_error)
}

fn config_error(_error: StreamConfigError) -> DecodeError {
    DecodeError::InvalidMessageSchema
}

fn required_timestamp_field(
    fields: &[(Value, Value)],
    message: TimestampMessageKind,
) -> Result<ProviderTimestamp, DetailedDecodeFailure> {
    let Some(value) = optional_value(fields, "t")? else {
        return Err(DetailedDecodeFailure::timestamp(
            message,
            TimestampWireKind::Missing,
            TimestampDiagnosticProblem::MissingRequiredField,
        ));
    };
    match value {
        Value::Ext(type_code, payload) if *type_code == -1 => {
            decode_timestamp_extension(*type_code, payload).map_err(|_| {
                DetailedDecodeFailure::timestamp(
                    message,
                    TimestampWireKind::TimestampExtension,
                    TimestampDiagnosticProblem::InvalidTimestampExtension,
                )
            })
        }
        Value::String(value) => {
            let Some(value) = value.as_str() else {
                return Err(DetailedDecodeFailure::timestamp(
                    message,
                    TimestampWireKind::String,
                    TimestampDiagnosticProblem::InvalidTimestampString,
                ));
            };
            if value.is_empty()
                || value.len() > MAX_LABEL_BYTES
                || value.chars().any(char::is_control)
            {
                return Err(DetailedDecodeFailure::timestamp(
                    message,
                    TimestampWireKind::String,
                    TimestampDiagnosticProblem::InvalidTimestampString,
                ));
            }
            ProviderTimestamp::parse(value).map_err(|()| {
                DetailedDecodeFailure::timestamp(
                    message,
                    TimestampWireKind::String,
                    TimestampDiagnosticProblem::InvalidTimestampString,
                )
            })
        }
        value => Err(DetailedDecodeFailure::timestamp(
            message,
            messagepack_value_kind(value),
            TimestampDiagnosticProblem::WrongWireType,
        )),
    }
}

fn messagepack_value_kind(value: &Value) -> TimestampWireKind {
    match value {
        Value::Nil => TimestampWireKind::Nil,
        Value::Boolean(_) => TimestampWireKind::Boolean,
        Value::Integer(_) => TimestampWireKind::Integer,
        Value::F32(_) | Value::F64(_) => TimestampWireKind::Float,
        Value::String(_) => TimestampWireKind::String,
        Value::Binary(_) => TimestampWireKind::Binary,
        Value::Array(_) => TimestampWireKind::Array,
        Value::Map(_) => TimestampWireKind::Map,
        Value::Ext(-1, _) => TimestampWireKind::TimestampExtension,
        Value::Ext(_, _) => TimestampWireKind::OtherExtension,
    }
}

fn label(value: &str) -> Result<String, DecodeError> {
    if value.is_empty() || value.len() > MAX_LABEL_BYTES || value.chars().any(char::is_control) {
        return Err(DecodeError::InvalidMessageSchema);
    }
    Ok(value.to_owned())
}

fn nonnegative_number(value: &Value) -> Result<MarketNumber, DecodeError> {
    let number = match value {
        Value::Integer(integer) if integer.as_i64().is_some() => {
            MarketNumber::Signed(integer.as_i64().expect("checked Some"))
        }
        Value::Integer(integer) => {
            MarketNumber::Unsigned(integer.as_u64().ok_or(DecodeError::InvalidMessageSchema)?)
        }
        Value::F32(value) if value.is_finite() => MarketNumber::Float32(*value),
        Value::F64(value) if value.is_finite() => MarketNumber::Float64(*value),
        _ => return Err(DecodeError::InvalidMessageSchema),
    };
    if number.to_f64() < 0.0 {
        return Err(DecodeError::InvalidMessageSchema);
    }
    Ok(number)
}

fn nonnegative_u64(value: &Value) -> Result<u64, DecodeError> {
    let Value::Integer(integer) = value else {
        return Err(DecodeError::InvalidMessageSchema);
    };
    integer.as_u64().ok_or(DecodeError::InvalidMessageSchema)
}

fn required_integer(fields: &[(Value, Value)], key: &str) -> Result<i64, DecodeError> {
    let Value::Integer(integer) = required_value(fields, key)? else {
        return Err(DecodeError::InvalidMessageSchema);
    };
    integer.as_i64().ok_or(DecodeError::InvalidMessageSchema)
}

fn required_text<'a>(
    fields: &'a [(Value, Value)],
    key: &str,
    max_bytes: usize,
) -> Result<&'a str, DecodeError> {
    let Value::String(value) = required_value(fields, key)? else {
        return Err(DecodeError::InvalidMessageSchema);
    };
    let value = value.as_str().ok_or(DecodeError::InvalidMessageSchema)?;
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(DecodeError::InvalidMessageSchema);
    }
    Ok(value)
}

fn required_value<'a>(fields: &'a [(Value, Value)], key: &str) -> Result<&'a Value, DecodeError> {
    optional_value(fields, key)?.ok_or(DecodeError::InvalidMessageSchema)
}

fn optional_value<'a>(
    fields: &'a [(Value, Value)],
    key: &str,
) -> Result<Option<&'a Value>, DecodeError> {
    let mut found = None;
    for (field_key, field_value) in fields {
        if field_key.as_str() == Some(key) {
            if found.is_some() {
                return Err(DecodeError::DuplicateField);
            }
            found = Some(field_value);
        }
    }
    Ok(found)
}

fn validate_unique_known(fields: &[(Value, Value)], known: &[&str]) -> Result<(), DecodeError> {
    for key in known {
        let _ = optional_value(fields, key)?;
    }
    Ok(())
}

fn classify_provider_error(code: i64) -> ProviderErrorKind {
    match code {
        401 | 402 | 404 => ProviderErrorKind::Authentication,
        405 | 409 | 410 | 413 => ProviderErrorKind::SubscriptionRejected,
        406 => ProviderErrorKind::ConnectionLimit,
        407 => ProviderErrorKind::SlowClient,
        412 => ProviderErrorKind::MessagePackRequired,
        _ => ProviderErrorKind::Other,
    }
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod tests;
