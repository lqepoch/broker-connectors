//! Bounded, read-only JSON codec for Schwab Streamer response/data/notify frames.
//! 提供有界且脱敏的 Streamer JSON 帧解码。

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

/// Maximum accepted serialized WebSocket frame size.
/// 中文摘要：Streamer 收发单帧的最大序列化字节数；超限帧在解析前拒绝。
pub const MAX_WIRE_FRAME_BYTES: usize = 2 * 1024 * 1024;
/// Maximum number of items accepted in any response/data/notify/content array.
/// 中文摘要：单个 Streamer 响应、数据或通知数组接受的最大条目数。
pub const MAX_WIRE_ARRAY_ITEMS: usize = 4096;

/// A validated Streamer JSON frame with unknown object fields preserved.
/// 中文摘要：经大小、JSON 深度和数组条目限制校验的响应、行情与通知帧；未知对象字段保留但不建立 authority。
#[derive(Clone, PartialEq)]
pub struct StreamerWireFrame {
    /// Optional response records present in the frame.
    /// 中文摘要：帧中可选的命令响应数组；缺失与空数组保持区分。
    pub response: Option<Vec<StreamerCommandResponse>>,
    /// Optional service data records present in the frame.
    /// 中文摘要：行情数据负载。
    pub data: Option<Vec<StreamerDataPayload>>,
    /// Optional heartbeat/notification records present in the frame.
    /// 中文摘要：异步通知负载。
    pub notify: Option<Vec<StreamerNotifyPayload>>,
    /// Unknown top-level fields, preserved for forward compatibility.
    /// 中文摘要：未建模的帧顶层字段，以原始 JSON 值保留供前向兼容检查。
    pub extra_fields: BTreeMap<String, Value>,
}

impl Debug for StreamerWireFrame {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerWireFrame")
            .field("response", &"[REDACTED]")
            .field("data", &"[REDACTED]")
            .field("notify", &"[REDACTED]")
            .field("extra_fields", &"[REDACTED]")
            .finish()
    }
}

/// A validated command response record.
/// 中文摘要：已校验的 Streamer 命令 ACK 投影；仅解析结果，不自行推进订阅状态。
#[derive(Clone, PartialEq, Serialize)]
pub struct StreamerCommandResponse {
    /// Streamer service name.
    /// 中文摘要：该命令或数据行所属的 Streamer 服务。
    pub service: String,
    /// Request identifier normalized to a string.
    /// 中文摘要：用于关联命令与确认的请求标识符。
    pub request_id: String,
    /// Streamer command name.
    /// 中文摘要：要发送或已确认的命令类别。
    pub command: String,
    /// Finite numeric server timestamp.
    /// 中文摘要：broker 提供的事件时间戳。
    pub timestamp: f64,
    /// Command response code and message.
    /// 中文摘要：已解码的协议内容。
    pub content: StreamerResponseContent,
    /// Unknown response fields, preserved for forward compatibility.
    /// 中文摘要：未建模的帧顶层字段，以原始 JSON 值保留供前向兼容检查。
    pub extra_fields: BTreeMap<String, Value>,
}

impl Debug for StreamerCommandResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerCommandResponse")
            .field("service", &"[REDACTED]")
            .field("request_id", &"[REDACTED]")
            .field("command", &"[REDACTED]")
            .field("timestamp", &"[REDACTED]")
            .field("content", &"[REDACTED]")
            .field("extra_fields", &"[REDACTED]")
            .finish()
    }
}

/// A validated response content object.
/// 中文摘要：命令响应的整数结果码与 broker 文本消息。
#[derive(Clone, PartialEq, Serialize)]
pub struct StreamerResponseContent {
    /// Integer command result code.
    /// 中文摘要：稳定的分类代码。
    pub code: i64,
    /// Broker-provided response message.
    /// 中文摘要：broker 在该响应内容中返回的说明文本。
    pub msg: String,
}

impl Debug for StreamerResponseContent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerResponseContent")
            .field("code", &"[REDACTED]")
            .field("msg", &"[REDACTED]")
            .finish()
    }
}

/// A validated service data record.
/// 中文摘要：一个服务的行情数据包，包含事件时间戳和按序数据行。
#[derive(Clone, PartialEq, Serialize)]
pub struct StreamerDataPayload {
    /// Streamer service name.
    /// 中文摘要：该命令或数据行所属的 Streamer 服务。
    pub service: String,
    /// Finite numeric server timestamp.
    /// 中文摘要：broker 提供的事件时间戳。
    pub timestamp: f64,
    /// Streamer command name.
    /// 中文摘要：要发送或已确认的命令类别。
    pub command: String,
    /// Validated data rows.
    /// 中文摘要：已解码的协议内容。
    pub content: Vec<StreamerDataRow>,
    /// Unknown payload fields, preserved for forward compatibility.
    /// 中文摘要：未建模的帧顶层字段，以原始 JSON 值保留供前向兼容检查。
    pub extra_fields: BTreeMap<String, Value>,
}

impl Debug for StreamerDataPayload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerDataPayload")
            .field("service", &"[REDACTED]")
            .field("timestamp", &"[REDACTED]")
            .field("command", &"[REDACTED]")
            .field("content", &"[REDACTED]")
            .field("extra_fields", &"[REDACTED]")
            .finish()
    }
}

/// A validated data row with arbitrary service fields retained as JSON values.
/// 中文摘要：单个服务数据行；字段以原始 JSON 值保留，字段时间与来源 provenance 由上层状态维护。
#[derive(Clone, PartialEq, Serialize)]
pub struct StreamerDataRow {
    /// Optional row key.
    /// 中文摘要：该服务订阅或返回的键。
    pub key: Option<String>,
    /// Remaining row fields, including numeric Schwab field identifiers.
    /// 中文摘要：限制或选择 broker 返回字段的列表。
    pub fields: BTreeMap<String, Value>,
}

impl Debug for StreamerDataRow {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerDataRow")
            .field("key", &"[REDACTED]")
            .field("fields", &"[REDACTED]")
            .finish()
    }
}

/// A validated notification record.
/// 中文摘要：通知帧投影；可选 heartbeat 不作为行情 freshness 证据。
#[derive(Clone, PartialEq, Serialize)]
pub struct StreamerNotifyPayload {
    /// Optional heartbeat text.
    /// 中文摘要：通知帧中可选的心跳内容，不作为市场行情新鲜度证明。
    pub heartbeat: Option<String>,
    /// Unknown notification fields, preserved for forward compatibility.
    /// 中文摘要：未建模的帧顶层字段，以原始 JSON 值保留供前向兼容检查。
    pub extra_fields: BTreeMap<String, Value>,
}

impl Debug for StreamerNotifyPayload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerNotifyPayload")
            .field("heartbeat", &"[REDACTED]")
            .field("extra_fields", &"[REDACTED]")
            .finish()
    }
}

/// Safe public error categories for frame decoding.
///
/// These errors never include source bytes, parsed field values, or serde error
/// text, so callers can log them without retaining broker payload contents.
/// 中文摘要：帧为空、超限、JSON 无效或 schema 错误的脱敏分类；不包含原始字节。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerWireError {
    /// The input did not contain any bytes.
    /// 接收 frame 中没有 JSON 字节。
    EmptyFrame,
    /// The serialized frame exceeded [`MAX_WIRE_FRAME_BYTES`].
    /// 序列化 frame 超过固定字节上限。
    FrameTooLarge {
        /// Maximum serialized frame size accepted by the decoder, in bytes.
        /// 解码器接受的序列化 frame 字节数上限。
        limit: usize,
    },
    /// The input was not valid JSON or exceeded the JSON nesting limit.
    /// 接收 frame 不是有效且有界的 JSON。
    MalformedJson,
    /// The JSON value violated a required field or field type.
    /// JSON frame 不符合必需的 Streamer 字段 schema。
    InvalidSchema,
}

impl Display for StreamerWireError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyFrame => formatter.write_str("empty Streamer frame"),
            Self::FrameTooLarge { limit } => {
                write!(formatter, "Streamer frame exceeds {limit} bytes")
            }
            Self::MalformedJson => formatter.write_str("malformed Streamer JSON frame"),
            Self::InvalidSchema => formatter.write_str("invalid Streamer frame schema"),
        }
    }
}

impl Error for StreamerWireError {}

/// Parses one bounded JSON frame without network or socket access.
/// 中文摘要：在帧字节数、嵌套深度和数组条目上限内解码 JSON；错误不包含原始字节或 payload 值。
///
/// # Errors
/// Returns [`StreamerWireError::EmptyFrame`] for empty input,
/// [`StreamerWireError::FrameTooLarge`] above the frame bound,
/// [`StreamerWireError::MalformedJson`] for invalid bounded JSON, or
/// [`StreamerWireError::InvalidSchema`] when fields violate the local schema.
pub fn parse_streamer_frame(bytes: &[u8]) -> Result<StreamerWireFrame, StreamerWireError> {
    if bytes.is_empty() {
        return Err(StreamerWireError::EmptyFrame);
    }
    if bytes.len() > MAX_WIRE_FRAME_BYTES {
        return Err(StreamerWireError::FrameTooLarge {
            limit: MAX_WIRE_FRAME_BYTES,
        });
    }

    let raw = serde_json::from_slice::<RawStreamerFrame>(bytes).map_err(|error| {
        if error.classify() == serde_json::error::Category::Data {
            StreamerWireError::InvalidSchema
        } else {
            StreamerWireError::MalformedJson
        }
    })?;
    Ok(raw.into_public())
}

/// Returns whether a parsed command response code represents success.
///
/// This intentionally follows the current Node implementation: code zero is
/// generic success for any service/command; 26/27/28/29 are accepted only for
/// SUBS/UNSUBS/ADD/VIEW respectively. No service-specific override currently
/// exists in `src/types/streamer.ts`.
/// 中文摘要：按协议成功码范围分类命令结果；不负责关联响应或推进状态。
#[must_use]
pub fn is_successful_streamer_command(_service: &str, command: &str, code: i64) -> bool {
    if code == 0 {
        return true;
    }
    match command {
        "SUBS" => code == 26,
        "UNSUBS" => code == 27,
        "ADD" => code == 28,
        "VIEW" => code == 29,
        _ => false,
    }
}

#[derive(Deserialize)]
struct RawStreamerFrame {
    #[serde(default, deserialize_with = "deserialize_optional_bounded_vec")]
    response: Option<BoundedVec<RawCommandResponse>>,
    #[serde(default, deserialize_with = "deserialize_optional_bounded_vec")]
    data: Option<BoundedVec<RawDataPayload>>,
    #[serde(default, deserialize_with = "deserialize_optional_bounded_vec")]
    notify: Option<BoundedVec<RawNotifyPayload>>,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, Value>,
}

impl RawStreamerFrame {
    fn into_public(self) -> StreamerWireFrame {
        StreamerWireFrame {
            response: self.response.map(|records| {
                records
                    .0
                    .into_iter()
                    .map(RawCommandResponse::into_public)
                    .collect()
            }),
            data: self.data.map(|records| {
                records
                    .0
                    .into_iter()
                    .map(RawDataPayload::into_public)
                    .collect()
            }),
            notify: self.notify.map(|records| {
                records
                    .0
                    .into_iter()
                    .map(RawNotifyPayload::into_public)
                    .collect()
            }),
            extra_fields: self.extra_fields,
        }
    }
}

#[derive(Deserialize)]
struct RawCommandResponse {
    service: String,
    #[serde(rename = "requestid", deserialize_with = "deserialize_request_id")]
    request_id: String,
    command: String,
    #[serde(deserialize_with = "deserialize_finite_number")]
    timestamp: f64,
    content: RawResponseContent,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, Value>,
}

impl RawCommandResponse {
    fn into_public(self) -> StreamerCommandResponse {
        StreamerCommandResponse {
            service: self.service,
            request_id: self.request_id,
            command: self.command,
            timestamp: self.timestamp,
            content: self.content.into_public(),
            extra_fields: self.extra_fields,
        }
    }
}

#[derive(Deserialize)]
struct RawResponseContent {
    #[serde(deserialize_with = "deserialize_integer_code")]
    code: i64,
    msg: String,
}

impl RawResponseContent {
    fn into_public(self) -> StreamerResponseContent {
        StreamerResponseContent {
            code: self.code,
            msg: self.msg,
        }
    }
}

#[derive(Deserialize)]
struct RawDataPayload {
    service: String,
    #[serde(deserialize_with = "deserialize_finite_number")]
    timestamp: f64,
    command: String,
    content: BoundedVec<RawDataRow>,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, Value>,
}

impl RawDataPayload {
    fn into_public(self) -> StreamerDataPayload {
        StreamerDataPayload {
            service: self.service,
            timestamp: self.timestamp,
            command: self.command,
            content: self
                .content
                .0
                .into_iter()
                .map(RawDataRow::into_public)
                .collect(),
            extra_fields: self.extra_fields,
        }
    }
}

#[derive(Deserialize)]
struct RawDataRow {
    #[serde(default, deserialize_with = "deserialize_optional_non_null_string")]
    key: Option<String>,
    #[serde(flatten)]
    fields: BTreeMap<String, Value>,
}

impl RawDataRow {
    fn into_public(self) -> StreamerDataRow {
        StreamerDataRow {
            key: self.key,
            fields: self.fields,
        }
    }
}

#[derive(Deserialize)]
struct RawNotifyPayload {
    #[serde(default, deserialize_with = "deserialize_optional_non_null_string")]
    heartbeat: Option<String>,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, Value>,
}

impl RawNotifyPayload {
    fn into_public(self) -> StreamerNotifyPayload {
        StreamerNotifyPayload {
            heartbeat: self.heartbeat,
            extra_fields: self.extra_fields,
        }
    }
}

struct BoundedVec<T>(Vec<T>);

impl<'de, T> Deserialize<'de> for BoundedVec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoundedVecVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    formatter,
                    "an array with at most {MAX_WIRE_ARRAY_ITEMS} items"
                )
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let capacity = sequence.size_hint().unwrap_or(0).min(MAX_WIRE_ARRAY_ITEMS);
                let mut values = Vec::with_capacity(capacity);
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX_WIRE_ARRAY_ITEMS {
                        return Err(de::Error::custom("Streamer array exceeds item limit"));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVecVisitor(std::marker::PhantomData))
    }
}

fn deserialize_optional_bounded_vec<'de, T, D>(
    deserializer: D,
) -> Result<Option<BoundedVec<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    BoundedVec::<T>::deserialize(deserializer).map(Some)
}

fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

fn deserialize_finite_number<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let timestamp = deserializer.deserialize_any(JavascriptNumberVisitor)?;
    if timestamp.is_finite() {
        Ok(timestamp)
    } else {
        Err(de::Error::custom(
            "timestamp must coerce to a finite number",
        ))
    }
}

struct JavascriptNumberVisitor;

fn integer_as_javascript_number<E, N>(value: N) -> Result<f64, E>
where
    E: de::Error,
    serde_json::Number: From<N>,
{
    serde_json::Number::from(value)
        .as_f64()
        .ok_or_else(|| E::custom("timestamp must coerce to a finite number"))
}

impl<'de> Visitor<'de> for JavascriptNumberVisitor {
    type Value = f64;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value coercible to a finite JavaScript number")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(if value { 1.0 } else { 0.0 })
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        integer_as_javascript_number(value)
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        integer_as_javascript_number(value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        integer_as_javascript_number(value)
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        integer_as_javascript_number(value)
    }

    fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(f64::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        parse_javascript_number(value)
            .ok_or_else(|| E::custom("timestamp must coerce to a finite number"))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(0.0)
    }

    fn visit_seq<A>(self, sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let value = Value::deserialize(SeqAccessDeserializer::new(sequence))?;
        javascript_number(&value)
            .ok_or_else(|| de::Error::custom("timestamp must coerce to a finite number"))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // serde_json uses a private map representation for arbitrary-precision
        // numbers. Deserializing through Value here preserves that path while
        // keeping ordinary numeric timestamps allocation-free.
        let value = Value::deserialize(MapAccessDeserializer::new(map))?;
        javascript_number(&value)
            .ok_or_else(|| de::Error::custom("timestamp must coerce to a finite number"))
    }
}

fn javascript_number(value: &Value) -> Option<f64> {
    match value {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(number) => number.as_f64(),
        Value::String(value) => parse_javascript_number(value),
        Value::Array(values) => {
            let converted = values
                .iter()
                .map(javascript_array_element_to_string)
                .collect::<Option<Vec<_>>>()?
                .join(",");
            parse_javascript_number(&converted)
        }
        Value::Object(_) => Some(f64::NAN),
    }
}

fn javascript_array_element_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => Some(String::new()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(values) => Some(
            values
                .iter()
                .map(javascript_array_element_to_string)
                .collect::<Option<Vec<_>>>()?
                .join(","),
        ),
        Value::Object(_) => Some("[object Object]".to_owned()),
    }
}

fn parse_javascript_number(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Some(0.0);
    }
    if matches!(value, "Infinity" | "+Infinity") {
        return Some(f64::INFINITY);
    }
    if value == "-Infinity" {
        return Some(f64::NEG_INFINITY);
    }
    if value.starts_with("0x") || value.starts_with("0X") {
        return parse_radix_number(value.get(2..)?, 16);
    }
    if value.starts_with("0b") || value.starts_with("0B") {
        return parse_radix_number(value.get(2..)?, 2);
    }
    if value.starts_with("0o") || value.starts_with("0O") {
        return parse_radix_number(value.get(2..)?, 8);
    }
    value.parse().ok()
}

fn parse_radix_number(digits: &str, radix: u32) -> Option<f64> {
    if digits.is_empty() {
        return None;
    }
    let mut result = 0.0;
    for character in digits.chars() {
        let digit = character.to_digit(radix)?;
        result = result * f64::from(radix) + f64::from(digit);
    }
    Some(result)
}

fn deserialize_integer_code<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let code = match value {
        Value::Number(number) => integer_from_number(&number),
        Value::String(value) => integer_from_string(&value),
        _ => None,
    };
    code.ok_or_else(|| de::Error::custom("code must be an integer or integer string"))
}

fn integer_from_number(number: &Number) -> Option<i64> {
    if let Some(value) = number.as_i64() {
        return Some(value);
    }
    if let Some(value) = number.as_u64() {
        return i64::try_from(value).ok();
    }
    let value = number.as_f64()?;
    if !value.is_finite()
        || value.fract() != 0.0
        || value < -9_223_372_036_854_775_808.0
        || value >= 9_223_372_036_854_775_808.0
    {
        return None;
    }
    format!("{value:.0}").parse().ok()
}

fn integer_from_string(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    let digits = if bytes.first() == Some(&b'-') {
        bytes.get(1..)?
    } else {
        bytes
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    value.parse().ok()
}

fn deserialize_request_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(value) => Ok(value),
        Value::Number(number) => {
            let Some(value) = number.as_f64() else {
                return Err(de::Error::custom(
                    "requestid must be a string or safe integer",
                ));
            };
            if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_SAFE_INTEGER {
                return Err(de::Error::custom(
                    "requestid must be a string or safe integer",
                ));
            }
            Ok(format!("{value:.0}"))
        }
        _ => Err(de::Error::custom(
            "requestid must be a string or safe integer",
        )),
    }
}
