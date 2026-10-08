//! Provider timestamps, local ingest provenance, and typed option stream events.
//!
//! provider 时间戳、本地接收 provenance 与类型化期权流事件。

use std::cmp::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use market_contracts::{DecimalString, NumericEncodingV1};
use tokio::time::Instant;

use crate::config::OptionContractSymbol;

/// Monotonic identifier for one local WebSocket connection attempt.
/// 单次本地 WebSocket 连接尝试的单调代次标识。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionGeneration(u64);

impl SessionGeneration {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the local generation number.
    /// 返回本地会话代次编号。
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Per-session local sequence and monotonic receipt time, separate from provider time.
/// The sequence is assigned at local decode time and is not a provider sequence.
/// 每个会话内的本地序号与单调接收时间；与 provider 时间戳分开保存。
/// 序号由本地解码时分配，不是 provider 序列号。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestStamp {
    /// Local connection generation that received this update.
    /// 接收该更新的本地连接代次。
    pub generation: SessionGeneration,
    /// Local monotonically increasing sequence used only to break equal-provider-time ties.
    /// Provider 时间相同时才使用的本地单调递增接收序号。
    pub sequence: u64,
    /// One-based inbound provider frame sequence within this source generation; zero means no raw-frame link.
    /// 来源代次内从 1 开始的入站 provider 帧序号；零表示没有原始帧关联。
    pub raw_frame_sequence: u64,
    /// One-based market-event ordinal within the referenced frame; zero means no raw-frame link.
    /// 所关联原始帧内从 1 开始的行情事件序号；零表示没有原始帧关联。
    pub raw_frame_event_ordinal: u32,
    /// Number of normalized market events expected from the referenced frame.
    /// 所关联原始帧预期产生的规范化行情事件总数。
    pub raw_frame_event_count: u32,
    /// Monotonic local receipt time.
    /// 本地单调时钟接收时间。
    pub received_at: Instant,
    /// Local wall-clock receive time captured when the provider frame was decoded.
    /// 解码 provider frame 时捕获的本地墙上时钟接收时间。
    pub received_at_utc: DateTime<Utc>,
}

/// Frame metadata captured before any market event is normalized.
/// 在行情事件规范化前捕获的原始帧元数据。
#[derive(Clone, Eq, PartialEq)]
pub struct InboundRawMarketFrame {
    /// Capture UUIDv4 when a trusted pre-decode sink was configured.
    /// 配置可信解码前 sink 时的捕获 UUIDv4。
    pub capture_instance_id: Option<broker_ports::RawCaptureInstanceId>,
    /// Source-local generation that received the raw frame.
    /// 接收原始帧的来源本地代次。
    pub generation: SessionGeneration,
    /// One-based frame sequence within this source generation.
    /// 来源代次内从 1 开始的帧序号。
    pub frame_sequence: u64,
    /// Wall-clock receipt time for the complete binary application frame.
    /// 完整二进制应用帧接收时的墙上时钟时间。
    pub received_at_utc: DateTime<Utc>,
    /// Exact wire encoding observed before decoding.
    /// 解码前观察到的精确 wire 编码。
    pub wire_encoding: broker_ports::RawFrameWireEncoding,
    /// Count of quote/trade messages successfully decoded from the frame.
    /// 从帧中成功解码的 quote/trade 消息数量。
    pub event_count: u32,
    /// Sorted unique symbols found in successfully decoded quote/trade messages.
    /// 成功解码的 quote/trade 消息中按字典序排列的唯一 symbol。
    pub symbols: Vec<String>,
    /// Homogeneous numeric encoding evidence, or absent when mixed/unknown.
    /// 统一数值编码证据；混合或未知时为空。
    pub numeric_encoding: Option<NumericEncodingV1>,
    /// Fixed outcome of frame decoding and classification.
    /// 帧解码和分类的固定结果。
    pub disposition: broker_ports::RawFrameDisposition,
    /// Exact inbound frame bytes under the process-wide raw-data budget.
    /// 受进程级原始数据预算约束的精确入站帧字节。
    pub payload: broker_ports::RawFramePayload,
}

impl std::fmt::Debug for InboundRawMarketFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InboundRawMarketFrame")
            .field("capture_instance_id", &self.capture_instance_id)
            .field("generation", &self.generation)
            .field("frame_sequence", &self.frame_sequence)
            .field("received_at_utc", &self.received_at_utc)
            .field("wire_encoding", &self.wire_encoding)
            .field("event_count", &self.event_count)
            .field("symbol_count", &self.symbols.len())
            .field("numeric_encoding", &self.numeric_encoding)
            .field("disposition", &self.disposition)
            .field("payload", &self.payload)
            .finish()
    }
}

/// Exact provider timestamp components retained independently of local ingest order.
/// Original RFC 3339 text is retained only when supplied by the wire format.
/// 精确保存 provider 时间戳分量，并与本地接收顺序分开。
/// 仅当 wire 格式提供 RFC 3339 文本时才保留原始文本。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTimestamp {
    raw: String,
    unix_seconds: i64,
    nanosecond: u32,
}

impl ProviderTimestamp {
    pub(crate) fn parse(value: &str) -> Result<Self, ()> {
        let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| ())?;
        Ok(Self {
            raw: value.to_owned(),
            unix_seconds: parsed.timestamp(),
            nanosecond: parsed.timestamp_subsec_nanos(),
        })
    }

    /// Builds a timestamp from exact Unix components when the wire source has no text form.
    /// 当 wire 来源没有文本表示时，使用精确 Unix 分量构造时间戳。
    /// Returns `None` for invalid nanoseconds or a UTC instant outside Chrono's range.
    /// 纳秒无效或 UTC 时间点超出 Chrono 范围时返回 `None`。
    pub(crate) fn from_unix_parts(unix_seconds: i64, nanosecond: u32) -> Option<Self> {
        if nanosecond >= 1_000_000_000 {
            return None;
        }
        DateTime::<Utc>::from_timestamp(unix_seconds, nanosecond)?;
        Some(Self {
            raw: String::new(),
            unix_seconds,
            nanosecond,
        })
    }

    /// Returns original RFC 3339 text, or empty when the wire timestamp was binary.
    /// 返回原始 RFC 3339 文本；wire 时间戳为二进制扩展时返回空字符串。
    #[must_use]
    pub fn as_rfc3339(&self) -> &str {
        &self.raw
    }

    /// Returns the exact Unix second component of the provider timestamp.
    /// 返回 provider 时间戳的精确 Unix 秒分量。
    #[must_use]
    pub fn unix_seconds(&self) -> i64 {
        self.unix_seconds
    }

    /// Returns the exact nanosecond fraction of the provider timestamp.
    /// 返回 provider 时间戳的精确纳秒分量。
    #[must_use]
    pub fn nanosecond(&self) -> u32 {
        self.nanosecond
    }

    /// Compares numeric instants independently of the wire encoding or original text.
    /// 比较数值时间点，不依赖 wire 编码或原始文本格式。
    pub(crate) fn cmp_instant(&self, other: &Self) -> Ordering {
        self.unix_seconds
            .cmp(&other.unix_seconds)
            .then_with(|| self.nanosecond.cmp(&other.nanosecond))
    }

    pub(crate) fn to_system_time(&self) -> Option<SystemTime> {
        let total_nanos =
            i128::from(self.unix_seconds) * 1_000_000_000 + i128::from(self.nanosecond);
        let magnitude = total_nanos.unsigned_abs();
        let seconds = u64::try_from(magnitude / 1_000_000_000).ok()?;
        let nanos = u32::try_from(magnitude % 1_000_000_000).ok()?;
        let duration = Duration::new(seconds, nanos);
        if total_nanos >= 0 {
            UNIX_EPOCH.checked_add(duration)
        } else {
            UNIX_EPOCH.checked_sub(duration)
        }
    }
}

/// Number decoded from a `MessagePack` integer or IEEE-754 field without converting its wire kind.
/// 从 `MessagePack` 整数或 IEEE-754 字段解码的数值；保留原 wire 数值类别。
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MarketNumber {
    /// Signed `MessagePack` integer.
    /// 有符号 `MessagePack` 整数。
    Signed(i64),
    /// Unsigned `MessagePack` integer.
    /// 无符号 `MessagePack` 整数。
    Unsigned(u64),
    /// IEEE-754 single-precision value.
    /// IEEE-754 单精度数值。
    Float32(f32),
    /// IEEE-754 double-precision value.
    /// IEEE-754 双精度数值。
    Float64(f64),
}

impl MarketNumber {
    /// Converts the retained wire value to `f64`; this conversion may round integer values above 2^53.
    /// 将保留的 wire 数值转换为 `f64`；大于 2^53 的整数转换可能发生舍入。
    #[allow(clippy::cast_precision_loss)] // This explicitly lossy API is not used for canonical decimal conversion.
    #[must_use]
    pub fn to_f64(self) -> f64 {
        match self {
            Self::Signed(value) => value as f64,
            Self::Unsigned(value) => value as f64,
            Self::Float32(value) => f64::from(value),
            Self::Float64(value) => value,
        }
    }

    pub(crate) fn decimal_string(self) -> Option<DecimalString> {
        let value = match self {
            Self::Signed(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Float32(value) if value.is_finite() => value.to_string(),
            Self::Float64(value) if value.is_finite() => value.to_string(),
            Self::Float32(_) | Self::Float64(_) => return None,
        };
        DecimalString::new(value).ok()
    }

    pub(crate) const fn encoding(self) -> NumericEncodingV1 {
        match self {
            Self::Signed(_) | Self::Unsigned(_) => NumericEncodingV1::IntegerToken,
            Self::Float32(_) => NumericEncodingV1::BinaryFloat32ShortestDecimal,
            Self::Float64(_) => NumericEncodingV1::BinaryFloat64ShortestDecimal,
        }
    }
}

/// Typed provider quote fields from one option contract.
/// 单个期权合约的类型化 provider 报价字段。
#[derive(Clone, Debug, PartialEq)]
pub struct OptionQuote {
    /// Provider option contract symbol.
    /// Provider 期权合约代码。
    pub symbol: OptionContractSymbol,
    /// Provider quote timestamp, separate from [`IngestStamp`].
    /// Provider 报价时间戳；与 [`IngestStamp`] 分开保存。
    pub timestamp: ProviderTimestamp,
    /// Bid exchange code.
    /// 买方交易所代码。
    pub bid_exchange: String,
    /// Bid price in its original `MessagePack` numeric category.
    /// 保留原 `MessagePack` 数值类别的买价。
    pub bid_price: MarketNumber,
    /// Bid size.
    /// 买方数量。
    pub bid_size: u64,
    /// Ask exchange code.
    /// 卖方交易所代码。
    pub ask_exchange: String,
    /// Ask price in its original `MessagePack` numeric category.
    /// 保留原 `MessagePack` 数值类别的卖价。
    pub ask_price: MarketNumber,
    /// Ask size.
    /// 卖方数量。
    pub ask_size: u64,
    /// Provider quote conditions; an absent condition field is an empty list.
    /// Provider 报价条件；缺失时为空列表。
    pub conditions: Vec<String>,
    /// SHA-256 of the complete raw `MessagePack` application frame carrying this quote.
    /// 承载此报价的完整原始 `MessagePack` 应用 frame 的 SHA-256。
    pub raw_frame_sha256: String,
}

/// Typed provider trade fields from one option contract.
/// 单个期权合约的类型化 provider 成交字段。
#[derive(Clone, Debug, PartialEq)]
pub struct OptionTrade {
    /// Provider option contract symbol.
    /// Provider 期权合约代码。
    pub symbol: OptionContractSymbol,
    /// Provider trade timestamp, separate from [`IngestStamp`].
    /// Provider 成交时间戳；与 [`IngestStamp`] 分开保存。
    pub timestamp: ProviderTimestamp,
    /// Trade price in its original `MessagePack` numeric category.
    /// 保留原 `MessagePack` 数值类别的成交价。
    pub price: MarketNumber,
    /// Trade size.
    /// 成交数量。
    pub size: u64,
    /// Provider exchange code.
    /// Provider 交易所代码。
    pub exchange: String,
    /// Provider trade conditions; an absent condition field is an empty list.
    /// Provider 成交条件；缺失时为空列表。
    pub conditions: Vec<String>,
    /// SHA-256 of the complete raw `MessagePack` application frame carrying this trade.
    /// 承载此成交的完整原始 `MessagePack` 应用 frame 的 SHA-256。
    pub raw_frame_sha256: String,
}

/// Relative provider timestamp classification used only for session readiness gating.
/// 仅供会话 ready gate 使用的 provider 时间戳相对分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataFreshness {
    /// The provider timestamp falls inside the configured age and future-skew window.
    /// Provider 时间戳位于配置的年龄与未来偏差窗口内。
    Fresh,
    /// The provider timestamp is older than the configured readiness age.
    /// Provider 时间戳早于配置的 ready 年龄范围。
    Stale,
    /// The provider timestamp exceeds the configured future-skew allowance.
    /// Provider 时间戳超过配置的未来偏差容限。
    FutureDated,
}

/// Public session phases emitted on the protected control lane.
/// 通过独立控制队列发送的公开会话阶段。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPhase {
    /// A new bounded connection attempt is starting.
    /// 正在开始新的有界连接尝试。
    Connecting,
    /// The WebSocket is open and the initial provider connection control is pending.
    /// WebSocket 已打开，正在等待 provider 初始连接控制消息。
    AwaitingConnected,
    /// The local client is sending the injected credential pair.
    /// 本地客户端正在发送注入的凭证对。
    Authenticating,
    /// Provider authentication acknowledgement is pending.
    /// 正在等待 provider 认证回执。
    AwaitingAuthentication,
    /// The complete desired option subscription is being sent.
    /// 正在发送完整期望期权订阅。
    Subscribing,
    /// Provider subscription acknowledgement is pending.
    /// 正在等待 provider 订阅回执。
    AwaitingSubscriptionAcknowledgement,
    /// All desired subscriptions are acknowledged; waiting for fresh data.
    /// 全部期望订阅已确认，正在等待新鲜行情。
    AwaitingFreshData,
    /// Subscription state and fresh data are present for this generation.
    /// 当前代次的订阅已确认且已有新鲜行情。
    Ready,
    /// The prior generation has been invalidated after session loss.
    /// 旧会话已丢失，先前代次已经失效。
    SessionLost,
    /// The session has stopped or was cancelled.
    /// 会话已停止或已取消。
    Closed,
}

/// Stable cause category attached to session loss and shutdown control events.
/// 会话丢失和关闭控制事件携带的固定原因类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionStatusCause {
    /// A transport or peer close ended the WebSocket.
    /// WebSocket 传输错误或对端关闭。
    TransportLost,
    /// Authentication was rejected or timed out.
    /// 认证被拒绝或超时。
    AuthenticationRejected,
    /// The local authentication deadline expired before the provider acknowledged it.
    /// Provider 确认前本地认证期限已过。
    AuthenticationTimeout,
    /// The complete desired subscription set was not acknowledged in time.
    /// 完整期望订阅集合未能及时获得回执。
    AcknowledgementTimeout,
    /// One local WebSocket connect attempt exceeded its deadline.
    /// 单次本地 WebSocket 连接尝试超过期限。
    ConnectionTimeout,
    /// The provider refused a connection because its account limit was reached.
    /// Provider 因账户连接上限拒绝连接。
    ConnectionLimitReached,
    /// Desired subscriptions were refused or did not receive an exact acknowledgement.
    /// 期望订阅被拒绝或未收到完全匹配的回执。
    SubscriptionRejected,
    /// A bounded quote, trade, or control consumer could not accept more data.
    /// 有界报价、成交或控制消费者无法接收更多数据。
    ConsumerOverloaded,
    /// A consumer closed one of the bounded delivery lanes.
    /// 消费者关闭了一个有界交付队列。
    ConsumerClosed,
    /// A malformed or oversized provider message stopped the session.
    /// 畸形或超大 provider 消息使会话停止。
    ProtocolViolation,
    /// The caller cancelled the session.
    /// 调用方取消了会话。
    Cancelled,
}

/// Full option subscription state reported by the provider after an update.
/// provider 在订阅更新后返回的完整期权订阅状态。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionAcknowledgement {
    /// Currently acknowledged quote symbols.
    /// 当前已确认的报价代码。
    pub quotes: std::collections::BTreeSet<OptionContractSymbol>,
    /// Currently acknowledged trade symbols.
    /// 当前已确认的成交代码。
    pub trades: std::collections::BTreeSet<OptionContractSymbol>,
}

pub(crate) fn classify_freshness(
    timestamp: &ProviderTimestamp,
    now: SystemTime,
    max_age: Duration,
    future_skew: Duration,
) -> DataFreshness {
    let Some(source_time) = timestamp.to_system_time() else {
        return DataFreshness::FutureDated;
    };
    if source_time > now {
        return if source_time.duration_since(now).unwrap_or_default() <= future_skew {
            DataFreshness::Fresh
        } else {
            DataFreshness::FutureDated
        };
    }
    if now.duration_since(source_time).unwrap_or_default() > max_age {
        DataFreshness::Stale
    } else {
        DataFreshness::Fresh
    }
}
