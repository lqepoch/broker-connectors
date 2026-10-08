#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Provider-neutral, read-only broker market-data port contracts.
//!
//! This crate depends on the frozen `trading-core::market-contracts` schema and
//! contains no provider SDK, credential storage, account authority, execution
//! transport, cache, retry loop, or scheduler.
//!
//! # 简体中文
//!
//! 本 crate 提供供应商中立的只读券商行情端口合同。
//!
//! 它依赖已冻结的 `trading-core::market-contracts` schema，不含供应商 SDK、凭证存储、账户权威、
//! 执行传输、缓存、重试循环或调度器。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use market_contracts::{
    ControlEventEnvelopeV1, EntitlementState, MarketEventEnvelopeV1, NumericEncodingV1,
    UtcTimestamp,
};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

/// Maximum byte length retained for one raw provider application frame.
/// 单个原始 provider 应用帧允许保留的最大字节数。
pub const MAX_RAW_FRAME_BYTES: usize = 1024 * 1024;

/// Process-wide bound for raw frame bytes held by adapters and downstream consumers.
/// adapter 和下游消费者持有的原始帧进程级总字节上限。
pub const MAX_IN_FLIGHT_RAW_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Process-wide bound for raw frame records, including small frames.
/// 包括小帧在内的原始帧进程级记录数量上限。
pub const MAX_IN_FLIGHT_RAW_FRAME_RECORDS: usize = 1_024;

static GLOBAL_RAW_FRAME_BUDGET: OnceLock<RawFrameBudget> = OnceLock::new();

/// Maximum number of requested instruments in one local subscription.
/// 单次本地订阅允许请求的最大合约数量。
pub const MAX_SUBSCRIPTION_INSTRUMENTS: usize = 4_096;

/// Maximum UTF-8 byte length accepted for one instrument identifier.
/// 单个合约标识允许的最大 UTF-8 字节数。
pub const MAX_INSTRUMENT_ID_BYTES: usize = 256;

/// A boxed `Send` future used to keep read-port traits object-safe.
/// 用于保持只读端口 trait 对象安全的装箱 `Send` future。
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A validated request for one explicit provider/feed subscription.
/// 一个已校验且明确指定供应商与 feed 的订阅请求。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataSubscriptionRequest {
    provider: String,
    feed: String,
    quote_symbols: Vec<String>,
    trade_symbols: Vec<String>,
}

impl MarketDataSubscriptionRequest {
    /// Creates a bounded subscription request, rejecting duplicates and empty channel sets.
    /// 创建有界订阅请求，并拒绝重复代码和空行情 channel 集合。
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for malformed identifiers, duplicate symbols, or empty channel
    /// sets; returns `LimitExceeded` when the generic request bound is exceeded.
    ///
    /// # 错误
    ///
    /// 标识格式错误、代码重复或 channel 集合为空时返回 `InvalidRequest`；超过通用请求上限时返回
    /// `LimitExceeded`。
    pub fn new(
        provider: impl Into<String>,
        feed: impl Into<String>,
        quote_symbols: impl IntoIterator<Item = String>,
        trade_symbols: impl IntoIterator<Item = String>,
    ) -> Result<Self, BrokerPortError> {
        let provider = provider.into();
        let feed = feed.into();
        validate_source_id(&provider)?;
        validate_source_id(&feed)?;
        let quote_symbols = collect_symbols(quote_symbols)?;
        let trade_symbols = collect_symbols(trade_symbols)?;
        if quote_symbols.is_empty() && trade_symbols.is_empty() {
            return Err(BrokerPortError::InvalidRequest);
        }
        if quote_symbols.len().saturating_add(trade_symbols.len()) > MAX_SUBSCRIPTION_INSTRUMENTS {
            return Err(BrokerPortError::LimitExceeded);
        }
        Ok(Self {
            provider,
            feed,
            quote_symbols,
            trade_symbols,
        })
    }

    /// Returns the exact provider identifier.
    /// 返回精确的供应商标识。
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the exact feed identifier.
    /// 返回精确的 feed 标识。
    #[must_use]
    pub fn feed(&self) -> &str {
        &self.feed
    }

    /// Returns requested quote identifiers in deterministic lexical order.
    /// 按确定性字典序返回报价订阅标识。
    #[must_use]
    pub fn quote_symbols(&self) -> &[String] {
        &self.quote_symbols
    }

    /// Returns requested trade identifiers in deterministic lexical order.
    /// 按确定性字典序返回成交订阅标识。
    #[must_use]
    pub fn trade_symbols(&self) -> &[String] {
        &self.trade_symbols
    }
}

/// Stable, secret-free port error categories.
/// 固定且不包含秘密的端口错误类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerPortError {
    /// The request is malformed or has no instruments.
    /// 请求格式无效或未包含合约。
    InvalidRequest,
    /// A local resource or protocol limit was exceeded.
    /// 超出本地资源或协议上限。
    LimitExceeded,
    /// The selected provider/feed is not available in this adapter.
    /// 当前 adapter 不支持所选供应商或 feed。
    UnsupportedSource,
    /// The upstream transport failed without a safe detailed classification.
    /// 上游传输失败，且没有安全的详细分类。
    Transport,
    /// The provider rejected authentication, entitlement, or the requested subscription.
    /// 供应商拒绝认证、权限或所请求的订阅。
    ProviderRejected,
    /// A bounded delivery lane could not accept an event.
    /// 有界交付队列无法接收事件。
    Overloaded,
    /// The provider response did not satisfy the frozen protocol contract.
    /// 供应商响应不符合冻结的协议合同。
    ProtocolViolation,
}

/// One ordered market/control record lane plus a cancellation signal.
/// 单一有序行情/控制记录队列及取消信号。
pub struct MarketDataSession {
    /// Provider-neutral market and control records in adapter-assigned sequence order.
    /// 按 adapter 分配的序号顺序排列的供应商中立行情及控制记录。
    pub records: mpsc::Receiver<MarketDataItem>,
    cancel: watch::Sender<bool>,
}

/// One item in the ordered read-only market-data session lane.
/// 只读行情会话有序队列中的一条记录。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketDataItem {
    /// A validated market event and its optional byte-exact source-frame correlation.
    /// 已校验行情事件及其可选的逐字节原始帧关联信息。
    Event {
        /// Frozen cross-language market event envelope.
        /// 冻结的跨语言行情事件信封。
        envelope: MarketEventEnvelopeV1,
        /// Source-frame identity and one-based position within that frame.
        /// 原始帧身份及该帧内从 1 开始的事件位置。
        raw_frame: Option<RawFrameReference>,
    },
    /// A connection, acknowledgement, or failure control.
    /// 连接、订阅回执或失败控制事件。
    Control(ControlEventEnvelopeV1),
    /// One byte-exact inbound provider application frame.
    /// 一条逐字节保真的 provider 入站应用帧。
    RawFrame(RawMarketFrame),
}

/// A process-wide byte and record budget for raw application frames.
/// 原始应用帧使用的进程级字节与记录预算。
#[derive(Clone)]
pub struct RawFrameBudget {
    bytes: Arc<Semaphore>,
    records: Arc<Semaphore>,
}

impl RawFrameBudget {
    /// Returns the process-wide shared raw-frame budget.
    /// 返回进程内所有 adapter 共享的原始帧预算。
    #[must_use]
    pub fn global() -> Self {
        GLOBAL_RAW_FRAME_BUDGET
            .get_or_init(|| Self {
                bytes: Arc::new(Semaphore::new(MAX_IN_FLIGHT_RAW_FRAME_BYTES)),
                records: Arc::new(Semaphore::new(MAX_IN_FLIGHT_RAW_FRAME_RECORDS)),
            })
            .clone()
    }
}

/// A bounded raw frame whose budget lease follows it through the consumer queue.
/// 有界原始帧；预算租约随记录一直保留到消费者释放该记录。
#[derive(Clone)]
pub struct RawFramePayload {
    inner: Arc<RawFramePayloadInner>,
}

struct RawFramePayloadInner {
    bytes: Arc<[u8]>,
    sha256: String,
    _byte_permit: Option<OwnedSemaphorePermit>,
    _record_permit: OwnedSemaphorePermit,
}

impl RawFramePayload {
    /// Captures one exact frame under the process-wide byte and record budgets.
    /// 在进程级字节和记录预算内捕获一条精确原始帧。
    ///
    /// # Errors
    ///
    /// Returns `RawFrameCaptureError` for an oversized or over-budget frame.
    ///
    /// # 错误
    ///
    /// 帧超过单帧上限或超出进程预算时返回 `RawFrameCaptureError`。
    pub fn capture(bytes: Vec<u8>) -> Result<Self, RawFrameCaptureError> {
        if bytes.len() > MAX_RAW_FRAME_BYTES {
            return Err(RawFrameCaptureError::Oversized);
        }
        let budget = RawFrameBudget::global();
        let permits = u32::try_from(bytes.len()).map_err(|_| RawFrameCaptureError::Oversized)?;
        let byte_permit = if permits == 0 {
            None
        } else {
            Some(
                Arc::clone(&budget.bytes)
                    .try_acquire_many_owned(permits)
                    .map_err(|_| RawFrameCaptureError::BudgetExceeded)?,
            )
        };
        let record_permit = Arc::clone(&budget.records)
            .try_acquire_owned()
            .map_err(|_| RawFrameCaptureError::BudgetExceeded)?;
        let digest = Sha256::digest(&bytes);
        let mut sha256 = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            write!(&mut sha256, "{byte:02x}").expect("writing into a String cannot fail");
        }
        Ok(Self {
            inner: Arc::new(RawFramePayloadInner {
                bytes: Arc::from(bytes),
                sha256,
                _byte_permit: byte_permit,
                _record_permit: record_permit,
            }),
        })
    }

    /// Returns the exact inbound bytes without exposing them through `Debug`.
    /// 返回精确入站字节，但不会通过 `Debug` 暴露内容。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner.bytes
    }

    /// Returns the lowercase SHA-256 calculated over the exact frame bytes.
    /// 返回对精确帧字节计算所得的小写 SHA-256。
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.inner.sha256
    }
}

impl std::fmt::Debug for RawFramePayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawFramePayload")
            .field("len_bytes", &self.inner.bytes.len())
            .field("sha256", &self.inner.sha256)
            .finish_non_exhaustive()
    }
}

impl PartialEq for RawFramePayload {
    fn eq(&self, other: &Self) -> bool {
        self.inner.sha256 == other.inner.sha256 && self.inner.bytes == other.inner.bytes
    }
}

impl Eq for RawFramePayload {}

/// Stable failure classes for bounded raw frame capture.
/// 有界原始帧捕获的固定失败类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameCaptureError {
    /// The provider frame exceeded the per-frame byte limit.
    /// provider 帧超过单帧字节上限。
    Oversized,
    /// The process-wide byte or record budget is exhausted.
    /// 进程级字节或记录预算已耗尽。
    BudgetExceeded,
}

/// Why the exact frame was retained, including rejected application frames.
/// 保留该精确帧的原因，包括被拒绝的应用帧。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameDisposition {
    /// The frame decoded and contained one or more supported market events.
    /// 帧已解码，且包含一个或多个受支持行情事件。
    DecodedMarketData,
    /// The frame included an unknown provider message type.
    /// 帧包含未知 provider 消息类型。
    UnknownMessage,
    /// The frame included a provider error message.
    /// 帧包含 provider 错误消息。
    ProviderError,
    /// The application frame could not be decoded or violated the protocol.
    /// 应用帧无法解码或违反协议。
    DecodeFailure,
}

/// A raw provider frame retained with the source identity and decode outcome.
/// 保留 provider 原始帧、来源身份及解码结果。
#[derive(Clone, Eq, PartialEq)]
pub struct RawMarketFrame {
    /// Provider identity such as `alpaca`.
    /// provider 身份，例如 `alpaca`。
    pub provider: String,
    /// Exact market feed identity such as `opra`.
    /// 精确行情 feed 身份，例如 `opra`。
    pub feed: String,
    /// Independently verified entitlement state; Alpaca stream currently reports unknown.
    /// 独立验证的 entitlement；当前 Alpaca stream 报告为 unknown。
    pub entitlement: EntitlementState,
    /// Homogeneous encoding evidence for market prices in this frame, if known.
    /// 若已知，则为本帧行情价格的统一编码证据。
    pub numeric_encoding: Option<NumericEncodingV1>,
    /// Canonical port generation shared with linked market-event envelopes.
    /// 与关联行情事件信封共享的 canonical port 代次。
    pub generation: u64,
    /// One-based frame sequence within the source generation.
    /// 来源代次内从 1 开始的帧序号。
    pub frame_sequence: u64,
    /// UTC time at which the complete application frame was received.
    /// 完整应用帧接收时的 UTC 时间。
    pub received_timestamp_utc: UtcTimestamp,
    /// Number of normalized quote/trade events contained in this frame.
    /// 本帧包含的规范化 quote/trade 事件数量。
    pub event_count: u32,
    /// Sorted unique symbols found in successfully decoded market messages in this frame.
    /// 本帧成功解码的行情消息中按 UTF-8 字典序排列的唯一 symbol。
    pub symbols: Vec<String>,
    /// Fixed decode or diagnostic outcome for the frame.
    /// 本帧固定的解码或诊断结果。
    pub disposition: RawFrameDisposition,
    /// Exact frame bytes, retained under the process-wide memory budget.
    /// 受进程级内存预算约束保留的精确帧字节。
    pub payload: RawFramePayload,
}

impl std::fmt::Debug for RawMarketFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawMarketFrame")
            .field("provider", &self.provider)
            .field("feed", &self.feed)
            .field("entitlement", &self.entitlement)
            .field("numeric_encoding", &self.numeric_encoding)
            .field("generation", &self.generation)
            .field("frame_sequence", &self.frame_sequence)
            .field("received_timestamp_utc", &self.received_timestamp_utc)
            .field("event_count", &self.event_count)
            .field("symbol_count", &self.symbols.len())
            .field("disposition", &self.disposition)
            .field("payload", &self.payload)
            .finish()
    }
}

/// Correlation from one canonical market event to its exact source frame.
/// 从一条规范化行情事件关联至其精确来源帧。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawFrameReference {
    /// Canonical port generation shared with the linked event envelope.
    /// 与关联事件信封共享的 canonical port 代次。
    pub generation: u64,
    /// One-based source frame sequence within the generation.
    /// 代次内从 1 开始的来源帧序号。
    pub frame_sequence: u64,
    /// One-based quote/trade event ordinal within the frame.
    /// 帧内从 1 开始的 quote/trade 事件序号。
    pub event_ordinal: u32,
    /// Number of normalized quote/trade events in the frame.
    /// 帧内规范化 quote/trade 事件总数。
    pub event_count: u32,
    /// Lowercase SHA-256 of the exact source frame bytes.
    /// 精确来源帧字节的小写 SHA-256。
    pub frame_sha256: String,
}

impl MarketDataSession {
    /// Creates a session from one bounded ordered lane and its cancellation signal.
    /// 使用单个有界有序队列和取消信号创建会话。
    #[must_use]
    pub fn new(records: mpsc::Receiver<MarketDataItem>, cancel: watch::Sender<bool>) -> Self {
        Self { records, cancel }
    }

    /// Requests graceful cancellation of this local subscription task.
    /// 请求优雅取消当前本地订阅任务。
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}

/// Read-only market-data provider interface.
/// 只读行情供应商接口。
pub trait MarketDataPort: Send + Sync {
    /// Starts one bounded provider subscription for the exact requested source.
    /// 为指定的精确来源启动一个有界供应商订阅。
    fn subscribe(
        &self,
        request: MarketDataSubscriptionRequest,
    ) -> PortFuture<'_, Result<MarketDataSession, BrokerPortError>>;
}

fn validate_source_id(value: &str) -> Result<(), BrokerPortError> {
    if value.is_empty()
        || value.len() > 128
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(BrokerPortError::InvalidRequest);
    }
    Ok(())
}

fn collect_symbols(
    symbols: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, BrokerPortError> {
    let mut values = Vec::new();
    for value in symbols {
        if values.len() == MAX_SUBSCRIPTION_INSTRUMENTS {
            return Err(BrokerPortError::LimitExceeded);
        }
        if value.is_empty()
            || value.len() > MAX_INSTRUMENT_ID_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(BrokerPortError::InvalidRequest);
        }
        values.push(value);
    }
    values.sort();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(BrokerPortError::InvalidRequest);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_request_sorts_symbols_and_preserves_exact_feed() {
        let request = MarketDataSubscriptionRequest::new(
            "alpaca",
            "opra",
            [
                "QQQ261218C00500000".to_owned(),
                "QQQ261218P00500000".to_owned(),
            ],
            ["QQQ261218C00500000".to_owned()],
        )
        .expect("valid candidate request");
        assert_eq!(request.provider(), "alpaca");
        assert_eq!(request.feed(), "opra");
        assert_eq!(request.quote_symbols()[0], "QQQ261218C00500000");
        assert_eq!(request.trade_symbols().len(), 1);
    }

    #[test]
    fn subscription_rejects_duplicates_empty_and_control_characters() {
        assert_eq!(
            MarketDataSubscriptionRequest::new(
                "alpaca",
                "sip",
                ["QQQ".to_owned(), "QQQ".to_owned()],
                []
            ),
            Err(BrokerPortError::InvalidRequest)
        );
        assert_eq!(
            MarketDataSubscriptionRequest::new("alpaca", "sip", [], []),
            Err(BrokerPortError::InvalidRequest)
        );
        assert_eq!(
            MarketDataSubscriptionRequest::new("alpaca", "sip", ["QQQ\n".to_owned()], []),
            Err(BrokerPortError::InvalidRequest)
        );
    }

    #[test]
    fn subscription_stops_consuming_an_unbounded_symbol_iterator_at_the_limit() {
        let request = MarketDataSubscriptionRequest::new(
            "alpaca",
            "sip",
            std::iter::repeat("QQQ".to_owned()),
            [],
        );
        assert_eq!(request, Err(BrokerPortError::LimitExceeded));
    }

    #[test]
    fn raw_frame_payload_is_bounded_and_debug_redacts_wire_bytes() {
        assert_eq!(
            RawFramePayload::capture(vec![0; MAX_RAW_FRAME_BYTES + 1]),
            Err(RawFrameCaptureError::Oversized)
        );

        let wire_bytes = b"synthetic-wire-body".to_vec();
        let payload = RawFramePayload::capture(wire_bytes.clone())
            .expect("small synthetic wire frame fits the process budget");
        assert_eq!(payload.as_bytes(), wire_bytes);
        let debug = format!("{payload:?}");
        assert!(debug.contains("len_bytes"));
        assert!(debug.contains(payload.sha256()));
        assert!(!debug.contains("synthetic-wire-body"));
    }
}
