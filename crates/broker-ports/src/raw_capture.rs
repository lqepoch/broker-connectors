//! Pre-decode capture and post-decode finalization contract for exact provider frames.
//!
//! A successful pre-decode acknowledgement is a promise made by the selected sink
//! implementation. The port cannot prove that the implementation actually flushed
//! its bytes or metadata. Production composition must therefore supply a reviewed,
//! trusted sink; a synthetic test sink is not evidence of durable storage.
//!
//! # 简体中文
//!
//! 本模块定义精确 provider frame 的解码前捕获与解码后定稿合同。
//!
//! 解码前 ACK 是所选 sink 实现作出的承诺，端口无法证明实现确实完成了数据和元数据持久化。
//! 生产组合必须注入经过审查的可信 sink；合成测试 sink 不能作为耐久存储证据。

use std::{fmt, sync::Arc};

use market_contracts::{EntitlementState, NumericEncodingV1, UtcTimestamp};
use sha2::{Digest, Sha256};

use crate::{
    MAX_INSTRUMENT_ID_BYTES, PortFuture, RawFrameDisposition, RawFramePayload, validate_source_id,
};

/// Maximum number of decoded market events or distinct symbols retained for one frame.
/// 单条 frame 最多保留的已解码行情事件数或唯一样本代码数。
pub const MAX_RAW_FRAME_FINALIZATION_ITEMS: usize = 512;

/// Opaque `UUIDv4` identity for one logical raw-capture subscription.
/// 一个逻辑原始捕获订阅的不透明 `UUIDv4` 身份。
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RawCaptureInstanceId([u8; 16]);

impl RawCaptureInstanceId {
    /// Constructs an ID from RFC 9562 `UUIDv4` bytes.
    /// 使用符合 RFC 9562 的 `UUIDv4` 字节构造身份。
    ///
    /// This checks the UUID version and variant bits. It does not prove that the
    /// bytes were generated randomly; the trusted sink factory owns that duty.
    /// 此方法只检查 UUID 版本和变体位，不证明字节由随机源生成；该职责属于可信 sink factory。
    ///
    /// # Errors
    ///
    /// Returns [`RawCaptureInstanceIdError::NotUuidV4`] if the version or variant bits do not match.
    ///
    /// # 错误
    ///
    /// 版本或变体位不匹配时返回 [`RawCaptureInstanceIdError::NotUuidV4`]。
    pub fn new(bytes: [u8; 16]) -> Result<Self, RawCaptureInstanceIdError> {
        if bytes[6] >> 4 != 4 || bytes[8] >> 6 != 2 {
            return Err(RawCaptureInstanceIdError::NotUuidV4);
        }
        Ok(Self(bytes))
    }

    /// Returns the fixed UUID bytes for a sink journal key.
    /// 返回供 sink 日志键使用的固定 UUID 字节。
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for RawCaptureInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RawCaptureInstanceId")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Exact identity key shared by a persisted frame, its pre-decode receipt, and its finalization.
/// 耐久帧、解码前回执及定稿共用的精确身份键。
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RawFrameCaptureKey {
    capture_instance_id: RawCaptureInstanceId,
    source_generation: u64,
    frame_sequence: u64,
    frame_sha256: String,
}

impl RawFrameCaptureKey {
    /// Reconstructs a bounded key from its persisted identity fields.
    /// 根据已持久化的身份字段重建有界 key。
    ///
    /// This key is correlation metadata only; constructing one does not create
    /// a capture request, an acknowledgement, or evidence of durability.
    /// 此 key 仅用于关联；构造 key 不会创建捕获请求、ACK 或耐久性证据。
    ///
    /// # Errors
    ///
    /// Returns a fixed error when a generation/sequence is zero or the hash is
    /// not exactly 64 lowercase hexadecimal characters.
    ///
    /// # 错误
    ///
    /// 代次/序号为零或摘要不是 64 位小写十六进制时返回固定错误。
    pub fn new(
        capture_instance_id: RawCaptureInstanceId,
        source_generation: u64,
        frame_sequence: u64,
        frame_sha256: impl Into<String>,
    ) -> Result<Self, RawFrameCaptureKeyError> {
        let frame_sha256 = frame_sha256.into();
        if source_generation == 0 || frame_sequence == 0 {
            return Err(RawFrameCaptureKeyError::InvalidSequence);
        }
        if !valid_sha256(&frame_sha256) {
            return Err(RawFrameCaptureKeyError::InvalidSha256);
        }
        Ok(Self {
            capture_instance_id,
            source_generation,
            frame_sequence,
            frame_sha256,
        })
    }

    fn from_capture(
        capture_instance_id: RawCaptureInstanceId,
        source_generation: u64,
        frame_sequence: u64,
        frame_sha256: String,
    ) -> Self {
        Self::new(
            capture_instance_id,
            source_generation,
            frame_sequence,
            frame_sha256,
        )
        .expect("validated capture identity and SHA-256 form a valid key")
    }

    /// Returns the logical capture-instance identity.
    /// 返回逻辑捕获实例身份。
    #[must_use]
    pub const fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.capture_instance_id
    }

    /// Returns the adapter-local generation recorded before canonical projection.
    /// 返回 canonical 投影前记录的 adapter 本地代次。
    #[must_use]
    pub const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    /// Returns the one-based frame sequence within the source generation.
    /// 返回来源代次内从 1 开始的帧序号。
    #[must_use]
    pub const fn frame_sequence(&self) -> u64 {
        self.frame_sequence
    }

    /// Returns the lowercase SHA-256 of the exact source bytes.
    /// 返回精确来源字节的小写 SHA-256。
    #[must_use]
    pub fn frame_sha256(&self) -> &str {
        &self.frame_sha256
    }
}

/// Invalid persisted identity fields for [`RawFrameCaptureKey`].
/// [`RawFrameCaptureKey`] 的持久化身份字段无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameCaptureKeyError {
    /// The source generation or frame sequence is zero.
    /// 来源代次或帧序号为零。
    InvalidSequence,
    /// The SHA-256 is not lowercase hexadecimal with exactly 64 bytes.
    /// SHA-256 不是恰好 64 字节的小写十六进制值。
    InvalidSha256,
}

impl fmt::Debug for RawFrameCaptureKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawFrameCaptureKey")
            .field("capture_instance_id", &self.capture_instance_id)
            .field("source_generation", &self.source_generation)
            .field("frame_sequence", &self.frame_sequence)
            .field("frame_sha256", &self.frame_sha256)
            .finish()
    }
}

/// Invalid capture-instance identifier category.
/// 捕获实例身份无效的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawCaptureInstanceIdError {
    /// The value does not use the `UUIDv4` version and RFC variant bits.
    /// 值不符合 `UUIDv4` 版本和 RFC 变体位。
    NotUuidV4,
}

/// Wire encoding of an exact provider application frame.
/// Provider 应用 frame 的 wire 编码格式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RawFrameWireEncoding {
    /// JSON application bytes.
    /// JSON 应用字节。
    Json,
    /// `MessagePack` application bytes.
    /// `MessagePack` 应用字节。
    MessagePack,
    /// The adapter does not know the application encoding.
    /// Adapter 不知道应用编码格式。
    Unknown,
}

/// Immutable identity and exact bytes presented to a raw sink before decoding.
/// 解码前提交给 raw sink 的不可变身份与精确字节。
#[derive(Clone, Eq, PartialEq)]
pub struct RawFrameCapture {
    capture_key: RawFrameCaptureKey,
    provider: String,
    feed: String,
    entitlement: EntitlementState,
    received_timestamp_utc: UtcTimestamp,
    wire_encoding: RawFrameWireEncoding,
    payload: RawFramePayload,
}

impl RawFrameCapture {
    /// Creates a bounded, validated pre-decode capture request.
    /// 创建有界且已校验的解码前捕获请求。
    ///
    /// The capture ID is created by the trusted sink factory and remains stable
    /// across reconnect generations for one logical subscription. A restart or a
    /// new logical subscription uses a new `UUIDv4`.
    /// 捕获 ID 由可信 sink factory 创建；同一逻辑订阅重连时保持不变，进程重启或新逻辑订阅应创建新 `UUIDv4`。
    /// # Errors
    ///
    /// Returns a fixed error when a source field is invalid or an identity sequence is zero.
    ///
    /// # 错误
    ///
    /// 来源字段无效或身份序号为零时返回固定错误。
    #[allow(clippy::too_many_arguments)] // These validated fields are the complete immutable capture header.
    pub fn new(
        capture_instance_id: RawCaptureInstanceId,
        provider: impl Into<String>,
        feed: impl Into<String>,
        entitlement: EntitlementState,
        source_generation: u64,
        frame_sequence: u64,
        received_timestamp_utc: UtcTimestamp,
        wire_encoding: RawFrameWireEncoding,
        payload: RawFramePayload,
    ) -> Result<Self, RawFrameCaptureRequestError> {
        let provider = provider.into();
        let feed = feed.into();
        validate_source_id(&provider).map_err(|_| RawFrameCaptureRequestError::InvalidSource)?;
        validate_source_id(&feed).map_err(|_| RawFrameCaptureRequestError::InvalidSource)?;
        if source_generation == 0 || frame_sequence == 0 {
            return Err(RawFrameCaptureRequestError::InvalidSequence);
        }
        Ok(Self {
            capture_key: RawFrameCaptureKey::from_capture(
                capture_instance_id,
                source_generation,
                frame_sequence,
                payload.sha256().to_owned(),
            ),
            provider,
            feed,
            entitlement,
            received_timestamp_utc,
            wire_encoding,
            payload,
        })
    }

    /// Returns the logical capture identity.
    /// 返回逻辑捕获身份。
    #[must_use]
    pub const fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.capture_key.capture_instance_id
    }

    /// Returns the exact provider identifier.
    /// 返回精确的 provider 标识。
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

    /// Returns the independently observed entitlement classification.
    /// 返回独立观察到的 entitlement 分类。
    #[must_use]
    pub const fn entitlement(&self) -> EntitlementState {
        self.entitlement
    }

    /// Returns the source-local connection generation, before canonical projection.
    /// 返回 canonical 投影前的来源本地连接代次。
    #[must_use]
    pub const fn source_generation(&self) -> u64 {
        self.capture_key.source_generation
    }

    /// Returns the one-based frame sequence within this generation.
    /// 返回该代次内从 1 开始的 frame 序号。
    #[must_use]
    pub const fn frame_sequence(&self) -> u64 {
        self.capture_key.frame_sequence
    }

    /// Returns the shared source identity and exact byte hash for this frame.
    /// 返回此帧共用的来源身份和精确字节摘要。
    #[must_use]
    pub const fn capture_key(&self) -> &RawFrameCaptureKey {
        &self.capture_key
    }

    /// Returns the UTC receive timestamp captured before decoding.
    /// 返回解码前记录的 UTC 接收时间。
    #[must_use]
    pub const fn received_timestamp_utc(&self) -> &UtcTimestamp {
        &self.received_timestamp_utc
    }

    /// Returns the wire encoding known at receipt time.
    /// 返回接收时已知的 wire 编码。
    #[must_use]
    pub const fn wire_encoding(&self) -> RawFrameWireEncoding {
        self.wire_encoding
    }

    /// Returns the exact bounded frame payload.
    /// 返回有界的精确 frame payload。
    #[must_use]
    pub const fn payload(&self) -> &RawFramePayload {
        &self.payload
    }
}

impl fmt::Debug for RawFrameCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawFrameCapture")
            .field("capture_key", &self.capture_key)
            .field("provider", &self.provider)
            .field("feed", &self.feed)
            .field("entitlement", &self.entitlement)
            .field("received_timestamp_utc", &self.received_timestamp_utc)
            .field("wire_encoding", &self.wire_encoding)
            .field("payload", &self.payload)
            .finish()
    }
}

/// Invalid field category for a pre-decode capture request.
/// 解码前捕获请求字段无效的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameCaptureRequestError {
    /// Provider or feed identifier is empty, oversized, padded, or contains controls.
    /// Provider 或 feed 标识为空、过长、含首尾空白或控制字符。
    InvalidSource,
    /// Generation and frame sequence must both be positive.
    /// generation 与 frame sequence 都必须为正数。
    InvalidSequence,
}

/// Shared decode summary supplied to the sink after a frame has been classified.
/// Frame 分类后的共享解码摘要，供 sink 定稿。
#[derive(Clone, Eq, PartialEq)]
pub struct RawFrameFinalization {
    event_count: u32,
    symbols: Vec<String>,
    numeric_encoding: Option<NumericEncodingV1>,
    disposition: RawFrameDisposition,
}

impl RawFrameFinalization {
    /// Creates a bounded finalization summary with sorted unique symbol identifiers.
    /// 创建有界的定稿摘要，并要求 symbol 标识按字典序唯一排列。
    /// # Errors
    ///
    /// Returns a fixed error when the summary is invalid or exceeds its fixed bounds.
    ///
    /// # 错误
    ///
    /// 摘要无效或超过固定上限时返回固定错误。
    pub fn new(
        event_count: u32,
        symbols: Vec<String>,
        numeric_encoding: Option<NumericEncodingV1>,
        disposition: RawFrameDisposition,
    ) -> Result<Self, RawFrameFinalizationError> {
        if usize::try_from(event_count).map_err(|_| RawFrameFinalizationError::LimitExceeded)?
            > MAX_RAW_FRAME_FINALIZATION_ITEMS
            || symbols.len() > MAX_RAW_FRAME_FINALIZATION_ITEMS
        {
            return Err(RawFrameFinalizationError::LimitExceeded);
        }
        if symbols.iter().any(|symbol| {
            symbol.is_empty()
                || symbol.len() > MAX_INSTRUMENT_ID_BYTES
                || symbol.chars().any(char::is_control)
        }) || symbols.windows(2).any(|pair| pair[0] >= pair[1])
            || event_count == 0 && (!symbols.is_empty() || numeric_encoding.is_some())
            || event_count > 0 && symbols.is_empty()
            || event_count > 0 && symbols.len() > usize::try_from(event_count).unwrap_or(usize::MAX)
            || matches!(disposition, RawFrameDisposition::DecodedMarketData) && event_count == 0
            || matches!(numeric_encoding, Some(NumericEncodingV1::Unspecified))
            || matches!(
                disposition,
                RawFrameDisposition::ControlMessage | RawFrameDisposition::DecodeFailure
            ) && (event_count != 0 || !symbols.is_empty() || numeric_encoding.is_some())
        {
            return Err(RawFrameFinalizationError::InvalidSummary);
        }
        Ok(Self {
            event_count,
            symbols,
            numeric_encoding,
            disposition,
        })
    }

    /// Returns the number of normalized quote/trade events expected from the frame.
    /// 返回该 frame 预期产生的规范化 quote/trade 事件数。
    #[must_use]
    pub const fn event_count(&self) -> u32 {
        self.event_count
    }

    /// Returns sorted unique symbols observed while decoding.
    /// 返回解码时观察到的按字典序排列的唯一 symbol。
    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Returns the homogeneous numeric encoding, if one was established.
    /// 若已确定统一数值编码，则返回该编码。
    #[must_use]
    pub const fn numeric_encoding(&self) -> Option<NumericEncodingV1> {
        self.numeric_encoding
    }

    /// Returns the fixed decode disposition.
    /// 返回固定解码 disposition。
    #[must_use]
    pub const fn disposition(&self) -> RawFrameDisposition {
        self.disposition
    }

    /// Computes the domain-separated hash bound by a post-decode ACK.
    /// 计算由解码后 ACK 绑定的独立域摘要。
    #[must_use]
    fn sha256(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"lqepoch.raw-frame-finalization.v1\0");
        hasher.update(self.event_count.to_be_bytes());
        hasher.update([disposition_tag(self.disposition)]);
        hasher.update([numeric_encoding_tag(self.numeric_encoding)]);
        hasher.update(
            u32::try_from(self.symbols.len())
                .expect("symbol count is bounded")
                .to_be_bytes(),
        );
        for symbol in &self.symbols {
            let bytes = symbol.as_bytes();
            hasher.update(
                u32::try_from(bytes.len())
                    .expect("symbol bytes are bounded")
                    .to_be_bytes(),
            );
            hasher.update(bytes);
        }
        hex_sha256(hasher.finalize().as_slice())
    }
}

impl fmt::Debug for RawFrameFinalization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawFrameFinalization")
            .field("event_count", &self.event_count)
            .field("symbol_count", &self.symbols.len())
            .field("numeric_encoding", &self.numeric_encoding)
            .field("disposition", &self.disposition)
            .finish()
    }
}

/// Invalid or oversized post-decode summary category.
/// 解码后摘要无效或超限的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameFinalizationError {
    /// The summary exceeded a fixed field or collection bound.
    /// 摘要超过固定字段或集合上限。
    LimitExceeded,
    /// Symbols were invalid, duplicate, or not sorted, or the summary was malformed.
    /// Symbol 无效、重复、未排序或摘要格式错误。
    InvalidSummary,
}

/// Receipt returned only after the sink acknowledges the pre-decode capture intent.
/// Sink 确认解码前捕获意图后返回的回执。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawFrameCaptureAck {
    capture_key: RawFrameCaptureKey,
}

impl RawFrameCaptureAck {
    /// Constructs an ACK value for one capture identity.
    /// 为一条捕获身份构造 ACK 值。
    ///
    /// This constructor does not perform I/O or prove durability. A sink may use
    /// it only after honoring [`RawFrameSink::persist_before_decode`].
    /// 此构造函数不执行 I/O，也不证明持久性。Sink 只有履行 [`RawFrameSink::persist_before_decode`] 后才可使用。
    #[must_use]
    pub fn for_capture(capture: &RawFrameCapture) -> Self {
        Self {
            capture_key: capture.capture_key.clone(),
        }
    }

    /// Returns whether the ACK binds the exact capture identity and byte hash.
    /// 判断 ACK 是否绑定精确捕获身份和原始字节摘要。
    #[must_use]
    pub fn matches(&self, capture: &RawFrameCapture) -> bool {
        self.capture_key == capture.capture_key
    }

    /// Returns the captured `UUIDv4` identity.
    /// 返回捕获 `UUIDv4` 身份。
    #[must_use]
    pub const fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.capture_key.capture_instance_id
    }

    /// Returns the acknowledged source-local generation.
    /// 返回已确认的来源本地代次。
    #[must_use]
    pub const fn source_generation(&self) -> u64 {
        self.capture_key.source_generation
    }

    /// Returns the acknowledged one-based frame sequence.
    /// 返回已确认的 frame 序号。
    #[must_use]
    pub const fn frame_sequence(&self) -> u64 {
        self.capture_key.frame_sequence
    }

    /// Returns the lowercase SHA-256 of the exact frame bytes.
    /// 返回精确 frame 字节的小写 SHA-256。
    #[must_use]
    pub fn frame_sha256(&self) -> &str {
        &self.capture_key.frame_sha256
    }

    /// Returns the exact key acknowledged by the sink.
    /// 返回 sink 已确认的精确身份键。
    #[must_use]
    pub const fn capture_key(&self) -> &RawFrameCaptureKey {
        &self.capture_key
    }
}

/// Receipt returned only after a matching post-decode summary is durably finalized.
/// 匹配的解码后摘要完成持久化定稿后返回的回执。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawFrameFinalizationAck {
    capture_key: RawFrameCaptureKey,
    summary_sha256: String,
}

impl RawFrameFinalizationAck {
    /// Constructs a finalization ACK value for the matching pre-decode receipt and summary.
    /// 为匹配的解码前回执和摘要构造定稿 ACK 值。
    ///
    /// This constructor does not perform I/O or prove durability. A sink may use
    /// it only after honoring [`RawFrameSink::finalize_after_decode`].
    /// 此构造函数不执行 I/O，也不证明持久性。Sink 只有履行 [`RawFrameSink::finalize_after_decode`] 后才可使用。
    #[must_use]
    pub fn for_finalization(
        predecode_ack: &RawFrameCaptureAck,
        summary: &RawFrameFinalization,
    ) -> Self {
        Self {
            capture_key: predecode_ack.capture_key.clone(),
            summary_sha256: summary.sha256(),
        }
    }

    /// Returns whether this ACK matches the pre-decode identity and exact summary hash.
    /// 判断 ACK 是否匹配解码前身份及精确摘要哈希。
    #[must_use]
    pub fn matches(
        &self,
        predecode_ack: &RawFrameCaptureAck,
        summary: &RawFrameFinalization,
    ) -> bool {
        self.capture_key == predecode_ack.capture_key && self.summary_sha256 == summary.sha256()
    }

    /// Returns the exact capture identity carried through finalization.
    /// 返回定稿阶段继续绑定的精确捕获身份。
    #[must_use]
    pub const fn capture_key(&self) -> &RawFrameCaptureKey {
        &self.capture_key
    }

    /// Returns the lowercase SHA-256 of the acknowledged finalization summary.
    /// 返回已确认定稿摘要的小写 SHA-256。
    #[must_use]
    pub fn summary_sha256(&self) -> &str {
        &self.summary_sha256
    }
}

/// Fixed error categories for a raw sink; implementations must not return provider or payload text.
/// Raw sink 的固定错误类别；实现不得返回 provider 或 payload 原文。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawFrameSinkError {
    /// The bounded queue, byte budget, or disk quota is full.
    /// 有界队列、字节预算或磁盘额度已满。
    CapacityExceeded,
    /// The sink is unavailable or a write failed.
    /// Sink 不可用或写入失败。
    Unavailable,
    /// The sink cannot determine whether the operation became durable.
    /// Sink 无法确认该操作是否已持久化。
    Ambiguous,
    /// The sink was cancelled before returning a matching acknowledgement.
    /// Sink 在返回匹配 ACK 前被取消。
    Cancelled,
    /// The capture or generation has already been poisoned.
    /// 当前捕获或代次已被毒化。
    Poisoned,
}

/// Trusted asynchronous boundary for durable pre-decode capture and finalization.
/// 用于解码前耐久捕获和解码后定稿的可信异步边界。
///
/// One sink instance serves exactly one logical subscription. Its `UUIDv4` remains
/// stable across reconnect generations; a process restart or new logical
/// subscription must use a new ID. `persist_before_decode` must not acknowledge
/// until both exact bytes and recovery identity metadata are locally durable.
/// `finalize_after_decode` must require a matching pre-decode receipt and durably
/// bind the summary before returning. Neither acknowledgement proves Drive
/// upload, entitlement, provider completeness, or economic correctness.
///
/// 一个 sink 实例只服务一个逻辑订阅。其 `UUIDv4` 在重连代次间保持稳定；进程重启或新订阅必须使用新 ID。
/// `persist_before_decode` 只有在精确字节和恢复身份元数据均已本地持久化后才可 ACK。
/// `finalize_after_decode` 必须验证匹配的解码前回执，并在返回前持久化绑定摘要。两种 ACK 均不证明
/// Drive 上传、entitlement、provider 完整性或经济值真实性。
pub trait RawFrameSink: Send + Sync {
    /// Returns this sink's `UUIDv4` for its single logical capture subscription.
    /// 返回该 sink 单一逻辑捕获订阅的 `UUIDv4`。
    fn capture_instance_id(&self) -> RawCaptureInstanceId;

    /// Persists exact bytes and recovery identity metadata before the adapter decodes them.
    /// 在 adapter 解码前持久化精确字节和恢复身份元数据。
    fn persist_before_decode<'a>(
        &'a self,
        capture: &'a RawFrameCapture,
    ) -> PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>>;

    /// Durably binds the shared decode summary to its matching pre-decode receipt.
    /// 将共享解码摘要耐久绑定到匹配的解码前回执。
    fn finalize_after_decode<'a>(
        &'a self,
        predecode_ack: &'a RawFrameCaptureAck,
        summary: &'a RawFrameFinalization,
    ) -> PortFuture<'a, Result<RawFrameFinalizationAck, RawFrameSinkError>>;
}

/// Creates one raw sink per logical subscription.
/// 为每个逻辑订阅创建一个独立 raw sink。
///
/// The factory owns capture-instance identity creation. The returned sink keeps
/// that identity stable across reconnect generations; a process restart creates
/// a new sink and therefore a new identity. Implementations should keep creation
/// bounded and must not load or expose provider credentials.
/// Factory 负责创建捕获实例身份。返回的 sink 在重连代次间保持该身份不变；进程重启时创建新 sink，
/// 因此也应使用新身份。实现应限制创建开销，且不得读取或暴露 provider 凭证。
pub trait RawFrameSinkFactory: Send + Sync {
    /// Creates a sink for one validated provider/feed pair.
    /// 为一组已校验的 provider/feed 创建单独 sink。
    ///
    /// # Errors
    ///
    /// Returns a fixed [`RawFrameSinkError`] category when the sink cannot be created.
    ///
    /// # 错误
    ///
    /// 无法创建 sink 时返回固定类别 [`RawFrameSinkError`]。
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> Result<Arc<dyn RawFrameSink>, RawFrameSinkError>;
}

fn disposition_tag(disposition: RawFrameDisposition) -> u8 {
    match disposition {
        RawFrameDisposition::DecodedMarketData => 1,
        RawFrameDisposition::ControlMessage => 2,
        RawFrameDisposition::UnknownMessage => 3,
        RawFrameDisposition::ProviderError => 4,
        RawFrameDisposition::DecodeFailure => 5,
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn numeric_encoding_tag(encoding: Option<NumericEncodingV1>) -> u8 {
    match encoding {
        None => 0,
        Some(NumericEncodingV1::Unspecified) => 1,
        Some(NumericEncodingV1::DecimalToken) => 2,
        Some(NumericEncodingV1::IntegerToken) => 3,
        Some(NumericEncodingV1::BinaryFloat32ShortestDecimal) => 4,
        Some(NumericEncodingV1::BinaryFloat64ShortestDecimal) => 5,
        Some(NumericEncodingV1::RawMessagePackBytes) => 6,
        Some(NumericEncodingV1::RawJsonBytes) => 7,
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing into a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use market_contracts::{EntitlementState, NumericEncodingV1, UtcTimestamp};

    use crate::{RawFramePayload, RawFrameWireEncoding};

    use super::{
        RawCaptureInstanceId, RawCaptureInstanceIdError, RawFrameCapture, RawFrameCaptureAck,
        RawFrameCaptureKey, RawFrameCaptureKeyError, RawFrameCaptureRequestError,
        RawFrameDisposition, RawFrameFinalization, RawFrameFinalizationAck,
        RawFrameFinalizationError, numeric_encoding_tag,
    };

    fn capture_id(fill: u8) -> RawCaptureInstanceId {
        let mut bytes = [fill; 16];
        bytes[6] = 0x40 | (bytes[6] & 0x0f);
        bytes[8] = 0x80 | (bytes[8] & 0x3f);
        RawCaptureInstanceId::new(bytes).expect("synthetic UUIDv4 bytes")
    }

    fn capture(generation: u64, sequence: u64, payload_bytes: &[u8]) -> RawFrameCapture {
        let timestamp =
            UtcTimestamp::parse("2026-10-08T12:00:00.000000000Z").expect("valid UTC timestamp");
        let payload = RawFramePayload::capture(payload_bytes.to_vec()).expect("bounded payload");
        RawFrameCapture::new(
            capture_id(1),
            "alpaca",
            "opra",
            EntitlementState::Unknown,
            generation,
            sequence,
            timestamp,
            RawFrameWireEncoding::MessagePack,
            payload,
        )
        .expect("valid capture")
    }

    #[test]
    fn capture_id_requires_uuid_v4_version_and_rfc_variant() {
        assert!(capture_id(1).as_bytes()[..] != [0; 16]);
        assert_eq!(
            RawCaptureInstanceId::new([0; 16]),
            Err(RawCaptureInstanceIdError::NotUuidV4)
        );
    }

    #[test]
    fn predecode_ack_binds_capture_generation_sequence_and_exact_hash() {
        let original = capture(3, 9, b"synthetic-msgpack-frame");
        let ack = RawFrameCaptureAck::for_capture(&original);
        assert!(ack.matches(&original));
        assert!(!ack.matches(&capture(4, 9, b"synthetic-msgpack-frame")));
        assert!(!ack.matches(&capture(3, 10, b"synthetic-msgpack-frame")));
        assert!(!ack.matches(&capture(3, 9, b"different-frame")));
    }

    #[test]
    fn finalization_ack_binds_the_stable_summary_hash() {
        let capture = capture(1, 1, b"synthetic-msgpack-frame");
        let predecode_ack = RawFrameCaptureAck::for_capture(&capture);
        let summary = RawFrameFinalization::new(
            2,
            vec![
                "AAPL250117C00100000".to_owned(),
                "AAPL250117P00100000".to_owned(),
            ],
            Some(NumericEncodingV1::IntegerToken),
            RawFrameDisposition::DecodedMarketData,
        )
        .expect("valid summary");
        let final_ack = RawFrameFinalizationAck::for_finalization(&predecode_ack, &summary);
        assert!(final_ack.matches(&predecode_ack, &summary));
        assert!(
            !final_ack.matches(
                &predecode_ack,
                &RawFrameFinalization::new(
                    1,
                    vec!["AAPL250117C00100000".to_owned()],
                    Some(NumericEncodingV1::IntegerToken),
                    RawFrameDisposition::DecodedMarketData,
                )
                .expect("valid changed summary")
            )
        );
    }

    #[test]
    fn raw_json_and_messagepack_finalizations_have_distinct_stable_tags() {
        assert_eq!(numeric_encoding_tag(None), 0);
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::Unspecified)),
            1
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::DecimalToken)),
            2
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::IntegerToken)),
            3
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::BinaryFloat32ShortestDecimal)),
            4
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::BinaryFloat64ShortestDecimal)),
            5
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::RawMessagePackBytes)),
            6
        );
        assert_eq!(
            numeric_encoding_tag(Some(NumericEncodingV1::RawJsonBytes)),
            7
        );
    }

    #[test]
    fn finalization_rejects_unsorted_duplicate_or_oversized_symbols() {
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["Z".to_owned(), "A".to_owned()],
                None,
                RawFrameDisposition::UnknownMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["A".to_owned(), "A".to_owned()],
                None,
                RawFrameDisposition::UnknownMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["A".repeat(super::MAX_INSTRUMENT_ID_BYTES + 1)],
                None,
                RawFrameDisposition::UnknownMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
    }

    #[test]
    fn finalization_enforces_quarantine_and_mixed_numeric_summary_rules() {
        assert!(
            RawFrameFinalization::new(
                1,
                vec!["AAPL250117C00100000".to_owned()],
                None,
                RawFrameDisposition::DecodedMarketData,
            )
            .is_ok(),
            "mixed numeric encodings remain valid with no homogeneous encoding"
        );
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["AAPL250117C00100000".to_owned()],
                Some(NumericEncodingV1::Unspecified),
                RawFrameDisposition::DecodedMarketData,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["AAPL250117C00100000".to_owned()],
                Some(NumericEncodingV1::IntegerToken),
                RawFrameDisposition::DecodeFailure,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
        assert_eq!(
            RawFrameFinalization::new(
                0,
                Vec::new(),
                Some(NumericEncodingV1::IntegerToken),
                RawFrameDisposition::ControlMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
    }

    #[test]
    fn capture_key_keeps_source_generation_when_reconnect_reuses_sequence_and_bytes() {
        let first = capture(10, 1, b"same-synthetic-frame");
        let second = capture(11, 1, b"same-synthetic-frame");

        assert_eq!(
            first.capture_key().capture_instance_id(),
            second.capture_key().capture_instance_id()
        );
        assert_eq!(
            first.capture_key().frame_sequence(),
            second.capture_key().frame_sequence()
        );
        assert_eq!(
            first.capture_key().frame_sha256(),
            second.capture_key().frame_sha256()
        );
        assert_eq!(first.capture_key().source_generation(), 10);
        assert_eq!(second.capture_key().source_generation(), 11);
        assert_ne!(first.capture_key(), second.capture_key());
    }

    #[test]
    fn capture_key_reconstruction_rejects_unbounded_or_invalid_identity_fields() {
        let id = capture_id(4);
        assert_eq!(
            RawFrameCaptureKey::new(id, 0, 1, "a".repeat(64)),
            Err(RawFrameCaptureKeyError::InvalidSequence)
        );
        assert_eq!(
            RawFrameCaptureKey::new(id, 1, 0, "a".repeat(64)),
            Err(RawFrameCaptureKeyError::InvalidSequence)
        );
        assert_eq!(
            RawFrameCaptureKey::new(id, 1, 1, "A".repeat(64)),
            Err(RawFrameCaptureKeyError::InvalidSha256)
        );
        assert_eq!(
            RawFrameCaptureKey::new(id, 1, 1, "a".repeat(65)),
            Err(RawFrameCaptureKeyError::InvalidSha256)
        );
        let reconstructed = RawFrameCaptureKey::new(id, 1, 1, "a".repeat(64))
            .expect("valid persisted identity fields reconstruct a correlation key");
        assert_eq!(reconstructed.capture_instance_id(), id);
        assert_eq!(reconstructed.source_generation(), 1);
        assert_eq!(reconstructed.frame_sequence(), 1);
        assert_eq!(reconstructed.frame_sha256(), "a".repeat(64));
    }

    #[test]
    fn control_finalization_requires_no_events_or_symbols() {
        let control =
            RawFrameFinalization::new(0, Vec::new(), None, RawFrameDisposition::ControlMessage)
                .expect("subscription acknowledgement is a control frame");
        assert_eq!(control.disposition(), RawFrameDisposition::ControlMessage);
        assert_eq!(
            RawFrameFinalization::new(
                1,
                vec!["AAPL250117C00100000".to_owned()],
                None,
                RawFrameDisposition::ControlMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
        assert_eq!(
            RawFrameFinalization::new(
                0,
                vec!["AAPL250117C00100000".to_owned()],
                None,
                RawFrameDisposition::ControlMessage,
            ),
            Err(RawFrameFinalizationError::InvalidSummary)
        );
    }

    #[test]
    fn capture_request_rejects_zero_generation_or_sequence() {
        let timestamp = UtcTimestamp::parse("2026-10-08T12:00:00Z").expect("valid UTC timestamp");
        let payload = RawFramePayload::capture(b"synthetic".to_vec()).expect("bounded payload");
        assert_eq!(
            RawFrameCapture::new(
                capture_id(2),
                "alpaca",
                "opra",
                EntitlementState::Unknown,
                0,
                1,
                timestamp.clone(),
                RawFrameWireEncoding::MessagePack,
                payload.clone(),
            ),
            Err(RawFrameCaptureRequestError::InvalidSequence)
        );
        assert_eq!(
            RawFrameCapture::new(
                capture_id(2),
                "alpaca",
                "opra",
                EntitlementState::Unknown,
                1,
                0,
                timestamp,
                RawFrameWireEncoding::MessagePack,
                payload,
            ),
            Err(RawFrameCaptureRequestError::InvalidSequence)
        );
    }

    #[test]
    fn debug_output_does_not_include_raw_payload_bytes() {
        let capture = capture(1, 1, b"never-log-this-synthetic-raw-payload");
        assert!(!format!("{capture:?}").contains("never-log-this-synthetic-raw-payload"));
        assert!(format!("{capture:?}").contains("[REDACTED]"));
    }
}
