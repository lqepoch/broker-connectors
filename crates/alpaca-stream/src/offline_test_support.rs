//! Default-off in-process replay of one reviewed synthetic Alpaca protocol fixture.
//!
//! Enable `offline-test-support` only in local offline capture tooling. This module owns no
//! network connector, credential input, arbitrary fixture input, or production fallback. It
//! drives the regular Alpaca session runner and market projector through an in-process scripted
//! socket. The local `FixtureEnd` is test control-plane state, not an Alpaca protocol watermark.
//!
//! 仅用于离线测试工具的默认关闭、进程内合成 Alpaca 协议回放。
//!
//! 只有本地离线采集工具可以启用 `offline-test-support`。本模块不拥有网络 connector、外部凭证、
//! 任意 fixture 输入或生产 fallback；它通过进程内脚本 socket 驱动正式 Alpaca session runner
//! 和 market projector。`FixtureEnd` 仅属于本地测试控制面，不是 Alpaca 协议水位。

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use broker_ports::{
    MarketDataItem, PortFuture, RawFrameCapture, RawFrameCaptureAck, RawFrameCaptureKey,
    RawFrameDisposition, RawFrameFinalization, RawFrameFinalizationAck, RawFrameSink,
    RawFrameSinkError, RawFrameSinkFactory, RawFrameWireEncoding,
};
use market_contracts::EntitlementState;
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, watch};
use tokio::time;
use zeroize::Zeroizing;

use crate::config::{
    DesiredSubscriptions, OptionContractSymbol, OptionFeed, StreamConfig, StreamEnvironment,
    StreamLimits,
};
use crate::credentials::{AlpacaCredentials, CredentialFailure, CredentialProvider};
use crate::lane::{LaneReceivers, create_lanes};
use crate::market_port::{EventProjector, project_update};
use crate::session::{SessionExit, StreamError, run_fixture_session};
use crate::state::ReconnectPolicy;
use crate::transport::{ConnectFailure, SocketConnector, SocketFailure, SocketFrame, StreamSocket};
use crate::update::StreamUpdate;

const FIXTURE_SYMBOL: &str = "AAPL270115C00150000";
const FIXTURE_KEY_ID: &str = "offline-fixture-key";
const FIXTURE_SECRET: &str = "offline-fixture-secret";
const FIXTURE_FRAME_COUNT: usize = 4;
const FIXTURE_FRAME_LIMIT: usize = 1_024;
const FIXTURE_BYTES_LIMIT: usize = 8 * 1_024;
const CAPTURE_FRAME_COUNT: usize = 2;
const CAPTURE_BYTES_LIMIT: usize = 4 * 1_024;
const RESULT_ITEM_LIMIT: usize = 64;
const RUN_DEADLINE: Duration = Duration::from_secs(15);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(2);
const EXPECTED_ENDPOINT: &str = "wss://stream.data.sandbox.alpaca.markets/v1beta1/opra";
const FIXTURE_TIMESTAMP_UNIX_SECONDS: u64 = 1_791_460_800;

// Updated only when the reviewed fixture bytes intentionally change. MDP pins this value in its
// LocalTest publisher policy so a source edit cannot silently replace the accepted fixture.
const FIXTURE_SHA256: &str = "852850f472e4434267bb9d853234fbbced201e229b57d9d1f3c5a0c914818c8c";

/// The only fixture that the local offline seam is allowed to replay.
/// 离线 seam 唯一允许回放的 fixture。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReviewedFixtureId {
    /// A `MessagePack` connected/authenticated/subscription-ACK/trade transcript.
    /// 一组 `MessagePack` connected/authenticated/subscription-ACK/trade 协议帧。
    AlpacaOpraTradeV1,
}

impl ReviewedFixtureId {
    /// Returns the stable manifest identifier for this fixture.
    /// 返回此 fixture 稳定的 manifest 标识。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlpacaOpraTradeV1 => "alpaca-opra-trade-v1",
        }
    }

    /// Returns the pinned SHA-256 input seal for the exact provider frames.
    /// 返回精确 provider 帧输入封印的固定 SHA-256。
    #[must_use]
    pub const fn expected_sha256(self) -> &'static str {
        match self {
            Self::AlpacaOpraTradeV1 => FIXTURE_SHA256,
        }
    }
}

/// Local test-control terminal state, separate from provider ACKs and watermarks.
/// 与 provider ACK 和水位分离的本地测试控制面终止状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineFixtureTerminal {
    /// The scripted in-process transport consumed the fixed input and emitted its local marker.
    /// 进程内脚本传输已消费固定输入并发出本地 marker。
    FixtureEnd,
}

/// Bounded projected records and the SDK-created receipt for one exact fixture run.
/// 一次精确 fixture 回放产生的有界投影记录和 SDK 构造的回执。
pub struct OfflineFixtureCapture {
    items: Vec<MarketDataItem>,
    receipt: OfflineFixtureReceipt,
}

impl OfflineFixtureCapture {
    /// Borrows the provider-neutral records emitted by the existing Alpaca projector.
    /// 借用现有 Alpaca projector 产生的 provider-neutral 记录。
    #[must_use]
    pub fn items(&self) -> &[MarketDataItem] {
        &self.items
    }

    /// Borrows the receipt that only this SDK run can construct.
    /// 借用仅此 SDK 回放可以构造的回执。
    #[must_use]
    pub const fn receipt(&self) -> &OfflineFixtureReceipt {
        &self.receipt
    }

    /// Consumes the result into projected records and its unforgeable-in-crate receipt.
    /// 消费结果并取出投影记录及 crate 内不可伪造的回执。
    #[must_use]
    pub fn into_parts(self) -> (Vec<MarketDataItem>, OfflineFixtureReceipt) {
        (self.items, self.receipt)
    }
}

/// Private-field receipt binding the fixed input to runner reads and matching two-phase ACKs.
/// 私有字段回执，将固定输入、runner 接收记录和匹配的两阶段 ACK 绑定在一起。
pub struct OfflineFixtureReceipt {
    fixture_id: ReviewedFixtureId,
    fixture_sha256: String,
    fixture_freshness_clock_unix_seconds: u64,
    terminal_state: OfflineFixtureTerminal,
    script_frame_count: u32,
    runner_received_frame_count: u32,
    runner_received_digest_sha256: String,
    captured_frame_count: u32,
    raw_market_frame_count: u32,
    predecode_ack_count: u32,
    finalization_ack_count: u32,
    captured_bytes: u64,
    ordered_raw_frames_sha256: String,
    finalization_rollup_sha256: String,
    output_item_count: u32,
}

impl OfflineFixtureReceipt {
    /// Returns the fixed reviewed fixture identifier.
    /// 返回固定的已审阅 fixture 标识。
    #[must_use]
    pub const fn fixture_id(&self) -> ReviewedFixtureId {
        self.fixture_id
    }

    /// Returns the pinned SHA-256 seal of all provider application frames in the fixture.
    /// 返回 fixture 所有 provider 应用帧的固定 SHA-256 封印。
    #[must_use]
    pub fn fixture_sha256(&self) -> &str {
        &self.fixture_sha256
    }

    /// Returns the fixed replay clock used only for provider-timestamp freshness classification.
    /// 返回仅用于 provider 时间戳新鲜度分类的固定回放时钟。
    #[must_use]
    pub const fn fixture_freshness_clock_unix_seconds(&self) -> u64 {
        self.fixture_freshness_clock_unix_seconds
    }

    /// Returns the local control-plane terminal state.
    /// 返回本地控制面的终止状态。
    #[must_use]
    pub const fn terminal_state(&self) -> OfflineFixtureTerminal {
        self.terminal_state
    }

    /// Returns provider binary frames present in the static script, excluding `FixtureEnd`.
    /// 返回静态脚本中的 provider binary 帧数，不包含 `FixtureEnd`。
    #[must_use]
    pub const fn script_frame_count(&self) -> u32 {
        self.script_frame_count
    }

    /// Returns provider binary frames actually delivered to the session runner.
    /// 返回实际交给 session runner 的 provider binary 帧数。
    #[must_use]
    pub const fn runner_received_frame_count(&self) -> u32 {
        self.runner_received_frame_count
    }

    /// Returns the ordered digest of every frame actually delivered to the runner.
    /// 返回实际交给 runner 的全部帧的有序摘要。
    #[must_use]
    pub fn runner_received_digest_sha256(&self) -> &str {
        &self.runner_received_digest_sha256
    }

    /// Returns post-auth frames captured by the injected raw sink.
    /// 返回注入 raw sink 捕获的认证后帧数。
    #[must_use]
    pub const fn captured_frame_count(&self) -> u32 {
        self.captured_frame_count
    }

    /// Returns captured provider frames containing at least one decoded quote or trade.
    /// 返回至少包含一条已解码 quote 或 trade 的捕获 provider 帧数。
    #[must_use]
    pub const fn raw_market_frame_count(&self) -> u32 {
        self.raw_market_frame_count
    }

    /// Returns matching pre-decode ACKs returned by the injected sink.
    /// 返回注入 sink 返回且与捕获匹配的解码前 ACK 数。
    #[must_use]
    pub const fn predecode_ack_count(&self) -> u32 {
        self.predecode_ack_count
    }

    /// Returns matching post-decode finalization ACKs returned by the injected sink.
    /// 返回注入 sink 返回且与定稿摘要匹配的解码后 ACK 数。
    #[must_use]
    pub const fn finalization_ack_count(&self) -> u32 {
        self.finalization_ack_count
    }

    /// Returns exact bytes presented to the injected sink after authentication.
    /// 返回认证后提交给注入 sink 的精确字节数。
    #[must_use]
    pub const fn captured_bytes(&self) -> u64 {
        self.captured_bytes
    }

    /// Returns the ordered digest of exact persisted capture keys and payload bytes.
    /// 返回精确持久化捕获 key 与 payload 字节的有序摘要。
    #[must_use]
    pub fn ordered_raw_frames_sha256(&self) -> &str {
        &self.ordered_raw_frames_sha256
    }

    /// Returns the ordered digest of finalization ACKs matched to each captured frame.
    /// 返回与每条捕获帧匹配的定稿 ACK 有序摘要。
    #[must_use]
    pub fn finalization_rollup_sha256(&self) -> &str {
        &self.finalization_rollup_sha256
    }

    /// Returns the count of bounded projected `MarketDataItem` records.
    /// 返回有界投影 `MarketDataItem` 记录数。
    #[must_use]
    pub const fn output_item_count(&self) -> u32 {
        self.output_item_count
    }
}

/// Stable secret-free failure categories for an offline fixture run.
/// 离线 fixture 回放的稳定、无秘密错误类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineFixtureError {
    /// The fixed fixture did not match its pinned input seal.
    /// 固定 fixture 与其固定输入封印不一致。
    FixtureMismatch,
    /// The bounded run was cancelled by its caller.
    /// 调用方取消了有界回放。
    Cancelled,
    /// The bounded run or cleanup exceeded its local deadline.
    /// 有界回放或清理超过本地期限。
    Deadline,
    /// The existing Alpaca runner rejected or did not finish the protocol transcript.
    /// 现有 Alpaca runner 拒绝了协议文本或未完成协议文本。
    SessionFailed,
    /// The injected sink failed or returned an unmatched ACK.
    /// 注入 sink 失败或返回了不匹配 ACK。
    CaptureFailed,
    /// The existing event projector rejected an output record.
    /// 现有事件 projector 拒绝了输出记录。
    ProjectionFailed,
    /// A fixed frame, byte, ACK, or output bound was exceeded or mismatched.
    /// 固定帧、字节、ACK 或输出上限超出或不匹配。
    LimitOrSealMismatch,
    /// The reviewed fixture configuration could not be constructed.
    /// 无法构造已审阅 fixture 配置。
    ConfigurationFailed,
}

impl Display for OfflineFixtureError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::FixtureMismatch => "ALPACA_OFFLINE_FIXTURE_MISMATCH",
            Self::Cancelled => "ALPACA_OFFLINE_FIXTURE_CANCELLED",
            Self::Deadline => "ALPACA_OFFLINE_FIXTURE_DEADLINE",
            Self::SessionFailed => "ALPACA_OFFLINE_FIXTURE_SESSION_FAILED",
            Self::CaptureFailed => "ALPACA_OFFLINE_FIXTURE_CAPTURE_FAILED",
            Self::ProjectionFailed => "ALPACA_OFFLINE_FIXTURE_PROJECTION_FAILED",
            Self::LimitOrSealMismatch => "ALPACA_OFFLINE_FIXTURE_LIMIT_OR_SEAL_MISMATCH",
            Self::ConfigurationFailed => "ALPACA_OFFLINE_FIXTURE_CONFIGURATION_FAILED",
        };
        formatter.write_str(label)
    }
}

impl Error for OfflineFixtureError {}

/// Replays the one fixed fixture through the ordinary runner, sink contract, and projector.
/// 通过普通 runner、sink 合同和 projector 回放唯一固定 fixture。
///
/// `sink_factory` receives exactly `("alpaca", "opra")`. The SDK accepts no credentials,
/// network address, path, frame bytes, or provider selector from the caller. The returned
/// receipt proves only that this fixed local input was consumed and that the injected sink
/// returned matching ACKs; it does not prove entitlement, provider completeness, or durable
/// storage beyond the sink's own contract.
///
/// `sink_factory` 只会收到 `("alpaca", "opra")`。SDK 不接受调用方传入凭证、网络地址、路径、帧字节或
/// provider selector。回执仅证明本地固定输入已消费且注入 sink 返回匹配 ACK；它不证明 entitlement、
/// provider 完整性，也不超出 sink 自身合同证明持久性。
///
/// # Errors
///
/// Returns a fixed [`OfflineFixtureError`] category when cancellation, timeout, protocol,
/// projection, frame bounds, fixture seal, or sink acknowledgements fail.
pub async fn capture_reviewed_fixture(
    fixture_id: ReviewedFixtureId,
    sink_factory: Arc<dyn RawFrameSinkFactory>,
    mut cancellation: watch::Receiver<bool>,
) -> Result<OfflineFixtureCapture, OfflineFixtureError> {
    if *cancellation.borrow_and_update() {
        return Err(OfflineFixtureError::Cancelled);
    }
    let frames = reviewed_fixture_frames()?;
    let fixture_sha256 = input_digest(&frames)?;
    if fixture_sha256 != fixture_id.expected_sha256() {
        return Err(OfflineFixtureError::FixtureMismatch);
    }
    let fixture_id = match fixture_id {
        ReviewedFixtureId::AlpacaOpraTradeV1 => fixture_id,
    };
    run_scripted_capture(
        fixture_id,
        frames.clone(),
        FixtureScript::reviewed(frames),
        sink_factory,
        cancellation,
    )
    .await
}

#[derive(Clone)]
struct FixtureScript {
    frames: Vec<Vec<u8>>,
    marker_after_frames: Option<usize>,
    outbound_frames: Vec<Zeroizing<Vec<u8>>>,
    market_item_delivered: Arc<Notify>,
}

impl FixtureScript {
    fn reviewed(frames: Vec<Vec<u8>>) -> Self {
        Self {
            frames,
            marker_after_frames: Some(FIXTURE_FRAME_COUNT),
            outbound_frames: Vec::new(),
            market_item_delivered: Arc::new(Notify::new()),
        }
    }
}

struct FixtureCredentials;

impl CredentialProvider for FixtureCredentials {
    fn load_credentials(
        &mut self,
    ) -> impl Future<Output = Result<AlpacaCredentials, CredentialFailure>> + Send {
        std::future::ready(
            AlpacaCredentials::new(FIXTURE_KEY_ID, FIXTURE_SECRET)
                .map_err(|_| CredentialFailure::Unavailable),
        )
    }
}

#[derive(Default)]
struct InputAudit {
    received_frames: Vec<Vec<u8>>,
    fixture_end_count: usize,
    fixture_end_after_frames: Option<usize>,
    close_count: usize,
    outbound_count: usize,
    outbound_mismatch: bool,
    overflow: bool,
}

struct ScriptedConnector {
    script: FixtureScript,
    audit: Arc<Mutex<InputAudit>>,
    connect_count: usize,
}

impl SocketConnector for ScriptedConnector {
    type Socket = ScriptedSocket;

    fn connect(
        &mut self,
        endpoint: &'static str,
    ) -> impl Future<Output = Result<Self::Socket, ConnectFailure>> + Send {
        let Some(connect_count) = self.connect_count.checked_add(1) else {
            return std::future::ready(Err(ConnectFailure::EndpointRejected));
        };
        self.connect_count = connect_count;
        if endpoint != EXPECTED_ENDPOINT || self.connect_count != 1 {
            return std::future::ready(Err(ConnectFailure::EndpointRejected));
        }
        std::future::ready(Ok(ScriptedSocket {
            frames: self.script.frames.clone(),
            marker_after_frames: self.script.marker_after_frames,
            outbound_frames: self.script.outbound_frames.clone(),
            market_item_delivered: Arc::clone(&self.script.market_item_delivered),
            next_frame: 0,
            next_outbound: 0,
            marker_sent: false,
            closed: false,
            audit: Arc::clone(&self.audit),
        }))
    }
}

struct ScriptedSocket {
    frames: Vec<Vec<u8>>,
    marker_after_frames: Option<usize>,
    outbound_frames: Vec<Zeroizing<Vec<u8>>>,
    market_item_delivered: Arc<Notify>,
    next_frame: usize,
    next_outbound: usize,
    marker_sent: bool,
    closed: bool,
    audit: Arc<Mutex<InputAudit>>,
}

impl StreamSocket for ScriptedSocket {
    fn send_binary(
        &mut self,
        payload: Zeroizing<Vec<u8>>,
    ) -> impl Future<Output = Result<(), SocketFailure>> + Send {
        let expected = self.outbound_frames.get(self.next_outbound);
        let matches = expected.is_some_and(|expected| expected.as_slice() == payload.as_slice());
        let result = if let Some(next_outbound) = self.next_outbound.checked_add(1) {
            self.next_outbound = next_outbound;
            match self.audit.lock() {
                Ok(mut audit) => {
                    if let Some(outbound_count) = audit.outbound_count.checked_add(1) {
                        audit.outbound_count = outbound_count;
                        audit.outbound_mismatch |= !matches;
                        if matches {
                            Ok(())
                        } else {
                            Err(SocketFailure::Transport)
                        }
                    } else {
                        Err(SocketFailure::Transport)
                    }
                }
                Err(_) => Err(SocketFailure::Transport),
            }
        } else {
            Err(SocketFailure::Transport)
        };
        std::future::ready(result)
    }

    async fn receive(&mut self) -> Result<Option<SocketFrame>, SocketFailure> {
        if self.closed {
            return Ok(None);
        }
        if !self.marker_sent && self.marker_after_frames == Some(self.next_frame) {
            if self.next_frame == FIXTURE_FRAME_COUNT {
                self.market_item_delivered.notified().await;
            }
            self.marker_sent = true;
            let mut audit = self.audit.lock().map_err(|_| SocketFailure::Transport)?;
            audit.fixture_end_count = audit
                .fixture_end_count
                .checked_add(1)
                .ok_or(SocketFailure::Transport)?;
            audit.fixture_end_after_frames = Some(self.next_frame);
            return Ok(Some(SocketFrame::FixtureEnd));
        }
        let Some(frame) = self.frames.get(self.next_frame).cloned() else {
            return Ok(None);
        };
        if frame.is_empty() || frame.len() > FIXTURE_FRAME_LIMIT {
            return Err(SocketFailure::Transport);
        }
        {
            let mut audit = self.audit.lock().map_err(|_| SocketFailure::Transport)?;
            let byte_count = audit
                .received_frames
                .iter()
                .try_fold(0usize, |sum, bytes| sum.checked_add(bytes.len()))
                .and_then(|sum| sum.checked_add(frame.len()))
                .ok_or(SocketFailure::Transport)?;
            if audit.received_frames.len() >= FIXTURE_FRAME_COUNT
                || byte_count > FIXTURE_BYTES_LIMIT
            {
                audit.overflow = true;
                return Err(SocketFailure::Transport);
            }
            audit.received_frames.push(frame.clone());
        }
        self.next_frame = self
            .next_frame
            .checked_add(1)
            .ok_or(SocketFailure::Transport)?;
        Ok(Some(SocketFrame::Binary(frame)))
    }

    fn close(&mut self) -> impl Future<Output = ()> + Send {
        if !self.closed {
            self.closed = true;
            if let Ok(mut audit) = self.audit.lock() {
                audit.close_count = audit.close_count.saturating_add(1);
            }
        }
        std::future::ready(())
    }
}

#[derive(Clone)]
struct CaptureAudit {
    entries: Vec<CapturedFrame>,
    failed: bool,
}

impl Default for CaptureAudit {
    fn default() -> Self {
        Self {
            entries: Vec::with_capacity(CAPTURE_FRAME_COUNT),
            failed: false,
        }
    }
}

#[derive(Clone)]
struct CapturedFrame {
    capture_key: RawFrameCaptureKey,
    payload: Vec<u8>,
    predecode_ack: RawFrameCaptureAck,
    disposition: Option<RawFrameDisposition>,
    event_count: u32,
    finalization_sha256: Option<String>,
}

struct TrackingFactory {
    inner: Arc<dyn RawFrameSinkFactory>,
    expected_frames: Arc<Vec<Vec<u8>>>,
    audit: Arc<Mutex<CaptureAudit>>,
}

impl RawFrameSinkFactory for TrackingFactory {
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> Result<Arc<dyn RawFrameSink>, RawFrameSinkError> {
        if provider != "alpaca" || feed != "opra" {
            return Err(RawFrameSinkError::Unavailable);
        }
        let inner = self.inner.create_sink(provider, feed)?;
        Ok(Arc::new(TrackingSink {
            inner,
            expected_frames: Arc::clone(&self.expected_frames),
            audit: Arc::clone(&self.audit),
        }))
    }
}

struct TrackingSink {
    inner: Arc<dyn RawFrameSink>,
    expected_frames: Arc<Vec<Vec<u8>>>,
    audit: Arc<Mutex<CaptureAudit>>,
}

impl RawFrameSink for TrackingSink {
    fn capture_instance_id(&self) -> broker_ports::RawCaptureInstanceId {
        self.inner.capture_instance_id()
    }

    fn persist_before_decode<'a>(
        &'a self,
        capture: &'a RawFrameCapture,
    ) -> PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>> {
        Box::pin(async move {
            let index = {
                let audit = self.audit.lock().map_err(|_| RawFrameSinkError::Poisoned)?;
                if audit.failed || audit.entries.len() >= self.expected_frames.len() {
                    return Err(RawFrameSinkError::Poisoned);
                }
                audit.entries.len()
            };
            if capture.provider() != "alpaca"
                || capture.feed() != "opra"
                || capture.entitlement() != EntitlementState::Unknown
                || capture.wire_encoding() != RawFrameWireEncoding::MessagePack
                || capture.source_generation() != 1
                || capture.frame_sequence() != u64::try_from(index + 1).unwrap_or(u64::MAX)
                || self.expected_frames.get(index).map(Vec::as_slice)
                    != Some(capture.payload().as_bytes())
            {
                self.poison();
                return Err(RawFrameSinkError::Poisoned);
            }
            let acknowledgement = self.inner.persist_before_decode(capture).await?;
            if !acknowledgement.matches(capture) {
                self.poison();
                return Err(RawFrameSinkError::Poisoned);
            }
            let mut audit = self.audit.lock().map_err(|_| RawFrameSinkError::Poisoned)?;
            if audit.failed || audit.entries.len() != index {
                audit.failed = true;
                return Err(RawFrameSinkError::Poisoned);
            }
            audit.entries.push(CapturedFrame {
                capture_key: capture.capture_key().clone(),
                payload: capture.payload().as_bytes().to_vec(),
                predecode_ack: acknowledgement.clone(),
                disposition: None,
                event_count: 0,
                finalization_sha256: None,
            });
            Ok(acknowledgement)
        })
    }

    fn finalize_after_decode<'a>(
        &'a self,
        predecode_ack: &'a RawFrameCaptureAck,
        summary: &'a RawFrameFinalization,
    ) -> PortFuture<'a, Result<RawFrameFinalizationAck, RawFrameSinkError>> {
        Box::pin(async move {
            let index = usize::try_from(predecode_ack.frame_sequence())
                .ok()
                .and_then(|sequence| sequence.checked_sub(1))
                .ok_or(RawFrameSinkError::Poisoned)?;
            {
                let audit = self.audit.lock().map_err(|_| RawFrameSinkError::Poisoned)?;
                let Some(entry) = audit.entries.get(index) else {
                    return Err(RawFrameSinkError::Poisoned);
                };
                if audit.failed
                    || entry.predecode_ack != *predecode_ack
                    || entry.finalization_sha256.is_some()
                    || !expected_summary(index, summary)
                {
                    drop(audit);
                    self.poison();
                    return Err(RawFrameSinkError::Poisoned);
                }
            }
            let acknowledgement = self
                .inner
                .finalize_after_decode(predecode_ack, summary)
                .await?;
            if !acknowledgement.matches(predecode_ack, summary) {
                self.poison();
                return Err(RawFrameSinkError::Poisoned);
            }
            let mut audit = self.audit.lock().map_err(|_| RawFrameSinkError::Poisoned)?;
            if audit.failed {
                return Err(RawFrameSinkError::Poisoned);
            }
            let Some(entry) = audit.entries.get_mut(index) else {
                audit.failed = true;
                return Err(RawFrameSinkError::Poisoned);
            };
            if entry.finalization_sha256.is_some() {
                return Err(RawFrameSinkError::Poisoned);
            }
            entry.disposition = Some(summary.disposition());
            entry.event_count = summary.event_count();
            entry.finalization_sha256 = Some(acknowledgement.summary_sha256().to_owned());
            Ok(acknowledgement)
        })
    }
}

impl TrackingSink {
    fn poison(&self) {
        if let Ok(mut audit) = self.audit.lock() {
            audit.failed = true;
        }
    }
}

fn expected_summary(index: usize, summary: &RawFrameFinalization) -> bool {
    match index {
        0 => {
            summary.disposition() == RawFrameDisposition::ControlMessage
                && summary.event_count() == 0
                && summary.symbols().is_empty()
        }
        1 => {
            summary.disposition() == RawFrameDisposition::DecodedMarketData
                && summary.event_count() == 1
                && summary.symbols().len() == 1
                && summary.symbols()[0] == FIXTURE_SYMBOL
        }
        _ => false,
    }
}

async fn run_scripted_capture(
    fixture_id: ReviewedFixtureId,
    expected_input_frames: Vec<Vec<u8>>,
    mut script: FixtureScript,
    sink_factory: Arc<dyn RawFrameSinkFactory>,
    mut cancellation: watch::Receiver<bool>,
) -> Result<OfflineFixtureCapture, OfflineFixtureError> {
    if expected_input_frames.len() != FIXTURE_FRAME_COUNT
        || expected_input_frames
            .iter()
            .any(|frame| frame.is_empty() || frame.len() > FIXTURE_FRAME_LIMIT)
        || input_bytes(&expected_input_frames)? > FIXTURE_BYTES_LIMIT
    {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let expected_sha256 = input_digest(&expected_input_frames)?;
    if expected_sha256 != fixture_id.expected_sha256() {
        return Err(OfflineFixtureError::FixtureMismatch);
    }
    let config = fixture_config()?;
    script.outbound_frames = expected_outbound_frames(&config)?;
    let expected_capture_frames = Arc::new(expected_input_frames[2..].to_vec());
    let input_audit = Arc::new(Mutex::new(InputAudit::default()));
    let capture_audit = Arc::new(Mutex::new(CaptureAudit::default()));
    let tracking_factory: Arc<dyn RawFrameSinkFactory> = Arc::new(TrackingFactory {
        inner: sink_factory,
        expected_frames: expected_capture_frames,
        audit: Arc::clone(&capture_audit),
    });
    let sink = tracking_factory
        .create_sink("alpaca", "opra")
        .map_err(|_| OfflineFixtureError::CaptureFailed)?;
    let (publishers, receivers, consumers_closed) = create_lanes(&config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let market_item_delivered = Arc::clone(&script.market_item_delivered);
    let connector = ScriptedConnector {
        script,
        audit: Arc::clone(&input_audit),
        connect_count: 0,
    };
    let shutdown_for_collector = shutdown_tx.clone();
    let run_and_collect = async move {
        tokio::join!(
            run_fixture_session(
                config,
                FixtureCredentials,
                connector,
                shutdown_rx,
                consumers_closed,
                publishers,
                Some(sink),
                fixture_freshness_time(),
            ),
            collect_items(receivers, shutdown_for_collector, market_item_delivered),
        )
    };
    tokio::pin!(run_and_collect);
    let completed = tokio::select! {
        biased;
        () = wait_for_cancellation(&mut cancellation) => {
            shutdown_tx.send_replace(true);
            let _ = time::timeout(CLEANUP_DEADLINE, &mut run_and_collect).await;
            return Err(OfflineFixtureError::Cancelled);
        }
        () = time::sleep(RUN_DEADLINE) => {
            shutdown_tx.send_replace(true);
            let _ = time::timeout(CLEANUP_DEADLINE, &mut run_and_collect).await;
            return Err(OfflineFixtureError::Deadline);
        },
        result = &mut run_and_collect => result,
    };
    let (session_result, items_result) = completed;
    let items = items_result?;
    match session_result {
        Ok(SessionExit::FixtureEnd) => {}
        Ok(SessionExit::Cancelled) => return Err(OfflineFixtureError::Cancelled),
        Err(StreamError::RawCaptureFailed) => return Err(OfflineFixtureError::CaptureFailed),
        Err(_) => return Err(OfflineFixtureError::SessionFailed),
    }
    build_receipt(
        fixture_id,
        expected_sha256,
        items,
        &input_audit,
        &capture_audit,
    )
}

fn build_receipt(
    fixture_id: ReviewedFixtureId,
    expected_sha256: String,
    items: Vec<MarketDataItem>,
    input_audit: &Mutex<InputAudit>,
    capture_audit: &Mutex<CaptureAudit>,
) -> Result<OfflineFixtureCapture, OfflineFixtureError> {
    if items.len() > RESULT_ITEM_LIMIT || !items_have_expected_identity(&items) {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let input = input_audit
        .lock()
        .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let received_digest = input_digest(&input.received_frames)?;
    if input.overflow
        || input.received_frames.len() != FIXTURE_FRAME_COUNT
        || input.fixture_end_count != 1
        || input.fixture_end_after_frames != Some(FIXTURE_FRAME_COUNT)
        || input.close_count != 1
        || input.outbound_count != 2
        || input.outbound_mismatch
        || received_digest != expected_sha256
    {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let received_frame_count = u32::try_from(input.received_frames.len())
        .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    drop(input);

    let capture = capture_audit
        .lock()
        .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    if capture.failed
        || capture.entries.len() != CAPTURE_FRAME_COUNT
        || capture
            .entries
            .iter()
            .any(|entry| entry.disposition.is_none() || entry.finalization_sha256.is_none())
    {
        return Err(OfflineFixtureError::CaptureFailed);
    }
    let captured_bytes = capture
        .entries
        .iter()
        .try_fold(0_u64, |sum, entry| {
            sum.checked_add(u64::try_from(entry.payload.len()).ok()?)
        })
        .ok_or(OfflineFixtureError::LimitOrSealMismatch)?;
    if captured_bytes > u64::try_from(CAPTURE_BYTES_LIMIT).unwrap_or(u64::MAX) {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let raw_market_frame_count = capture
        .entries
        .iter()
        .filter(|entry| {
            entry.disposition == Some(RawFrameDisposition::DecodedMarketData)
                && entry.event_count > 0
        })
        .count();
    let ordered_raw_frames_sha256 = ordered_raw_frames_digest(&capture.entries)?;
    let finalization_rollup_sha256 = finalization_digest(&capture.entries)?;
    let captured_frame_count = u32::try_from(capture.entries.len())
        .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let raw_market_frame_count = u32::try_from(raw_market_frame_count)
        .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let item_count =
        u32::try_from(items.len()).map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let receipt = OfflineFixtureReceipt {
        fixture_id,
        fixture_sha256: expected_sha256,
        fixture_freshness_clock_unix_seconds: FIXTURE_TIMESTAMP_UNIX_SECONDS,
        terminal_state: OfflineFixtureTerminal::FixtureEnd,
        script_frame_count: u32::try_from(FIXTURE_FRAME_COUNT)
            .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?,
        runner_received_frame_count: received_frame_count,
        runner_received_digest_sha256: received_digest,
        captured_frame_count,
        raw_market_frame_count,
        predecode_ack_count: captured_frame_count,
        finalization_ack_count: captured_frame_count,
        captured_bytes,
        ordered_raw_frames_sha256,
        finalization_rollup_sha256,
        output_item_count: item_count,
    };
    Ok(OfflineFixtureCapture { items, receipt })
}

async fn collect_items(
    mut receivers: LaneReceivers,
    shutdown: watch::Sender<bool>,
    market_item_delivered: Arc<Notify>,
) -> Result<Vec<MarketDataItem>, OfflineFixtureError> {
    let mut projector = EventProjector::default();
    let mut items = Vec::with_capacity(16);
    let mut quotes_closed = false;
    let mut trades_closed = false;
    let mut controls_closed = false;
    loop {
        if quotes_closed && trades_closed && controls_closed {
            return Ok(items);
        }
        let update = tokio::select! {
            biased;
            control = receivers.controls.recv(), if !controls_closed => if let Some(event) = control {
                Some(StreamUpdate::Control(event))
            } else {
                controls_closed = true;
                None
            },
            quote = receivers.quotes.recv(), if !quotes_closed => if let Some(update) = quote {
                Some(StreamUpdate::Quote(update))
            } else {
                quotes_closed = true;
                None
            },
            trade = receivers.trades.recv(), if !trades_closed => if let Some(update) = trade {
                Some(StreamUpdate::Trade(update))
            } else {
                trades_closed = true;
                None
            },
        };
        let Some(update) = update else { continue };
        match project_update(&mut projector, update, OptionFeed::Opra) {
            Ok(Some(item)) if items.len() < RESULT_ITEM_LIMIT => {
                let delivered_market_event = matches!(item, MarketDataItem::Event { .. });
                items.push(item);
                if delivered_market_event {
                    market_item_delivered.notify_one();
                }
            }
            Ok(Some(_)) => {
                shutdown.send_replace(true);
                return Err(OfflineFixtureError::LimitOrSealMismatch);
            }
            Ok(None) => {}
            Err(_) => {
                shutdown.send_replace(true);
                return Err(OfflineFixtureError::ProjectionFailed);
            }
        }
    }
}

async fn wait_for_cancellation(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

fn fixture_config() -> Result<StreamConfig, OfflineFixtureError> {
    let symbol = OptionContractSymbol::new(FIXTURE_SYMBOL)
        .map_err(|_| OfflineFixtureError::ConfigurationFailed)?;
    let subscriptions = DesiredSubscriptions::new([], [symbol])
        .map_err(|_| OfflineFixtureError::ConfigurationFailed)?;
    let limits = StreamLimits {
        connect_timeout: Duration::from_secs(2),
        authentication_timeout: Duration::from_secs(2),
        acknowledgement_timeout: Duration::from_secs(2),
        quote_capacity: 1,
        trade_capacity: 1,
        control_capacity: 16,
        ..StreamLimits::default()
    };
    let reconnect = ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1), 1, 0)
        .map_err(|_| OfflineFixtureError::ConfigurationFailed)?;
    StreamConfig::with_limits_and_reconnect(
        StreamEnvironment::Sandbox,
        OptionFeed::Opra,
        subscriptions,
        limits,
        reconnect,
    )
    .map_err(|_| OfflineFixtureError::ConfigurationFailed)
}

fn fixture_freshness_time() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(FIXTURE_TIMESTAMP_UNIX_SECONDS)
}

fn expected_outbound_frames(
    config: &StreamConfig,
) -> Result<Vec<Zeroizing<Vec<u8>>>, OfflineFixtureError> {
    let credentials = AlpacaCredentials::new(FIXTURE_KEY_ID, FIXTURE_SECRET)
        .map_err(|_| OfflineFixtureError::ConfigurationFailed)?;
    let auth = crate::protocol::encode_auth(&credentials)
        .map_err(|()| OfflineFixtureError::ConfigurationFailed)?;
    let subscriptions = crate::protocol::encode_subscriptions(&config.subscriptions)
        .map_err(|()| OfflineFixtureError::ConfigurationFailed)?;
    Ok(vec![auth, Zeroizing::new(subscriptions)])
}

fn reviewed_fixture_frames() -> Result<Vec<Vec<u8>>, OfflineFixtureError> {
    let connected = success_frame("connected")?;
    let authenticated = success_frame("authenticated")?;
    let subscription = message_frame(vec![
        ("T", rmpv::Value::from("subscription")),
        ("quotes", rmpv::Value::Array(Vec::new())),
        (
            "trades",
            rmpv::Value::Array(vec![rmpv::Value::from(FIXTURE_SYMBOL)]),
        ),
    ])?;
    let trade = message_frame(vec![
        ("T", rmpv::Value::from("t")),
        ("S", rmpv::Value::from(FIXTURE_SYMBOL)),
        ("t", rmpv::Value::from("2026-10-08T12:00:00.000000000Z")),
        ("p", rmpv::Value::F64(1.25)),
        ("s", rmpv::Value::from(1_u64)),
        ("x", rmpv::Value::from("X")),
        ("c", rmpv::Value::Array(Vec::new())),
    ])?;
    Ok(vec![connected, authenticated, subscription, trade])
}

fn success_frame(message: &str) -> Result<Vec<u8>, OfflineFixtureError> {
    message_frame(vec![
        ("T", rmpv::Value::from("success")),
        ("msg", rmpv::Value::from(message)),
    ])
}

fn message_frame(fields: Vec<(&str, rmpv::Value)>) -> Result<Vec<u8>, OfflineFixtureError> {
    let values = fields
        .into_iter()
        .map(|(key, value)| (rmpv::Value::from(key), value))
        .collect();
    let frame = rmpv::Value::Array(vec![rmpv::Value::Map(values)]);
    let mut bytes = Vec::new();
    rmpv::encode::write_value(&mut bytes, &frame)
        .map_err(|_| OfflineFixtureError::ConfigurationFailed)?;
    if bytes.is_empty() || bytes.len() > FIXTURE_FRAME_LIMIT {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    Ok(bytes)
}

fn input_bytes(frames: &[Vec<u8>]) -> Result<usize, OfflineFixtureError> {
    frames.iter().try_fold(0usize, |sum, frame| {
        sum.checked_add(frame.len())
            .ok_or(OfflineFixtureError::LimitOrSealMismatch)
    })
}

fn input_digest(frames: &[Vec<u8>]) -> Result<String, OfflineFixtureError> {
    if frames.len() > FIXTURE_FRAME_COUNT
        || frames.iter().any(|frame| frame.len() > FIXTURE_FRAME_LIMIT)
        || input_bytes(frames)? > FIXTURE_BYTES_LIMIT
    {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let count =
        u32::try_from(frames.len()).map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let mut digest = Sha256::new();
    digest.update(b"eqoboard.alpaca.offline-fixture-input.v1\0");
    digest.update(count.to_be_bytes());
    for frame in frames {
        let length =
            u32::try_from(frame.len()).map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
        digest.update(length.to_be_bytes());
        digest.update(frame);
    }
    Ok(hex_digest(digest.finalize().as_slice()))
}

fn ordered_raw_frames_digest(entries: &[CapturedFrame]) -> Result<String, OfflineFixtureError> {
    let count =
        u32::try_from(entries.len()).map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let mut digest = Sha256::new();
    digest.update(b"eqoboard.alpaca.offline-fixture-raw-frames.v1\0");
    digest.update(count.to_be_bytes());
    for entry in entries {
        let key = &entry.capture_key;
        let length = u32::try_from(entry.payload.len())
            .map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
        digest.update(key.capture_instance_id().as_bytes());
        digest.update(key.source_generation().to_be_bytes());
        digest.update(key.frame_sequence().to_be_bytes());
        digest.update(length.to_be_bytes());
        digest.update(&entry.payload);
    }
    Ok(hex_digest(digest.finalize().as_slice()))
}

fn finalization_digest(entries: &[CapturedFrame]) -> Result<String, OfflineFixtureError> {
    let count =
        u32::try_from(entries.len()).map_err(|_| OfflineFixtureError::LimitOrSealMismatch)?;
    let mut digest = Sha256::new();
    digest.update(b"eqoboard.alpaca.offline-fixture-finalization.v1\0");
    digest.update(count.to_be_bytes());
    for entry in entries {
        let summary_sha256 = entry
            .finalization_sha256
            .as_deref()
            .ok_or(OfflineFixtureError::CaptureFailed)?;
        digest.update(entry.capture_key.capture_instance_id().as_bytes());
        digest.update(entry.capture_key.source_generation().to_be_bytes());
        digest.update(entry.capture_key.frame_sequence().to_be_bytes());
        digest.update(decode_sha256(entry.capture_key.frame_sha256())?);
        digest.update(decode_sha256(summary_sha256)?);
    }
    Ok(hex_digest(digest.finalize().as_slice()))
}

fn decode_sha256(hex: &str) -> Result<[u8; 32], OfflineFixtureError> {
    if hex.len() != 64 {
        return Err(OfflineFixtureError::LimitOrSealMismatch);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(OfflineFixtureError::LimitOrSealMismatch)?;
        let low = hex_nibble(pair[1]).ok_or(OfflineFixtureError::LimitOrSealMismatch)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn items_have_expected_identity(items: &[MarketDataItem]) -> bool {
    items.iter().all(|item| match item {
        MarketDataItem::Event { envelope, .. } => {
            let source = &envelope.metadata.source;
            source.provider == "alpaca"
                && source.feed == "opra"
                && source.entitlement == EntitlementState::Unknown
                && source.source_record_id.is_none()
        }
        MarketDataItem::Control(envelope) => {
            let source = &envelope.metadata.source;
            source.provider == "alpaca"
                && source.feed == "opra"
                && source.entitlement == EntitlementState::Unknown
                && source.source_record_id.is_none()
        }
        MarketDataItem::RawFrame(frame) => {
            frame.provider == "alpaca"
                && frame.feed == "opra"
                && frame.entitlement == EntitlementState::Unknown
                && frame.capture_key.is_some()
                && frame.wire_encoding == RawFrameWireEncoding::MessagePack
        }
    })
}

#[cfg(test)]
#[path = "offline_test_support_tests.rs"]
mod tests;
