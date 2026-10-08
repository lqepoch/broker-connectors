//! Authenticated session orchestration, acknowledgement gates, and bounded recovery.
//!
//! 认证会话编排、回执 gate 与有界恢复。

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::SystemTime;

use tokio::sync::watch;
use tokio::time::{self, Instant};
use zeroize::Zeroizing;

use crate::config::StreamConfig;
use crate::credentials::{CredentialFailure, CredentialProvider};
use crate::diagnostics::{ProtocolViolationReason, log_lane_failure, log_protocol_violation};
use crate::lane::{
    LaneFailure, LanePublishers, QuoteUpdate, StreamHandle, TradeUpdate, create_lanes,
    publish_phase, publish_provider_error, publish_subscription, publish_unknown,
};
use crate::model::{
    DataFreshness, IngestStamp, SessionGeneration, SessionPhase, SessionStatusCause,
    classify_freshness,
};
use crate::protocol::{
    ProviderError, ProviderErrorKind, ProviderMessage, SuccessMessage,
    decode_frame_with_diagnostics, encode_auth, encode_subscriptions,
};
use crate::state::{InternalPhase, PhaseMachine, reconnect_delay};
use crate::transport::{
    ConnectFailure, SocketConnector, SocketFrame, StreamSocket, TokioConnector,
};
use broker_ports::RawFrameSink;

mod runner;

const CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
/// Checked process-local allocation separates retry-jitter streams for overlapping sessions.
/// 经检查的进程内分配为并行会话隔离重试抖动序列。
static NEXT_SESSION_RETRY_SEED: AtomicU64 = AtomicU64::new(0x236a_1aca_5eed);

/// Supplies wall time for provider timestamp freshness checks.
/// 为 provider 时间戳新鲜度检查提供墙上时钟。
trait FreshnessClock: Send + Sync + 'static {
    fn now(&self) -> SystemTime;
}

/// Carries deterministic per-session timing and retry-jitter inputs.
/// 承载每个会话的确定性时钟与重试抖动输入。
struct SessionRuntime<W> {
    retry_seed: u64,
    freshness_clock: W,
}

#[derive(Clone, Copy)]
struct SystemFreshnessClock;

impl FreshnessClock for SystemFreshnessClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[cfg(any(test, feature = "offline-test-support"))]
#[derive(Clone, Copy)]
struct FixedFreshnessClock(SystemTime);

#[cfg(any(test, feature = "offline-test-support"))]
impl FreshnessClock for FixedFreshnessClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn next_session_retry_seed() -> Option<u64> {
    NEXT_SESSION_RETRY_SEED
        .try_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| current.checked_add(1),
        )
        .ok()
}

/// Owns one read-only Alpaca options WebSocket session and its injected credential source.
/// 持有一个只读 Alpaca 期权 WebSocket 会话及其注入的凭证来源。
pub struct AlpacaOptionsStream<P> {
    config: StreamConfig,
    credential_provider: P,
    raw_frame_sink: Option<Arc<dyn RawFrameSink>>,
}

impl<P: CredentialProvider> AlpacaOptionsStream<P> {
    /// Creates an options stream using fixed configuration and injected credentials.
    /// Without [`Self::with_raw_frame_sink`], raw records remain memory-only diagnostics.
    /// 使用固定配置与注入凭证创建期权流。未调用 [`Self::with_raw_frame_sink`] 时，原始记录仅作内存诊断。
    pub fn new(config: StreamConfig, credential_provider: P) -> Self {
        Self {
            config,
            credential_provider,
            raw_frame_sink: None,
        }
    }

    /// Requires a trusted sink for pre-decode capture and post-decode finalization.
    /// 设置可信 sink，并要求行情帧在解码与事件发布前完成两阶段确认。
    #[must_use]
    pub fn with_raw_frame_sink(mut self, sink: Arc<dyn RawFrameSink>) -> Self {
        self.raw_frame_sink = Some(sink);
        self
    }

    /// Starts one bounded session task and returns its independent consumer lanes.
    /// 启动一个有界会话任务并返回彼此隔离的消费者队列。
    pub fn spawn(self) -> StreamHandle {
        let (publishers, receivers, consumers_closed) = create_lanes(&self.config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_session(
            self.config,
            self.credential_provider,
            TokioConnector,
            shutdown_rx,
            consumers_closed,
            publishers,
            self.raw_frame_sink,
        ));
        receivers.into_handle(shutdown_tx, task)
    }
}

/// Stable end state returned when the caller cancels the session.
/// 调用方取消会话时返回的固定结束状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionExit {
    /// The caller requested cancellation and the owned socket was closed.
    /// 调用方请求取消，且会话任务已关闭其拥有的 socket。
    Cancelled,
    /// The reviewed offline fixture reached its local test-control terminal marker.
    /// This state is not a provider protocol watermark or market-data completeness claim.
    #[cfg(feature = "offline-test-support")]
    FixtureEnd,
}

/// Stable, secret-free error categories returned by the session task.
/// 会话任务返回的不携带秘密值的固定错误类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamError {
    /// The session task was joined more than once.
    /// 同一个会话任务被重复 join。
    TaskAlreadyJoined,
    /// The async task panicked or was aborted outside the session API.
    /// 异步任务在会话 API 之外 panic 或被中止。
    TaskTerminated,
    /// The bounded reconnect budget was exhausted.
    /// 有界重连预算已经耗尽。
    RetryLimitReached,
    /// The injected credential source had no valid credential pair.
    /// 注入的凭证来源没有可用凭证。
    CredentialsUnavailable,
    /// Authentication was rejected by the provider.
    /// Provider 拒绝了认证。
    AuthenticationRejected,
    /// The authentication deadline expired.
    /// 认证期限已过。
    AuthenticationTimeout,
    /// The endpoint rejected the request or the configured feed path.
    /// Endpoint 拒绝了请求或配置的 feed 路径。
    EndpointRejected,
    /// The provider reported its account connection limit.
    /// Provider 报告账户连接数已达上限。
    ConnectionLimitReached,
    /// The provider rejected the requested subscription set.
    /// Provider 拒绝了所请求的订阅集合。
    SubscriptionRejected,
    /// The provider reported that its WebSocket client was too slow.
    /// Provider 报告 WebSocket 客户端消费过慢。
    ProviderSlowClient,
    /// The provider rejected the `MessagePack` WebSocket content type.
    /// Provider 拒绝了 `MessagePack` WebSocket content type。
    MessagePackRejected,
    /// The provider returned an unclassified numeric error code.
    /// Provider 返回了当前未分类的数字错误码。
    ProviderRejected(i64),
    /// The authentication exchange failed before its protocol deadline.
    /// 认证交换在协议期限内未能完成。
    ConnectionTimeout,
    /// The full subscription state was not acknowledged before its deadline.
    /// Provider 未在期限内确认完整订阅状态。
    AcknowledgementTimeout,
    /// The peer closed the socket or the transport failed.
    /// 对端关闭 socket 或传输层失败。
    TransportLost,
    /// The provider sent a malformed, oversized, or out-of-order frame.
    /// Provider 发送了畸形、超大或顺序错误的 frame。
    ProtocolViolation,
    /// A configured raw sink failed, timed out, or returned a mismatched acknowledgement.
    /// 已配置的 raw sink 失败、超时或返回不匹配的确认。
    RawCaptureFailed,
    /// The local session generation or ingest sequence could not advance safely.
    /// 本地会话代次或接收序号无法安全递增。
    SequenceExhausted,
    /// A bounded quote, trade, or control lane reached its capacity.
    /// 有界报价、成交或控制队列已满。
    ConsumerOverloaded,
    /// A quote, trade, or control receiver closed while its session was active.
    /// 会话运行期间报价、成交或控制消费者关闭。
    ConsumerClosed,
    /// The internal checked phase machine rejected a transition.
    /// 内部校验状态机拒绝了阶段转换。
    InternalStateViolation,
}

impl Display for StreamError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskAlreadyJoined => formatter.write_str("ALPACA_STREAM_TASK_ALREADY_JOINED"),
            Self::TaskTerminated => formatter.write_str("ALPACA_STREAM_TASK_TERMINATED"),
            Self::RetryLimitReached => formatter.write_str("ALPACA_STREAM_RETRY_LIMIT_REACHED"),
            Self::CredentialsUnavailable => {
                formatter.write_str("ALPACA_STREAM_CREDENTIALS_UNAVAILABLE")
            }
            Self::AuthenticationRejected => {
                formatter.write_str("ALPACA_STREAM_AUTHENTICATION_REJECTED")
            }
            Self::AuthenticationTimeout => {
                formatter.write_str("ALPACA_STREAM_AUTHENTICATION_TIMEOUT")
            }
            Self::EndpointRejected => formatter.write_str("ALPACA_STREAM_ENDPOINT_REJECTED"),
            Self::ConnectionLimitReached => {
                formatter.write_str("ALPACA_STREAM_CONNECTION_LIMIT_REACHED")
            }
            Self::SubscriptionRejected => {
                formatter.write_str("ALPACA_STREAM_SUBSCRIPTION_REJECTED")
            }
            Self::ProviderSlowClient => formatter.write_str("ALPACA_STREAM_PROVIDER_SLOW_CLIENT"),
            Self::MessagePackRejected => formatter.write_str("ALPACA_STREAM_MESSAGEPACK_REJECTED"),
            Self::ProviderRejected(code) => {
                write!(formatter, "ALPACA_STREAM_PROVIDER_REJECTED_{code}")
            }
            Self::ConnectionTimeout => formatter.write_str("ALPACA_STREAM_CONNECTION_TIMEOUT"),
            Self::AcknowledgementTimeout => {
                formatter.write_str("ALPACA_STREAM_ACKNOWLEDGEMENT_TIMEOUT")
            }
            Self::TransportLost => formatter.write_str("ALPACA_STREAM_TRANSPORT_LOST"),
            Self::ProtocolViolation => formatter.write_str("ALPACA_STREAM_PROTOCOL_VIOLATION"),
            Self::RawCaptureFailed => formatter.write_str("ALPACA_STREAM_RAW_CAPTURE_FAILED"),
            Self::SequenceExhausted => formatter.write_str("ALPACA_STREAM_SEQUENCE_EXHAUSTED"),
            Self::ConsumerOverloaded => formatter.write_str("ALPACA_STREAM_CONSUMER_OVERLOADED"),
            Self::ConsumerClosed => formatter.write_str("ALPACA_STREAM_CONSUMER_CLOSED"),
            Self::InternalStateViolation => {
                formatter.write_str("ALPACA_STREAM_INTERNAL_STATE_VIOLATION")
            }
        }
    }
}

impl Error for StreamError {}

struct Runner<P, C, W> {
    config: StreamConfig,
    credential_provider: P,
    connector: C,
    shutdown: watch::Receiver<bool>,
    consumers_closed: watch::Receiver<bool>,
    publishers: LanePublishers,
    raw_frame_sink: Option<Arc<dyn RawFrameSink>>,
    raw_capture_instance_id: Option<broker_ports::RawCaptureInstanceId>,
    phases: PhaseMachine,
    generation: u64,
    retry_seed: u64,
    freshness_clock: W,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AttemptFailure {
    error: StreamError,
    cause: SessionStatusCause,
    retryable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptEnd {
    Failed(AttemptFailure),
    Cancelled,
    ConsumersClosed,
    #[cfg(feature = "offline-test-support")]
    FixtureEnd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Abort<T> {
    Completed(T),
    Cancelled,
    ConsumersClosed,
}

#[derive(Clone, Copy)]
enum HandshakeTarget {
    Connected,
    Authenticated,
    Subscription,
}

enum ReceiveFailure {
    Timeout(StreamError),
    Transport,
}

pub(crate) async fn run_session<P, C>(
    config: StreamConfig,
    credential_provider: P,
    connector: C,
    shutdown: watch::Receiver<bool>,
    consumers_closed: watch::Receiver<bool>,
    publishers: LanePublishers,
    raw_frame_sink: Option<Arc<dyn RawFrameSink>>,
) -> Result<SessionExit, StreamError>
where
    P: CredentialProvider,
    C: SocketConnector,
{
    let Some(retry_seed) = next_session_retry_seed() else {
        publishers.close().await;
        return Err(StreamError::SequenceExhausted);
    };
    run_session_with_seed_and_clock(
        config,
        credential_provider,
        connector,
        shutdown,
        consumers_closed,
        publishers,
        raw_frame_sink,
        SessionRuntime {
            retry_seed,
            freshness_clock: SystemFreshnessClock,
        },
    )
    .await
}

#[cfg(feature = "offline-test-support")]
#[allow(clippy::too_many_arguments)] // Reuses the same explicit runtime inputs as the normal session entrypoint.
pub(crate) async fn run_fixture_session<P, C>(
    config: StreamConfig,
    credential_provider: P,
    connector: C,
    shutdown: watch::Receiver<bool>,
    consumers_closed: watch::Receiver<bool>,
    publishers: LanePublishers,
    raw_frame_sink: Option<Arc<dyn RawFrameSink>>,
    freshness_time: SystemTime,
) -> Result<SessionExit, StreamError>
where
    P: CredentialProvider,
    C: SocketConnector,
{
    let Some(retry_seed) = next_session_retry_seed() else {
        publishers.close().await;
        return Err(StreamError::SequenceExhausted);
    };
    run_session_with_seed_and_clock(
        config,
        credential_provider,
        connector,
        shutdown,
        consumers_closed,
        publishers,
        raw_frame_sink,
        SessionRuntime {
            retry_seed,
            freshness_clock: FixedFreshnessClock(freshness_time),
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Runtime inputs stay explicit in the deterministic session harness.
async fn run_session_with_seed_and_clock<P, C, W>(
    config: StreamConfig,
    credential_provider: P,
    connector: C,
    shutdown: watch::Receiver<bool>,
    consumers_closed: watch::Receiver<bool>,
    publishers: LanePublishers,
    raw_frame_sink: Option<Arc<dyn RawFrameSink>>,
    runtime: SessionRuntime<W>,
) -> Result<SessionExit, StreamError>
where
    P: CredentialProvider,
    C: SocketConnector,
    W: FreshnessClock,
{
    let raw_capture_instance_id = raw_frame_sink
        .as_ref()
        .map(|sink| sink.capture_instance_id());
    let mut runner = Runner {
        config,
        credential_provider,
        connector,
        shutdown,
        consumers_closed,
        publishers,
        raw_frame_sink,
        raw_capture_instance_id,
        phases: PhaseMachine::new(),
        generation: 0,
        retry_seed: runtime.retry_seed,
        freshness_clock: runtime.freshness_clock,
    };
    let result = runner.run().await;
    runner.publishers.close().await;
    result
}

fn failed(error: StreamError, cause: SessionStatusCause, retryable: bool) -> AttemptEnd {
    AttemptEnd::Failed(AttemptFailure {
        error,
        cause,
        retryable,
    })
}

fn terminal(error: StreamError, cause: SessionStatusCause) -> AttemptFailure {
    AttemptFailure {
        error,
        cause,
        retryable: false,
    }
}

fn provider_failure(error: ProviderError) -> AttemptEnd {
    match error.kind {
        ProviderErrorKind::Authentication => failed(
            StreamError::AuthenticationRejected,
            SessionStatusCause::AuthenticationRejected,
            false,
        ),
        ProviderErrorKind::ConnectionLimit => failed(
            StreamError::ConnectionLimitReached,
            SessionStatusCause::ConnectionLimitReached,
            false,
        ),
        ProviderErrorKind::SubscriptionRejected => failed(
            StreamError::SubscriptionRejected,
            SessionStatusCause::SubscriptionRejected,
            false,
        ),
        ProviderErrorKind::MessagePackRequired => failed(
            StreamError::MessagePackRejected,
            SessionStatusCause::ProtocolViolation,
            false,
        ),
        ProviderErrorKind::SlowClient => failed(
            StreamError::ProviderSlowClient,
            SessionStatusCause::ConsumerOverloaded,
            true,
        ),
        ProviderErrorKind::Other => failed(
            StreamError::ProviderRejected(error.code),
            SessionStatusCause::ProtocolViolation,
            true,
        ),
    }
}

fn map_lane_failure(error: LaneFailure) -> StreamError {
    log_lane_failure(error);
    match error {
        LaneFailure::QuoteCapacityExceeded
        | LaneFailure::TradeOverloaded
        | LaneFailure::RawFrameOverloaded
        | LaneFailure::ControlOverloaded => StreamError::ConsumerOverloaded,
        LaneFailure::QuoteReceiverClosed
        | LaneFailure::TradeReceiverClosed
        | LaneFailure::ControlReceiverClosed => StreamError::ConsumerClosed,
    }
}

fn lane_failure(error: LaneFailure) -> AttemptFailure {
    let cause = match error {
        LaneFailure::QuoteCapacityExceeded
        | LaneFailure::TradeOverloaded
        | LaneFailure::RawFrameOverloaded
        | LaneFailure::ControlOverloaded => SessionStatusCause::ConsumerOverloaded,
        LaneFailure::QuoteReceiverClosed
        | LaneFailure::TradeReceiverClosed
        | LaneFailure::ControlReceiverClosed => SessionStatusCause::ConsumerClosed,
    };
    terminal(map_lane_failure(error), cause)
}

async fn cancellable<F: Future>(
    future: F,
    shutdown: &mut watch::Receiver<bool>,
    consumers_closed: &mut watch::Receiver<bool>,
) -> Abort<F::Output> {
    tokio::pin!(future);
    tokio::select! {
        biased;
        () = wait_for_true(shutdown) => Abort::Cancelled,
        () = wait_for_true(consumers_closed) => Abort::ConsumersClosed,
        output = &mut future => Abort::Completed(output),
    }
}

async fn wait_for_true(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
