//! Single-owner asynchronous Streamer session orchestration.
//!
//! The runtime accepts an authenticated-session factory and socket port. The
//! concrete TLS adapter implements the Node-characterized WebSocket handshake;
//! OAuth and REST StreamerInfo/token-provider integration remain blocked on
//! the credential contract in #171.
//! 定义单所有者 Streamer runtime、有界通道、计时器和重连编排。

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout};

mod critical;
mod fingerprint;
mod market_data;

use critical::CriticalBuffer;
use market_data::MarketDataBuffer;

pub use critical::CriticalEventReceiver;
pub use market_data::{MarketDataFieldProvenance, MarketDataReceiver, MarketDataUpdate};

use crate::command::{
    AckDisposition, AckIgnoreReason, CommandAcknowledgement, ConnectionGeneration, RequestId,
    ServiceReadiness, StreamerCommand, SubscriptionCommand,
};
use crate::manifest::StreamerService;
use crate::state::{BoundedKeySet, MAX_KEY_BYTES, ServiceStateError, ServiceSubscriptionManager};
use crate::wire::{
    MAX_WIRE_FRAME_BYTES, StreamerDataPayload, StreamerNotifyPayload, StreamerWireError,
    is_successful_streamer_command, parse_streamer_frame,
};

/// Maximum accepted capacity of the control mailbox.
/// 中文摘要：runtime 控制消息队列容量上限；满时控制发送报错，不无限排队。
pub const MAX_CONTROL_CAPACITY: usize = 256;
/// Maximum accepted number of queued critical events.
/// 中文摘要：关键事件队列容量上限；过载会让 runtime 失败关闭。
pub const MAX_CRITICAL_CAPACITY: usize = 256;
/// Maximum queued bytes retained by critical delivery.
/// 中文摘要：关键事件队列估算的总字节上限；超限时不丢弃关键事件而是失败关闭。
pub const MAX_CRITICAL_BYTES: usize = 32 * 1024 * 1024;
/// Maximum number of distinct coalesced market-data keys.
/// 中文摘要：单连接代次可保留的合并行情 key 总数上限。
pub const MAX_MARKET_DATA_KEYS: usize = 4096;
/// Maximum distinct (service, key) ordering fences retained per generation.
/// 中文摘要：去重与时间顺序 fence 可保留的 service/key 对总数上限。
pub const MAX_MARKET_DATA_FENCE_KEYS: usize = MAX_MARKET_DATA_KEYS;
/// Maximum UTF-8 key length retained by one ordering fence.
/// 中文摘要：单个行情 fence key 可占用的最大字节数。
pub const MAX_MARKET_DATA_FENCE_KEY_BYTES: usize = MAX_KEY_BYTES;
// Covers the SHA-256 digest, timestamp/revision, owned String header/allocation,
// ordered-map entry, and allocator slack in addition to the key's UTF-8 bytes.
const MARKET_DATA_ORDER_FENCE_ENTRY_OVERHEAD: usize = 256;
/// Maximum estimated retained bytes for one generation's ordering fences.
/// 中文摘要：行情排序 fence 估算的总字节上限。
pub const MAX_MARKET_DATA_FENCE_BYTES: usize = MAX_MARKET_DATA_FENCE_KEYS
    * (MAX_MARKET_DATA_FENCE_KEY_BYTES + MARKET_DATA_ORDER_FENCE_ENTRY_OVERHEAD);
/// Maximum distinct fields retained for one sparse market-data row.
/// 中文摘要：单条合并行情允许保留的最大字段数。
pub const MAX_MERGED_MARKET_FIELDS: usize = 128;
/// Maximum serialized size of one merged sparse market-data row.
/// 中文摘要：单条合并行情及其 provenance 允许占用的最大字节数。
pub const MAX_MERGED_MARKET_ROW_BYTES: usize = 64 * 1024;
/// Maximum estimated retained bytes for field provenance and duplicate-detection metadata.
///
/// The estimate includes each field key's JSON-encoded size and a fixed
/// allowance for the provenance value and ordered-map entry, plus row-level
/// metadata used to recognize exact repeats of the latest sparse delta.
/// 中文摘要：单条合并行情字段来源记录的最大字节数。
pub const MAX_MERGED_MARKET_PROVENANCE_BYTES: usize = MAX_MERGED_MARKET_ROW_BYTES
    + MAX_MERGED_MARKET_FIELDS * MARKET_FIELD_PROVENANCE_OVERHEAD
    + MARKET_ROW_PROVENANCE_METADATA_OVERHEAD;

const DEFAULT_CONTROL_CAPACITY: usize = 32;
const DEFAULT_CRITICAL_CAPACITY: usize = 64;
const DEFAULT_CRITICAL_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MARKET_DATA_KEYS: usize = 1024;
const MARKET_FIELD_PROVENANCE_OVERHEAD: usize = 128;
const MARKET_ROW_PROVENANCE_METADATA_OVERHEAD: usize = 16;

/// Safe, fixed-category failure reported by an injected session port.
///
/// No provider error text, URL, token, or raw wire payload crosses this
/// diagnostic boundary.
/// 中文摘要：socket 与认证端口的脱敏失败分类；不包含 provider 或网络库的错误文本。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortFailure {
    /// Credentials or session metadata were unavailable to the adapter.
    /// 适配器无法取得登录凭证或会话元数据。
    AuthenticationUnavailable,
    /// The adapter could not establish an authenticated session.
    /// 无法建立已认证的 socket。
    ConnectFailed,
    /// The adapter could not complete a bounded command send.
    /// 无法完成有界命令发送。
    SendFailed,
    /// The adapter could not read from the current socket.
    /// 无法读取当前 socket。
    ReceiveFailed,
    /// The remote socket closed normally or unexpectedly.
    /// 远端 socket 已关闭。
    Closed,
}

/// Injectable boundary that must return only after a session is authenticated.
///
/// Production implementations must keep credentials private, avoid logging
/// handshake material, and enforce TLS and endpoint policy. The concrete
/// Schwab wire adapter uses the current Node characterization; credential
/// lookup remains an injected boundary. The returned
/// future is cancelled by drop when [`SessionConfig::connect_timeout`] expires.
/// The future must directly own in-progress sockets and authentication buffers
/// and be cancellation-safe: it must not detach handshake/session tasks, and
/// dropping the future must synchronously close directly owned half-open
/// sockets and zeroize/clear directly owned authentication buffers. If it
/// starts child tasks, it must retain their handles and request abort on drop;
/// child tasks may own only non-sensitive data and must never retain credentials
/// or authentication buffers. Tokio abort is a cancellation request; the task
/// may run its destructor after this future's drop returns.
/// 中文摘要：定义 authenticated交易时段工厂 的注入边界；具体实现仍须遵守类型说明中的安全约束。
pub trait AuthenticatedSessionFactory: Send + 'static {
    /// The single socket owned by the runtime for a connection generation.
    /// 中文摘要：指定认证连接成功后返回的 socket 实现类型。
    type Socket: StreamerSocket;

    /// Opens and authenticates exactly one socket for the requested generation.
    ///
    /// If this future is dropped before it returns, implementations must
    /// synchronously close its directly owned partial transports and clear its
    /// directly owned temporary authentication material. Any child task must
    /// be non-detached, hold no authentication data, and receive an abort
    /// request as described by the trait-level cancellation contract.
    /// 中文摘要：建立并认证当前连接代次的 socket；取消时必须清理半开资源和认证材料。
    fn connect_authenticated(
        &mut self,
        generation: ConnectionGeneration,
        login_request_id: RequestId,
    ) -> impl Future<Output = Result<Self::Socket, PortFailure>> + Send;
}

/// Injectable single-socket operations owned exclusively by the session task.
/// 中文摘要：定义 Streamersocket 的注入边界；具体实现仍须遵守类型说明中的安全约束。
pub trait StreamerSocket: Send + 'static {
    /// Sends a planned read-only service command using adapter-owned session
    /// correlation metadata. Any error or runtime timeout is treated as an
    /// uncertain frame write: the runtime discards this socket and reconnects
    /// with a full replay of desired subscriptions. Implementations must not
    /// log command keys or provider headers.
    /// 中文摘要：发送一条计划内的只读订阅命令；不允许输出 key 或认证 header。
    fn send_subscription(
        &mut self,
        command: &StreamerCommand,
    ) -> impl Future<Output = Result<(), PortFailure>> + Send;

    /// Reads one complete serialized JSON frame, or returns None on close.
    /// The adapter must enforce its frame allocation bound. This future must
    /// be cancellation-safe because control and ACK deadlines are polled beside
    /// it by the runtime.
    /// 中文摘要：接收一个有界、完整的应用层 JSON frame。
    fn receive_frame(
        &mut self,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, PortFailure>> + Send;

    /// Reads either a bounded application frame or a WebSocket control frame
    /// that proves connection liveness. Existing mock ports may implement only
    /// `receive_frame`; the default adapter wraps application frames.
    /// 中文摘要：接收应用数据或证明连接存活的 WebSocket 控制事件。
    fn receive_event(
        &mut self,
    ) -> impl Future<Output = Result<Option<SocketEvent>, PortFailure>> + Send {
        async move {
            self.receive_frame()
                .await
                .map(|frame| frame.map(SocketEvent::Frame))
        }
    }

    /// Sends one client-side WebSocket ping. The session task calls this at the
    /// bounded interval configured below; simple fake ports may use the no-op
    /// default.
    /// 中文摘要：发送共享 WebSocket 上的客户端 ping。
    fn send_ping(&mut self) -> impl Future<Output = Result<(), PortFailure>> + Send {
        async { Ok(()) }
    }
}

/// One bounded read from the currently owned socket.
/// 中文摘要：socket 端口只向 runtime 暴露已校验帧、存活事件或关闭状态。
pub enum SocketEvent {
    /// A complete UTF-8 or binary-encoded JSON application frame.
    /// 已接收的应用层 JSON frame。
    Frame(Vec<u8>),
    /// A WebSocket PING/PONG proved that the peer is still responsive.
    /// 已接收能够证明 socket 对端仍有响应的控制帧。
    Liveness,
}

/// Runtime timeouts and bounded queue capacities.
/// 中文摘要：连接、ACK、发送、心跳、重连和通道容量的运行时配置。
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Deadline for authenticated socket creation.
    /// 中文摘要：创建已认证 socket 的期限。
    pub connect_timeout: Duration,
    /// Maximum time to await one service command ACK.
    /// 中文摘要：等待服务命令确认的最大时长。
    pub acknowledgement_timeout: Duration,
    /// Upper bound for a single socket command send.
    /// 中文摘要：一次 socket 命令发送的最大时长。
    pub send_timeout: Duration,
    /// Interval for server-liveness evaluation and client WebSocket pings.
    /// 中文摘要：运行时检查心跳状态的间隔。
    pub heartbeat_check_interval: Duration,
    /// Maximum silence since a valid JSON frame or peer control response.
    /// 中文摘要：有效 JSON frame 或 peer control 响应允许的最长静默期。
    pub heartbeat_timeout: Duration,
    /// Client ping interval for the shared socket.
    /// 中文摘要：共享 socket 的客户端 ping 周期。
    pub client_ping_interval: Duration,
    /// Initial reconnect delay, doubled after each failed connection.
    /// 中文摘要：首次重连等待时长。
    pub reconnect_initial_delay: Duration,
    /// Maximum reconnect delay.
    /// 中文摘要：重连等待时长的最大值。
    pub reconnect_max_delay: Duration,
    /// Capacity of the actor/control mailbox.
    /// 中文摘要：控制 mailbox 的容量。
    pub control_capacity: usize,
    /// Capacity of critical activity/control/diagnostic delivery.
    /// 中文摘要：关键事件接收队列容量。
    pub critical_capacity: usize,
    /// Maximum bytes retained by critical delivery.
    /// 中文摘要：关键事件队列允许保留的最大字节数。
    pub critical_bytes: usize,
    /// Maximum distinct service/key entries retained by quote coalescing.
    /// 中文摘要：行情合并缓冲区允许保留的不同服务/key 数量。
    pub market_data_capacity: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(15),
            acknowledgement_timeout: Duration::from_secs(15),
            send_timeout: Duration::from_secs(5),
            heartbeat_check_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(20),
            client_ping_interval: Duration::from_secs(5),
            reconnect_initial_delay: Duration::from_secs(2),
            reconnect_max_delay: Duration::from_secs(30),
            control_capacity: DEFAULT_CONTROL_CAPACITY,
            critical_capacity: DEFAULT_CRITICAL_CAPACITY,
            critical_bytes: DEFAULT_CRITICAL_BYTES,
            market_data_capacity: DEFAULT_MARKET_DATA_KEYS,
        }
    }
}

impl SessionConfig {
    fn validate(&self) -> Result<(), SessionConfigError> {
        if self.connect_timeout.is_zero()
            || self.acknowledgement_timeout.is_zero()
            || self.send_timeout.is_zero()
            || self.heartbeat_check_interval.is_zero()
            || self.heartbeat_timeout.is_zero()
            || self.client_ping_interval.is_zero()
            || self.reconnect_initial_delay.is_zero()
            || self.reconnect_max_delay < self.reconnect_initial_delay
        {
            return Err(SessionConfigError::InvalidDuration);
        }
        if self.control_capacity == 0 || self.control_capacity > MAX_CONTROL_CAPACITY {
            return Err(SessionConfigError::InvalidControlCapacity {
                maximum: MAX_CONTROL_CAPACITY,
            });
        }
        if self.critical_capacity == 0 || self.critical_capacity > MAX_CRITICAL_CAPACITY {
            return Err(SessionConfigError::InvalidCriticalCapacity {
                maximum: MAX_CRITICAL_CAPACITY,
            });
        }
        if self.critical_bytes == 0 || self.critical_bytes > MAX_CRITICAL_BYTES {
            return Err(SessionConfigError::InvalidCriticalBytes {
                maximum: MAX_CRITICAL_BYTES,
            });
        }
        if self.market_data_capacity == 0 || self.market_data_capacity > MAX_MARKET_DATA_KEYS {
            return Err(SessionConfigError::InvalidMarketDataCapacity {
                maximum: MAX_MARKET_DATA_KEYS,
            });
        }
        Ok(())
    }
}

/// Invalid session limits; no task or port starts on this error.
/// 中文摘要：runtime 配置不满足有界时长或参数约束时返回的固定错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionConfigError {
    /// Durations must be positive and max reconnect delay >= initial.
    /// 运行时期限必须为正且重连上限不能小于起始延迟。
    InvalidDuration,
    /// Control capacity was zero or exceeded its compile-time limit.
    /// 控制 mailbox 容量为零或超过固定上限。
    InvalidControlCapacity {
        /// Largest control-mailbox capacity accepted by this runtime.
        /// 该 runtime 接受的控制 mailbox 容量上限。
        maximum: usize,
    },
    /// Critical event capacity was zero or exceeded its compile-time limit.
    /// 关键事件队列容量为零或超过固定上限。
    InvalidCriticalCapacity {
        /// Largest number of critical events the output queue may hold.
        /// 关键事件输出队列允许容纳的最大事件数。
        maximum: usize,
    },
    /// Critical byte capacity was zero or exceeded its compile-time limit.
    /// 关键事件字节预算为零或超过固定上限。
    InvalidCriticalBytes {
        /// Largest serialized payload budget accepted for critical events.
        /// 关键事件允许使用的最大序列化负载字节预算。
        maximum: usize,
    },
    /// Market-data capacity was zero or exceeded its compile-time limit.
    /// 行情合并容量为零或超过固定上限。
    InvalidMarketDataCapacity {
        /// Largest number of distinct market-data keys the coalescing buffer may retain.
        /// 行情合并缓冲区可保留的不同标的键数量上限。
        maximum: usize,
    },
}

impl Display for SessionConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDuration => formatter.write_str("invalid Streamer session duration"),
            Self::InvalidControlCapacity { maximum } => {
                write!(
                    formatter,
                    "control capacity must be between 1 and {maximum}"
                )
            }
            Self::InvalidCriticalCapacity { maximum } => {
                write!(
                    formatter,
                    "critical capacity must be between 1 and {maximum}"
                )
            }
            Self::InvalidCriticalBytes { maximum } => {
                write!(
                    formatter,
                    "critical byte capacity must be between 1 and {maximum}"
                )
            }
            Self::InvalidMarketDataCapacity { maximum } => {
                write!(
                    formatter,
                    "market-data capacity must be between 1 and {maximum}"
                )
            }
        }
    }
}

impl Error for SessionConfigError {}

/// Cause associated with a delivered service readiness transition.
/// 中文摘要：报告单个服务 readiness 变化的原因；不把失败转换为已确认订阅。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStatusCause {
    /// Desired state was accepted by the local state machine.
    /// 调用方修改了该服务的期望 key 集合。
    DesiredStateChanged,
    /// A command was sent and awaits its correlated response.
    /// 服务命令已发送，正在等待有界期限内的确认。
    CommandSent,
    /// The matching ACK was accepted.
    /// 当前服务命令已收到确认。
    Acknowledged,
    /// The matching ACK was rejected by the remote service.
    /// 当前服务命令被拒绝。
    Rejected,
    /// The command did not receive an ACK before its deadline.
    /// 确认等待期限已到。
    TimedOut,
    /// A disconnect invalidated pending commands.
    /// 服务失去当前 socket 连接代次。
    Disconnected,
    /// A retry or VIEW was requested while disconnected.
    /// 无法分配请求标识符。
    RequestUnavailable,
}

/// Bounded critical events for account/runtime consumers.
/// 中文摘要：runtime 输出的连接、ACK、服务 readiness、通知及过载事件。
pub enum SessionEvent {
    /// Connection state for the single owned socket.
    /// 连接代次生命周期事件。
    Connection {
        /// Socket generation associated with this connection transition.
        /// 用于隔离重连前后数据的连接代次。
        generation: ConnectionGeneration,
        /// Whether the owned socket is currently connected and authenticated.
        /// 当前受管 socket 是否已连接并完成认证。
        connected: bool,
        /// Sanitized port failure when the connection transition failed.
        /// 已分类且脱敏的端口失败。
        failure: Option<PortFailure>,
    },
    /// Current readiness of one independently managed service.
    /// 服务就绪状态或状态变化事件。
    ServiceStatus {
        /// Socket generation associated with this event or readiness update.
        /// 用于隔离重连前后数据的连接代次。
        generation: Option<ConnectionGeneration>,
        /// Streamer service whose readiness changed.
        /// 该命令或数据行所属的 Streamer 服务。
        service: StreamerService,
        /// Current readiness state for this service and generation.
        /// 该服务当前代次的确认就绪状态。
        readiness: ServiceReadiness,
        /// Classified reason for the readiness-state transition.
        /// 状态变化或失败原因类别。
        cause: ServiceStatusCause,
    },
    /// Account activity payload delivered losslessly until bounded queue
    /// overflow. Overflow terminates the runtime with an explicit error.
    /// 必须通过有界关键事件通道投递的账户活动消息。
    Activity {
        /// Socket generation associated with this event or readiness update.
        /// 用于隔离重连前后数据的连接代次。
        generation: ConnectionGeneration,
        /// Size-bounded account-activity payload delivered through the critical event queue.
        /// 经过大小限制的协议负载。
        payload: StreamerDataPayload,
    },
    /// Malformed frame or bounded-delivery protocol issue.
    /// 不含原始 provider 负载的固定协议错误类别。
    ProtocolFault(StreamerWireError),
    /// A response could not be correlated with a supported service command.
    /// 与待确认服务命令不匹配的响应。
    UnmatchedResponse,
    /// A valid response was stale or mismatched.
    /// 状态机忽略的过时或不匹配确认。
    IgnoredAcknowledgement(AckIgnoreReason),
    /// An unrecognized notification preserved for an upper-layer handler.
    /// 有界 Streamer 通知。
    Notification(StreamerNotifyPayload),
}

impl Debug for SessionEvent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection {
                generation,
                connected,
                failure,
            } => formatter
                .debug_struct("Connection")
                .field("generation", generation)
                .field("connected", connected)
                .field("failure", failure)
                .finish(),
            Self::ServiceStatus {
                generation,
                service,
                readiness,
                cause,
            } => formatter
                .debug_struct("ServiceStatus")
                .field("generation", generation)
                .field("service", service)
                .field("readiness", readiness)
                .field("cause", cause)
                .finish(),
            Self::Activity { generation, .. } => formatter
                .debug_struct("Activity")
                .field("generation", generation)
                .field("payload", &"[REDACTED]")
                .finish(),
            Self::ProtocolFault(error) => {
                formatter.debug_tuple("ProtocolFault").field(error).finish()
            }
            Self::UnmatchedResponse => formatter.write_str("UnmatchedResponse"),
            Self::IgnoredAcknowledgement(reason) => formatter
                .debug_tuple("IgnoredAcknowledgement")
                .field(reason)
                .finish(),
            Self::Notification(_) => formatter
                .debug_tuple("Notification")
                .field(&"[REDACTED]")
                .finish(),
        }
    }
}

/// Bounded control sender used by actors and market-data supervisors.
/// 中文摘要：向唯一 runtime 所有者发送有界控制消息的可克隆句柄；关闭或容量错误通过结果返回。
#[derive(Clone)]
pub struct StreamerControl {
    sender: mpsc::Sender<ControlMessage>,
}

impl Debug for StreamerControl {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerControl")
            .field("sender", &"[REDACTED]")
            .finish()
    }
}

impl StreamerControl {
    /// Replaces one market service's complete desired key set.
    ///
    /// Keys are validated and bounded before entering the mailbox. The
    /// mandatory account activity key cannot be removed by a market actor.
    /// Callers should submit each complete desired set after full and fills
    /// snapshots. Repeating an unchanged set is safe and retries an unconfirmed
    /// service without sending a command for already acknowledged services.
    /// 中文摘要：有界校验后替换单个服务的期望 key；更新经有界控制队列发送给 runtime。
    ///
    /// # Errors
    /// Returns [`SessionControlError::InvalidKeys`] when the supplied key set
    /// violates a hard state bound, [`SessionControlError::MandatoryActivitySubscription`]
    /// when an actor attempts to remove the required activity key, or a queue
    /// error when the runtime cannot accept the command.
    pub fn set_desired<I, K>(
        &self,
        service: StreamerService,
        keys: I,
    ) -> Result<(), SessionControlError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let keys = BoundedKeySet::from_keys(keys).map_err(SessionControlError::InvalidKeys)?;
        if service == StreamerService::AcctActivity
            && (keys.iter().count() != 1 || keys.iter().next() != Some("Account Activity"))
        {
            return Err(SessionControlError::MandatoryActivitySubscription);
        }
        self.enqueue(ControlMessage::SetDesired { service, keys })
    }

    /// Retries one service after a prior rejection/timeout without disturbing
    /// other services or the shared activity socket.
    /// 中文摘要：请求重新同步单个服务，不改变其期望 key。
    ///
    /// # Errors
    /// Returns [`SessionControlError::QueueFull`] when the bounded mailbox is
    /// full or [`SessionControlError::RuntimeStopped`] after shutdown.
    pub fn retry(&self, service: StreamerService) -> Result<(), SessionControlError> {
        self.enqueue(ControlMessage::Retry(service))
    }

    /// Requests a read-only VIEW of one market-data service.
    /// 中文摘要：请求只读查看单个服务的当前订阅，不修改期望状态。
    ///
    /// # Errors
    /// Returns [`SessionControlError::ViewUnsupported`] for account activity,
    /// or a queue error when the runtime cannot accept the command.
    pub fn view(&self, service: StreamerService) -> Result<(), SessionControlError> {
        if service == StreamerService::AcctActivity {
            return Err(SessionControlError::ViewUnsupported);
        }
        self.enqueue(ControlMessage::View(service))
    }

    /// Requests orderly runtime shutdown.
    /// 中文摘要：通过控制队列请求 owner task 关闭；队列已满或已关闭时返回错误。
    ///
    /// # Errors
    /// Returns [`SessionControlError::QueueFull`] when the bounded mailbox is
    /// full or [`SessionControlError::RuntimeStopped`] after shutdown.
    pub fn shutdown(&self) -> Result<(), SessionControlError> {
        self.enqueue(ControlMessage::Shutdown)
    }

    fn enqueue(&self, message: ControlMessage) -> Result<(), SessionControlError> {
        self.sender.try_send(message).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => SessionControlError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => SessionControlError::RuntimeStopped,
        })
    }
}

/// Errors from bounded actor-to-session control operations.
/// 中文摘要：发送控制命令失败时的固定分类，包括有界队列容量与 runtime 关闭。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionControlError {
    /// The supplied key set violated a hard state bound or wire constraint.
    /// 请求的 key 集合未通过校验或资源上限检查。
    InvalidKeys(ServiceStateError),
    /// The sole automation activity key cannot be removed by a market actor.
    /// 必需的账户活动订阅不能被移除。
    MandatoryActivitySubscription,
    /// Current local contract excludes VIEW for account activity.
    /// 该 runtime 不支持 VIEW 状态协调。
    ViewUnsupported,
    /// The bounded control mailbox is full; caller must stop new work/retry.
    /// 有界控制队列没有可用容量。
    QueueFull,
    /// Runtime has stopped or the owner task has been dropped.
    /// 会话 runtime 已停止，不再接受控制命令。
    RuntimeStopped,
}

impl Display for SessionControlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeys(error) => write!(formatter, "invalid subscription keys: {error}"),
            Self::MandatoryActivitySubscription => {
                formatter.write_str("ACCT_ACTIVITY is a mandatory shared session service")
            }
            Self::ViewUnsupported => formatter.write_str("VIEW is unsupported for this service"),
            Self::QueueFull => formatter.write_str("Streamer control queue is full"),
            Self::RuntimeStopped => formatter.write_str("Streamer runtime is stopped"),
        }
    }
}

impl Error for SessionControlError {}

/// Fatal session errors. Queue overflow is explicit; callers should mark
/// dependent trading work inhibited until a fresh session and reconciliation.
/// 中文摘要：runtime 终止原因；容量或协议故障会关闭接收端并清理待交付行情。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRunError {
    /// Critical delivery exceeded its count or byte bound.
    /// 关键事件数量或字节容量超限；runtime 会失败关闭。
    CriticalDeliveryOverflow,
    /// Critical consumer closed while the session was running.
    /// 需要投递关键事件时接收端已关闭。
    CriticalConsumerClosed,
    /// Market data exceeded its bounded coalescing capacity.
    /// 行情行或排序 fence 容量超限；待发报价会被丢弃。
    MarketDataCapacityExceeded,
    /// Market-data consumer closed while its service was active.
    /// 投递行情更新时接收端已关闭。
    MarketDataConsumerClosed,
    /// A bounded internal request ID or revision counter was exhausted.
    /// 单调连接代次、请求或修订标识达到上限。
    StateExhausted(ServiceStateError),
}

impl Display for SessionRunError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CriticalDeliveryOverflow => {
                formatter.write_str("critical Streamer delivery queue exceeded its bound")
            }
            Self::CriticalConsumerClosed => {
                formatter.write_str("critical Streamer delivery consumer closed")
            }
            Self::MarketDataCapacityExceeded => {
                formatter.write_str("market data exceeded its bounded coalescing capacity")
            }
            Self::MarketDataConsumerClosed => formatter.write_str("market-data consumer closed"),
            Self::StateExhausted(error) => write!(formatter, "Streamer state exhausted: {error}"),
        }
    }
}

impl Error for SessionRunError {}

/// Receivers returned when the session owner is created.
/// 中文摘要：控制与事件接收端集合；关键事件和行情使用各自有界缓冲。
pub struct StreamerSessionChannels {
    /// Cloneable bounded command sender for vertical actors/supervisors.
    /// 中文摘要：用于发送有界控制命令的句柄；队列满或 runtime 已关闭时发送会失败。
    pub control: StreamerControl,
    /// Critical activity/control consumer; exactly one consumer is permitted.
    /// 中文摘要：唯一的关键事件接收端；必须及时读取，过载会使 runtime 失败关闭。
    pub critical: CriticalEventReceiver,
    /// Coalesced market-data consumer; exactly one consumer is permitted.
    /// 中文摘要：唯一的合并行情接收端；容量耗尽会关闭接收端并丢弃待交付行情。
    pub market_data: MarketDataReceiver,
}

/// The sole owner of a Streamer socket and service subscription state.
/// 中文摘要：独占管理一个 Streamer socket 和其连接代次的运行时。
pub struct StreamerRuntime<F: AuthenticatedSessionFactory> {
    factory: F,
    config: SessionConfig,
    controls: mpsc::Receiver<ControlMessage>,
    critical: Arc<CriticalBuffer>,
    market_data: Arc<MarketDataBuffer>,
    subscriptions: ServiceSubscriptionManager,
}

impl<F: AuthenticatedSessionFactory> StreamerRuntime<F> {
    /// Creates an owner task and its bounded actor/event channels.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    ///
    /// # Errors
    /// Returns [`SessionConfigError`] when any configured capacity or duration
    /// is zero, exceeds its fixed bound, or has an invalid ordering.
    pub fn new(
        factory: F,
        config: SessionConfig,
    ) -> Result<(Self, StreamerSessionChannels), SessionConfigError> {
        config.validate()?;
        let (control_sender, controls) = mpsc::channel(config.control_capacity);
        let critical = Arc::new(CriticalBuffer::new(
            config.critical_capacity,
            config.critical_bytes,
        ));
        let market_data = Arc::new(MarketDataBuffer::new(config.market_data_capacity));
        let channels = StreamerSessionChannels {
            control: StreamerControl {
                sender: control_sender,
            },
            critical: CriticalEventReceiver {
                buffer: critical.clone(),
            },
            market_data: MarketDataReceiver {
                buffer: market_data.clone(),
            },
        };
        Ok((
            Self {
                factory,
                config,
                controls,
                critical,
                market_data,
                subscriptions: ServiceSubscriptionManager::new(),
            },
            channels,
        ))
    }

    /// Runs until orderly shutdown, fatal bounded-delivery failure, or an
    /// internal monotonic counter exhaustion. All exits close both receivers;
    /// fatal errors also discard queued market-data rows and ordering fences.
    /// If the owner task is aborted, the market-data receiver performs that
    /// discard on its next poll before returning None.
    /// 中文摘要：运行单 socket 会话状态机，处理 ACK、心跳和重连计时器。
    ///
    /// # Errors
    /// Returns [`SessionRunError`] when bounded event delivery, market-data
    /// delivery, or a monotonic state counter can no longer proceed safely.
    pub async fn run(mut self) -> Result<(), SessionRunError> {
        let result = self.run_loop().await;
        self.close_channels().await;
        result
    }

    async fn run_loop(&mut self) -> Result<(), SessionRunError> {
        let mut reconnect_attempt = 0u32;
        loop {
            if self.drain_offline_controls().await? {
                return Ok(());
            }

            let replay = self
                .subscriptions
                .reconnect()
                .map_err(SessionRunError::StateExhausted)?;
            let generation = self.subscriptions.connection_generation().ok_or(
                SessionRunError::StateExhausted(ServiceStateError::NotConnected),
            )?;
            self.market_data.reset_generation(Some(generation)).await;

            let connect_result = timeout(
                self.config.connect_timeout,
                self.factory
                    .connect_authenticated(generation, replay.login_request_id()),
            )
            .await;
            let socket = match connect_result {
                Ok(Ok(socket)) => socket,
                Ok(Err(failure)) => {
                    self.report_disconnect(generation, failure).await?;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if self.wait_before_reconnect(reconnect_attempt).await? {
                        return Ok(());
                    }
                    continue;
                }
                Err(_) => {
                    self.report_disconnect(generation, PortFailure::ConnectFailed)
                        .await?;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if self.wait_before_reconnect(reconnect_attempt).await? {
                        return Ok(());
                    }
                    continue;
                }
            };

            reconnect_attempt = 0;
            self.push_critical(SessionEvent::Connection {
                generation,
                connected: true,
                failure: None,
            })
            .await?;

            match self.serve_connection(socket, generation, replay).await? {
                ConnectionEnd::Shutdown => {
                    self.subscriptions.disconnect();
                    self.market_data.reset_generation(None).await;
                    self.push_critical(SessionEvent::Connection {
                        generation,
                        connected: false,
                        failure: None,
                    })
                    .await?;
                    return Ok(());
                }
                ConnectionEnd::Disconnected(failure) => {
                    self.report_disconnect(generation, failure).await?;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if self.wait_before_reconnect(reconnect_attempt).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    // Keep connection I/O, ACK correlation, and heartbeat deadlines visible
    // in one state-machine method; splitting them obscures cancellation order.
    #[allow(clippy::too_many_lines)]
    async fn serve_connection(
        &mut self,
        mut socket: F::Socket,
        generation: ConnectionGeneration,
        replay: crate::ReplayPlan,
    ) -> Result<ConnectionEnd, SessionRunError> {
        let mut deadlines = BTreeMap::<RequestId, PendingDeadline>::new();
        let mut last_peer_liveness = Instant::now();
        let mut next_heartbeat_check = last_peer_liveness + self.config.heartbeat_check_interval;
        let mut next_client_ping = last_peer_liveness + self.config.client_ping_interval;
        for command in replay.iter() {
            if let Err(failure) = self
                .send_command(&mut socket, command, &mut deadlines)
                .await
            {
                return Ok(ConnectionEnd::Disconnected(failure));
            }
            self.push_critical(SessionEvent::ServiceStatus {
                generation: Some(generation),
                service: command.service(),
                readiness: ServiceReadiness::Pending,
                cause: ServiceStatusCause::CommandSent,
            })
            .await?;
        }

        loop {
            let next_deadline = deadlines.values().map(|entry| entry.deadline).min();
            tokio::select! {
                command = self.controls.recv() => {
                    let Some(command) = command else {
                        return Ok(ConnectionEnd::Shutdown);
                    };
                    if matches!(command, ControlMessage::Shutdown) {
                        return Ok(ConnectionEnd::Shutdown);
                    }
                    let service = command.service();
                    let is_desired_update = matches!(command, ControlMessage::SetDesired { .. });
                    let planned = self.apply_online_control(command).await?;
                    if let Some(planned) = planned {
                        if let Err(failure) = self.send_command(&mut socket, &planned, &mut deadlines).await {
                            return Ok(ConnectionEnd::Disconnected(failure));
                        }
                        self.push_critical(SessionEvent::ServiceStatus {
                            generation: Some(generation),
                            service: planned.service(),
                            readiness: ServiceReadiness::Pending,
                            cause: ServiceStatusCause::CommandSent,
                        }).await?;
                    } else {
                        self.push_critical(SessionEvent::ServiceStatus {
                            generation: Some(generation),
                            service,
                            readiness: self.subscriptions.readiness(service),
                            cause: if is_desired_update {
                                ServiceStatusCause::DesiredStateChanged
                            } else {
                                ServiceStatusCause::RequestUnavailable
                            },
                        }).await?;
                    }
                }
                incoming = socket.receive_event() => {
                    match incoming {
                        Ok(Some(SocketEvent::Liveness)) => {
                            last_peer_liveness = Instant::now();
                        }
                        Ok(Some(SocketEvent::Frame(bytes))) => {
                            if bytes.len() > MAX_WIRE_FRAME_BYTES {
                                self.push_critical(SessionEvent::ProtocolFault(StreamerWireError::FrameTooLarge {
                                    limit: MAX_WIRE_FRAME_BYTES,
                                })).await?;
                                continue;
                            }
                            match parse_streamer_frame(&bytes) {
                                Ok(frame) => {
                                    // Node's StreamerClient treats every
                                    // schema-valid response/data/notify frame
                                    // as server liveness; malformed frames do
                                    // not postpone disconnect detection.
                                    last_peer_liveness = Instant::now();
                                    if let Some(failure) = self
                                        .route_frame(frame, generation, &mut socket, &mut deadlines)
                                        .await?
                                    {
                                        return Ok(ConnectionEnd::Disconnected(failure));
                                    }
                                }
                                Err(error) => self.push_critical(SessionEvent::ProtocolFault(error)).await?,
                            }
                        }
                        Ok(None) => return Ok(ConnectionEnd::Disconnected(PortFailure::Closed)),
                        Err(failure) => return Ok(ConnectionEnd::Disconnected(failure)),
                    }
                }
                () = wait_for_deadline(next_deadline), if next_deadline.is_some() => {
                    self.expire_acknowledgements(generation, &mut deadlines).await?;
                }
                () = sleep_until(next_client_ping) => {
                    next_client_ping = Instant::now() + self.config.client_ping_interval;
                    match timeout(self.config.send_timeout, socket.send_ping()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(failure)) => return Ok(ConnectionEnd::Disconnected(failure)),
                        Err(_) => return Ok(ConnectionEnd::Disconnected(PortFailure::SendFailed)),
                    }
                }
                () = sleep_until(next_heartbeat_check) => {
                    let now = Instant::now();
                    if now.saturating_duration_since(last_peer_liveness) >= self.config.heartbeat_timeout {
                        return Ok(ConnectionEnd::Disconnected(PortFailure::ReceiveFailed));
                    }
                    next_heartbeat_check = now + self.config.heartbeat_check_interval;
                }
            }
        }
    }

    async fn apply_online_control(
        &mut self,
        message: ControlMessage,
    ) -> Result<Option<StreamerCommand>, SessionRunError> {
        match message {
            ControlMessage::SetDesired { service, keys } => {
                self.subscriptions
                    .set_desired(service, keys.iter())
                    .map_err(SessionRunError::StateExhausted)?;
                self.market_data
                    .retain_keys(service, keys.iter().map(str::to_owned).collect())
                    .await;
                self.subscriptions
                    .next_command(service)
                    .map_err(SessionRunError::StateExhausted)
            }
            ControlMessage::Retry(service) => self
                .subscriptions
                .next_command(service)
                .map_err(SessionRunError::StateExhausted),
            ControlMessage::View(service) => self
                .subscriptions
                .view_command(service)
                .map_err(SessionRunError::StateExhausted),
            ControlMessage::Shutdown => Ok(None),
        }
    }

    async fn send_command(
        &self,
        socket: &mut F::Socket,
        command: &StreamerCommand,
        deadlines: &mut BTreeMap<RequestId, PendingDeadline>,
    ) -> Result<(), PortFailure> {
        match timeout(self.config.send_timeout, socket.send_subscription(command)).await {
            Ok(Ok(())) => {
                deadlines.insert(
                    command.request_id(),
                    PendingDeadline {
                        service: command.service(),
                        generation: command.connection_generation(),
                        deadline: Instant::now() + self.config.acknowledgement_timeout,
                    },
                );
                Ok(())
            }
            Ok(Err(failure)) => Err(failure),
            Err(_) => Err(PortFailure::SendFailed),
        }
    }

    // Frame routing updates subscription state and bounded market data as one
    // ordered transition, so retain its local sequencing in one function.
    #[allow(clippy::too_many_lines)]
    async fn route_frame(
        &mut self,
        frame: crate::StreamerWireFrame,
        generation: ConnectionGeneration,
        socket: &mut F::Socket,
        deadlines: &mut BTreeMap<RequestId, PendingDeadline>,
    ) -> Result<Option<PortFailure>, SessionRunError> {
        if let Some(responses) = frame.response {
            for response in responses {
                let Some(service) = service_from_name(&response.service) else {
                    self.push_critical(SessionEvent::UnmatchedResponse).await?;
                    continue;
                };
                let Some(command) = command_from_name(&response.command) else {
                    self.push_critical(SessionEvent::UnmatchedResponse).await?;
                    continue;
                };
                let Some(request_id) = request_id_from_wire(&response.request_id) else {
                    self.push_critical(SessionEvent::UnmatchedResponse).await?;
                    continue;
                };
                let acknowledgement = CommandAcknowledgement::new(
                    generation,
                    request_id,
                    service,
                    command,
                    is_successful_streamer_command(
                        &response.service,
                        &response.command,
                        response.content.code,
                    ),
                );
                let pending_revision = self
                    .subscriptions
                    .pending_command(service)
                    .map(StreamerCommand::desired_revision);
                match self.subscriptions.acknowledge(acknowledgement) {
                    AckDisposition::Accepted { readiness } => {
                        deadlines.remove(&request_id);
                        self.push_critical(SessionEvent::ServiceStatus {
                            generation: Some(generation),
                            service,
                            readiness,
                            cause: ServiceStatusCause::Acknowledged,
                        })
                        .await?;
                        let desired_changed_while_viewing = command == SubscriptionCommand::View
                            && pending_revision
                                != Some(self.subscriptions.desired_revision(service));
                        let needs_followup =
                            command != SubscriptionCommand::View || desired_changed_while_viewing;
                        let next = if needs_followup {
                            self.subscriptions
                                .next_command(service)
                                .map_err(SessionRunError::StateExhausted)?
                        } else {
                            None
                        };
                        if let Some(next) = next {
                            if let Err(failure) = self.send_command(socket, &next, deadlines).await
                            {
                                return Ok(Some(failure));
                            }
                            self.push_critical(SessionEvent::ServiceStatus {
                                generation: Some(generation),
                                service,
                                readiness: ServiceReadiness::Pending,
                                cause: ServiceStatusCause::CommandSent,
                            })
                            .await?;
                        }
                    }
                    AckDisposition::Rejected => {
                        deadlines.remove(&request_id);
                        self.push_critical(SessionEvent::ServiceStatus {
                            generation: Some(generation),
                            service,
                            readiness: self.subscriptions.readiness(service),
                            cause: ServiceStatusCause::Rejected,
                        })
                        .await?;
                    }
                    AckDisposition::Ignored(reason) => {
                        self.push_critical(SessionEvent::IgnoredAcknowledgement(reason))
                            .await?;
                    }
                }
            }
        }

        if let Some(data) = frame.data {
            for payload in data {
                match service_from_name(&payload.service) {
                    Some(StreamerService::AcctActivity) => {
                        self.push_critical(SessionEvent::Activity {
                            generation,
                            payload,
                        })
                        .await?;
                    }
                    Some(
                        service @ (StreamerService::LevelOneEquities
                        | StreamerService::LevelOneOptions),
                    ) => {
                        for row in payload.content {
                            let Some(key) = row.key else {
                                self.push_critical(SessionEvent::ProtocolFault(
                                    StreamerWireError::InvalidSchema,
                                ))
                                .await?;
                                continue;
                            };
                            if BoundedKeySet::normalize_key(&key).is_err() {
                                self.push_critical(SessionEvent::ProtocolFault(
                                    StreamerWireError::InvalidSchema,
                                ))
                                .await?;
                                continue;
                            }
                            if self.subscriptions.readiness(service) != ServiceReadiness::Ready
                                || !self.subscriptions.is_desired_key(service, &key)
                            {
                                continue;
                            }
                            self.market_data
                                .push_delta(service, generation, key, payload.timestamp, row.fields)
                                .await?;
                        }
                    }
                    None => {
                        self.push_critical(SessionEvent::ProtocolFault(
                            StreamerWireError::InvalidSchema,
                        ))
                        .await?;
                    }
                }
            }
        }

        if let Some(notifications) = frame.notify {
            for notification in notifications {
                if notification.heartbeat.is_none() || !notification.extra_fields.is_empty() {
                    self.push_critical(SessionEvent::Notification(notification))
                        .await?;
                }
            }
        }
        Ok(None)
    }

    async fn expire_acknowledgements(
        &mut self,
        generation: ConnectionGeneration,
        deadlines: &mut BTreeMap<RequestId, PendingDeadline>,
    ) -> Result<(), SessionRunError> {
        let now = Instant::now();
        let expired = deadlines
            .iter()
            .filter_map(|(request_id, entry)| (entry.deadline <= now).then_some(*request_id))
            .collect::<Vec<_>>();
        for request_id in expired {
            let Some(entry) = deadlines.remove(&request_id) else {
                continue;
            };
            if !self
                .subscriptions
                .timeout_command(entry.service, entry.generation, request_id)
            {
                continue;
            }
            self.push_critical(SessionEvent::ServiceStatus {
                generation: Some(generation),
                service: entry.service,
                readiness: self.subscriptions.readiness(entry.service),
                cause: ServiceStatusCause::TimedOut,
            })
            .await?;
        }
        Ok(())
    }

    async fn drain_offline_controls(&mut self) -> Result<bool, SessionRunError> {
        loop {
            match self.controls.try_recv() {
                Ok(ControlMessage::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Ok(true);
                }
                Ok(ControlMessage::SetDesired { service, keys }) => {
                    self.subscriptions
                        .set_desired(service, keys.iter())
                        .map_err(SessionRunError::StateExhausted)?;
                    self.market_data
                        .retain_keys(service, keys.iter().map(str::to_owned).collect())
                        .await;
                }
                Ok(ControlMessage::Retry(_)) => {}
                Ok(ControlMessage::View(service)) => {
                    self.push_critical(SessionEvent::ServiceStatus {
                        generation: None,
                        service,
                        readiness: ServiceReadiness::Disconnected,
                        cause: ServiceStatusCause::RequestUnavailable,
                    })
                    .await?;
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(false),
            }
        }
    }

    async fn wait_before_reconnect(&mut self, attempt: u32) -> Result<bool, SessionRunError> {
        let deadline = Instant::now() + self.reconnect_delay(attempt);
        loop {
            tokio::select! {
                () = sleep_until(deadline) => return Ok(false),
                command = self.controls.recv() => {
                    let Some(command) = command else {
                        return Ok(true);
                    };
                    match command {
                        ControlMessage::Shutdown => return Ok(true),
                        ControlMessage::SetDesired { service, keys } => {
                            self.subscriptions
                                .set_desired(service, keys.iter())
                                .map_err(SessionRunError::StateExhausted)?;
                            self.market_data
                                .retain_keys(service, keys.iter().map(str::to_owned).collect())
                                .await;
                        }
                        ControlMessage::Retry(_) => {}
                        ControlMessage::View(service) => {
                            self.push_critical(SessionEvent::ServiceStatus {
                                generation: None,
                                service,
                                readiness: ServiceReadiness::Disconnected,
                                cause: ServiceStatusCause::RequestUnavailable,
                            }).await?;
                        }
                    }
                }
            }
        }
    }

    fn reconnect_delay(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let factor = 1u32 << shift;
        self.config
            .reconnect_initial_delay
            .saturating_mul(factor)
            .min(self.config.reconnect_max_delay)
    }

    async fn report_disconnect(
        &mut self,
        generation: ConnectionGeneration,
        failure: PortFailure,
    ) -> Result<(), SessionRunError> {
        self.subscriptions.disconnect();
        self.market_data.reset_generation(None).await;
        self.push_critical(SessionEvent::Connection {
            generation,
            connected: false,
            failure: Some(failure),
        })
        .await?;
        for service in crate::SERVICE_MANIFESTS.map(super::manifest::ServiceManifest::service) {
            let readiness = self.subscriptions.readiness(service);
            if readiness != ServiceReadiness::NotDesired {
                self.push_critical(SessionEvent::ServiceStatus {
                    generation: Some(generation),
                    service,
                    readiness,
                    cause: ServiceStatusCause::Disconnected,
                })
                .await?;
            }
        }
        Ok(())
    }

    async fn push_critical(&self, event: SessionEvent) -> Result<(), SessionRunError> {
        self.critical.push(event).await
    }

    async fn close_channels(&self) {
        self.critical.close().await;
        self.market_data.close().await;
    }
}

struct PendingDeadline {
    service: StreamerService,
    generation: ConnectionGeneration,
    deadline: Instant,
}

enum ConnectionEnd {
    Shutdown,
    Disconnected(PortFailure),
}

enum ControlMessage {
    SetDesired {
        service: StreamerService,
        keys: BoundedKeySet,
    },
    Retry(StreamerService),
    View(StreamerService),
    Shutdown,
}

impl ControlMessage {
    fn service(&self) -> StreamerService {
        match self {
            Self::SetDesired { service, .. } | Self::Retry(service) | Self::View(service) => {
                *service
            }
            Self::Shutdown => StreamerService::AcctActivity,
        }
    }
}

impl<F: AuthenticatedSessionFactory> Drop for StreamerRuntime<F> {
    fn drop(&mut self) {
        self.critical.close_without_waiting();
        self.market_data.close_without_waiting();
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

fn service_from_name(name: &str) -> Option<StreamerService> {
    match name {
        "ACCT_ACTIVITY" => Some(StreamerService::AcctActivity),
        "LEVELONE_EQUITIES" => Some(StreamerService::LevelOneEquities),
        "LEVELONE_OPTIONS" => Some(StreamerService::LevelOneOptions),
        _ => None,
    }
}

fn command_from_name(name: &str) -> Option<SubscriptionCommand> {
    match name {
        "SUBS" => Some(SubscriptionCommand::Subs),
        "ADD" => Some(SubscriptionCommand::Add),
        "UNSUBS" => Some(SubscriptionCommand::Unsubs),
        "VIEW" => Some(SubscriptionCommand::View),
        _ => None,
    }
}

fn request_id_from_wire(value: &str) -> Option<RequestId> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed.to_string() == value).then(|| RequestId::new(parsed))
}
