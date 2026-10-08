//! Immutable subscription commands and acknowledgements returned by the core.
//! 定义有界 Streamer 命令、确认和连接代次标识。

use crate::manifest::{SERVICE_COUNT, StreamerService};
use crate::state::BoundedKeySet;
use std::fmt::{self, Debug, Formatter};

/// Monotonic connection epoch owned by [`crate::ServiceSubscriptionManager`].
/// 中文摘要：用于拒绝旧 socket 回调的单调连接代次标记。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionGeneration(u64);

impl ConnectionGeneration {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric generation to attach to callbacks from this socket.
    /// 中文摘要：返回用于隔离不同 socket 生命周期的连接代次数值。
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Monotonic request identifier assigned to a planned command.
/// 中文摘要：在 LOGIN 与订阅命令之间关联响应的单调请求编号。
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId(u64);

impl RequestId {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric request identifier.
    /// 中文摘要：返回用于关联命令与确认的单调请求编号。
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the decimal representation expected by a JSON wire adapter.
    /// 中文摘要：生成供 JSON wire adapter 使用的十进制文本。
    #[must_use]
    pub fn as_wire_value(self) -> String {
        self.0.to_string()
    }
}

/// A command which this crate can plan for a service subscription.
/// 中文摘要：限定服务订阅状态操作：替换、增加、移除 key 或查询当前订阅。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionCommand {
    /// Replace the service's complete key set.
    /// 替换该服务的完整订阅 key 集合。
    Subs,
    /// Add only the listed keys.
    /// 只向该服务当前订阅集合增加指定 key。
    Add,
    /// Remove only the listed keys.
    /// 只从该服务当前订阅集合移除指定 key。
    Unsubs,
    /// Read the service's current subscriptions without changing desired state.
    /// 读取当前订阅集合，不改变期望状态。
    View,
}

impl SubscriptionCommand {
    /// Returns the exact Streamer command name.
    /// 中文摘要：返回 SUBS、ADD、UNSUBS 或 VIEW 的准确 wire 命令名。
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Subs => "SUBS",
            Self::Add => "ADD",
            Self::Unsubs => "UNSUBS",
            Self::View => "VIEW",
        }
    }
}

/// Bounded immutable payload and correlation data for a single command.
/// 中文摘要：绑定连接代次、请求编号、服务、命令、订阅修订号和有界 key 的不可变 wire 计划；Debug 会脱敏内容。
#[derive(Clone, Eq, PartialEq)]
pub struct StreamerCommand {
    connection_generation: ConnectionGeneration,
    request_id: RequestId,
    service: StreamerService,
    command: SubscriptionCommand,
    desired_revision: u64,
    keys: BoundedKeySet,
    fields: &'static str,
}

impl Debug for StreamerCommand {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerCommand")
            .field("connection_generation", &"[REDACTED]")
            .field("request_id", &"[REDACTED]")
            .field("service", &"[REDACTED]")
            .field("command", &"[REDACTED]")
            .field("desired_revision", &"[REDACTED]")
            .field("keys", &"[REDACTED]")
            .finish()
    }
}

impl StreamerCommand {
    pub(crate) const fn new(
        connection_generation: ConnectionGeneration,
        request_id: RequestId,
        service: StreamerService,
        command: SubscriptionCommand,
        desired_revision: u64,
        keys: BoundedKeySet,
    ) -> Self {
        Self {
            connection_generation,
            request_id,
            service,
            command,
            desired_revision,
            keys,
            fields: service.manifest().fields(),
        }
    }

    /// Returns the socket generation that must match the ACK.
    /// 中文摘要：返回该命令或 ACK 捕获的 socket 代次；旧代次不能推进当前状态。
    #[must_use]
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the request correlation ID.
    /// 中文摘要：返回用于将命令与 ACK 匹配的请求编号。
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the target service.
    /// 中文摘要：返回此命令或 ACK 指定的独立跟踪服务。
    #[must_use]
    pub const fn service(&self) -> StreamerService {
        self.service
    }

    /// Returns the planned mutation.
    /// 中文摘要：返回此命令或 ACK 表示的订阅操作。
    #[must_use]
    pub const fn command(&self) -> SubscriptionCommand {
        self.command
    }

    /// Returns the desired-state revision captured when this command was planned.
    /// 中文摘要：返回规划该命令时捕获的期望状态修订号。
    #[must_use]
    pub const fn desired_revision(&self) -> u64 {
        self.desired_revision
    }

    /// Returns the immutable field snapshot carried by this command.
    /// 中文摘要：返回该命令按固定服务 manifest 编码的字段列表。
    #[must_use]
    pub const fn fields(&self) -> &'static str {
        self.fields
    }

    /// Iterates the bounded key payload in deterministic sorted order.
    /// 中文摘要：按确定性顺序迭代命令携带的有界订阅 key。
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.keys.iter()
    }

    /// Returns the comma-separated key payload for a wire adapter.
    /// 中文摘要：将命令中的有界 key 编码为逗号分隔的 wire 值。
    #[must_use]
    pub fn keys_csv(&self) -> String {
        self.keys.to_csv()
    }

    pub(crate) fn key_set(&self) -> &BoundedKeySet {
        &self.keys
    }
}

/// Correlated response values extracted by an external wire adapter.
/// 中文摘要：外部 wire 适配器解析出的 ACK 字段；状态机仍须匹配代次、请求编号、服务和命令。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandAcknowledgement {
    connection_generation: ConnectionGeneration,
    request_id: RequestId,
    service: StreamerService,
    command: SubscriptionCommand,
    accepted: bool,
}

impl CommandAcknowledgement {
    /// Creates an ACK value from already parsed wire fields.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    #[must_use]
    pub const fn new(
        connection_generation: ConnectionGeneration,
        request_id: RequestId,
        service: StreamerService,
        command: SubscriptionCommand,
        accepted: bool,
    ) -> Self {
        Self {
            connection_generation,
            request_id,
            service,
            command,
            accepted,
        }
    }

    /// Returns the callback's socket generation.
    /// 中文摘要：返回该命令或 ACK 捕获的 socket 代次；旧代次不能推进当前状态。
    #[must_use]
    pub const fn connection_generation(self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the request correlation ID.
    /// 中文摘要：返回用于将命令与 ACK 匹配的请求编号。
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the service named by the response.
    /// 中文摘要：返回此命令或 ACK 指定的独立跟踪服务。
    #[must_use]
    pub const fn service(self) -> StreamerService {
        self.service
    }

    /// Returns the command named by the response.
    /// 中文摘要：返回此命令或 ACK 表示的订阅操作。
    #[must_use]
    pub const fn command(self) -> SubscriptionCommand {
        self.command
    }

    /// Returns whether the wire adapter classified the response as accepted.
    /// 中文摘要：返回 wire 适配器对 ACK 是否接受的分类；状态管理器仍须校验关联信息后才应用。
    #[must_use]
    pub const fn accepted(self) -> bool {
        self.accepted
    }
}

/// Readiness for one independently managed service.
/// 中文摘要：描述单个服务的期望订阅与当前连接确认状态，不表示数据新鲜度。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceReadiness {
    /// No key set has been requested for this service.
    /// 该服务当前没有期望订阅的 key。
    NotDesired,
    /// A non-empty desired state exists but there is no active connection.
    /// 服务当前没有连接 socket 上的有效确认订阅。
    Disconnected,
    /// A command is awaiting its matching ACK.
    /// 该服务有一条命令正在等待确认。
    Pending,
    /// Desired and acknowledged keys match on the current connection.
    /// 已确认 key 与当前期望状态一致。
    Ready,
    /// The desired state differs from broker ACK state and no command is pending.
    /// 服务的期望状态仍需同步。
    NeedsSync,
    /// The most recent matching command ACK rejected this service's request.
    /// 最近命令失败或超时；期望状态仍予保留。
    Degraded,
}

/// Why an ACK was ignored without changing service state.
/// 中文摘要：说明 ACK 因连接代次、请求编号、服务或命令不匹配而被丢弃的原因。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckIgnoreReason {
    /// The callback came from a previous or otherwise inactive socket generation.
    /// 确认属于较旧的连接代次。
    StaleConnection,
    /// No pending command matches the request ID.
    /// 请求标识符不匹配任何待确认命令。
    UnknownRequest,
    /// The request ID belongs to a different service.
    /// 确认消息标识了不同服务。
    WrongService,
    /// The request ID and service match, but the command does not.
    /// 确认消息标识了不同命令。
    WrongCommand,
}

/// Result of applying a response to the current pending-command fence.
/// 中文摘要：说明匹配 ACK 已推进状态、拒绝命令或因过期/不匹配被忽略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckDisposition {
    /// Matching ACK applied and service state advanced.
    /// 确认匹配并推进了对应服务状态。
    Accepted {
        /// Service readiness after the matching ACK was applied; readiness does not prove quote freshness.
        /// 应用匹配 ACK 后的服务就绪状态；就绪状态不证明报价新鲜度。
        readiness: ServiceReadiness,
    },
    /// Matching rejection cleared only this service's pending command.
    /// 匹配的确认拒绝了该命令；期望 key 仍予保留。
    Rejected,
    /// Stale or mismatched ACK; no state was changed.
    /// 确认已过时或与待确认命令不匹配。
    Ignored(AckIgnoreReason),
}

/// Fixed-size command collection emitted when a new connection is established.
/// 中文摘要：新连接上的 LOGIN 请求编号及按固定服务顺序生成的订阅重放命令。
#[derive(Clone, Eq, PartialEq)]
pub struct ReplayPlan {
    login_request_id: RequestId,
    commands: [Option<StreamerCommand>; SERVICE_COUNT],
}

impl Debug for ReplayPlan {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayPlan")
            .field("login_request_id", &"[REDACTED]")
            .field("commands", &"[REDACTED]")
            .finish()
    }
}

impl ReplayPlan {
    pub(crate) const fn new(
        login_request_id: RequestId,
        commands: [Option<StreamerCommand>; SERVICE_COUNT],
    ) -> Self {
        Self {
            login_request_id,
            commands,
        }
    }

    /// Returns the unique request identifier reserved for this socket's
    /// correlated `ADMIN/LOGIN` handshake.
    /// 中文摘要：返回为该连接的 `ADMIN/LOGIN` 握手预留的请求编号。
    #[must_use]
    pub const fn login_request_id(&self) -> RequestId {
        self.login_request_id
    }

    /// Returns the replay command for a service, if that service has desired keys.
    /// 中文摘要：读取
    #[must_use]
    pub fn get(&self, service: StreamerService) -> Option<&StreamerCommand> {
        self.commands[service.index()].as_ref()
    }

    /// Iterates replay commands in stable manifest order.
    /// 中文摘要：按固定服务 manifest 顺序迭代已生成的重放命令。
    pub fn iter(&self) -> impl Iterator<Item = &StreamerCommand> {
        self.commands.iter().filter_map(Option::as_ref)
    }
}
