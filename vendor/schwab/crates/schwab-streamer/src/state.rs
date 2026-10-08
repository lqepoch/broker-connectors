//! Bounded desired/acknowledged state and command correlation fences.
//! 按服务维护有界的期望订阅状态和已确认状态。

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

use crate::command::{
    AckDisposition, AckIgnoreReason, CommandAcknowledgement, ConnectionGeneration, ReplayPlan,
    RequestId, ServiceReadiness, StreamerCommand, SubscriptionCommand,
};
use crate::manifest::{SERVICE_COUNT, SERVICE_MANIFESTS, StreamerService};

/// Maximum number of unique keys accepted for one service.
/// 中文摘要：每个 Streamer 服务允许的期望订阅 key 数量上限。
pub const MAX_KEYS_PER_SERVICE: usize = 4096;
/// Maximum number of input items consumed when creating one desired key set.
/// 中文摘要：一次 key 输入可遍历的最大元素数，防止去重前的大输入耗尽资源。
pub const MAX_KEY_INPUT_ITEMS: usize = 8192;
/// Maximum UTF-8 byte length of one key, after trimming outer whitespace.
/// 中文摘要：单个订阅 key 的最大 UTF-8 字节数。
pub const MAX_KEY_BYTES: usize = 256;
/// Maximum serialized byte length of a comma-separated service key list.
/// 中文摘要：排序和编码后的整组订阅 key 最大字节数。
pub const MAX_SERIALIZED_KEY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct BoundedKeySet {
    keys: BTreeSet<String>,
    serialized_bytes: usize,
}

impl BoundedKeySet {
    /// Validates a single wire key and returns its trimmed form without
    /// allocating. Blank input remains valid because `from_keys` ignores it.
    pub(crate) fn normalize_key(raw_key: &str) -> Result<&str, ServiceStateError> {
        let key = raw_key.trim();
        if key.is_empty() {
            return Ok(key);
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(ServiceStateError::KeyTooLong {
                limit: MAX_KEY_BYTES,
            });
        }
        if key.contains(',') || key.chars().any(char::is_control) {
            return Err(ServiceStateError::InvalidKey);
        }
        Ok(key)
    }

    pub(crate) fn from_keys<I, K>(keys: I) -> Result<Self, ServiceStateError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let mut result = Self::default();
        let mut input_items = 0usize;
        for raw_key in keys {
            input_items =
                input_items
                    .checked_add(1)
                    .ok_or(ServiceStateError::TooManyInputKeys {
                        limit: MAX_KEY_INPUT_ITEMS,
                    })?;
            if input_items > MAX_KEY_INPUT_ITEMS {
                return Err(ServiceStateError::TooManyInputKeys {
                    limit: MAX_KEY_INPUT_ITEMS,
                });
            }

            let key = Self::normalize_key(raw_key.as_ref())?;
            if key.is_empty() {
                continue;
            }

            if result.keys.contains(key) {
                continue;
            }
            if result.keys.len() >= MAX_KEYS_PER_SERVICE {
                return Err(ServiceStateError::TooManyKeys {
                    limit: MAX_KEYS_PER_SERVICE,
                });
            }

            let separator_bytes = usize::from(!result.keys.is_empty());
            result.serialized_bytes = result
                .serialized_bytes
                .checked_add(separator_bytes)
                .and_then(|size| size.checked_add(key.len()))
                .ok_or(ServiceStateError::KeySetTooLarge {
                    limit: MAX_SERIALIZED_KEY_BYTES,
                })?;
            if result.serialized_bytes > MAX_SERIALIZED_KEY_BYTES {
                return Err(ServiceStateError::KeySetTooLarge {
                    limit: MAX_SERIALIZED_KEY_BYTES,
                });
            }
            result.keys.insert(key.to_owned());
        }
        Ok(result)
    }

    fn from_static_key(key: &'static str) -> Self {
        Self {
            keys: BTreeSet::from([key.to_owned()]),
            serialized_bytes: key.len(),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        self.keys.iter().map(String::as_str)
    }

    pub(crate) fn to_csv(&self) -> String {
        let mut result = String::with_capacity(self.serialized_bytes);
        for (index, key) in self.keys.iter().enumerate() {
            if index != 0 {
                result.push(',');
            }
            result.push_str(key);
        }
        result
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn difference(&self, other: &Self) -> Self {
        let keys = self.keys.difference(&other.keys).cloned().collect();
        let mut result = Self {
            keys,
            serialized_bytes: 0,
        };
        result.recount_bytes();
        result
    }

    fn extend(&mut self, other: &Self) {
        for key in &other.keys {
            self.keys.insert(key.clone());
        }
        self.recount_bytes();
    }

    fn remove_all(&mut self, other: &Self) {
        for key in &other.keys {
            self.keys.remove(key);
        }
        self.recount_bytes();
    }

    fn recount_bytes(&mut self) {
        self.serialized_bytes = self
            .keys
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(self.keys.len().saturating_sub(1));
    }

    fn into_vec(self) -> Vec<String> {
        self.keys.into_iter().collect()
    }
}

#[derive(Clone)]
struct ServiceState {
    desired: Option<BoundedKeySet>,
    acknowledged: Option<BoundedKeySet>,
    desired_fields: Option<&'static str>,
    acknowledged_fields: Option<&'static str>,
    desired_revision: u64,
    pending: Option<StreamerCommand>,
    last_failure: bool,
    remote_state_unknown: bool,
}

impl ServiceState {
    fn new(service: StreamerService) -> Self {
        Self {
            desired: match service {
                StreamerService::AcctActivity => {
                    Some(BoundedKeySet::from_static_key("Account Activity"))
                }
                StreamerService::LevelOneEquities | StreamerService::LevelOneOptions => None,
            },
            acknowledged: None,
            desired_fields: match service {
                StreamerService::AcctActivity => Some(service.manifest().fields()),
                StreamerService::LevelOneEquities | StreamerService::LevelOneOptions => None,
            },
            acknowledged_fields: None,
            desired_revision: 0,
            pending: None,
            last_failure: false,
            remote_state_unknown: false,
        }
    }

    fn readiness(&self, connected: bool) -> ServiceReadiness {
        let Some(desired) = self.desired.as_ref() else {
            return ServiceReadiness::NotDesired;
        };
        if self.pending.is_some() {
            return ServiceReadiness::Pending;
        }
        if self.last_failure || self.remote_state_unknown {
            return ServiceReadiness::Degraded;
        }
        if !connected {
            if desired.is_empty() {
                return ServiceReadiness::Ready;
            }
            return ServiceReadiness::Disconnected;
        }
        if desired.is_empty()
            && self
                .acknowledged
                .as_ref()
                .is_some_and(BoundedKeySet::is_empty)
        {
            return ServiceReadiness::Ready;
        }
        match self.acknowledged.as_ref() {
            Some(acknowledged)
                if acknowledged == desired && self.acknowledged_fields == self.desired_fields =>
            {
                ServiceReadiness::Ready
            }
            _ => ServiceReadiness::NeedsSync,
        }
    }
}

/// Validation or sequencing failure from the bounded state machine.
/// 中文摘要：服务 key 集合输入超限、非法或请求编号耗尽时返回的固定错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStateError {
    /// The iterator supplied too many raw items (including duplicates/empty items).
    /// 不可信输入中的 key 数量超过配置上限。
    TooManyInputKeys {
        /// Maximum number of raw iterator items accepted before canonicalization.
        /// 规范化前最多接受的原始迭代项目数。
        limit: usize,
    },
    /// The canonical set contains too many unique keys.
    /// 规范化后的期望 key 集合超过项目数上限。
    TooManyKeys {
        /// Maximum number of unique canonical subscription keys.
        /// 规范化后最多允许的唯一订阅 key 数量。
        limit: usize,
    },
    /// One canonical key exceeded the per-key UTF-8 bound.
    /// 订阅 key 超出 UTF-8 字节上限。
    KeyTooLong {
        /// Maximum UTF-8 byte length of one canonical key.
        /// 单个规范化 key 允许的最大 UTF-8 字节数。
        limit: usize,
    },
    /// The comma-separated key payload exceeded its serialized byte bound.
    /// 逗号分隔后的 key 集合超过字节上限。
    KeySetTooLarge {
        /// Maximum serialized byte length of the comma-separated key set.
        /// 逗号分隔 key 集合序列化后的最大字节数。
        limit: usize,
    },
    /// A key contains a comma or control character and cannot be represented safely.
    /// 订阅 key 为空、有歧义或包含控制字符。
    InvalidKey,
    /// A desired non-empty subscription cannot be planned without an active connection.
    /// 没有活动连接代次时请求了依赖连接的状态变化。
    NotConnected,
    /// The local connection generation counter is exhausted.
    /// 单调连接代次计数器达到上限。
    GenerationExhausted,
    /// The local request ID counter is exhausted.
    /// 单调请求标识符计数器达到上限。
    RequestIdExhausted,
    /// The per-service desired revision counter is exhausted.
    /// 期望状态修订计数器达到上限。
    RevisionExhausted,
    /// Schwab's current contract does not support VIEW for account activity.
    /// 状态核心不协调 VIEW 结果。
    ViewUnsupported,
}

impl Display for ServiceStateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyInputKeys { limit } => {
                write!(formatter, "subscription input exceeds {limit} items")
            }
            Self::TooManyKeys { limit } => {
                write!(formatter, "subscription exceeds {limit} unique keys")
            }
            Self::KeyTooLong { limit } => {
                write!(formatter, "subscription key exceeds {limit} bytes")
            }
            Self::KeySetTooLarge { limit } => {
                write!(
                    formatter,
                    "serialized subscription keys exceed {limit} bytes"
                )
            }
            Self::InvalidKey => formatter.write_str("subscription key is not wire-safe"),
            Self::NotConnected => formatter.write_str("Streamer connection is not active"),
            Self::GenerationExhausted => formatter.write_str("connection generation exhausted"),
            Self::RequestIdExhausted => formatter.write_str("command request ID exhausted"),
            Self::RevisionExhausted => formatter.write_str("desired-state revision exhausted"),
            Self::ViewUnsupported => formatter.write_str("VIEW is unsupported for this service"),
        }
    }
}

impl Error for ServiceStateError {}

/// Offline desired/acknowledged subscription state for the three fixed services.
///
/// Key input is canonicalized to a sorted set and rejected atomically when a
/// count or byte bound is exceeded. There is at most one pending command per
/// service, so this type has no command queue. A caller must dispatch commands
/// returned by this type and pass parsed ACK metadata back to [`Self::acknowledge`].
/// 中文摘要：按服务独立管理、校验和计划订阅变更的状态机。
#[derive(Clone)]
pub struct ServiceSubscriptionManager {
    states: [ServiceState; SERVICE_COUNT],
    generation: Option<ConnectionGeneration>,
    next_generation: u64,
    next_request_id: Option<u64>,
}

impl Debug for ServiceSubscriptionManager {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceSubscriptionManager")
            .field("connection", &"[REDACTED]")
            .field("state", &"[REDACTED]")
            .finish()
    }
}

impl Default for ServiceSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceSubscriptionManager {
    /// Creates state with the mandatory account-activity key desired.
    ///
    /// Equity and option services remain undesired until configured explicitly.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    #[must_use]
    pub fn new() -> Self {
        Self {
            states: std::array::from_fn(|index| {
                ServiceState::new(SERVICE_MANIFESTS[index].service())
            }),
            generation: None,
            next_generation: 0,
            next_request_id: Some(1),
        }
    }

    /// Establishes a new logical connection generation and replays desired keys.
    ///
    /// A reconnect always clears acknowledged state and pending requests while
    /// preserving desired keys. Non-empty desired services receive fresh SUBS
    /// commands in deterministic service order. Empty desired services require
    /// no command on a new socket.
    /// 中文摘要：推进连接代次、清除连接内 ACK 状态，并为保留的期望 key 生成有界重放计划。
    ///
    /// # Errors
    /// Returns [`ServiceStateError::GenerationExhausted`] or
    /// [`ServiceStateError::RequestIdExhausted`] when a monotonic identifier
    /// cannot be advanced without wrapping.
    pub fn reconnect(&mut self) -> Result<ReplayPlan, ServiceStateError> {
        let generation_value = self
            .next_generation
            .checked_add(1)
            .ok_or(ServiceStateError::GenerationExhausted)?;
        let generation = ConnectionGeneration::new(generation_value);
        let command_count = self
            .states
            .iter()
            .filter(|state| state.desired.as_ref().is_some_and(|keys| !keys.is_empty()))
            .count();
        let login_request_value = self
            .next_request_id
            .ok_or(ServiceStateError::RequestIdExhausted)?;
        let login_request_id = RequestId::new(login_request_value);
        let next_after_login = login_request_value.checked_add(1);
        let (request_ids, next_request_id) =
            Self::reserve_request_ids(next_after_login, command_count)?;

        self.next_generation = generation_value;
        self.next_request_id = next_request_id;
        self.generation = Some(generation);

        let mut commands: [Option<StreamerCommand>; SERVICE_COUNT] = std::array::from_fn(|_| None);
        let mut request_index = 0usize;
        for service in SERVICE_MANIFESTS.map(super::manifest::ServiceManifest::service) {
            let index = service.index();
            let state = &mut self.states[index];
            state.pending = None;
            state.last_failure = false;
            state.remote_state_unknown = false;
            state.acknowledged = match state.desired.as_ref() {
                Some(keys) if keys.is_empty() => Some(BoundedKeySet::default()),
                _ => None,
            };
            state.acknowledged_fields =
                if state.desired.as_ref().is_some_and(BoundedKeySet::is_empty) {
                    state.desired_fields
                } else {
                    None
                };
            let Some(desired) = state.desired.as_ref().filter(|keys| !keys.is_empty()) else {
                continue;
            };
            let request_id =
                request_ids[request_index].ok_or(ServiceStateError::RequestIdExhausted)?;
            request_index += 1;
            let command = StreamerCommand::new(
                generation,
                request_id,
                service,
                SubscriptionCommand::Subs,
                state.desired_revision,
                desired.clone(),
            );
            state.pending = Some(command.clone());
            commands[index] = Some(command);
        }
        Ok(ReplayPlan::new(login_request_id, commands))
    }

    /// Marks the current connection absent and invalidates all ACK/pending state.
    /// Desired keys remain available for the next [`Self::reconnect`].
    /// 中文摘要：标记连接断开并清除待确认/已确认状态，同时保留期望订阅。
    pub fn disconnect(&mut self) {
        self.generation = None;
        for state in &mut self.states {
            state.acknowledged = None;
            state.acknowledged_fields = None;
            state.pending = None;
            state.last_failure = false;
            state.remote_state_unknown = false;
        }
    }

    /// Replaces a service's desired keys after validating the whole input.
    ///
    /// Input is bounded, trimmed, deduplicated, and sorted. If validation fails,
    /// the prior desired and acknowledged state is unchanged. Repeating the
    /// same canonical set is idempotent and does not advance its revision.
    /// 中文摘要：校验并替换单个服务的期望 key 集合；仅状态变化时增加修订号。
    ///
    /// # Errors
    /// Returns a key-validation error when the input exceeds a hard bound or
    /// contains an invalid key, and [`ServiceStateError::RevisionExhausted`]
    /// when the desired-state revision cannot be advanced.
    pub fn set_desired<I, K>(
        &mut self,
        service: StreamerService,
        keys: I,
    ) -> Result<u64, ServiceStateError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let desired = BoundedKeySet::from_keys(keys)?;
        let state = &mut self.states[service.index()];
        if state.desired.as_ref() == Some(&desired) {
            return Ok(state.desired_revision);
        }
        let revision = state
            .desired_revision
            .checked_add(1)
            .ok_or(ServiceStateError::RevisionExhausted)?;
        state.desired_revision = revision;
        state.desired = Some(desired.clone());
        state.desired_fields = Some(service.manifest().fields());
        state.last_failure = state.remote_state_unknown;

        if desired.is_empty()
            && self.generation.is_some()
            && state.pending.is_none()
            && state.acknowledged.is_none()
            && !state.remote_state_unknown
        {
            state.acknowledged = Some(BoundedKeySet::default());
            state.acknowledged_fields = state.desired_fields;
        }
        Ok(revision)
    }

    /// Plans the next minimal command for one service, if it needs syncing.
    ///
    /// Pure additions produce ADD, pure removals produce UNSUBS, and mixed
    /// changes produce a single replacement SUBS. A service has at most one
    /// outstanding command; desired changes made while one is pending are
    /// planned after its matching ACK.
    /// 中文摘要：规划下一条有界订阅变更，用于同步期望状态与当前 ACK 状态。
    ///
    /// # Errors
    /// Returns [`ServiceStateError::NotConnected`] when a non-empty desired
    /// state needs a connection, or [`ServiceStateError::RequestIdExhausted`]
    /// when the next request identifier cannot be reserved.
    pub fn next_command(
        &mut self,
        service: StreamerService,
    ) -> Result<Option<StreamerCommand>, ServiceStateError> {
        let index = service.index();
        if self.states[index].pending.is_some() {
            return Ok(None);
        }
        let Some(desired) = self.states[index].desired.clone() else {
            return Ok(None);
        };
        if desired.is_empty() {
            let state = &self.states[index];
            if state
                .acknowledged
                .as_ref()
                .is_some_and(BoundedKeySet::is_empty)
                || (self.generation.is_none() && state.acknowledged.is_none())
            {
                return Ok(None);
            }
            if state.acknowledged.is_none() && state.remote_state_unknown {
                // An ambiguous timed-out UNSUBS can leave any prior key active.
                // Empty SUBS semantics are not locally verified, so keep this
                // service degraded until a new socket generation proves an
                // empty initial state. Do not affect the shared connection or
                // peers.
                return Ok(None);
            }
        }
        let generation = self.generation.ok_or(ServiceStateError::NotConnected)?;
        let acknowledged = self.states[index].acknowledged.clone();
        let fields_match =
            self.states[index].desired_fields == self.states[index].acknowledged_fields;
        let desired_revision = self.states[index].desired_revision;
        let (command_kind, keys) = if acknowledged.as_ref() == Some(&desired) && !fields_match {
            (Some(SubscriptionCommand::Subs), Some(desired.clone()))
        } else {
            Self::next_change(&desired, acknowledged.as_ref())
        };
        let (Some(command_kind), Some(keys)) = (command_kind, keys) else {
            return Ok(None);
        };
        let (request_ids, next_request_id) = Self::reserve_request_ids(self.next_request_id, 1)?;
        let request_id = request_ids[0].ok_or(ServiceStateError::RequestIdExhausted)?;
        self.next_request_id = next_request_id;

        let command = StreamerCommand::new(
            generation,
            request_id,
            service,
            command_kind,
            desired_revision,
            keys,
        );
        self.states[index].pending = Some(command.clone());
        Ok(Some(command))
    }

    /// Plans a read-only VIEW request for a market-data service.
    ///
    /// Account activity is excluded because the current local contract says
    /// that service supports only SUBS and UNSUBS. VIEW uses the current
    /// desired key set (or an empty set if the service is not desired) and is
    /// correlated through the same one-pending-command fence.
    /// 中文摘要：为所选服务规划只读 `VIEW` 命令，不改变期望订阅。
    ///
    /// # Errors
    /// Returns [`ServiceStateError::ViewUnsupported`] for account activity,
    /// [`ServiceStateError::NotConnected`] without an active generation, or
    /// [`ServiceStateError::RequestIdExhausted`] when an identifier is spent.
    pub fn view_command(
        &mut self,
        service: StreamerService,
    ) -> Result<Option<StreamerCommand>, ServiceStateError> {
        if service == StreamerService::AcctActivity {
            return Err(ServiceStateError::ViewUnsupported);
        }
        let index = service.index();
        if self.states[index].pending.is_some() {
            return Ok(None);
        }
        let generation = self.generation.ok_or(ServiceStateError::NotConnected)?;
        let keys = self.states[index].desired.clone().unwrap_or_default();
        let desired_revision = self.states[index].desired_revision;
        let (request_ids, next_request_id) = Self::reserve_request_ids(self.next_request_id, 1)?;
        let request_id = request_ids[0].ok_or(ServiceStateError::RequestIdExhausted)?;
        self.next_request_id = next_request_id;
        let command = StreamerCommand::new(
            generation,
            request_id,
            service,
            SubscriptionCommand::View,
            desired_revision,
            keys,
        );
        self.states[index].pending = Some(command.clone());
        Ok(Some(command))
    }

    /// Expires a pending command only when its generation and request ID still
    /// match. A timeout degrades that service while leaving the connection and
    /// other services intact.
    /// 中文摘要：仅使匹配的待处理请求超时，并将服务恢复到可再次同步状态。
    pub fn timeout_command(
        &mut self,
        service: StreamerService,
        generation: ConnectionGeneration,
        request_id: RequestId,
    ) -> bool {
        self.invalidate_pending_command(service, generation, request_id)
    }

    fn invalidate_pending_command(
        &mut self,
        service: StreamerService,
        generation: ConnectionGeneration,
        request_id: RequestId,
    ) -> bool {
        if self.generation != Some(generation) {
            return false;
        }
        let state = &mut self.states[service.index()];
        let matches = state.pending.as_ref().is_some_and(|pending| {
            pending.connection_generation() == generation && pending.request_id() == request_id
        });
        if !matches {
            return false;
        }
        let was_mutation = state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.command() != SubscriptionCommand::View);
        state.pending = None;
        // A SUBS/ADD/UNSUBS with an unknown outcome may have reached the remote
        // service. The old acknowledged set is no longer a usable base for
        // another delta command; recovery must issue a complete SUBS snapshot.
        if was_mutation {
            state.acknowledged = None;
            state.acknowledged_fields = None;
            state.remote_state_unknown = true;
        }
        state.last_failure = true;
        true
    }

    /// Applies an ACK only when generation, request ID, service, and command match.
    /// Rejection or mismatched ACK affects no other service.
    /// 中文摘要：按连接代次、请求 ID 和命令精确匹配确认，只更新对应服务状态。
    pub fn acknowledge(&mut self, ack: CommandAcknowledgement) -> AckDisposition {
        if self.generation != Some(ack.connection_generation()) {
            return AckDisposition::Ignored(AckIgnoreReason::StaleConnection);
        }

        let service_index = ack.service().index();
        let Some(pending) = self.states[service_index].pending.as_ref() else {
            if self.states.iter().any(|state| {
                state
                    .pending
                    .as_ref()
                    .is_some_and(|item| item.request_id() == ack.request_id())
            }) {
                return AckDisposition::Ignored(AckIgnoreReason::WrongService);
            }
            return AckDisposition::Ignored(AckIgnoreReason::UnknownRequest);
        };
        if pending.request_id() != ack.request_id() {
            if self.states.iter().any(|state| {
                state
                    .pending
                    .as_ref()
                    .is_some_and(|item| item.request_id() == ack.request_id())
            }) {
                return AckDisposition::Ignored(AckIgnoreReason::WrongService);
            }
            return AckDisposition::Ignored(AckIgnoreReason::UnknownRequest);
        }
        if pending.command() != ack.command() {
            return AckDisposition::Ignored(AckIgnoreReason::WrongCommand);
        }

        let Some(completed) = self.states[service_index].pending.take() else {
            return AckDisposition::Ignored(AckIgnoreReason::UnknownRequest);
        };
        if !ack.accepted() {
            self.states[service_index].last_failure = true;
            return AckDisposition::Rejected;
        }

        self.states[service_index].last_failure = false;
        let acknowledged = match completed.command() {
            SubscriptionCommand::Subs => {
                self.states[service_index].remote_state_unknown = false;
                completed.key_set().clone()
            }
            SubscriptionCommand::Add => {
                let mut keys = self.states[service_index]
                    .acknowledged
                    .clone()
                    .unwrap_or_default();
                keys.extend(completed.key_set());
                keys
            }
            SubscriptionCommand::Unsubs => {
                let mut keys = self.states[service_index]
                    .acknowledged
                    .clone()
                    .unwrap_or_default();
                keys.remove_all(completed.key_set());
                keys
            }
            SubscriptionCommand::View => {
                return AckDisposition::Accepted {
                    readiness: self.readiness(ack.service()),
                };
            }
        };
        self.states[service_index].acknowledged = Some(acknowledged);
        self.states[service_index].acknowledged_fields = Some(completed.fields());
        AckDisposition::Accepted {
            readiness: self.readiness(ack.service()),
        }
    }

    /// Returns the current connection generation, if connected.
    /// 中文摘要：返回当前 socket 代次；断开时返回 `None`。
    #[must_use]
    pub const fn connection_generation(&self) -> Option<ConnectionGeneration> {
        self.generation
    }

    /// Returns the current readiness of one service.
    /// 中文摘要：返回当前连接代次的服务就绪状态。
    #[must_use]
    pub fn readiness(&self, service: StreamerService) -> ServiceReadiness {
        self.states[service.index()].readiness(self.generation.is_some())
    }

    /// Returns the current desired keys in deterministic sorted order.
    /// 中文摘要：借用调用方期望保留的键集合。
    pub fn desired_keys(&self, service: StreamerService) -> Option<Vec<String>> {
        self.states[service.index()]
            .desired
            .clone()
            .map(BoundedKeySet::into_vec)
    }

    /// Returns the immutable service field manifest currently desired.
    /// 中文摘要：该服务存在期望 key 时返回固定 manifest 字段。
    #[must_use]
    pub fn desired_fields(&self, service: StreamerService) -> Option<&'static str> {
        self.states[service.index()].desired_fields
    }

    /// Returns whether a key belongs to the current desired set.
    /// 中文摘要：检查 key 是否属于该服务当前期望集合。
    #[must_use]
    pub fn is_desired_key(&self, service: StreamerService, key: &str) -> bool {
        self.states[service.index()]
            .desired
            .as_ref()
            .is_some_and(|desired| desired.keys.contains(key))
    }

    /// Returns the current acknowledged keys in deterministic sorted order.
    /// 中文摘要：若当前连接已有确认，则返回 broker 已确认 key 集合的副本。
    pub fn acknowledged_keys(&self, service: StreamerService) -> Option<Vec<String>> {
        self.states[service.index()]
            .acknowledged
            .clone()
            .map(BoundedKeySet::into_vec)
    }

    /// Returns the service field manifest confirmed by the last matching ACK.
    /// 中文摘要：返回当前已确认订阅所用的固定 manifest 字段。
    #[must_use]
    pub fn acknowledged_fields(&self, service: StreamerService) -> Option<&'static str> {
        self.states[service.index()].acknowledged_fields
    }

    /// Returns the revision associated with the service's desired state.
    /// 中文摘要：返回该服务单调递增的期望状态修订号。
    #[must_use]
    pub fn desired_revision(&self, service: StreamerService) -> u64 {
        self.states[service.index()].desired_revision
    }

    /// Returns the currently pending immutable command for a service.
    /// 中文摘要：借用该服务正在等待匹配 ACK 或超时处理的命令。
    #[must_use]
    pub fn pending_command(&self, service: StreamerService) -> Option<&StreamerCommand> {
        self.states[service.index()].pending.as_ref()
    }

    fn next_change(
        desired: &BoundedKeySet,
        acknowledged: Option<&BoundedKeySet>,
    ) -> (Option<SubscriptionCommand>, Option<BoundedKeySet>) {
        let Some(acknowledged) = acknowledged else {
            return (Some(SubscriptionCommand::Subs), Some(desired.clone()));
        };
        if desired == acknowledged {
            return (None, None);
        }
        let additions = desired.difference(acknowledged);
        let removals = acknowledged.difference(desired);
        match (additions.is_empty(), removals.is_empty()) {
            (false, true) => (Some(SubscriptionCommand::Add), Some(additions)),
            (true, false) => (Some(SubscriptionCommand::Unsubs), Some(removals)),
            (false, false) => (Some(SubscriptionCommand::Subs), Some(desired.clone())),
            (true, true) => (None, None),
        }
    }

    fn reserve_request_ids(
        first: Option<u64>,
        count: usize,
    ) -> Result<([Option<RequestId>; SERVICE_COUNT], Option<u64>), ServiceStateError> {
        let mut request_ids = [None; SERVICE_COUNT];
        let mut next = first;
        for slot in request_ids.iter_mut().take(count) {
            let value = next.ok_or(ServiceStateError::RequestIdExhausted)?;
            *slot = Some(RequestId::new(value));
            next = value.checked_add(1);
        }
        Ok((request_ids, next))
    }
}
