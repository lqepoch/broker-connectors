//! Bounded, read-only broker account-event subscriptions.
//!
//! Events carry the complete account namespace, a subscription-instance ID,
//! process-local generation, provider sequence/time when available, and local
//! receive time. They are observations only: they do not replace an account
//! snapshot, durable cursor, or reconciliation owner.
//!
//! # 简体中文
//!
//! 本模块定义有界的只读券商账户事件订阅。
//!
//! 事件携带完整账户命名空间、订阅实例 ID、进程内代次、可用时的供应商序号/时间及本地接收时间。
//! 它们只是观察结果，不能替代账户快照、持久游标或对账 owner。

use crate::{
    BrokerPortError, PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace,
    ReadAdmissionProvenance,
};
use domain::AccountNamespace;
use market_contracts::UtcTimestamp;
use std::fmt;
use std::num::NonZeroU64;

/// Maximum local queue bound for one account-event subscription.
/// 单个账户事件订阅的最大本地队列上限。
pub const MAX_BROKER_EVENT_BUFFER: usize = 10_000;
const MAX_SUBSCRIPTION_ID_BYTES: usize = 128;

/// Positive connection generation local to one running process.
/// 单个运行进程内的正数连接代次。
///
/// Generations restart with the process and are not durable identifiers. Pair
/// them with [`BrokerEventSource::subscription_id`] and do not persist them as
/// globally unique cursors.
///
/// 进程重启后代次会重新开始，因此它不是持久标识。必须与
/// [`BrokerEventSource::subscription_id`] 配对，不能将其持久化为全局唯一游标。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BrokerEventGeneration(NonZeroU64);

impl BrokerEventGeneration {
    /// Create a positive process-local generation.
    /// 创建正数进程内代次。
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Return the generation number.
    /// 返回代次编号。
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Opaque identity for one event subscription within a process.
/// 一个进程内事件订阅的不透明身份。
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BrokerEventSubscriptionId(String);

impl BrokerEventSubscriptionId {
    /// Create a bounded subscription instance identifier.
    /// 创建有界订阅实例标识。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerEventSubscriptionRequestError::InvalidSubscriptionId`] when the
    /// identifier is empty, oversized, padded with edge whitespace, or contains controls.
    ///
    /// # 错误
    ///
    /// 标识为空、过长、含首尾空白或控制字符时，返回
    /// [`BrokerEventSubscriptionRequestError::InvalidSubscriptionId`]。
    pub fn new(value: impl Into<String>) -> Result<Self, BrokerEventSubscriptionRequestError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_SUBSCRIPTION_ID_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(BrokerEventSubscriptionRequestError::InvalidSubscriptionId);
        }
        Ok(Self(value))
    }

    /// Return the subscription ID for explicit correlation.
    /// 返回用于显式关联的订阅 ID。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BrokerEventSubscriptionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerEventSubscriptionId")
            .field("len_bytes", &self.0.len())
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Fixed account namespace, subscription instance, and local generation.
/// 固定的账户命名空间、订阅实例和本地代次。
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct BrokerEventSource {
    namespace: AccountNamespace,
    subscription_id: BrokerEventSubscriptionId,
    generation: BrokerEventGeneration,
    admission_provenance: ReadAdmissionProvenance,
}

impl fmt::Debug for BrokerEventSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerEventSource")
            .field("namespace", &"[REDACTED]")
            .field("subscription_id", &self.subscription_id)
            .field("generation", &self.generation)
            .field("admission_provenance", &"[REDACTED]")
            .finish()
    }
}

impl BrokerEventSource {
    /// Bind a validated subscription request to its process-local generation.
    /// 将已校验的订阅请求绑定到进程内代次。
    #[must_use]
    pub fn from_request<A: ReadAdmissionEvidence>(
        request: &BrokerEventSubscriptionRequest<A>,
        generation: BrokerEventGeneration,
    ) -> Self {
        Self {
            namespace: request.namespace.clone(),
            subscription_id: request.subscription_id.clone(),
            generation,
            admission_provenance: request.admission.provenance().clone(),
        }
    }

    /// Return the complete broker/environment/account identity.
    /// 返回完整券商/环境/账户身份。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }

    /// Return this subscription's process-local identity.
    /// 返回本订阅的进程内身份。
    #[must_use]
    pub const fn subscription_id(&self) -> &BrokerEventSubscriptionId {
        &self.subscription_id
    }

    /// Return this subscription's process-local generation.
    /// 返回本订阅的进程内代次。
    #[must_use]
    pub const fn generation(&self) -> BrokerEventGeneration {
        self.generation
    }

    /// Return the read-admission provenance bound to this subscription.
    /// 返回绑定到此订阅的读取准入来源。
    #[must_use]
    pub const fn admission_provenance(&self) -> &ReadAdmissionProvenance {
        &self.admission_provenance
    }
}

/// Sequence-continuity evidence for one provider event.
/// 一条供应商事件的序号连续性证据。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BrokerEventContinuity {
    /// The event immediately follows a previously observed provider sequence.
    /// 事件紧接已观察到的上一供应商序号。
    Contiguous {
        /// Previous sequence supplied by the provider.
        /// 供应商提供的前一序号。
        previous: u64,
        /// Current sequence supplied by the provider.
        /// 供应商提供的当前序号。
        current: u64,
    },
    /// A provider sequence discontinuity was observed.
    /// 检测到供应商序号不连续。
    Gap {
        /// Previous sequence before the gap.
        /// 缺口发生前的序号。
        previous: u64,
        /// Current sequence after the gap.
        /// 缺口后的当前序号。
        current: u64,
    },
    /// No provider sequence evidence was available.
    /// 没有可用供应商序号证据。
    Unknown,
}

/// Provider-neutral lifecycle control records for an event stream.
/// 事件流使用的供应商中立生命周期控制记录。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BrokerEventControl {
    /// The provider event stream was established.
    /// 已建立供应商事件流。
    Connected,
    /// The provider event stream stopped.
    /// 供应商事件流已停止。
    Disconnected,
    /// The adapter is rebuilding its local event view.
    /// adapter 正在重建本地事件视图。
    Resynchronizing,
    /// The consumer must read an authoritative snapshot to recover continuity.
    /// 消费方必须读取权威快照以恢复连续性。
    SnapshotRequired,
    /// The adapter observed provider-side lag or dropped event evidence.
    /// adapter 检测到供应商侧延迟或丢失事件证据。
    ProviderLag,
}

/// Market or lifecycle event payload delivered by a broker event source.
/// 券商事件源交付的市场或生命周期事件载荷。
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum BrokerEventPayload<E> {
    /// One normalized provider-neutral domain event.
    /// 一条规范化的供应商中立领域事件。
    Event(E),
    /// One explicit lifecycle or continuity control record.
    /// 一条显式生命周期或连续性控制记录。
    Control(BrokerEventControl),
}

impl<E> fmt::Debug for BrokerEventPayload<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event(_) => formatter.debug_tuple("Event").field(&"[REDACTED]").finish(),
            Self::Control(control) => formatter.debug_tuple("Control").field(control).finish(),
        }
    }
}

/// Provider-neutral event envelope with identity, timestamps, and sequence evidence.
/// 携带身份、时间戳和序号证据的供应商中立事件信封。
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct BrokerEventEnvelope<E> {
    source: BrokerEventSource,
    provider_sequence: Option<u64>,
    provider_timestamp: Option<UtcTimestamp>,
    received_at: UtcTimestamp,
    continuity: BrokerEventContinuity,
    payload: BrokerEventPayload<E>,
}

impl<E> fmt::Debug for BrokerEventEnvelope<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerEventEnvelope")
            .field("source", &self.source)
            .field("provider_sequence", &self.provider_sequence)
            .field("provider_timestamp", &self.provider_timestamp)
            .field("received_at", &self.received_at)
            .field("continuity", &self.continuity)
            .field("payload", &self.payload)
            .finish()
    }
}

/// Invalid provider event sequence evidence.
/// 供应商事件序号证据无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerEventEnvelopeError {
    /// Sequence values conflict with the asserted continuity state.
    /// 序号值与声明的连续性状态冲突。
    SequenceMismatch,
}

impl<E> BrokerEventEnvelope<E> {
    /// Create one event and validate supplied continuity evidence.
    /// 创建一条事件并校验提供的连续性证据。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerEventEnvelopeError::SequenceMismatch`] if the asserted
    /// continuity does not match the provider sequence values.
    ///
    /// # 错误
    ///
    /// 声明的连续性与供应商序号不一致时，返回
    /// [`BrokerEventEnvelopeError::SequenceMismatch`]。
    pub fn new(
        source: BrokerEventSource,
        provider_sequence: Option<u64>,
        provider_timestamp: Option<UtcTimestamp>,
        received_at: UtcTimestamp,
        continuity: BrokerEventContinuity,
        payload: BrokerEventPayload<E>,
    ) -> Result<Self, BrokerEventEnvelopeError> {
        let valid = match continuity {
            BrokerEventContinuity::Contiguous { previous, current } => {
                provider_sequence == Some(current) && previous.checked_add(1) == Some(current)
            }
            BrokerEventContinuity::Gap { previous, current } => {
                provider_sequence == Some(current)
                    && previous.checked_add(1).is_some_and(|next| current > next)
            }
            BrokerEventContinuity::Unknown => true,
        };
        if !valid {
            return Err(BrokerEventEnvelopeError::SequenceMismatch);
        }
        Ok(Self {
            source,
            provider_sequence,
            provider_timestamp,
            received_at,
            continuity,
            payload,
        })
    }

    /// Return the account and subscription identity for this event.
    /// 返回本事件的账户及订阅身份。
    #[must_use]
    pub const fn source(&self) -> &BrokerEventSource {
        &self.source
    }

    /// Return the provider event sequence when present.
    /// 若存在供应商事件序号，则返回该序号。
    #[must_use]
    pub const fn provider_sequence(&self) -> Option<u64> {
        self.provider_sequence
    }

    /// Borrow provider event time when reported.
    /// 若报告了供应商事件时间，则借用该时间。
    #[must_use]
    pub const fn provider_timestamp(&self) -> Option<&UtcTimestamp> {
        self.provider_timestamp.as_ref()
    }

    /// Borrow the local receive timestamp.
    /// 借用本地接收时间戳。
    #[must_use]
    pub const fn received_at(&self) -> &UtcTimestamp {
        &self.received_at
    }

    /// Return explicit continuity evidence without inferring missing sequences.
    /// 返回显式连续性证据，不推断缺失序号。
    #[must_use]
    pub const fn continuity(&self) -> BrokerEventContinuity {
        self.continuity
    }

    /// Borrow the normalized event or lifecycle control payload.
    /// 借用规范化事件或生命周期控制载荷。
    #[must_use]
    pub const fn payload(&self) -> &BrokerEventPayload<E> {
        &self.payload
    }
}

/// Explicit account-scoped request for one bounded event subscription.
/// 一个显式指定账户范围且有界的事件订阅请求。
pub struct BrokerEventSubscriptionRequest<A> {
    namespace: AccountNamespace,
    subscription_id: BrokerEventSubscriptionId,
    maximum_buffered_events: usize,
    admission: A,
}

/// Invalid broker-event subscription request.
/// 券商事件订阅请求无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerEventSubscriptionRequestError {
    /// The local event buffer is zero or exceeds its bound.
    /// 本地事件缓冲区为零或超过上限。
    InvalidBufferBound,
    /// The admission evidence belongs to another account namespace.
    /// 准入证据属于其他账户命名空间。
    AdmissionNamespaceMismatch,
    /// The subscription ID is empty, oversized, padded, or contains controls.
    /// 订阅 ID 为空、过长、带边缘空白或含控制字符。
    InvalidSubscriptionId,
}

impl<A: ReadAdmissionEvidence> BrokerEventSubscriptionRequest<A> {
    /// Create a subscription request with explicit namespace and buffer limits.
    /// 使用显式命名空间及缓冲上限创建订阅请求。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerEventSubscriptionRequestError::InvalidBufferBound`] when the
    /// buffer is zero or exceeds [`MAX_BROKER_EVENT_BUFFER`], or
    /// [`BrokerEventSubscriptionRequestError::AdmissionNamespaceMismatch`] when
    /// the admission evidence is not bound to `namespace`.
    ///
    /// # 错误
    ///
    /// 缓冲区为零或超过 [`MAX_BROKER_EVENT_BUFFER`] 时，返回
    /// [`BrokerEventSubscriptionRequestError::InvalidBufferBound`]；准入证据指向其他命名空间时，
    /// 返回 [`BrokerEventSubscriptionRequestError::AdmissionNamespaceMismatch`]。
    pub fn new(
        namespace: AccountNamespace,
        subscription_id: BrokerEventSubscriptionId,
        maximum_buffered_events: usize,
        admission: A,
    ) -> Result<Self, BrokerEventSubscriptionRequestError> {
        if maximum_buffered_events == 0 || maximum_buffered_events > MAX_BROKER_EVENT_BUFFER {
            return Err(BrokerEventSubscriptionRequestError::InvalidBufferBound);
        }
        if admission.namespace() != &ReadAdmissionNamespace::Account(namespace.clone()) {
            return Err(BrokerEventSubscriptionRequestError::AdmissionNamespaceMismatch);
        }
        Ok(Self {
            namespace,
            subscription_id,
            maximum_buffered_events,
            admission,
        })
    }

    /// Return the exact broker/environment/account namespace.
    /// 返回精确的券商/环境/账户命名空间。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }

    /// Return the subscription instance identifier.
    /// 返回订阅实例标识。
    #[must_use]
    pub const fn subscription_id(&self) -> &BrokerEventSubscriptionId {
        &self.subscription_id
    }

    /// Return the maximum number of locally buffered event records.
    /// 返回本地允许缓冲的最大事件记录数。
    #[must_use]
    pub const fn maximum_buffered_events(&self) -> usize {
        self.maximum_buffered_events
    }

    /// Borrow the exact runtime admission evidence for this subscription.
    /// 借用本订阅使用的精确运行时准入证据。
    #[must_use]
    pub const fn admission(&self) -> &A {
        &self.admission
    }
}

/// Pull-based, backpressured account-event stream.
/// 采用拉取式背压的账户事件流。
pub trait BrokerEventStream<E>: Send
where
    E: Send + Sync + 'static,
{
    /// Wait for the next ordered event; `None` means the stream has ended.
    ///
    /// Implementations must emit lifecycle and continuity controls before
    /// termination when they have evidence of disconnect, lag, or required
    /// resynchronization. A stream is not an account-state authority.
    ///
    /// 等待下一条有序事件；`None` 表示流已结束。
    ///
    /// 实现若检测到断连、延迟或需要重新同步，必须在终止前发出生命周期和连续性控制。事件流不是账户状态权威。
    fn next(&mut self) -> PortFuture<'_, Option<Result<BrokerEventEnvelope<E>, BrokerPortError>>>;
}

/// Boxed future that opens one event stream.
/// 用于建立一个事件流的装箱 future。
pub type BrokerEventStreamFuture<'a, E> =
    PortFuture<'a, Result<Box<dyn BrokerEventStream<E> + 'a>, BrokerPortError>>;

/// Read-only broker account-event subscription port.
/// 只读券商账户事件订阅端口。
///
/// `Event` must be a provider-neutral domain event (or control-capable enum),
/// never a raw SDK DTO. This contract carries no event-source implementation;
/// an adapter that lacks a documented event protocol must report unsupported
/// rather than returning an empty stream as success.
/// Implementations must also bound active sessions, aggregate buffered bytes,
/// and each normalized event size; the requested item-count bound alone is not
/// a memory bound for an unconstrained event type.
///
/// `Event` 必须是供应商中立领域事件（或包含控制事件的枚举），不能是原始 SDK DTO。本合同不包含
/// 事件源实现；没有文档化事件协议的 adapter 必须报告不支持，不能用空流冒充成功。
pub trait BrokerEventPort: Send + Sync {
    /// Shared provider-neutral account-event payload type.
    /// 共享的供应商中立账户事件载荷类型。
    type Event: Send + Sync + 'static;

    /// Runtime admission evidence from the trusted shared read-budget owner.
    /// 来自可信共享读取预算 owner 的运行时准入证据。
    type Admission: ReadAdmissionEvidence;

    /// Return the fixed complete namespace served by this source.
    /// 返回此数据源服务的固定完整命名空间。
    fn account_namespace(&self) -> &AccountNamespace;

    /// Open a bounded ordered event stream for the explicitly named account.
    /// 为显式命名账户打开有界有序事件流。
    ///
    /// Implementations reject a namespace mismatch before opening a provider
    /// session and enforce `maximum_buffered_events` on all internal buffering.
    ///
    /// 实现必须在打开供应商会话前拒绝命名空间不匹配，并对所有内部缓冲执行
    /// `maximum_buffered_events` 上限。
    fn subscribe(
        &self,
        request: BrokerEventSubscriptionRequest<Self::Admission>,
    ) -> BrokerEventStreamFuture<'_, Self::Event>;
}

#[cfg(test)]
mod tests {
    use super::{
        BrokerEventContinuity, BrokerEventControl, BrokerEventEnvelope, BrokerEventEnvelopeError,
        BrokerEventGeneration, BrokerEventPayload, BrokerEventPort, BrokerEventSource,
        BrokerEventStream, BrokerEventStreamFuture, BrokerEventSubscriptionId,
        BrokerEventSubscriptionRequest, BrokerEventSubscriptionRequestError,
        MAX_BROKER_EVENT_BUFFER,
    };
    use crate::{
        BrokerPortError, PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace,
        ReadAdmissionProvenance,
    };
    use domain::{AccountNamespace, AccountScope, BrokerEnvironment, ExecutionBrokerId};
    use market_contracts::UtcTimestamp;
    use std::collections::VecDeque;

    fn namespace() -> AccountNamespace {
        AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            BrokerEnvironment::Paper,
            AccountScope::new("synthetic-account").unwrap(),
        )
    }

    fn timestamp(second: u8) -> UtcTimestamp {
        UtcTimestamp::parse(&format!("2026-10-08T14:30:{second:02}Z")).unwrap()
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeAdmission {
        namespace: ReadAdmissionNamespace,
        provenance: ReadAdmissionProvenance,
    }

    impl FakeAdmission {
        fn for_namespace(namespace: AccountNamespace, decision: &str) -> Self {
            Self {
                namespace: ReadAdmissionNamespace::Account(namespace),
                provenance: ReadAdmissionProvenance::new("synthetic-read", "policy-v1", decision)
                    .unwrap(),
            }
        }
    }

    impl ReadAdmissionEvidence for FakeAdmission {
        fn namespace(&self) -> &ReadAdmissionNamespace {
            &self.namespace
        }

        fn provenance(&self) -> &ReadAdmissionProvenance {
            &self.provenance
        }
    }

    fn request(decision: &str) -> BrokerEventSubscriptionRequest<FakeAdmission> {
        let namespace = namespace();
        BrokerEventSubscriptionRequest::new(
            namespace.clone(),
            BrokerEventSubscriptionId::new("synthetic-subscription-1").unwrap(),
            4,
            FakeAdmission::for_namespace(namespace, decision),
        )
        .unwrap()
    }

    fn source() -> BrokerEventSource {
        BrokerEventSource::from_request(
            &request("decision-1"),
            BrokerEventGeneration::new(1).unwrap(),
        )
    }

    #[test]
    fn sequence_evidence_requires_exact_contiguous_or_gap_values() {
        let contiguous = BrokerEventEnvelope::new(
            source(),
            Some(2),
            Some(timestamp(1)),
            timestamp(2),
            BrokerEventContinuity::Contiguous {
                previous: 1,
                current: 2,
            },
            BrokerEventPayload::Event("synthetic-fill".to_owned()),
        )
        .unwrap();
        assert_eq!(contiguous.provider_sequence(), Some(2));
        assert_eq!(
            contiguous.continuity(),
            BrokerEventContinuity::Contiguous {
                previous: 1,
                current: 2
            }
        );
        assert_eq!(
            BrokerEventEnvelope::<String>::new(
                source(),
                Some(3),
                None,
                timestamp(3),
                BrokerEventContinuity::Contiguous {
                    previous: 1,
                    current: 3,
                },
                BrokerEventPayload::Event("synthetic-fill".to_owned()),
            ),
            Err(BrokerEventEnvelopeError::SequenceMismatch)
        );
        assert_eq!(
            BrokerEventEnvelope::<String>::new(
                source(),
                Some(4),
                None,
                timestamp(4),
                BrokerEventContinuity::Gap {
                    previous: 1,
                    current: 4,
                },
                BrokerEventPayload::Control(BrokerEventControl::SnapshotRequired),
            )
            .unwrap()
            .continuity(),
            BrokerEventContinuity::Gap {
                previous: 1,
                current: 4
            }
        );
    }

    #[test]
    fn debug_redacts_account_namespace_admission_and_event_payload() {
        let event = BrokerEventEnvelope::new(
            source(),
            None,
            None,
            timestamp(1),
            BrokerEventContinuity::Unknown,
            BrokerEventPayload::Event("synthetic-private-fill-detail".to_owned()),
        )
        .unwrap();
        let debug = format!("{event:?}");
        assert!(!debug.contains("synthetic-account"));
        assert!(!debug.contains("decision-1"));
        assert!(!debug.contains("synthetic-private-fill-detail"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn event_request_bounds_buffer_and_redacts_subscription_identity() {
        let subscription_id = BrokerEventSubscriptionId::new("synthetic-subscription-secret")
            .expect("synthetic subscription ID is valid");
        assert!(!format!("{subscription_id:?}").contains("synthetic-subscription-secret"));
        let account_namespace = namespace();
        let request = BrokerEventSubscriptionRequest::new(
            account_namespace.clone(),
            subscription_id.clone(),
            1,
            FakeAdmission::for_namespace(account_namespace, "decision-2"),
        )
        .unwrap();
        assert_eq!(request.maximum_buffered_events(), 1);
        for invalid in [0, MAX_BROKER_EVENT_BUFFER + 1] {
            assert!(matches!(
                BrokerEventSubscriptionRequest::new(
                    namespace(),
                    subscription_id.clone(),
                    invalid,
                    FakeAdmission::for_namespace(namespace(), "decision-3"),
                ),
                Err(BrokerEventSubscriptionRequestError::InvalidBufferBound)
            ));
        }
        let wrong_namespace = AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            BrokerEnvironment::Paper,
            AccountScope::new("other-synthetic-account").unwrap(),
        );
        assert!(matches!(
            BrokerEventSubscriptionRequest::new(
                namespace(),
                subscription_id,
                1,
                FakeAdmission::for_namespace(wrong_namespace, "decision-4"),
            ),
            Err(BrokerEventSubscriptionRequestError::AdmissionNamespaceMismatch)
        ));
    }

    #[tokio::test]
    async fn fake_event_port_emits_data_then_disconnect_without_credentials() {
        let port = FakeEventPort {
            namespace: namespace(),
        };
        let request = BrokerEventSubscriptionRequest::new(
            namespace(),
            BrokerEventSubscriptionId::new("synthetic-subscription-2").unwrap(),
            4,
            FakeAdmission::for_namespace(namespace(), "decision-stream"),
        )
        .unwrap();
        let mut stream = port.subscribe(request).await.unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(first.payload(), BrokerEventPayload::Event(_)));
        assert_eq!(
            first.source().admission_provenance().decision_id(),
            "decision-stream"
        );
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(
            second.payload(),
            &BrokerEventPayload::Control(BrokerEventControl::Disconnected)
        );
        assert!(stream.next().await.is_none());
    }

    struct FakeEventPort {
        namespace: AccountNamespace,
    }

    impl BrokerEventPort for FakeEventPort {
        type Event = String;
        type Admission = FakeAdmission;

        fn account_namespace(&self) -> &AccountNamespace {
            &self.namespace
        }

        fn subscribe(
            &self,
            request: BrokerEventSubscriptionRequest<Self::Admission>,
        ) -> BrokerEventStreamFuture<'_, Self::Event> {
            Box::pin(async move {
                if request.namespace() != &self.namespace {
                    return Err(BrokerPortError::ProtocolViolation);
                }
                let event_source = BrokerEventSource::from_request(
                    &request,
                    BrokerEventGeneration::new(1).unwrap(),
                );
                let records = VecDeque::from([
                    BrokerEventEnvelope::new(
                        event_source.clone(),
                        Some(10),
                        Some(timestamp(10)),
                        timestamp(11),
                        BrokerEventContinuity::Unknown,
                        BrokerEventPayload::Event("synthetic-fill".to_owned()),
                    )
                    .unwrap(),
                    BrokerEventEnvelope::new(
                        event_source,
                        None,
                        None,
                        timestamp(12),
                        BrokerEventContinuity::Unknown,
                        BrokerEventPayload::Control(BrokerEventControl::Disconnected),
                    )
                    .unwrap(),
                ]);
                Ok(Box::new(FakeEventStream { records }) as Box<dyn BrokerEventStream<String>>)
            })
        }
    }

    struct FakeEventStream<E> {
        records: VecDeque<BrokerEventEnvelope<E>>,
    }

    impl<E> BrokerEventStream<E> for FakeEventStream<E>
    where
        E: Send + Sync + 'static,
    {
        fn next(
            &mut self,
        ) -> PortFuture<'_, Option<Result<BrokerEventEnvelope<E>, BrokerPortError>>> {
            Box::pin(async move { self.records.pop_front().map(Ok) })
        }
    }
}
