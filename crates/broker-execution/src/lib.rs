#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Provider-neutral execution command and outcome contracts.
//!
//! This module defines a software port only. It contains no broker writer, transport,
//! account authority, risk policy, outbox, or retry loop. A route is descriptive data and
//! never grants permission to submit. Applications remain responsible for authorization,
//! account ownership, durable intent state, and reconciliation before they call an adapter.
//! Unknown outcomes must be reconciled; they must never be retried automatically.
//!
//! # 简体中文
//!
//! 本模块定义供应商中立的软件执行端口合同。
//!
//! 本模块不包含券商写入器、传输、账户权威、风控策略、outbox 或重试循环。路由只是描述性数据，
//! 不会授予提交权限。应用仍负责调用 adapter 前的授权、账户归属、意图持久化和对账。
//! UNKNOWN 结果必须先对账，绝不能自动重试。

use domain::{
    BrokerEnvironment, ExecutionRoute, IntentId, LogicalOrderId, OptionComboIntent,
    ProviderOrderEvidence, ProviderOrderIdentity, Revision, RoutedOrderIdentity,
};
#[cfg(feature = "offline-fake")]
use domain::{
    BrokerNativeOrderReference, DeliverableAsset, ExecutionBrokerId, OptionExerciseStyle,
    OptionSettlementType, ProviderValue,
};
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
#[cfg(feature = "offline-fake")]
use std::mem::size_of_val;
use std::pin::Pin;

/// Boxed `Send` future used by the object-safe execution port.
/// object-safe 执行端口使用的装箱 `Send` future。
pub type ExecutionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Maximum number of option legs accepted by one provider-neutral execution request.
/// 单个供应商中立执行请求允许的最大期权腿数。
pub const MAX_EXECUTION_LEGS: usize = 16;

/// Maximum queued or recorded commands in the explicitly enabled offline fake.
/// 显式启用的离线 fake 最多排队或记录的命令数。
pub const MAX_OFFLINE_FAKE_COMMANDS: usize = 128;

/// Maximum accounted input and command-journal data retained by one offline fake.
/// 单个离线 fake 可保留的输入和命令日志计量数据上限。
pub const MAX_OFFLINE_FAKE_RETAINED_BYTES: usize = 1024 * 1024;

#[cfg(feature = "offline-fake")]
const MAX_CORE_IDENTIFIER_BYTES: usize = 256;

/// Validated submit request carrying one immutable core option-combo intent.
/// 携带一个不可变 core 期权组合意图的已校验提交请求。
#[derive(Clone)]
pub struct ExecutionSubmitRequest {
    intent: OptionComboIntent,
    expected_revision: Revision,
}

/// Validated replacement request tied to one known provider order and logical order lineage.
/// 绑定到一个已知供应商订单和逻辑订单链的已校验改单请求。
#[derive(Clone)]
pub struct ExecutionReplaceRequest {
    current: RoutedOrderIdentity,
    expected_revision: Revision,
    replacement: OptionComboIntent,
}

/// Validated cancellation request tied to one known provider order and revision.
/// 绑定到一个已知供应商订单及修订号的已校验撤单请求。
#[derive(Clone)]
pub struct ExecutionCancelRequest {
    current: RoutedOrderIdentity,
    expected_revision: Revision,
}

/// One immutable command passed to an execution port.
/// 传给执行端口的一个不可变命令。
#[derive(Clone)]
pub enum ExecutionCommand {
    /// Submit a new order intent.
    /// 提交新的订单意图。
    Submit(Box<ExecutionSubmitRequest>),
    /// Replace the current order with a new intent on the same logical order lineage.
    /// 在同一逻辑订单链上使用新意图替换当前订单。
    Replace(Box<ExecutionReplaceRequest>),
    /// Cancel the current provider order.
    /// 撤销当前供应商订单。
    Cancel(Box<ExecutionCancelRequest>),
}

/// Stable result classification for one execution command.
/// 单个执行命令的固定结果分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcomeCategory {
    /// The provider acknowledged the requested operation.
    /// Provider 已确认所请求的操作。
    Accepted,
    /// The command is known not to have reached the provider.
    /// 已确认命令没有到达 provider。
    DefinitelyNotSent,
    /// The provider definitively rejected the command.
    /// Provider 已明确拒绝命令。
    Rejected,
    /// The command may have reached the provider, but its result is not known.
    /// 命令可能已到达 provider，但结果未知。
    Unknown,
}

/// Fixed, low-cardinality reason for a non-accepted outcome.
/// 非成功结果使用的固定低基数原因。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcomeReason {
    /// The provider or operation is not supported by this adapter.
    /// 此 adapter 不支持该 provider 或操作。
    Unsupported,
    /// The adapter rejected the request before sending it.
    /// adapter 在发送前拒绝了请求。
    InvalidRequest,
    /// Local execution is disabled for this request.
    /// 此请求的本地执行功能已关闭。
    ExecutionDisabled,
    /// The bounded offline fake reached its configured capacity.
    /// 有界离线 fake 达到配置容量。
    CapacityExceeded,
    /// The provider definitively rejected the command.
    /// Provider 明确拒绝命令。
    ProviderRejected,
    /// The transport failed before a definitive provider response was received.
    /// 收到明确 provider 响应前传输失败。
    TransportFailure,
    /// The bounded operation deadline elapsed without a definitive response.
    /// 有界操作期限到期，仍没有明确响应。
    DeadlineElapsed,
    /// The provider response did not match the frozen command contract.
    /// Provider 响应与冻结的命令合同不匹配。
    ProtocolViolation,
    /// The adapter task stopped before a definitive result was available.
    /// adapter 任务在产生明确结果前停止。
    WorkerStopped,
    /// The offline fake has no scripted result for this command.
    /// 离线 fake 没有为此命令准备模拟结果。
    FakeScriptExhausted,
}

/// A classified execution result with private fields that prevent unvalidated acceptance.
/// 分类后的执行结果；字段私有，避免未校验结果被标为成功。
pub struct ExecutionOutcome {
    state: ExecutionOutcomeState,
}

enum ExecutionOutcomeState {
    // This remains unconstructible in the default build until a reviewed provider adapter exists.
    #[allow(dead_code)]
    Accepted {
        order: Box<RoutedOrderIdentity>,
        revision: Revision,
    },
    DefinitelyNotSent(ExecutionOutcomeReason),
    Rejected(ExecutionOutcomeReason),
    Unknown(ExecutionOutcomeReason),
}

/// Request construction failure that proves no provider command was sent.
/// 请求构造失败；可以确认没有发送 provider 命令。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionRequestError {
    /// Live routes are not accepted by this public software port contract.
    /// 此公共软件端口合同不接受 Live 路由。
    LiveDisabled,
    /// The order contains more than the bounded maximum number of legs.
    /// 订单腿数超过固定上限。
    TooManyLegs,
    /// The order repeats the same qualified instrument.
    /// 订单重复使用同一个完整核验合约。
    DuplicateInstrument,
    /// Current order evidence does not contain a known native provider identity.
    /// 当前订单证据未包含已知的供应商原生身份。
    ProviderIdentityUnknown,
    /// Replacement changes the fixed route or logical-order lineage.
    /// 替换请求改变固定路由或逻辑订单链。
    ReplacementLineageMismatch,
    /// The revision cannot safely advance without wrapping.
    /// 修订号无法在不回绕的情况下安全推进。
    RevisionExhausted,
}

/// Accepted-response validation failure.
/// 成功回执验证失败。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAcceptanceError {
    /// The acknowledgement kind does not match the submitted command kind.
    /// 回执类型与提交的命令类型不匹配。
    AcknowledgementKindMismatch,
    /// Returned provider identity belongs to another account namespace.
    /// 返回的供应商身份属于另一个账户命名空间。
    NamespaceMismatch,
    /// Returned provider identity changed during a cancel acknowledgement.
    /// 撤单确认期间返回的供应商身份发生变化。
    CancelIdentityMismatch,
    /// A changed replacement identity omitted explicit predecessor linkage.
    /// 更换后的订单身份缺少明确的前序订单关联。
    ReplaceLinkMissing,
    /// The replacement link names a different predecessor than the known current identity.
    /// 替换关联指向的前序身份与已知当前身份不同。
    ReplacePredecessorMismatch,
    /// Returned local revision is not the checked successor of the request revision.
    /// 返回的本地修订不是请求修订的安全后继值。
    RevisionMismatch,
    /// Returned intent, logical order, or fixed route differs from the request.
    /// 返回的意图、逻辑订单或固定路由与请求不同。
    LineageMismatch,
    /// The route or response unexpectedly names the Live environment.
    /// 路由或响应意外指向 Live 环境。
    LiveDisabled,
}

/// Provider identity evidence bound to an accepted command kind.
/// 与成功命令类型绑定的供应商身份证据。
#[cfg(any(test, feature = "offline-fake"))]
enum ProviderOrderAcknowledgement {
    /// Provider assigned an identity to a submitted order.
    /// Provider 为新提交订单分配了身份。
    #[allow(dead_code)]
    Submit(Box<ProviderOrderIdentity>),
    /// Provider acknowledged cancellation without changing the known order identity.
    /// Provider 确认撤单，并保持已知订单身份不变。
    Cancel(Box<ProviderOrderIdentity>),
    /// Provider acknowledged a replacement with an explicit old-to-new identity link.
    /// Provider 确认改单，并提供明确的旧身份到新身份关联。
    Replace(Box<ProviderOrderReplacementLink>),
}

/// Explicit provider identity transition for one replace acknowledgement.
/// 单个改单回执中的明确供应商身份转换。
#[cfg(any(test, feature = "offline-fake"))]
struct ProviderOrderReplacementLink {
    predecessor: ProviderOrderIdentity,
    replacement: ProviderOrderIdentity,
}

#[cfg(any(test, feature = "offline-fake"))]
impl ProviderOrderReplacementLink {
    /// Record a replace acknowledgement that retains the current provider identity.
    /// 记录保留当前供应商身份的改单回执。
    fn same_identity(identity: ProviderOrderIdentity) -> Self {
        Self {
            predecessor: identity.clone(),
            replacement: identity,
        }
    }

    /// Validate a provider-reported `replaces` parent for a new identity.
    /// 校验新身份对应的 provider `replaces` 前序字段。
    fn from_reported_replaces(
        predecessor: ProviderOrderIdentity,
        replacement: ProviderOrderIdentity,
        reported_predecessor: Option<ProviderOrderIdentity>,
    ) -> Result<Self, ExecutionAcceptanceError> {
        let reported_predecessor =
            reported_predecessor.ok_or(ExecutionAcceptanceError::ReplaceLinkMissing)?;
        if reported_predecessor != predecessor {
            return Err(ExecutionAcceptanceError::ReplacePredecessorMismatch);
        }
        if predecessor.account_namespace() != replacement.account_namespace() {
            return Err(ExecutionAcceptanceError::NamespaceMismatch);
        }
        Ok(Self {
            predecessor,
            replacement,
        })
    }
}

/// Provider-neutral submit, replace, and cancel command boundary.
/// 供应商中立的提交、改单和撤单命令边界。
///
/// Implementations must preserve command classification. In particular, a transport timeout or
/// disconnect after sending is [`ExecutionOutcomeCategory::Unknown`], never `Rejected` and never
/// an automatic retry. This trait grants no authorization; its caller must own admission, risk,
/// durable intent/outbox, and reconciliation.
///
/// 实现必须保留命令结果分类。发送后超时或断连属于
/// [`ExecutionOutcomeCategory::Unknown`]，不能归类为 `Rejected`，也不能自动重试。
/// 本 trait 不授予授权；调用方必须拥有准入、风控、持久意图/outbox 和对账能力。
pub trait ExecutionPort: Send + Sync {
    /// Submit one validated immutable option-combo intent.
    /// 提交一个已校验且不可变的期权组合意图。
    fn submit(&self, request: ExecutionSubmitRequest) -> ExecutionFuture<'_, ExecutionOutcome>;

    /// Replace one known provider order at an exact expected revision.
    /// 按精确预期修订替换一个已知供应商订单。
    fn replace(&self, request: ExecutionReplaceRequest) -> ExecutionFuture<'_, ExecutionOutcome>;

    /// Cancel one known provider order at an exact expected revision.
    /// 按精确预期修订撤销一个已知供应商订单。
    fn cancel(&self, request: ExecutionCancelRequest) -> ExecutionFuture<'_, ExecutionOutcome>;
}

impl ExecutionSubmitRequest {
    /// Create a bounded Paper-route request from the frozen core intent and expected revision.
    /// 根据冻结 core 意图和预期修订创建有界 Paper 路由请求。
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionRequestError::LiveDisabled`] for a Live route, or a bounded leg /
    /// duplicate-instrument error when the intent exceeds this port's accepted shape.
    ///
    /// # 错误
    ///
    /// Live 路由返回 [`ExecutionRequestError::LiveDisabled`]；超出腿数或重复合约时返回对应的固定错误。
    pub fn new(
        intent: OptionComboIntent,
        expected_revision: Revision,
    ) -> Result<Self, ExecutionRequestError> {
        validate_intent(&intent)?;
        expected_revision
            .checked_next()
            .ok_or(ExecutionRequestError::RevisionExhausted)?;
        Ok(Self {
            intent,
            expected_revision,
        })
    }

    /// Borrow the exact core intent passed to the provider port.
    /// 借用传给 provider 端口的精确 core 意图。
    #[must_use]
    pub const fn intent(&self) -> &OptionComboIntent {
        &self.intent
    }

    /// Return the caller-owned expected order revision.
    /// 返回调用方拥有的预期订单修订。
    #[must_use]
    pub const fn expected_revision(&self) -> Revision {
        self.expected_revision
    }
}

impl ExecutionReplaceRequest {
    /// Create a replacement on the same Paper route and logical-order lineage.
    /// 在相同 Paper 路由和逻辑订单链上创建替换请求。
    ///
    /// The replacement may use a new intent ID. The accepted response must bind to the new intent
    /// and existing logical order, route, account namespace, and next revision. If the provider
    /// assigns a new native order ID, its reported predecessor must exactly match the known order.
    /// 替换请求可以使用新的 intent ID。成功回执必须绑定新意图和原逻辑订单、路由、账户命名空间及下一修订。若 provider 分配新的原生订单 ID，报告的前序身份必须与已知当前订单完全一致。
    ///
    /// # Errors
    ///
    /// Returns a fixed request error when the current identity is unknown, the route is Live, the
    /// replacement exceeds its bounds or changes lineage, or the revision cannot advance.
    pub fn new(
        current: RoutedOrderIdentity,
        expected_revision: Revision,
        replacement: OptionComboIntent,
    ) -> Result<Self, ExecutionRequestError> {
        validate_known_paper_identity(&current)?;
        validate_intent(&replacement)?;
        if current.route() != replacement.route()
            || current.logical_order_id() != replacement.logical_order_id()
        {
            return Err(ExecutionRequestError::ReplacementLineageMismatch);
        }
        expected_revision
            .checked_next()
            .ok_or(ExecutionRequestError::RevisionExhausted)?;
        Ok(Self {
            current,
            expected_revision,
            replacement,
        })
    }

    /// Borrow the known current order identity.
    /// 借用已知的当前订单身份。
    #[must_use]
    pub const fn current(&self) -> &RoutedOrderIdentity {
        &self.current
    }

    /// Return the expected order revision supplied by the application authority.
    /// 返回应用权威提供的预期订单修订。
    #[must_use]
    pub const fn expected_revision(&self) -> Revision {
        self.expected_revision
    }

    /// Borrow the replacement intent.
    /// 借用替换意图。
    #[must_use]
    pub const fn replacement(&self) -> &OptionComboIntent {
        &self.replacement
    }
}

impl ExecutionCancelRequest {
    /// Create a cancellation for one known Paper order and exact expected revision.
    /// 为一个已知 Paper 订单和精确预期修订创建撤单请求。
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionRequestError::LiveDisabled`],
    /// [`ExecutionRequestError::ProviderIdentityUnknown`], or
    /// [`ExecutionRequestError::RevisionExhausted`] when validation fails.
    pub fn new(
        current: RoutedOrderIdentity,
        expected_revision: Revision,
    ) -> Result<Self, ExecutionRequestError> {
        validate_known_paper_identity(&current)?;
        expected_revision
            .checked_next()
            .ok_or(ExecutionRequestError::RevisionExhausted)?;
        Ok(Self {
            current,
            expected_revision,
        })
    }

    /// Borrow the known current order identity.
    /// 借用已知的当前订单身份。
    #[must_use]
    pub const fn current(&self) -> &RoutedOrderIdentity {
        &self.current
    }

    /// Return the expected order revision supplied by the application authority.
    /// 返回应用权威提供的预期订单修订。
    #[must_use]
    pub const fn expected_revision(&self) -> Revision {
        self.expected_revision
    }
}

impl ExecutionCommand {
    /// Return the fixed command kind without exposing provider or account data.
    /// 返回固定命令类型，不暴露 provider 或账户数据。
    #[must_use]
    pub const fn category(&self) -> ExecutionCommandCategory {
        match self {
            Self::Submit(_) => ExecutionCommandCategory::Submit,
            Self::Replace(_) => ExecutionCommandCategory::Replace,
            Self::Cancel(_) => ExecutionCommandCategory::Cancel,
        }
    }

    #[allow(dead_code)]
    fn expected_binding(&self) -> (&IntentId, &LogicalOrderId, &ExecutionRoute, Revision) {
        match self {
            Self::Submit(request) => (
                request.intent.intent_id(),
                request.intent.logical_order_id(),
                request.intent.route(),
                request.expected_revision,
            ),
            Self::Replace(request) => (
                request.replacement.intent_id(),
                request.replacement.logical_order_id(),
                request.replacement.route(),
                request.expected_revision,
            ),
            Self::Cancel(request) => (
                request.current.intent_id(),
                request.current.logical_order_id(),
                request.current.route(),
                request.expected_revision,
            ),
        }
    }
}

/// Fixed provider-neutral command kind.
/// 固定的供应商中立命令类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionCommandCategory {
    /// Submit a new intent.
    /// 提交新意图。
    Submit,
    /// Replace an existing order.
    /// 替换现有订单。
    Replace,
    /// Cancel an existing order.
    /// 撤销现有订单。
    Cancel,
}

impl ExecutionOutcome {
    /// Construct an accepted outcome only after validating every request-bound identity field.
    /// 仅在校验所有请求绑定身份字段后，构造成功结果。
    ///
    /// # Errors
    ///
    /// Returns a fixed protocol validation error when acknowledgement kind, provider namespace,
    /// cancel identity, replacement linkage, next revision, or command lineage does not match.
    #[cfg(any(test, feature = "offline-fake"))]
    #[allow(dead_code)]
    pub(crate) fn accepted_for(
        command: &ExecutionCommand,
        acknowledgement: ProviderOrderAcknowledgement,
        revision: Revision,
    ) -> Result<Self, ExecutionAcceptanceError> {
        let (intent_id, logical_order_id, route, expected_revision) = command.expected_binding();
        if route.account_namespace().environment() == BrokerEnvironment::Live {
            return Err(ExecutionAcceptanceError::LiveDisabled);
        }
        let expected_next = expected_revision
            .checked_next()
            .ok_or(ExecutionAcceptanceError::RevisionMismatch)?;
        if revision != expected_next {
            return Err(ExecutionAcceptanceError::RevisionMismatch);
        }
        let provider_order = match (command, acknowledgement) {
            (ExecutionCommand::Submit(_), ProviderOrderAcknowledgement::Submit(identity)) => {
                *identity
            }
            (ExecutionCommand::Cancel(request), ProviderOrderAcknowledgement::Cancel(identity)) => {
                if known_identity(request.current()) != Some(identity.as_ref()) {
                    return Err(ExecutionAcceptanceError::CancelIdentityMismatch);
                }
                *identity
            }
            (ExecutionCommand::Replace(request), ProviderOrderAcknowledgement::Replace(link)) => {
                let predecessor = known_identity(request.current())
                    .ok_or(ExecutionAcceptanceError::ReplacePredecessorMismatch)?;
                if &link.predecessor != predecessor {
                    return Err(ExecutionAcceptanceError::ReplacePredecessorMismatch);
                }
                if link.predecessor.account_namespace() != route.account_namespace()
                    || link.replacement.account_namespace() != route.account_namespace()
                {
                    return Err(ExecutionAcceptanceError::NamespaceMismatch);
                }
                link.replacement
            }
            _ => return Err(ExecutionAcceptanceError::AcknowledgementKindMismatch),
        };
        if provider_order.account_namespace() != route.account_namespace() {
            return Err(ExecutionAcceptanceError::NamespaceMismatch);
        }
        let order = RoutedOrderIdentity::new(
            intent_id.clone(),
            logical_order_id.clone(),
            route.clone(),
            ProviderOrderEvidence::Known(provider_order),
        )
        .map_err(|_| ExecutionAcceptanceError::LineageMismatch)?;
        Ok(Self {
            state: ExecutionOutcomeState::Accepted {
                order: Box::new(order),
                revision,
            },
        })
    }

    /// Construct a definitive local outcome that guarantees the provider did not receive the command.
    /// 构造确定的本地结果，保证 provider 未收到命令。
    #[must_use]
    pub const fn definitely_not_sent(reason: ExecutionOutcomeReason) -> Self {
        Self {
            state: ExecutionOutcomeState::DefinitelyNotSent(reason),
        }
    }

    /// Construct a definitive provider rejection.
    /// 构造 provider 明确拒绝结果。
    #[must_use]
    pub const fn rejected(reason: ExecutionOutcomeReason) -> Self {
        Self {
            state: ExecutionOutcomeState::Rejected(reason),
        }
    }

    /// Construct an uncertain result that requires caller-owned reconciliation.
    /// 构造需要调用方对账的未知结果。
    #[must_use]
    pub const fn unknown(reason: ExecutionOutcomeReason) -> Self {
        Self {
            state: ExecutionOutcomeState::Unknown(reason),
        }
    }

    /// Return the fixed classification of this outcome.
    /// 返回此结果的固定分类。
    #[must_use]
    pub const fn category(&self) -> ExecutionOutcomeCategory {
        match self.state {
            ExecutionOutcomeState::Accepted { .. } => ExecutionOutcomeCategory::Accepted,
            ExecutionOutcomeState::DefinitelyNotSent(_) => {
                ExecutionOutcomeCategory::DefinitelyNotSent
            }
            ExecutionOutcomeState::Rejected(_) => ExecutionOutcomeCategory::Rejected,
            ExecutionOutcomeState::Unknown(_) => ExecutionOutcomeCategory::Unknown,
        }
    }

    /// Return the bounded, low-cardinality reason for a non-accepted result.
    /// 返回非成功结果有界且低基数的原因。
    #[must_use]
    pub const fn reason(&self) -> Option<ExecutionOutcomeReason> {
        match self.state {
            ExecutionOutcomeState::Accepted { .. } => None,
            ExecutionOutcomeState::DefinitelyNotSent(reason)
            | ExecutionOutcomeState::Rejected(reason)
            | ExecutionOutcomeState::Unknown(reason) => Some(reason),
        }
    }

    /// Borrow the validated routed identity only when the provider acknowledged the command.
    /// 仅在 provider 已确认命令时借用已校验的路由身份。
    #[must_use]
    pub fn accepted_order(&self) -> Option<&RoutedOrderIdentity> {
        match &self.state {
            ExecutionOutcomeState::Accepted { order, .. } => Some(order.as_ref()),
            ExecutionOutcomeState::DefinitelyNotSent(_)
            | ExecutionOutcomeState::Rejected(_)
            | ExecutionOutcomeState::Unknown(_) => None,
        }
    }

    /// Return the caller-owned revision only when the provider acknowledged the command.
    /// 仅在 provider 已确认命令时返回调用方拥有的修订号。
    #[must_use]
    pub const fn accepted_revision(&self) -> Option<Revision> {
        match self.state {
            ExecutionOutcomeState::Accepted { revision, .. } => Some(revision),
            ExecutionOutcomeState::DefinitelyNotSent(_)
            | ExecutionOutcomeState::Rejected(_)
            | ExecutionOutcomeState::Unknown(_) => None,
        }
    }
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionOutcome")
            .field("category", &self.category())
            .field("reason", &self.reason())
            .field(
                "accepted_order",
                &self.accepted_order().map(|_| "[REDACTED]"),
            )
            .field("accepted_revision", &self.accepted_revision())
            .finish()
    }
}

impl fmt::Debug for ExecutionSubmitRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionSubmitRequest")
            .field("intent", &"[REDACTED]")
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

impl fmt::Debug for ExecutionReplaceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionReplaceRequest")
            .field("current", &"[REDACTED]")
            .field("expected_revision", &self.expected_revision)
            .field("replacement", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for ExecutionCancelRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionCancelRequest")
            .field("current", &"[REDACTED]")
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

impl fmt::Debug for ExecutionCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionCommand")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}

fn validate_intent(intent: &OptionComboIntent) -> Result<(), ExecutionRequestError> {
    if intent.route().account_namespace().environment() == BrokerEnvironment::Live {
        return Err(ExecutionRequestError::LiveDisabled);
    }
    if intent.legs().len() > MAX_EXECUTION_LEGS {
        return Err(ExecutionRequestError::TooManyLegs);
    }
    let mut instruments = HashSet::with_capacity(intent.legs().len());
    if intent
        .legs()
        .iter()
        .any(|leg| !instruments.insert(leg.instrument()))
    {
        return Err(ExecutionRequestError::DuplicateInstrument);
    }
    Ok(())
}

fn validate_known_paper_identity(
    current: &RoutedOrderIdentity,
) -> Result<(), ExecutionRequestError> {
    if current.route().account_namespace().environment() == BrokerEnvironment::Live {
        return Err(ExecutionRequestError::LiveDisabled);
    }
    if known_identity(current).is_none() {
        return Err(ExecutionRequestError::ProviderIdentityUnknown);
    }
    Ok(())
}

fn known_identity(order: &RoutedOrderIdentity) -> Option<&ProviderOrderIdentity> {
    match order.provider_order() {
        ProviderOrderEvidence::Known(identity) => Some(identity),
        ProviderOrderEvidence::NotAssigned
        | ProviderOrderEvidence::Unavailable(_)
        | ProviderOrderEvidence::Unknown(_) => None,
    }
}

#[cfg(feature = "offline-fake")]
fn accounted_command_bytes(command: &ExecutionCommand) -> usize {
    let mut bytes = size_of_val(command);
    match command {
        ExecutionCommand::Submit(request) => {
            bytes = add_bytes(bytes, size_of_val(request.as_ref()));
            add_bytes(bytes, accounted_intent_bytes(request.intent()))
        }
        ExecutionCommand::Replace(request) => {
            bytes = add_bytes(bytes, size_of_val(request.as_ref()));
            bytes = add_bytes(bytes, accounted_routed_order_bytes(request.current()));
            add_bytes(bytes, accounted_intent_bytes(request.replacement()))
        }
        ExecutionCommand::Cancel(request) => {
            bytes = add_bytes(bytes, size_of_val(request.as_ref()));
            add_bytes(bytes, accounted_routed_order_bytes(request.current()))
        }
    }
}

#[cfg(feature = "offline-fake")]
fn accounted_intent_bytes(intent: &OptionComboIntent) -> usize {
    let mut bytes = size_of_val(intent);
    bytes = add_bytes(bytes, intent.intent_id().as_str().len());
    bytes = add_bytes(bytes, intent.logical_order_id().as_str().len());
    bytes = add_bytes(bytes, accounted_route_bytes(intent.route()));
    for leg in intent.legs() {
        bytes = add_bytes(bytes, size_of_val(leg));
        bytes = add_bytes(bytes, accounted_instrument_bytes(leg.instrument()));
    }
    bytes
}

#[cfg(feature = "offline-fake")]
fn accounted_route_bytes(route: &ExecutionRoute) -> usize {
    let mut bytes = size_of_val(route);
    bytes = add_bytes(bytes, route.strategy_instance_id().as_str().len());
    bytes = add_bytes(bytes, size_of_val(route.account_namespace()));
    bytes = add_bytes(
        bytes,
        route.account_namespace().account_scope().as_str().len(),
    );
    add_bytes(
        bytes,
        accounted_execution_broker_bytes(route.account_namespace().broker()),
    )
}

#[cfg(feature = "offline-fake")]
fn accounted_execution_broker_bytes(broker: &ExecutionBrokerId) -> usize {
    match broker {
        ExecutionBrokerId::Other(code) => code.as_str().len(),
        ExecutionBrokerId::Alpaca
        | ExecutionBrokerId::Schwab
        | ExecutionBrokerId::InteractiveBrokers => 0,
    }
}

#[cfg(feature = "offline-fake")]
fn accounted_instrument_bytes(instrument: &domain::InstrumentKey) -> usize {
    let option = instrument.as_option();
    let contract = option.contract();
    let mut bytes = size_of_val(instrument);
    bytes = add_bytes(bytes, size_of_val(option));
    bytes = add_bytes(bytes, size_of_val(contract));
    bytes = add_bytes(bytes, size_of_val(contract.symbol()));
    bytes = add_bytes(bytes, size_of_val(contract.deliverable()));
    bytes = add_bytes(bytes, contract.symbol().underlying().as_str().len());
    bytes = add_bytes(bytes, contract.currency().as_str().len());
    bytes = add_bytes(bytes, option.trading_class().as_str().len());
    for component in contract.deliverable().components() {
        bytes = add_bytes(bytes, size_of_val(component));
        bytes = add_bytes(
            bytes,
            match component.asset() {
                DeliverableAsset::Equity(underlying) => underlying.as_str().len(),
                DeliverableAsset::Cash(currency) => currency.as_str().len(),
            },
        );
    }
    bytes = add_bytes(
        bytes,
        match option.exercise_style() {
            OptionExerciseStyle::Other(code) => code.as_str().len(),
            OptionExerciseStyle::American
            | OptionExerciseStyle::European
            | OptionExerciseStyle::Bermudan => 0,
        },
    );
    add_bytes(
        bytes,
        match option.settlement_type() {
            OptionSettlementType::Other(code) => code.as_str().len(),
            OptionSettlementType::Physical | OptionSettlementType::Cash => 0,
        },
    )
}

#[cfg(feature = "offline-fake")]
fn accounted_routed_order_bytes(order: &RoutedOrderIdentity) -> usize {
    let mut bytes = size_of_val(order);
    bytes = add_bytes(bytes, order.intent_id().as_str().len());
    bytes = add_bytes(bytes, order.logical_order_id().as_str().len());
    bytes = add_bytes(bytes, accounted_route_bytes(order.route()));
    match order.provider_order() {
        ProviderOrderEvidence::Known(identity) => {
            add_bytes(bytes, accounted_provider_identity_bytes(identity))
        }
        ProviderOrderEvidence::Unknown(reference) => add_bytes(
            bytes,
            accounted_metadata_reference_bytes(reference.metadata()),
        ),
        ProviderOrderEvidence::NotAssigned | ProviderOrderEvidence::Unavailable(_) => bytes,
    }
}

#[cfg(feature = "offline-fake")]
fn accounted_provider_identity_bytes(identity: &ProviderOrderIdentity) -> usize {
    let mut bytes = size_of_val(identity);
    bytes = add_bytes(bytes, size_of_val(identity.account_namespace()));
    bytes = add_bytes(
        bytes,
        identity.account_namespace().account_scope().as_str().len(),
    );
    bytes = add_bytes(
        bytes,
        accounted_execution_broker_bytes(identity.account_namespace().broker()),
    );
    add_bytes(
        bytes,
        accounted_native_order_reference_bytes(identity.native()),
    )
}

#[cfg(feature = "offline-fake")]
fn accounted_native_order_reference_bytes(reference: &BrokerNativeOrderReference) -> usize {
    match reference {
        BrokerNativeOrderReference::Schwab(value) => size_of_val(value)
            .saturating_add(value.account_hash().as_str().len())
            .saturating_add(value.order_id().as_str().len()),
        BrokerNativeOrderReference::InteractiveBrokers(value) => {
            // Core 0a2eaff bounds this identifier at 256 bytes but does not expose its text.
            let bytes = size_of_val(value).saturating_add(MAX_CORE_IDENTIFIER_BYTES);
            let bytes = match value.permanent_id() {
                ProviderValue::Unknown(reference) => {
                    add_bytes(bytes, accounted_metadata_reference_bytes(reference))
                }
                ProviderValue::Known(_) | ProviderValue::Unavailable(_) => bytes,
            };
            match value.order_ref() {
                ProviderValue::Known(order_ref) => add_bytes(bytes, order_ref.as_str().len()),
                ProviderValue::Unknown(reference) => {
                    add_bytes(bytes, accounted_metadata_reference_bytes(reference))
                }
                ProviderValue::Unavailable(_) => bytes,
            }
        }
        BrokerNativeOrderReference::Alpaca(value) => {
            size_of_val(value).saturating_add(value.as_str().len())
        }
        BrokerNativeOrderReference::Other(value) => {
            let bytes = size_of_val(value);
            let bytes = add_bytes(bytes, value.order_id().as_str().len());
            add_bytes(bytes, accounted_execution_broker_bytes(value.broker()))
        }
    }
}

#[cfg(feature = "offline-fake")]
fn accounted_metadata_reference_bytes(reference: &domain::ProviderMetadataRef) -> usize {
    let mut bytes = size_of_val(reference);
    bytes = add_bytes(bytes, reference.record_id().as_str().len());
    let source_bytes = match reference.source() {
        domain::MetadataSource::Execution(ExecutionBrokerId::Other(code)) => code.as_str().len(),
        domain::MetadataSource::MarketData(domain::MarketDataProviderId::Other(code)) => {
            code.as_str().len()
        }
        domain::MetadataSource::Execution(
            ExecutionBrokerId::Alpaca
            | ExecutionBrokerId::Schwab
            | ExecutionBrokerId::InteractiveBrokers,
        )
        | domain::MetadataSource::MarketData(
            domain::MarketDataProviderId::Alpaca
            | domain::MarketDataProviderId::Schwab
            | domain::MarketDataProviderId::InteractiveBrokers,
        ) => 0,
    };
    add_bytes(bytes, source_bytes)
}

#[cfg(feature = "offline-fake")]
fn add_bytes(total: usize, additional: usize) -> usize {
    total.saturating_add(additional)
}

#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;

#[cfg(feature = "offline-fake")]
mod fake;

#[cfg(feature = "offline-fake")]
pub use fake::{FakeExecutionOutcomePlan, FakeExecutionPort, FakeExecutionPortError};
