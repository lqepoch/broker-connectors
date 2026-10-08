//! Explicitly enabled, synthetic-only execution port fake.
//!
//! This fake never opens a socket, reads credentials, or contacts a broker. Both its outcome
//! script and command journal have a caller-selected hard capacity. Capacity exhaustion is
//! surfaced and never drops an existing record.
//!
//! # 简体中文
//!
//! 仅在显式启用 feature 后可用的合成数据执行端口 fake。
//!
//! 此 fake 不打开 socket、不读取凭证，也不连接券商。结果脚本和命令记录都具有调用方指定的
//! 硬容量上限。容量不足会显式返回错误，不会静默丢弃已有记录。

use crate::{
    ExecutionCancelRequest, ExecutionCommand, ExecutionOutcome, ExecutionOutcomeReason,
    ExecutionPort, ExecutionReplaceRequest, ExecutionSubmitRequest, MAX_OFFLINE_FAKE_COMMANDS,
    MAX_OFFLINE_FAKE_RETAINED_BYTES, ProviderOrderAcknowledgement, ProviderOrderReplacementLink,
    accounted_command_bytes, accounted_provider_identity_bytes,
};
use domain::{ProviderOrderEvidence, ProviderOrderIdentity};
use std::collections::VecDeque;
use std::fmt;
use std::mem::size_of_val;
use std::sync::{Arc, Mutex, MutexGuard};

/// Scripted deterministic outcome used by the offline execution fake.
/// 离线执行 fake 使用的确定性模拟结果。
pub enum FakeExecutionOutcomePlan {
    /// Acknowledge a submit or cancel command with this synthetic provider identity.
    /// 使用此合成 provider 身份确认提交或撤单命令。
    Accepted(Box<ProviderOrderIdentity>),
    /// Acknowledge a replace with an identity and optional provider-reported predecessor.
    /// 使用订单身份和可选的 provider 前序字段确认改单。
    ReplaceAcknowledged {
        /// New or retained synthetic provider identity returned by the replace response.
        /// 改单响应返回的新身份或保留身份。
        provider_order: Box<ProviderOrderIdentity>,
        /// Exact predecessor identity reported by the provider when it assigned a new ID.
        /// Provider 分配新 ID 时报告的精确前序身份。
        replaces: Option<Box<ProviderOrderIdentity>>,
    },
    /// Prove the command was not sent.
    /// 确认命令没有发送。
    DefinitelyNotSent(ExecutionOutcomeReason),
    /// Return a definitive synthetic provider rejection.
    /// 返回合成 provider 明确拒绝。
    Rejected(ExecutionOutcomeReason),
    /// Return an uncertain outcome that requires reconciliation.
    /// 返回需要对账的未知结果。
    Unknown(ExecutionOutcomeReason),
}

impl FakeExecutionOutcomePlan {
    fn accounted_bytes(&self) -> usize {
        let mut bytes = size_of_val(self);
        match self {
            Self::Accepted(identity) => {
                bytes = bytes.saturating_add(accounted_provider_identity_bytes(identity));
            }
            Self::ReplaceAcknowledged {
                provider_order,
                replaces,
            } => {
                bytes = bytes.saturating_add(accounted_provider_identity_bytes(provider_order));
                if let Some(predecessor) = replaces {
                    bytes = bytes.saturating_add(accounted_provider_identity_bytes(predecessor));
                }
            }
            Self::DefinitelyNotSent(_) | Self::Rejected(_) | Self::Unknown(_) => {}
        }
        bytes
    }
}

/// Fake construction or bounded scripting failure.
/// Fake 构造或有界模拟脚本错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeExecutionPortError {
    /// The requested capacity is zero or exceeds the fixed safety bound.
    /// 容量为零或超过固定安全上限。
    InvalidCapacity,
    /// The outcome script reached its configured capacity.
    /// 结果脚本达到配置容量。
    ScriptCapacityExceeded,
    /// The aggregate accounted data budget for queued outcomes and recorded commands was reached.
    /// 已达到排队结果与命令记录的聚合数据计量上限。
    RetainedByteCapacityExceeded,
}

/// Synthetic execution port with separately bounded input script and command journal.
/// 分别限制输入脚本和命令记录容量的合成执行端口。
#[derive(Clone)]
pub struct FakeExecutionPort {
    inner: Arc<Mutex<FakeState>>,
}

struct FakeState {
    capacity: usize,
    retained_bytes: usize,
    command_bytes: usize,
    outcomes: VecDeque<FakeExecutionOutcomePlan>,
    commands: VecDeque<ExecutionCommand>,
}

impl FakeExecutionPort {
    /// Create a fake with explicit nonzero script and journal capacities.
    /// 使用显式非零脚本与记录容量创建 fake。
    ///
    /// # Errors
    ///
    /// Returns [`FakeExecutionPortError::InvalidCapacity`] unless `capacity` is in
    /// `1..=MAX_OFFLINE_FAKE_COMMANDS`.
    ///
    /// # 错误
    ///
    /// `capacity` 不在 `1..=MAX_OFFLINE_FAKE_COMMANDS` 时返回
    /// [`FakeExecutionPortError::InvalidCapacity`]。
    pub fn new(capacity: usize) -> Result<Self, FakeExecutionPortError> {
        if capacity == 0 || capacity > MAX_OFFLINE_FAKE_COMMANDS {
            return Err(FakeExecutionPortError::InvalidCapacity);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(FakeState {
                capacity,
                retained_bytes: capacity.saturating_mul(
                    std::mem::size_of::<ExecutionCommand>()
                        + std::mem::size_of::<FakeExecutionOutcomePlan>(),
                ),
                command_bytes: 0,
                outcomes: VecDeque::with_capacity(capacity),
                commands: VecDeque::with_capacity(capacity),
            })),
        })
    }

    /// Append one scripted result or report capacity exhaustion.
    /// 添加一个模拟结果；容量不足时显式报告。
    ///
    /// # Errors
    ///
    /// Returns [`FakeExecutionPortError::ScriptCapacityExceeded`] when the configured queue is full.
    pub fn script(&self, outcome: FakeExecutionOutcomePlan) -> Result<(), FakeExecutionPortError> {
        let mut state = lock_state(&self.inner);
        if state.outcomes.len() >= state.capacity {
            return Err(FakeExecutionPortError::ScriptCapacityExceeded);
        }
        let outcome_bytes = outcome.accounted_bytes();
        if state.retained_bytes.saturating_add(outcome_bytes) > MAX_OFFLINE_FAKE_RETAINED_BYTES {
            return Err(FakeExecutionPortError::RetainedByteCapacityExceeded);
        }
        state.retained_bytes = state.retained_bytes.saturating_add(outcome_bytes);
        state.outcomes.push_back(outcome);
        Ok(())
    }

    /// Drain all recorded synthetic commands without exceeding the configured bound.
    /// 排空记录的合成命令；记录数量不会超过配置上限。
    #[must_use]
    pub fn take_recorded_commands(&self) -> Vec<ExecutionCommand> {
        let mut state = lock_state(&self.inner);
        state.retained_bytes = state.retained_bytes.saturating_sub(state.command_bytes);
        state.command_bytes = 0;
        state.commands.drain(..).collect()
    }

    /// Return the number of scripted outcomes currently queued.
    /// 返回当前排队的模拟结果数。
    #[must_use]
    pub fn queued_outcomes(&self) -> usize {
        lock_state(&self.inner).outcomes.len()
    }

    /// Return the number of recorded commands currently retained.
    /// 返回当前保留的命令记录数。
    #[must_use]
    pub fn recorded_commands(&self) -> usize {
        lock_state(&self.inner).commands.len()
    }

    fn run(&self, command: &ExecutionCommand) -> ExecutionOutcome {
        let outcome = {
            let mut state = lock_state(&self.inner);
            let command_bytes = accounted_command_bytes(command);
            if state.commands.len() >= state.capacity
                || state.retained_bytes.saturating_add(command_bytes)
                    > MAX_OFFLINE_FAKE_RETAINED_BYTES
            {
                return ExecutionOutcome::definitely_not_sent(
                    ExecutionOutcomeReason::CapacityExceeded,
                );
            }
            state.retained_bytes = state.retained_bytes.saturating_add(command_bytes);
            state.command_bytes = state.command_bytes.saturating_add(command_bytes);
            state.commands.push_back(command.clone());
            let outcome = state.outcomes.pop_front();
            if let Some(outcome) = &outcome {
                state.retained_bytes = state
                    .retained_bytes
                    .saturating_sub(outcome.accounted_bytes());
            }
            outcome
        };

        match outcome {
            Some(FakeExecutionOutcomePlan::Accepted(provider_order)) => {
                let Some(revision) = command.expected_revision().checked_next() else {
                    return ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation);
                };
                let acknowledgement = match command {
                    ExecutionCommand::Submit(_) => {
                        ProviderOrderAcknowledgement::Submit(provider_order)
                    }
                    ExecutionCommand::Cancel(_) => {
                        ProviderOrderAcknowledgement::Cancel(provider_order)
                    }
                    ExecutionCommand::Replace(_) => {
                        return ExecutionOutcome::unknown(
                            ExecutionOutcomeReason::ProtocolViolation,
                        );
                    }
                };
                match ExecutionOutcome::accepted_for(command, acknowledgement, revision) {
                    Ok(outcome) => outcome,
                    Err(_) => ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation),
                }
            }
            Some(FakeExecutionOutcomePlan::ReplaceAcknowledged {
                provider_order,
                replaces,
            }) => {
                let Some(revision) = command.expected_revision().checked_next() else {
                    return ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation);
                };
                let ExecutionCommand::Replace(request) = command else {
                    return ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation);
                };
                let Some(predecessor) = known_identity(request.current()).cloned() else {
                    return ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation);
                };
                let link = if *provider_order == predecessor && replaces.is_none() {
                    Ok(ProviderOrderReplacementLink::same_identity(*provider_order))
                } else {
                    ProviderOrderReplacementLink::from_reported_replaces(
                        predecessor,
                        *provider_order,
                        replaces.map(|identity| *identity),
                    )
                };
                match link.and_then(|link| {
                    ExecutionOutcome::accepted_for(
                        command,
                        ProviderOrderAcknowledgement::Replace(Box::new(link)),
                        revision,
                    )
                }) {
                    Ok(outcome) => outcome,
                    Err(_) => ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation),
                }
            }
            Some(FakeExecutionOutcomePlan::DefinitelyNotSent(reason)) => {
                ExecutionOutcome::definitely_not_sent(reason)
            }
            Some(FakeExecutionOutcomePlan::Rejected(reason)) => ExecutionOutcome::rejected(reason),
            Some(FakeExecutionOutcomePlan::Unknown(reason)) => ExecutionOutcome::unknown(reason),
            None => ExecutionOutcome::unknown(ExecutionOutcomeReason::FakeScriptExhausted),
        }
    }
}

impl ExecutionPort for FakeExecutionPort {
    fn submit(
        &self,
        request: ExecutionSubmitRequest,
    ) -> crate::ExecutionFuture<'_, ExecutionOutcome> {
        Box::pin(async move { self.run(&ExecutionCommand::Submit(Box::new(request))) })
    }

    fn replace(
        &self,
        request: ExecutionReplaceRequest,
    ) -> crate::ExecutionFuture<'_, ExecutionOutcome> {
        Box::pin(async move { self.run(&ExecutionCommand::Replace(Box::new(request))) })
    }

    fn cancel(
        &self,
        request: ExecutionCancelRequest,
    ) -> crate::ExecutionFuture<'_, ExecutionOutcome> {
        Box::pin(async move { self.run(&ExecutionCommand::Cancel(Box::new(request))) })
    }
}

impl fmt::Debug for FakeExecutionOutcomePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::Accepted(_) => "Accepted",
            Self::ReplaceAcknowledged { .. } => "ReplaceAcknowledged",
            Self::DefinitelyNotSent(_) => "DefinitelyNotSent",
            Self::Rejected(_) => "Rejected",
            Self::Unknown(_) => "Unknown",
        };
        formatter
            .debug_struct("FakeExecutionOutcomePlan")
            .field("category", &category)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for FakeExecutionPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock_state(&self.inner);
        formatter
            .debug_struct("FakeExecutionPort")
            .field("capacity", &state.capacity)
            .field("queued_outcomes", &state.outcomes.len())
            .field("recorded_commands", &state.commands.len())
            .finish()
    }
}

impl ExecutionCommand {
    fn expected_revision(&self) -> domain::Revision {
        match self {
            Self::Submit(request) => request.expected_revision(),
            Self::Replace(request) => request.expected_revision(),
            Self::Cancel(request) => request.expected_revision(),
        }
    }
}

fn lock_state(inner: &Mutex<FakeState>) -> MutexGuard<'_, FakeState> {
    inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn known_identity(order: &domain::RoutedOrderIdentity) -> Option<&ProviderOrderIdentity> {
    match order.provider_order() {
        ProviderOrderEvidence::Known(identity) => Some(identity),
        ProviderOrderEvidence::NotAssigned
        | ProviderOrderEvidence::Unavailable(_)
        | ProviderOrderEvidence::Unknown(_) => None,
    }
}

#[cfg(test)]
mod tests;
