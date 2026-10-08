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
};
use domain::ProviderOrderIdentity;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

/// Scripted deterministic outcome used by the offline execution fake.
/// 离线执行 fake 使用的确定性模拟结果。
pub enum FakeExecutionOutcomePlan {
    /// Acknowledge the command with this synthetic provider identity.
    /// 使用此合成 provider 身份确认命令。
    Accepted(Box<ProviderOrderIdentity>),
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
}

/// Synthetic execution port with separately bounded input script and command journal.
/// 分别限制输入脚本和命令记录容量的合成执行端口。
#[derive(Clone)]
pub struct FakeExecutionPort {
    inner: Arc<Mutex<FakeState>>,
}

struct FakeState {
    capacity: usize,
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
        state.outcomes.push_back(outcome);
        Ok(())
    }

    /// Drain all recorded synthetic commands without exceeding the configured bound.
    /// 排空记录的合成命令；记录数量不会超过配置上限。
    #[must_use]
    pub fn take_recorded_commands(&self) -> Vec<ExecutionCommand> {
        lock_state(&self.inner).commands.drain(..).collect()
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
            if state.commands.len() >= state.capacity {
                return ExecutionOutcome::definitely_not_sent(
                    ExecutionOutcomeReason::CapacityExceeded,
                );
            }
            state.commands.push_back(command.clone());
            state.outcomes.pop_front()
        };

        match outcome {
            Some(FakeExecutionOutcomePlan::Accepted(provider_order)) => {
                let Some(revision) = command.expected_revision().checked_next() else {
                    return ExecutionOutcome::unknown(ExecutionOutcomeReason::ProtocolViolation);
                };
                match ExecutionOutcome::accepted_for(command, *provider_order, revision) {
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

#[cfg(test)]
mod tests;
