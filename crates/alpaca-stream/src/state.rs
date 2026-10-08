//! Checked connection phases and bounded reconnect timing.
//!
//! 经校验的连接阶段与有界重连计时策略。

use std::time::Duration;

use crate::model::SessionPhase;

/// Maximum number of reconnect attempts allowed by one local session.
/// 单个本地会话允许的最大重连尝试数。
pub const MAX_RECONNECT_ATTEMPTS: u8 = 8;
/// Maximum local reconnect delay.
/// 本地重连等待时间最大值。
pub const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// Maximum configured jitter percentage around a capped exponential delay.
/// 指数退避等待时间允许配置的最大抖动百分比。
pub const MAX_RECONNECT_JITTER_PERCENT: u8 = 50;

/// Bounded exponential reconnect policy with deterministic injectable jitter.
/// 带有界指数退避与可注入确定性抖动的重连策略。
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    initial_delay: Duration,
    max_delay: Duration,
    max_attempts: u8,
    jitter_percent: u8,
}

impl ReconnectPolicy {
    /// Creates a reconnect policy that cannot exceed local attempt or delay ceilings.
    /// 创建不会超过本地尝试次数或等待时长上限的重连策略。
    ///
    /// # Errors
    ///
    /// Returns [`ReconnectPolicyError::InvalidPolicy`] if a bound is zero, inconsistent, or too large.
    pub fn new(
        initial_delay: Duration,
        max_delay: Duration,
        max_attempts: u8,
        jitter_percent: u8,
    ) -> Result<Self, ReconnectPolicyError> {
        let policy = Self {
            initial_delay,
            max_delay,
            max_attempts,
            jitter_percent,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub(crate) fn validate(&self) -> Result<(), ReconnectPolicyError> {
        if self.initial_delay.is_zero()
            || self.max_delay < self.initial_delay
            || self.max_delay > MAX_RECONNECT_DELAY
            || self.max_attempts == 0
            || self.max_attempts > MAX_RECONNECT_ATTEMPTS
            || self.jitter_percent > MAX_RECONNECT_JITTER_PERCENT
        {
            return Err(ReconnectPolicyError::InvalidPolicy);
        }
        Ok(())
    }

    /// Returns the maximum reconnect attempts after the initial connection.
    /// 返回首次连接之外允许的最大重连次数。
    #[must_use]
    pub fn max_attempts(&self) -> u8 {
        self.max_attempts
    }

    /// Returns the bounded initial reconnect delay.
    /// 返回有界重连初始等待时间。
    #[must_use]
    pub fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Returns the maximum reconnect delay.
    /// 返回重连等待时间上限。
    #[must_use]
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Returns the configured percentage of deterministic jitter.
    /// 返回配置的确定性抖动百分比。
    #[must_use]
    pub fn jitter_percent(&self) -> u8 {
        self.jitter_percent
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
            max_attempts: 5,
            jitter_percent: 20,
        }
    }
}

/// Stable reconnect policy construction failure.
/// 重连策略构造失败的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconnectPolicyError {
    /// The policy contains zero, inconsistent, or over-limit bounds.
    /// 策略包含零值、不一致或超限上界。
    InvalidPolicy,
}

/// Calculates one capped exponential retry delay using a caller-supplied jitter seed.
/// 使用调用方提供的抖动 seed 计算一段封顶的指数退避时长。
#[must_use]
pub fn reconnect_delay(policy: &ReconnectPolicy, retry_index: u8, jitter_seed: u64) -> Duration {
    let shift = u32::from(retry_index.min(63));
    let initial = policy.initial_delay.as_nanos();
    let capped = initial
        .saturating_mul(1u128.checked_shl(shift).unwrap_or(u128::MAX))
        .min(policy.max_delay.as_nanos());
    let jitter = capped.saturating_mul(u128::from(policy.jitter_percent)) / 100;
    let span = jitter.saturating_mul(2).saturating_add(1);
    let sample = u128::from(mix_seed(jitter_seed ^ u64::from(retry_index))) % span;
    let minimum = capped.saturating_sub(jitter).max(1_000_000);
    let maximum = capped
        .saturating_add(jitter)
        .min(policy.max_delay.as_nanos());
    let jittered = minimum.saturating_add(sample).min(maximum);
    duration_from_nanos(jittered)
}

fn mix_seed(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = u64::try_from(nanos / 1_000_000_000)
        .expect("Duration nanoseconds originate from a representable Duration bound");
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000)
        .expect("subsecond nanoseconds are strictly below one billion");
    Duration::new(seconds, subsec_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_and_jitter_never_exceed_configured_bounds() {
        let policy =
            ReconnectPolicy::new(Duration::from_millis(250), Duration::from_secs(3), 4, 50)
                .expect("valid bounded policy");
        for retry in 0..policy.max_attempts() {
            let first = reconnect_delay(&policy, retry, 0x236);
            let second = reconnect_delay(&policy, retry, 0x236);
            assert_eq!(first, second);
            assert!(!first.is_zero());
            assert!(first <= policy.max_delay());
        }
    }

    #[test]
    fn independently_injected_session_seeds_produce_replayable_jitter() {
        let policy = ReconnectPolicy::new(Duration::from_secs(1), Duration::from_secs(8), 4, 50)
            .expect("valid bounded policy");
        let first_seed = 0x236a_1aca_5eed;
        let second_seed = 0x236a_1aca_5eee;
        let first_schedule: Vec<_> = (0..policy.max_attempts())
            .map(|retry| reconnect_delay(&policy, retry, first_seed))
            .collect();
        let repeated_first_schedule: Vec<_> = (0..policy.max_attempts())
            .map(|retry| reconnect_delay(&policy, retry, first_seed))
            .collect();
        let second_schedule: Vec<_> = (0..policy.max_attempts())
            .map(|retry| reconnect_delay(&policy, retry, second_seed))
            .collect();

        assert_eq!(first_schedule, repeated_first_schedule);
        assert_ne!(first_schedule, second_schedule);
        assert!(
            first_schedule
                .iter()
                .all(|delay| *delay <= policy.max_delay())
        );
        assert!(
            second_schedule
                .iter()
                .all(|delay| *delay <= policy.max_delay())
        );
    }

    #[test]
    fn checked_phase_machine_rejects_skipping_acknowledgements() {
        let mut phases = PhaseMachine::new();
        assert_eq!(
            phases.transition(InternalPhase::Ready),
            Err(PhaseTransitionError::InvalidTransition)
        );
        assert!(phases.transition(InternalPhase::AwaitingConnected).is_ok());
        assert!(phases.transition(InternalPhase::SessionLost).is_ok());
        assert!(phases.transition(InternalPhase::Connecting).is_ok());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InternalPhase {
    Connecting,
    AwaitingConnected,
    Authenticating,
    AwaitingAuthentication,
    Subscribing,
    AwaitingSubscriptionAcknowledgement,
    AwaitingFreshData,
    Ready,
    SessionLost,
    Closed,
}

impl InternalPhase {
    pub(crate) fn public(self) -> SessionPhase {
        match self {
            Self::Connecting => SessionPhase::Connecting,
            Self::AwaitingConnected => SessionPhase::AwaitingConnected,
            Self::Authenticating => SessionPhase::Authenticating,
            Self::AwaitingAuthentication => SessionPhase::AwaitingAuthentication,
            Self::Subscribing => SessionPhase::Subscribing,
            Self::AwaitingSubscriptionAcknowledgement => {
                SessionPhase::AwaitingSubscriptionAcknowledgement
            }
            Self::AwaitingFreshData => SessionPhase::AwaitingFreshData,
            Self::Ready => SessionPhase::Ready,
            Self::SessionLost => SessionPhase::SessionLost,
            Self::Closed => SessionPhase::Closed,
        }
    }

    pub(crate) fn allows(self, next: Self) -> bool {
        use InternalPhase as Phase;
        matches!(
            (self, next),
            (
                Phase::Connecting,
                Phase::AwaitingConnected | Phase::SessionLost | Phase::Closed
            ) | (
                Phase::AwaitingConnected,
                Phase::Authenticating | Phase::SessionLost | Phase::Closed,
            ) | (
                Phase::Authenticating,
                Phase::AwaitingAuthentication | Phase::SessionLost | Phase::Closed,
            ) | (
                Phase::AwaitingAuthentication,
                Phase::Subscribing | Phase::SessionLost | Phase::Closed,
            ) | (
                Phase::Subscribing,
                Phase::AwaitingSubscriptionAcknowledgement | Phase::SessionLost | Phase::Closed,
            ) | (
                Phase::AwaitingSubscriptionAcknowledgement,
                Phase::AwaitingFreshData | Phase::SessionLost | Phase::Closed,
            ) | (
                Phase::AwaitingFreshData,
                Phase::Ready | Phase::SessionLost | Phase::Closed
            ) | (Phase::Ready, Phase::SessionLost | Phase::Closed)
                | (Phase::SessionLost, Phase::Connecting | Phase::Closed)
        )
    }
}

#[derive(Debug)]
pub(crate) struct PhaseMachine {
    phase: InternalPhase,
}

impl PhaseMachine {
    pub(crate) fn new() -> Self {
        Self {
            phase: InternalPhase::Connecting,
        }
    }

    pub(crate) fn phase(&self) -> InternalPhase {
        self.phase
    }

    pub(crate) fn transition(&mut self, next: InternalPhase) -> Result<(), PhaseTransitionError> {
        if !self.phase.allows(next) {
            return Err(PhaseTransitionError::InvalidTransition);
        }
        self.phase = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PhaseTransitionError {
    InvalidTransition,
}
