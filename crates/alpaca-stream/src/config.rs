//! Allowlisted stream endpoints, option feed identity, subscriptions, and local resource bounds.
//!
//! allowlist WebSocket endpoint、期权 feed 身份、订阅集合与本地资源上限。

use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::time::Duration;

use crate::state::ReconnectPolicy;

/// Maximum UTF-8 bytes in one option contract symbol accepted by this crate.
/// 本 crate 接受的单个期权合约代码最大 UTF-8 字节数。
pub const MAX_OPTION_SYMBOL_BYTES: usize = 64;
/// Maximum requested contract count in either one channel.
/// 每个行情 channel 可请求的合约数量上限；这是本地资源限制，不代表 provider 套餐额度。
pub const MAX_DESIRED_SYMBOLS_PER_CHANNEL: usize = 4_096;
/// Maximum local connection deadline for one WebSocket attempt.
/// 单次 WebSocket 尝试允许配置的本地连接期限上限。
pub const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum local subscription acknowledgement deadline.
/// 本地订阅回执期限上限。
pub const MAX_ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum provider timestamp age accepted for the readiness gate.
/// ready gate 接受的 provider 时间戳最大年龄。
pub const MAX_READY_AGE: Duration = Duration::from_secs(60);
/// Maximum provider timestamp skew accepted into the future.
/// provider 时间戳允许的最大未来偏差。
pub const MAX_READY_FUTURE_SKEW: Duration = Duration::from_secs(5);

/// One approved Alpaca Market Data WebSocket environment.
/// Alpaca Market Data WebSocket 的允许环境。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamEnvironment {
    /// The production market-data endpoint.
    /// 正式行情 endpoint。
    Production,
    /// The Alpaca sandbox market-data endpoint.
    /// Alpaca sandbox 行情 endpoint。
    Sandbox,
}

/// Option feed identity carried through every decoded event.
/// 随每条解码行情传递的期权 feed 身份。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionFeed {
    /// OPRA options feed.
    /// OPRA 期权 feed。
    Opra,
    /// Indicative options feed.
    /// Indicative 期权 feed。
    Indicative,
    /// A delayed feed category retained for explicit classification; Alpaca's reviewed options
    /// WebSocket endpoint does not currently define a delayed feed path, so this value is rejected.
    /// 保留用于明确分类的 delayed feed 类别；当前审阅的 Alpaca 期权 WebSocket 文档没有定义该路径，因此会拒绝此值。
    Delayed,
}

impl OptionFeed {
    /// Returns the exact Alpaca feed segment.
    /// 返回精确的 Alpaca feed 路径片段。
    ///
    /// # Errors
    ///
    /// Returns [`StreamConfigError::UnsupportedFeed`] for the delayed feed, which has no reviewed endpoint.
    pub fn as_str(self) -> Result<&'static str, StreamConfigError> {
        self.endpoint_segment()
    }

    pub(crate) fn endpoint_segment(self) -> Result<&'static str, StreamConfigError> {
        match self {
            Self::Opra => Ok("opra"),
            Self::Indicative => Ok("indicative"),
            Self::Delayed => Err(StreamConfigError::UnsupportedFeed),
        }
    }
}

/// A bounded option contract identifier used for subscription and quote coalescing.
/// 用于订阅和报价合并的有界期权合约标识。
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct OptionContractSymbol(String);

impl OptionContractSymbol {
    /// Validates and owns one provider option symbol without parsing it as an OCC identity.
    /// 校验并持有一个 provider 期权代码；本类型不将其解析为 OCC 合约身份。
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, oversized, or non-alphanumeric symbol.
    pub fn new(value: impl Into<String>) -> Result<Self, StreamConfigError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_OPTION_SYMBOL_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(StreamConfigError::InvalidSymbol);
        }
        Ok(Self(value))
    }

    /// Returns the validated provider symbol.
    /// 返回已校验的 provider 期权代码。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for OptionContractSymbol {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("OptionContractSymbol([REDACTED])")
    }
}

/// Fixed desired quote and trade subscriptions for one connection session.
/// 单个连接会话的固定期望报价与成交订阅集合。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DesiredSubscriptions {
    pub(crate) quotes: BTreeSet<OptionContractSymbol>,
    pub(crate) trades: BTreeSet<OptionContractSymbol>,
}

impl DesiredSubscriptions {
    /// Builds unique bounded quote and trade symbol sets; wildcard subscriptions are rejected.
    /// 构造唯一且有界的报价与成交代码集合；拒绝 wildcard 订阅。
    ///
    /// # Errors
    ///
    /// Returns an error for duplicates, per-channel limits, or an empty request.
    pub fn new(
        quotes: impl IntoIterator<Item = OptionContractSymbol>,
        trades: impl IntoIterator<Item = OptionContractSymbol>,
    ) -> Result<Self, StreamConfigError> {
        let quotes = collect_unique(quotes)?;
        let trades = collect_unique(trades)?;
        if quotes.is_empty() && trades.is_empty() {
            return Err(StreamConfigError::EmptySubscriptions);
        }
        Ok(Self { quotes, trades })
    }

    /// Returns the desired quote symbol count.
    /// 返回期望报价代码数量。
    #[must_use]
    pub fn quote_count(&self) -> usize {
        self.quotes.len()
    }

    /// Returns the desired trade symbol count.
    /// 返回期望成交代码数量。
    #[must_use]
    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }

    /// Reports whether the quote channel includes this validated symbol.
    /// 判断报价 channel 是否包含指定的已校验代码。
    #[must_use]
    pub fn wants_quote(&self, symbol: &OptionContractSymbol) -> bool {
        self.quotes.contains(symbol)
    }

    /// Reports whether the trade channel includes this validated symbol.
    /// 判断成交 channel 是否包含指定的已校验代码。
    #[must_use]
    pub fn wants_trade(&self, symbol: &OptionContractSymbol) -> bool {
        self.trades.contains(symbol)
    }
}

fn collect_unique(
    symbols: impl IntoIterator<Item = OptionContractSymbol>,
) -> Result<BTreeSet<OptionContractSymbol>, StreamConfigError> {
    let mut result = BTreeSet::new();
    for symbol in symbols {
        if !result.insert(symbol) {
            return Err(StreamConfigError::DuplicateSymbol);
        }
        if result.len() > MAX_DESIRED_SYMBOLS_PER_CHANNEL {
            return Err(StreamConfigError::SubscriptionLimitExceeded);
        }
    }
    Ok(result)
}

/// Locally enforced deadlines, freshness rules, and delivery capacities.
/// 本地实施的时限、新鲜度规则和交付容量。
#[derive(Clone, Debug)]
pub struct StreamLimits {
    /// Deadline for opening one WebSocket connection.
    /// 建立单条 WebSocket 连接的期限。
    pub connect_timeout: Duration,
    /// Authentication deadline; it must not exceed Alpaca's documented ten-second window.
    /// 认证期限不得超过 Alpaca 文档规定的十秒窗口。
    pub authentication_timeout: Duration,
    /// Deadline for the full subscription acknowledgement.
    /// 等待完整订阅回执的期限。
    pub acknowledgement_timeout: Duration,
    /// Maximum provider timestamp age eligible to move the session to ready.
    /// 可使会话进入 ready 的 provider 时间戳最大年龄。
    pub ready_max_age: Duration,
    /// Allowed provider timestamp skew into the future for ready gating.
    /// ready gate 允许的 provider 时间戳最大未来偏差。
    pub ready_future_skew: Duration,
    /// Maximum number of distinct pending option quotes retained by the coalescing lane.
    /// 报价合并队列最多保留的不同期权合约数量。
    pub quote_capacity: usize,
    /// Maximum queued trades; saturation terminates the session rather than dropping a trade.
    /// 成交队列上限；队列满时结束会话，不静默丢弃成交。
    pub trade_capacity: usize,
    /// Maximum queued control/error events; saturation terminates the session.
    /// 控制/错误事件队列上限；队列满时结束会话。
    pub control_capacity: usize,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            authentication_timeout: Duration::from_secs(5),
            acknowledgement_timeout: Duration::from_secs(5),
            ready_max_age: Duration::from_secs(2),
            ready_future_skew: Duration::from_millis(100),
            quote_capacity: 1_024,
            trade_capacity: 1_024,
            control_capacity: 64,
        }
    }
}

/// Immutable validated Alpaca options stream configuration.
/// 不可变且已经校验的 Alpaca 期权流配置。
#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub(crate) environment: StreamEnvironment,
    pub(crate) feed: OptionFeed,
    pub(crate) subscriptions: DesiredSubscriptions,
    pub(crate) limits: StreamLimits,
    pub(crate) reconnect: ReconnectPolicy,
}

impl StreamConfig {
    /// Validates an allowlisted environment/feed pair and non-empty desired subscriptions.
    /// 校验 allowlist 环境/feed 组合及非空期望订阅。
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported feeds or invalid subscription, retry, and capacity bounds.
    pub fn new(
        environment: StreamEnvironment,
        feed: OptionFeed,
        subscriptions: DesiredSubscriptions,
    ) -> Result<Self, StreamConfigError> {
        Self::with_limits_and_reconnect(
            environment,
            feed,
            subscriptions,
            StreamLimits::default(),
            ReconnectPolicy::default(),
        )
    }

    /// Validates an allowlisted environment/feed pair with explicit local deadlines and retry bounds.
    /// 使用显式本地期限与重试上限校验 allowlist 环境/feed 组合。
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported feeds or invalid subscription, retry, and capacity bounds.
    pub fn with_limits_and_reconnect(
        environment: StreamEnvironment,
        feed: OptionFeed,
        subscriptions: DesiredSubscriptions,
        limits: StreamLimits,
        reconnect: ReconnectPolicy,
    ) -> Result<Self, StreamConfigError> {
        feed.endpoint_segment()?;
        if subscriptions.quotes.is_empty() && subscriptions.trades.is_empty() {
            return Err(StreamConfigError::EmptySubscriptions);
        }
        if limits.connect_timeout.is_zero()
            || limits.connect_timeout > MAX_CONNECT_TIMEOUT
            || limits.authentication_timeout.is_zero()
            || limits.authentication_timeout > Duration::from_secs(10)
            || limits.acknowledgement_timeout.is_zero()
            || limits.acknowledgement_timeout > MAX_ACKNOWLEDGEMENT_TIMEOUT
            || limits.ready_max_age.is_zero()
            || limits.ready_max_age > MAX_READY_AGE
            || limits.ready_future_skew > MAX_READY_FUTURE_SKEW
            || limits.quote_capacity == 0
            || limits.quote_capacity > MAX_DESIRED_SYMBOLS_PER_CHANNEL
            || limits.quote_capacity < subscriptions.quote_count()
            || limits.trade_capacity == 0
            || limits.trade_capacity > MAX_DESIRED_SYMBOLS_PER_CHANNEL
            || limits.control_capacity == 0
            || limits.control_capacity > MAX_CONTROL_CAPACITY
        {
            return Err(StreamConfigError::InvalidLimits);
        }
        reconnect
            .validate()
            .map_err(|_| StreamConfigError::InvalidReconnectPolicy)?;
        Ok(Self {
            environment,
            feed,
            subscriptions,
            limits,
            reconnect,
        })
    }

    /// Returns the explicitly configured options feed.
    /// 返回显式配置的期权 feed。
    #[must_use]
    pub fn feed(&self) -> OptionFeed {
        self.feed
    }

    /// Returns the production or sandbox endpoint with the allowlisted feed path.
    /// 返回带 allowlist feed 路径的正式或 sandbox endpoint。
    #[must_use]
    pub fn endpoint(&self) -> &'static str {
        let segment = match self.feed {
            OptionFeed::Opra => "opra",
            OptionFeed::Indicative => "indicative",
            OptionFeed::Delayed => unreachable!("delayed feed is rejected during configuration"),
        };
        match (self.environment, segment) {
            (StreamEnvironment::Production, "opra") => {
                "wss://stream.data.alpaca.markets/v1beta1/opra"
            }
            (StreamEnvironment::Production, "indicative") => {
                "wss://stream.data.alpaca.markets/v1beta1/indicative"
            }
            (StreamEnvironment::Sandbox, "opra") => {
                "wss://stream.data.sandbox.alpaca.markets/v1beta1/opra"
            }
            (StreamEnvironment::Sandbox, "indicative") => {
                "wss://stream.data.sandbox.alpaca.markets/v1beta1/indicative"
            }
            _ => unreachable!("only fixed feed segments are constructed"),
        }
    }

    /// Returns the validated immutable subscription set.
    /// 返回已校验且不可变的订阅集合。
    #[must_use]
    pub fn subscriptions(&self) -> &DesiredSubscriptions {
        &self.subscriptions
    }

    /// Returns the local capacity and freshness limits.
    /// 返回本地容量与新鲜度上限。
    #[must_use]
    pub fn limits(&self) -> &StreamLimits {
        &self.limits
    }
}

/// Maximum accepted control-event lane capacity.
/// 控制事件队列接受的最大容量。
pub const MAX_CONTROL_CAPACITY: usize = 1_024;

/// Safe validation categories for a stream configuration.
/// 流配置校验失败的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamConfigError {
    /// The provider delayed feed is not defined by the reviewed option stream endpoint.
    /// 已审阅的期权流 endpoint 未定义 provider delayed feed。
    UnsupportedFeed,
    /// A symbol is empty, too long, or contains a non-alphanumeric byte.
    /// 代码为空、过长或包含非字母数字字节。
    InvalidSymbol,
    /// A channel contains the same symbol more than once.
    /// 同一 channel 中重复订阅代码。
    DuplicateSymbol,
    /// Both desired channel sets are empty.
    /// 报价与成交期望订阅均为空。
    EmptySubscriptions,
    /// A local capacity or protocol deadline is zero, over its local ceiling, or inconsistent.
    /// 本地容量或协议期限为零、超过本地上限或相互矛盾。
    InvalidLimits,
    /// A desired symbol count exceeds the local bound.
    /// 期望订阅数量超过本地上限。
    SubscriptionLimitExceeded,
    /// The reconnect policy is invalid or exceeds its local bound.
    /// 重连策略无效或超过本地上限。
    InvalidReconnectPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions() -> DesiredSubscriptions {
        let symbol = OptionContractSymbol::new("AAPL260123C00150000")
            .expect("synthetic option symbol is valid");
        DesiredSubscriptions::new([symbol], []).expect("non-empty synthetic quote set")
    }

    #[test]
    fn only_allowlisted_option_feeds_construct_endpoints_without_fallback() {
        let opra = StreamConfig::new(
            StreamEnvironment::Production,
            OptionFeed::Opra,
            subscriptions(),
        )
        .expect("OPRA feed is allowlisted");
        let indicative = StreamConfig::new(
            StreamEnvironment::Sandbox,
            OptionFeed::Indicative,
            subscriptions(),
        )
        .expect("indicative sandbox feed is allowlisted");
        assert_eq!(
            opra.endpoint(),
            "wss://stream.data.alpaca.markets/v1beta1/opra"
        );
        assert_eq!(
            indicative.endpoint(),
            "wss://stream.data.sandbox.alpaca.markets/v1beta1/indicative"
        );
        assert_eq!(
            StreamConfig::new(
                StreamEnvironment::Production,
                OptionFeed::Delayed,
                subscriptions(),
            )
            .err(),
            Some(StreamConfigError::UnsupportedFeed)
        );
    }

    #[test]
    fn all_deadlines_and_local_queues_have_enforced_upper_bounds() {
        let limits = StreamLimits {
            connect_timeout: MAX_CONNECT_TIMEOUT + Duration::from_millis(1),
            ..StreamLimits::default()
        };
        assert_eq!(
            StreamConfig::with_limits_and_reconnect(
                StreamEnvironment::Production,
                OptionFeed::Opra,
                subscriptions(),
                limits,
                ReconnectPolicy::default(),
            )
            .err(),
            Some(StreamConfigError::InvalidLimits)
        );
        let limits = StreamLimits {
            ready_max_age: MAX_READY_AGE + Duration::from_millis(1),
            ..StreamLimits::default()
        };
        assert_eq!(
            StreamConfig::with_limits_and_reconnect(
                StreamEnvironment::Production,
                OptionFeed::Opra,
                subscriptions(),
                limits,
                ReconnectPolicy::default(),
            )
            .err(),
            Some(StreamConfigError::InvalidLimits)
        );
    }

    #[test]
    fn contract_debug_output_never_discloses_the_symbol() {
        let symbol = OptionContractSymbol::new("AAPL260123C00150000")
            .expect("synthetic option symbol is valid");
        assert_eq!(format!("{symbol:?}"), "OptionContractSymbol([REDACTED])");
    }
}
