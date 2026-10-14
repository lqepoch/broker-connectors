#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Bounded, read-only Alpaca options and stock-bars REST access over the pinned SDK.
//!
//! Option reads return shared `market-contracts` quote/trade payloads. Stock-bars reads return a
//! separate bounded adapter observation and never collect or publish a history. Stock history can
//! be read one page at a time or in a finite four-page window. A requested feed is not entitlement
//! evidence; REST reads do not provide a trusted sequence, stream watermark, or completion receipt.
//!
//! 本 crate 基于固定版本的社区 Rust SDK 提供有界、只读 Alpaca 期权及股票 bars REST 接口。
//! 股票历史可逐页或以最多四页的窗口读取。期权读取返回共享 `market-contracts` quote/trade 类型；
//! 股票 bars 返回独立的有界适配器观察值，不负责历史采集或发布。请求 feed 不构成 entitlement
//! 证据；REST 读取不提供可信序号、水位或完整性回执。

mod client;
mod error;
mod model;
mod request;
mod stock_bars;
mod stock_bars_window;

pub use client::{
    AlpacaRestCredentials, AlpacaRestLimits, AlpacaRestReadClient, DEFAULT_OPERATION_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT, MAX_OPERATION_TIMEOUT, MAX_REQUEST_TIMEOUT, MAX_RETRY_BUDGET,
};
pub use error::AlpacaRestError;
pub use model::{AlpacaOptionsObservation, OptionsSnapshotWindow};
pub use request::{
    AlpacaOptionsRequest, MAX_OPTIONS_SYMBOLS_PER_REQUEST, MAX_PAGE_CURSOR_BYTES,
    MAX_SNAPSHOT_PAGE_SIZE, MAX_SNAPSHOT_PAGES_PER_WINDOW, OptionsPageCursor,
    OptionsSnapshotWindowRequest, RequestedOptionsFeed,
};
pub use stock_bars::{
    AlpacaStockBarObservation, AlpacaStockBarsPage, AlpacaStockBarsRequest,
    MAX_STOCK_BARS_PAGE_SIZE, RequestedStockBarsFeed, StockBarsCursor, StockBarsTimeframe,
};
pub use stock_bars_window::{
    AlpacaStockBarsWindow, AlpacaStockBarsWindowRequest, MAX_STOCK_BARS_PAGES_PER_WINDOW,
};

/// Alpaca Trading API plan label used only for documenting stock-bars access rules.
/// 仅用于描述股票 bars 数据规则的 Alpaca Trading API 计划名，不表示账户 entitlement 已验证。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaStockBarsPlanProfile {
    /// Basic plan; historical data is unavailable for the most recent 15 minutes.
    Basic,
    /// Algo Trader Plus plan; the documented 15-minute historical restriction does not apply.
    AlgoTraderPlus,
}

/// Historical stock-bars feed names described by Alpaca's market-data API.
/// Alpaca 行情 API 文档中的历史股票 bars feed 名称。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaHistoricalStockBarsFeed {
    /// Consolidated Securities Information Processor data.
    Sip,
    /// Blue Ocean ATS data used for overnight-session history.
    Boats,
    /// Derived overnight feed, which the historical bars endpoint does not accept.
    Overnight,
}

/// Documented historical access for a feed and plan profile.
/// 文档所述 feed/计划组合的历史访问边界；不是当前账户权限证明。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocumentedStockBarsAccess {
    /// History is available only when the requested end is at least 15 minutes old.
    AvailableWithMinimum15MinuteAge,
    /// The documented Basic-plan 15-minute history restriction does not apply.
    AvailableWithout15MinuteAgeLimit,
    /// The historical-bars endpoint does not support this feed.
    UnsupportedForHistoricalBars,
}

/// Return Alpaca's documented stock-bars access rule for a named plan profile.
///
/// The plan profile is caller-selected reference information only. This function does not inspect
/// credentials, verify an Alpaca account subscription, or determine the effective response feed.
///
/// # Official behavior represented
///
/// Basic can query SIP history ending at least 15 minutes ago and receives 15-minute-delayed
/// BOATS history. Algo Trader Plus removes the documented 15-minute recency restriction. For
/// historical overnight data, Alpaca documents `boats`; `overnight` is not accepted by the
/// historical-bars endpoint.
///
/// # Errors
///
/// This function is total and returns an explicit status for every documented feed/profile pair.
#[must_use]
pub const fn documented_stock_bars_access(
    feed: AlpacaHistoricalStockBarsFeed,
    plan: AlpacaStockBarsPlanProfile,
) -> DocumentedStockBarsAccess {
    match (feed, plan) {
        (AlpacaHistoricalStockBarsFeed::Overnight, _) => {
            DocumentedStockBarsAccess::UnsupportedForHistoricalBars
        }
        (
            AlpacaHistoricalStockBarsFeed::Sip | AlpacaHistoricalStockBarsFeed::Boats,
            AlpacaStockBarsPlanProfile::Basic,
        ) => DocumentedStockBarsAccess::AvailableWithMinimum15MinuteAge,
        (
            AlpacaHistoricalStockBarsFeed::Sip | AlpacaHistoricalStockBarsFeed::Boats,
            AlpacaStockBarsPlanProfile::AlgoTraderPlus,
        ) => DocumentedStockBarsAccess::AvailableWithout15MinuteAgeLimit,
    }
}

/// REST operation names used by the explicit capability report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaRestOperation {
    /// Latest option quote observations.
    LatestOptionQuotes,
    /// Latest option trade observations.
    LatestOptionTrades,
    /// A bounded options snapshots page window.
    OptionSnapshotWindow,
    /// Feed-qualified historical option bars.
    FeedQualifiedOptionBars,
    /// Feed-qualified historical option trades.
    FeedQualifiedOptionTrades,
    /// One page of historical stock bars with a fixed SIP request selector.
    /// 表示单页 SIP 请求能力；不证明有效行情来源、权限或历史完整性。
    HistoricalStockSipBarsPage,
    /// One page of historical stock bars with explicit SIP or BOATS intent.
    /// 单页 SIP/BOATS 历史 bars 请求能力；不证明有效行情来源、权限或历史完整性。
    HistoricalStockBarsPage,
    /// A finite multi-page historical stock-bars window for explicit SIP or BOATS intent.
    /// 有界多页 SIP/BOATS 历史 bars 读取；不表示完整归档能力。
    HistoricalStockBarsWindow,
    /// Trusted provider stream watermark or continuation evidence.
    TrustedWatermark,
}

/// Why an operation is unavailable in the current adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaUnsupportedReason {
    /// The reviewed official endpoint/SDK request has no explicit feed selector.
    NoFeedSelector,
    /// No reviewed Alpaca REST response provides a trusted continuity watermark.
    NoTrustedWatermark,
}

/// Availability of one documented Alpaca REST operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaCapabilityStatus {
    /// The operation is implemented by this read-only adapter.
    Supported,
    /// The adapter deliberately fails closed for this operation.
    Unsupported(AlpacaUnsupportedReason),
}

/// Return the implementation status for one provider operation.
#[must_use]
pub const fn capability_status(operation: AlpacaRestOperation) -> AlpacaCapabilityStatus {
    match operation {
        AlpacaRestOperation::LatestOptionQuotes
        | AlpacaRestOperation::LatestOptionTrades
        | AlpacaRestOperation::OptionSnapshotWindow
        | AlpacaRestOperation::HistoricalStockSipBarsPage
        | AlpacaRestOperation::HistoricalStockBarsPage
        | AlpacaRestOperation::HistoricalStockBarsWindow => AlpacaCapabilityStatus::Supported,
        AlpacaRestOperation::FeedQualifiedOptionBars
        | AlpacaRestOperation::FeedQualifiedOptionTrades => {
            AlpacaCapabilityStatus::Unsupported(AlpacaUnsupportedReason::NoFeedSelector)
        }
        AlpacaRestOperation::TrustedWatermark => {
            AlpacaCapabilityStatus::Unsupported(AlpacaUnsupportedReason::NoTrustedWatermark)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AlpacaCapabilityStatus, AlpacaHistoricalStockBarsFeed, AlpacaRestOperation,
        AlpacaStockBarsPlanProfile, AlpacaUnsupportedReason, DocumentedStockBarsAccess,
        capability_status, documented_stock_bars_access,
    };

    #[test]
    fn feed_unqualified_history_and_watermarks_are_reported_unsupported() {
        assert_eq!(
            capability_status(AlpacaRestOperation::FeedQualifiedOptionBars),
            AlpacaCapabilityStatus::Unsupported(AlpacaUnsupportedReason::NoFeedSelector)
        );
        assert_eq!(
            capability_status(AlpacaRestOperation::FeedQualifiedOptionTrades),
            AlpacaCapabilityStatus::Unsupported(AlpacaUnsupportedReason::NoFeedSelector)
        );
        assert_eq!(
            capability_status(AlpacaRestOperation::TrustedWatermark),
            AlpacaCapabilityStatus::Unsupported(AlpacaUnsupportedReason::NoTrustedWatermark)
        );
    }

    #[test]
    fn one_page_sip_stock_bars_are_reported_supported() {
        assert_eq!(
            capability_status(AlpacaRestOperation::HistoricalStockSipBarsPage),
            AlpacaCapabilityStatus::Supported
        );
    }

    #[test]
    fn one_page_explicit_feed_stock_bars_are_reported_supported() {
        assert_eq!(
            capability_status(AlpacaRestOperation::HistoricalStockBarsPage),
            AlpacaCapabilityStatus::Supported
        );
    }

    #[test]
    fn stock_bars_plan_matrix_keeps_historical_feed_rules_distinct() {
        assert_eq!(
            documented_stock_bars_access(
                AlpacaHistoricalStockBarsFeed::Sip,
                AlpacaStockBarsPlanProfile::Basic,
            ),
            DocumentedStockBarsAccess::AvailableWithMinimum15MinuteAge
        );
        assert_eq!(
            documented_stock_bars_access(
                AlpacaHistoricalStockBarsFeed::Boats,
                AlpacaStockBarsPlanProfile::Basic,
            ),
            DocumentedStockBarsAccess::AvailableWithMinimum15MinuteAge
        );
        assert_eq!(
            documented_stock_bars_access(
                AlpacaHistoricalStockBarsFeed::Sip,
                AlpacaStockBarsPlanProfile::AlgoTraderPlus,
            ),
            DocumentedStockBarsAccess::AvailableWithout15MinuteAgeLimit
        );
        assert_eq!(
            documented_stock_bars_access(
                AlpacaHistoricalStockBarsFeed::Boats,
                AlpacaStockBarsPlanProfile::AlgoTraderPlus,
            ),
            DocumentedStockBarsAccess::AvailableWithout15MinuteAgeLimit
        );
        for plan in [
            AlpacaStockBarsPlanProfile::Basic,
            AlpacaStockBarsPlanProfile::AlgoTraderPlus,
        ] {
            assert_eq!(
                documented_stock_bars_access(AlpacaHistoricalStockBarsFeed::Overnight, plan),
                DocumentedStockBarsAccess::UnsupportedForHistoricalBars
            );
        }
    }
}
