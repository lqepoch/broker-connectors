#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Bounded, read-only Alpaca options and single-page stock-bars REST access over the pinned SDK.
//!
//! Option reads return shared `market-contracts` quote/trade payloads. Stock-bars reads return a
//! separate bounded adapter observation and never collect or publish a history. A requested feed
//! is not entitlement evidence; REST reads do not provide a trusted sequence, stream watermark,
//! or completion receipt.
//!
//! 本 crate 基于固定版本的社区 Rust SDK 提供有界、只读 Alpaca 期权及单页股票 bars REST 接口。
//! 期权读取返回共享 `market-contracts` quote/trade 类型；股票 bars 返回独立的有界适配器观察值，
//! 不负责历史采集或发布。请求 feed 不构成 entitlement 证据；REST 读取不提供可信序号、水位或完整性回执。

mod client;
mod error;
mod model;
mod request;
mod stock_bars;

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
        | AlpacaRestOperation::HistoricalStockSipBarsPage => AlpacaCapabilityStatus::Supported,
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
        AlpacaCapabilityStatus, AlpacaRestOperation, AlpacaUnsupportedReason, capability_status,
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
}
