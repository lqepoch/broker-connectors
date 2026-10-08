//! Lossless typed projections for selected Market Data read responses.
//!
//! These DTOs describe validated response syntax. They do not establish quote,
//! market-hours, instrument, or account authority.
//! 定义 Schwab 市场数据读取响应的类型化投影。

mod common;
mod history;
mod hours;
mod instruments;
mod options;
mod quotes;

pub use history::{MoverItem, MoversResponse, PriceHistoryCandle, PriceHistoryResponse};
pub use hours::{MarketHoursProduct, MarketHoursResponse, MarketSessionTime};
pub use instruments::{InstrumentDetail, InstrumentSummary, InstrumentsSearchResponse};
pub use options::{
    OptionChainResponse, OptionContract, OptionExpiration, OptionExpirationChainResponse,
};
pub use quotes::{
    QuoteDetail, QuoteItem, QuoteReference, QuoteSeriesCandle, QuotesResponse, SingleQuoteResponse,
};

use std::fmt;

use serde_json::Value;

use crate::read_response::{ReadResponseError, ReadResponseKind};

/// Typed Market Data response selected by the allow-listed GET route.
/// 中文摘要：市场数据只读端点对应的类型化响应集合。
#[derive(Clone, PartialEq)]
pub enum MarketReadResponse {
    /// Batch quote records keyed by the broker response symbol.
    /// 批量报价响应的类型化投影。
    Quotes(QuotesResponse),
    /// One quote/history-shaped response from `GET /{symbol}/quotes`.
    /// 单标的报价响应的类型化投影。
    SingleQuote(SingleQuoteResponse),
    /// Option-chain response, including dynamic expiration and strike keys.
    /// 期权链响应的类型化投影。
    OptionChain(OptionChainResponse),
    /// Option expiration metadata response.
    /// 期权到期日响应的类型化投影。
    OptionExpirationChain(OptionExpirationChainResponse),
    /// Historical OHLCV response.
    /// 历史价格响应的类型化投影。
    PriceHistory(PriceHistoryResponse),
    /// Market movers screener response.
    /// 市场涨跌榜结果的类型化投影。
    Movers(MoversResponse),
    /// Batch and single market-hours response record.
    /// 市场交易时间的类型化投影。
    MarketHours(MarketHoursResponse),
    /// Instrument search response wrapper.
    /// 标的搜索结果的类型化投影。
    InstrumentsSearch(InstrumentsSearchResponse),
    /// One instrument detail response.
    /// 单个标的详情的类型化投影。
    InstrumentDetail(InstrumentDetail),
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {$ (
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
            }
        }
    )+};
}

redacted_debug!(
    MarketReadResponse,
    QuotesResponse,
    QuoteItem,
    QuoteReference,
    QuoteDetail,
    SingleQuoteResponse,
    QuoteSeriesCandle,
    OptionChainResponse,
    OptionContract,
    OptionExpirationChainResponse,
    OptionExpiration,
    PriceHistoryResponse,
    PriceHistoryCandle,
    MoversResponse,
    MoverItem,
    MarketHoursResponse,
    MarketHoursProduct,
    MarketSessionTime,
    InstrumentsSearchResponse,
    InstrumentSummary,
);

/// Projects a response after `read_response::validate` has checked the Node
/// schema family for the selected route.
pub(crate) fn project(
    kind: ReadResponseKind,
    value: &Value,
) -> Result<Option<MarketReadResponse>, ReadResponseError> {
    let model = match kind {
        ReadResponseKind::Quotes => MarketReadResponse::Quotes(quotes::project_quotes(value)?),
        ReadResponseKind::SingleQuote => {
            MarketReadResponse::SingleQuote(quotes::project_single_quote(value)?)
        }
        ReadResponseKind::OptionChain => {
            MarketReadResponse::OptionChain(options::project_option_chain(value)?)
        }
        ReadResponseKind::OptionExpirationChain => MarketReadResponse::OptionExpirationChain(
            options::project_option_expiration_chain(value)?,
        ),
        ReadResponseKind::PriceHistory => {
            MarketReadResponse::PriceHistory(history::project_price_history(value)?)
        }
        ReadResponseKind::Movers => MarketReadResponse::Movers(history::project_movers(value)?),
        ReadResponseKind::MarketHours => {
            MarketReadResponse::MarketHours(hours::project_market_hours(value)?)
        }
        ReadResponseKind::InstrumentsSearch => {
            MarketReadResponse::InstrumentsSearch(instruments::project_instruments_search(value)?)
        }
        ReadResponseKind::InstrumentDetail => MarketReadResponse::InstrumentDetail(
            instruments::project_instrument_summary(value, "instrument")?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(model))
}
