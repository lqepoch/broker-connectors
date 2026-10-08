//! Quote, reference, and price-history response projections.
//! 定义报价、参考信息和价格历史响应投影。

use std::collections::BTreeMap;

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    object, optional_boolean, optional_number, optional_string, required_number, required_string,
    unknown_fields,
};
use crate::response_models::{UnknownFields, WireNumber};

/// Batch quote response rows. The map key is the quote symbol returned by
/// Schwab and is kept independently from each row's `symbol` field.
/// 中文摘要：以 broker 报价代码为键的批量报价映射；键与行内 symbol 分开保留。
#[derive(Clone, PartialEq)]
pub struct QuotesResponse {
    /// Quote rows keyed by the symbol key from the broker response.
    /// 按 broker key 索引的响应条目。
    pub items: BTreeMap<String, QuoteItem>,
}

/// One quote row with known wire fields and additive values retained.
/// 中文摘要：一个证券报价行，包含行情、参考数据和未建模扩展字段；DTO 本身不证明报价仍然新鲜。
#[derive(Clone, PartialEq)]
pub struct QuoteItem {
    /// Broker asset-class category for the quoted instrument.
    /// broker 资产主类别。
    pub asset_main_type: Option<String>,
    /// Broker asset subcategory for the quoted instrument.
    /// broker 资产子类别。
    pub asset_sub_type: Option<String>,
    /// Instrument symbol reported within this quote row.
    /// 标的或合约交易代码。
    pub symbol: String,
    /// Broker category assigned to this quote.
    /// broker 报价类别。
    pub quote_type: Option<String>,
    /// Broker realtime flag; not a freshness or entitlement proof.
    /// broker 报价是否标记为实时。
    pub realtime: Option<bool>,
    /// Broker-provided session or security identifier.
    /// broker 提供的会话或证券标识。
    pub ssid: Option<WireNumber>,
    /// Static instrument reference details included with the row.
    /// 证券静态参考信息。
    pub reference: Option<QuoteReference>,
    /// Typed current or latest quote details included with the row.
    /// 实时或最近报价字段集合。
    pub quote: Option<QuoteDetail>,
    /// Unmodeled regular-session quote properties retained from the response.
    /// regular 时段的附加报价字段。
    pub regular: Option<UnknownFields>,
    /// Unmodeled fundamental properties retained from the response.
    /// 基本面附加字段。
    pub fundamental: Option<UnknownFields>,
    /// Unmodeled extended-session quote properties retained from the response.
    /// 延长时段的附加报价字段。
    pub extended: Option<UnknownFields>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// Static identifier and exchange details accompanying one quote row.
/// 中文摘要：单条报价附带的标识符和交易所静态信息。
pub struct QuoteReference {
    /// CUSIP identifier reported for the instrument.
    /// 证券的 CUSIP 标识符。
    pub cusip: Option<String>,
    /// Broker description of the instrument.
    /// 标的或产品说明。
    pub description: Option<String>,
    /// Exchange code reported for the instrument.
    /// 交易所代码。
    pub exchange: Option<String>,
    /// Exchange name reported for the instrument.
    /// 交易所名称。
    pub exchange_name: Option<String>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// Typed bid, ask, trade, and volume values from one broker quote row.
/// 中文摘要：单条 broker 报价中的买卖价、成交和成交量类型化字段。
pub struct QuoteDetail {
    /// Broker-reported ask price.
    /// 卖方报价价格。
    pub ask_price: Option<WireNumber>,
    /// Broker-reported ask size.
    /// 卖方报价数量。
    pub ask_size: Option<WireNumber>,
    /// Source timestamp of the ask quote.
    /// 卖方报价来源时间。
    pub ask_time: Option<WireNumber>,
    /// Broker-reported bid price.
    /// 买方报价价格。
    pub bid_price: Option<WireNumber>,
    /// Broker-reported bid size.
    /// 买方报价数量。
    pub bid_size: Option<WireNumber>,
    /// Source timestamp of the bid quote.
    /// 买方报价来源时间。
    pub bid_time: Option<WireNumber>,
    /// Most recent traded price reported by the broker.
    /// 最近成交价格。
    pub last_price: Option<WireNumber>,
    /// Size of the most recent trade reported by the broker.
    /// 最近成交数量。
    pub last_size: Option<WireNumber>,
    /// Broker mark value; it is not recalculated by this projection.
    /// 响应中的 broker mark 值。
    pub mark: Option<WireNumber>,
    /// Source timestamp of the quote data.
    /// 报价来源时间。
    pub quote_time: Option<WireNumber>,
    /// Source timestamp of the latest trade.
    /// 最近成交来源时间。
    pub trade_time: Option<WireNumber>,
    /// Cumulative volume reported for the session.
    /// 累计成交量。
    pub total_volume: Option<WireNumber>,
    /// Volatility value reported with the quote.
    /// 响应中的波动率数值。
    pub volatility: Option<WireNumber>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Response for the legacy single-symbol quote route. Fields not explicitly
/// modeled by the current Zod schema remain in `unknown_fields`.
/// 中文摘要：单证券 quote 路由的历史兼容响应；未建模字段保留在扩展映射中。
#[derive(Clone, PartialEq)]
pub struct SingleQuoteResponse {
    /// Symbol associated with this legacy single-quote response.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Whether the broker marks the response as empty.
    /// broker 是否将结果标记为空。
    pub empty: Option<bool>,
    /// Previous-session closing price.
    /// 前一交易日收盘价。
    pub previous_close: Option<WireNumber>,
    /// Date associated with the previous close.
    /// 前一收盘价对应的日期。
    pub previous_close_date: Option<WireNumber>,
    /// Price candles included in the single-quote response.
    /// 按响应顺序返回的价格蜡烛数据。
    pub candles: Option<Vec<QuoteSeriesCandle>>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// One interval of quote-series OHLCV data and its broker timestamp.
/// 中文摘要：一段报价序列的 OHLCV 数据及其 broker 时间戳。
pub struct QuoteSeriesCandle {
    /// Opening price for the candle interval.
    /// 周期内开盘价。
    pub open: WireNumber,
    /// Highest price for the candle interval.
    /// 周期内最高价。
    pub high: WireNumber,
    /// Lowest price for the candle interval.
    /// 周期内最低价。
    pub low: WireNumber,
    /// Closing price for the candle interval.
    /// 周期内收盘价。
    pub close: WireNumber,
    /// Traded volume for the candle interval.
    /// 周期内成交量。
    pub volume: WireNumber,
    /// Broker timestamp identifying the candle interval.
    /// 蜡烛对应的 broker 时间戳。
    pub datetime: WireNumber,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_quotes(value: &Value) -> Result<QuotesResponse, ReadResponseError> {
    let rows = object(value, "quotes")?;
    let items = rows
        .iter()
        .map(|(key, row)| Ok((key.clone(), project_quote_item(row)?)))
        .collect::<Result<_, ReadResponseError>>()?;
    Ok(QuotesResponse { items })
}

fn project_quote_item(value: &Value) -> Result<QuoteItem, ReadResponseError> {
    let fields = object(value, "quotes[]")?;
    let reference = fields
        .get("reference")
        .map(project_quote_reference)
        .transpose()?;
    let quote = fields.get("quote").map(project_quote_detail).transpose()?;
    Ok(QuoteItem {
        asset_main_type: optional_string(fields, "assetMainType", "quotes[].assetMainType")?,
        asset_sub_type: optional_string(fields, "assetSubType", "quotes[].assetSubType")?,
        symbol: required_string(fields, "symbol", "quotes[].symbol")?,
        quote_type: optional_string(fields, "quoteType", "quotes[].quoteType")?,
        realtime: optional_boolean(fields, "realtime", "quotes[].realtime")?,
        ssid: optional_number(fields, "ssid", "quotes[].ssid")?,
        reference,
        quote,
        regular: fields
            .get("regular")
            .map(|value| project_loose_fields(value, "quotes[].regular"))
            .transpose()?,
        fundamental: fields
            .get("fundamental")
            .map(|value| project_loose_fields(value, "quotes[].fundamental"))
            .transpose()?,
        extended: fields
            .get("extended")
            .map(|value| project_loose_fields(value, "quotes[].extended"))
            .transpose()?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "assetMainType",
                "assetSubType",
                "symbol",
                "quoteType",
                "realtime",
                "ssid",
                "reference",
                "quote",
                "regular",
                "fundamental",
                "extended",
            ],
        ),
    })
}

fn project_quote_reference(value: &Value) -> Result<QuoteReference, ReadResponseError> {
    let fields = object(value, "quotes[].reference")?;
    Ok(QuoteReference {
        cusip: optional_string(fields, "cusip", "quotes[].reference.cusip")?,
        description: optional_string(fields, "description", "quotes[].reference.description")?,
        exchange: optional_string(fields, "exchange", "quotes[].reference.exchange")?,
        exchange_name: optional_string(fields, "exchangeName", "quotes[].reference.exchangeName")?,
        unknown_fields: unknown_fields(
            fields,
            &["cusip", "description", "exchange", "exchangeName"],
        ),
    })
}

fn project_quote_detail(value: &Value) -> Result<QuoteDetail, ReadResponseError> {
    let fields = object(value, "quotes[].quote")?;
    Ok(QuoteDetail {
        ask_price: optional_number(fields, "askPrice", "quotes[].quote.askPrice")?,
        ask_size: optional_number(fields, "askSize", "quotes[].quote.askSize")?,
        ask_time: optional_number(fields, "askTime", "quotes[].quote.askTime")?,
        bid_price: optional_number(fields, "bidPrice", "quotes[].quote.bidPrice")?,
        bid_size: optional_number(fields, "bidSize", "quotes[].quote.bidSize")?,
        bid_time: optional_number(fields, "bidTime", "quotes[].quote.bidTime")?,
        last_price: optional_number(fields, "lastPrice", "quotes[].quote.lastPrice")?,
        last_size: optional_number(fields, "lastSize", "quotes[].quote.lastSize")?,
        mark: optional_number(fields, "mark", "quotes[].quote.mark")?,
        quote_time: optional_number(fields, "quoteTime", "quotes[].quote.quoteTime")?,
        trade_time: optional_number(fields, "tradeTime", "quotes[].quote.tradeTime")?,
        total_volume: optional_number(fields, "totalVolume", "quotes[].quote.totalVolume")?,
        volatility: optional_number(fields, "volatility", "quotes[].quote.volatility")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "askPrice",
                "askSize",
                "askTime",
                "bidPrice",
                "bidSize",
                "bidTime",
                "lastPrice",
                "lastSize",
                "mark",
                "quoteTime",
                "tradeTime",
                "totalVolume",
                "volatility",
            ],
        ),
    })
}

fn project_loose_fields(
    value: &Value,
    path: &'static str,
) -> Result<UnknownFields, ReadResponseError> {
    Ok(unknown_fields(object(value, path)?, &[]))
}

pub(super) fn project_single_quote(
    value: &Value,
) -> Result<SingleQuoteResponse, ReadResponseError> {
    let fields = object(value, "singleQuote")?;
    let candles = fields
        .get("candles")
        .map(|value| {
            let values = value.as_array().ok_or(ReadResponseError::SchemaViolation {
                field: "singleQuote.candles",
            })?;
            values
                .iter()
                .map(project_quote_series_candle)
                .collect::<Result<_, _>>()
        })
        .transpose()?;
    Ok(SingleQuoteResponse {
        symbol: optional_string(fields, "symbol", "singleQuote.symbol")?,
        empty: optional_boolean(fields, "empty", "singleQuote.empty")?,
        previous_close: optional_number(fields, "previousClose", "singleQuote.previousClose")?,
        previous_close_date: optional_number(
            fields,
            "previousCloseDate",
            "singleQuote.previousCloseDate",
        )?,
        candles,
        unknown_fields: unknown_fields(
            fields,
            &[
                "symbol",
                "empty",
                "previousClose",
                "previousCloseDate",
                "candles",
            ],
        ),
    })
}

fn project_quote_series_candle(value: &Value) -> Result<QuoteSeriesCandle, ReadResponseError> {
    let fields = object(value, "singleQuote.candles[]")?;
    Ok(QuoteSeriesCandle {
        open: required_number(fields, "open", "singleQuote.candles[].open")?,
        high: required_number(fields, "high", "singleQuote.candles[].high")?,
        low: required_number(fields, "low", "singleQuote.candles[].low")?,
        close: required_number(fields, "close", "singleQuote.candles[].close")?,
        volume: required_number(fields, "volume", "singleQuote.candles[].volume")?,
        datetime: required_number(fields, "datetime", "singleQuote.candles[].datetime")?,
        unknown_fields: unknown_fields(
            fields,
            &["open", "high", "low", "close", "volume", "datetime"],
        ),
    })
}
