//! Historical-price and market-mover response projections.
//! 定义历史价格与市场涨跌榜响应投影。

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    object, optional_boolean, optional_number, optional_string, required_number, unknown_fields,
};
use crate::response_models::{UnknownFields, WireNumber};

#[derive(Clone, PartialEq)]
/// Typed historical-price response; unknown properties remain additive data and do not establish authority.
/// 中文摘要：类型化历史价格响应；未知属性作为附加数据保留，且不会建立 authority。
pub struct PriceHistoryResponse {
    /// Symbol associated with the returned history.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Whether the broker marks the history result as empty.
    /// broker 是否将结果标记为空。
    pub empty: Option<bool>,
    /// Previous-session closing price included with the history.
    /// 前一交易日收盘价。
    pub previous_close: Option<WireNumber>,
    /// Date associated with the previous close.
    /// 前一收盘价对应的日期。
    pub previous_close_date: Option<WireNumber>,
    /// Historical price bars in broker response order.
    /// 按响应顺序返回的价格蜡烛数据。
    pub candles: Vec<PriceHistoryCandle>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// One historical OHLCV price bar with the timestamp reported by the broker.
/// 中文摘要：一根历史 OHLCV 价格蜡烛及 broker 报告的时间戳。
pub struct PriceHistoryCandle {
    /// Opening price for the historical bar.
    /// 周期内开盘价。
    pub open: WireNumber,
    /// Highest price for the historical bar.
    /// 周期内最高价。
    pub high: WireNumber,
    /// Lowest price for the historical bar.
    /// 周期内最低价。
    pub low: WireNumber,
    /// Closing price for the historical bar.
    /// 周期内收盘价。
    pub close: WireNumber,
    /// Traded volume for the historical bar.
    /// 周期内成交量。
    pub volume: WireNumber,
    /// Broker timestamp identifying the historical bar.
    /// 蜡烛对应的 broker 时间戳。
    pub datetime: WireNumber,
}

#[derive(Clone, PartialEq)]
/// Typed result set returned by the broker market-mover screener.
/// 中文摘要：broker 市场涨跌榜筛选器返回的类型化结果集。
pub struct MoversResponse {
    /// Mover rows returned by the broker’s screener.
    /// 按 broker 排名返回的筛选结果。
    pub screeners: Vec<MoverItem>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// One broker market-mover row with its optional price, volume, and ranking fields.
/// 中文摘要：一条 broker 涨跌榜记录及其可选价格、成交量和排序字段。
pub struct MoverItem {
    /// Price change reported for this mover row.
    /// 相对参考价格的变动值。
    pub change: Option<WireNumber>,
    /// Instrument description reported by the broker.
    /// 标的或产品说明。
    pub description: Option<String>,
    /// Direction or ranking label reported by the screener.
    /// 涨跌方向或排序方向。
    pub direction: Option<String>,
    /// Latest price reported for this mover row.
    /// 最近成交价格。
    pub last: Option<WireNumber>,
    /// Instrument symbol reported for this mover row.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Cumulative volume reported for this mover row.
    /// 累计成交量。
    pub total_volume: Option<WireNumber>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_price_history(
    value: &Value,
) -> Result<PriceHistoryResponse, ReadResponseError> {
    let fields = object(value, "priceHistory")?;
    let rows = fields.get("candles").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "priceHistory.candles",
        },
    )?;
    let candles = rows
        .iter()
        .map(project_price_history_candle)
        .collect::<Result<_, _>>()?;
    Ok(PriceHistoryResponse {
        symbol: optional_string(fields, "symbol", "priceHistory.symbol")?,
        empty: optional_boolean(fields, "empty", "priceHistory.empty")?,
        previous_close: optional_number(fields, "previousClose", "priceHistory.previousClose")?,
        previous_close_date: optional_number(
            fields,
            "previousCloseDate",
            "priceHistory.previousCloseDate",
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

fn project_price_history_candle(value: &Value) -> Result<PriceHistoryCandle, ReadResponseError> {
    let fields = object(value, "priceHistory.candles[]")?;
    Ok(PriceHistoryCandle {
        open: required_number(fields, "open", "priceHistory.candles[].open")?,
        high: required_number(fields, "high", "priceHistory.candles[].high")?,
        low: required_number(fields, "low", "priceHistory.candles[].low")?,
        close: required_number(fields, "close", "priceHistory.candles[].close")?,
        volume: required_number(fields, "volume", "priceHistory.candles[].volume")?,
        datetime: required_number(fields, "datetime", "priceHistory.candles[].datetime")?,
    })
}

pub(super) fn project_movers(value: &Value) -> Result<MoversResponse, ReadResponseError> {
    let fields = object(value, "movers")?;
    let rows = fields.get("screeners").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "movers.screeners",
        },
    )?;
    let screeners = rows.iter().map(project_mover).collect::<Result<_, _>>()?;
    Ok(MoversResponse {
        screeners,
        unknown_fields: unknown_fields(fields, &["screeners"]),
    })
}

fn project_mover(value: &Value) -> Result<MoverItem, ReadResponseError> {
    let fields = object(value, "movers.screeners[]")?;
    Ok(MoverItem {
        change: optional_number(fields, "change", "movers.screeners[].change")?,
        description: optional_string(fields, "description", "movers.screeners[].description")?,
        direction: optional_string(fields, "direction", "movers.screeners[].direction")?,
        last: optional_number(fields, "last", "movers.screeners[].last")?,
        symbol: optional_string(fields, "symbol", "movers.screeners[].symbol")?,
        total_volume: optional_number(fields, "totalVolume", "movers.screeners[].totalVolume")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "change",
                "description",
                "direction",
                "last",
                "symbol",
                "totalVolume",
            ],
        ),
    })
}
