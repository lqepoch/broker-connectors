//! Instrument-search and instrument-detail response projections.
//! 定义标的搜索和标的详情响应投影。

use serde_json::Value;

use super::ReadResponseError;
use super::common::{object, optional_string, unknown_fields};
use crate::response_models::UnknownFields;

/// Instrument summary rows returned by the search endpoint.
/// 中文摘要：按 broker 原顺序排列的标的搜索结果及未识别的响应字段。
#[derive(Clone, PartialEq)]
pub struct InstrumentsSearchResponse {
    /// Instrument matches in broker response order.
    /// 中文摘要：符合查询条件的标的记录。
    pub instruments: Vec<InstrumentSummary>,
    /// Additive response-wrapper fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Optional identifying and descriptive fields for an instrument.
/// 中文摘要：标的搜索或详情中用于识别证券的代码、CUSIP、描述和交易所信息。
#[derive(Clone, PartialEq)]
pub struct InstrumentSummary {
    /// CUSIP identifier.
    /// 中文摘要：证券 CUSIP 标识符。
    pub cusip: Option<String>,
    /// Broker symbol.
    /// 中文摘要：标的或合约交易代码。
    pub symbol: Option<String>,
    /// Broker description.
    /// 中文摘要：标的或产品说明。
    pub description: Option<String>,
    /// Exchange label.
    /// 中文摘要：交易所代码。
    pub exchange: Option<String>,
    /// Asset family.
    /// 中文摘要：标的资产类别。
    pub asset_type: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// `GET /instruments/{cusip}` uses the same schema as one search row.
/// 中文摘要：与单条搜索结果共用相同字段结构的标的详情响应。
pub type InstrumentDetail = InstrumentSummary;

pub(super) fn project_instruments_search(
    value: &Value,
) -> Result<InstrumentsSearchResponse, ReadResponseError> {
    let fields = object(value, "instrumentsSearch")?;
    let rows = fields.get("instruments").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "instrumentsSearch.instruments",
        },
    )?;
    let instruments = rows
        .iter()
        .map(|row| project_instrument_summary(row, "instrumentsSearch.instruments[]"))
        .collect::<Result<_, _>>()?;
    Ok(InstrumentsSearchResponse {
        instruments,
        unknown_fields: unknown_fields(fields, &["instruments"]),
    })
}

pub(super) fn project_instrument_summary(
    value: &Value,
    path: &'static str,
) -> Result<InstrumentSummary, ReadResponseError> {
    let fields = object(value, path)?;
    Ok(InstrumentSummary {
        cusip: optional_string(fields, "cusip", "instrumentSummary.cusip")?,
        symbol: optional_string(fields, "symbol", "instrumentSummary.symbol")?,
        description: optional_string(fields, "description", "instrumentSummary.description")?,
        exchange: optional_string(fields, "exchange", "instrumentSummary.exchange")?,
        asset_type: optional_string(fields, "assetType", "instrumentSummary.assetType")?,
        unknown_fields: unknown_fields(
            fields,
            &["cusip", "symbol", "description", "exchange", "assetType"],
        ),
    })
}
