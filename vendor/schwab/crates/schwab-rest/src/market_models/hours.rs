//! Market schedule and session-time response projections.
//! 定义市场日程和交易时段响应投影。

use std::collections::BTreeMap;

use serde_json::Value;

use super::ReadResponseError;
use super::common::{object, optional_string, required_boolean, required_string, unknown_fields};
use crate::response_models::UnknownFields;

/// Market-hours records keyed by market and product identifiers.
/// 中文摘要：按市场与产品代码索引的交易日程响应。
#[derive(Clone, PartialEq)]
pub struct MarketHoursResponse {
    /// Dynamic market keys mapped to their dynamic product keys.
    /// 中文摘要：按市场名称索引的交易时间记录。
    pub markets: BTreeMap<String, BTreeMap<String, MarketHoursProduct>>,
}

/// One market-hours product and its named session arrays.
/// 中文摘要：一个市场产品的日期、开市标记及命名交易时段。
#[derive(Clone, PartialEq)]
pub struct MarketHoursProduct {
    /// Market date.
    /// 中文摘要：查询或市场记录日期。
    pub date: String,
    /// Market type.
    /// 中文摘要：市场类别。
    pub market_type: String,
    /// Product identifier.
    /// 中文摘要：产品类别。
    pub product: String,
    /// Optional product display name.
    /// 中文摘要：产品显示名称。
    pub product_name: Option<String>,
    /// Whether the product is open.
    /// 中文摘要：broker 返回的市场开市状态。
    pub is_open: bool,
    /// Sessions keyed by broker-provided session name.
    /// 中文摘要：按时段类别索引的交易时间。
    pub session_hours: BTreeMap<String, Vec<MarketSessionTime>>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Start and end of a market session.
/// 中文摘要：broker 返回的单段市场时段起止时间；保留原始时间文本。
#[derive(Clone, PartialEq)]
pub struct MarketSessionTime {
    /// Session start string as returned by the broker.
    /// 中文摘要：时段起始时间。
    pub start: String,
    /// Session end string as returned by the broker.
    /// 中文摘要：时段结束时间。
    pub end: String,
    /// Additive wire fields retained losslessly. The current Node Zod schema
    /// accepts but strips these fields from its parsed session-time object.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_market_hours(
    value: &Value,
) -> Result<MarketHoursResponse, ReadResponseError> {
    let markets = object(value, "marketHours")?;
    let markets = markets
        .iter()
        .map(|(market, products)| {
            let products = object(products, "marketHours.products")?;
            let products = products
                .iter()
                .map(|(key, value)| Ok((key.clone(), project_market_hours_product(value)?)))
                .collect::<Result<_, ReadResponseError>>()?;
            Ok((market.clone(), products))
        })
        .collect::<Result<_, ReadResponseError>>()?;
    Ok(MarketHoursResponse { markets })
}

fn project_market_hours_product(value: &Value) -> Result<MarketHoursProduct, ReadResponseError> {
    let fields = object(value, "marketHours.product")?;
    let sessions = object(
        fields
            .get("sessionHours")
            .ok_or(ReadResponseError::SchemaViolation {
                field: "marketHours.product.sessionHours",
            })?,
        "marketHours.product.sessionHours",
    )?;
    let session_hours = sessions
        .iter()
        .map(|(key, value)| {
            let values = value.as_array().ok_or(ReadResponseError::SchemaViolation {
                field: "marketHours.sessionHours[]",
            })?;
            let values = values
                .iter()
                .map(project_market_session_time)
                .collect::<Result<_, _>>()?;
            Ok((key.clone(), values))
        })
        .collect::<Result<_, ReadResponseError>>()?;

    Ok(MarketHoursProduct {
        date: required_string(fields, "date", "marketHours.product.date")?,
        market_type: required_string(fields, "marketType", "marketHours.product.marketType")?,
        product: required_string(fields, "product", "marketHours.product.product")?,
        product_name: optional_string(fields, "productName", "marketHours.product.productName")?,
        is_open: required_boolean(fields, "isOpen", "marketHours.product.isOpen")?,
        session_hours,
        unknown_fields: unknown_fields(
            fields,
            &[
                "date",
                "marketType",
                "product",
                "productName",
                "isOpen",
                "sessionHours",
            ],
        ),
    })
}

fn project_market_session_time(value: &Value) -> Result<MarketSessionTime, ReadResponseError> {
    let fields = object(value, "marketHours.sessionHours[]")?;
    Ok(MarketSessionTime {
        start: required_string(fields, "start", "marketHours.session.start")?,
        end: required_string(fields, "end", "marketHours.session.end")?,
        unknown_fields: unknown_fields(fields, &["start", "end"]),
    })
}
