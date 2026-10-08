//! Shared lossless wire numbers and unknown response fields.
//! 定义无损 wire 数值和未知响应字段。

use std::collections::BTreeMap;

use serde_json::{Map, Number, Value};

use super::BrokerAccountNumber;
use crate::read_response::{ExactDecimal, ReadResponseError};

/// Instrument identifiers embedded in accounts, orders, and transactions.
/// 中文摘要：账户、订单或交易记录引用的证券标的及 broker 扩展字段。
#[derive(Clone, PartialEq)]
pub struct Instrument {
    /// Asset family, including future values unknown to this SDK version.
    /// 中文摘要：标的资产类别。
    pub asset_type: Option<String>,
    /// CUSIP identifier.
    /// 中文摘要：证券 CUSIP 标识符。
    pub cusip: Option<String>,
    /// Broker symbol.
    /// 中文摘要：标的或合约交易代码。
    pub symbol: Option<String>,
    /// Broker description.
    /// 中文摘要：标的或产品说明。
    pub description: Option<String>,
    /// Schwab instrument identifier.
    /// 中文摘要：Schwab 的证券标的编号；保留原始精确数值。
    pub instrument_id: Option<WireNumber>,
    /// Day-over-day net change.
    /// 中文摘要：相对前一交易日的净变动数值。
    pub net_change: Option<WireNumber>,
    /// Broker instrument type.
    /// 中文摘要：Schwab 提供的标的类型字符串，包含未来版本新增的值。
    pub instrument_type: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Exact JSON numeric token from the response body.
/// 中文摘要：保留 broker JSON 数字原始 token 的无损数值包装。
#[derive(Clone, Eq, PartialEq)]
pub struct WireNumber(String);

impl WireNumber {
    /// Returns the exact numeric token, including exponent and trailing zeros.
    /// 中文摘要：返回该值的文本或 wire 表示。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Converts to the crate's bounded exact-decimal arithmetic representation.
    /// 中文摘要：将精确 JSON 数值解析为有界十进制表示；精度超界时返回固定错误。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn exact_decimal(&self) -> Result<ExactDecimal, ReadResponseError> {
        ExactDecimal::parse(&self.0)
    }

    pub(crate) fn from_number(number: &Number) -> Result<Self, ReadResponseError> {
        // This check mirrors Node's finite-number schema constraint. The
        // converted value is never used as DTO data or numeric authority.
        if !number.as_f64().is_some_and(f64::is_finite) {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        let lexeme = number.to_string();
        Ok(Self(lexeme))
    }
}

/// Additive response fields retained as original JSON values.
/// 中文摘要：保留模型未识别的附加 JSON 字段。
#[derive(Clone, Default, PartialEq)]
pub struct UnknownFields(BTreeMap<String, Value>);

impl UnknownFields {
    /// Reads one unrecognized field.
    /// 中文摘要：读取
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// Iterates unrecognized field names and values.
    /// 中文摘要：迭代未识别字段名及其保留的 JSON 值，不解释字段语义。
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    /// Returns whether no unrecognized fields were present.
    /// 中文摘要：判断该值是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn from_object(fields: &Map<String, Value>, known: &[&str]) -> Self {
        Self(
            fields
                .iter()
                .filter(|(key, _)| !known.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }
}

pub(super) fn project_instrument(value: &Value) -> Result<Instrument, ReadResponseError> {
    let fields = object(value, "instrument")?;
    Ok(Instrument {
        asset_type: string(fields, "assetType", "instrument.assetType")?,
        cusip: string(fields, "cusip", "instrument.cusip")?,
        symbol: string(fields, "symbol", "instrument.symbol")?,
        description: string(fields, "description", "instrument.description")?,
        instrument_id: number(fields, "instrumentId", "instrument.instrumentId")?,
        net_change: number(fields, "netChange", "instrument.netChange")?,
        instrument_type: string(fields, "type", "instrument.type")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "assetType",
                "cusip",
                "symbol",
                "description",
                "instrumentId",
                "netChange",
                "type",
            ],
        ),
    })
}

pub(super) fn object<'a>(
    value: &'a Value,
    path: &'static str,
) -> Result<&'a Map<String, Value>, ReadResponseError> {
    value
        .as_object()
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn array<'a>(
    value: &'a Value,
    path: &'static str,
) -> Result<&'a Vec<Value>, ReadResponseError> {
    value
        .as_array()
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn required_string(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<String, ReadResponseError> {
    string(fields, key, path)?
        .filter(|value| !value.is_empty())
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn string(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<Option<String>, ReadResponseError> {
    match fields.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn boolean(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<Option<bool>, ReadResponseError> {
    match fields.get(key) {
        None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn number(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<Option<WireNumber>, ReadResponseError> {
    fields
        .get(key)
        .map(|value| wire_number(value, path))
        .transpose()
}

pub(super) fn wire_number(
    value: &Value,
    path: &'static str,
) -> Result<WireNumber, ReadResponseError> {
    value
        .as_number()
        .ok_or(ReadResponseError::SchemaViolation { field: path })
        .and_then(WireNumber::from_number)
}

pub(super) fn account_number(
    fields: &Map<String, Value>,
    key: &str,
) -> Result<Option<BrokerAccountNumber>, ReadResponseError> {
    match fields.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(BrokerAccountNumber::Text(value.clone()))),
        Some(Value::Number(value)) => Ok(Some(BrokerAccountNumber::Number(
            WireNumber::from_number(value)?,
        ))),
        _ => Err(ReadResponseError::SchemaViolation {
            field: "order.accountNumber",
        }),
    }
}

pub(super) fn optional_object<T>(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
    project: impl FnOnce(&Value) -> Result<T, ReadResponseError>,
) -> Result<Option<T>, ReadResponseError> {
    fields
        .get(key)
        .map(project)
        .transpose()
        .map_err(|error| match error {
            ReadResponseError::SchemaViolation { .. } => {
                ReadResponseError::SchemaViolation { field: path }
            }
            other => other,
        })
}

pub(super) fn optional_array<T>(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
    mut project: impl FnMut(&Value) -> Result<T, ReadResponseError>,
) -> Result<Option<Vec<T>>, ReadResponseError> {
    let Some(value) = fields.get(key) else {
        return Ok(None);
    };
    array(value, path)?
        .iter()
        .map(&mut project)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn unknown_fields(fields: &Map<String, Value>, known: &[&str]) -> UnknownFields {
    UnknownFields::from_object(fields, known)
}
