//! Shared wire-number and additive-field types for market responses.
//! 定义市场响应共用的 wire 数值和附加字段类型。

use serde_json::{Map, Value};

use crate::read_response::ReadResponseError;
use crate::response_models::{UnknownFields, WireNumber};

pub(super) fn object<'a>(
    value: &'a Value,
    path: &'static str,
) -> Result<&'a Map<String, Value>, ReadResponseError> {
    value
        .as_object()
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn required_string(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<String, ReadResponseError> {
    match fields.get(key) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn optional_string(
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

pub(super) fn required_boolean(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<bool, ReadResponseError> {
    fields
        .get(key)
        .and_then(Value::as_bool)
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn optional_boolean(
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

pub(super) fn optional_number(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<Option<WireNumber>, ReadResponseError> {
    match fields.get(key) {
        None => Ok(None),
        Some(Value::Number(value)) => Ok(Some(WireNumber::from_number(value)?)),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn required_number(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<WireNumber, ReadResponseError> {
    optional_number(fields, key, path)?.ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn unknown_fields(fields: &Map<String, Value>, known: &[&str]) -> UnknownFields {
    UnknownFields::from_object(fields, known)
}
