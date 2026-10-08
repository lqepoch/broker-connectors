//! Shared bounded validators for response object fields.
//! 提供响应对象字段的通用有界校验器。

use serde_json::{Map, Value};

use super::super::ReadResponseError;

const MAX_JSON_NODES: usize = 100_000;
const MAX_JSON_DEPTH: usize = 96;

pub(super) fn validate_array(
    value: &Value,
    path: &'static str,
    validate_item: impl Fn(&Value) -> Result<(), ReadResponseError>,
) -> Result<(), ReadResponseError> {
    let values = value
        .as_array()
        .ok_or(ReadResponseError::SchemaViolation { field: path })?;
    for item in values {
        validate_item(item)?;
    }
    Ok(())
}

pub(super) fn object<'a>(
    value: &'a Value,
    path: &'static str,
) -> Result<&'a Map<String, Value>, ReadResponseError> {
    value
        .as_object()
        .ok_or(ReadResponseError::SchemaViolation { field: path })
}

pub(super) fn required_nonempty_string(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    if object
        .get(key)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        Ok(())
    } else {
        Err(ReadResponseError::SchemaViolation { field: path })
    }
}

pub(super) fn optional_string(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
    nonempty: bool,
) -> Result<(), ReadResponseError> {
    match object.get(key) {
        None => Ok(()),
        Some(Value::String(value)) if !nonempty || !value.is_empty() => Ok(()),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn optional_number(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    match object.get(key) {
        None => Ok(()),
        Some(Value::Number(number)) if number.as_f64().is_some_and(f64::is_finite) => Ok(()),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn required_number(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    if object.get(key).is_some_and(Value::is_number) {
        optional_number(object, key, path)
    } else {
        Err(ReadResponseError::SchemaViolation { field: path })
    }
}

pub(super) fn optional_boolean(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    match object.get(key) {
        None | Some(Value::Bool(_)) => Ok(()),
        _ => Err(ReadResponseError::SchemaViolation { field: path }),
    }
}

pub(super) fn required_boolean(
    object: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    if object.get(key).is_some_and(Value::is_boolean) {
        Ok(())
    } else {
        Err(ReadResponseError::SchemaViolation { field: path })
    }
}

pub(in crate::read_response) fn check_complexity(value: &Value) -> Result<(), ReadResponseError> {
    let mut stack = vec![(value, 1usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH {
            return Err(ReadResponseError::JsonTooComplex);
        }
        match value {
            Value::Array(values) => stack.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => stack.extend(values.values().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}
