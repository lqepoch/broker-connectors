//! Shape validation for trader account, order, and transaction responses.
//! 校验账户、订单和交易记录响应的结构。

use reqwest::Url;
use serde_json::Value;

use super::super::ReadResponseError;
use super::common::{
    object, optional_boolean, optional_number, optional_string, required_nonempty_string,
};

pub(super) fn validate_account_number(value: &Value) -> Result<(), ReadResponseError> {
    let object = object(value, "accountNumbers[]")?;
    required_nonempty_string(object, "accountNumber", "accountNumbers[].accountNumber")?;
    required_nonempty_string(object, "hashValue", "accountNumbers[].hashValue")
}

pub(super) fn validate_account_response(value: &Value) -> Result<(), ReadResponseError> {
    let wrapper = object(value, "account")?;
    let security = wrapper
        .get("securitiesAccount")
        .ok_or(ReadResponseError::SchemaViolation {
            field: "securitiesAccount",
        })?;
    let account = object(security, "securitiesAccount")?;
    required_nonempty_string(account, "accountNumber", "securitiesAccount.accountNumber")?;
    optional_string(account, "type", "securitiesAccount.type", false)?;
    for key in [
        "roundTrips",
        "isDayTrader",
        "isClosingOnlyRestricted",
        "pfcbFlag",
    ] {
        if key == "roundTrips" {
            optional_number(account, key, "securitiesAccount.roundTrips")?;
        } else {
            optional_boolean(account, key, "securitiesAccount.flag")?;
        }
    }
    if let Some(positions) = account.get("positions") {
        let positions = positions
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "securitiesAccount.positions",
            })?;
        for position in positions {
            let position = object(position, "securitiesAccount.positions[]")?;
            for key in [
                "shortQuantity",
                "averagePrice",
                "currentDayProfitLoss",
                "currentDayProfitLossPercentage",
                "longQuantity",
                "marketValue",
            ] {
                optional_number(position, key, "securitiesAccount.positions[].number")?;
            }
            if let Some(instrument) = position.get("instrument") {
                validate_instrument(instrument, "securitiesAccount.positions[].instrument")?;
            }
        }
    }
    for key in ["initialBalances", "currentBalances", "projectedBalances"] {
        if let Some(balances) = account.get(key) {
            object(balances, "securitiesAccount.balances")?;
        }
    }
    Ok(())
}

fn validate_instrument(value: &Value, path: &'static str) -> Result<(), ReadResponseError> {
    let object = object(value, path)?;
    for key in ["assetType", "cusip", "symbol", "description", "type"] {
        optional_string(object, key, "instrument.string", false)?;
    }
    for key in ["instrumentId", "netChange"] {
        optional_number(object, key, "instrument.number")?;
    }
    Ok(())
}

pub(super) fn validate_order(value: &Value, depth: usize) -> Result<(), ReadResponseError> {
    if depth > 32 {
        return Err(ReadResponseError::JsonTooComplex);
    }
    let order = object(value, "order")?;
    for key in [
        "session",
        "duration",
        "orderType",
        "complexOrderStrategyType",
        "orderStrategyType",
        "status",
        "enteredTime",
        "closeTime",
        "tag",
        "statusDescription",
    ] {
        optional_string(order, key, "order.string", false)?;
    }
    for key in [
        "quantity",
        "filledQuantity",
        "remainingQuantity",
        "stopPrice",
        "price",
        "orderId",
    ] {
        optional_number(order, key, "order.number")?;
    }
    for key in ["cancelable", "editable"] {
        optional_boolean(order, key, "order.boolean")?;
    }
    if let Some(account_number) = order.get("accountNumber") {
        if !account_number.is_string() && !account_number.is_number() {
            return Err(ReadResponseError::SchemaViolation {
                field: "order.accountNumber",
            });
        }
        if account_number.is_number() {
            optional_number(order, "accountNumber", "order.accountNumber")?;
        }
    }
    if let Some(legs) = order.get("orderLegCollection") {
        let legs = legs.as_array().ok_or(ReadResponseError::SchemaViolation {
            field: "order.orderLegCollection",
        })?;
        for leg in legs {
            validate_order_leg(leg)?;
        }
    }
    if let Some(activities) = order.get("orderActivityCollection") {
        let activities = activities
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "order.orderActivityCollection",
            })?;
        for activity in activities {
            validate_order_activity(activity)?;
        }
    }
    if let Some(children) = order.get("childOrderStrategies") {
        let children = children
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "order.childOrderStrategies",
            })?;
        for child in children {
            validate_order(child, depth + 1)?;
        }
    }
    Ok(())
}

fn validate_order_leg(value: &Value) -> Result<(), ReadResponseError> {
    let leg = object(value, "order.orderLegCollection[]")?;
    for key in [
        "orderLegType",
        "instruction",
        "positionEffect",
        "quantityType",
    ] {
        optional_string(leg, key, "order.orderLegCollection[].string", false)?;
    }
    for key in ["legId", "quantity"] {
        optional_number(leg, key, "order.orderLegCollection[].number")?;
    }
    if let Some(instrument) = leg.get("instrument") {
        validate_instrument(instrument, "order.orderLegCollection[].instrument")?;
    }
    Ok(())
}

fn validate_order_activity(value: &Value) -> Result<(), ReadResponseError> {
    let activity = object(value, "order.orderActivityCollection[]")?;
    for key in ["activityType", "executionType"] {
        optional_string(
            activity,
            key,
            "order.orderActivityCollection[].string",
            false,
        )?;
    }
    for key in ["quantity", "orderRemainingQuantity"] {
        optional_number(activity, key, "order.orderActivityCollection[].number")?;
    }
    if let Some(legs) = activity.get("executionLegs") {
        let legs = legs.as_array().ok_or(ReadResponseError::SchemaViolation {
            field: "order.orderActivityCollection[].executionLegs",
        })?;
        for leg in legs {
            let leg = object(leg, "order.orderActivityCollection[].executionLegs[]")?;
            for key in [
                "legId",
                "price",
                "quantity",
                "mismarkedQuantity",
                "instrumentId",
            ] {
                optional_number(leg, key, "order.executionLeg.number")?;
            }
            optional_string(leg, "time", "order.executionLeg.time", false)?;
        }
    }
    Ok(())
}

pub(super) fn validate_transaction(value: &Value) -> Result<(), ReadResponseError> {
    let transaction = object(value, "transaction")?;
    for key in [
        "time",
        "description",
        "accountNumber",
        "type",
        "status",
        "tradeDate",
        "settlementDate",
    ] {
        optional_string(transaction, key, "transaction.string", false)?;
    }
    for key in ["activityId", "positionId", "orderId", "netAmount"] {
        optional_number(transaction, key, "transaction.number")?;
    }
    Ok(())
}

pub(super) fn validate_user_preferences_union(value: &Value) -> Result<(), ReadResponseError> {
    match value {
        Value::Object(_) => validate_user_preference(value),
        Value::Array(preferences) => {
            for preference in preferences {
                validate_user_preference(preference)?;
            }
            Ok(())
        }
        _ => Err(ReadResponseError::SchemaViolation {
            field: "userPreference",
        }),
    }
}

fn validate_user_preference(value: &Value) -> Result<(), ReadResponseError> {
    let preference = object(value, "userPreference")?;
    if let Some(accounts) = preference.get("accounts") {
        let accounts = accounts
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "userPreference.accounts",
            })?;
        for account in accounts {
            let account = object(account, "userPreference.accounts[]")?;
            required_nonempty_string(
                account,
                "accountNumber",
                "userPreference.accounts[].accountNumber",
            )?;
            for key in ["primaryAccount", "autoPositionEffect"] {
                optional_boolean(account, key, "userPreference.accounts[].boolean")?;
            }
            for key in ["type", "nickName", "accountColor", "displayAcctId"] {
                optional_string(account, key, "userPreference.accounts[].string", false)?;
            }
        }
    }
    if let Some(streamer_info) = preference.get("streamerInfo") {
        let streamer_info = streamer_info
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "userPreference.streamerInfo",
            })?;
        for info in streamer_info {
            validate_streamer_info(info)?;
        }
    }
    if let Some(offers) = preference.get("offers") {
        let offers = offers
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "userPreference.offers",
            })?;
        for offer in offers {
            let offer = object(offer, "userPreference.offers[]")?;
            optional_boolean(
                offer,
                "level2Permissions",
                "userPreference.offers[].level2Permissions",
            )?;
            optional_string(
                offer,
                "mktDataPermission",
                "userPreference.offers[].mktDataPermission",
                false,
            )?;
        }
    }
    Ok(())
}

fn validate_streamer_info(value: &Value) -> Result<(), ReadResponseError> {
    let info = object(value, "streamerInfo")?;
    for key in [
        "streamerSocketUrl",
        "schwabClientCustomerId",
        "schwabClientCorrelId",
        "schwabClientChannel",
        "schwabClientFunctionId",
    ] {
        required_nonempty_string(info, key, "streamerInfo.required")?;
    }
    let url = info
        .get("streamerSocketUrl")
        .and_then(Value::as_str)
        .ok_or(ReadResponseError::SchemaViolation {
            field: "streamerInfo.streamerSocketUrl",
        })?;
    Url::parse(url).map_err(|_| ReadResponseError::SchemaViolation {
        field: "streamerInfo.streamerSocketUrl",
    })?;
    Ok(())
}
