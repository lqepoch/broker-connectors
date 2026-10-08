//! Response-family validation entry points and shared complexity limits.
//! 定义响应类型校验入口及共享复杂度上限。

mod common;
mod market;
mod trader;

use serde_json::Value;

use super::{MAX_ACCOUNT_NUMBER_HASH_ROWS, ReadResponseError, ReadResponseKind};
use common::validate_array;
use market::{
    validate_expiration_chain, validate_instrument_summary, validate_instruments_search,
    validate_market_hours, validate_movers, validate_option_chain, validate_price_history,
    validate_quotes, validate_single_quote,
};
use trader::{
    validate_account_number, validate_account_response, validate_order, validate_transaction,
    validate_user_preferences_union,
};

pub(super) use common::check_complexity;

pub(super) fn check_account_number_hash_row_limit(
    kind: ReadResponseKind,
    value: &Value,
) -> Result<(), ReadResponseError> {
    if kind != ReadResponseKind::AccountNumbers {
        return Ok(());
    }

    let rows = value.as_array().ok_or(ReadResponseError::SchemaViolation {
        field: "accountNumbers",
    })?;
    if rows.len() > MAX_ACCOUNT_NUMBER_HASH_ROWS {
        return Err(ReadResponseError::AccountNumberRowsTooMany);
    }
    Ok(())
}

pub(super) fn validate(kind: ReadResponseKind, value: &Value) -> Result<(), ReadResponseError> {
    match kind {
        ReadResponseKind::AccountNumbers => {
            validate_array(value, "accountNumbers", validate_account_number)
        }
        ReadResponseKind::Accounts => validate_array(value, "accounts", validate_account_response),
        ReadResponseKind::Account => validate_account_response(value),
        ReadResponseKind::Orders => {
            validate_array(value, "orders", |order| validate_order(order, 0))
        }
        ReadResponseKind::Order => validate_order(value, 0),
        ReadResponseKind::Transactions => {
            validate_array(value, "transactions", validate_transaction)
        }
        ReadResponseKind::Transaction => match value {
            Value::Array(_) => validate_array(value, "transaction", validate_transaction),
            _ => validate_transaction(value),
        },
        ReadResponseKind::UserPreferences => validate_user_preferences_union(value),
        ReadResponseKind::Quotes => validate_quotes(value),
        ReadResponseKind::SingleQuote => validate_single_quote(value),
        ReadResponseKind::OptionChain => validate_option_chain(value),
        ReadResponseKind::OptionExpirationChain => validate_expiration_chain(value),
        ReadResponseKind::PriceHistory => validate_price_history(value),
        ReadResponseKind::Movers => validate_movers(value),
        ReadResponseKind::MarketHours => validate_market_hours(value),
        ReadResponseKind::InstrumentsSearch => validate_instruments_search(value),
        ReadResponseKind::InstrumentDetail => validate_instrument_summary(value, "instrument"),
    }
}
