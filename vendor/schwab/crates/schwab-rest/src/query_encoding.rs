//! Query-pair assembly and WHATWG-compatible form encoding for fixed REST routes.
//! 固定 REST 路由的查询参数组装与 WHATWG 兼容表单编码。

use crate::request_types::{
    DecimalQuery, OptionChainQuery, OrdersQuery, QueryText, TransactionsQuery,
};

pub(crate) fn append_orders_query(query: &mut Vec<(String, String)>, params: &OrdersQuery) {
    push_text(query, "fromEnteredTime", Some(&params.from_entered_time));
    push_text(query, "toEnteredTime", Some(&params.to_entered_time));
    push_i64(query, "maxResults", params.max_results);
    push_text(query, "status", params.status.as_ref());
}

pub(crate) fn append_transactions_query(
    query: &mut Vec<(String, String)>,
    params: &TransactionsQuery,
) {
    push_text(query, "startDate", Some(&params.start_date));
    push_text(query, "endDate", Some(&params.end_date));
    push_text(query, "types", Some(&params.types));
    push_text(query, "symbol", params.symbol.as_ref());
}

pub(crate) fn append_option_chain_query(
    query: &mut Vec<(String, String)>,
    params: &OptionChainQuery,
) {
    push_text(query, "symbol", Some(&params.symbol));
    push_text(query, "contractType", params.contract_type.as_ref());
    let include_underlying_quote = params.include_underlying_quote.or(params.include_quotes);
    push_bool(query, "includeUnderlyingQuote", include_underlying_quote);
    push_text(query, "strategy", params.strategy.as_ref());
    push_i64(query, "interval", params.interval);
    push_i64(query, "strikeCount", params.strike_count);
    push_decimal(query, "strike", params.strike.as_ref());
    push_text(query, "range", params.range.as_ref());
    push_text(query, "fromDate", params.from_date.as_ref());
    push_text(query, "toDate", params.to_date.as_ref());
    push_decimal(query, "volatility", params.volatility.as_ref());
    push_decimal(query, "underlyingPrice", params.underlying_price.as_ref());
    push_decimal(query, "interestRate", params.interest_rate.as_ref());
    push_i64(query, "daysToExpiration", params.days_to_expiration);
    push_text(query, "expMonth", params.exp_month.as_ref());
    push_text(query, "optionType", params.option_type.as_ref());
    push_text(query, "entitlement", params.entitlement.as_ref());
}

pub(crate) fn push_text(
    query: &mut Vec<(String, String)>,
    key: &'static str,
    value: Option<&QueryText>,
) {
    push_pair_option(query, key, value.map(QueryText::as_str));
}

pub(crate) fn push_decimal(
    query: &mut Vec<(String, String)>,
    key: &'static str,
    value: Option<&DecimalQuery>,
) {
    push_pair_option(query, key, value.map(DecimalQuery::as_str));
}

pub(crate) fn push_i64(query: &mut Vec<(String, String)>, key: &'static str, value: Option<i64>) {
    if let Some(value) = value {
        push_pair(query, key, &value.to_string());
    }
}

pub(crate) fn push_bool(query: &mut Vec<(String, String)>, key: &'static str, value: Option<bool>) {
    if let Some(value) = value {
        push_pair(query, key, if value { "true" } else { "false" });
    }
}

pub(crate) fn push_pair_option(
    query: &mut Vec<(String, String)>,
    key: &'static str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        push_pair(query, key, value);
    }
}

pub(crate) fn push_pair(query: &mut Vec<(String, String)>, key: &str, value: &str) {
    query.push((key.to_owned(), value.to_owned()));
}

/// Mirrors WHATWG URLSearchParams application/x-www-form-urlencoded
/// serialization used by the Node HttpClient for query names and values.
pub(crate) fn encode_query_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_') {
            encoded.push(char::from(byte));
        } else if byte == b' ' {
            encoded.push('+');
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}
