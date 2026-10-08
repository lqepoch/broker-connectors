//! Shape validation for market-data response families.
//! 校验市场数据响应各类型的结构。

use serde_json::Value;

use super::super::ReadResponseError;
use super::common::{
    object, optional_boolean, optional_number, optional_string, required_boolean,
    required_nonempty_string, required_number,
};

pub(super) fn validate_quotes(value: &Value) -> Result<(), ReadResponseError> {
    let quotes = object(value, "quotes")?;
    for item in quotes.values() {
        validate_quote_item(item)?;
    }
    Ok(())
}

fn validate_quote_item(value: &Value) -> Result<(), ReadResponseError> {
    let item = object(value, "quotes[*]")?;
    for key in ["assetMainType", "assetSubType", "quoteType"] {
        optional_string(item, key, "quotes[*].string", false)?;
    }
    required_nonempty_string(item, "symbol", "quotes[*].symbol")?;
    optional_boolean(item, "realtime", "quotes[*].realtime")?;
    optional_number(item, "ssid", "quotes[*].ssid")?;
    if let Some(reference) = item.get("reference") {
        let reference = object(reference, "quotes[*].reference")?;
        optional_string(reference, "cusip", "quotes[*].reference.cusip", true)?;
        for key in ["description", "exchange", "exchangeName"] {
            optional_string(reference, key, "quotes[*].reference.string", false)?;
        }
    }
    if let Some(quote) = item.get("quote") {
        let quote = object(quote, "quotes[*].quote")?;
        for key in [
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
        ] {
            optional_number(quote, key, "quotes[*].quote.number")?;
        }
    }
    for key in ["regular", "fundamental", "extended"] {
        if let Some(section) = item.get(key) {
            object(section, "quotes[*].section")?;
        }
    }
    Ok(())
}

pub(super) fn validate_single_quote(value: &Value) -> Result<(), ReadResponseError> {
    let quote = object(value, "singleQuote")?;
    optional_string(quote, "symbol", "singleQuote.symbol", false)?;
    optional_boolean(quote, "empty", "singleQuote.empty")?;
    for key in ["previousClose", "previousCloseDate"] {
        optional_number(quote, key, "singleQuote.number")?;
    }
    if let Some(candles) = quote.get("candles") {
        let candles = candles
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "singleQuote.candles",
            })?;
        for candle in candles {
            let candle = object(candle, "singleQuote.candles[]")?;
            for key in ["open", "high", "low", "close", "volume", "datetime"] {
                required_number(candle, key, "singleQuote.candle.number")?;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_option_chain(value: &Value) -> Result<(), ReadResponseError> {
    let chain = object(value, "optionChain")?;
    for key in ["symbol", "status"] {
        optional_string(chain, key, "optionChain.string", false)?;
    }
    for key in ["isDelayed", "isIndex"] {
        optional_boolean(chain, key, "optionChain.boolean")?;
    }
    optional_number(chain, "underlyingPrice", "optionChain.number")?;
    for key in ["callExpDateMap", "putExpDateMap"] {
        if let Some(expirations) = chain.get(key) {
            let expirations = object(expirations, "optionChain.expirations")?;
            for strikes in expirations.values() {
                let strikes = object(strikes, "optionChain.strikes")?;
                for contract in strikes.values() {
                    validate_option_contract(contract)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_option_contract(value: &Value) -> Result<(), ReadResponseError> {
    let contract = object(value, "optionChain.contract")?;
    if let Some(put_call) = contract.get("putCall")
        && !matches!(put_call.as_str(), Some("PUT" | "CALL"))
    {
        return Err(ReadResponseError::SchemaViolation {
            field: "optionChain.contract.putCall",
        });
    }
    for key in ["symbol", "expirationDate"] {
        optional_string(contract, key, "optionChain.contract.string", false)?;
    }
    for key in [
        "bidPrice",
        "askPrice",
        "markPrice",
        "strikePrice",
        "multiplier",
    ] {
        optional_number(contract, key, "optionChain.contract.number")?;
    }
    optional_boolean(
        contract,
        "isNonStandard",
        "optionChain.contract.isNonStandard",
    )
}

pub(super) fn validate_expiration_chain(value: &Value) -> Result<(), ReadResponseError> {
    let chain = object(value, "expirationChain")?;
    optional_string(chain, "symbol", "expirationChain.symbol", false)?;
    optional_boolean(chain, "isDelayed", "expirationChain.isDelayed")?;
    optional_boolean(chain, "isIndex", "expirationChain.isIndex")?;
    optional_number(chain, "underlyingPrice", "expirationChain.underlyingPrice")?;
    if let Some(expirations) = chain.get("expirationList") {
        let expirations = expirations
            .as_array()
            .ok_or(ReadResponseError::SchemaViolation {
                field: "expirationChain.expirationList",
            })?;
        for expiration in expirations {
            let expiration = object(expiration, "expirationChain.expirationList[]")?;
            required_nonempty_string(
                expiration,
                "expirationDate",
                "expirationChain.expirationDate",
            )?;
            required_number(
                expiration,
                "daysToExpiration",
                "expirationChain.daysToExpiration",
            )?;
            optional_string(
                expiration,
                "expirationType",
                "expirationChain.expirationType",
                false,
            )?;
            optional_boolean(expiration, "standard", "expirationChain.standard")?;
        }
    }
    Ok(())
}

pub(super) fn validate_price_history(value: &Value) -> Result<(), ReadResponseError> {
    let history = object(value, "priceHistory")?;
    optional_string(history, "symbol", "priceHistory.symbol", false)?;
    optional_boolean(history, "empty", "priceHistory.empty")?;
    for key in ["previousClose", "previousCloseDate"] {
        optional_number(history, key, "priceHistory.number")?;
    }
    let candles = history.get("candles").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "priceHistory.candles",
        },
    )?;
    for candle in candles {
        let candle = object(candle, "priceHistory.candles[]")?;
        for key in ["open", "high", "low", "close", "volume", "datetime"] {
            required_number(candle, key, "priceHistory.candle.number")?;
        }
    }
    Ok(())
}

pub(super) fn validate_movers(value: &Value) -> Result<(), ReadResponseError> {
    let movers = object(value, "movers")?;
    let screeners = movers.get("screeners").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "movers.screeners",
        },
    )?;
    for screener in screeners {
        let screener = object(screener, "movers.screeners[]")?;
        for key in ["change", "last", "totalVolume"] {
            optional_number(screener, key, "movers.screener.number")?;
        }
        for key in ["description", "symbol"] {
            optional_string(screener, key, "movers.screener.string", false)?;
        }
        if let Some(direction) = screener.get("direction")
            && !matches!(direction.as_str(), Some("up" | "down"))
        {
            return Err(ReadResponseError::SchemaViolation {
                field: "movers.screener.direction",
            });
        }
    }
    Ok(())
}

pub(super) fn validate_market_hours(value: &Value) -> Result<(), ReadResponseError> {
    let markets = object(value, "marketHours")?;
    for products in markets.values() {
        let products = object(products, "marketHours.products")?;
        for product in products.values() {
            let product = object(product, "marketHours.product")?;
            for key in ["date", "marketType", "product"] {
                required_nonempty_string(product, key, "marketHours.product.required")?;
            }
            optional_string(
                product,
                "productName",
                "marketHours.product.productName",
                false,
            )?;
            required_boolean(product, "isOpen", "marketHours.product.isOpen")?;
            let sessions = product
                .get("sessionHours")
                .and_then(Value::as_object)
                .ok_or(ReadResponseError::SchemaViolation {
                    field: "marketHours.product.sessionHours",
                })?;
            for hours in sessions.values() {
                let hours = hours.as_array().ok_or(ReadResponseError::SchemaViolation {
                    field: "marketHours.sessionHours[]",
                })?;
                for time in hours {
                    let time = object(time, "marketHours.sessionHours[]")?;
                    required_nonempty_string(time, "start", "marketHours.session.start")?;
                    required_nonempty_string(time, "end", "marketHours.session.end")?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_instruments_search(value: &Value) -> Result<(), ReadResponseError> {
    let search = object(value, "instrumentsSearch")?;
    let instruments = search.get("instruments").and_then(Value::as_array).ok_or(
        ReadResponseError::SchemaViolation {
            field: "instrumentsSearch.instruments",
        },
    )?;
    for instrument in instruments {
        validate_instrument_summary(instrument, "instrumentsSearch.instruments[]")?;
    }
    Ok(())
}

pub(super) fn validate_instrument_summary(
    value: &Value,
    path: &'static str,
) -> Result<(), ReadResponseError> {
    let summary = object(value, path)?;
    for key in ["cusip", "symbol", "description", "exchange", "assetType"] {
        optional_string(summary, key, "instrumentSummary.string", false)?;
    }
    Ok(())
}
