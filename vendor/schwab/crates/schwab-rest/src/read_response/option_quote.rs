//! Structural option-quote normalization that preserves exact numeric values.
//! 提供保留精确数值的结构化期权报价归一化。

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde_json::{Map, Value};

use crate::routes::trim_option_quote_symbol;

use super::{ExactDecimal, ExactRatio, ParsedReadResponse, ReadResponseError, ReadResponseKind};

impl ParsedReadResponse {
    /// Normalizes requested option codes and projects matching rows in caller order; duplicate or missing contracts return fixed errors.
    /// 按 Node 兼容规则归一化交易代码后读取期权报价。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn option_quotes(
        &self,
        requested_symbols: &[&str],
        observed_at_ms: i64,
    ) -> Result<Vec<NormalizedOptionQuote>, ReadResponseError> {
        if self.kind != ReadResponseKind::Quotes {
            return Err(ReadResponseError::WrongResponseKind);
        }
        if requested_symbols.is_empty() || observed_at_ms < 0 {
            return Err(ReadResponseError::QuoteUnavailable);
        }
        let root = self
            .json
            .as_ref()
            .ok_or(ReadResponseError::QuoteUnavailable)?;
        let map = root
            .as_object()
            .ok_or(ReadResponseError::SchemaViolation { field: "quotes" })?;
        let mut seen = BTreeSet::new();
        let mut alias_index: Option<BTreeMap<&str, &Value>> = None;
        let mut quotes = Vec::with_capacity(requested_symbols.len());
        for (index, symbol) in requested_symbols.iter().enumerate() {
            let normalized = trim_option_quote_symbol(symbol);
            if normalized.is_empty() || !seen.insert(normalized) {
                return Err(ReadResponseError::DuplicateQuoteSymbol);
            }

            let item = if let Some(item) = map.get(normalized) {
                // Raw response keys always take precedence over compatibility
                // aliases, just as they did in the previous full-map index.
                item
            } else {
                // Most quote requests use the exact provider key. Build the
                // compatibility index only after the first exact miss, and
                // retain only aliases that can satisfy this request or a later
                // one. The map's iteration order and per-row alias order keep
                // the previous first-alias-wins behavior.
                if alias_index.is_none() {
                    let requested_aliases: BTreeSet<&str> = requested_symbols[index..]
                        .iter()
                        .map(|requested| trim_option_quote_symbol(requested))
                        .filter(|requested| !requested.is_empty())
                        .collect();
                    let mut aliases = BTreeMap::new();
                    for (key, item) in map {
                        let normalized_key = trim_option_quote_symbol(key);
                        if !normalized_key.is_empty() && requested_aliases.contains(normalized_key)
                        {
                            aliases.entry(normalized_key).or_insert(item);
                        }
                        if let Some(symbol) = item.get("symbol").and_then(Value::as_str)
                            && !symbol.is_empty()
                        {
                            if requested_aliases.contains(symbol) {
                                aliases.entry(symbol).or_insert(item);
                            }
                            let normalized_symbol = trim_option_quote_symbol(symbol);
                            if !normalized_symbol.is_empty()
                                && requested_aliases.contains(normalized_symbol)
                            {
                                aliases.entry(normalized_symbol).or_insert(item);
                            }
                        }
                    }
                    alias_index = Some(aliases);
                }
                alias_index
                    .as_ref()
                    .and_then(|aliases| aliases.get(normalized).copied())
                    .ok_or(ReadResponseError::QuoteUnavailable)?
            };
            quotes.push(normalize_option_quote(item, normalized, observed_at_ms)?);
        }
        Ok(quotes)
    }

    /// Returns validated normalized legs in caller order. Synthetic spread
    /// prices are intentionally left to the Decimal pricing crate.
    /// 中文摘要：返回精确保留 long/short 顺序的两条报价腿。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn vertical_quote_legs(
        &self,
        long_symbol: &str,
        short_symbol: &str,
        observed_at_ms: i64,
    ) -> Result<VerticalQuoteLegs, ReadResponseError> {
        let quotes = self.option_quotes(&[long_symbol, short_symbol], observed_at_ms)?;
        let mut quotes = quotes.into_iter();
        let long = quotes.next().ok_or(ReadResponseError::QuoteUnavailable)?;
        let short = quotes.next().ok_or(ReadResponseError::QuoteUnavailable)?;
        Ok(VerticalQuoteLegs { long, short })
    }
}

/// The pre-optimization implementation, compiled only into tests so the
/// production response API exposes no reference-only method.
#[cfg(test)]
pub(crate) fn option_quotes_indexed_reference(
    response: &ParsedReadResponse,
    requested_symbols: &[&str],
    observed_at_ms: i64,
) -> Result<Vec<NormalizedOptionQuote>, ReadResponseError> {
    if response.kind != ReadResponseKind::Quotes {
        return Err(ReadResponseError::WrongResponseKind);
    }
    if requested_symbols.is_empty() || observed_at_ms < 0 {
        return Err(ReadResponseError::QuoteUnavailable);
    }
    let root = response
        .json
        .as_ref()
        .ok_or(ReadResponseError::QuoteUnavailable)?;
    let map = root
        .as_object()
        .ok_or(ReadResponseError::SchemaViolation { field: "quotes" })?;
    let mut index: BTreeMap<String, &Value> = BTreeMap::new();
    for (key, item) in map {
        index.insert(key.clone(), item);
    }
    for (key, item) in map {
        let normalized_key = trim_option_quote_symbol(key);
        if !normalized_key.is_empty() {
            index.entry(normalized_key.to_owned()).or_insert(item);
        }
        if let Some(symbol) = item.get("symbol").and_then(Value::as_str)
            && !symbol.is_empty()
        {
            index.entry(symbol.to_owned()).or_insert(item);
            let normalized_symbol = trim_option_quote_symbol(symbol);
            if !normalized_symbol.is_empty() {
                index.entry(normalized_symbol.to_owned()).or_insert(item);
            }
        }
    }
    let mut seen = BTreeSet::new();
    requested_symbols
        .iter()
        .map(|symbol| {
            let normalized = trim_option_quote_symbol(symbol);
            if normalized.is_empty() || !seen.insert(normalized.to_owned()) {
                return Err(ReadResponseError::DuplicateQuoteSymbol);
            }
            let item = index
                .get(normalized)
                .copied()
                .ok_or(ReadResponseError::QuoteUnavailable)?;
            normalize_option_quote(item, normalized, observed_at_ms)
        })
        .collect()
}

/// Structurally normalized single-option quote. Decimal fields preserve wire
/// values exactly, but this type does not prove freshness, non-future time,
/// uncrossed NBBO, entitlement, or tradeability. A market-data quality gate
/// must establish those properties before trading uses a quote. Debug never
/// emits symbols, prices, or unknown source data.
/// 中文摘要：结构化期权报价投影；不代表报价已通过新鲜度或可交易性闸门。
pub struct NormalizedOptionQuote {
    /// Requested option symbol after compatibility whitespace normalization.
    /// 按兼容规则去除首尾空白后的请求期权代码。
    pub symbol: String,
    /// Underlying symbol reported for the option contract.
    /// 该期权合约对应的标的代码。
    pub underlying: Option<String>,
    /// Broker contract classification, typically call or put.
    /// 期权合约类型。
    pub contract_type: Option<String>,
    /// Option expiration value reported by the quote row.
    /// 期权到期日。
    pub expiration: Option<String>,
    /// Exact decimal strike price from the quote row.
    /// 期权行权价。
    pub strike: Option<ExactDecimal>,
    /// Broker-provided realtime indicator; it does not prove quote freshness.
    /// broker 报价是否标记为实时。
    pub realtime: Option<bool>,
    /// Broker quote category reported for this contract.
    /// broker 报价类别。
    pub quote_type: Option<String>,
    /// Exact decimal bid price.
    /// 买方报价价格。
    pub bid: Option<ExactDecimal>,
    /// Exact decimal ask price.
    /// 卖方报价价格。
    pub ask: Option<ExactDecimal>,
    /// Exact decimal bid size reported by the broker.
    /// 买方报价数量。
    pub bid_size: Option<ExactDecimal>,
    /// Exact decimal ask size reported by the broker.
    /// 卖方报价数量。
    pub ask_size: Option<ExactDecimal>,
    /// Exact decimal broker mark value.
    /// 响应中的 broker mark 值。
    pub mark: Option<ExactDecimal>,
    /// Exact decimal last-traded price.
    /// 最近成交价格。
    pub last: Option<ExactDecimal>,
    /// Exact midpoint derived from bid and ask when both are present.
    /// 由报价腿得到的结构化中间价。
    pub mid: Option<ExactDecimal>,
    /// Exact ask-minus-bid spread when both sides are present.
    /// 由报价 bid/ask 计算的结构化价差。
    pub spread: Option<ExactDecimal>,
    /// Exact spread-to-mid ratio; this diagnostic is not a quality decision.
    /// 价差相对中间价的精确比值；不经过二进制浮点舍入。
    pub spread_percent_of_mid: Option<ExactRatio>,
    /// Exact source quote timestamp value from the broker payload.
    /// 报价来源时间。
    pub quote_time: Option<ExactDecimal>,
    /// Exact source trade timestamp value from the broker payload.
    /// 最近成交来源时间。
    pub trade_time: Option<ExactDecimal>,
    /// Signed diagnostic age `observed_at_ms - quoteTime`; negative means the
    /// source timestamp is in the future. This is not a freshness or
    /// tradability validation result.
    /// 中文摘要：观察时刻与报价来源时间的有符号差值；该值本身不判定新鲜度。
    pub quote_age_ms: Option<ExactDecimal>,
    /// Exact broker-reported option delta.
    /// 响应中的 delta 希腊值。
    pub delta: Option<ExactDecimal>,
    /// Exact broker-reported option gamma.
    /// 响应中的 gamma 希腊值。
    pub gamma: Option<ExactDecimal>,
    /// Exact broker-reported option theta.
    /// 响应中的 theta 希腊值。
    pub theta: Option<ExactDecimal>,
    /// Exact broker-reported option vega.
    /// 响应中的 vega 希腊值。
    pub vega: Option<ExactDecimal>,
    /// Exact broker-reported option rho.
    /// 响应中的 rho 希腊值。
    pub rho: Option<ExactDecimal>,
    /// Exact volatility value reported by the broker.
    /// 响应中的波动率数值。
    pub volatility: Option<ExactDecimal>,
    /// Exact broker-reported open-interest count.
    /// broker 报告的精确未平仓合约数。
    pub open_interest: Option<ExactDecimal>,
    /// Exact broker-reported cumulative contract volume.
    /// 累计成交量。
    pub total_volume: Option<ExactDecimal>,
    /// Exact underlying price included in this option quote row.
    /// 期权链或报价所对应的标的价格。
    pub underlying_price: Option<ExactDecimal>,
    /// Exact broker-reported theoretical option value.
    /// broker 报告的精确理论期权价值。
    pub theoretical_option_value: Option<ExactDecimal>,
    /// Exact broker-reported option time value.
    /// broker 报告的精确期权时间价值。
    pub time_value: Option<ExactDecimal>,
    /// Exact broker-reported option intrinsic value.
    /// broker 报告的精确期权内在价值。
    pub intrinsic_value: Option<ExactDecimal>,
}

impl fmt::Debug for NormalizedOptionQuote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedOptionQuote")
            .field("symbol", &"[REDACTED]")
            .field("quote", &"[REDACTED]")
            .finish()
    }
}

/// Two structurally normalized legs for a vertical. This does not establish
/// quote freshness or market quality and does not generate a synthetic market;
/// the market-data gate and pricing layer own those respective decisions.
/// 中文摘要：按 long/short 角色保留的两个规范化报价腿；不计算订单价格。
pub struct VerticalQuoteLegs {
    /// Normalized quote assigned to the caller-designated long leg.
    /// 多头报价腿。
    pub long: NormalizedOptionQuote,
    /// Normalized quote assigned to the caller-designated short leg.
    /// 空头报价腿。
    pub short: NormalizedOptionQuote,
}

impl fmt::Debug for VerticalQuoteLegs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerticalQuoteLegs")
            .field("long", &"[REDACTED]")
            .field("short", &"[REDACTED]")
            .finish()
    }
}

fn normalize_option_quote(
    item: &Value,
    requested_symbol: &str,
    observed_at_ms: i64,
) -> Result<NormalizedOptionQuote, ReadResponseError> {
    if item.get("assetMainType").and_then(Value::as_str) != Some("OPTION") {
        return Err(ReadResponseError::QuoteNotOption);
    }
    let item_symbol = item
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or(ReadResponseError::QuoteIdentityMismatch)?;
    if trim_option_quote_symbol(item_symbol) != requested_symbol {
        return Err(ReadResponseError::QuoteIdentityMismatch);
    }
    let empty = Map::new();
    let reference = item
        .get("reference")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let quote = item
        .get("quote")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let parsed = parse_occ_symbol(item_symbol);

    let bid = optional_decimal(quote, "bidPrice")?;
    let ask = optional_decimal(quote, "askPrice")?;
    let spread = match (&bid, &ask) {
        (Some(bid), Some(ask)) => Some(ask.checked_sub(bid)?),
        _ => None,
    };
    let mid = match (&bid, &ask) {
        (Some(bid), Some(ask)) => Some(bid.checked_add(ask)?.checked_div_two()?),
        _ => None,
    };
    let spread_percent_of_mid = match (&spread, &mid) {
        (Some(spread), Some(mid)) if mid.coefficient != 0 => Some(ExactRatio {
            numerator: spread.checked_mul_hundred()?,
            denominator: mid.checked_abs()?,
        }),
        _ => None,
    };
    let quote_time = ExactDecimal::positive(optional_decimal(quote, "quoteTime")?);
    let trade_time = ExactDecimal::positive(optional_decimal(quote, "tradeTime")?);
    let observed_at = ExactDecimal::from_integer(i128::from(observed_at_ms));
    let quote_age_ms = match quote_time.as_ref() {
        Some(source_time) => Some(observed_at.checked_sub(source_time)?),
        None => None,
    };

    let contract_type = reference
        .get("contractType")
        .and_then(Value::as_str)
        .and_then(|value| match value {
            "C" | "CALL" => Some("CALL".to_owned()),
            "P" | "PUT" => Some("PUT".to_owned()),
            _ => None,
        })
        .or_else(|| parsed.as_ref().map(|parsed| parsed.contract_type.clone()));
    let expiration = expiration_from_reference(reference)
        .or_else(|| parsed.as_ref().map(|parsed| parsed.expiration.clone()));
    let strike = optional_decimal(reference, "strikePrice")?
        .or_else(|| parsed.as_ref().map(|parsed| parsed.strike.clone()));
    let underlying = reference
        .get("underlying")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| parsed.as_ref().map(|parsed| parsed.underlying.clone()));

    Ok(NormalizedOptionQuote {
        symbol: item_symbol.to_owned(),
        underlying,
        contract_type,
        expiration,
        strike,
        realtime: item.get("realtime").and_then(Value::as_bool),
        quote_type: item
            .get("quoteType")
            .and_then(Value::as_str)
            .map(str::to_owned),
        bid,
        ask,
        bid_size: optional_decimal(quote, "bidSize")?,
        ask_size: optional_decimal(quote, "askSize")?,
        mark: optional_decimal(quote, "mark")?,
        last: optional_decimal(quote, "lastPrice")?,
        mid,
        spread,
        spread_percent_of_mid,
        quote_time,
        trade_time,
        quote_age_ms,
        delta: optional_decimal(quote, "delta")?,
        gamma: optional_decimal(quote, "gamma")?,
        theta: optional_decimal(quote, "theta")?,
        vega: optional_decimal(quote, "vega")?,
        rho: optional_decimal(quote, "rho")?,
        volatility: optional_decimal(quote, "volatility")?,
        open_interest: optional_decimal(quote, "openInterest")?,
        total_volume: optional_decimal(quote, "totalVolume")?,
        underlying_price: optional_decimal(quote, "underlyingPrice")?,
        theoretical_option_value: optional_decimal(quote, "theoreticalOptionValue")?,
        time_value: optional_decimal(quote, "timeValue")?,
        intrinsic_value: optional_decimal(quote, "moneyIntrinsicValue")?,
    })
}

struct ParsedOccSymbol {
    underlying: String,
    expiration: String,
    contract_type: String,
    strike: ExactDecimal,
}

fn parse_occ_symbol(symbol: &str) -> Option<ParsedOccSymbol> {
    let bytes = symbol.as_bytes();
    if bytes.len() != 21
        || !bytes[6..12].iter().all(u8::is_ascii_digit)
        || !matches!(bytes[12], b'C' | b'P')
        || !bytes[13..21].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let underlying = std::str::from_utf8(&bytes[..6]).ok()?.trim_end().to_owned();
    if underlying.is_empty() {
        return None;
    }
    let compact = std::str::from_utf8(&bytes[6..12]).ok()?;
    let year = 2_000 + compact[0..2].parse::<u32>().ok()?;
    let month = compact[2..4].parse::<u32>().ok()?;
    let day = compact[4..6].parse::<u32>().ok()?;
    if !valid_calendar_date(year, month, day) {
        return None;
    }
    let strike_raw = std::str::from_utf8(&bytes[13..21]).ok()?;
    let coefficient = strike_raw.parse::<i128>().ok()?;
    let strike = ExactDecimal {
        coefficient,
        scale: 3,
    }
    .normalized();
    Some(ParsedOccSymbol {
        underlying,
        expiration: format!("{year:04}-{month:02}-{day:02}"),
        contract_type: if bytes[12] == b'C' { "CALL" } else { "PUT" }.to_owned(),
        strike,
    })
}

#[cfg(test)]
pub(crate) fn parse_occ_symbol_fields_for_test(
    symbol: &str,
) -> Option<(String, String, String, String)> {
    parse_occ_symbol(symbol).map(|parsed| {
        (
            parsed.underlying,
            parsed.expiration,
            parsed.contract_type,
            parsed.strike.as_string(),
        )
    })
}

fn expiration_from_reference(reference: &Map<String, Value>) -> Option<String> {
    let year = reference.get("expirationYear")?.as_number()?.as_u64()?;
    let month = reference.get("expirationMonth")?.as_number()?.as_u64()?;
    let day = reference.get("expirationDay")?.as_number()?.as_u64()?;
    let (year, month, day) = (
        u32::try_from(year).ok()?,
        u32::try_from(month).ok()?,
        u32::try_from(day).ok()?,
    );
    if !valid_calendar_date(year, month, day) {
        return None;
    }
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn valid_calendar_date(year: u32, month: u32, day: u32) -> bool {
    if !(1..=12).contains(&month) || day == 0 {
        return false;
    }
    let leap_year =
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 if leap_year => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    day <= days
}

fn optional_decimal(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Option<ExactDecimal>, ReadResponseError> {
    match object.get(key) {
        None
        | Some(
            Value::Null | Value::Bool(_) | Value::String(_) | Value::Array(_) | Value::Object(_),
        ) => Ok(None),
        Some(Value::Number(number)) => ExactDecimal::from_number(number).map(Some),
    }
}
