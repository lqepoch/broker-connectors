use std::fmt;

use alpaca_data::stocks::{
    Adjustment, BarsRequest, BarsResponse, Currency, DataFeed, Sort, TimeFrame,
};
use market_contracts::{
    DecimalString, EntitlementState, MarketDataSourceV1, NumericEncodingV1, UtcTimestamp,
};

use crate::{AlpacaRestError, MAX_PAGE_CURSOR_BYTES};

/// Maximum number of stock bars requested from one provider page.
pub const MAX_STOCK_BARS_PAGE_SIZE: u16 = 1_000;

/// Fixed feed intent for the stock-history facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestedStockBarsFeed {
    /// Request the SIP feed. This is request intent, not source or entitlement evidence.
    Sip,
}

impl RequestedStockBarsFeed {
    /// Return the exact provider query value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sip => "sip",
        }
    }
}

/// Supported Alpaca stock-bar intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StockBarsTimeframe {
    /// One-minute bars.
    Minute1,
    /// Five-minute bars.
    Minute5,
    /// Fifteen-minute bars.
    Minute15,
    /// One-hour bars.
    Hour1,
    /// One-day bars.
    Day1,
}

impl StockBarsTimeframe {
    /// Return the exact Alpaca timeframe value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minute1 => "1Min",
            Self::Minute5 => "5Min",
            Self::Minute15 => "15Min",
            Self::Hour1 => "1Hour",
            Self::Day1 => "1Day",
        }
    }

    fn to_sdk(self) -> TimeFrame {
        TimeFrame::from(self.as_str())
    }
}

/// Validated identity for one single-symbol SIP bars query.
#[derive(Clone, Eq, PartialEq)]
struct StockBarsQueryIdentity {
    symbol: String,
    timeframe: StockBarsTimeframe,
    start: UtcTimestamp,
    end: UtcTimestamp,
    limit: u16,
    feed: RequestedStockBarsFeed,
    adjustment: &'static str,
    sort: &'static str,
    currency: &'static str,
    asof: &'static str,
}

/// Validated single-symbol request for one page of SIP-intent historical stock bars.
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsRequest {
    identity: StockBarsQueryIdentity,
    cursor: Option<StockBarsCursor>,
}

impl AlpacaStockBarsRequest {
    /// Validate an uppercase stock symbol, a closed UTC time range, and a page size.
    ///
    /// The request always fixes feed to `sip`, adjustment to `raw`, sort to `asc`, currency to
    /// `USD`, and `asof` to `-`. A SIP request does not prove that Alpaca returned SIP data or
    /// that the account is entitled to it.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] for a non-canonical symbol, invalid UTC range,
    /// or page size outside 1 through 1,000.
    pub fn new(
        symbol: impl Into<String>,
        timeframe: StockBarsTimeframe,
        start: UtcTimestamp,
        end: UtcTimestamp,
        limit: u16,
    ) -> Result<Self, AlpacaRestError> {
        let symbol = symbol.into();
        if !valid_stock_symbol(&symbol)
            || start > end
            || limit == 0
            || limit > MAX_STOCK_BARS_PAGE_SIZE
        {
            return Err(AlpacaRestError::InvalidRequest);
        }
        Ok(Self {
            identity: StockBarsQueryIdentity {
                symbol,
                timeframe,
                start,
                end,
                limit,
                feed: RequestedStockBarsFeed::Sip,
                adjustment: "raw",
                sort: "asc",
                currency: "USD",
                asof: "-",
            },
            cursor: None,
        })
    }

    /// Continue exactly the query that produced `cursor`.
    ///
    /// A cursor cannot be reused with a different symbol, timeframe, time range, page size, or
    /// any fixed SIP query parameter.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] when the cursor belongs to another query.
    pub fn with_cursor(mut self, cursor: StockBarsCursor) -> Result<Self, AlpacaRestError> {
        if self.identity != cursor.identity {
            return Err(AlpacaRestError::InvalidRequest);
        }
        self.cursor = Some(cursor);
        Ok(self)
    }

    /// Return the canonical stock symbol.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.identity.symbol
    }

    /// Return the fixed SIP request intent.
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedStockBarsFeed {
        self.identity.feed
    }

    pub(crate) fn to_sdk_request(&self) -> BarsRequest {
        BarsRequest {
            symbols: vec![self.identity.symbol.clone()],
            timeframe: self.identity.timeframe.to_sdk(),
            start: Some(self.identity.start.as_str().to_owned()),
            end: Some(self.identity.end.as_str().to_owned()),
            limit: Some(u32::from(self.identity.limit)),
            adjustment: Some(Adjustment::from(self.identity.adjustment)),
            feed: Some(DataFeed::Sip),
            sort: Some(Sort::Asc),
            asof: Some(self.identity.asof.to_owned()),
            currency: Some(Currency::from(self.identity.currency)),
            page_token: self.cursor.as_ref().map(|cursor| cursor.token.clone()),
        }
    }

    fn previous_timestamp(&self) -> Option<&UtcTimestamp> {
        self.cursor
            .as_ref()
            .and_then(|cursor| cursor.last_bar_timestamp.as_ref())
    }
}

impl fmt::Debug for AlpacaStockBarsRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsRequest")
            .field("symbol", &self.identity.symbol)
            .field("timeframe", &self.identity.timeframe)
            .field("start", &self.identity.start)
            .field("end", &self.identity.end)
            .field("limit", &self.identity.limit)
            .field("requested_feed", &self.identity.feed)
            .field("has_cursor", &self.cursor.is_some())
            .finish()
    }
}

/// Opaque, redacted continuation for the next single page of the same SIP bars query.
#[derive(Clone, Eq, PartialEq)]
pub struct StockBarsCursor {
    identity: StockBarsQueryIdentity,
    token: String,
    last_bar_timestamp: Option<UtcTimestamp>,
}

impl StockBarsCursor {
    fn from_provider(
        identity: StockBarsQueryIdentity,
        token: String,
        last_bar_timestamp: Option<UtcTimestamp>,
        previous_token: Option<&str>,
    ) -> Result<Self, AlpacaRestError> {
        if token.is_empty()
            || token.len() > MAX_PAGE_CURSOR_BYTES
            || token.bytes().any(|byte| byte.is_ascii_control())
            || previous_token.is_some_and(|previous| previous == token)
        {
            return Err(AlpacaRestError::ProtocolViolation);
        }
        Ok(Self {
            identity,
            token,
            last_bar_timestamp,
        })
    }
}

impl fmt::Debug for StockBarsCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StockBarsCursor([REDACTED])")
    }
}

/// One exact-decimal historical bar with separate request intent and unknown source evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaStockBarObservation {
    symbol: String,
    timestamp: UtcTimestamp,
    open: DecimalString,
    high: DecimalString,
    low: DecimalString,
    close: DecimalString,
    volume: u64,
    trade_count: Option<u64>,
    volume_weighted_price: Option<DecimalString>,
    source: MarketDataSourceV1,
    requested_feed: RequestedStockBarsFeed,
}

impl AlpacaStockBarObservation {
    /// Return the canonical stock symbol.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Return the provider's UTC bar-start timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> &UtcTimestamp {
        &self.timestamp
    }

    /// Return the exact decimal open price.
    #[must_use]
    pub const fn open(&self) -> &DecimalString {
        &self.open
    }

    /// Return the exact decimal high price.
    #[must_use]
    pub const fn high(&self) -> &DecimalString {
        &self.high
    }

    /// Return the exact decimal low price.
    #[must_use]
    pub const fn low(&self) -> &DecimalString {
        &self.low
    }

    /// Return the exact decimal close price.
    #[must_use]
    pub const fn close(&self) -> &DecimalString {
        &self.close
    }

    /// Return the provider's non-negative integer volume.
    #[must_use]
    pub const fn volume(&self) -> u64 {
        self.volume
    }

    /// Return the optional provider trade count.
    #[must_use]
    pub const fn trade_count(&self) -> Option<u64> {
        self.trade_count
    }

    /// Return the optional exact-decimal volume-weighted price.
    #[must_use]
    pub const fn volume_weighted_price(&self) -> Option<&DecimalString> {
        self.volume_weighted_price.as_ref()
    }

    /// Return source evidence, whose effective feed and entitlement remain unknown.
    #[must_use]
    pub const fn source(&self) -> &MarketDataSourceV1 {
        &self.source
    }

    /// Return the fixed SIP request intent, not proof of the returned source.
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedStockBarsFeed {
        self.requested_feed
    }
}

/// The result from exactly one historical stock-bars page.
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsPage {
    bars: Vec<AlpacaStockBarObservation>,
    response_observed_at: UtcTimestamp,
    next_cursor: Option<StockBarsCursor>,
}

impl AlpacaStockBarsPage {
    pub(crate) fn from_provider(
        request: &AlpacaStockBarsRequest,
        response: BarsResponse,
        response_observed_at: UtcTimestamp,
    ) -> Result<Self, AlpacaRestError> {
        if response
            .currency
            .as_ref()
            .is_some_and(|currency| currency.as_str() != request.identity.currency)
            || response.bars.len() != 1
        {
            return Err(AlpacaRestError::ProtocolViolation);
        }

        let mut response_bars = response.bars;
        let sdk_bars = response_bars
            .remove(request.symbol())
            .ok_or(AlpacaRestError::ProtocolViolation)?;
        if !response_bars.is_empty() || sdk_bars.len() > usize::from(request.identity.limit) {
            return Err(AlpacaRestError::ProtocolViolation);
        }

        let mut bars = Vec::with_capacity(sdk_bars.len());
        let mut previous_timestamp = request.previous_timestamp().cloned();
        for bar in sdk_bars {
            let timestamp = parse_timestamp(bar.t.as_deref())?;
            if timestamp < request.identity.start
                || timestamp > request.identity.end
                || previous_timestamp
                    .as_ref()
                    .is_some_and(|previous| timestamp <= *previous)
            {
                return Err(AlpacaRestError::ProtocolViolation);
            }
            let open = bar.o.ok_or(AlpacaRestError::ProtocolViolation)?;
            let high = bar.h.ok_or(AlpacaRestError::ProtocolViolation)?;
            let low = bar.l.ok_or(AlpacaRestError::ProtocolViolation)?;
            let close = bar.c.ok_or(AlpacaRestError::ProtocolViolation)?;
            if high < open || high < low || high < close || low > open || low > close {
                return Err(AlpacaRestError::ProtocolViolation);
            }

            let source = MarketDataSourceV1::new(
                "alpaca",
                "unknown",
                EntitlementState::Unknown,
                NumericEncodingV1::DecimalToken,
                None,
            )
            .map_err(|_| AlpacaRestError::ProtocolViolation)?;
            bars.push(AlpacaStockBarObservation {
                symbol: request.identity.symbol.clone(),
                timestamp: timestamp.clone(),
                open: positive_decimal(open)?,
                high: positive_decimal(high)?,
                low: positive_decimal(low)?,
                close: positive_decimal(close)?,
                volume: bar.v.ok_or(AlpacaRestError::ProtocolViolation)?,
                trade_count: bar.n,
                volume_weighted_price: bar.vw.map(positive_decimal).transpose()?,
                source,
                requested_feed: request.requested_feed(),
            });
            previous_timestamp = Some(timestamp);
        }

        let last_timestamp = bars
            .last()
            .map(|bar| bar.timestamp.clone())
            .or_else(|| request.previous_timestamp().cloned());
        let previous_token = request.cursor.as_ref().map(|cursor| cursor.token.as_str());
        let next_cursor = response
            .next_page_token
            .map(|token| {
                StockBarsCursor::from_provider(
                    request.identity.clone(),
                    token,
                    last_timestamp,
                    previous_token,
                )
            })
            .transpose()?;

        Ok(Self {
            bars,
            response_observed_at,
            next_cursor,
        })
    }

    /// Borrow bars returned from this provider page.
    #[must_use]
    pub fn bars(&self) -> &[AlpacaStockBarObservation] {
        &self.bars
    }

    /// Return when the SDK returned the decoded HTTP response to the adapter.
    ///
    /// This local observation time is not the source timestamp or historical point-in-time
    /// availability.
    #[must_use]
    pub const fn response_observed_at(&self) -> &UtcTimestamp {
        &self.response_observed_at
    }

    /// Return whether Alpaca supplied a continuation token for another page.
    #[must_use]
    pub const fn has_next_page(&self) -> bool {
        self.next_cursor.is_some()
    }

    /// Take the opaque continuation token, if present.
    #[must_use]
    pub fn take_next_cursor(&mut self) -> Option<StockBarsCursor> {
        self.next_cursor.take()
    }
}

impl fmt::Debug for AlpacaStockBarsPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsPage")
            .field("bar_count", &self.bars.len())
            .field("response_observed_at", &self.response_observed_at)
            .field("has_next_page", &self.next_cursor.is_some())
            .finish()
    }
}

fn valid_stock_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= 32
        && alpaca_data::stocks::display_stock_symbol(symbol) == symbol
        && symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || b".-".contains(&byte))
}

fn parse_timestamp(value: Option<&str>) -> Result<UtcTimestamp, AlpacaRestError> {
    value
        .and_then(|value| UtcTimestamp::parse(value).ok())
        .ok_or(AlpacaRestError::ProtocolViolation)
}

fn positive_decimal(value: impl fmt::Display) -> Result<DecimalString, AlpacaRestError> {
    let value = value.to_string();
    if value.starts_with('-') || !value.bytes().any(|byte| (b'1'..=b'9').contains(&byte)) {
        return Err(AlpacaRestError::ProtocolViolation);
    }
    DecimalString::new(value).map_err(|_| AlpacaRestError::ProtocolViolation)
}

#[cfg(test)]
#[path = "stock_bars_tests.rs"]
mod tests;
