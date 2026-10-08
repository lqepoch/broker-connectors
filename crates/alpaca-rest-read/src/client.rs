use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::time::Duration;

use alpaca_data::options::{
    LatestQuotesRequest, LatestTradesRequest, Quote, Snapshot, SnapshotsRequest, Trade,
};
use alpaca_http::RetryConfig;
use chrono::{SecondsFormat, Utc};
use market_contracts::{DecimalString, MarketEventV1, UtcTimestamp};
use zeroize::Zeroizing;

use crate::{
    AlpacaOptionsObservation, AlpacaOptionsRequest, AlpacaRestError, OptionsPageCursor,
    OptionsSnapshotWindow, OptionsSnapshotWindowRequest, RequestedOptionsFeed,
};

/// Per-request HTTP deadline.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Complete deadline for an operation that may fetch several snapshot pages.
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(20);
/// Maximum configurable HTTP deadline for one SDK request.
pub const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// Maximum configurable deadline for a finite snapshot window.
pub const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Maximum total wait budget spent between retries.
pub const MAX_RETRY_BUDGET: Duration = Duration::from_secs(2);
const DEFAULT_RETRY_BUDGET: Duration = Duration::from_millis(500);
const MAX_IN_FLIGHT_REQUESTS: usize = 2;
const MAX_CREDENTIAL_FIELD_BYTES: usize = 1_024;
const DATA_API_ORIGIN: &str = "https://data.alpaca.markets";

/// Bounds for one instance of the Alpaca REST read adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaRestLimits {
    request_timeout: Duration,
    operation_timeout: Duration,
    retry_budget: Duration,
}

impl Default for AlpacaRestLimits {
    fn default() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            retry_budget: DEFAULT_RETRY_BUDGET,
        }
    }
}

impl AlpacaRestLimits {
    /// Construct limits within the adapter's hard upper bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidConfiguration`] when a deadline is zero, exceeds its
    /// hard maximum, or the operation deadline is shorter than the request deadline.
    pub fn new(
        request_timeout: Duration,
        operation_timeout: Duration,
        retry_budget: Duration,
    ) -> Result<Self, AlpacaRestError> {
        if request_timeout.is_zero()
            || request_timeout > MAX_REQUEST_TIMEOUT
            || operation_timeout < request_timeout
            || operation_timeout > MAX_OPERATION_TIMEOUT
            || retry_budget > MAX_RETRY_BUDGET
        {
            return Err(AlpacaRestError::InvalidConfiguration);
        }
        Ok(Self {
            request_timeout,
            operation_timeout,
            retry_budget,
        })
    }

    /// Return the request-level deadline.
    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Return the overall operation deadline.
    #[must_use]
    pub const fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    /// Return the total retry-wait budget.
    #[must_use]
    pub const fn retry_budget(self) -> Duration {
        self.retry_budget
    }
}

/// Explicitly injected credentials for one Alpaca REST client.
pub struct AlpacaRestCredentials {
    key_id: Zeroizing<String>,
    secret: Zeroizing<String>,
}

impl AlpacaRestCredentials {
    /// Validate and own a synthetic or operator-injected credential pair in zeroizing buffers.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidConfiguration`] for empty, oversized, or non-printable
    /// credential values.
    pub fn new(
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, AlpacaRestError> {
        let key_id = Zeroizing::new(key_id.into());
        let secret = Zeroizing::new(secret.into());
        if !valid_credential(&key_id) || !valid_credential(&secret) {
            return Err(AlpacaRestError::InvalidConfiguration);
        }
        Ok(Self { key_id, secret })
    }

    fn into_parts(self) -> (Zeroizing<String>, Zeroizing<String>) {
        (self.key_id, self.secret)
    }
}

impl fmt::Debug for AlpacaRestCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AlpacaRestCredentials([REDACTED])")
    }
}

fn valid_credential(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CREDENTIAL_FIELD_BYTES
        && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

/// Read-only wrapper over the pinned community Alpaca Rust SDK.
///
/// It does not read environment variables, expose upstream SDK types, connect until a read method
/// is explicitly called, or provide any trading API. The production HTTPS market-data origin is
/// fixed inside the vendored SDK patch.
pub struct AlpacaRestReadClient {
    sdk: alpaca_data::Client,
    limits: AlpacaRestLimits,
}

impl AlpacaRestReadClient {
    /// Build a client with explicitly injected credentials and finite request/retry budgets.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidConfiguration`] if credential values, SDK configuration,
    /// or the fixed market-data origin are invalid.
    pub fn new(
        credentials: AlpacaRestCredentials,
        limits: AlpacaRestLimits,
    ) -> Result<Self, AlpacaRestError> {
        let (key_id, secret) = credentials.into_parts();
        let retry = RetryConfig::default()
            .with_max_retries(1)
            .with_retry_on_429(true)
            .with_respect_retry_after(true)
            .with_total_retry_budget(limits.retry_budget());
        let builder = alpaca_data::Client::builder()
            .credentials_zeroizing(key_id, secret)
            .base_url_str(DATA_API_ORIGIN)
            .map_err(|_| AlpacaRestError::InvalidConfiguration)?
            .timeout(limits.request_timeout())
            .max_in_flight(MAX_IN_FLIGHT_REQUESTS)
            .retry_config(retry);
        let sdk = builder.build().map_err(map_sdk_error)?;
        Ok(Self { sdk, limits })
    }

    /// Read latest option quotes using the request's explicit OPRA or indicative feed.
    ///
    /// # Errors
    ///
    /// Returns a fixed [`AlpacaRestError`] category when the provider rejects the read, transport
    /// fails, the operation times out, or its response violates bounds or the shared contract.
    pub async fn latest_option_quotes(
        &self,
        request: &AlpacaOptionsRequest,
    ) -> Result<Vec<AlpacaOptionsObservation>, AlpacaRestError> {
        let operation = async {
            let response = self
                .sdk
                .options()
                .latest_quotes(LatestQuotesRequest {
                    symbols: request.symbols().to_vec(),
                    feed: Some(request.requested_feed().to_sdk()),
                })
                .await
                .map_err(map_sdk_error)?;
            let received_at = now_utc()?;
            map_latest_quotes(request, response.quotes, &received_at)
        };
        tokio::time::timeout(self.limits.operation_timeout(), operation)
            .await
            .map_err(|_| AlpacaRestError::Timeout)?
    }

    /// Read latest option trades using the request's explicit OPRA or indicative feed.
    ///
    /// # Errors
    ///
    /// Returns a fixed [`AlpacaRestError`] category when the provider rejects the read, transport
    /// fails, the operation times out, or its response violates bounds or the shared contract.
    pub async fn latest_option_trades(
        &self,
        request: &AlpacaOptionsRequest,
    ) -> Result<Vec<AlpacaOptionsObservation>, AlpacaRestError> {
        let operation = async {
            let response = self
                .sdk
                .options()
                .latest_trades(LatestTradesRequest {
                    symbols: request.symbols().to_vec(),
                    feed: Some(request.requested_feed().to_sdk()),
                })
                .await
                .map_err(map_sdk_error)?;
            let received_at = now_utc()?;
            map_latest_trades(request, response.trades, &received_at)
        };
        tokio::time::timeout(self.limits.operation_timeout(), operation)
            .await
            .map_err(|_| AlpacaRestError::Timeout)?
    }

    /// Read at most the requested number of explicit-feed option snapshot pages.
    ///
    /// The operation is finite, rejects cursor cycles and duplicate or unrequested symbols, and
    /// returns an opaque continuation cursor when more pages remain. It intentionally projects
    /// only the shared quote/trade payloads; bars and float-valued Greeks are not exposed here.
    ///
    /// # Errors
    ///
    /// Returns a fixed [`AlpacaRestError`] category when the provider rejects a page, transport
    /// fails, the operation times out, or a page violates bounds or the shared contract.
    pub async fn snapshot_window(
        &self,
        mut request: OptionsSnapshotWindowRequest,
    ) -> Result<OptionsSnapshotWindow, AlpacaRestError> {
        let operation = async {
            let mut next_token = request.take_cursor().map(OptionsPageCursor::into_provider);
            let requested_symbols = request
                .base()
                .symbols()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut seen_symbols = BTreeSet::new();
            let mut seen_cursors = HashSet::new();
            if let Some(token) = &next_token
                && !seen_cursors.insert(token.clone())
            {
                return Err(AlpacaRestError::InvalidRequest);
            }
            let mut observations = Vec::with_capacity(
                request.base().symbols().len() * 2 * usize::from(request.page_limit()),
            );
            let mut pages_read = 0;
            let mut next_cursor = None;

            for _ in 0..request.page_limit() {
                let response = self
                    .sdk
                    .options()
                    .snapshots(SnapshotsRequest {
                        symbols: request.base().symbols().to_vec(),
                        feed: Some(request.base().requested_feed().to_sdk()),
                        limit: Some(u32::from(request.page_size())),
                        page_token: next_token.take(),
                    })
                    .await
                    .map_err(map_sdk_error)?;
                pages_read += 1;
                let received_at = now_utc()?;
                let mut page_snapshots = response.snapshots.into_iter().collect::<Vec<_>>();
                page_snapshots.sort_by(|left, right| left.0.cmp(&right.0));
                if page_snapshots.len() > request.base().symbols().len() {
                    return Err(AlpacaRestError::ProtocolViolation);
                }

                for (symbol, snapshot) in page_snapshots {
                    if !requested_symbols.contains(symbol.as_str())
                        || !seen_symbols.insert(symbol.clone())
                    {
                        return Err(AlpacaRestError::ProtocolViolation);
                    }
                    append_snapshot_observations(
                        request.base().requested_feed(),
                        &symbol,
                        snapshot,
                        &received_at,
                        &mut observations,
                    )?;
                }

                let Some(token) = response.next_page_token else {
                    next_cursor = None;
                    break;
                };
                let cursor = OptionsPageCursor::from_provider(token)?;
                let token = cursor.clone().into_provider();
                if !seen_cursors.insert(token.clone()) {
                    return Err(AlpacaRestError::ProtocolViolation);
                }
                next_token = Some(token);
                next_cursor = Some(cursor);
            }

            Ok(OptionsSnapshotWindow::new(
                observations,
                pages_read,
                next_cursor,
            ))
        };
        tokio::time::timeout(self.limits.operation_timeout(), operation)
            .await
            .map_err(|_| AlpacaRestError::Timeout)?
    }
}

impl fmt::Debug for AlpacaRestReadClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaRestReadClient")
            .field("origin", &DATA_API_ORIGIN)
            .field("limits", &self.limits)
            .field("credentials", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

fn map_latest_quotes(
    request: &AlpacaOptionsRequest,
    quotes: std::collections::HashMap<String, Quote>,
    received_at: &UtcTimestamp,
) -> Result<Vec<AlpacaOptionsObservation>, AlpacaRestError> {
    if quotes.len() > request.symbols().len() {
        return Err(AlpacaRestError::ProtocolViolation);
    }
    let requested = request
        .symbols()
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut quotes = quotes.into_iter().collect::<Vec<_>>();
    quotes.sort_by(|left, right| left.0.cmp(&right.0));
    quotes
        .into_iter()
        .map(|(symbol, quote)| {
            if !requested.contains(symbol.as_str()) {
                return Err(AlpacaRestError::ProtocolViolation);
            }
            map_quote(request.requested_feed(), &symbol, &quote, received_at)
        })
        .collect()
}

fn map_latest_trades(
    request: &AlpacaOptionsRequest,
    trades: std::collections::HashMap<String, Trade>,
    received_at: &UtcTimestamp,
) -> Result<Vec<AlpacaOptionsObservation>, AlpacaRestError> {
    if trades.len() > request.symbols().len() {
        return Err(AlpacaRestError::ProtocolViolation);
    }
    let requested = request
        .symbols()
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut trades = trades.into_iter().collect::<Vec<_>>();
    trades.sort_by(|left, right| left.0.cmp(&right.0));
    trades
        .into_iter()
        .map(|(symbol, trade)| {
            if !requested.contains(symbol.as_str()) {
                return Err(AlpacaRestError::ProtocolViolation);
            }
            map_trade(request.requested_feed(), &symbol, &trade, received_at)
        })
        .collect()
}

fn append_snapshot_observations(
    requested_feed: RequestedOptionsFeed,
    symbol: &str,
    snapshot: Snapshot,
    received_at: &UtcTimestamp,
    observations: &mut Vec<AlpacaOptionsObservation>,
) -> Result<(), AlpacaRestError> {
    if let Some(quote) = snapshot.latest_quote {
        observations.push(map_quote(requested_feed, symbol, &quote, received_at)?);
    }
    if let Some(trade) = snapshot.latest_trade {
        observations.push(map_trade(requested_feed, symbol, &trade, received_at)?);
    }
    Ok(())
}

fn map_quote(
    requested_feed: RequestedOptionsFeed,
    symbol: &str,
    quote: &Quote,
    received_at: &UtcTimestamp,
) -> Result<AlpacaOptionsObservation, AlpacaRestError> {
    let provider_timestamp = parse_timestamp(quote.t.as_deref())?;
    let event = MarketEventV1::OptionQuote {
        symbol: symbol.to_owned(),
        bid: quote.bp.map(decimal_string).transpose()?,
        ask: quote.ap.map(decimal_string).transpose()?,
        bid_size: quote.bs.map(integer_string).transpose()?,
        ask_size: quote.r#as.map(integer_string).transpose()?,
    };
    AlpacaOptionsObservation::new(
        requested_feed,
        provider_timestamp,
        received_at.clone(),
        event,
    )
    .map_err(|_| AlpacaRestError::ProtocolViolation)
}

fn map_trade(
    requested_feed: RequestedOptionsFeed,
    symbol: &str,
    trade: &Trade,
    received_at: &UtcTimestamp,
) -> Result<AlpacaOptionsObservation, AlpacaRestError> {
    let provider_timestamp = parse_timestamp(trade.t.as_deref())?;
    let price = trade
        .p
        .map(decimal_string)
        .transpose()?
        .ok_or(AlpacaRestError::ProtocolViolation)?;
    let size = trade
        .s
        .map(integer_string)
        .transpose()?
        .ok_or(AlpacaRestError::ProtocolViolation)?;
    let event = MarketEventV1::OptionTrade {
        symbol: symbol.to_owned(),
        price,
        size,
    };
    AlpacaOptionsObservation::new(
        requested_feed,
        provider_timestamp,
        received_at.clone(),
        event,
    )
    .map_err(|_| AlpacaRestError::ProtocolViolation)
}

fn decimal_string(value: impl fmt::Display) -> Result<DecimalString, AlpacaRestError> {
    DecimalString::new(value.to_string()).map_err(|_| AlpacaRestError::ProtocolViolation)
}

fn integer_string(value: u64) -> Result<DecimalString, AlpacaRestError> {
    DecimalString::new(value.to_string()).map_err(|_| AlpacaRestError::ProtocolViolation)
}

fn parse_timestamp(value: Option<&str>) -> Result<UtcTimestamp, AlpacaRestError> {
    value
        .and_then(|value| UtcTimestamp::parse(value).ok())
        .ok_or(AlpacaRestError::ProtocolViolation)
}

fn now_utc() -> Result<UtcTimestamp, AlpacaRestError> {
    let value = Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true);
    UtcTimestamp::parse(&value).map_err(|_| AlpacaRestError::ProtocolViolation)
}

fn map_sdk_error(error: alpaca_data::Error) -> AlpacaRestError {
    use alpaca_data::Error as DataError;
    use alpaca_http::Error as HttpError;

    match error {
        DataError::MissingCredentials | DataError::InvalidConfiguration(_) => {
            AlpacaRestError::InvalidConfiguration
        }
        DataError::InvalidRequest(_) => AlpacaRestError::InvalidRequest,
        DataError::Http(HttpError::RateLimited(_)) => AlpacaRestError::RateLimited,
        DataError::Http(HttpError::ResponseBodyTooLarge(_)) => AlpacaRestError::ResponseTooLarge,
        DataError::Http(HttpError::InvalidResponseEncoding(_) | HttpError::Deserialize { .. }) => {
            AlpacaRestError::ProtocolViolation
        }
        DataError::Http(HttpError::HttpStatus(meta)) if (400..500).contains(&meta.status()) => {
            AlpacaRestError::ProviderRejected
        }
        DataError::Http(_) => AlpacaRestError::Transport,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alpaca_data::options::{Quote, Trade};
    use chrono::{SecondsFormat, Utc};
    use market_contracts::{EntitlementState, MarketEventV1, NumericEncodingV1, UtcTimestamp};

    use super::{
        AlpacaRestCredentials, AlpacaRestLimits, MAX_OPERATION_TIMEOUT, MAX_REQUEST_TIMEOUT,
        MAX_RETRY_BUDGET, map_quote, map_trade,
    };
    use crate::{AlpacaRestError, RequestedOptionsFeed};

    fn timestamp() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }

    fn utc_now() -> UtcTimestamp {
        UtcTimestamp::parse(&timestamp()).expect("system UTC timestamp is valid")
    }

    #[test]
    fn credential_debug_is_redacted_and_validation_is_bounded() {
        let credentials = AlpacaRestCredentials::new("synthetic-test-key", "synthetic-test-secret")
            .expect("synthetic credentials are valid");
        let debug = format!("{credentials:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("synthetic-test-key"));
        assert!(!debug.contains("synthetic-test-secret"));
        assert!(matches!(
            AlpacaRestCredentials::new("", "synthetic-test-secret"),
            Err(AlpacaRestError::InvalidConfiguration)
        ));
        assert!(matches!(
            AlpacaRestCredentials::new("bad\nkey", "synthetic-test-secret"),
            Err(AlpacaRestError::InvalidConfiguration)
        ));
    }

    #[test]
    fn client_build_is_offline_and_debug_redacts_injected_credentials() {
        let client = super::AlpacaRestReadClient::new(
            AlpacaRestCredentials::new("synthetic-client-key", "synthetic-client-secret")
                .expect("synthetic credentials are valid"),
            AlpacaRestLimits::default(),
        )
        .expect("fixed-origin SDK client builds without a provider call");

        let debug = format!("{client:?}");
        assert!(debug.contains("https://data.alpaca.markets"));
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("synthetic-client-key"));
        assert!(!debug.contains("synthetic-client-secret"));
    }

    #[test]
    fn client_limits_have_fixed_upper_bounds_and_ordered_deadlines() {
        assert!(
            AlpacaRestLimits::new(Duration::ZERO, Duration::from_secs(1), Duration::ZERO).is_err()
        );
        assert!(
            AlpacaRestLimits::new(
                MAX_REQUEST_TIMEOUT + Duration::from_millis(1),
                MAX_OPERATION_TIMEOUT,
                Duration::ZERO
            )
            .is_err()
        );
        assert!(
            AlpacaRestLimits::new(
                Duration::from_secs(10),
                Duration::from_secs(9),
                Duration::ZERO
            )
            .is_err()
        );
        assert!(
            AlpacaRestLimits::new(
                Duration::from_secs(1),
                Duration::from_secs(2),
                MAX_RETRY_BUDGET + Duration::from_millis(1)
            )
            .is_err()
        );
    }

    #[test]
    fn sdk_quotes_map_to_shared_sparse_market_contract_with_unknown_feed_evidence() {
        let observation = map_quote(
            RequestedOptionsFeed::Opra,
            "QQQ261218C00500000",
            &Quote {
                t: Some(timestamp()),
                bx: Some("X".to_owned()),
                bp: Some("1.2300".parse().expect("valid synthetic decimal")),
                bs: Some(2),
                ax: Some("Y".to_owned()),
                ap: Some("1.2500".parse().expect("valid synthetic decimal")),
                r#as: Some(3),
                c: None,
            },
            &utc_now(),
        )
        .expect("synthetic quote maps");

        assert_eq!(observation.requested_feed(), RequestedOptionsFeed::Opra);
        assert_eq!(observation.source().provider, "alpaca");
        assert_eq!(observation.source().feed, "unknown");
        assert_eq!(observation.source().entitlement, EntitlementState::Unknown);
        assert_eq!(
            observation.source().numeric_encoding,
            NumericEncodingV1::DecimalToken
        );
        assert!(matches!(
            observation.event(),
            MarketEventV1::OptionQuote { bid: Some(bid), ask: Some(ask), .. }
                if bid.as_str() == "1.2300" && ask.as_str() == "1.2500"
        ));
    }

    #[test]
    fn sdk_wire_decimal_numbers_and_strings_are_exact_or_rejected() {
        use alpaca_data::options::Quote;

        let exact_number: Quote = serde_json::from_str(r#"{"bp":0.1234567890123456789012345678}"#)
            .expect("representable synthetic JSON number preserves all decimal digits");
        assert_eq!(
            exact_number.bp.expect("bid price is present").to_string(),
            "0.1234567890123456789012345678"
        );

        let exact_string: Quote =
            serde_json::from_str(r#"{"bp":"0.1234567890123456789012345678"}"#)
                .expect("representable synthetic JSON string preserves all decimal digits");
        assert_eq!(
            exact_string.bp.expect("bid price is present").to_string(),
            "0.1234567890123456789012345678"
        );

        for raw in [
            r#"{"bp":0.12345678901234567890123456789}"#,
            r#"{"bp":"0.12345678901234567890123456789"}"#,
            r#"{"bp":1.2345e-28}"#,
            r#"{"bp":"1.2345e-28"}"#,
            r#"{"bp":79228162514264337593543950336}"#,
            r#"{"bp":"79228162514264337593543950336"}"#,
        ] {
            assert!(
                serde_json::from_str::<Quote>(raw).is_err(),
                "inexact or out-of-range synthetic quote was accepted: {raw}"
            );
        }

        for raw in [
            r#"{"bp":1.234567890123456789012345678e-1}"#,
            r#"{"bp":"1.234567890123456789012345678e-1"}"#,
        ] {
            let quote: Quote = serde_json::from_str(raw)
                .expect("exact representable synthetic scientific token is accepted");
            assert_eq!(
                quote.bp.expect("bid price is present").to_string(),
                "0.1234567890123456789012345678"
            );
        }
    }

    #[test]
    fn incomplete_or_invalid_sdk_values_fail_closed() {
        let no_timestamp = map_quote(
            RequestedOptionsFeed::Indicative,
            "QQQ261218C00500000",
            &Quote {
                bp: Some("1.0".parse().expect("valid synthetic decimal")),
                ..Quote::default()
            },
            &utc_now(),
        );
        assert_eq!(no_timestamp, Err(AlpacaRestError::ProtocolViolation));

        let no_price = map_trade(
            RequestedOptionsFeed::Indicative,
            "QQQ261218C00500000",
            &Trade {
                t: Some(timestamp()),
                s: Some(1),
                ..Trade::default()
            },
            &utc_now(),
        );
        assert_eq!(no_price, Err(AlpacaRestError::ProtocolViolation));

        let negative_trade = map_trade(
            RequestedOptionsFeed::Indicative,
            "QQQ261218C00500000",
            &Trade {
                t: Some(timestamp()),
                p: Some("-1".parse().expect("valid synthetic decimal")),
                s: Some(1),
                ..Trade::default()
            },
            &utc_now(),
        );
        assert_eq!(negative_trade, Err(AlpacaRestError::ProtocolViolation));
    }
}
