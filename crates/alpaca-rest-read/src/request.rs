use std::collections::BTreeSet;
use std::fmt;

use market_contracts::parse_occ_symbol_candidate;

use crate::AlpacaRestError;

/// Maximum symbols in one Alpaca options REST request.
pub const MAX_OPTIONS_SYMBOLS_PER_REQUEST: usize = 100;
/// Maximum datapoints requested from one options snapshot page.
pub const MAX_SNAPSHOT_PAGE_SIZE: u16 = 1_000;
/// Maximum pages read by one bounded snapshot-window operation.
pub const MAX_SNAPSHOT_PAGES_PER_WINDOW: u8 = 4;
/// Maximum provider cursor length accepted or returned by the adapter.
pub const MAX_PAGE_CURSOR_BYTES: usize = 512;

/// An explicit Alpaca options REST feed selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestedOptionsFeed {
    /// Request Alpaca's OPRA feed. This is a request value, not entitlement evidence.
    Opra,
    /// Request Alpaca's indicative options feed.
    Indicative,
}

impl RequestedOptionsFeed {
    /// Return the exact feed query parameter value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Opra => "opra",
            Self::Indicative => "indicative",
        }
    }

    pub(crate) const fn to_sdk(self) -> alpaca_data::options::OptionsFeed {
        match self {
            Self::Opra => alpaca_data::options::OptionsFeed::Opra,
            Self::Indicative => alpaca_data::options::OptionsFeed::Indicative,
        }
    }
}

/// Validated, bounded options symbols and one explicitly selected feed.
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaOptionsRequest {
    feed: RequestedOptionsFeed,
    symbols: Vec<String>,
}

impl AlpacaOptionsRequest {
    /// Validate and sort a non-empty request containing at most 100 OCC candidates.
    ///
    /// The symbols remain unqualified candidates; validation does not assert deliverable,
    /// multiplier, settlement, exercise style, or a broker instrument identity.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] for an empty request, invalid OCC candidate,
    /// duplicate symbol, lowercase symbol, or more than 100 symbols.
    pub fn new(
        feed: RequestedOptionsFeed,
        symbols: impl IntoIterator<Item = String>,
    ) -> Result<Self, AlpacaRestError> {
        let mut unique = BTreeSet::new();
        for symbol in symbols {
            if unique.len() == MAX_OPTIONS_SYMBOLS_PER_REQUEST
                || symbol != symbol.to_ascii_uppercase()
                || parse_occ_symbol_candidate(&symbol).is_err()
            {
                return Err(AlpacaRestError::InvalidRequest);
            }
            if !unique.insert(symbol) {
                return Err(AlpacaRestError::InvalidRequest);
            }
        }
        if unique.is_empty() {
            return Err(AlpacaRestError::InvalidRequest);
        }
        Ok(Self {
            feed,
            symbols: unique.into_iter().collect(),
        })
    }

    /// Return the explicitly requested feed.
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedOptionsFeed {
        self.feed
    }

    /// Return symbols in deterministic lexical order.
    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
}

impl fmt::Debug for AlpacaOptionsRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaOptionsRequest")
            .field("requested_feed", &self.feed)
            .field("symbol_count", &self.symbols.len())
            .field("symbols", &"[REDACTED]")
            .finish()
    }
}

/// Opaque Alpaca continuation token returned by a prior snapshot request.
#[derive(Clone, Eq, PartialEq)]
pub struct OptionsPageCursor(String);

impl OptionsPageCursor {
    pub(crate) fn from_provider(value: String) -> Result<Self, AlpacaRestError> {
        if value.is_empty()
            || value.len() > MAX_PAGE_CURSOR_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(AlpacaRestError::ProtocolViolation);
        }
        Ok(Self(value))
    }

    pub(crate) fn into_provider(self) -> String {
        self.0
    }
}

impl fmt::Debug for OptionsPageCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OptionsPageCursor([REDACTED])")
    }
}

/// A finite snapshot read that performs at most four HTTP pages.
#[derive(Clone, Eq, PartialEq)]
pub struct OptionsSnapshotWindowRequest {
    base: AlpacaOptionsRequest,
    page_size: u16,
    page_limit: u8,
    cursor: Option<OptionsPageCursor>,
}

impl OptionsSnapshotWindowRequest {
    /// Create a bounded snapshot request with a page size from 1 through 1,000.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] when the page size or page limit is outside the
    /// documented bounds.
    pub fn new(
        base: AlpacaOptionsRequest,
        page_size: u16,
        page_limit: u8,
    ) -> Result<Self, AlpacaRestError> {
        if page_size == 0
            || page_size > MAX_SNAPSHOT_PAGE_SIZE
            || page_limit == 0
            || page_limit > MAX_SNAPSHOT_PAGES_PER_WINDOW
        {
            return Err(AlpacaRestError::InvalidRequest);
        }
        Ok(Self {
            base,
            page_size,
            page_limit,
            cursor: None,
        })
    }

    /// Continue a prior finite window using its opaque provider cursor.
    #[must_use]
    pub fn with_cursor(mut self, cursor: OptionsPageCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub(crate) const fn base(&self) -> &AlpacaOptionsRequest {
        &self.base
    }

    pub(crate) const fn page_size(&self) -> u16 {
        self.page_size
    }

    pub(crate) const fn page_limit(&self) -> u8 {
        self.page_limit
    }

    pub(crate) fn take_cursor(&mut self) -> Option<OptionsPageCursor> {
        self.cursor.take()
    }
}

impl fmt::Debug for OptionsSnapshotWindowRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptionsSnapshotWindowRequest")
            .field("request", &self.base)
            .field("page_size", &self.page_size)
            .field("page_limit", &self.page_limit)
            .field("has_cursor", &self.cursor.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AlpacaOptionsRequest, MAX_OPTIONS_SYMBOLS_PER_REQUEST, MAX_SNAPSHOT_PAGE_SIZE,
        MAX_SNAPSHOT_PAGES_PER_WINDOW, OptionsSnapshotWindowRequest, RequestedOptionsFeed,
    };
    use crate::AlpacaRestError;

    const CALL: &str = "QQQ261218C00500000";

    #[test]
    fn request_requires_valid_unique_symbols_and_explicit_feed() {
        let request = AlpacaOptionsRequest::new(
            RequestedOptionsFeed::Opra,
            ["QQQ261218P00500000".to_owned(), CALL.to_owned()],
        )
        .expect("synthetic OCC candidates are valid");

        assert_eq!(request.requested_feed(), RequestedOptionsFeed::Opra);
        assert_eq!(request.symbols(), [CALL, "QQQ261218P00500000"]);
        assert_eq!(
            AlpacaOptionsRequest::new(RequestedOptionsFeed::Indicative, []),
            Err(AlpacaRestError::InvalidRequest)
        );
        assert_eq!(
            AlpacaOptionsRequest::new(
                RequestedOptionsFeed::Opra,
                [CALL.to_owned(), CALL.to_owned()],
            ),
            Err(AlpacaRestError::InvalidRequest)
        );
        assert_eq!(
            AlpacaOptionsRequest::new(RequestedOptionsFeed::Opra, ["not-an-occ".to_owned()]),
            Err(AlpacaRestError::InvalidRequest)
        );
    }

    #[test]
    fn request_and_snapshot_window_limits_are_enforced() {
        let too_many = (0..=MAX_OPTIONS_SYMBOLS_PER_REQUEST)
            .map(|index| format!("QQQ261218C{:08}", index + 1));
        assert_eq!(
            AlpacaOptionsRequest::new(RequestedOptionsFeed::Opra, too_many),
            Err(AlpacaRestError::InvalidRequest)
        );

        let base = AlpacaOptionsRequest::new(RequestedOptionsFeed::Opra, [CALL.to_owned()])
            .expect("synthetic request is valid");
        assert_eq!(
            OptionsSnapshotWindowRequest::new(base.clone(), 0, 1),
            Err(AlpacaRestError::InvalidRequest)
        );
        assert_eq!(
            OptionsSnapshotWindowRequest::new(base.clone(), MAX_SNAPSHOT_PAGE_SIZE + 1, 1),
            Err(AlpacaRestError::InvalidRequest)
        );
        assert_eq!(
            OptionsSnapshotWindowRequest::new(base.clone(), 1, 0),
            Err(AlpacaRestError::InvalidRequest)
        );
        assert_eq!(
            OptionsSnapshotWindowRequest::new(base, 1, MAX_SNAPSHOT_PAGES_PER_WINDOW + 1),
            Err(AlpacaRestError::InvalidRequest)
        );
    }

    #[test]
    fn request_debug_redacts_symbol_and_cursor_values() {
        let request = AlpacaOptionsRequest::new(RequestedOptionsFeed::Opra, [CALL.to_owned()])
            .expect("synthetic request is valid");
        assert!(!format!("{request:?}").contains(CALL));
    }
}
