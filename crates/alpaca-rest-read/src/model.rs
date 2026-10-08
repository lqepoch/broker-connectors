use std::fmt;

use market_contracts::{MarketDataSourceV1, MarketEventV1, UtcTimestamp};

use crate::{OptionsPageCursor, RequestedOptionsFeed};

/// One sparse options quote or trade observation using the shared market event payload.
///
/// `source.feed()` is `unknown` because the REST response does not independently attest the
/// effective feed. `requested_feed()` retains the query parameter without upgrading it to OPRA
/// entitlement. REST observations carry no provider sequence, stream generation, or completeness
/// watermark and must not be treated as a continuous event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaOptionsObservation {
    source: MarketDataSourceV1,
    requested_feed: RequestedOptionsFeed,
    provider_timestamp: UtcTimestamp,
    received_at: UtcTimestamp,
    event: MarketEventV1,
}

impl AlpacaOptionsObservation {
    pub(crate) fn new(
        requested_feed: RequestedOptionsFeed,
        provider_timestamp: UtcTimestamp,
        received_at: UtcTimestamp,
        event: MarketEventV1,
    ) -> Result<Self, market_contracts::MarketWireError> {
        event.validate()?;
        let source = MarketDataSourceV1::new(
            "alpaca",
            "unknown",
            market_contracts::EntitlementState::Unknown,
            market_contracts::NumericEncodingV1::DecimalToken,
            None,
        )?;
        Ok(Self {
            source,
            requested_feed,
            provider_timestamp,
            received_at,
            event,
        })
    }

    /// Return typed source evidence. Its effective feed and entitlement remain unknown.
    #[must_use]
    pub const fn source(&self) -> &MarketDataSourceV1 {
        &self.source
    }

    /// Return the exact feed query parameter used for this REST read.
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedOptionsFeed {
        self.requested_feed
    }

    /// Return the provider timestamp associated with this quote or trade.
    #[must_use]
    pub const fn provider_timestamp(&self) -> &UtcTimestamp {
        &self.provider_timestamp
    }

    /// Return when this adapter received the completed HTTP response.
    #[must_use]
    pub const fn received_at(&self) -> &UtcTimestamp {
        &self.received_at
    }

    /// Borrow the validated shared quote or trade payload.
    #[must_use]
    pub const fn event(&self) -> &MarketEventV1 {
        &self.event
    }
}

/// Results from one finite set of options snapshot pages.
#[derive(Clone, Eq, PartialEq)]
pub struct OptionsSnapshotWindow {
    observations: Vec<AlpacaOptionsObservation>,
    pages_read: u8,
    next_cursor: Option<OptionsPageCursor>,
}

impl OptionsSnapshotWindow {
    pub(crate) const fn new(
        observations: Vec<AlpacaOptionsObservation>,
        pages_read: u8,
        next_cursor: Option<OptionsPageCursor>,
    ) -> Self {
        Self {
            observations,
            pages_read,
            next_cursor,
        }
    }

    /// Borrow the normalized quote and trade observations returned by the requested pages.
    #[must_use]
    pub fn observations(&self) -> &[AlpacaOptionsObservation] {
        &self.observations
    }

    /// Return the number of provider pages read by this operation.
    #[must_use]
    pub const fn pages_read(&self) -> u8 {
        self.pages_read
    }

    /// Take the opaque cursor needed to request another finite window.
    #[must_use]
    pub fn take_next_cursor(&mut self) -> Option<OptionsPageCursor> {
        self.next_cursor.take()
    }
}

impl fmt::Debug for OptionsSnapshotWindow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptionsSnapshotWindow")
            .field("observation_count", &self.observations.len())
            .field("pages_read", &self.pages_read)
            .field("has_next_cursor", &self.next_cursor.is_some())
            .finish()
    }
}
