use std::fmt;

/// Classified catalog-adapter failure without exposing SDK-owned errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IbkrCatalogError {
    /// The API endpoint is not a loopback address with a nonzero port.
    InvalidEndpoint,
    /// The exchange is empty, malformed, or the implicit SMART route.
    InvalidExchange,
    /// The currency is not a validated three-letter code.
    InvalidCurrency,
    /// A configured timeout is zero or exceeds the 60-second per-phase maximum.
    InvalidTimeout,
    /// The local connection attempt exceeded its configured deadline.
    ConnectTimeout,
    /// The SDK could not establish the requested local or remote API session.
    ConnectFailed,
    /// The catalog request exceeded its response deadline.
    RequestTimeout,
    /// The catalog result exceeded the adapter's hard row limit.
    TooManyRows,
    /// The provider returned no matching contract.
    NoMatch,
    /// The provider returned multiple records for an exact query.
    AmbiguousMatch,
    /// A provider row did not preserve the exact query identity or required provider id.
    InvalidProviderRow,
    /// The provider rejected the catalog request.
    ProviderRejected,
    /// The pinned SDK reported a request failure without safe terminal evidence.
    SdkRequestFailed,
    /// The client session could not confirm a terminal request state and is unusable.
    SessionPoisoned,
    /// The bounded SDK shutdown did not complete before its deadline.
    DisconnectTimeout,
}

impl fmt::Display for IbkrCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEndpoint => "IBKR_CATALOG_INVALID_ENDPOINT",
            Self::InvalidExchange => "IBKR_CATALOG_INVALID_EXCHANGE",
            Self::InvalidCurrency => "IBKR_CATALOG_INVALID_CURRENCY",
            Self::InvalidTimeout => "IBKR_CATALOG_INVALID_TIMEOUT",
            Self::ConnectTimeout => "IBKR_CATALOG_CONNECT_TIMEOUT",
            Self::ConnectFailed => "IBKR_CATALOG_CONNECT_FAILED",
            Self::RequestTimeout => "IBKR_CATALOG_REQUEST_TIMEOUT",
            Self::TooManyRows => "IBKR_CATALOG_TOO_MANY_ROWS",
            Self::NoMatch => "IBKR_CATALOG_NO_MATCH",
            Self::AmbiguousMatch => "IBKR_CATALOG_AMBIGUOUS_MATCH",
            Self::InvalidProviderRow => "IBKR_CATALOG_INVALID_PROVIDER_ROW",
            Self::ProviderRejected => "IBKR_CATALOG_PROVIDER_REJECTED",
            Self::SdkRequestFailed => "IBKR_CATALOG_SDK_REQUEST_FAILED",
            Self::SessionPoisoned => "IBKR_CATALOG_SESSION_POISONED",
            Self::DisconnectTimeout => "IBKR_CATALOG_DISCONNECT_TIMEOUT",
        })
    }
}

impl std::error::Error for IbkrCatalogError {}
