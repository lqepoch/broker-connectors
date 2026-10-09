use std::fmt;

/// Fixed, secret-free failure categories returned by the REST adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaRestError {
    /// Invalid local credentials or client limits.
    InvalidConfiguration,
    /// Invalid symbol, time range, page size, cursor, or duplicate request value.
    InvalidRequest,
    /// The upstream returned an HTTP rate-limit response.
    RateLimited,
    /// Alpaca rejected the request, key, or feed entitlement.
    ProviderRejected,
    /// The HTTP operation failed without a safe detailed classification.
    Transport,
    /// The response could not be decoded or violated the shared market contract.
    ProtocolViolation,
    /// The response exceeded the fixed HTTP-body byte cap.
    ResponseTooLarge,
    /// A request or complete bounded page window exceeded its deadline.
    Timeout,
}

impl fmt::Display for AlpacaRestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "ALPACA_REST_INVALID_CONFIGURATION",
            Self::InvalidRequest => "ALPACA_REST_INVALID_REQUEST",
            Self::RateLimited => "ALPACA_REST_RATE_LIMITED",
            Self::ProviderRejected => "ALPACA_REST_PROVIDER_REJECTED",
            Self::Transport => "ALPACA_REST_TRANSPORT_FAILURE",
            Self::ProtocolViolation => "ALPACA_REST_PROTOCOL_VIOLATION",
            Self::ResponseTooLarge => "ALPACA_REST_RESPONSE_TOO_LARGE",
            Self::Timeout => "ALPACA_REST_TIMEOUT",
        })
    }
}

impl std::error::Error for AlpacaRestError {}
