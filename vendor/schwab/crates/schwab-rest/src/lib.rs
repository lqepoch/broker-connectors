#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Read-only Schwab REST request contract and client core.
//!
//! The client uses injected token-provider and transport ports. Its HTTPS
//! adapter is read-only and the route table accepts only explicit GET methods.
//! 通过注入的 token 与传输端口提供有界只读 REST 路由和类型化响应；不获取凭证，也不包含 broker 写入路由。

mod client;
mod market_models;
mod normalized_option_quote_api;
mod query_encoding;
mod read_api;
mod read_response;
mod request_types;
mod response_models;
mod routes;
mod transport;

pub use client::{
    AccessToken, AccessTokenProvider, BoxFuture, HttpMethod, HttpRequest, HttpResponse,
    HttpTransport, HttpTransportError, MAX_HEADER_COUNT, MAX_HEADER_NAME_BYTES,
    MAX_HEADER_VALUE_BYTES, MAX_RESPONSE_BODY_BYTES, MAX_RESPONSE_HEADER_BYTES, ReadAdmissionError,
    ReadAdmissionPort, ReadEndpoint, ReadPriority, RedirectPolicy, RequestDispatchCertainty,
    ResponseLimitError, RestError, RestResponse, SCHWAB_API_ROOT, SchwabRestClient,
    TokenProviderError,
};
pub use market_models::{
    InstrumentDetail, InstrumentSummary, InstrumentsSearchResponse, MarketHoursProduct,
    MarketHoursResponse, MarketReadResponse, MarketSessionTime, MoverItem, MoversResponse,
    OptionChainResponse, OptionContract, OptionExpiration, OptionExpirationChainResponse,
    PriceHistoryCandle, PriceHistoryResponse, QuoteDetail, QuoteItem, QuoteReference,
    QuoteSeriesCandle, QuotesResponse, SingleQuoteResponse,
};
pub use normalized_option_quote_api::NormalizedOptionQuoteReadResponse;
pub use read_api::{ReadApiError, TypedReadResponse};
pub use read_response::{
    ExactDecimal, ExactRatio, MAX_ACCOUNT_NUMBER_HASH_ROWS, NormalizedOptionQuote,
    ParsedReadResponse, ReadResponseError, ReadResponseKind, VerticalQuoteLegs,
};
pub use response_models::{
    AccountNumberHash, AccountResponse, BalanceSnapshot, BrokerAccountNumber, ExecutionLeg,
    Instrument, OfferInfo, Order, OrderActivity, OrderLeg, Position, SecuritiesAccount,
    StreamerInfo, TraderReadResponse, Transaction, TransactionResponse, TransactionTransferItem,
    TransactionUser, UnknownFields, UserPreference, UserPreferenceAccount, UserPreferencesResponse,
    WireNumber,
};
pub use routes::{
    AccountsQuery, BrokerIdentifier, CsvValues, DecimalQuery, InstrumentSearchQuery,
    MarketHoursQuery, MarketsQuery, MoversQuery, OptionChainQuery, OptionExpirationQuery,
    OrdersQuery, PathIdentifier, PriceHistoryQuery, QueryExtensions, QueryText, QuotesQuery,
    ReadRequest, ReadRequestError, TransactionsQuery,
};
pub use transport::SchwabHttpsTransport;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod transport_tests;

#[cfg(test)]
mod read_api_tests;

#[cfg(test)]
mod read_response_tests;

// Order mutation contracts remain test-only. No Place/Replace/Cancel transport
// capability is compiled into the production REST crate.
#[cfg(test)]
mod mutation_contract;
