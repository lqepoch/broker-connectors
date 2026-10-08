#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Read-only public facade over the bounded `schwab-rest` client.
//!
//! Every request requires an injected token provider, transport, and shared
//! read-admission port. Project-specific policy is supplied by an adapter.
//! This crate does not implement OAuth, transport,
//! parsing, retries, Streamer connectivity, or broker mutations; it re-exports
//! the existing fixed-origin, read-only `SchwabHttpsTransport`.
//! 将现有 Trader 与 Market Data GET 能力组合为只读 facade；不增加 OAuth、自动重试、authority 或订单写入能力。

/// Module exposing the market data API.
/// 提供 market data API 的模块。
pub mod market_data;
/// Module exposing the trader API.
/// 提供 trader API 的模块。
pub mod trader;

pub use market_data::MarketData;
pub use trader::Trader;

pub use schwab_rest::{
    AccessToken, AccessTokenProvider, AccountNumberHash, AccountResponse, AccountsQuery,
    BalanceSnapshot, BoxFuture, BrokerIdentifier, CsvValues, DecimalQuery, HttpMethod, HttpRequest,
    HttpResponse, HttpTransport, HttpTransportError, Instrument, InstrumentSearchQuery,
    MarketHoursQuery, MarketsQuery, MoversQuery, NormalizedOptionQuote,
    NormalizedOptionQuoteReadResponse, OptionChainQuery, OptionExpirationQuery, OrdersQuery,
    PathIdentifier, Position, PriceHistoryQuery, QueryExtensions, QueryText, QuotesQuery,
    ReadAdmissionError, ReadAdmissionPort, ReadApiError, ReadPriority, ReadRequestError,
    ReadResponseError, RestError, RestResponse, SchwabHttpsTransport, SecuritiesAccount,
    StreamerInfo, TokenProviderError, TraderReadResponse, TransactionsQuery, TypedReadResponse,
    UserPreferencesResponse, WireNumber,
};

use schwab_rest::SchwabRestClient;
use std::sync::Arc;

/// Builder for [`SchwabSdk`]. A gate must be supplied explicitly; there is
/// intentionally no unmetered constructor or default budget.
/// 中文摘要：要求显式注入 token、transport 和共享读取预算的 builder。
pub struct SchwabSdkBuilder<P, T> {
    token_provider: P,
    transport: T,
    read_admission: Arc<dyn ReadAdmissionPort>,
}

impl<P, T> SchwabSdkBuilder<P, T> {
    /// Creates a builder with caller-owned credentials, transport, and read-admission port.
    /// Clone and pass the same port to clients that share local policy.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    pub fn new(
        token_provider: P,
        transport: T,
        read_admission: Arc<dyn ReadAdmissionPort>,
    ) -> Self {
        Self {
            token_provider,
            transport,
            read_admission,
        }
    }
}

impl<P, T> SchwabSdkBuilder<P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Builds the read-only facade. Runtime or OAuth wiring is not implied.
    /// 中文摘要：构造只读 facade，不隐含 OAuth、runtime 或 broker authority。
    pub fn build(self) -> SchwabSdk<P, T> {
        SchwabSdk {
            rest: SchwabRestClient::new(self.token_provider, self.transport, self.read_admission),
        }
    }
}

/// Read-only Schwab SDK facade backed by the existing `schwab-rest` client.
///
/// The facade owns one client and returns lightweight borrowed Trader and
/// Market Data views. All endpoint validation, typed parsing, admission,
/// transport dispatch, response metadata, and error classification stay in
/// `schwab-rest`.
/// 中文摘要：只读公共 SDK facade；不增加 OAuth 或 broker mutation 能力。
pub struct SchwabSdk<P, T> {
    rest: SchwabRestClient<P, T>,
}

impl<P, T> SchwabSdk<P, T> {
    /// Starts construction with an explicit shared read-admission port.
    /// 中文摘要：用注入的 token 提供器、传输端口和共享读取准入端口开始构造只读 facade。
    pub fn builder(
        token_provider: P,
        transport: T,
        read_admission: Arc<dyn ReadAdmissionPort>,
    ) -> SchwabSdkBuilder<P, T> {
        SchwabSdkBuilder::new(token_provider, transport, read_admission)
    }
}

impl<P, T> SchwabSdk<P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Borrows the existing typed Trader GET facade.
    /// 中文摘要：借用 Trader 只读 GET facade。
    pub const fn trader(&self) -> Trader<'_, P, T> {
        Trader::new(&self.rest)
    }

    /// Borrows the existing typed and normalized Market Data GET facade.
    /// 中文摘要：借用 Market Data 只读 GET facade。
    pub const fn market_data(&self) -> MarketData<'_, P, T> {
        MarketData::new(&self.rest)
    }
}
