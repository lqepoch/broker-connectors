//! Typed convenience reads over the fixed read-only REST route set.
//! 基于固定只读 REST 路由提供类型化读取便捷接口。

use std::fmt;

use crate::client::{
    AccessTokenProvider, HttpTransport, ReadPriority, RequestDispatchCertainty, RestError,
    RestResponse, SchwabRestClient,
};
use crate::read_response::{ParsedReadResponse, ReadResponseError};
use crate::response_models::StreamerInfo;
use crate::routes::{
    AccountsQuery, BrokerIdentifier, CsvValues, InstrumentSearchQuery, MarketHoursQuery,
    MarketsQuery, MoversQuery, OptionChainQuery, OptionExpirationQuery, OrdersQuery,
    PathIdentifier, PriceHistoryQuery, QueryExtensions, QuotesQuery, ReadRequest, ReadRequestError,
    TransactionsQuery,
};

/// A successful HTTP response paired with the DTO projection selected by its
/// allow-listed endpoint. The raw response remains available for headers such
/// as `Retry-After` and rate-limit metadata.
/// 中文摘要：同时保留原始 REST 元数据和按端点校验后的模型；投影成功不授予账户 authority 或报价新鲜度。
pub struct TypedReadResponse {
    response: RestResponse,
    parsed: ParsedReadResponse,
}

impl TypedReadResponse {
    /// Returns the original bounded REST response carried by this value.
    /// 借用该类型化结果或错误所关联的原始有界 REST 响应。
    pub const fn response(&self) -> &RestResponse {
        &self.response
    }

    /// Returns the route-selected schema projection; it does not establish account authority or quote freshness.
    /// 借用按路由选择的 schema 投影；它不建立账户 authority 或报价 freshness。
    pub const fn parsed(&self) -> &ParsedReadResponse {
        &self.parsed
    }

    /// Consumes the wrapper and returns the original REST response, discarding its parsed projection.
    /// 消费包装值并取出原始 REST 响应，同时丢弃解析投影。
    pub fn into_response(self) -> RestResponse {
        self.response
    }
}

impl fmt::Debug for TypedReadResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedReadResponse")
            .field("route", &self.response.endpoint().route_name())
            .field("status", &self.response.status())
            .field("attempts", &self.response.attempts())
            .field("parsed", &self.parsed)
            .finish()
    }
}

/// Fixed errors for the typed read facade. HTTP status errors retain their
/// bounded raw response in `RestError`; schema errors retain their successful
/// response so callers can inspect headers and body without logging them.
/// 中文摘要：只读 REST 查询或响应投影的错误；错误不授予账户或订单 authority。
#[derive(Debug)]
pub enum ReadApiError {
    /// The read request could not be constructed or admitted.
    /// 无法构造或准入该只读请求。
    Request(ReadRequestError),
    /// The underlying REST transport returned a fixed failure category.
    /// 底层 REST 传输返回固定失败类别。
    Rest(RestError),
    /// The successful HTTP body failed bounded parsing or schema validation.
    /// 成功 HTTP 响应正文未通过有界解析或结构校验。
    Response {
        /// Successful HTTP response retained for schema inspection; its body must not be logged.
        /// HTTP 或协议响应值。
        response: RestResponse,
        /// Bounded parsing or schema error associated with the response.
        /// 字段或事件来源。
        source: ReadResponseError,
    },
}

impl ReadApiError {
    /// Reports whether the request could have reached Schwab. Response-schema
    /// failures retain a successful HTTP response and are therefore treated
    /// as sent.
    /// 中文摘要：报告本地是否能确定请求未发送。
    pub const fn request_dispatch_certainty(&self) -> RequestDispatchCertainty {
        match self {
            Self::Request(_) => RequestDispatchCertainty::DefinitelyNotSent,
            Self::Rest(error) => error.request_dispatch_certainty(),
            Self::Response { .. } => RequestDispatchCertainty::MayHaveBeenSent,
        }
    }

    /// Returns a stable machine-readable failure code without response values.
    /// 返回稳定的机器可读错误代码，不包含响应字段值。
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Request(error) => RestError::Request(*error).code(),
            Self::Rest(error) => error.code(),
            Self::Response { source, .. } => source.code(),
        }
    }

    /// Returns the original bounded REST response carried by this value.
    /// 借用该类型化结果或错误所关联的原始有界 REST 响应。
    pub fn response(&self) -> Option<&RestResponse> {
        match self {
            Self::Request(_) => None,
            Self::Rest(error) => error.response(),
            Self::Response { response, .. } => Some(response),
        }
    }

    /// Returns the fixed parser/schema error only for a response-projection failure.
    /// 仅在响应投影失败时返回固定解析/schema 错误分类。
    pub const fn response_error(&self) -> Option<ReadResponseError> {
        match self {
            Self::Response { source, .. } => Some(*source),
            _ => None,
        }
    }
}

impl fmt::Display for ReadApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ReadApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            Self::Rest(error) => Some(error),
            Self::Response { source, .. } => Some(source),
        }
    }
}

impl From<ReadRequestError> for ReadApiError {
    fn from(error: ReadRequestError) -> Self {
        Self::Request(error)
    }
}

impl<P, T> SchwabRestClient<P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Sends one GET for an allow-listed Trader or Market Data read request.
    /// The response remains bounded raw bytes; callers that need schema
    /// validation can use `read_typed`. Account, order, and transaction
    /// authority reads use urgent local admission by default; other generic
    /// reads use refresh priority.
    /// 中文摘要：按请求类别默认的本地准入优先级发送一次 allowlist GET；不重试、不写入。
    pub async fn read(&self, request: ReadRequest) -> Result<RestResponse, RestError> {
        let priority = default_read_priority(&request);
        self.read_with_priority(request, priority).await
    }

    /// Sends one allow-listed GET using the explicit application priority.
    /// Priority changes local headroom only; it is not a provider quota.
    /// 中文摘要：按指定本地优先级发送一次 allowlist GET；优先级只影响本地准入，不代表 Schwab 配额。
    pub async fn read_with_priority(
        &self,
        request: ReadRequest,
        priority: ReadPriority,
    ) -> Result<RestResponse, RestError> {
        let endpoint = request.endpoint().map_err(RestError::Request)?;
        self.get_endpoint(endpoint, priority).await
    }

    /// Sends one allow-listed GET and validates its response using the schema
    /// family attached to that request route. Account, order, and transaction
    /// authority reads use urgent local admission by default; other generic
    /// reads use refresh priority.
    /// 中文摘要：发送一次 allowlist GET，并按该路由绑定的 schema 校验正文。
    pub async fn read_typed(
        &self,
        request: ReadRequest,
    ) -> Result<TypedReadResponse, ReadApiError> {
        let priority = default_read_priority(&request);
        self.read_typed_with_priority(request, priority).await
    }

    /// Typed counterpart to [`Self::read_with_priority`].
    /// 中文摘要：按指定本地优先级执行类型化读取；投影失败时仍保留有界原始响应。
    pub async fn read_typed_with_priority(
        &self,
        request: ReadRequest,
        priority: ReadPriority,
    ) -> Result<TypedReadResponse, ReadApiError> {
        let response =
            self.read_with_priority(request, priority)
                .await
                .map_err(|error| match error {
                    RestError::Request(request) => ReadApiError::Request(request),
                    error => ReadApiError::Rest(error),
                })?;
        let parsed = match ParsedReadResponse::from_endpoint_response(&response) {
            Ok(parsed) => parsed,
            Err(source) => return Err(ReadApiError::Response { response, source }),
        };
        Ok(TypedReadResponse { response, parsed })
    }

    /// Typed account-number-to-hash response. The existing `account_numbers`
    /// method remains available for callers that need only raw response bytes.
    /// 中文摘要：读取并解析账户编号映射响应。
    pub async fn account_numbers_typed(&self) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed_with_priority(ReadRequest::AccountNumbers, ReadPriority::Urgent)
            .await
    }

    /// Reads the account list using validated query options and returns its typed projection.
    /// 按已校验查询选项读取账户列表并返回类型化投影。
    pub async fn accounts(&self, query: AccountsQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Accounts(query)).await
    }

    /// Reads one account through a validated path identifier and account-query options.
    /// 使用已校验路径标识和账户查询选项读取单个账户。
    pub async fn account(
        &self,
        account_hash: impl AsRef<str>,
        query: AccountsQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Account {
            account_hash: PathIdentifier::new(account_hash)?,
            query,
        })
        .await
    }

    /// Reads one account’s orders with validated time range and order filters.
    /// 按已校验时间范围和订单筛选条件读取指定账户的订单。
    pub async fn orders(
        &self,
        account_hash: impl AsRef<str>,
        query: OrdersQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Orders {
            account_hash: PathIdentifier::new(account_hash)?,
            query,
        })
        .await
    }

    /// Fetches account orders while preserving the Node SDK's additive query
    /// parameters on its fixed, allow-listed GET route.
    /// 中文摘要：读取订单并保留受限、无冲突的附加查询项。
    pub async fn orders_with_query_extensions(
        &self,
        account_hash: impl AsRef<str>,
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        let request = ReadRequest::orders_with_extensions(account_hash, query, extensions)?;
        self.read_typed(request).await
    }

    /// Reads one order through a validated account hash and broker order identifier.
    /// 按已校验账户哈希和 broker 订单编号读取单笔订单。
    pub async fn order(
        &self,
        account_hash: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Order {
            account_hash: PathIdentifier::new(account_hash)?,
            order_id: BrokerIdentifier::new(order_id)?,
        })
        .await
    }

    /// Reads cross-account orders for the validated time range without an account hash in the route.
    /// 按已校验时间范围读取跨账户订单，路由不包含账户哈希。
    pub async fn orders_across_accounts(
        &self,
        query: OrdersQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::OrdersAcrossAccounts(query))
            .await
    }

    /// Reads cross-account orders while preserving bounded, nonconflicting query extensions.
    /// 读取跨账户订单并保留有界且不冲突的扩展查询参数。
    pub async fn orders_across_accounts_with_query_extensions(
        &self,
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::orders_across_accounts_with_extensions(
            query, extensions,
        ))
        .await
    }

    /// Reads an account’s transaction records using the validated date-range query.
    /// 按已校验日期范围查询指定账户的交易记录。
    pub async fn transactions(
        &self,
        account_hash: impl AsRef<str>,
        query: TransactionsQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Transactions {
            account_hash: PathIdentifier::new(account_hash)?,
            query,
        })
        .await
    }

    /// Fetches account transactions with the Node SDK's additive query
    /// parameters on the existing fixed GET route.
    /// 中文摘要：读取交易记录并保留受限附加查询项。
    pub async fn transactions_with_query_extensions(
        &self,
        account_hash: impl AsRef<str>,
        query: TransactionsQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        let request = ReadRequest::transactions_with_extensions(account_hash, query, extensions)?;
        self.read_typed(request).await
    }

    /// Reads one transaction through a validated account hash and broker transaction identifier.
    /// 按已校验账户哈希和 broker 交易编号读取单条交易记录。
    pub async fn transaction(
        &self,
        account_hash: impl AsRef<str>,
        transaction_id: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Transaction {
            account_hash: PathIdentifier::new(account_hash)?,
            transaction_id: BrokerIdentifier::new(transaction_id)?,
        })
        .await
    }

    /// Fetches a schema-validated preferences response while retaining all
    /// HTTP metadata. The existing `user_preferences` method remains the raw
    /// response variant.
    /// 中文摘要：读取并解析用户偏好响应。
    pub async fn user_preferences_typed(&self) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed_with_priority(ReadRequest::UserPreferences, ReadPriority::Urgent)
            .await
    }

    /// Matches Node's `getStreamerInfo` convenience selection: first
    /// preference, then first streamer-info row.
    /// 中文摘要：从只读用户偏好响应中读取 streamer 启动元数据。
    pub async fn streamer_info(&self) -> Result<StreamerInfo, ReadApiError> {
        let typed = self
            .read_typed_with_priority(ReadRequest::StreamerInfo, ReadPriority::Urgent)
            .await?;
        let result = typed.parsed().streamer_info_model().cloned();
        match result {
            Ok(info) => Ok(info),
            Err(source) => Err(ReadApiError::Response {
                response: typed.into_response(),
                source,
            }),
        }
    }

    /// Reads and validates the quote-map response for the supplied query; the DTO does not establish freshness.
    /// 读取并校验给定查询的报价映射；该 DTO 不证明报价新鲜度。
    pub async fn quotes(&self, query: QuotesQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Quotes(query)).await
    }

    /// Reads one symbol through a validated path segment with an optional field selection.
    /// 通过已校验的单一路径段读取单标的报价，并可指定字段。
    pub async fn quote(
        &self,
        symbol: impl AsRef<str>,
        fields: Option<CsvValues>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Quote {
            symbol: PathIdentifier::new(symbol)?,
            fields,
        })
        .await
    }

    /// Reads one option code and returns its schema-validated raw quote projection.
    /// 读取一个期权代码并返回通过 schema 校验的原始报价投影。
    pub async fn option_quote(
        &self,
        symbol: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::option_quote(symbol)?).await
    }

    /// Normalizes and validates the requested option codes, then performs one bounded quote read.
    /// 归一化并校验期权代码后执行一次有界报价读取。
    pub async fn option_quotes<I, S>(&self, symbols: I) -> Result<TypedReadResponse, ReadApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read_typed(ReadRequest::option_quotes(symbols)?).await
    }

    /// Reads long and short option legs in caller order; it neither calculates a spread price nor certifies freshness.
    /// 按调用顺序读取 long/short 两腿报价；不计算价差，也不证明报价新鲜。
    pub async fn vertical_option_quote(
        &self,
        long_symbol: impl AsRef<str>,
        short_symbol: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::vertical_option_quote(
            long_symbol,
            short_symbol,
        )?)
        .await
    }

    /// Reads option-chain data using the validated chain filters.
    /// 使用已校验的链查询条件读取期权链数据。
    pub async fn option_chains(
        &self,
        query: OptionChainQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::OptionChains(Box::new(query)))
            .await
    }

    /// Reads contracts for the validated expiration-chain query.
    /// 按已校验到期日链查询读取合约。
    pub async fn option_expiration_chain(
        &self,
        query: OptionExpirationQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::OptionExpirationChain(query))
            .await
    }

    /// Reads historical price bars using the validated symbol and period parameters.
    /// 按已校验标的和周期参数读取历史价格 bar。
    pub async fn price_history(
        &self,
        query: PriceHistoryQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::PriceHistory(query)).await
    }

    /// Reads market movers through a validated symbol/market path and query.
    /// 通过已校验标的/市场路径及查询读取涨跌榜。
    pub async fn movers(
        &self,
        symbol: impl AsRef<str>,
        query: MoversQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Movers {
            symbol: PathIdentifier::new(symbol)?,
            query,
        })
        .await
    }

    /// Reads trading-hours data for the selected validated market set.
    /// 读取所选已校验市场集合的交易时段数据。
    pub async fn markets(&self, query: MarketsQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::Markets(query)).await
    }

    /// Reads one validated market path for the requested date and options.
    /// 按已校验市场路径及指定日期和选项读取交易时段。
    pub async fn market_hours(
        &self,
        market: impl AsRef<str>,
        query: MarketHoursQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::MarketHours {
            market: PathIdentifier::new(market)?,
            query,
        })
        .await
    }

    /// Searches instruments using the validated query and preserves broker response order.
    /// 使用已校验条件搜索标的，并保留 broker 响应顺序。
    pub async fn search_instruments(
        &self,
        query: InstrumentSearchQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::SearchInstruments(query)).await
    }

    /// Reads instrument details through one validated CUSIP path segment.
    /// 通过已校验的单个 CUSIP 路径段读取标的详情。
    pub async fn instrument_by_cusip(
        &self,
        cusip: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.read_typed(ReadRequest::InstrumentByCusip(PathIdentifier::new(cusip)?))
            .await
    }
}

fn default_read_priority(request: &ReadRequest) -> ReadPriority {
    match request {
        ReadRequest::AccountNumbers
        | ReadRequest::Accounts(_)
        | ReadRequest::Account { .. }
        | ReadRequest::Orders { .. }
        | ReadRequest::OrdersWithExtensions { .. }
        | ReadRequest::Order { .. }
        | ReadRequest::OrdersAcrossAccounts(_)
        | ReadRequest::OrdersAcrossAccountsWithExtensions { .. }
        | ReadRequest::Transactions { .. }
        | ReadRequest::TransactionsWithExtensions { .. }
        | ReadRequest::Transaction { .. } => ReadPriority::Urgent,
        _ => ReadPriority::Refresh,
    }
}
