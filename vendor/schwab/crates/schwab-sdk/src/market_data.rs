//! Typed and normalized Market Data GET facade. All work delegates to
//! `schwab-rest`; quote projections are structural data, not freshness or
//! tradeability authorization.
//! 基于现有类型化 REST 客户端提供只读市场数据 facade。

use schwab_rest::{AccessTokenProvider, HttpTransport, SchwabRestClient};

pub use schwab_rest::{
    CsvValues, DecimalQuery, InstrumentSearchQuery, MarketHoursQuery, MarketsQuery, MoversQuery,
    OptionChainQuery, OptionExpirationQuery, PriceHistoryQuery, QueryText, QuotesQuery,
};

use crate::{NormalizedOptionQuoteReadResponse, ReadApiError, TypedReadResponse};

/// Borrowed Market Data GET operations over the SDK's shared REST client.
/// 中文摘要：对固定 Market Data GET 路由的轻量借用 facade。
pub struct MarketData<'a, P, T> {
    client: &'a SchwabRestClient<P, T>,
}

impl<'a, P, T> MarketData<'a, P, T> {
    pub(crate) const fn new(client: &'a SchwabRestClient<P, T>) -> Self {
        Self { client }
    }
}

impl<P, T> MarketData<'_, P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Reads quotes for validated symbols and fields.
    /// 中文摘要：按已校验的查询参数读取市场报价。
    pub async fn quotes(&self, query: QuotesQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.client.quotes(query).await
    }

    /// Reads one symbol's quote response.
    /// 中文摘要：按单个路径段读取交易代码报价。
    pub async fn quote(
        &self,
        symbol: impl AsRef<str>,
        fields: Option<CsvValues>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.quote(symbol, fields).await
    }

    /// Reads one option quote using the existing REST response validation.
    /// 中文摘要：构造单个规范化期权报价读取请求。
    pub async fn option_quote(
        &self,
        symbol: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.option_quote(symbol).await
    }

    /// Reads multiple option quotes using the existing REST response validation.
    /// 中文摘要：将 broker option-quote map 按规则解析为有序结构化报价；不判定新鲜度。
    pub async fn option_quotes<I, S>(&self, symbols: I) -> Result<TypedReadResponse, ReadApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.client.option_quotes(symbols).await
    }

    /// Reads and structurally normalizes option quotes in one GET.
    ///
    /// `observed_at_ms` is supplied by the caller for deterministic age
    /// diagnostics. The result does not prove quote freshness or tradeability.
    /// 中文摘要：读取规范化期权报价并同时保留原始响应元数据；不判定 freshness 或 tradability。
    pub async fn normalized_option_quotes<I, S>(
        &self,
        symbols: I,
        fields: Option<CsvValues>,
        observed_at_ms: i64,
    ) -> Result<NormalizedOptionQuoteReadResponse, ReadApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.client
            .normalized_option_quotes(symbols, fields, observed_at_ms)
            .await
    }

    /// Reads two option legs through the existing typed GET route.
    /// 中文摘要：构造按 long/short 顺序读取双腿报价的请求。
    pub async fn vertical_option_quote(
        &self,
        long_symbol: impl AsRef<str>,
        short_symbol: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client
            .vertical_option_quote(long_symbol, short_symbol)
            .await
    }

    /// Reads an option chain.
    /// 中文摘要：读取期权链 DTO。
    pub async fn option_chains(
        &self,
        query: OptionChainQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.option_chains(query).await
    }

    /// Reads option expiration dates.
    /// 中文摘要：读取期权到期日链 DTO。
    pub async fn option_expiration_chain(
        &self,
        query: OptionExpirationQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.option_expiration_chain(query).await
    }

    /// Reads price history.
    /// 中文摘要：按查询参数读取历史价格。
    pub async fn price_history(
        &self,
        query: PriceHistoryQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.price_history(query).await
    }

    /// Reads market movers for one market symbol.
    /// 中文摘要：读取指定标的或市场的涨跌榜。
    pub async fn movers(
        &self,
        symbol: impl AsRef<str>,
        query: MoversQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.movers(symbol, query).await
    }

    /// Reads market hours for a set of markets.
    /// 中文摘要：读取所选市场的交易时间。
    pub async fn markets(&self, query: MarketsQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.client.markets(query).await
    }

    /// Reads market hours for one market.
    /// 中文摘要：读取指定市场的交易时间。
    pub async fn market_hours(
        &self,
        market: impl AsRef<str>,
        query: MarketHoursQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.market_hours(market, query).await
    }

    /// Searches instruments.
    /// 中文摘要：按已校验查询条件搜索标的。
    pub async fn search_instruments(
        &self,
        query: InstrumentSearchQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.search_instruments(query).await
    }

    /// Reads an instrument by CUSIP.
    /// 中文摘要：按 CUSIP 路径段读取标的详情。
    pub async fn instrument_by_cusip(
        &self,
        cusip: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.instrument_by_cusip(cusip).await
    }
}
