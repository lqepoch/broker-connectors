use std::fmt;

use alpaca_data::stocks::{
    Adjustment, BarsRequest, BarsResponse, Currency, DataFeed, Sort, TimeFrame,
};
use market_contracts::{
    DecimalString, EntitlementState, MarketDataSourceV1, NumericEncodingV1, UtcTimestamp,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{AlpacaRestError, MAX_PAGE_CURSOR_BYTES};

pub(crate) const ALPACA_DATA_API_ORIGIN: &str = "https://data.alpaca.markets";
const ALPACA_STOCK_BARS_API_PATH_TEMPLATE: &str = "/v2/stocks/{symbol}/bars";

/// Maximum number of stock bars requested from one provider page.
/// 单页最多请求此数量；该上限不代表所有历史数据已被读取。
pub const MAX_STOCK_BARS_PAGE_SIZE: u16 = 1_000;

/// Fixed feed intent for the stock-history facade.
/// 该枚举记录请求意图，不构成返回来源或账户权限证据。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedStockBarsFeed {
    /// Request the SIP feed. This is request intent, not source or entitlement evidence.
    /// 请求 SIP 不证明响应来自 SIP，也不证明账户具有相应权限。
    Sip,
    /// Request historical bars from the BOATS feed.
    /// 请求 BOATS 历史数据；请求意图不证明响应来源或 entitlement。
    Boats,
}

impl RequestedStockBarsFeed {
    /// Return the exact provider query value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Boats => "boats",
        }
    }

    fn to_sdk(self) -> DataFeed {
        match self {
            Self::Sip => DataFeed::Sip,
            Self::Boats => DataFeed::Boats,
        }
    }
}

/// Supported Alpaca stock-bar intervals.
/// 本枚举列出该适配器接受的股票 K 线周期。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Provider ordering for historical stock bars.
/// 历史股票 bars 的 provider 排序方向。旧构造器默认按时间升序。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StockBarsSort {
    /// Oldest bar first; this remains the default for existing constructors.
    #[default]
    Asc,
    /// Newest bar first; useful for bounded latest-history backfills.
    Desc,
}

impl StockBarsSort {
    /// Return the exact provider query value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }

    fn to_sdk(self) -> Sort {
        match self {
            Self::Asc => Sort::Asc,
            Self::Desc => Sort::Desc,
        }
    }
}

/// Successful response class recorded by an Alpaca stock-bars query witness.
/// Alpaca 股票 bars 查询见证记录的成功响应类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaStockBarsQuerySuccessClass {
    /// The pinned SDK returned a successful, decoded response; its exact HTTP status is not kept.
    /// 固定 SDK 返回并解码了成功响应；此类别不保留精确 HTTP 状态码。
    SdkReturnedDecodedResponse,
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

/// Validated identity for one single-symbol SIP or BOATS bars query.
#[derive(Clone, Eq, PartialEq)]
struct StockBarsQueryIdentity {
    symbol: String,
    timeframe: StockBarsTimeframe,
    start: UtcTimestamp,
    end: UtcTimestamp,
    limit: u16,
    feed: RequestedStockBarsFeed,
    sort: StockBarsSort,
    adjustment: &'static str,
    currency: &'static str,
    asof: &'static str,
}

/// Validated single-symbol request for one page of explicit-feed historical stock bars.
/// 此请求固定单一股票与 feed 意图，但不声明响应来源、权限或历史完整性。
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsRequest {
    identity: StockBarsQueryIdentity,
    cursor: Option<StockBarsCursor>,
}

impl AlpacaStockBarsRequest {
    /// Validate an uppercase stock symbol, a closed UTC time range, and a page size.
    ///
    /// The request defaults to `sip` and fixes adjustment to `raw`, sort to `asc`, currency to
    /// `USD`, and `asof` to `-`. Use [`Self::new_with_feed`] to select `boats`. A requested feed
    /// does not prove the response source or account entitlement.
    /// 默认请求 SIP；其他固定参数为 raw、asc、USD 和 `asof=-`。使用 `new_with_feed` 选择 BOATS。
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
        Self::new_with_feed(
            RequestedStockBarsFeed::Sip,
            symbol,
            timeframe,
            start,
            end,
            limit,
        )
    }

    /// Validate a historical stock-bars query with an explicit SIP or BOATS feed.
    ///
    /// A requested feed is intent only. Effective source and account entitlement remain unknown
    /// unless separately verified. Historical overnight bars use `boats`; `overnight` is not a
    /// supported historical-bars feed.
    /// 请求 feed 仅表示意图；实际来源和账户权限仍未知。历史隔夜 bars 使用 `boats`，不使用 `overnight`。
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] for a non-canonical symbol, invalid UTC range,
    /// or page size outside 1 through 1,000.
    pub fn new_with_feed(
        feed: RequestedStockBarsFeed,
        symbol: impl Into<String>,
        timeframe: StockBarsTimeframe,
        start: UtcTimestamp,
        end: UtcTimestamp,
        limit: u16,
    ) -> Result<Self, AlpacaRestError> {
        Self::new_with_feed_and_sort(
            feed,
            StockBarsSort::Asc,
            symbol,
            timeframe,
            start,
            end,
            limit,
        )
    }

    /// Validate a historical stock-bars query with explicit feed and provider sort order.
    ///
    /// Existing constructors keep their ascending default. Descending order is useful when a
    /// bounded reader must start at the latest bars; the returned rows retain provider order.
    /// Descending and ascending cursors are distinct query identities and cannot be interchanged.
    /// 显式选择 feed 和顺序；现有构造器仍默认升序。降序结果保持 provider 顺序，游标绑定顺序。
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] for a non-canonical symbol, invalid UTC range,
    /// or page size outside 1 through 1,000.
    pub fn new_with_feed_and_sort(
        feed: RequestedStockBarsFeed,
        sort: StockBarsSort,
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
                feed,
                sort,
                adjustment: "raw",
                currency: "USD",
                asof: "-",
            },
            cursor: None,
        })
    }

    /// Continue exactly the query that produced `cursor`.
    ///
    /// A cursor cannot be reused with a different symbol, timeframe, time range, page size, or
    /// any fixed query parameter, including the selected feed or sort direction.
    /// 游标绑定完整查询身份，不能跨股票、周期、时间范围、页大小或固定参数复用。
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] when the cursor belongs to another query.
    pub fn with_cursor(mut self, cursor: StockBarsCursor) -> Result<Self, AlpacaRestError> {
        if self.identity != cursor.identity || !cursor.is_well_formed() {
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

    /// Return the requested bar timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> StockBarsTimeframe {
        self.identity.timeframe
    }

    /// Return the inclusive UTC query start.
    #[must_use]
    pub const fn start(&self) -> &UtcTimestamp {
        &self.identity.start
    }

    /// Return the inclusive UTC query end.
    #[must_use]
    pub const fn end(&self) -> &UtcTimestamp {
        &self.identity.end
    }

    /// Return the maximum number of bars requested from one provider page.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.identity.limit
    }

    /// Return the explicit SIP or BOATS request intent.
    /// 返回请求意图，不是响应来源或权限证据。
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedStockBarsFeed {
        self.identity.feed
    }

    /// Return the requested provider ordering.
    #[must_use]
    pub const fn sort(&self) -> StockBarsSort {
        self.identity.sort
    }

    pub(crate) fn to_sdk_request(&self) -> BarsRequest {
        BarsRequest {
            symbols: vec![self.identity.symbol.clone()],
            timeframe: self.identity.timeframe.to_sdk(),
            start: Some(self.identity.start.as_str().to_owned()),
            end: Some(self.identity.end.as_str().to_owned()),
            limit: Some(u32::from(self.identity.limit)),
            adjustment: Some(Adjustment::from(self.identity.adjustment)),
            feed: Some(self.identity.feed.to_sdk()),
            sort: Some(self.identity.sort.to_sdk()),
            asof: Some(self.identity.asof.to_owned()),
            currency: Some(Currency::from(self.identity.currency)),
            page_token: self.cursor.as_ref().map(|cursor| cursor.token.clone()),
        }
    }

    pub(crate) fn cursor(&self) -> Option<&StockBarsCursor> {
        self.cursor.as_ref()
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
            .field("sort", &self.identity.sort)
            .field("has_cursor", &self.cursor.is_some())
            .finish()
    }
}

/// Opaque, redacted continuation for the next page of the same historical bars query.
/// 游标仅用于继续同一查询；其存在或缺失均不证明历史数据完整。
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

    pub(crate) fn provider_token(&self) -> &str {
        &self.token
    }

    fn is_well_formed(&self) -> bool {
        valid_stock_symbol(&self.identity.symbol)
            && self.identity.start <= self.identity.end
            && (1..=MAX_STOCK_BARS_PAGE_SIZE).contains(&self.identity.limit)
            && self.identity.adjustment == "raw"
            && self.identity.currency == "USD"
            && self.identity.asof == "-"
            && !self.token.is_empty()
            && self.token.len() <= MAX_PAGE_CURSOR_BYTES
            && !self.token.bytes().any(|byte| byte.is_ascii_control())
            && self.last_bar_timestamp.as_ref().is_none_or(|timestamp| {
                timestamp >= &self.identity.start && timestamp <= &self.identity.end
            })
    }
}

/// Opaque in-process witness for one successfully decoded Alpaca stock-bars query page.
/// 单页 Alpaca 股票 bars 查询的进程内不透明见证。
///
/// The value is constructed only by the read client after the pinned SDK returns a decoded
/// success and the page passes the adapter's validation. Its feed is the request's documented
/// source selector, not a response echo or an independently verified effective feed. It does not
/// establish account plan, entitlement, completeness, or licensing. The response digest covers
/// a stable projection of the SDK-typed response, not the HTTP wire body. This type has no public
/// constructor or serde implementation; do not treat a serialized copy as a trusted witness.
///
/// 该值仅由只读客户端在固定 SDK 成功解码并通过页面校验后构造。feed 是请求中的文档化来源选择器，
/// 不是响应回显，也不是独立核实的有效来源。它不证明账户计划、权限、完整性或许可。响应摘要覆盖
/// SDK 类型响应的稳定投影，不是 HTTP 原始报文摘要。该类型没有公共构造器或 serde 实现；序列化副本
/// 不应视为可信见证。
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsQueryAccessWitness {
    identity: StockBarsQueryIdentity,
    success_class: AlpacaStockBarsQuerySuccessClass,
    response_observed_at: UtcTimestamp,
    typed_response_sha256: String,
    row_count: u32,
    request_cursor_sha256: Option<String>,
    next_page_cursor_sha256: Option<String>,
}

impl AlpacaStockBarsQueryAccessWitness {
    /// Return the reviewed single-symbol historical-bars API contract identifier.
    #[must_use]
    pub const fn api_contract_id(&self) -> &'static str {
        "alpaca-stock-bars-single-symbol-v2/typed-observation-v1"
    }

    /// Return the HTTP method defined by the reviewed API contract.
    #[must_use]
    pub const fn api_method(&self) -> &'static str {
        "GET"
    }

    /// Return the fixed production HTTPS origin used by this adapter.
    #[must_use]
    pub const fn api_origin(&self) -> &'static str {
        ALPACA_DATA_API_ORIGIN
    }

    /// Return the path template defined by the reviewed API contract.
    #[must_use]
    pub const fn api_path_template(&self) -> &'static str {
        ALPACA_STOCK_BARS_API_PATH_TEMPLATE
    }

    /// Return the SDK success class; the exact HTTP status is not available in this adapter.
    #[must_use]
    pub const fn success_class(&self) -> AlpacaStockBarsQuerySuccessClass {
        self.success_class
    }

    /// Return the symbol bound to this request.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.identity.symbol
    }

    /// Return the requested source-feed selector, not an effective-feed assertion.
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedStockBarsFeed {
        self.identity.feed
    }

    /// Return the raw adjustment selector sent by this adapter.
    #[must_use]
    pub const fn adjustment(&self) -> &'static str {
        self.identity.adjustment
    }

    /// Return the fixed price currency sent by this adapter.
    #[must_use]
    pub const fn currency(&self) -> &'static str {
        self.identity.currency
    }

    /// Return the fixed symbol-mapping as-of selector sent by this adapter.
    #[must_use]
    pub const fn asof(&self) -> &'static str {
        self.identity.asof
    }

    /// Return the requested bar timeframe.
    #[must_use]
    pub const fn timeframe(&self) -> StockBarsTimeframe {
        self.identity.timeframe
    }

    /// Return the inclusive UTC start bound.
    #[must_use]
    pub const fn start_inclusive(&self) -> &UtcTimestamp {
        &self.identity.start
    }

    /// Return the inclusive UTC end bound.
    #[must_use]
    pub const fn end_inclusive(&self) -> &UtcTimestamp {
        &self.identity.end
    }

    /// Return the requested provider sort order.
    #[must_use]
    pub const fn sort(&self) -> StockBarsSort {
        self.identity.sort
    }

    /// Return the maximum number of rows requested in this page.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.identity.limit
    }

    /// Return SHA-256 of the incoming provider cursor, if this request continued a page.
    #[must_use]
    pub fn request_cursor_sha256(&self) -> Option<&str> {
        self.request_cursor_sha256.as_deref()
    }

    /// Return SHA-256 of the stable SDK-typed response projection, not the HTTP wire body.
    #[must_use]
    pub fn typed_response_sha256(&self) -> &str {
        &self.typed_response_sha256
    }

    /// Return when the decoded SDK response was observed by this process.
    #[must_use]
    pub const fn response_observed_at(&self) -> &UtcTimestamp {
        &self.response_observed_at
    }

    /// Return the number of validated bars in this response page.
    #[must_use]
    pub const fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Return SHA-256 of the next provider cursor, if a continuation was returned.
    #[must_use]
    pub fn next_page_cursor_sha256(&self) -> Option<&str> {
        self.next_page_cursor_sha256.as_deref()
    }

    /// Return whether this page contains at least one validated bar.
    /// An empty successful response proves query success only, not a returned row's source.
    /// 返回页面是否包含已校验 bar。成功空响应只表示查询成功，不证明任何返回行的来源。
    #[must_use]
    pub const fn contains_rows(&self) -> bool {
        self.row_count > 0
    }

    /// Return whether Alpaca supplied a continuation cursor; absence does not prove completeness.
    #[must_use]
    pub const fn has_next_page(&self) -> bool {
        self.next_page_cursor_sha256.is_some()
    }
}

impl fmt::Debug for AlpacaStockBarsQueryAccessWitness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsQueryAccessWitness")
            .field("api_contract_id", &self.api_contract_id())
            .field("success_class", &self.success_class)
            .field("row_count", &self.row_count)
            .field("has_request_cursor", &self.request_cursor_sha256.is_some())
            .field("has_next_page", &self.has_next_page())
            .finish()
    }
}

pub(crate) fn typed_bars_response_sha256(response: &BarsResponse) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"alpaca-stock-bars-sdk-typed-response-v1\0");
    update_optional_string(
        &mut hasher,
        response.currency.as_ref().map(Currency::as_str),
    );
    update_optional_string(&mut hasher, response.next_page_token.as_deref());

    let mut symbols = response.bars.iter().collect::<Vec<_>>();
    symbols.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    update_length(&mut hasher, symbols.len());
    for (symbol, bars) in symbols {
        update_string(&mut hasher, symbol);
        update_length(&mut hasher, bars.len());
        for bar in bars {
            update_optional_string(&mut hasher, bar.t.as_deref());
            update_optional_decimal(&mut hasher, bar.o.as_ref());
            update_optional_decimal(&mut hasher, bar.h.as_ref());
            update_optional_decimal(&mut hasher, bar.l.as_ref());
            update_optional_decimal(&mut hasher, bar.c.as_ref());
            update_optional_u64(&mut hasher, bar.v);
            update_optional_u64(&mut hasher, bar.n);
            update_optional_decimal(&mut hasher, bar.vw.as_ref());
        }
    }
    let digest = hasher.finalize();
    digest_hex(&digest)
}

fn update_length(hasher: &mut Sha256, length: usize) {
    let length = u64::try_from(length).unwrap_or(u64::MAX);
    hasher.update(length.to_be_bytes());
}

fn update_string(hasher: &mut Sha256, value: &str) {
    update_length(hasher, value.len());
    hasher.update(value.as_bytes());
}

fn update_optional_string(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            update_string(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn update_optional_decimal<T: fmt::Display>(hasher: &mut Sha256, value: Option<&T>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            update_string(hasher, &value.to_string());
        }
        None => hasher.update([0]),
    }
}

fn update_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        None => hasher.update([0]),
    }
}

fn digest_hex(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(digest.len() * 2);
    for &byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest_hex(&digest)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StockBarsCursorWire {
    version: u8,
    symbol: String,
    timeframe: StockBarsTimeframe,
    start: UtcTimestamp,
    end: UtcTimestamp,
    page_size: u16,
    feed: RequestedStockBarsFeed,
    sort: StockBarsSort,
    page_token: String,
    last_bar_timestamp: Option<UtcTimestamp>,
}

impl Serialize for StockBarsCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        StockBarsCursorWire {
            version: 1,
            symbol: self.identity.symbol.clone(),
            timeframe: self.identity.timeframe,
            start: self.identity.start.clone(),
            end: self.identity.end.clone(),
            page_size: self.identity.limit,
            feed: self.identity.feed,
            sort: self.identity.sort,
            page_token: self.token.clone(),
            last_bar_timestamp: self.last_bar_timestamp.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StockBarsCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StockBarsCursorWire::deserialize(deserializer)?;
        let cursor = Self {
            identity: StockBarsQueryIdentity {
                symbol: wire.symbol,
                timeframe: wire.timeframe,
                start: wire.start,
                end: wire.end,
                limit: wire.page_size,
                feed: wire.feed,
                sort: wire.sort,
                adjustment: "raw",
                currency: "USD",
                asof: "-",
            },
            token: wire.page_token,
            last_bar_timestamp: wire.last_bar_timestamp,
        };
        if wire.version != 1 || !cursor.is_well_formed() {
            return Err(serde::de::Error::custom("invalid stock-bars cursor"));
        }
        Ok(cursor)
    }
}

impl fmt::Debug for StockBarsCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StockBarsCursor([REDACTED])")
    }
}

/// One exact-decimal historical bar with separate request intent and unknown source evidence.
/// 价格保留精确十进制；有效来源和权限保持未知，不能据此判定数据已获授权。
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
    /// 实际 feed 与 entitlement 均为 unknown；请求 SIP 不会提升来源可信度。
    #[must_use]
    pub const fn source(&self) -> &MarketDataSourceV1 {
        &self.source
    }

    /// Return the explicit SIP or BOATS request intent, not proof of the returned source.
    /// 返回请求意图，不表示响应 feed 或权限已经确认。
    #[must_use]
    pub const fn requested_feed(&self) -> RequestedStockBarsFeed {
        self.requested_feed
    }
}

/// The result from exactly one historical stock-bars page, retaining the requested provider order.
/// 结果只覆盖单页；无 continuation token 不等于上游证明历史查询完整。
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsPage {
    bars: Vec<AlpacaStockBarObservation>,
    response_observed_at: UtcTimestamp,
    next_cursor: Option<StockBarsCursor>,
    query_access_witness: Option<AlpacaStockBarsQueryAccessWitness>,
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
                    .is_some_and(|previous| match request.identity.sort {
                        StockBarsSort::Asc => timestamp <= *previous,
                        StockBarsSort::Desc => timestamp >= *previous,
                    })
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
            query_access_witness: None,
        })
    }

    pub(crate) fn attach_query_access_witness(
        &mut self,
        request: &AlpacaStockBarsRequest,
        typed_response_sha256: String,
    ) {
        self.query_access_witness = Some(AlpacaStockBarsQueryAccessWitness {
            identity: request.identity.clone(),
            success_class: AlpacaStockBarsQuerySuccessClass::SdkReturnedDecodedResponse,
            response_observed_at: self.response_observed_at.clone(),
            typed_response_sha256,
            row_count: u32::try_from(self.bars.len())
                .expect("validated stock-bars page is bounded to 1,000 rows"),
            request_cursor_sha256: request
                .cursor
                .as_ref()
                .map(|cursor| sha256_hex(cursor.provider_token().as_bytes())),
            next_page_cursor_sha256: self
                .next_cursor
                .as_ref()
                .map(|cursor| sha256_hex(cursor.provider_token().as_bytes())),
        });
    }

    /// Borrow bars returned from this provider page in the requested sort order.
    #[must_use]
    pub fn bars(&self) -> &[AlpacaStockBarObservation] {
        &self.bars
    }

    /// Return when the SDK returned the decoded HTTP response to the adapter.
    ///
    /// This local observation time is not the source timestamp or historical point-in-time
    /// availability.
    /// 这是本机观察时间，不是行情源时间或历史可得时间。
    #[must_use]
    pub const fn response_observed_at(&self) -> &UtcTimestamp {
        &self.response_observed_at
    }

    /// Borrow the in-process witness attached only after a real SDK read and page validation.
    /// Pages made by the parser from synthetic payloads do not carry a witness.
    /// 仅在真实 SDK 读取且页面校验成功后附加进程内见证；合成 payload 解析页没有此见证。
    #[must_use]
    pub fn query_access_witness(&self) -> Option<&AlpacaStockBarsQueryAccessWitness> {
        self.query_access_witness.as_ref()
    }

    /// Return whether Alpaca supplied a continuation token for another page.
    /// 仅表示是否收到下一页游标；无游标不证明所有历史数据均已返回。
    #[must_use]
    pub const fn has_next_page(&self) -> bool {
        self.next_cursor.is_some()
    }

    /// Take the opaque continuation token, if present.
    /// 取出用于继续同一查询的不透明游标，不代表数据完整性证据。
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
            .field(
                "has_query_access_witness",
                &self.query_access_witness.is_some(),
            )
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
