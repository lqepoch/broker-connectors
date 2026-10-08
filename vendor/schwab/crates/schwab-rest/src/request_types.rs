//! Validated inputs accepted by the read-only REST request builders.
//! 只读 REST 请求构造器接受的已校验输入。

use std::fmt;

use crate::read_response::ExactDecimal;

const MAX_PATH_IDENTIFIER_BYTES: usize = 256;
const MAX_QUERY_TEXT_BYTES: usize = 4_096;
pub(crate) const MAX_QUERY_ITEMS: usize = 100;

/// A validated opaque identifier used only as one encoded path segment.
/// Account hashes are deliberately redacted from Debug output.
/// 仅作为一个编码后路径段使用的已校验不透明标识符。
/// 账户哈希在 Debug 输出中会被隐藏。
#[derive(Clone, Eq, PartialEq)]
pub struct PathIdentifier(String);

impl PathIdentifier {
    /// Validates and stores one bounded path identifier.
    /// 校验并保存一个有长度上限的路径标识符。
    pub fn new(value: impl AsRef<str>) -> Result<Self, ReadRequestError> {
        let value = value.as_ref().trim();
        if value.is_empty()
            || value.len() > MAX_PATH_IDENTIFIER_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(ReadRequestError::InvalidPathIdentifier);
        }
        Ok(Self(value.to_owned()))
    }

    pub(crate) fn encoded(&self) -> String {
        percent_encode(self.0.as_bytes())
    }
}

impl fmt::Debug for PathIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PathIdentifier([REDACTED])")
    }
}

/// A positive Schwab int64 identifier. Its decimal spelling is retained so
/// values above JavaScript's safe integer range are never rounded.
/// Schwab 的正 int64 标识符。保留十进制文本，避免超出 JavaScript 安全整数范围时发生舍入。
#[derive(Clone, Eq, PartialEq)]
pub struct BrokerIdentifier(String);

impl BrokerIdentifier {
    /// Validates and stores a positive Schwab order or transaction identifier.
    /// 校验并保存一个正数 Schwab 订单或交易标识符。
    pub fn new(value: impl AsRef<str>) -> Result<Self, ReadRequestError> {
        let value = value.as_ref().trim();
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ReadRequestError::InvalidNumericIdentifier);
        }
        let parsed = value
            .parse::<u64>()
            .map_err(|_| ReadRequestError::InvalidNumericIdentifier)?;
        if parsed == 0 || parsed > i64::MAX as u64 {
            return Err(ReadRequestError::InvalidNumericIdentifier);
        }
        Ok(Self(value.to_owned()))
    }

    pub(crate) fn encoded(&self) -> String {
        self.0.clone()
    }
}

impl fmt::Debug for BrokerIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrokerIdentifier([REDACTED])")
    }
}

/// A bounded query string. Control characters are rejected before encoding.
/// The formatter hides symbols and other potentially identifying values.
/// 有长度上限的查询文本；编码前会拒绝控制字符。
/// 格式化输出会隐藏交易代码等可能用于识别的信息。
#[derive(Clone, Eq, PartialEq)]
pub struct QueryText(String);

impl QueryText {
    /// Validates and stores query text without changing its spelling.
    /// 校验并保存查询文本，不改变其原始拼写。
    pub fn new(value: impl Into<String>) -> Result<Self, ReadRequestError> {
        let value = value.into();
        if value.len() > MAX_QUERY_TEXT_BYTES || value.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(ReadRequestError::InvalidQueryValue);
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for QueryText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryText([REDACTED])")
    }
}

/// A finite decimal query value retained as text to avoid formatting drift.
/// 以文本形式保留的有限十进制查询值，避免格式化造成数值变化。
#[derive(Clone, Eq, PartialEq)]
pub struct DecimalQuery(String);

impl DecimalQuery {
    /// Validates an exact finite decimal and retains its original spelling.
    /// 校验精确有限十进制数，并保留其原始拼写。
    pub fn new(value: impl Into<String>) -> Result<Self, ReadRequestError> {
        let value = value.into();
        if value.len() > MAX_QUERY_TEXT_BYTES || ExactDecimal::parse(&value).is_err() {
            return Err(ReadRequestError::InvalidQueryValue);
        }
        QueryText::new(value.clone())?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DecimalQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DecimalQuery([REDACTED])")
    }
}

/// A bounded comma-separated field or symbol list.
/// 有长度和项目数上限的逗号分隔字段或交易代码列表。
#[derive(Clone, Eq, PartialEq)]
pub struct CsvValues(String);

impl CsvValues {
    /// Represents an explicitly empty optional field selection (`fields=`).
    /// Required symbol/market lists reject this value when their route is built.
    /// 表示显式的空可选字段选择（`fields=`）。
    /// 构造路由时，必填交易代码或市场列表会拒绝此值。
    pub fn empty() -> Self {
        Self(String::new())
    }

    /// Builds a list containing exactly one validated value.
    /// 构造仅包含一个已校验值的列表。
    pub fn one(value: impl Into<String>) -> Result<Self, ReadRequestError> {
        Self::from_values([value.into()])
    }

    /// Validates and joins a bounded list whose items cannot contain commas.
    /// 校验并拼接有上限的列表；单个项目不能包含逗号。
    pub fn from_values<I, S>(values: I) -> Result<Self, ReadRequestError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values = values
            .into_iter()
            .map(|value| QueryText::new(value.into()))
            .collect::<Result<Vec<_>, _>>()?;
        if values.is_empty() || values.len() > MAX_QUERY_ITEMS {
            return Err(ReadRequestError::InvalidList);
        }
        if values
            .iter()
            .any(|value| value.as_str().trim().is_empty() || value.as_str().contains(','))
        {
            return Err(ReadRequestError::InvalidList);
        }
        let joined = values
            .iter()
            .map(QueryText::as_str)
            .collect::<Vec<_>>()
            .join(",");
        if joined.len() > MAX_QUERY_TEXT_BYTES {
            return Err(ReadRequestError::InvalidList);
        }
        Ok(Self(joined))
    }

    /// Preserves the Node SDK's explicit single-string form, including
    /// comma-separated values, while bounding its size and rejecting control
    /// characters. Array/list callers should use `from_values`, which rejects
    /// commas inside an element rather than silently splitting it.
    /// 保留 Node SDK 的显式单字符串形式（包括逗号分隔值），同时限制长度并拒绝控制字符。
    /// 数组或列表调用方应使用 `from_values`；该方法会拒绝项目中的逗号，而不会静默拆分。
    pub fn from_csv(value: impl Into<String>) -> Result<Self, ReadRequestError> {
        let value = QueryText::new(value.into())?;
        if value.as_str().trim().is_empty() {
            return Err(ReadRequestError::InvalidList);
        }
        Ok(Self(value.0))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CsvValues {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CsvValues([REDACTED])")
    }
}

/// Bounded additive query parameters for Node Trader request objects that
/// permit caller-defined keys. These values can only extend an existing
/// allow-listed GET route; they cannot select a route, method, or body.
/// 为允许调用方自定义键的 Node Trader 请求提供有界附加查询参数。
/// 这些值只能扩展已有白名单 GET 路由，不能选择路由、HTTP 方法或请求体。
#[derive(Clone, Default, Eq, PartialEq)]
pub struct QueryExtensions(Vec<(QueryText, String)>);

impl QueryExtensions {
    /// Creates an empty set of additive query parameters.
    /// 创建一个空的附加查询参数集合。
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an optional string parameter. `None` mirrors Node's omission of
    /// `undefined` and `null`; an empty string is retained as `key=`.
    /// 添加可选字符串参数。`None` 与 Node 忽略 `undefined` 和 `null` 的行为一致；空字符串保留为 `key=`。
    pub fn push_optional_text(
        &mut self,
        key: impl Into<String>,
        value: Option<&str>,
    ) -> Result<(), ReadRequestError> {
        let Some(value) = value else {
            return Ok(());
        };
        let value = QueryText::new(value.to_owned())?;
        self.push(key, value.as_str().to_owned())
    }

    /// Adds an optional finite exact decimal parameter. Values keep their
    /// original decimal spelling and do not pass through binary floating
    /// point. `None` is omitted.
    /// 添加可选的精确有限十进制参数。保留原始十进制文本，不经过二进制浮点转换；`None` 会被省略。
    pub fn push_optional_number(
        &mut self,
        key: impl Into<String>,
        value: Option<&str>,
    ) -> Result<(), ReadRequestError> {
        let Some(value) = value else {
            return Ok(());
        };
        let value = DecimalQuery::new(value.to_owned())?;
        self.push(key, value.as_str().to_owned())
    }

    /// Adds an optional boolean parameter. `false` is retained as the string
    /// `false`; only `None` is omitted.
    /// 添加可选布尔参数。`false` 会保留为字符串 `false`；只有 `None` 会被省略。
    pub fn push_optional_bool(
        &mut self,
        key: impl Into<String>,
        value: Option<bool>,
    ) -> Result<(), ReadRequestError> {
        let Some(value) = value else {
            return Ok(());
        };
        self.push(key, value.to_string())
    }

    fn push(&mut self, key: impl Into<String>, value: String) -> Result<(), ReadRequestError> {
        let key = key.into();
        if key.is_empty()
            || key.len() > MAX_QUERY_TEXT_BYTES
            || key.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(ReadRequestError::InvalidQueryKey);
        }
        let key = QueryText::new(key)?;
        if self.0.len() >= MAX_QUERY_ITEMS {
            return Err(ReadRequestError::TooManyQueryParameters);
        }
        if self.0.iter().any(|(existing, _)| existing == &key) {
            return Err(ReadRequestError::DuplicateQueryParameter);
        }
        self.0.push((key, value));
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl fmt::Debug for QueryExtensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryExtensions([REDACTED])")
    }
}

/// A stable validation error produced while building a read request.
/// 构造读取请求时产生的稳定校验错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadRequestError {
    /// The path identifier is empty, oversized, or contains control characters.
    /// 路径标识符为空、超长或包含控制字符。
    InvalidPathIdentifier,
    /// The broker identifier is not a positive int64 decimal value.
    /// Broker 标识符不是正 int64 十进制值。
    InvalidNumericIdentifier,
    /// The query key is empty, oversized, or contains control characters.
    /// 查询键为空、超长或包含控制字符。
    InvalidQueryKey,
    /// The query value is oversized, malformed, or contains control characters.
    /// 查询值超长、格式无效或包含控制字符。
    InvalidQueryValue,
    /// A required list is empty or violates its item constraints.
    /// 必填列表为空或不符合项目约束。
    InvalidList,
    /// A query key occurs more than once.
    /// 查询键重复。
    DuplicateQueryParameter,
    /// An extension key conflicts with a field already represented by the route.
    /// 附加参数键与路由已定义的字段冲突。
    ConflictingQueryParameter,
    /// The request contains more query parameters than the configured bound.
    /// 请求中的查询参数数量超过上限。
    TooManyQueryParameters,
    /// The encoded request target exceeds its configured bound.
    /// 编码后的请求目标超过长度上限。
    TargetTooLong,
}

impl fmt::Display for ReadRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPathIdentifier => "REST_READ_PATH_IDENTIFIER_INVALID",
            Self::InvalidNumericIdentifier => "REST_READ_NUMERIC_IDENTIFIER_INVALID",
            Self::InvalidQueryKey => "REST_READ_QUERY_KEY_INVALID",
            Self::InvalidQueryValue => "REST_READ_QUERY_VALUE_INVALID",
            Self::InvalidList => "REST_READ_LIST_INVALID",
            Self::DuplicateQueryParameter => "REST_READ_QUERY_PARAMETER_DUPLICATE",
            Self::ConflictingQueryParameter => "REST_READ_QUERY_PARAMETER_CONFLICT",
            Self::TooManyQueryParameters => "REST_READ_QUERY_PARAMETER_LIMIT",
            Self::TargetTooLong => "REST_READ_TARGET_TOO_LONG",
        })
    }
}

impl std::error::Error for ReadRequestError {}

/// Optional field selection for account read routes.
/// 账户读取路由的可选字段选择。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AccountsQuery {
    /// Account properties to request through Schwab’s `fields` filter.
    /// 传入 Schwab `fields` 筛选参数的账户属性选择。
    pub fields: Option<QueryText>,
}

/// Required time range and optional filters for order-list reads.
/// 订单列表读取所需的时间范围和可选筛选条件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrdersQuery {
    /// Inclusive lower bound for the order-entry time range.
    /// 订单录入时间范围的起点。
    pub from_entered_time: QueryText,
    /// Upper bound for the order-entry time range.
    /// 订单录入时间范围的终点。
    pub to_entered_time: QueryText,
    /// Optional upper limit on the number of orders requested.
    /// broker 查询结果的数量上限。
    pub max_results: Option<i64>,
    /// Optional broker order-status filter.
    /// broker 或服务返回的状态文本。
    pub status: Option<QueryText>,
}

/// Required time range and optional filters for transaction reads.
/// 交易读取所需的时间范围和可选筛选条件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionsQuery {
    /// Lower date or timestamp bound for returned transactions.
    /// 查询时间范围的起始日期或时刻。
    pub start_date: QueryText,
    /// Upper date or timestamp bound for returned transactions.
    /// 查询时间范围的结束日期或时刻。
    pub end_date: QueryText,
    /// Transaction-type filter sent to the broker.
    /// 交易记录类型筛选值。
    pub types: QueryText,
    /// Optional symbol filter for the transaction search.
    /// 标的或合约交易代码。
    pub symbol: Option<QueryText>,
}

/// Symbol and field selection for quote reads.
/// 报价读取使用的交易代码和字段选择。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotesQuery {
    /// Bounded comma-separated symbols requested in the quote response.
    /// 报价查询使用的有界逗号分隔标的列表。
    pub symbols: CsvValues,
    /// Optional comma-separated quote fields selected in the response.
    /// 可选的逗号分隔报价字段投影。
    pub fields: Option<CsvValues>,
    /// Whether to include indicative quotes in the response.
    /// 是否在报价响应中包含 indicative 报价。
    pub indicative: Option<bool>,
}

/// Typed filters accepted by the option-chain route.
/// 期权链路由接受的类型化筛选条件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionChainQuery {
    /// Underlying symbol whose option chain is requested.
    /// 标的或合约交易代码。
    pub symbol: QueryText,
    /// Optional option-contract category filter.
    /// 期权合约类型。
    pub contract_type: Option<QueryText>,
    /// Whether the chain response should include the underlying quote.
    /// 是否在期权链响应中请求标的报价。
    pub include_underlying_quote: Option<bool>,
    /// Deprecated Node SDK alias; `include_underlying_quote` takes precedence.
    /// 已弃用的 Node SDK 别名；`include_underlying_quote` 优先。
    pub include_quotes: Option<bool>,
    /// Optional option-chain strategy filter.
    /// 期权链策略筛选值。
    pub strategy: Option<QueryText>,
    /// Optional strike interval for the returned chain.
    /// 查询使用的数值间隔。
    pub interval: Option<i64>,
    /// Optional number of strikes requested around the target.
    /// 请求的行权价数量。
    pub strike_count: Option<i64>,
    /// Optional exact strike price filter.
    /// 期权行权价。
    pub strike: Option<DecimalQuery>,
    /// Optional broker range selector for the chain.
    /// 期权链行权价范围选择。
    pub range: Option<QueryText>,
    /// Optional lower expiration-date bound.
    /// 期权链起始日期。
    pub from_date: Option<QueryText>,
    /// Optional upper expiration-date bound.
    /// 期权链结束日期。
    pub to_date: Option<QueryText>,
    /// Optional implied-volatility input sent with the chain request.
    /// 随期权链请求发送的可选隐含波动率参数。
    pub volatility: Option<DecimalQuery>,
    /// Optional underlying-price input for the chain request.
    /// 期权链请求使用的可选标的价格参数。
    pub underlying_price: Option<DecimalQuery>,
    /// Optional interest-rate input retained as exact decimal text.
    /// 构造期权链时请求使用的利率文本。
    pub interest_rate: Option<DecimalQuery>,
    /// Optional days-to-expiration filter.
    /// 请求的到期天数。
    pub days_to_expiration: Option<i64>,
    /// Optional expiration-month filter.
    /// 期权到期月份筛选值。
    pub exp_month: Option<QueryText>,
    /// Optional call/put type filter.
    /// 期权类型筛选值。
    pub option_type: Option<QueryText>,
    /// Optional broker entitlement selector for chain data.
    /// broker 返回或查询使用的 entitlement 标记。
    pub entitlement: Option<QueryText>,
}

/// Typed filters accepted by the option-expiration route.
/// 期权到期日路由接受的类型化筛选条件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionExpirationQuery {
    /// Underlying symbol whose option expiration dates are requested.
    /// 标的或合约交易代码。
    pub symbol: QueryText,
    /// Optional option-contract category filter.
    /// 期权合约类型。
    pub contract_type: Option<QueryText>,
    /// Optional expiration-month filter.
    /// 期权到期月份筛选值。
    pub exp_month: Option<QueryText>,
    /// Optional call/put type filter.
    /// 期权类型筛选值。
    pub option_type: Option<QueryText>,
}

/// Symbol, interval, and session options for price-history reads.
/// 历史价格读取使用的交易代码、周期和交易时段选项。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceHistoryQuery {
    /// Symbol whose historical price bars are requested.
    /// 标的或合约交易代码。
    pub symbol: QueryText,
    /// Optional unit used to interpret the requested history period.
    /// 历史价格周期类别。
    pub period_type: Option<QueryText>,
    /// Optional number of history periods requested.
    /// 历史价格周期数量。
    pub period: Option<i64>,
    /// Optional unit used to interpret each bar frequency.
    /// 历史价格频率类别。
    pub frequency_type: Option<QueryText>,
    /// Optional number of frequency units per price bar.
    /// 历史价格频率值。
    pub frequency: Option<i64>,
    /// Optional inclusive start timestamp in milliseconds.
    /// 查询时间范围的起始日期或时刻。
    pub start_date: Option<i64>,
    /// Optional end timestamp in milliseconds.
    /// 查询时间范围的结束日期或时刻。
    pub end_date: Option<i64>,
    /// Whether to request extended-hours bars.
    /// 是否请求延长交易时段数据。
    pub need_extended_hours_data: Option<bool>,
    /// Whether to request the previous-close value.
    /// 是否请求前一收盘价。
    pub need_previous_close: Option<bool>,
}

/// Optional sorting and frequency filters for market movers.
/// 市场涨跌幅榜的可选排序和频率筛选条件。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MoversQuery {
    /// Optional market-mover sort key.
    /// 涨跌榜排序方式。
    pub sort: Option<QueryText>,
    /// Optional sampling frequency for the mover query.
    /// 涨跌榜查询使用的可选采样频率。
    pub frequency: Option<i64>,
}

/// Market selection and optional date for market-hours reads.
/// 市场时间读取使用的市场选择和可选日期。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketsQuery {
    /// Bounded comma-separated market identifiers to query.
    /// 市场代码列表。
    pub markets: CsvValues,
    /// Optional trading date for market-hours data.
    /// 交易日或请求日期。
    pub date: Option<QueryText>,
}

/// Optional date for a single-market hours read.
/// 单个市场时间读取的可选日期。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarketHoursQuery {
    /// Optional trading date for the selected market.
    /// 交易日或请求日期。
    pub date: Option<QueryText>,
}

/// Symbol selection and projection for instrument search.
/// 标的搜索使用的交易代码和投影类型。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSearchQuery {
    /// Bounded comma-separated symbols used to search instruments.
    /// 用于搜索标的的有界逗号分隔代码列表。
    pub symbols: CsvValues,
    /// Broker projection that selects the instrument fields returned.
    /// instrument 搜索返回的字段投影。
    pub projection: QueryText,
}

fn percent_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}
