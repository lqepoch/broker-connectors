//! Read-response parsing and typed projection facade.
//! 提供有界 JSON 解码、结构校验和类型化 REST 响应投影。

mod decimal;
mod option_quote;
mod validation;

pub use decimal::{ExactDecimal, ExactRatio};
pub use option_quote::{NormalizedOptionQuote, VerticalQuoteLegs};
#[cfg(test)]
pub(crate) use option_quote::{option_quotes_indexed_reference, parse_occ_symbol_fields_for_test};

use std::fmt;

use serde_json::Value;

use crate::client::{MAX_RESPONSE_BODY_BYTES, ReadEndpoint, RestResponse};
use crate::market_models::{self, MarketReadResponse};
use crate::response_models::{
    self, StreamerInfo, TraderReadResponse, Transaction, UserPreferencesResponse,
};
use validation::{check_complexity, validate};

/// Maximum number of account-number/hash rows accepted from one REST response.
/// This matches the hard snapshot-mapping limit in `account-state`.
/// 中文摘要：单次账户编号/哈希响应允许投影的最大行数。
pub const MAX_ACCOUNT_NUMBER_HASH_ROWS: usize = 4_096;

/// Response families that have Node SDK schemas in the characterization
/// baseline. This parser only handles successful REST responses.
/// 中文摘要：按固定 endpoint 选择的只读响应 schema，不接受任意调用方指定类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadResponseKind {
    /// Validated account-number and account-hash mapping response.
    /// 已校验的账户编号与账户哈希映射响应。
    AccountNumbers,
    /// Validated account-list response.
    /// 已校验的账户列表响应。
    Accounts,
    /// Validated single-account response.
    /// 已校验的单账户响应。
    Account,
    /// Validated order-list response.
    /// 已校验的订单列表响应。
    Orders,
    /// Validated single-order response.
    /// 已校验的单订单响应。
    Order,
    /// Validated transaction-list response.
    /// 已校验的交易记录列表响应。
    Transactions,
    /// Validated single-transaction response.
    /// 已校验的单条交易记录响应。
    Transaction,
    /// Validated user-preference and Streamer bootstrap response.
    /// 已校验的用户偏好与 Streamer 启动信息响应。
    UserPreferences,
    /// Validated quote-map response.
    /// 已校验的报价映射响应。
    Quotes,
    /// Validated single-symbol quote response.
    /// 已校验的单标的报价响应。
    SingleQuote,
    /// Validated option-chain response.
    /// 已校验的期权链响应。
    OptionChain,
    /// Validated option-expiration response.
    /// 已校验的期权到期日响应。
    OptionExpirationChain,
    /// Validated historical-price response.
    /// 已校验的历史价格响应。
    PriceHistory,
    /// Validated market-mover response.
    /// 已校验的市场涨跌榜响应。
    Movers,
    /// Validated market-hours response.
    /// 已校验的市场交易时间响应。
    MarketHours,
    /// Validated instrument-search response.
    /// 已校验的标的搜索响应。
    InstrumentsSearch,
    /// Validated instrument-detail response.
    /// 已校验的标的详情响应。
    InstrumentDetail,
}

impl ReadResponseKind {
    /// Returns the response family for an allow-listed read endpoint.
    ///
    /// Route names are internal identifiers attached by typed request
    /// builders; no URL or caller-provided path is inspected here.
    /// 中文摘要：按 allowlist 路由名选择固定 schema；未知路由返回 `None`，不会检查调用方 URL。
    #[must_use]
    pub fn for_endpoint(endpoint: &ReadEndpoint) -> Option<Self> {
        match endpoint.route_name() {
            "trader-account-numbers" => Some(Self::AccountNumbers),
            "trader-accounts" => Some(Self::Accounts),
            "trader-account" => Some(Self::Account),
            "trader-orders" | "trader-orders-across-accounts" => Some(Self::Orders),
            "trader-order" => Some(Self::Order),
            "trader-transactions" => Some(Self::Transactions),
            "trader-transaction" => Some(Self::Transaction),
            "trader-user-preferences" => Some(Self::UserPreferences),
            "market-quotes" | "market-option-quotes" | "market-vertical-option-quote" => {
                Some(Self::Quotes)
            }
            "market-quote" => Some(Self::SingleQuote),
            "market-option-chains" => Some(Self::OptionChain),
            "market-option-expiration-chain" => Some(Self::OptionExpirationChain),
            "market-price-history" => Some(Self::PriceHistory),
            "market-movers" => Some(Self::Movers),
            "market-hours-batch" | "market-hours" => Some(Self::MarketHours),
            "market-instrument-search" => Some(Self::InstrumentsSearch),
            "market-instrument-by-cusip" => Some(Self::InstrumentDetail),
            _ => None,
        }
    }
}

/// A fixed, payload-free parser or DTO failure category. Field paths are
/// schema names only and never contain values, symbols, account hashes, or
/// broker-provided error strings.
/// 中文摘要：JSON 结构、端点 schema 或数值边界错误；不保留原始响应内容。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadResponseError {
    /// The response status is not successful.
    /// HTTP 响应状态不是成功状态。
    HttpStatus,
    /// The response body exceeds the parser byte bound.
    /// 响应正文超出解析器字节上限。
    BodyTooLarge,
    /// The response body is not valid JSON.
    /// 响应正文不是有效 JSON。
    InvalidJson,
    /// The decoded JSON exceeds the node or nesting bound.
    /// 解码后的 JSON 超出节点数或嵌套深度上限。
    JsonTooComplex,
    /// The account-number snapshot exceeds its row bound.
    /// 账户编号快照超过行数上限。
    AccountNumberRowsTooMany,
    /// A required response field has the wrong shape or value type.
    /// 必需响应字段的结构或值类型不符合 schema。
    SchemaViolation {
        /// Static response-field path whose shape or value type failed validation.
        /// 校验失败的静态响应字段路径；不包含原始 broker 字段值。
        field: &'static str,
    },
    /// The parsed response family does not match the selected endpoint.
    /// 解析出的响应类型与所选端点不匹配。
    WrongResponseKind,
    /// The endpoint has no typed response projection.
    /// 该端点没有对应的类型化响应投影。
    UnsupportedEndpoint,
    /// The transaction endpoint returned no transaction item.
    /// 交易记录端点没有返回交易条目。
    TransactionNotFound,
    /// The preference response contains no usable Streamer information.
    /// 用户偏好响应中没有可用的 Streamer 信息。
    StreamerInfoUnavailable,
    /// The requested option quote is missing from the response.
    /// 响应中缺少请求的期权报价。
    QuoteUnavailable,
    /// The returned quote does not identify an option contract.
    /// 返回报价未标识为期权合约。
    QuoteNotOption,
    /// The returned contract symbol does not match the requested symbol after allowed padding trim.
    /// 按允许的尾部填充处理后，返回合约代码仍与请求不匹配。
    QuoteIdentityMismatch,
    /// The normalized quote request contains duplicate contract symbols.
    /// 规范化后的报价请求包含重复合约代码。
    DuplicateQuoteSymbol,
    /// A numeric token cannot fit the bounded exact-decimal representation.
    /// 数值 token 无法装入有界精确十进制表示。
    DecimalOutOfRange,
    /// Exact quote arithmetic exceeded the supported coefficient or scale.
    /// 精确报价运算超出支持的系数或小数位范围。
    DecimalArithmeticOutOfRange,
}

impl ReadResponseError {
    /// Returns the stable parser error code without response values or serde text.
    /// 返回稳定的固定错误代码。
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::HttpStatus => "REST_READ_RESPONSE_HTTP_STATUS",
            Self::BodyTooLarge => "REST_READ_RESPONSE_BODY_TOO_LARGE",
            Self::InvalidJson => "REST_READ_RESPONSE_INVALID_JSON",
            Self::JsonTooComplex => "REST_READ_RESPONSE_JSON_TOO_COMPLEX",
            Self::AccountNumberRowsTooMany => "REST_ACCOUNT_NUMBER_HASH_ROWS_TOO_MANY",
            Self::SchemaViolation { .. } => "REST_READ_RESPONSE_SCHEMA_INVALID",
            Self::WrongResponseKind => "REST_READ_RESPONSE_KIND_MISMATCH",
            Self::UnsupportedEndpoint => "REST_READ_RESPONSE_ENDPOINT_UNSUPPORTED",
            Self::TransactionNotFound => "REST_TRANSACTION_NOT_FOUND",
            Self::StreamerInfoUnavailable => "REST_STREAMER_INFO_UNAVAILABLE",
            Self::QuoteUnavailable => "REST_OPTION_QUOTE_UNAVAILABLE",
            Self::QuoteNotOption => "REST_SYMBOL_NOT_OPTION",
            Self::QuoteIdentityMismatch => "REST_OPTION_QUOTE_IDENTITY_MISMATCH",
            Self::DuplicateQuoteSymbol => "REST_OPTION_QUOTE_SYMBOL_DUPLICATE",
            Self::DecimalOutOfRange => "REST_READ_DECIMAL_OUT_OF_RANGE",
            Self::DecimalArithmeticOutOfRange => "REST_READ_DECIMAL_ARITHMETIC_OUT_OF_RANGE",
        }
    }

    /// Performs field for read response error.
    /// 执行 read response error 的 field 操作。
    #[must_use]
    pub const fn field(self) -> Option<&'static str> {
        match self {
            Self::SchemaViolation { field } => Some(field),
            _ => None,
        }
    }
}

impl fmt::Display for ReadResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ReadResponseError {}

/// Lossless validated JSON document. Unknown fields remain available through
/// `json()`, while Debug only reports kind/status/size.
/// 中文摘要：将成功 REST body 解码为有界 JSON，并按已知端点生成类型化只读投影。
pub struct ParsedReadResponse {
    kind: ReadResponseKind,
    status: u16,
    body_bytes: usize,
    json: Option<Value>,
    trader: Option<TraderReadResponse>,
    user_preferences: Option<UserPreferencesResponse>,
    market: Option<MarketReadResponse>,
}

impl ParsedReadResponse {
    /// Parses a response using the schema family attached to its typed route.
    /// This prevents route callers from pairing an allow-listed request with a
    /// different response validator.
    /// 中文摘要：根据响应绑定的 allowlist 端点选择 schema，并校验成功响应的有界正文。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn from_endpoint_response(response: &RestResponse) -> Result<Self, ReadResponseError> {
        let kind = ReadResponseKind::for_endpoint(response.endpoint())
            .ok_or(ReadResponseError::UnsupportedEndpoint)?;
        Self::from_response(kind, response)
    }

    /// Performs from response for parsed read response.
    /// 执行 parsed read response 的 from response 操作。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn from_response(
        kind: ReadResponseKind,
        response: &RestResponse,
    ) -> Result<Self, ReadResponseError> {
        Self::parse(kind, response.status(), response.body())
    }

    /// Performs parse for parsed read response.
    /// 执行 parsed read response 的 parse 操作。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn parse(
        kind: ReadResponseKind,
        status: u16,
        body: &[u8],
    ) -> Result<Self, ReadResponseError> {
        if !(200..300).contains(&status) {
            return Err(ReadResponseError::HttpStatus);
        }
        if body.len() > MAX_RESPONSE_BODY_BYTES {
            return Err(ReadResponseError::BodyTooLarge);
        }
        let (json, trader, user_preferences, market) = if body.is_empty() {
            (None, None, None, None)
        } else {
            let value: Value =
                serde_json::from_slice(body).map_err(|_| ReadResponseError::InvalidJson)?;
            validation::check_account_number_hash_row_limit(kind, &value)?;
            check_complexity(&value)?;
            validate(kind, &value)?;
            let trader = response_models::project(kind, &value)?;
            let user_preferences = if kind == ReadResponseKind::UserPreferences {
                Some(response_models::project_user_preferences(&value)?)
            } else {
                None
            };
            let market = market_models::project(kind, &value)?;
            (Some(value), trader, user_preferences, market)
        };
        Ok(Self {
            kind,
            status,
            body_bytes: body.len(),
            json,
            trader,
            user_preferences,
            market,
        })
    }

    /// Performs kind for parsed read response.
    /// 执行 parsed read response 的 kind 操作。
    #[must_use]
    pub const fn kind(&self) -> ReadResponseKind {
        self.kind
    }

    /// Returns the HTTP status recorded with the parsed body.
    /// 返回 broker HTTP 状态码。
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// An empty 200/204 response is preserved as `None` and skips DTO schema
    /// validation, matching the Node transport characterization.
    /// 中文摘要：借用已解码 JSON；空的 200/204 正文保持为 `None`，不执行 DTO 校验。
    #[must_use]
    pub fn json(&self) -> Option<&Value> {
        self.json.as_ref()
    }

    /// Returns the lossless typed Trader response for account, order, and
    /// transaction routes. A successful empty response has no model.
    /// 中文摘要：账户、订单或交易记录路由有非空有效正文时，返回对应类型化投影。
    #[must_use]
    pub fn trader_model(&self) -> Option<&TraderReadResponse> {
        self.trader.as_ref()
    }

    /// Returns the typed object-or-array preference response. The projection
    /// describes the validated response shape; it does not establish an
    /// authoritative or current broker snapshot.
    /// 中文摘要：返回已校验的单对象或数组偏好形态，不构成权威或当前 broker 快照。
    #[must_use]
    pub fn user_preferences_model(&self) -> Option<&UserPreferencesResponse> {
        self.user_preferences.as_ref()
    }

    /// Returns the typed Market Data response for market-hours or instrument
    /// routes. This is a projection of response syntax, not market authority.
    /// 中文摘要：返回类型化交易时段或标的投影，不建立行情 authority。
    #[must_use]
    pub fn market_model(&self) -> Option<&MarketReadResponse> {
        self.market.as_ref()
    }

    /// Returns the typed single-transaction object or first array row, matching
    /// Node's `getTransaction` convenience selection.
    /// 中文摘要：仅对交易记录端点返回单对象或数组首项；路由不符或结果为空时返回固定错误。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn transaction_model_convenience(&self) -> Result<&Transaction, ReadResponseError> {
        if self.kind != ReadResponseKind::Transaction {
            return Err(ReadResponseError::WrongResponseKind);
        }
        match self.trader.as_ref() {
            Some(TraderReadResponse::Transaction(response)) => response.first(),
            _ => Err(ReadResponseError::TransactionNotFound),
        }
    }

    /// Node's `getTransaction` accepts either an object or array and selects
    /// the first array member. Empty arrays use a fixed not-found error.
    /// 中文摘要：仅对交易记录端点返回已校验的原始 JSON 对象或数组首项。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn transaction_convenience(&self) -> Result<&Value, ReadResponseError> {
        if self.kind != ReadResponseKind::Transaction {
            return Err(ReadResponseError::WrongResponseKind);
        }
        match self.json.as_ref() {
            Some(Value::Array(values)) => {
                values.first().ok_or(ReadResponseError::TransactionNotFound)
            }
            Some(Value::Object(_)) => self
                .json
                .as_ref()
                .ok_or(ReadResponseError::TransactionNotFound),
            _ => Err(ReadResponseError::TransactionNotFound),
        }
    }

    /// Compatibility accessor for Node's first preference and first
    /// `streamerInfo` entry. The returned value is from the validated source
    /// JSON; prefer `streamer_info_model()` when typed fields are sufficient.
    /// 中文摘要：从只读用户偏好响应中读取 streamer 启动元数据。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn streamer_info(&self) -> Result<&Value, ReadResponseError> {
        if self.kind != ReadResponseKind::UserPreferences {
            return Err(ReadResponseError::WrongResponseKind);
        }
        let preferences = self
            .json
            .as_ref()
            .ok_or(ReadResponseError::StreamerInfoUnavailable)?;
        let preference = match preferences {
            Value::Array(values) => values.first(),
            Value::Object(_) => Some(preferences),
            _ => None,
        }
        .ok_or(ReadResponseError::StreamerInfoUnavailable)?;
        preference
            .get("streamerInfo")
            .and_then(Value::as_array)
            .and_then(|values| values.first())
            .ok_or(ReadResponseError::StreamerInfoUnavailable)
    }

    /// Returns the typed first `streamerInfo` in the first preference, matching
    /// Node's `getStreamerInfo` convenience selection. `streamer_info()` stays
    /// available for callers that need the original validated JSON value.
    /// 中文摘要：返回首个偏好中的首条类型化 StreamerInfo；URL 语法校验不授权网络目标。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn streamer_info_model(&self) -> Result<&StreamerInfo, ReadResponseError> {
        if self.kind != ReadResponseKind::UserPreferences {
            return Err(ReadResponseError::WrongResponseKind);
        }
        self.user_preferences
            .as_ref()
            .and_then(UserPreferencesResponse::first)
            .and_then(|preference| preference.streamer_info.as_ref())
            .and_then(|values| values.first())
            .ok_or(ReadResponseError::StreamerInfoUnavailable)
    }
}

impl fmt::Debug for ParsedReadResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedReadResponse")
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("body_bytes", &self.body_bytes)
            .field("json", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
