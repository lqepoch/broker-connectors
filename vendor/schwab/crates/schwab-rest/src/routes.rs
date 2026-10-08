//! Finite read-only endpoint builders with encoded identifiers and fixed query keys.
//! 提供有限的只读端点构造器，并编码标识符且固定查询键。

use std::fmt;

use crate::client::ReadEndpoint;
use crate::query_encoding::{
    append_option_chain_query, append_orders_query, append_transactions_query,
    encode_query_component, push_bool, push_i64, push_pair, push_pair_option, push_text,
};
use crate::request_types::MAX_QUERY_ITEMS;
pub use crate::request_types::{
    AccountsQuery, BrokerIdentifier, CsvValues, DecimalQuery, InstrumentSearchQuery,
    MarketHoursQuery, MarketsQuery, MoversQuery, OptionChainQuery, OptionExpirationQuery,
    OrdersQuery, PathIdentifier, PriceHistoryQuery, QueryExtensions, QueryText, QuotesQuery,
    ReadRequestError, TransactionsQuery,
};

const MAX_REQUEST_TARGET_BYTES: usize = 16_384;

/// Finite set of read operations exposed by this REST slice. There is no
/// caller-controlled method, URL, path, query key, body, or mutation route.
/// 此 REST 范围内的有限只读操作集合。调用方不能指定 HTTP 方法、URL、路径、查询键、请求体或变更路由。
#[derive(Clone, Eq, PartialEq)]
pub enum ReadRequest {
    /// Reads account-number and account-hash mappings.
    /// 读取账户编号与账户哈希的映射。
    AccountNumbers,
    /// Reads accounts using the requested field selection.
    /// 按指定字段选择读取账户列表。
    Accounts(AccountsQuery),
    /// Reads one account using its validated hash and field selection.
    /// 使用已校验的账户哈希和字段选择读取单个账户。
    Account {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: AccountsQuery,
    },
    /// Reads orders for one account and time range.
    /// 按账户和时间范围读取订单。
    Orders {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: OrdersQuery,
    },
    /// Reads account orders and adds bounded, non-conflicting query fields.
    /// 读取账户订单，并附加有界且不冲突的查询字段。
    OrdersWithExtensions {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: OrdersQuery,
        /// Bounded additive query parameters that cannot replace route-owned keys.
        /// 有界附加查询参数；不能覆盖该路由自身的查询键。
        extensions: QueryExtensions,
    },
    /// Reads one order by its validated broker identifier.
    /// 按已校验的 broker 标识符读取单个订单。
    Order {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Validated broker order identifier encoded as one path segment.
        /// broker 订单标识符。
        order_id: BrokerIdentifier,
    },
    /// Reads orders across accounts for the supplied time range.
    /// 按给定时间范围读取跨账户订单。
    OrdersAcrossAccounts(OrdersQuery),
    /// Reads cross-account orders with bounded additive query fields.
    /// 读取跨账户订单，并附加有界查询字段。
    OrdersAcrossAccountsWithExtensions {
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: OrdersQuery,
        /// Bounded additive query parameters that cannot replace route-owned keys.
        /// 有界附加查询参数；不能覆盖该路由自身的查询键。
        extensions: QueryExtensions,
    },
    /// Reads transactions for one account and time range.
    /// 按账户和时间范围读取交易记录。
    Transactions {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: TransactionsQuery,
    },
    /// Reads account transactions with bounded additive query fields.
    /// 读取账户交易记录，并附加有界查询字段。
    TransactionsWithExtensions {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: TransactionsQuery,
        /// Bounded additive query parameters that cannot replace route-owned keys.
        /// 有界附加查询参数；不能覆盖该路由自身的查询键。
        extensions: QueryExtensions,
    },
    /// Reads one transaction by its validated broker identifier.
    /// 按已校验的 broker 标识符读取单笔交易记录。
    Transaction {
        /// Validated account hash encoded as this route’s account path segment.
        /// 账户哈希；其包装类型会在 Debug 输出中脱敏。
        account_hash: PathIdentifier,
        /// Validated broker transaction identifier encoded as one path segment.
        /// broker 交易记录标识符。
        transaction_id: BrokerIdentifier,
    },
    /// Reads the user-preference resource.
    /// 读取用户偏好资源。
    UserPreferences,
    /// Reads streamer bootstrap metadata from the read-only preference route.
    /// 从只读偏好路由读取 streamer 启动元数据。
    StreamerInfo,
    /// Reads quotes for a validated symbol list.
    /// 读取已校验交易代码列表的报价。
    Quotes(QuotesQuery),
    /// Reads a quote using the symbol as one encoded path segment.
    /// 将交易代码作为单个编码路径段读取报价。
    Quote {
        /// Validated quote symbol encoded as one path segment.
        /// 编码为报价路由单个路径段的已校验交易代码。
        symbol: PathIdentifier,
        /// Optional CSV quote-field projection forwarded to the broker.
        /// 转发给 broker 的可选逗号分隔报价字段投影。
        fields: Option<CsvValues>,
    },
    /// Reads normalized option quotes for the supplied symbol list.
    /// 读取给定交易代码列表对应的规范化期权报价。
    OptionQuotes {
        /// Bounded option-symbol list supplied to the normalized quote route.
        /// 传入规范化报价路由的有界期权代码列表。
        symbols: CsvValues,
        /// Optional CSV quote-field projection forwarded to the broker.
        /// 转发给 broker 的可选逗号分隔报价字段投影。
        fields: Option<CsvValues>,
    },
    /// Reads a two-leg option quote while preserving long/short order.
    /// 按多头腿、空头腿的顺序读取双腿期权报价。
    VerticalOptionQuote {
        /// Option symbol for the caller-designated long leg.
        /// 调用方指定多头腿的期权代码。
        long_symbol: QueryText,
        /// Option symbol for the caller-designated short leg.
        /// 调用方指定空头腿的期权代码。
        short_symbol: QueryText,
    },
    /// Reads the option chain for one symbol.
    /// 读取单个交易代码的期权链。
    OptionChains(Box<OptionChainQuery>),
    /// Reads expiration dates and filters for one option symbol.
    /// 读取单个期权交易代码的到期日及筛选信息。
    OptionExpirationChain(OptionExpirationQuery),
    /// Reads historical prices using the supplied interval and session filters.
    /// 按给定周期和交易时段筛选条件读取历史价格。
    PriceHistory(PriceHistoryQuery),
    /// Reads market movers for one validated symbol or market identifier.
    /// 按一个已校验的交易代码或市场标识符读取涨跌幅榜。
    Movers {
        /// Validated instrument symbol or market identifier encoded by this route.
        /// 编码到该路由路径中的已校验交易代码或市场标识符。
        symbol: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: MoversQuery,
    },
    /// Reads hours for a selected set of markets.
    /// 读取所选市场的交易时间。
    Markets(MarketsQuery),
    /// Reads hours for one market and optional date.
    /// 读取单个市场及可选日期的交易时间。
    MarketHours {
        /// Validated market identifier encoded as one path segment.
        /// 市场路径标识或名称。
        market: PathIdentifier,
        /// Typed query values serialized into this route’s request target.
        /// 序列化到该路由请求目标中的类型化查询值。
        query: MarketHoursQuery,
    },
    /// Searches instruments using symbols and an explicit projection.
    /// 使用交易代码和明确的投影类型搜索标的。
    SearchInstruments(InstrumentSearchQuery),
    /// Reads one instrument by its validated CUSIP path segment.
    /// 按已校验的 CUSIP 路径段读取单个标的。
    InstrumentByCusip(PathIdentifier),
}

impl ReadRequest {
    /// Builds an account-orders GET while preserving safe additive parameters
    /// accepted by Node's record-based `OrdersQuery` contract.
    /// 构造账户订单 GET 请求，并保留 Node 记录型 `OrdersQuery` 合约允许的安全附加参数。
    pub fn orders_with_extensions(
        account_hash: impl AsRef<str>,
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Result<Self, ReadRequestError> {
        Ok(Self::OrdersWithExtensions {
            account_hash: PathIdentifier::new(account_hash)?,
            query,
            extensions,
        })
    }

    /// Builds a cross-account orders GET with safe additive parameters.
    /// 构造跨账户订单 GET 请求，并添加安全的附加查询参数。
    pub fn orders_across_accounts_with_extensions(
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Self {
        Self::OrdersAcrossAccountsWithExtensions { query, extensions }
    }

    /// Builds an account-transactions GET with safe additive parameters
    /// accepted by Node's record-based `TransactionsParams` contract.
    /// 构造账户交易 GET 请求，并保留 Node 记录型 `TransactionsParams` 合约允许的安全附加参数。
    pub fn transactions_with_extensions(
        account_hash: impl AsRef<str>,
        query: TransactionsQuery,
        extensions: QueryExtensions,
    ) -> Result<Self, ReadRequestError> {
        Ok(Self::TransactionsWithExtensions {
            account_hash: PathIdentifier::new(account_hash)?,
            query,
            extensions,
        })
    }

    /// Builds the current Node SDK option-quote request shape.
    ///
    /// `MarketDataApiClient.getOptionQuote(s)` trims trailing whitespace from
    /// each symbol, discards empty symbols, rejects an empty result or
    /// duplicates after trimming, and defaults `fields` to `quote,reference`.
    /// This constructor preserves that request contract while keeping the
    /// lower-level `Quotes` request available for callers of generic
    /// `getQuotes`.
    /// 此构造器保留该请求约定，同时继续提供底层 `Quotes` 请求供通用 `getQuotes` 调用方使用。
    pub fn option_quotes<I, S>(symbols: I) -> Result<Self, ReadRequestError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::option_quotes_with_fields(symbols, None)
    }

    /// Builds an option-quote request with the Node SDK's normalized symbols
    /// and default fields. Passing `None` selects `quote,reference`.
    /// 按 Node SDK 规则规范化交易代码并构造期权报价请求。传入 `None` 时默认字段为 `quote,reference`。
    pub fn option_quotes_with_fields<I, S>(
        symbols: I,
        fields: Option<CsvValues>,
    ) -> Result<Self, ReadRequestError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let normalized = normalize_option_quote_symbols(symbols)?;
        let symbols = CsvValues::from_values(normalized)?;
        let fields = match fields {
            Some(fields) => Some(fields),
            None => Some(CsvValues::from_values(["quote", "reference"])?),
        };
        Ok(Self::OptionQuotes { symbols, fields })
    }

    /// Builds the single-option convenience request with the same padding,
    /// empty-symbol, and default-field behavior as Node's `getOptionQuote`.
    /// 构造单个期权便捷请求，并保持与 Node `getOptionQuote` 相同的空白修剪、空值处理和默认字段行为。
    pub fn option_quote(symbol: impl AsRef<str>) -> Result<Self, ReadRequestError> {
        Self::option_quotes([symbol])
    }

    /// Builds the two-leg quote request used by Node's
    /// `getVerticalOptionQuote`, preserving caller order and its option-quote
    /// request defaults.
    /// 构造 Node `getVerticalOptionQuote` 使用的双腿报价请求，保留调用方顺序和期权报价默认值。
    pub fn vertical_option_quote(
        long_symbol: impl AsRef<str>,
        short_symbol: impl AsRef<str>,
    ) -> Result<Self, ReadRequestError> {
        let normalized = [
            trim_option_quote_symbol(long_symbol.as_ref()),
            trim_option_quote_symbol(short_symbol.as_ref()),
        ];
        if normalized.iter().any(|symbol| symbol.is_empty()) || normalized[0] == normalized[1] {
            return Err(ReadRequestError::InvalidList);
        }
        Ok(Self::VerticalOptionQuote {
            long_symbol: QueryText::new(normalized[0].to_owned())?,
            short_symbol: QueryText::new(normalized[1].to_owned())?,
        })
    }

    pub(crate) fn endpoint(&self) -> Result<ReadEndpoint, ReadRequestError> {
        let mut query: Vec<(String, String)> = Vec::new();
        let (route_name, mut path) = match self {
            Self::AccountNumbers => (
                "trader-account-numbers",
                "/trader/v1/accounts/accountNumbers".to_owned(),
            ),
            Self::Accounts(params) => {
                push_text(&mut query, "fields", params.fields.as_ref());
                ("trader-accounts", "/trader/v1/accounts".to_owned())
            }
            Self::Account {
                account_hash,
                query: params,
            } => {
                push_text(&mut query, "fields", params.fields.as_ref());
                (
                    "trader-account",
                    format!("/trader/v1/accounts/{}", account_hash.encoded()),
                )
            }
            Self::Orders {
                account_hash,
                query: params,
            } => {
                append_orders_query(&mut query, params);
                (
                    "trader-orders",
                    format!("/trader/v1/accounts/{}/orders", account_hash.encoded()),
                )
            }
            Self::OrdersWithExtensions {
                account_hash,
                query: params,
                extensions,
            } => {
                append_orders_query(&mut query, params);
                append_extensions(
                    &mut query,
                    extensions,
                    &["fromEnteredTime", "toEnteredTime", "maxResults", "status"],
                )?;
                (
                    "trader-orders",
                    format!("/trader/v1/accounts/{}/orders", account_hash.encoded()),
                )
            }
            Self::Order {
                account_hash,
                order_id,
            } => (
                "trader-order",
                format!(
                    "/trader/v1/accounts/{}/orders/{}",
                    account_hash.encoded(),
                    order_id.encoded()
                ),
            ),
            Self::OrdersAcrossAccounts(params) => {
                append_orders_query(&mut query, params);
                (
                    "trader-orders-across-accounts",
                    "/trader/v1/orders".to_owned(),
                )
            }
            Self::OrdersAcrossAccountsWithExtensions {
                query: params,
                extensions,
            } => {
                append_orders_query(&mut query, params);
                append_extensions(
                    &mut query,
                    extensions,
                    &["fromEnteredTime", "toEnteredTime", "maxResults", "status"],
                )?;
                (
                    "trader-orders-across-accounts",
                    "/trader/v1/orders".to_owned(),
                )
            }
            Self::Transactions {
                account_hash,
                query: params,
            } => {
                append_transactions_query(&mut query, params);
                (
                    "trader-transactions",
                    format!(
                        "/trader/v1/accounts/{}/transactions",
                        account_hash.encoded()
                    ),
                )
            }
            Self::TransactionsWithExtensions {
                account_hash,
                query: params,
                extensions,
            } => {
                append_transactions_query(&mut query, params);
                append_extensions(
                    &mut query,
                    extensions,
                    &["startDate", "endDate", "types", "symbol"],
                )?;
                (
                    "trader-transactions",
                    format!(
                        "/trader/v1/accounts/{}/transactions",
                        account_hash.encoded()
                    ),
                )
            }
            Self::Transaction {
                account_hash,
                transaction_id,
            } => (
                "trader-transaction",
                format!(
                    "/trader/v1/accounts/{}/transactions/{}",
                    account_hash.encoded(),
                    transaction_id.encoded()
                ),
            ),
            Self::UserPreferences | Self::StreamerInfo => (
                "trader-user-preferences",
                "/trader/v1/userPreference".to_owned(),
            ),
            Self::Quotes(params) => {
                ensure_nonempty_csv(&params.symbols)?;
                push_pair(&mut query, "symbols", params.symbols.as_str());
                push_pair_option(
                    &mut query,
                    "fields",
                    params.fields.as_ref().map(CsvValues::as_str),
                );
                push_bool(&mut query, "indicative", params.indicative);
                ("market-quotes", "/marketdata/v1/quotes".to_owned())
            }
            Self::Quote { symbol, fields } => {
                push_pair_option(&mut query, "fields", fields.as_ref().map(CsvValues::as_str));
                (
                    "market-quote",
                    format!("/marketdata/v1/{}/quotes", symbol.encoded()),
                )
            }
            Self::OptionQuotes { symbols, fields } => {
                ensure_nonempty_csv(symbols)?;
                push_pair(&mut query, "symbols", symbols.as_str());
                push_pair_option(&mut query, "fields", fields.as_ref().map(CsvValues::as_str));
                ("market-option-quotes", "/marketdata/v1/quotes".to_owned())
            }
            Self::VerticalOptionQuote {
                long_symbol,
                short_symbol,
            } => {
                ensure_required_text(long_symbol)?;
                ensure_required_text(short_symbol)?;
                let long_symbol = trim_option_quote_symbol(long_symbol.as_str());
                let short_symbol = trim_option_quote_symbol(short_symbol.as_str());
                if long_symbol.is_empty() || short_symbol.is_empty() || long_symbol == short_symbol
                {
                    return Err(ReadRequestError::InvalidList);
                }
                let symbols = format!("{long_symbol},{short_symbol}");
                push_pair(&mut query, "symbols", &symbols);
                push_pair(&mut query, "fields", "quote,reference");
                (
                    "market-vertical-option-quote",
                    "/marketdata/v1/quotes".to_owned(),
                )
            }
            Self::OptionChains(params) => {
                ensure_required_text(&params.symbol)?;
                append_option_chain_query(&mut query, params);
                ("market-option-chains", "/marketdata/v1/chains".to_owned())
            }
            Self::OptionExpirationChain(params) => {
                ensure_required_text(&params.symbol)?;
                push_text(&mut query, "symbol", Some(&params.symbol));
                push_text(&mut query, "contractType", params.contract_type.as_ref());
                push_text(&mut query, "expMonth", params.exp_month.as_ref());
                push_text(&mut query, "optionType", params.option_type.as_ref());
                (
                    "market-option-expiration-chain",
                    "/marketdata/v1/expirationchain".to_owned(),
                )
            }
            Self::PriceHistory(params) => {
                ensure_required_text(&params.symbol)?;
                push_text(&mut query, "symbol", Some(&params.symbol));
                push_text(&mut query, "periodType", params.period_type.as_ref());
                push_i64(&mut query, "period", params.period);
                push_text(&mut query, "frequencyType", params.frequency_type.as_ref());
                push_i64(&mut query, "frequency", params.frequency);
                push_i64(&mut query, "startDate", params.start_date);
                push_i64(&mut query, "endDate", params.end_date);
                push_bool(
                    &mut query,
                    "needExtendedHoursData",
                    params.need_extended_hours_data,
                );
                push_bool(&mut query, "needPreviousClose", params.need_previous_close);
                (
                    "market-price-history",
                    "/marketdata/v1/pricehistory".to_owned(),
                )
            }
            Self::Movers {
                symbol,
                query: params,
            } => {
                push_text(&mut query, "sort", params.sort.as_ref());
                push_i64(&mut query, "frequency", params.frequency);
                (
                    "market-movers",
                    format!("/marketdata/v1/movers/{}", symbol.encoded()),
                )
            }
            Self::Markets(params) => {
                ensure_nonempty_csv(&params.markets)?;
                push_pair(&mut query, "markets", params.markets.as_str());
                push_text(&mut query, "date", params.date.as_ref());
                ("market-hours-batch", "/marketdata/v1/markets".to_owned())
            }
            Self::MarketHours {
                market,
                query: params,
            } => {
                push_text(&mut query, "date", params.date.as_ref());
                (
                    "market-hours",
                    format!("/marketdata/v1/markets/{}", market.encoded()),
                )
            }
            Self::SearchInstruments(params) => {
                ensure_required_text(&params.projection)?;
                ensure_nonempty_csv(&params.symbols)?;
                push_pair(&mut query, "symbol", params.symbols.as_str());
                push_text(&mut query, "projection", Some(&params.projection));
                (
                    "market-instrument-search",
                    "/marketdata/v1/instruments".to_owned(),
                )
            }
            Self::InstrumentByCusip(cusip) => (
                "market-instrument-by-cusip",
                format!("/marketdata/v1/instruments/{}", cusip.encoded()),
            ),
        };

        if query.len() > MAX_QUERY_ITEMS {
            return Err(ReadRequestError::TooManyQueryParameters);
        }
        if !query.is_empty() {
            path.push('?');
            for (index, (key, value)) in query.iter().enumerate() {
                if index > 0 {
                    path.push('&');
                }
                path.push_str(&encode_query_component(key.as_bytes()));
                path.push('=');
                path.push_str(&encode_query_component(value.as_bytes()));
            }
        }
        if path.len() > MAX_REQUEST_TARGET_BYTES || !path.starts_with('/') {
            return Err(ReadRequestError::TargetTooLong);
        }
        Ok(ReadEndpoint::allowlisted(route_name, path))
    }
}

impl fmt::Debug for ReadRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoint = self.endpoint();
        match endpoint {
            Ok(endpoint) => formatter
                .debug_struct("ReadRequest")
                .field("route", &endpoint.route_name())
                .field("target", &"[REDACTED]")
                .finish(),
            Err(error) => formatter
                .debug_struct("ReadRequest")
                .field("route", &"invalid")
                .field("error", &error)
                .finish(),
        }
    }
}

fn ensure_required_text(value: &QueryText) -> Result<(), ReadRequestError> {
    if value.as_str().trim().is_empty() {
        return Err(ReadRequestError::InvalidQueryValue);
    }
    Ok(())
}

fn ensure_nonempty_csv(value: &CsvValues) -> Result<(), ReadRequestError> {
    if value.as_str().trim().is_empty() {
        return Err(ReadRequestError::InvalidList);
    }
    Ok(())
}

/// Matches ECMAScript `String.prototype.trimEnd`, including BOM and excluding
/// NEXT LINE, for Node option-quote symbol normalization.
pub(crate) fn trim_option_quote_symbol(value: &str) -> &str {
    value.trim_end_matches(|character| {
        matches!(
            character,
            '\u{0009}'
                | '\u{000A}'
                | '\u{000B}'
                | '\u{000C}'
                | '\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'
                ..='\u{200A}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202F}'
                    | '\u{205F}'
                    | '\u{3000}'
                    | '\u{FEFF}'
        )
    })
}

pub(crate) fn normalize_option_quote_symbols<I, S>(
    symbols: I,
) -> Result<Vec<String>, ReadRequestError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = trim_option_quote_symbol(symbol.as_ref());
        if symbol.is_empty() {
            continue;
        }
        if normalized.iter().any(|existing| existing == symbol) {
            return Err(ReadRequestError::InvalidList);
        }
        normalized.push(symbol.to_owned());
        if normalized.len() > MAX_QUERY_ITEMS {
            return Err(ReadRequestError::InvalidList);
        }
    }
    if normalized.is_empty() {
        return Err(ReadRequestError::InvalidList);
    }
    Ok(normalized)
}

fn append_extensions(
    query: &mut Vec<(String, String)>,
    extensions: &QueryExtensions,
    reserved_keys: &[&str],
) -> Result<(), ReadRequestError> {
    if query.len().saturating_add(extensions.len()) > MAX_QUERY_ITEMS {
        return Err(ReadRequestError::TooManyQueryParameters);
    }
    for (key, value) in extensions.iter() {
        if reserved_keys.contains(&key) {
            return Err(ReadRequestError::ConflictingQueryParameter);
        }
        push_pair(query, key, value);
    }
    Ok(())
}
