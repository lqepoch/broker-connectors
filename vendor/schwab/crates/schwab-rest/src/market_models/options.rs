//! Option-chain and option-expiration response projections.
//! 定义期权链和期权到期日响应投影。

use std::collections::BTreeMap;

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    object, optional_boolean, optional_number, optional_string, required_number, required_string,
    unknown_fields,
};
use crate::response_models::{UnknownFields, WireNumber};

#[derive(Clone, PartialEq)]
/// Typed projection of the broker option-chain response, including call/put maps keyed by expiration and strike.
/// 中文摘要：broker 期权链响应的类型化投影，包含按到期日和行权价索引的 call/put 合约。
pub struct OptionChainResponse {
    /// Underlying symbol associated with the returned option chain.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Optional broker status for the chain response.
    /// broker 或服务返回的状态文本。
    pub status: Option<String>,
    /// Broker delay indicator; this does not establish freshness.
    /// 行情是否为延迟数据。
    pub is_delayed: Option<bool>,
    /// Whether the underlying is classified as an index.
    /// 该标的是否为指数。
    pub is_index: Option<bool>,
    /// Underlying price included with the option chain.
    /// 期权链或报价所对应的标的价格。
    pub underlying_price: Option<WireNumber>,
    /// Call contracts indexed by expiration and strike keys.
    /// call 合约到期日映射。
    pub call_exp_date_map: Option<BTreeMap<String, BTreeMap<String, OptionContract>>>,
    /// Put contracts indexed by expiration and strike keys.
    /// 按到期日和行权价索引的 put 合约。
    pub put_exp_date_map: Option<BTreeMap<String, BTreeMap<String, OptionContract>>>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// Typed broker fields for one option contract; unmodeled properties remain in `unknown_fields`.
/// 中文摘要：单个期权合约的 broker 类型化字段；未建模属性保留在 `unknown_fields`。
pub struct OptionContract {
    /// Broker call/put classification for this contract.
    /// 期权方向标记。
    pub put_call: Option<String>,
    /// Broker option-contract symbol.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Broker-reported bid price for the contract.
    /// 买方报价价格。
    pub bid_price: Option<WireNumber>,
    /// Broker-reported ask price for the contract.
    /// 卖方报价价格。
    pub ask_price: Option<WireNumber>,
    /// Broker-reported mark price for the contract.
    /// 期权合约 mark 价格。
    pub mark_price: Option<WireNumber>,
    /// Contract strike price.
    /// 期权行权价。
    pub strike_price: Option<WireNumber>,
    /// Contract expiration date as returned by the broker.
    /// 期权到期日期。
    pub expiration_date: Option<String>,
    /// Contract multiplier reported by the broker.
    /// 合约乘数。
    pub multiplier: Option<WireNumber>,
    /// Whether the broker identifies the contract as non-standard.
    /// 合约是否为非标准合约。
    pub is_non_standard: Option<bool>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// Typed projection of the broker response listing option expiration dates.
/// 中文摘要：broker 期权到期日列表响应的类型化投影。
pub struct OptionExpirationChainResponse {
    /// Underlying symbol associated with the expiration response.
    /// 标的或合约交易代码。
    pub symbol: Option<String>,
    /// Broker delay indicator; this does not establish freshness.
    /// 行情是否为延迟数据。
    pub is_delayed: Option<bool>,
    /// Whether the underlying is classified as an index.
    /// 该标的是否为指数。
    pub is_index: Option<bool>,
    /// Underlying price included with the expiration response.
    /// 期权链或报价所对应的标的价格。
    pub underlying_price: Option<WireNumber>,
    /// Expiration rows returned by the broker.
    /// broker 返回的到期日记录。
    pub expiration_list: Option<Vec<OptionExpiration>>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

#[derive(Clone, PartialEq)]
/// One broker-reported expiration row with optional classification fields.
/// 中文摘要：一条 broker 报告的到期日记录及其可选分类字段。
pub struct OptionExpiration {
    /// Expiration date reported by the broker.
    /// 期权到期日期。
    pub expiration_date: String,
    /// Broker-reported days remaining until expiration.
    /// broker 报告的到期剩余天数。
    pub days_to_expiration: WireNumber,
    /// Optional broker classification of the expiration.
    /// 到期日类别。
    pub expiration_type: Option<String>,
    /// Whether the broker marks the contract as standard.
    /// 该合约是否为标准合约。
    pub standard: Option<bool>,
    /// Additional response properties retained outside the typed projection; they do not establish authority.
    /// 保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_option_chain(
    value: &Value,
) -> Result<OptionChainResponse, ReadResponseError> {
    let fields = object(value, "optionChain")?;
    Ok(OptionChainResponse {
        symbol: optional_string(fields, "symbol", "optionChain.symbol")?,
        status: optional_string(fields, "status", "optionChain.status")?,
        is_delayed: optional_boolean(fields, "isDelayed", "optionChain.isDelayed")?,
        is_index: optional_boolean(fields, "isIndex", "optionChain.isIndex")?,
        underlying_price: optional_number(
            fields,
            "underlyingPrice",
            "optionChain.underlyingPrice",
        )?,
        call_exp_date_map: fields
            .get("callExpDateMap")
            .map(|value| project_option_contract_map(value, "optionChain.callExpDateMap"))
            .transpose()?,
        put_exp_date_map: fields
            .get("putExpDateMap")
            .map(|value| project_option_contract_map(value, "optionChain.putExpDateMap"))
            .transpose()?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "symbol",
                "status",
                "isDelayed",
                "isIndex",
                "underlyingPrice",
                "callExpDateMap",
                "putExpDateMap",
            ],
        ),
    })
}

fn project_option_contract_map(
    value: &Value,
    path: &'static str,
) -> Result<BTreeMap<String, BTreeMap<String, OptionContract>>, ReadResponseError> {
    let expirations = object(value, path)?;
    expirations
        .iter()
        .map(|(expiration, strikes)| {
            let strikes = object(strikes, "optionChain.expiration")?;
            let contracts = strikes
                .iter()
                .map(|(strike, contract)| Ok((strike.clone(), project_option_contract(contract)?)))
                .collect::<Result<_, ReadResponseError>>()?;
            Ok((expiration.clone(), contracts))
        })
        .collect()
}

fn project_option_contract(value: &Value) -> Result<OptionContract, ReadResponseError> {
    let fields = object(value, "optionChain.contract")?;
    Ok(OptionContract {
        put_call: optional_string(fields, "putCall", "optionChain.contract.putCall")?,
        symbol: optional_string(fields, "symbol", "optionChain.contract.symbol")?,
        bid_price: optional_number(fields, "bidPrice", "optionChain.contract.bidPrice")?,
        ask_price: optional_number(fields, "askPrice", "optionChain.contract.askPrice")?,
        mark_price: optional_number(fields, "markPrice", "optionChain.contract.markPrice")?,
        strike_price: optional_number(fields, "strikePrice", "optionChain.contract.strikePrice")?,
        expiration_date: optional_string(
            fields,
            "expirationDate",
            "optionChain.contract.expirationDate",
        )?,
        multiplier: optional_number(fields, "multiplier", "optionChain.contract.multiplier")?,
        is_non_standard: optional_boolean(
            fields,
            "isNonStandard",
            "optionChain.contract.isNonStandard",
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "putCall",
                "symbol",
                "bidPrice",
                "askPrice",
                "markPrice",
                "strikePrice",
                "expirationDate",
                "multiplier",
                "isNonStandard",
            ],
        ),
    })
}

pub(super) fn project_option_expiration_chain(
    value: &Value,
) -> Result<OptionExpirationChainResponse, ReadResponseError> {
    let fields = object(value, "optionExpirationChain")?;
    let expiration_list = fields
        .get("expirationList")
        .map(|value| {
            let values = value.as_array().ok_or(ReadResponseError::SchemaViolation {
                field: "optionExpirationChain.expirationList",
            })?;
            values
                .iter()
                .map(project_option_expiration)
                .collect::<Result<_, _>>()
        })
        .transpose()?;
    Ok(OptionExpirationChainResponse {
        symbol: optional_string(fields, "symbol", "optionExpirationChain.symbol")?,
        is_delayed: optional_boolean(fields, "isDelayed", "optionExpirationChain.isDelayed")?,
        is_index: optional_boolean(fields, "isIndex", "optionExpirationChain.isIndex")?,
        underlying_price: optional_number(
            fields,
            "underlyingPrice",
            "optionExpirationChain.underlyingPrice",
        )?,
        expiration_list,
        unknown_fields: unknown_fields(
            fields,
            &[
                "symbol",
                "isDelayed",
                "isIndex",
                "underlyingPrice",
                "expirationList",
            ],
        ),
    })
}

fn project_option_expiration(value: &Value) -> Result<OptionExpiration, ReadResponseError> {
    let fields = object(value, "optionExpirationChain.expirationList[]")?;
    Ok(OptionExpiration {
        expiration_date: required_string(
            fields,
            "expirationDate",
            "optionExpirationChain.expirationDate",
        )?,
        days_to_expiration: required_number(
            fields,
            "daysToExpiration",
            "optionExpirationChain.daysToExpiration",
        )?,
        expiration_type: optional_string(
            fields,
            "expirationType",
            "optionExpirationChain.expirationType",
        )?,
        standard: optional_boolean(fields, "standard", "optionExpirationChain.standard")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "expirationDate",
                "daysToExpiration",
                "expirationType",
                "standard",
            ],
        ),
    })
}
