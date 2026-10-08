//! Account, balance, and position response projections.
//! 定义账户、余额和持仓响应投影。

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::ReadResponseError;
use super::common::project_instrument;
use super::common::{Instrument, UnknownFields, WireNumber};
use super::common::{
    boolean, number, object, optional_array, optional_object, required_string, string,
    unknown_fields, wire_number,
};

/// Account number and opaque account hash returned by Schwab.
/// 中文摘要：账户编号与用于账户路径的 opaque hash；hash 应按敏感标识处理并避免进入日志。
#[derive(Clone, PartialEq)]
pub struct AccountNumberHash {
    /// Account number as returned on the wire.
    /// 中文摘要：broker 返回的账户编号。
    pub account_number: String,
    /// Opaque hash used in account-scoped paths.
    /// 中文摘要：对应账户专用请求路径中的不透明账户哈希；应按敏感标识处理并避免写入日志。
    pub hash_value: String,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// One account wrapper returned by the Trader API.
/// 中文摘要：Trader 单账户响应封装，附带账户数据和未识别的 broker 字段。
#[derive(Clone, PartialEq)]
pub struct AccountResponse {
    /// The securities account payload.
    /// 中文摘要：broker 返回的证券账户细节。
    pub securities_account: SecuritiesAccount,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Account identity, positions, and balance groups.
/// 中文摘要：证券账户的身份、余额与持仓快照；响应不构成资金授权或交易权限证明。
#[derive(Clone, PartialEq)]
pub struct SecuritiesAccount {
    /// Account number.
    /// 中文摘要：broker 返回的账户编号。
    pub account_number: String,
    /// Broker account type, when present.
    /// 中文摘要：账户类型。
    pub account_type: Option<String>,
    /// Round trips reported by Schwab.
    /// 中文摘要：Schwab 报告的账户往返交易次数，原样保留精确数值。
    pub round_trips: Option<WireNumber>,
    /// Day-trader status.
    /// 中文摘要：Schwab 报告的日内交易者标记；缺失不表示否。
    pub is_day_trader: Option<bool>,
    /// Closing-only restriction status.
    /// 中文摘要：账户是否受仅可平仓限制；这是响应状态，不是本地交易授权。
    pub is_closing_only_restricted: Option<bool>,
    /// Portfolio-cash flag.
    /// 中文摘要：Schwab 报告的 PFCB 标记；保留 broker 原值，不据此推导资金权限。
    pub pfcb_flag: Option<bool>,
    /// Positions included when the request asks for them.
    /// 中文摘要：账户持仓列表。
    pub positions: Option<Vec<Position>>,
    /// Initial balance values.
    /// 中文摘要：账户初始余额快照；其中未提供的余额项仍保持缺失。
    pub initial_balances: Option<BalanceSnapshot>,
    /// Current balance values.
    /// 中文摘要：Schwab 返回的当前余额快照；数据本身不授予下单或资金权限。
    pub current_balances: Option<BalanceSnapshot>,
    /// Projected balance values.
    /// 中文摘要：Schwab 返回的预测余额快照；它不是已结算现金或资金授权。
    pub projected_balances: Option<BalanceSnapshot>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Position quantities, values, and its instrument.
/// 中文摘要：单个持仓的数量、价格、价值和关联标的投影。
#[derive(Clone, PartialEq)]
pub struct Position {
    /// Short quantity.
    /// 中文摘要：空头数量。
    pub short_quantity: Option<WireNumber>,
    /// Average price.
    /// 中文摘要：平均成本价格。
    pub average_price: Option<WireNumber>,
    /// Current-day profit/loss.
    /// 中文摘要：当日盈亏。
    pub current_day_profit_loss: Option<WireNumber>,
    /// Current-day profit/loss percentage.
    /// 中文摘要：当日盈亏百分比。
    pub current_day_profit_loss_percentage: Option<WireNumber>,
    /// Long quantity.
    /// 中文摘要：多头数量。
    pub long_quantity: Option<WireNumber>,
    /// Settled long quantity.
    /// 中文摘要：已结算多头持仓数量，按原始精确数值保留。
    pub settled_long_quantity: Option<WireNumber>,
    /// Settled short quantity.
    /// 中文摘要：已结算空头持仓数量，按原始精确数值保留。
    pub settled_short_quantity: Option<WireNumber>,
    /// Aged quantity.
    /// 中文摘要：Schwab 标记为 aged 的持仓数量；该字段不改写其他数量字段。
    pub aged_quantity: Option<WireNumber>,
    /// Instrument identity.
    /// 中文摘要：该持仓或订单关联的证券标的。
    pub instrument: Option<Instrument>,
    /// Market value.
    /// 中文摘要：市场价值。
    pub market_value: Option<WireNumber>,
    /// Maintenance requirement.
    /// 中文摘要：该持仓对应的维持保证金要求，使用无损 wire 数值表示。
    pub maintenance_requirement: Option<WireNumber>,
    /// Average long price.
    /// 中文摘要：多头持仓平均价格，保留 broker 返回的精确数值。
    pub average_long_price: Option<WireNumber>,
    /// Average short price.
    /// 中文摘要：空头持仓平均价格，保留 broker 返回的精确数值。
    pub average_short_price: Option<WireNumber>,
    /// Tax-lot average long price.
    /// 中文摘要：按 tax lot 计算的多头平均价格。
    pub tax_lot_average_long_price: Option<WireNumber>,
    /// Tax-lot average short price.
    /// 中文摘要：按 tax lot 计算的空头平均价格。
    pub tax_lot_average_short_price: Option<WireNumber>,
    /// Long open profit/loss.
    /// 中文摘要：多头持仓未平仓盈亏金额。
    pub long_open_profit_loss: Option<WireNumber>,
    /// Short open profit/loss.
    /// 中文摘要：空头持仓未平仓盈亏金额。
    pub short_open_profit_loss: Option<WireNumber>,
    /// Previous-session long quantity.
    /// 中文摘要：前一交易时段多头数量。
    pub previous_session_long_quantity: Option<WireNumber>,
    /// Previous-session short quantity.
    /// 中文摘要：前一交易时段空头数量。
    pub previous_session_short_quantity: Option<WireNumber>,
    /// Current-day cost.
    /// 中文摘要：当前交易日计入的持仓成本金额。
    pub current_day_cost: Option<WireNumber>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Numeric account balances, with unrecognized values preserved separately.
/// 中文摘要：按 broker 字段名索引的精确余额数值，并单独保留未知字段。
#[derive(Clone, PartialEq)]
pub struct BalanceSnapshot {
    pub(super) values: BTreeMap<String, WireNumber>,
    pub(super) unknown_fields: UnknownFields,
}

impl BalanceSnapshot {
    /// Returns an exact wire number for a recognized balance key.
    /// 中文摘要：读取
    pub fn get(&self, key: &str) -> Option<&WireNumber> {
        self.values.get(key)
    }

    /// Iterates recognized balance keys and their exact wire numbers.
    /// 中文摘要：只迭代已识别的余额键及其精确 wire 数值；未知值通过独立访问器读取。
    pub fn values(&self) -> impl Iterator<Item = (&str, &WireNumber)> {
        self.values.iter().map(|(key, value)| (key.as_str(), value))
    }

    /// Returns unrecognized balance values without interpreting them.
    /// 中文摘要：返回未识别的余额字段，不为其推断资金含义或 authority。
    pub const fn unknown_fields(&self) -> &UnknownFields {
        &self.unknown_fields
    }
}

pub(super) fn project_account_number_hash(
    value: &Value,
) -> Result<AccountNumberHash, ReadResponseError> {
    let fields = object(value, "accountNumbers[]")?;
    Ok(AccountNumberHash {
        account_number: required_string(fields, "accountNumber", "accountNumbers[].accountNumber")?,
        hash_value: required_string(fields, "hashValue", "accountNumbers[].hashValue")?,
        unknown_fields: unknown_fields(fields, &["accountNumber", "hashValue"]),
    })
}

pub(super) fn project_account(value: &Value) -> Result<AccountResponse, ReadResponseError> {
    let fields = object(value, "account")?;
    let security = fields
        .get("securitiesAccount")
        .ok_or(ReadResponseError::SchemaViolation {
            field: "securitiesAccount",
        })?;
    Ok(AccountResponse {
        securities_account: project_securities_account(security)?,
        unknown_fields: unknown_fields(fields, &["securitiesAccount"]),
    })
}

fn project_securities_account(value: &Value) -> Result<SecuritiesAccount, ReadResponseError> {
    let fields = object(value, "securitiesAccount")?;
    Ok(SecuritiesAccount {
        account_number: required_string(
            fields,
            "accountNumber",
            "securitiesAccount.accountNumber",
        )?,
        account_type: string(fields, "type", "securitiesAccount.type")?,
        round_trips: number(fields, "roundTrips", "securitiesAccount.roundTrips")?,
        is_day_trader: boolean(fields, "isDayTrader", "securitiesAccount.isDayTrader")?,
        is_closing_only_restricted: boolean(
            fields,
            "isClosingOnlyRestricted",
            "securitiesAccount.isClosingOnlyRestricted",
        )?,
        pfcb_flag: boolean(fields, "pfcbFlag", "securitiesAccount.pfcbFlag")?,
        positions: optional_array(
            fields,
            "positions",
            "securitiesAccount.positions",
            project_position,
        )?,
        initial_balances: balance(
            fields,
            "initialBalances",
            "securitiesAccount.initialBalances",
        )?,
        current_balances: balance(
            fields,
            "currentBalances",
            "securitiesAccount.currentBalances",
        )?,
        projected_balances: balance(
            fields,
            "projectedBalances",
            "securitiesAccount.projectedBalances",
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "accountNumber",
                "type",
                "roundTrips",
                "isDayTrader",
                "isClosingOnlyRestricted",
                "pfcbFlag",
                "positions",
                "initialBalances",
                "currentBalances",
                "projectedBalances",
            ],
        ),
    })
}

fn project_position(value: &Value) -> Result<Position, ReadResponseError> {
    let fields = object(value, "securitiesAccount.positions[]")?;
    let known = [
        "shortQuantity",
        "averagePrice",
        "currentDayProfitLoss",
        "currentDayProfitLossPercentage",
        "longQuantity",
        "settledLongQuantity",
        "settledShortQuantity",
        "agedQuantity",
        "instrument",
        "marketValue",
        "maintenanceRequirement",
        "averageLongPrice",
        "averageShortPrice",
        "taxLotAverageLongPrice",
        "taxLotAverageShortPrice",
        "longOpenProfitLoss",
        "shortOpenProfitLoss",
        "previousSessionLongQuantity",
        "previousSessionShortQuantity",
        "currentDayCost",
    ];
    Ok(Position {
        short_quantity: number(fields, "shortQuantity", "position.shortQuantity")?,
        average_price: number(fields, "averagePrice", "position.averagePrice")?,
        current_day_profit_loss: number(
            fields,
            "currentDayProfitLoss",
            "position.currentDayProfitLoss",
        )?,
        current_day_profit_loss_percentage: number(
            fields,
            "currentDayProfitLossPercentage",
            "position.currentDayProfitLossPercentage",
        )?,
        long_quantity: number(fields, "longQuantity", "position.longQuantity")?,
        settled_long_quantity: number(
            fields,
            "settledLongQuantity",
            "position.settledLongQuantity",
        )?,
        settled_short_quantity: number(
            fields,
            "settledShortQuantity",
            "position.settledShortQuantity",
        )?,
        aged_quantity: number(fields, "agedQuantity", "position.agedQuantity")?,
        instrument: optional_object(
            fields,
            "instrument",
            "position.instrument",
            project_instrument,
        )?,
        market_value: number(fields, "marketValue", "position.marketValue")?,
        maintenance_requirement: number(
            fields,
            "maintenanceRequirement",
            "position.maintenanceRequirement",
        )?,
        average_long_price: number(fields, "averageLongPrice", "position.averageLongPrice")?,
        average_short_price: number(fields, "averageShortPrice", "position.averageShortPrice")?,
        tax_lot_average_long_price: number(
            fields,
            "taxLotAverageLongPrice",
            "position.taxLotAverageLongPrice",
        )?,
        tax_lot_average_short_price: number(
            fields,
            "taxLotAverageShortPrice",
            "position.taxLotAverageShortPrice",
        )?,
        long_open_profit_loss: number(fields, "longOpenProfitLoss", "position.longOpenProfitLoss")?,
        short_open_profit_loss: number(
            fields,
            "shortOpenProfitLoss",
            "position.shortOpenProfitLoss",
        )?,
        previous_session_long_quantity: number(
            fields,
            "previousSessionLongQuantity",
            "position.previousSessionLongQuantity",
        )?,
        previous_session_short_quantity: number(
            fields,
            "previousSessionShortQuantity",
            "position.previousSessionShortQuantity",
        )?,
        current_day_cost: number(fields, "currentDayCost", "position.currentDayCost")?,
        unknown_fields: unknown_fields(fields, &known),
    })
}

const BALANCE_KEYS: &[&str] = &[
    "accruedInterest",
    "availableFundsNonMarginableTrade",
    "bondValue",
    "buyingPower",
    "cashBalance",
    "cashAvailableForTrading",
    "cashReceipts",
    "dayTradingBuyingPower",
    "dayTradingBuyingPowerCall",
    "dayTradingEquityCall",
    "equity",
    "equityPercentage",
    "liquidationValue",
    "longMarginValue",
    "longOptionMarketValue",
    "longStockValue",
    "maintenanceCall",
    "maintenanceRequirement",
    "margin",
    "marginEquity",
    "moneyMarketFund",
    "mutualFundValue",
    "regTCall",
    "shortMarginValue",
    "shortOptionMarketValue",
    "shortStockValue",
    "totalCash",
    "isInCall",
    "unsettledCash",
    "pendingDeposits",
    "marginBalance",
    "shortBalance",
    "accountValue",
    "availableFunds",
    "buyingPowerNonMarginableTrade",
    "sma",
    "stockBuyingPower",
    "optionBuyingPower",
];

fn balance(
    fields: &Map<String, Value>,
    key: &str,
    path: &'static str,
) -> Result<Option<BalanceSnapshot>, ReadResponseError> {
    let Some(value) = fields.get(key) else {
        return Ok(None);
    };
    let balances = object(value, path)?;
    let mut values = BTreeMap::new();
    for key in BALANCE_KEYS {
        if let Some(value) = balances.get(*key) {
            values.insert(
                (*key).to_owned(),
                wire_number(value, "securitiesAccount.balance.number")?,
            );
        }
    }
    Ok(Some(BalanceSnapshot {
        values,
        unknown_fields: unknown_fields(balances, BALANCE_KEYS),
    }))
}
