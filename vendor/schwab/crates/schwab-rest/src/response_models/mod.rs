//! Lossless typed projections for current Trader account, order, transaction,
//! and user-preference response families.
//!
//! These models represent syntactically validated response bytes. They are
//! not proof that a response is a current or authoritative account snapshot.
//! 定义 Schwab Trader 读取响应的类型化投影。

mod accounts;
mod common;
mod orders;
mod preferences;
mod transactions;

pub use accounts::{
    AccountNumberHash, AccountResponse, BalanceSnapshot, Position, SecuritiesAccount,
};
pub use common::{Instrument, UnknownFields, WireNumber};
pub use orders::{BrokerAccountNumber, ExecutionLeg, Order, OrderActivity, OrderLeg};
pub use preferences::{
    OfferInfo, StreamerInfo, UserPreference, UserPreferenceAccount, UserPreferencesResponse,
};
pub use transactions::{
    Transaction, TransactionResponse, TransactionTransferItem, TransactionUser,
};

use std::fmt;

use serde_json::Value;

use crate::read_response::{ReadResponseError, ReadResponseKind};

/// A typed Trader response family selected by the allow-listed GET route.
/// 中文摘要：Trader 只读端点对应的类型化响应集合。
#[derive(Clone, PartialEq)]
pub enum TraderReadResponse {
    /// `GET /accounts/accountNumbers` response rows.
    /// 账户编号映射的类型化投影。
    AccountNumbers(Vec<AccountNumberHash>),
    /// `GET /accounts` response rows.
    /// 账户列表的类型化投影。
    Accounts(Vec<AccountResponse>),
    /// `GET /accounts/{accountHash}` response.
    /// 单账户的类型化投影。
    Account(Box<AccountResponse>),
    /// Account-scoped or cross-account order rows.
    /// 订单列表的类型化投影。
    Orders(Vec<Order>),
    /// `GET /accounts/{accountHash}/orders/{orderId}` response.
    /// 单订单的类型化投影。
    Order(Box<Order>),
    /// Account transaction rows.
    /// 交易记录列表的类型化投影。
    Transactions(Vec<Transaction>),
    /// `GET /accounts/{accountHash}/transactions/{transactionId}` response.
    /// 单条交易记录的类型化投影。
    Transaction(TransactionResponse),
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {$ (
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
            }
        }
    )+};
}

redacted_debug!(
    TraderReadResponse,
    UserPreferencesResponse,
    UserPreference,
    UserPreferenceAccount,
    StreamerInfo,
    OfferInfo,
    AccountNumberHash,
    AccountResponse,
    SecuritiesAccount,
    Position,
    Instrument,
    BalanceSnapshot,
    Order,
    BrokerAccountNumber,
    OrderLeg,
    OrderActivity,
    ExecutionLeg,
    TransactionResponse,
    Transaction,
    TransactionUser,
    TransactionTransferItem,
    WireNumber,
    UnknownFields,
);

/// Builds the Trader DTO matching a validated Node SDK response family.
pub(crate) fn project(
    kind: ReadResponseKind,
    value: &Value,
) -> Result<Option<TraderReadResponse>, ReadResponseError> {
    let model = match kind {
        ReadResponseKind::AccountNumbers => TraderReadResponse::AccountNumbers(
            common::array(value, "accountNumbers")?
                .iter()
                .map(accounts::project_account_number_hash)
                .collect::<Result<_, _>>()?,
        ),
        ReadResponseKind::Accounts => TraderReadResponse::Accounts(
            common::array(value, "accounts")?
                .iter()
                .map(accounts::project_account)
                .collect::<Result<_, _>>()?,
        ),
        ReadResponseKind::Account => {
            TraderReadResponse::Account(Box::new(accounts::project_account(value)?))
        }
        ReadResponseKind::Orders => TraderReadResponse::Orders(
            common::array(value, "orders")?
                .iter()
                .map(|order| orders::project_order(order, 0))
                .collect::<Result<_, _>>()?,
        ),
        ReadResponseKind::Order => {
            TraderReadResponse::Order(Box::new(orders::project_order(value, 0)?))
        }
        ReadResponseKind::Transactions => TraderReadResponse::Transactions(
            common::array(value, "transactions")?
                .iter()
                .map(transactions::project_transaction)
                .collect::<Result<_, _>>()?,
        ),
        ReadResponseKind::Transaction => match value {
            Value::Array(rows) => TraderReadResponse::Transaction(TransactionResponse::Many(
                rows.iter()
                    .map(transactions::project_transaction)
                    .collect::<Result<_, _>>()?,
            )),
            _ => TraderReadResponse::Transaction(TransactionResponse::One(Box::new(
                transactions::project_transaction(value)?,
            ))),
        },
        _ => return Ok(None),
    };
    Ok(Some(model))
}

/// Builds the typed object/array preference DTO after schema validation.
pub(crate) fn project_user_preferences(
    value: &Value,
) -> Result<UserPreferencesResponse, ReadResponseError> {
    preferences::project_user_preferences(value)
}
