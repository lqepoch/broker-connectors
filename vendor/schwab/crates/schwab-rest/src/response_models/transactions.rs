//! Transaction and transfer response projections.
//! 定义交易记录和转账响应投影。

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    Instrument, UnknownFields, WireNumber, number, object, optional_array, optional_object,
    project_instrument, string, unknown_fields,
};

/// Object or array shape returned by the single-transaction endpoint.
/// 中文摘要：保持 Trader 交易记录端点返回的数组形状和记录顺序。
#[derive(Clone, PartialEq)]
pub enum TransactionResponse {
    /// A single transaction object.
    /// broker 返回单个交易记录对象。
    One(Box<Transaction>),
    /// A transaction array, retained in response order.
    /// broker 返回交易记录数组。
    Many(Vec<Transaction>),
}

impl TransactionResponse {
    /// Mirrors Node's convenience helper by returning the object or first row.
    /// 中文摘要：单对象时返回该交易，数组时返回首项；空数组返回固定 not-found 错误。
    ///
    /// # Errors
    /// Returns [`ReadResponseError`] when the response kind, structure, required values, or numeric bounds do not satisfy this projection.
    pub fn first(&self) -> Result<&Transaction, ReadResponseError> {
        match self {
            Self::One(transaction) => Ok(transaction.as_ref()),
            Self::Many(transactions) => transactions
                .first()
                .ok_or(ReadResponseError::TransactionNotFound),
        }
    }
}

/// A transaction/trade record.
/// 中文摘要：账户交易记录的只读投影，包含交易日期、金额、状态和关联活动。
#[derive(Clone, PartialEq)]
pub struct Transaction {
    /// Activity identifier.
    /// 中文摘要：broker 为该账户活动分配的标识，保留精确 wire 数值。
    pub activity_id: Option<WireNumber>,
    /// Transaction timestamp.
    /// 中文摘要：broker 报告的交易记录时间戳。
    pub time: Option<String>,
    /// User that initiated the transaction.
    /// 中文摘要：broker 返回的用户附加字段。
    pub user: Option<TransactionUser>,
    /// Broker description.
    /// 中文摘要：标的或产品说明。
    pub description: Option<String>,
    /// Account number.
    /// 中文摘要：broker 返回的账户编号。
    pub account_number: Option<String>,
    /// Transaction type, including future values unknown to this SDK version.
    /// 中文摘要：broker 交易记录类型。
    pub transaction_type: Option<String>,
    /// Transaction status.
    /// 中文摘要：当前固定状态或 broker 状态。
    pub status: Option<String>,
    /// Sub-account code.
    /// 中文摘要：交易记录关联的子账户代码。
    pub sub_account: Option<String>,
    /// Trade date.
    /// 中文摘要：交易发生日期。
    pub trade_date: Option<String>,
    /// Settlement date.
    /// 中文摘要：交易结算日期；与交易日期含义不同。
    pub settlement_date: Option<String>,
    /// Position identifier.
    /// 中文摘要：交易记录关联的持仓编号，保留精确 wire 数值。
    pub position_id: Option<WireNumber>,
    /// Related order identifier.
    /// 中文摘要：broker 订单标识符。
    pub order_id: Option<WireNumber>,
    /// Net amount.
    /// 中文摘要：交易记录净金额。
    pub net_amount: Option<WireNumber>,
    /// Activity type.
    /// 中文摘要：交易活动类别；未知未来值仍保留为字符串。
    pub activity_type: Option<String>,
    /// Transfer/trade items.
    /// 中文摘要：交易记录中的转账明细。
    pub transfer_items: Option<Vec<TransactionTransferItem>>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// User identity fields embedded in a transaction.
/// 中文摘要：交易记录中由 broker 提供的用户身份字段；姓名和登录信息应避免写入日志。
#[derive(Clone, PartialEq)]
pub struct TransactionUser {
    /// Domain identifier.
    /// 中文摘要：交易发起用户所属的 domain 编号。
    pub cd_domain_id: Option<String>,
    /// Login name.
    /// 中文摘要：交易发起用户的登录名；属于账户数据，避免写入日志。
    pub login: Option<String>,
    /// User type.
    /// 中文摘要：交易发起用户的 broker 用户类别。
    pub user_type: Option<String>,
    /// Numeric user identifier.
    /// 中文摘要：交易发起用户编号，保留精确 wire 数值。
    pub user_id: Option<WireNumber>,
    /// System user name.
    /// 中文摘要：broker 报告的系统用户名。
    pub system_user_name: Option<String>,
    /// First name.
    /// 中文摘要：交易发起用户的名；属于个人信息，应避免写入日志。
    pub first_name: Option<String>,
    /// Last name.
    /// 中文摘要：交易发起用户的姓；属于个人信息，应避免写入日志。
    pub last_name: Option<String>,
    /// Broker representative code.
    /// 中文摘要：负责该交易的 broker 代表代码。
    pub broker_rep_code: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// One transfer or trade line item.
/// 中文摘要：交易或转账的一条明细，包括金额、成本、价格及持仓作用。
#[derive(Clone, PartialEq)]
pub struct TransactionTransferItem {
    /// Related instrument.
    /// 中文摘要：该持仓或订单关联的证券标的。
    pub instrument: Option<Instrument>,
    /// Transfer amount.
    /// 中文摘要：该转账或交易条目的金额，保留精确 wire 数值。
    pub amount: Option<WireNumber>,
    /// Cost basis.
    /// 中文摘要：该条目的成本基础金额，保留精确 wire 数值。
    pub cost: Option<WireNumber>,
    /// Transaction price.
    /// 中文摘要：broker 订单价格。
    pub price: Option<WireNumber>,
    /// Fee type.
    /// 中文摘要：broker 报告的费用类别。
    pub fee_type: Option<String>,
    /// Position effect, including future values unknown to this SDK version.
    /// 中文摘要：该条目对持仓的作用；未知未来值仍保留为字符串。
    pub position_effect: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_transaction(value: &Value) -> Result<Transaction, ReadResponseError> {
    let fields = object(value, "transaction")?;
    Ok(Transaction {
        activity_id: number(fields, "activityId", "transaction.activityId")?,
        time: string(fields, "time", "transaction.time")?,
        user: optional_object(fields, "user", "transaction.user", project_transaction_user)?,
        description: string(fields, "description", "transaction.description")?,
        account_number: string(fields, "accountNumber", "transaction.accountNumber")?,
        transaction_type: string(fields, "type", "transaction.type")?,
        status: string(fields, "status", "transaction.status")?,
        sub_account: string(fields, "subAccount", "transaction.subAccount")?,
        trade_date: string(fields, "tradeDate", "transaction.tradeDate")?,
        settlement_date: string(fields, "settlementDate", "transaction.settlementDate")?,
        position_id: number(fields, "positionId", "transaction.positionId")?,
        order_id: number(fields, "orderId", "transaction.orderId")?,
        net_amount: number(fields, "netAmount", "transaction.netAmount")?,
        activity_type: string(fields, "activityType", "transaction.activityType")?,
        transfer_items: optional_array(
            fields,
            "transferItems",
            "transaction.transferItems",
            project_transfer_item,
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "activityId",
                "time",
                "user",
                "description",
                "accountNumber",
                "type",
                "status",
                "subAccount",
                "tradeDate",
                "settlementDate",
                "positionId",
                "orderId",
                "netAmount",
                "activityType",
                "transferItems",
            ],
        ),
    })
}

fn project_transaction_user(value: &Value) -> Result<TransactionUser, ReadResponseError> {
    let fields = object(value, "transaction.user")?;
    Ok(TransactionUser {
        cd_domain_id: string(fields, "cdDomainId", "transaction.user.cdDomainId")?,
        login: string(fields, "login", "transaction.user.login")?,
        user_type: string(fields, "type", "transaction.user.type")?,
        user_id: number(fields, "userId", "transaction.user.userId")?,
        system_user_name: string(fields, "systemUserName", "transaction.user.systemUserName")?,
        first_name: string(fields, "firstName", "transaction.user.firstName")?,
        last_name: string(fields, "lastName", "transaction.user.lastName")?,
        broker_rep_code: string(fields, "brokerRepCode", "transaction.user.brokerRepCode")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "cdDomainId",
                "login",
                "type",
                "userId",
                "systemUserName",
                "firstName",
                "lastName",
                "brokerRepCode",
            ],
        ),
    })
}

fn project_transfer_item(value: &Value) -> Result<TransactionTransferItem, ReadResponseError> {
    let fields = object(value, "transaction.transferItems[]")?;
    Ok(TransactionTransferItem {
        instrument: optional_object(
            fields,
            "instrument",
            "transaction.transferItems[].instrument",
            project_instrument,
        )?,
        amount: number(fields, "amount", "transaction.transferItems[].amount")?,
        cost: number(fields, "cost", "transaction.transferItems[].cost")?,
        price: number(fields, "price", "transaction.transferItems[].price")?,
        fee_type: string(fields, "feeType", "transaction.transferItems[].feeType")?,
        position_effect: string(
            fields,
            "positionEffect",
            "transaction.transferItems[].positionEffect",
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "instrument",
                "amount",
                "cost",
                "price",
                "feeType",
                "positionEffect",
            ],
        ),
    })
}
