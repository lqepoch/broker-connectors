//! Order, order-leg, execution-activity, and broker-account projections.
//! 定义订单、订单腿、成交活动和 broker 账户投影。

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    Instrument, UnknownFields, WireNumber, account_number, boolean, number, object, optional_array,
    optional_object, project_instrument, string, unknown_fields,
};

/// A Schwab order, including nested strategy orders and execution activity.
/// 中文摘要：Schwab 订单响应的只读投影，包含策略子订单和成交活动；状态或 cancelable 标记不提供写操作能力。
#[derive(Clone, PartialEq)]
pub struct Order {
    /// Trading session.
    /// 中文摘要：订单使用的交易时段。
    pub session: Option<String>,
    /// Order duration.
    /// 中文摘要：订单的有效期。
    pub duration: Option<String>,
    /// Order type.
    /// 中文摘要：broker 订单类型。
    pub order_type: Option<String>,
    /// Cancellation time.
    /// 中文摘要：订单取消时间（若 broker 提供）。
    pub cancel_time: Option<String>,
    /// Complex strategy type.
    /// 中文摘要：订单使用的复杂策略类型，例如组合策略。
    pub complex_order_strategy_type: Option<String>,
    /// Requested quantity.
    /// 中文摘要：订单或成交数量。
    pub quantity: Option<WireNumber>,
    /// Filled quantity.
    /// 中文摘要：截至该响应时已成交的订单数量。
    pub filled_quantity: Option<WireNumber>,
    /// Remaining quantity.
    /// 中文摘要：截至该响应时尚未成交的订单数量。
    pub remaining_quantity: Option<WireNumber>,
    /// Requested destination.
    /// 中文摘要：订单请求提交到的交易目的地。
    pub requested_destination: Option<String>,
    /// Destination link name.
    /// 中文摘要：broker 为目的地链接返回的名称。
    pub destination_link_name: Option<String>,
    /// Scheduled release time.
    /// 中文摘要：定时订单计划释放给市场的时间。
    pub release_time: Option<String>,
    /// Stop price.
    /// 中文摘要：止损触发价格，按 broker 返回的精确数值保留。
    pub stop_price: Option<WireNumber>,
    /// Stop-price link basis.
    /// 中文摘要：止损价格联动所依据的市场价格类型。
    pub stop_price_link_basis: Option<String>,
    /// Stop-price link type.
    /// 中文摘要：止损价格联动采用的调整方式。
    pub stop_price_link_type: Option<String>,
    /// Stop-price offset.
    /// 中文摘要：止损联动价格使用的偏移量，保留精确 wire 数值。
    pub stop_price_offset: Option<WireNumber>,
    /// Stop type.
    /// 中文摘要：broker 为止损订单返回的类型字符串。
    pub stop_type: Option<String>,
    /// Price link basis.
    /// 中文摘要：限价联动所依据的市场价格类型。
    pub price_link_basis: Option<String>,
    /// Price link type.
    /// 中文摘要：限价联动采用的调整方式。
    pub price_link_type: Option<String>,
    /// Limit or net price.
    /// 中文摘要：broker 订单价格。
    pub price: Option<WireNumber>,
    /// Tax-lot method.
    /// 中文摘要：订单使用的 tax lot 处置方法。
    pub tax_lot_method: Option<String>,
    /// Order legs.
    /// 中文摘要：订单中的证券腿列表；缺失与空列表按响应原样区分。
    pub order_leg_collection: Option<Vec<OrderLeg>>,
    /// Activation price.
    /// 中文摘要：条件订单的激活价格，按无损 wire 数值保留。
    pub activation_price: Option<WireNumber>,
    /// Special order instruction.
    /// 中文摘要：附加在订单上的 broker 特殊指令。
    pub special_instruction: Option<String>,
    /// Strategy relationship.
    /// 中文摘要：broker 订单策略类别。
    pub order_strategy_type: Option<String>,
    /// Broker order identifier as an exact wire number.
    /// 中文摘要：broker 订单标识符。
    pub order_id: Option<WireNumber>,
    /// Whether cancellation is allowed.
    /// 中文摘要：broker 报告当前订单是否可取消；不代表本 crate 提供取消能力。
    pub cancelable: Option<bool>,
    /// Whether replacement is allowed.
    /// 中文摘要：broker 报告当前订单是否可编辑；不代表本 crate 提供替换能力。
    pub editable: Option<bool>,
    /// Broker order status, including unknown future strings.
    /// 中文摘要：当前固定状态或 broker 状态。
    pub status: Option<String>,
    /// Entered timestamp.
    /// 中文摘要：订单录入时间。
    pub entered_time: Option<String>,
    /// Close timestamp.
    /// 中文摘要：订单关闭时间。
    pub close_time: Option<String>,
    /// Order tag.
    /// 中文摘要：附加到请求中的已校验标签。
    pub tag: Option<String>,
    /// Account number in the wire type used by Schwab.
    /// 中文摘要：broker 返回的账户编号。
    pub account_number: Option<BrokerAccountNumber>,
    /// Order and execution activity rows.
    /// 中文摘要：订单关联的成交与活动记录列表。
    pub order_activity_collection: Option<Vec<OrderActivity>>,
    /// Identifiers of orders this order replaces.
    /// 中文摘要：该订单替换的其他订单标识列表。
    pub replacing_order_collection: Option<Vec<String>>,
    /// Child strategy orders.
    /// 中文摘要：该订单包含的子策略订单。
    pub child_order_strategies: Option<Vec<Order>>,
    /// Broker status description.
    /// 中文摘要：broker 状态说明。
    pub status_description: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Account number accepted from an order response as either text or number.
/// 中文摘要：兼容 Schwab 将账户编号编码为文本或 JSON 数字的两种 wire 形式。
#[derive(Clone, PartialEq)]
pub enum BrokerAccountNumber {
    /// Text wire representation.
    /// broker 以文本形式返回账户编号。
    Text(String),
    /// Numeric wire representation, retained exactly.
    /// broker 以无损 JSON 数字 token 形式返回账户编号。
    Number(WireNumber),
}

/// One order leg.
/// 中文摘要：订单中的一条证券腿及其数量、方向和标的投影。
#[derive(Clone, PartialEq)]
pub struct OrderLeg {
    /// Leg type.
    /// 中文摘要：证券腿的 broker 类型。
    pub order_leg_type: Option<String>,
    /// Leg identifier.
    /// 中文摘要：broker 分配给订单腿的编号，保留精确 wire 数值。
    pub leg_id: Option<WireNumber>,
    /// Leg instrument.
    /// 中文摘要：该持仓或订单关联的证券标的。
    pub instrument: Option<Instrument>,
    /// Order instruction.
    /// 中文摘要：订单腿执行指令。
    pub instruction: Option<String>,
    /// Position effect.
    /// 中文摘要：订单腿对持仓的作用，例如开仓或平仓；未知未来值仍作为字符串保留。
    pub position_effect: Option<String>,
    /// Leg quantity.
    /// 中文摘要：订单或成交数量。
    pub quantity: Option<WireNumber>,
    /// Quantity type.
    /// 中文摘要：订单腿数量采用的单位或数量类型。
    pub quantity_type: Option<String>,
    /// Dividend/capital gains election.
    /// 中文摘要：订单腿的股息或资本利得处理选择。
    pub div_cap_gains: Option<String>,
    /// Destination symbol.
    /// 中文摘要：转换或交割后的目标证券代码。
    pub to_symbol: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Activity row attached to an order.
/// 中文摘要：附加在订单上的成交或活动记录，可保留未来新增的活动类型。
#[derive(Clone, PartialEq)]
pub struct OrderActivity {
    /// Activity category, including future values unknown to this SDK version.
    /// 中文摘要：broker 报告的订单活动类别，未知未来值仍保留为字符串。
    pub activity_type: Option<String>,
    /// Execution type, including future values unknown to this SDK version.
    /// 中文摘要：broker 报告的成交类别，未知未来值仍保留为字符串。
    pub execution_type: Option<String>,
    /// Activity quantity.
    /// 中文摘要：订单或成交数量。
    pub quantity: Option<WireNumber>,
    /// Remaining order quantity after the activity.
    /// 中文摘要：本条活动记录完成后订单剩余的数量。
    pub order_remaining_quantity: Option<WireNumber>,
    /// Executed legs.
    /// 中文摘要：该活动记录包含的成交腿。
    pub execution_legs: Option<Vec<ExecutionLeg>>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// One executed leg in an order activity.
/// 中文摘要：订单活动中的一条实际成交腿，包含成交价格、数量及时间。
#[derive(Clone, PartialEq)]
pub struct ExecutionLeg {
    /// Leg identifier.
    /// 中文摘要：broker 分配给订单腿的编号，保留精确 wire 数值。
    pub leg_id: Option<WireNumber>,
    /// Execution price.
    /// 中文摘要：broker 订单价格。
    pub price: Option<WireNumber>,
    /// Executed quantity.
    /// 中文摘要：订单或成交数量。
    pub quantity: Option<WireNumber>,
    /// Mismarked quantity.
    /// 中文摘要：broker 在成交腿上标记为 mismarked 的数量。
    pub mismarked_quantity: Option<WireNumber>,
    /// Instrument identifier.
    /// 中文摘要：成交腿关联的证券编号，保留精确 wire 数值。
    pub instrument_id: Option<WireNumber>,
    /// Execution time.
    /// 中文摘要：broker 报告的该笔成交时间。
    pub time: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

pub(super) fn project_order(value: &Value, depth: usize) -> Result<Order, ReadResponseError> {
    if depth > 32 {
        return Err(ReadResponseError::JsonTooComplex);
    }
    let fields = object(value, "order")?;
    let known = [
        "session",
        "duration",
        "orderType",
        "cancelTime",
        "complexOrderStrategyType",
        "quantity",
        "filledQuantity",
        "remainingQuantity",
        "requestedDestination",
        "destinationLinkName",
        "releaseTime",
        "stopPrice",
        "stopPriceLinkBasis",
        "stopPriceLinkType",
        "stopPriceOffset",
        "stopType",
        "priceLinkBasis",
        "priceLinkType",
        "price",
        "taxLotMethod",
        "orderLegCollection",
        "activationPrice",
        "specialInstruction",
        "orderStrategyType",
        "orderId",
        "cancelable",
        "editable",
        "status",
        "enteredTime",
        "closeTime",
        "tag",
        "accountNumber",
        "orderActivityCollection",
        "replacingOrderCollection",
        "childOrderStrategies",
        "statusDescription",
    ];
    Ok(Order {
        session: string(fields, "session", "order.session")?,
        duration: string(fields, "duration", "order.duration")?,
        order_type: string(fields, "orderType", "order.orderType")?,
        cancel_time: string(fields, "cancelTime", "order.cancelTime")?,
        complex_order_strategy_type: string(
            fields,
            "complexOrderStrategyType",
            "order.complexOrderStrategyType",
        )?,
        quantity: number(fields, "quantity", "order.quantity")?,
        filled_quantity: number(fields, "filledQuantity", "order.filledQuantity")?,
        remaining_quantity: number(fields, "remainingQuantity", "order.remainingQuantity")?,
        requested_destination: string(
            fields,
            "requestedDestination",
            "order.requestedDestination",
        )?,
        destination_link_name: string(fields, "destinationLinkName", "order.destinationLinkName")?,
        release_time: string(fields, "releaseTime", "order.releaseTime")?,
        stop_price: number(fields, "stopPrice", "order.stopPrice")?,
        stop_price_link_basis: string(fields, "stopPriceLinkBasis", "order.stopPriceLinkBasis")?,
        stop_price_link_type: string(fields, "stopPriceLinkType", "order.stopPriceLinkType")?,
        stop_price_offset: number(fields, "stopPriceOffset", "order.stopPriceOffset")?,
        stop_type: string(fields, "stopType", "order.stopType")?,
        price_link_basis: string(fields, "priceLinkBasis", "order.priceLinkBasis")?,
        price_link_type: string(fields, "priceLinkType", "order.priceLinkType")?,
        price: number(fields, "price", "order.price")?,
        tax_lot_method: string(fields, "taxLotMethod", "order.taxLotMethod")?,
        order_leg_collection: optional_array(
            fields,
            "orderLegCollection",
            "order.orderLegCollection",
            project_order_leg,
        )?,
        activation_price: number(fields, "activationPrice", "order.activationPrice")?,
        special_instruction: string(fields, "specialInstruction", "order.specialInstruction")?,
        order_strategy_type: string(fields, "orderStrategyType", "order.orderStrategyType")?,
        order_id: number(fields, "orderId", "order.orderId")?,
        cancelable: boolean(fields, "cancelable", "order.cancelable")?,
        editable: boolean(fields, "editable", "order.editable")?,
        status: string(fields, "status", "order.status")?,
        entered_time: string(fields, "enteredTime", "order.enteredTime")?,
        close_time: string(fields, "closeTime", "order.closeTime")?,
        tag: string(fields, "tag", "order.tag")?,
        account_number: account_number(fields, "accountNumber")?,
        order_activity_collection: optional_array(
            fields,
            "orderActivityCollection",
            "order.orderActivityCollection",
            project_order_activity,
        )?,
        replacing_order_collection: optional_array(
            fields,
            "replacingOrderCollection",
            "order.replacingOrderCollection",
            |value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(ReadResponseError::SchemaViolation {
                        field: "order.replacingOrderCollection[]",
                    })
            },
        )?,
        child_order_strategies: optional_array(
            fields,
            "childOrderStrategies",
            "order.childOrderStrategies",
            |child| project_order(child, depth + 1),
        )?,
        status_description: string(fields, "statusDescription", "order.statusDescription")?,
        unknown_fields: unknown_fields(fields, &known),
    })
}

fn project_order_leg(value: &Value) -> Result<OrderLeg, ReadResponseError> {
    let fields = object(value, "order.orderLegCollection[]")?;
    Ok(OrderLeg {
        order_leg_type: string(
            fields,
            "orderLegType",
            "order.orderLegCollection[].orderLegType",
        )?,
        leg_id: number(fields, "legId", "order.orderLegCollection[].legId")?,
        instrument: optional_object(
            fields,
            "instrument",
            "order.orderLegCollection[].instrument",
            project_instrument,
        )?,
        instruction: string(
            fields,
            "instruction",
            "order.orderLegCollection[].instruction",
        )?,
        position_effect: string(
            fields,
            "positionEffect",
            "order.orderLegCollection[].positionEffect",
        )?,
        quantity: number(fields, "quantity", "order.orderLegCollection[].quantity")?,
        quantity_type: string(
            fields,
            "quantityType",
            "order.orderLegCollection[].quantityType",
        )?,
        div_cap_gains: string(
            fields,
            "divCapGains",
            "order.orderLegCollection[].divCapGains",
        )?,
        to_symbol: string(fields, "toSymbol", "order.orderLegCollection[].toSymbol")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "orderLegType",
                "legId",
                "instrument",
                "instruction",
                "positionEffect",
                "quantity",
                "quantityType",
                "divCapGains",
                "toSymbol",
            ],
        ),
    })
}

fn project_order_activity(value: &Value) -> Result<OrderActivity, ReadResponseError> {
    let fields = object(value, "order.orderActivityCollection[]")?;
    Ok(OrderActivity {
        activity_type: string(
            fields,
            "activityType",
            "order.orderActivityCollection[].activityType",
        )?,
        execution_type: string(
            fields,
            "executionType",
            "order.orderActivityCollection[].executionType",
        )?,
        quantity: number(
            fields,
            "quantity",
            "order.orderActivityCollection[].quantity",
        )?,
        order_remaining_quantity: number(
            fields,
            "orderRemainingQuantity",
            "order.orderActivityCollection[].orderRemainingQuantity",
        )?,
        execution_legs: optional_array(
            fields,
            "executionLegs",
            "order.orderActivityCollection[].executionLegs",
            project_execution_leg,
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "activityType",
                "executionType",
                "quantity",
                "orderRemainingQuantity",
                "executionLegs",
            ],
        ),
    })
}

fn project_execution_leg(value: &Value) -> Result<ExecutionLeg, ReadResponseError> {
    let fields = object(value, "order.orderActivityCollection[].executionLegs[]")?;
    Ok(ExecutionLeg {
        leg_id: number(fields, "legId", "executionLeg.legId")?,
        price: number(fields, "price", "executionLeg.price")?,
        quantity: number(fields, "quantity", "executionLeg.quantity")?,
        mismarked_quantity: number(
            fields,
            "mismarkedQuantity",
            "executionLeg.mismarkedQuantity",
        )?,
        instrument_id: number(fields, "instrumentId", "executionLeg.instrumentId")?,
        time: string(fields, "time", "executionLeg.time")?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "legId",
                "price",
                "quantity",
                "mismarkedQuantity",
                "instrumentId",
                "time",
            ],
        ),
    })
}
