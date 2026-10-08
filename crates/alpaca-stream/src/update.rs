//! Unified read side for the stream's independent bounded lanes.
//!
//! 把独立的有界行情与控制队列合并为一个只读接收入口。

use crate::{ControlEvent, QuoteUpdate, TradeUpdate};

/// One item received from the quote or protected control lane.
///
/// 从报价队列或受保护控制队列收到的一项内容。
#[derive(Clone, Debug, PartialEq)]
pub enum StreamUpdate {
    /// A latest-value quote update.
    /// 最新值报价更新。
    Quote(QuoteUpdate),
    /// A non-coalesced trade update.
    /// 一条不合并的成交更新。
    Trade(TradeUpdate),
    /// A session, subscription, provider-error, or quote-discard control event.
    /// 会话、订阅、provider 错误或报价丢弃控制事件。
    Control(ControlEvent),
}
