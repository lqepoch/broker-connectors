//! Bounded quote coalescing and isolated trade/control delivery lanes.
//!
//! 有界报价合并以及相互隔离的成交/控制交付队列。

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, mpsc, watch};
use tokio::task::JoinHandle;

use crate::StreamUpdate;
use crate::config::{OptionContractSymbol, OptionFeed, StreamConfig};
use crate::model::{
    DataFreshness, InboundRawMarketFrame, IngestStamp, OptionQuote, OptionTrade, ProviderTimestamp,
    SessionGeneration, SessionPhase, SessionStatusCause, SubscriptionAcknowledgement,
};
use crate::protocol::ProviderError;
use crate::session::{SessionExit, StreamError};

/// Latest coalesced quote for one contract and connection generation.
/// 单个合约及连接代次下合并后的最新报价。
#[derive(Clone, Debug, PartialEq)]
pub struct QuoteUpdate {
    /// Provider quote payload.
    /// Provider 报价内容。
    pub quote: OptionQuote,
    /// Explicit source feed identity.
    /// 明确的行情源 feed 身份。
    pub feed: OptionFeed,
    /// Local generation and ingest sequence.
    /// 本地会话代次与接收序号。
    pub ingest: IngestStamp,
    /// Provider timestamp freshness at local receipt time.
    /// 本地接收时 provider 时间戳的新鲜度分类。
    pub freshness: DataFreshness,
    /// Number of accepted later quotes for this key replaced before this update was received.
    /// 此更新被消费者接收前，同一代码有多少条获准替换的报价覆盖了它。
    pub coalesced_updates: u64,
}

/// One non-coalesced option trade update.
/// 一条不会合并的期权成交更新。
#[derive(Clone, Debug, PartialEq)]
pub struct TradeUpdate {
    /// Provider trade payload.
    /// Provider 成交内容。
    pub trade: OptionTrade,
    /// Explicit source feed identity.
    /// 明确的行情源 feed 身份。
    pub feed: OptionFeed,
    /// Local generation and ingest sequence.
    /// 本地会话代次与接收序号。
    pub ingest: IngestStamp,
    /// Provider timestamp freshness at local receipt time.
    /// 本地接收时 provider 时间戳的新鲜度分类。
    pub freshness: DataFreshness,
}

/// Why a quote was discarded before entering the coalescing lane.
/// 报价进入合并队列前被丢弃的原因。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuoteDiscardReason {
    /// Its provider timestamp was older than the latest accepted quote watermark for the contract in this generation.
    /// 其 provider 时间戳早于此代次该合约最近已接收报价的时序水位。
    OlderProviderTimestamp,
    /// Provider timestamps matched and its local ingest sequence was not newer.
    /// Provider 时间戳相同，且本地接收序号没有更新。
    EqualTimestampNotNewerIngest,
    /// Its provider timestamp was outside the configured maximum age.
    /// 其 provider 时间戳超过配置的最大允许年龄。
    Stale,
    /// Its provider timestamp exceeded the configured future-skew allowance.
    /// 其 provider 时间戳超过配置的未来偏差容限。
    FutureDated,
    /// It arrived from a generation that is no longer current for this lane.
    /// 其来源代次已不是此队列的当前代次。
    OldGeneration,
}

/// Control, error, phase, and subscription events use a lane independent from market data.
/// 控制、错误、阶段和订阅事件使用独立于行情数据的交付队列。
#[derive(Clone, Debug, PartialEq)]
pub enum ControlEvent {
    /// Exact bytes and bounded diagnostic metadata for one inbound market-data application frame.
    /// 一条入站行情应用帧的精确字节及有界诊断元数据。
    RawMarketFrame(InboundRawMarketFrame),
    /// Session state transition for one local generation.
    /// 单个本地代次的会话状态转换。
    PhaseChanged {
        /// Session generation associated with this state.
        /// 此状态对应的会话代次。
        generation: SessionGeneration,
        /// New public state-machine phase.
        /// 新的公开状态机阶段。
        phase: SessionPhase,
        /// Optional stable cause for loss or shutdown.
        /// 会话丢失或关闭时的可选固定原因。
        cause: Option<SessionStatusCause>,
    },
    /// Full subscription state acknowledged by the provider.
    /// Provider 已确认的完整订阅状态。
    SubscriptionAcknowledged {
        /// Session generation associated with the acknowledgement.
        /// 此回执对应的会话代次。
        generation: SessionGeneration,
        /// Current provider quote/trade subscription sets.
        /// Provider 当前报价/成交订阅集合。
        acknowledgement: SubscriptionAcknowledgement,
    },
    /// Sanitized provider error; raw provider text is never retained.
    /// 已脱敏的 provider 错误；不保留 provider 原始文本。
    ProviderError {
        /// Session generation associated with the error.
        /// 此错误对应的会话代次。
        generation: SessionGeneration,
        /// Fixed numeric code and category.
        /// 固定数字错误码与类别。
        error: ProviderError,
    },
    /// Unknown provider message type with no body or raw payload.
    /// 未知 provider 消息类型，不携带正文或原始 payload。
    UnknownMessage {
        /// Session generation associated with the message.
        /// 此消息对应的会话代次。
        generation: SessionGeneration,
        /// Bounded message type tag.
        /// 有界消息类型标签。
        type_tag: String,
    },
    /// A quote was rejected before queueing; the provider time and local ingest stamp remain auditable.
    /// 报价在入队前被拒绝；provider 时间和本地接收标记仍可审计。
    QuoteDiscarded {
        /// Provider option contract symbol.
        /// Provider 期权合约代码。
        symbol: OptionContractSymbol,
        /// Provider timestamp carried by the discarded quote.
        /// 被丢弃报价携带的 provider 时间戳。
        provider_timestamp: ProviderTimestamp,
        /// Local generation and ingest sequence assigned to the quote.
        /// 为该报价分配的本地代次与接收序号。
        ingest: IngestStamp,
        /// Stable reason category for the discard.
        /// 丢弃原因的固定类别。
        reason: QuoteDiscardReason,
    },
}

/// Bounded consumer of latest quote updates keyed by unique option contract.
/// 按唯一期权合约代码读取报价最新值的有界消费者。
pub struct QuoteReceiver {
    inner: Arc<QuoteLane>,
    close_tx: watch::Sender<bool>,
}

impl QuoteReceiver {
    /// Receives the next available coalesced quote or returns `None` after shutdown.
    /// 接收下一条合并报价；会话关闭后返回 `None`。
    pub async fn recv(&mut self) -> Option<QuoteUpdate> {
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.state.lock().await;
                if state.closed {
                    return None;
                }
                if let Some((_, update)) = state.pending.pop_first() {
                    return Some(update);
                }
            }
            notified.await;
        }
    }
}

impl Drop for QuoteReceiver {
    fn drop(&mut self) {
        self.close_tx.send_replace(true);
        self.inner.notify.notify_waiters();
    }
}

/// Bounded consumer of non-coalesced option trades.
/// 读取不合并期权成交的有界消费者。
pub struct TradeReceiver {
    inner: Arc<TradeLane>,
    close_tx: watch::Sender<bool>,
}

impl TradeReceiver {
    /// Receives the next trade or returns `None` after shutdown.
    /// 接收下一条成交；会话关闭后返回 `None`。
    pub async fn recv(&mut self) -> Option<TradeUpdate> {
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.state.lock().await;
                if let Some(trade) = state.pending.pop_front() {
                    return Some(trade);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for TradeReceiver {
    fn drop(&mut self) {
        self.close_tx.send_replace(true);
    }
}

/// Bounded consumer of session, subscription, error, and unknown-message events.
/// 读取会话、订阅、错误与未知消息事件的有界消费者。
pub struct ControlReceiver {
    receiver: mpsc::Receiver<ControlEvent>,
    close_tx: watch::Sender<bool>,
}

impl ControlReceiver {
    /// Receives the next critical control event or returns `None` after shutdown.
    /// 接收下一条关键控制事件；会话关闭后返回 `None`。
    pub async fn recv(&mut self) -> Option<ControlEvent> {
        self.receiver.recv().await
    }
}

impl Drop for ControlReceiver {
    fn drop(&mut self) {
        self.close_tx.send_replace(true);
    }
}

/// Owned stream task and its independent bounded consumer lanes.
/// 持有流任务及其相互独立的有界消费者队列。
pub struct StreamHandle {
    quotes: QuoteReceiver,
    trades: TradeReceiver,
    controls: ControlReceiver,
    quotes_closed: bool,
    controls_closed: bool,
    trades_closed: bool,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<Result<SessionExit, StreamError>>>,
}

impl StreamHandle {
    /// Borrows the coalesced quote consumer.
    /// 借用合并报价消费者。
    pub fn quotes(&mut self) -> &mut QuoteReceiver {
        &mut self.quotes
    }

    /// Borrows the non-coalesced trade consumer.
    /// 借用不合并成交消费者。
    pub fn trades(&mut self) -> &mut TradeReceiver {
        &mut self.trades
    }

    /// Borrows the protected control/error consumer.
    /// 借用受保护的控制/错误消费者。
    pub fn controls(&mut self) -> &mut ControlReceiver {
        &mut self.controls
    }

    /// Receives the next quote or control item without merging or dropping either lane.
    ///
    /// A closed lane is disabled while the other remains active; the method returns `None` only
    /// after both lanes are closed. Trade updates remain available through their dedicated lane.
    ///
    /// 读取下一条报价或控制事件，不会合并或丢弃任一队列。单个队列关闭后会继续读取另一队列；
    /// 只有两个队列都关闭时才返回 `None`。Trade 更新仍通过独立队列读取。
    pub async fn next_update(&mut self) -> Option<StreamUpdate> {
        loop {
            if self.quotes_closed && self.controls_closed {
                return None;
            }
            tokio::select! {
                biased;
                control = self.controls.recv(), if !self.controls_closed => match control {
                    Some(event) => return Some(StreamUpdate::Control(event)),
                    None => self.controls_closed = true,
                },
                quote = self.quotes.recv(), if !self.quotes_closed => match quote {
                    Some(update) => return Some(StreamUpdate::Quote(update)),
                    None => self.quotes_closed = true,
                },
            }
        }
    }

    /// Receives from the quote, trade, and protected control lanes without dropping an item.
    /// 从报价、成交和受保护控制队列读取事件，不会丢弃队列中的事件。
    pub async fn next_event(&mut self) -> Option<StreamUpdate> {
        loop {
            if self.quotes_closed && self.trades_closed && self.controls_closed {
                return None;
            }
            tokio::select! {
                biased;
                control = self.controls.recv(), if !self.controls_closed => match control {
                    Some(event) => return Some(StreamUpdate::Control(event)),
                    None => self.controls_closed = true,
                },
                quote = self.quotes.recv(), if !self.quotes_closed => match quote {
                    Some(update) => return Some(StreamUpdate::Quote(update)),
                    None => self.quotes_closed = true,
                },
                trade = self.trades.recv(), if !self.trades_closed => match trade {
                    Some(update) => return Some(StreamUpdate::Trade(update)),
                    None => self.trades_closed = true,
                },
            }
        }
    }

    /// Requests cancellation; the owned socket is closed by the session task.
    /// 请求取消；由会话任务关闭其拥有的 socket。
    pub fn cancel(&self) {
        self.shutdown_tx.send_replace(true);
    }

    /// Waits for the session task and returns its stable result category.
    /// 等待会话任务并返回固定结果类别。
    ///
    /// # Errors
    ///
    /// Returns an error when the task was already joined or terminated unexpectedly.
    pub async fn join(&mut self) -> Result<SessionExit, StreamError> {
        let task = self.task.take().ok_or(StreamError::TaskAlreadyJoined)?;
        task.await.unwrap_or(Err(StreamError::TaskTerminated))
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

pub(crate) struct LanePublishers {
    quotes: QuotePublisher,
    trades: TradePublisher,
    controls: mpsc::Sender<ControlEvent>,
}

pub(crate) struct LaneReceivers {
    pub(crate) quotes: QuoteReceiver,
    pub(crate) trades: TradeReceiver,
    pub(crate) controls: ControlReceiver,
}

impl LaneReceivers {
    pub(crate) fn into_handle(
        self,
        shutdown_tx: watch::Sender<bool>,
        task: JoinHandle<Result<SessionExit, StreamError>>,
    ) -> StreamHandle {
        StreamHandle {
            quotes: self.quotes,
            trades: self.trades,
            controls: self.controls,
            quotes_closed: false,
            controls_closed: false,
            trades_closed: false,
            shutdown_tx,
            task: Some(task),
        }
    }
}

pub(crate) fn create_lanes(
    config: &StreamConfig,
) -> (LanePublishers, LaneReceivers, watch::Receiver<bool>) {
    let (closed_tx, closed_rx) = watch::channel(false);
    let trade_lane = Arc::new(TradeLane {
        state: Mutex::new(TradeLaneState {
            generation: None,
            pending: VecDeque::new(),
            closed: false,
        }),
        notify: Notify::new(),
        capacity: config.limits.trade_capacity,
    });
    let (controls_tx, controls_rx) = mpsc::channel(config.limits.control_capacity);
    let quote_lane = Arc::new(QuoteLane {
        state: Mutex::new(QuoteLaneState {
            generation: None,
            pending: BTreeMap::new(),
            watermarks: BTreeMap::new(),
            closed: false,
        }),
        notify: Notify::new(),
        capacity: config.limits.quote_capacity,
    });
    let publishers = LanePublishers {
        quotes: QuotePublisher {
            inner: Arc::clone(&quote_lane),
        },
        trades: TradePublisher {
            inner: Arc::clone(&trade_lane),
        },
        controls: controls_tx,
    };
    let receivers = LaneReceivers {
        quotes: QuoteReceiver {
            inner: quote_lane,
            close_tx: closed_tx.clone(),
        },
        trades: TradeReceiver {
            inner: trade_lane,
            close_tx: closed_tx.clone(),
        },
        controls: ControlReceiver {
            receiver: controls_rx,
            close_tx: closed_tx,
        },
    };
    (publishers, receivers, closed_rx)
}

impl LanePublishers {
    pub(crate) async fn begin_generation(&self, generation: SessionGeneration) {
        self.quotes.begin_generation(generation).await;
        self.trades.begin_generation(generation).await;
    }

    pub(crate) async fn invalidate_generation(&self, generation: SessionGeneration) {
        self.quotes.invalidate_generation(generation).await;
        self.trades.invalidate_generation(generation).await;
    }

    pub(crate) async fn quote(&self, update: QuoteUpdate) -> Result<(), LaneFailure> {
        if let Some(event) = self.quotes.publish(update).await? {
            self.control(event)?;
        }
        Ok(())
    }

    pub(crate) async fn trade(&self, update: TradeUpdate) -> Result<(), LaneFailure> {
        self.trades.publish(update).await
    }

    pub(crate) fn control(&self, event: ControlEvent) -> Result<(), LaneFailure> {
        self.controls.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => LaneFailure::ControlOverloaded,
            mpsc::error::TrySendError::Closed(_) => LaneFailure::ControlReceiverClosed,
        })
    }

    pub(crate) async fn close(&self) {
        self.quotes.close().await;
        self.trades.close().await;
    }
}

pub(crate) fn publish_phase(
    publishers: &LanePublishers,
    generation: SessionGeneration,
    phase: SessionPhase,
    cause: Option<SessionStatusCause>,
) -> Result<(), LaneFailure> {
    publishers.control(ControlEvent::PhaseChanged {
        generation,
        phase,
        cause,
    })
}

pub(crate) fn publish_subscription(
    publishers: &LanePublishers,
    generation: SessionGeneration,
    acknowledgement: SubscriptionAcknowledgement,
) -> Result<(), LaneFailure> {
    publishers.control(ControlEvent::SubscriptionAcknowledged {
        generation,
        acknowledgement,
    })
}

pub(crate) fn publish_provider_error(
    publishers: &LanePublishers,
    generation: SessionGeneration,
    error: ProviderError,
) -> Result<(), LaneFailure> {
    publishers.control(ControlEvent::ProviderError { generation, error })
}

pub(crate) fn publish_unknown(
    publishers: &LanePublishers,
    generation: SessionGeneration,
    type_tag: String,
) -> Result<(), LaneFailure> {
    publishers.control(ControlEvent::UnknownMessage {
        generation,
        type_tag,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LaneFailure {
    QuoteCapacityExceeded,
    QuoteReceiverClosed,
    TradeOverloaded,
    TradeReceiverClosed,
    RawFrameOverloaded,
    ControlOverloaded,
    ControlReceiverClosed,
}

struct QuoteLane {
    state: Mutex<QuoteLaneState>,
    notify: Notify,
    capacity: usize,
}

struct QuoteLaneState {
    generation: Option<SessionGeneration>,
    pending: BTreeMap<OptionContractSymbol, QuoteUpdate>,
    /// Latest accepted provider-time and local-ingest pair, bounded by quote capacity per generation.
    /// 每代每个合约最近接收的 provider 时间与本地序号，数量受 quote 容量约束。
    watermarks: BTreeMap<OptionContractSymbol, QuoteWatermark>,
    closed: bool,
}

struct QuoteWatermark {
    provider_timestamp: ProviderTimestamp,
    local_ingest_sequence: u64,
}

struct QuotePublisher {
    inner: Arc<QuoteLane>,
}

impl QuotePublisher {
    async fn begin_generation(&self, generation: SessionGeneration) {
        let mut state = self.inner.state.lock().await;
        state.pending.clear();
        state.watermarks.clear();
        state.generation = Some(generation);
        drop(state);
        self.inner.notify.notify_waiters();
    }

    async fn invalidate_generation(&self, generation: SessionGeneration) {
        let mut state = self.inner.state.lock().await;
        if state.generation == Some(generation) {
            state.pending.clear();
            state.watermarks.clear();
            state.generation = None;
        }
        drop(state);
        self.inner.notify.notify_waiters();
    }

    async fn publish(&self, mut update: QuoteUpdate) -> Result<Option<ControlEvent>, LaneFailure> {
        let mut state = self.inner.state.lock().await;
        if state.closed {
            return Err(LaneFailure::QuoteReceiverClosed);
        }
        if state.generation != Some(update.ingest.generation) {
            return Ok(Some(quote_discarded(
                update,
                QuoteDiscardReason::OldGeneration,
            )));
        }
        match update.freshness {
            DataFreshness::Fresh => {}
            DataFreshness::Stale => {
                return Ok(Some(quote_discarded(update, QuoteDiscardReason::Stale)));
            }
            DataFreshness::FutureDated => {
                return Ok(Some(quote_discarded(
                    update,
                    QuoteDiscardReason::FutureDated,
                )));
            }
        }
        let symbol = update.quote.symbol.clone();
        if let Some(watermark) = state.watermarks.get(&symbol) {
            match update
                .quote
                .timestamp
                .cmp_instant(&watermark.provider_timestamp)
            {
                std::cmp::Ordering::Less => {
                    return Ok(Some(quote_discarded(
                        update,
                        QuoteDiscardReason::OlderProviderTimestamp,
                    )));
                }
                std::cmp::Ordering::Equal
                    if update.ingest.sequence <= watermark.local_ingest_sequence =>
                {
                    return Ok(Some(quote_discarded(
                        update,
                        QuoteDiscardReason::EqualTimestampNotNewerIngest,
                    )));
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {}
            }
        } else if state.watermarks.len() >= self.inner.capacity {
            return Err(LaneFailure::QuoteCapacityExceeded);
        }
        if let Some(pending) = state.pending.get(&symbol) {
            update.coalesced_updates = pending.coalesced_updates.saturating_add(1);
        } else if state.pending.len() >= self.inner.capacity {
            return Err(LaneFailure::QuoteCapacityExceeded);
        }
        state.watermarks.insert(
            symbol.clone(),
            QuoteWatermark {
                provider_timestamp: update.quote.timestamp.clone(),
                local_ingest_sequence: update.ingest.sequence,
            },
        );
        state.pending.insert(symbol, update);
        drop(state);
        self.inner.notify.notify_one();
        Ok(None)
    }

    async fn close(&self) {
        let mut state = self.inner.state.lock().await;
        state.pending.clear();
        state.watermarks.clear();
        state.generation = None;
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

fn quote_discarded(update: QuoteUpdate, reason: QuoteDiscardReason) -> ControlEvent {
    ControlEvent::QuoteDiscarded {
        symbol: update.quote.symbol,
        provider_timestamp: update.quote.timestamp,
        ingest: update.ingest,
        reason,
    }
}

struct TradeLane {
    state: Mutex<TradeLaneState>,
    notify: Notify,
    capacity: usize,
}

struct TradeLaneState {
    generation: Option<SessionGeneration>,
    pending: VecDeque<TradeUpdate>,
    closed: bool,
}

struct TradePublisher {
    inner: Arc<TradeLane>,
}

impl TradePublisher {
    async fn begin_generation(&self, generation: SessionGeneration) {
        let mut state = self.inner.state.lock().await;
        state.pending.clear();
        state.generation = Some(generation);
        drop(state);
        self.inner.notify.notify_waiters();
    }

    async fn invalidate_generation(&self, generation: SessionGeneration) {
        let mut state = self.inner.state.lock().await;
        if state.generation == Some(generation) {
            state.pending.clear();
            state.generation = None;
        }
        drop(state);
        self.inner.notify.notify_waiters();
    }

    async fn publish(&self, update: TradeUpdate) -> Result<(), LaneFailure> {
        let mut state = self.inner.state.lock().await;
        if state.closed || state.generation != Some(update.ingest.generation) {
            return Err(LaneFailure::TradeReceiverClosed);
        }
        if state.pending.len() >= self.inner.capacity {
            return Err(LaneFailure::TradeOverloaded);
        }
        state.pending.push_back(update);
        drop(state);
        self.inner.notify.notify_one();
        Ok(())
    }

    async fn close(&self) {
        let mut state = self.inner.state.lock().await;
        state.pending.clear();
        state.generation = None;
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

#[cfg(test)]
#[path = "lane_tests.rs"]
mod tests;
