//! Adapter from the bounded Alpaca stream to frozen cross-language market contracts.
//!
//! 将有界 Alpaca 行情流适配到已冻结的跨语言市场合同。

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use broker_ports::{
    BrokerPortError, MarketDataItem, MarketDataPort, MarketDataSession,
    MarketDataSubscriptionRequest, PortFuture, RawFrameReference, RawFrameSinkError,
    RawFrameSinkFactory, RawMarketFrame,
};
use chrono::{SecondsFormat, Utc};
use market_contracts::{
    ConnectionState, ControlEventEnvelopeV1, DecimalString, EntitlementState, EventMetadataV1,
    MAX_CONTROL_INSTRUMENTS, MarketControlEventV1, MarketDataSourceV1, MarketEventEnvelopeV1,
    MarketEventV1, NumericEncodingV1, RejectedInstrumentV1, UtcTimestamp,
};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::MAX_OPTION_SYMBOL_BYTES;
use crate::protocol::MAX_FRAME_MESSAGES;
use crate::{
    AlpacaCredentials, AlpacaOptionsStream, ControlEvent, CredentialFailure, CredentialProvider,
    MarketNumber, OptionContractSymbol, OptionFeed, QuoteUpdate, SessionGeneration, SessionPhase,
    SessionStatusCause, StreamConfig, StreamEnvironment, StreamUpdate, TradeUpdate,
};

const RECORD_LANE_CAPACITY: usize = 2_176;
/// One process-wide Alpaca options stream may be active through this adapter at a time.
/// 此 adapter 在单个进程内最多允许一个 Alpaca 期权流处于活动状态。
pub const MAX_ACTIVE_ALPACA_PORT_SESSIONS: usize = 1;

static NEXT_PORT_GENERATION: AtomicU64 = AtomicU64::new(1);
static ACTIVE_ALPACA_PORT_SESSIONS: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Read-only Alpaca OPRA/indicative WebSocket port with injected credentials.
/// 使用注入凭证的只读 Alpaca OPRA/indicative WebSocket 端口。
pub struct AlpacaOptionsMarketDataPort<P> {
    environment: StreamEnvironment,
    credentials: Arc<Mutex<P>>,
    raw_frame_sink_factory: Option<Arc<dyn RawFrameSinkFactory>>,
}

impl<P> AlpacaOptionsMarketDataPort<P>
where
    P: CredentialProvider,
{
    /// Creates an allowlisted provider port without loading credentials.
    /// Without [`Self::with_raw_frame_sink_factory`], its raw records are memory-only diagnostics.
    /// 为 allowlist 环境创建供应商端口，但不读取凭证；未配置 [`Self::with_raw_frame_sink_factory`] 时，原始记录仅作内存诊断。
    pub fn new(environment: StreamEnvironment, credential_provider: P) -> Self {
        Self {
            environment,
            credentials: Arc::new(Mutex::new(credential_provider)),
            raw_frame_sink_factory: None,
        }
    }

    /// Requires a trusted sink for each logical subscription instead of memory-only diagnostics.
    /// 为每个逻辑订阅配置可信 sink，避免仅保留内存诊断帧。
    #[must_use]
    pub fn with_raw_frame_sink_factory(mut self, factory: Arc<dyn RawFrameSinkFactory>) -> Self {
        self.raw_frame_sink_factory = Some(factory);
        self
    }
}

impl<P> MarketDataPort for AlpacaOptionsMarketDataPort<P>
where
    P: CredentialProvider,
{
    #[allow(clippy::too_many_lines)] // This boundary validates the full request before exposing one lane.
    fn subscribe(
        &self,
        request: MarketDataSubscriptionRequest,
    ) -> PortFuture<'_, Result<MarketDataSession, BrokerPortError>> {
        let environment = self.environment;
        let credentials = Arc::clone(&self.credentials);
        let raw_frame_sink_factory = self.raw_frame_sink_factory.as_ref().map(Arc::clone);
        Box::pin(async move {
            if request.provider() != "alpaca" {
                return Err(BrokerPortError::UnsupportedSource);
            }
            let feed = match request.feed() {
                "opra" => OptionFeed::Opra,
                "indicative" => OptionFeed::Indicative,
                _ => return Err(BrokerPortError::UnsupportedSource),
            };
            validate_shared_ack_capacity(request.quote_symbols(), request.trade_symbols())?;
            let active_session = active_session_slots()
                .try_acquire_owned()
                .map_err(|_| BrokerPortError::LimitExceeded)?;
            let quotes = request
                .quote_symbols()
                .iter()
                .cloned()
                .map(OptionContractSymbol::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| BrokerPortError::InvalidRequest)?;
            let trades = request
                .trade_symbols()
                .iter()
                .cloned()
                .map(OptionContractSymbol::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| BrokerPortError::InvalidRequest)?;
            let subscriptions = crate::DesiredSubscriptions::new(quotes, trades)
                .map_err(|_| BrokerPortError::InvalidRequest)?;
            let config = StreamConfig::new(environment, feed, subscriptions)
                .map_err(|_| BrokerPortError::InvalidRequest)?;
            let raw_frame_sink = raw_frame_sink_factory
                .as_ref()
                .map(|factory| factory.create_sink(request.provider(), request.feed()))
                .transpose()
                .map_err(map_raw_sink_error)?;
            let mut stream =
                AlpacaOptionsStream::new(config, SharedCredentialProvider(credentials));
            if let Some(sink) = raw_frame_sink {
                stream = stream.with_raw_frame_sink(sink);
            }
            let mut handle = stream.spawn();

            let (records_tx, records_rx) = mpsc::channel(RECORD_LANE_CAPACITY);
            let (cancel_tx, mut cancel_rx) = watch::channel(false);
            tokio::spawn(async move {
                let _active_session = active_session;
                let mut projector = EventProjector::default();
                let mut requested_stop = false;
                let mut failed_status_published = false;
                loop {
                    tokio::select! {
                        changed = cancel_rx.changed() => {
                            if changed.is_err() || *cancel_rx.borrow() {
                                requested_stop = true;
                                handle.cancel();
                                break;
                            }
                        }
                        update = handle.next_event() => {
                            let Some(update) = update else { break };
                            let result = match update {
                                StreamUpdate::Quote(update) => projector
                                    .quote(update, feed)
                                    .map(|(envelope, raw_frame)| {
                                        Some(MarketDataItem::Event { envelope, raw_frame })
                                    }),
                                StreamUpdate::Trade(update) => projector
                                    .trade(update, feed)
                                    .map(|(envelope, raw_frame)| {
                                        Some(MarketDataItem::Event { envelope, raw_frame })
                                    }),
                                StreamUpdate::Control(crate::ControlEvent::RawMarketFrame(frame)) => {
                                    projector.raw_frame(frame, feed).map(|frame| {
                                        Some(MarketDataItem::RawFrame(frame))
                                    })
                                }
                                StreamUpdate::Control(update) => projector
                                    .control(update, feed)
                                    .map(|event| event.map(MarketDataItem::Control)),
                            };
                            match result {
                                Ok(Some(record)) => {
                                    failed_status_published |= is_failure_record(&record);
                                    if !send_with_cancel(&records_tx, record, &mut cancel_rx).await {
                                        requested_stop = true;
                                        handle.cancel();
                                        break;
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    if let Some(failure) = projector.failure(feed) {
                                        let _ = send_with_cancel(
                                            &records_tx,
                                            MarketDataItem::Control(failure),
                                            &mut cancel_rx,
                                        )
                                        .await;
                                    }
                                    requested_stop = true;
                                    handle.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }
                if requested_stop {
                    handle.cancel();
                    let _ = handle.join().await;
                } else {
                    let joined = handle.join().await;
                    if !failed_status_published
                        && !matches!(joined, Ok(crate::SessionExit::Cancelled))
                        && let Some(failure) = projector.failure(feed)
                    {
                        let _ = send_with_cancel(
                            &records_tx,
                            MarketDataItem::Control(failure),
                            &mut cancel_rx,
                        )
                        .await;
                    }
                }
            });

            Ok(MarketDataSession::new(records_rx, cancel_tx))
        })
    }
}

fn map_raw_sink_error(error: RawFrameSinkError) -> BrokerPortError {
    match error {
        RawFrameSinkError::CapacityExceeded => BrokerPortError::Overloaded,
        RawFrameSinkError::Unavailable
        | RawFrameSinkError::Ambiguous
        | RawFrameSinkError::Cancelled
        | RawFrameSinkError::Poisoned => BrokerPortError::Transport,
    }
}

fn active_session_slots() -> Arc<Semaphore> {
    Arc::clone(
        ACTIVE_ALPACA_PORT_SESSIONS
            .get_or_init(|| Arc::new(Semaphore::new(MAX_ACTIVE_ALPACA_PORT_SESSIONS))),
    )
}

fn validate_shared_ack_capacity(
    quote_symbols: &[String],
    trade_symbols: &[String],
) -> Result<(), BrokerPortError> {
    let requested_channel_symbols = quote_symbols
        .len()
        .checked_add(trade_symbols.len())
        .ok_or(BrokerPortError::LimitExceeded)?;
    if requested_channel_symbols > MAX_CONTROL_INSTRUMENTS {
        return Err(BrokerPortError::LimitExceeded);
    }
    Ok(())
}

async fn send_with_cancel<T>(
    sender: &mpsc::Sender<T>,
    value: T,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        sent = sender.send(value) => sent.is_ok(),
        changed = cancel.changed() => changed.is_ok() && !*cancel.borrow(),
    }
}

fn is_failure_record(record: &MarketDataItem) -> bool {
    matches!(
        record,
        MarketDataItem::Control(ControlEventEnvelopeV1 {
            control: MarketControlEventV1::ConnectionStatus {
                state: ConnectionState::Failed
            },
            ..
        })
    )
}

#[derive(Clone)]
struct SharedCredentialProvider<P>(Arc<Mutex<P>>);

impl<P> CredentialProvider for SharedCredentialProvider<P>
where
    P: CredentialProvider,
{
    fn load_credentials(
        &mut self,
    ) -> impl Future<Output = Result<AlpacaCredentials, CredentialFailure>> + Send {
        let provider = Arc::clone(&self.0);
        async move { provider.lock().await.load_credentials().await }
    }
}

#[derive(Default)]
struct EventProjector {
    generation: Option<GenerationState>,
    pending_raw_frames: BTreeMap<(u64, u64), PendingRawFrameLink>,
}

struct PendingRawFrameLink {
    generation: u64,
    capture_instance_id: Option<broker_ports::RawCaptureInstanceId>,
    frame_sha256: String,
    event_count: u32,
    seen_ordinals: BTreeSet<u32>,
}

#[derive(Clone, Copy)]
struct GenerationState {
    source_generation: u64,
    generation: u64,
    sequence: u64,
    source_frame_sequence: u64,
}

const MAX_PENDING_RAW_FRAME_REFERENCES: usize = broker_ports::MAX_IN_FLIGHT_RAW_FRAME_RECORDS;

impl EventProjector {
    fn quote(
        &mut self,
        update: QuoteUpdate,
        feed: OptionFeed,
    ) -> Result<(MarketEventEnvelopeV1, Option<RawFrameReference>), BrokerPortError> {
        self.generation(update.ingest.generation)?;
        if update.coalesced_updates > 0 {
            return Err(BrokerPortError::ProtocolViolation);
        }
        let numeric_encoding = common_encoding(update.quote.bid_price, update.quote.ask_price)?;
        let quote = update.quote;
        let event = MarketEventV1::OptionQuote {
            symbol: quote.symbol.as_str().to_owned(),
            bid: Some(decimal_string(quote.bid_price)?),
            ask: Some(decimal_string(quote.ask_price)?),
            bid_size: Some(
                DecimalString::new(quote.bid_size.to_string())
                    .map_err(|_| BrokerPortError::ProtocolViolation)?,
            ),
            ask_size: Some(
                DecimalString::new(quote.ask_size.to_string())
                    .map_err(|_| BrokerPortError::ProtocolViolation)?,
            ),
        };
        let raw_frame = self.raw_frame_reference(update.ingest, &quote.raw_frame_sha256)?;
        let metadata = self.metadata(
            update.ingest.generation,
            update.ingest.received_at_utc,
            provider_timestamp(&quote.timestamp)?,
            numeric_encoding,
            Some(quote.raw_frame_sha256),
            feed,
        )?;
        let envelope = MarketEventEnvelopeV1 { metadata, event };
        envelope
            .validate()
            .map_err(|_| BrokerPortError::ProtocolViolation)?;
        Ok((envelope, raw_frame))
    }

    fn trade(
        &mut self,
        update: TradeUpdate,
        feed: OptionFeed,
    ) -> Result<(MarketEventEnvelopeV1, Option<RawFrameReference>), BrokerPortError> {
        self.generation(update.ingest.generation)?;
        let trade = update.trade;
        let numeric_encoding = encoding(trade.price);
        let event = MarketEventV1::OptionTrade {
            symbol: trade.symbol.as_str().to_owned(),
            price: decimal_string(trade.price)?,
            size: DecimalString::new(trade.size.to_string())
                .map_err(|_| BrokerPortError::ProtocolViolation)?,
        };
        let raw_frame = self.raw_frame_reference(update.ingest, &trade.raw_frame_sha256)?;
        let metadata = self.metadata(
            update.ingest.generation,
            update.ingest.received_at_utc,
            provider_timestamp(&trade.timestamp)?,
            numeric_encoding,
            Some(trade.raw_frame_sha256),
            feed,
        )?;
        let envelope = MarketEventEnvelopeV1 { metadata, event };
        envelope
            .validate()
            .map_err(|_| BrokerPortError::ProtocolViolation)?;
        Ok((envelope, raw_frame))
    }

    fn raw_frame(
        &mut self,
        frame: crate::InboundRawMarketFrame,
        feed: OptionFeed,
    ) -> Result<RawMarketFrame, BrokerPortError> {
        let source_generation = frame.generation.get();
        let generation = self.generation(frame.generation)?;
        let expected_feed = feed
            .as_str()
            .map_err(|_| BrokerPortError::UnsupportedSource)?;
        let event_count =
            usize::try_from(frame.event_count).map_err(|_| BrokerPortError::ProtocolViolation)?;
        if frame.frame_sequence == 0
            || event_count > MAX_FRAME_MESSAGES
            || !valid_sha256(frame.payload.sha256())
            || frame.payload.as_bytes().len() > broker_ports::MAX_RAW_FRAME_BYTES
            || frame.symbols.len() > MAX_FRAME_MESSAGES
            || frame
                .symbols
                .iter()
                .any(|symbol| symbol.is_empty() || symbol.len() > MAX_OPTION_SYMBOL_BYTES)
            || frame.symbols.windows(2).any(|pair| pair[0] >= pair[1])
            || event_count > 0 && frame.symbols.is_empty()
        {
            return Err(BrokerPortError::ProtocolViolation);
        }
        let generation_state = self
            .generation
            .as_mut()
            .ok_or(BrokerPortError::ProtocolViolation)?;
        if frame.frame_sequence <= generation_state.source_frame_sequence {
            return Err(BrokerPortError::ProtocolViolation);
        }
        if frame.event_count > 0 {
            if self.pending_raw_frames.len() >= MAX_PENDING_RAW_FRAME_REFERENCES {
                return Err(BrokerPortError::LimitExceeded);
            }
            let key = (source_generation, frame.frame_sequence);
            if self
                .pending_raw_frames
                .insert(
                    key,
                    PendingRawFrameLink {
                        generation,
                        capture_instance_id: frame.capture_instance_id,
                        frame_sha256: frame.payload.sha256().to_owned(),
                        event_count: frame.event_count,
                        seen_ordinals: BTreeSet::new(),
                    },
                )
                .is_some()
            {
                return Err(BrokerPortError::ProtocolViolation);
            }
        }
        generation_state.source_frame_sequence = frame.frame_sequence;
        Ok(RawMarketFrame {
            provider: "alpaca".to_owned(),
            feed: expected_feed.to_owned(),
            entitlement: EntitlementState::Unknown,
            capture_instance_id: frame.capture_instance_id,
            wire_encoding: frame.wire_encoding,
            numeric_encoding: frame.numeric_encoding,
            generation,
            frame_sequence: frame.frame_sequence,
            received_timestamp_utc: timestamp_from_datetime(frame.received_at_utc)?,
            event_count: frame.event_count,
            symbols: frame.symbols,
            disposition: frame.disposition,
            payload: frame.payload,
        })
    }

    fn raw_frame_reference(
        &mut self,
        ingest: crate::IngestStamp,
        frame_sha256: &str,
    ) -> Result<Option<RawFrameReference>, BrokerPortError> {
        let has_link = ingest.raw_frame_sequence != 0
            || ingest.raw_frame_event_ordinal != 0
            || ingest.raw_frame_event_count != 0;
        if !has_link {
            return Ok(None);
        }
        if ingest.raw_frame_sequence == 0
            || ingest.raw_frame_event_count == 0
            || ingest.raw_frame_event_ordinal == 0
            || ingest.raw_frame_event_ordinal > ingest.raw_frame_event_count
            || !valid_sha256(frame_sha256)
        {
            return Err(BrokerPortError::ProtocolViolation);
        }
        let generation = self.generation(ingest.generation)?;
        let key = (ingest.generation.get(), ingest.raw_frame_sequence);
        let Some(pending) = self.pending_raw_frames.get_mut(&key) else {
            return Err(BrokerPortError::ProtocolViolation);
        };
        if pending.generation != generation
            || pending.event_count != ingest.raw_frame_event_count
            || pending.frame_sha256 != frame_sha256
            || !pending.seen_ordinals.insert(ingest.raw_frame_event_ordinal)
        {
            return Err(BrokerPortError::ProtocolViolation);
        }
        let complete = pending.seen_ordinals.len()
            == usize::try_from(pending.event_count).map_err(|_| BrokerPortError::LimitExceeded)?;
        let capture_instance_id = pending.capture_instance_id;
        if complete {
            self.pending_raw_frames.remove(&key);
        }
        Ok(Some(RawFrameReference {
            capture_instance_id,
            generation,
            frame_sequence: ingest.raw_frame_sequence,
            event_ordinal: ingest.raw_frame_event_ordinal,
            event_count: ingest.raw_frame_event_count,
            frame_sha256: frame_sha256.to_owned(),
        }))
    }

    #[allow(clippy::too_many_lines)] // All stable control variants share one bounded sequencing and validation path.
    fn control(
        &mut self,
        event: ControlEvent,
        feed: OptionFeed,
    ) -> Result<Option<ControlEventEnvelopeV1>, BrokerPortError> {
        let (generation, control) = match event {
            ControlEvent::RawMarketFrame(_) => return Err(BrokerPortError::ProtocolViolation),
            ControlEvent::PhaseChanged {
                generation,
                phase,
                cause,
            } => {
                let generation_id = self.generation(generation)?;
                let state = match phase {
                    SessionPhase::Connecting => Some(ConnectionState::Connecting),
                    SessionPhase::AwaitingFreshData => Some(ConnectionState::Connected),
                    SessionPhase::SessionLost => Some(ConnectionState::Disconnected),
                    SessionPhase::Closed if cause == Some(SessionStatusCause::Cancelled) => {
                        Some(ConnectionState::Disconnected)
                    }
                    SessionPhase::Closed => Some(ConnectionState::Failed),
                    _ => None,
                };
                let Some(state) = state else {
                    return Ok(None);
                };
                (
                    generation_id,
                    MarketControlEventV1::ConnectionStatus { state },
                )
            }
            ControlEvent::SubscriptionAcknowledged {
                generation,
                acknowledgement,
            } => {
                let generation_id = self.generation(generation)?;
                let acknowledged = acknowledgement
                    .quotes
                    .iter()
                    .chain(acknowledgement.trades.iter())
                    .map(|symbol| symbol.as_str().to_owned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                if acknowledged.is_empty() {
                    return Err(BrokerPortError::ProtocolViolation);
                }
                let request_id = format!("alpaca-session-{generation_id}");
                let subscription_id = format!("local-subscription-{generation_id}");
                (
                    generation_id,
                    MarketControlEventV1::SubscriptionAck {
                        request_id,
                        subscription_id,
                        acknowledged,
                        rejected: Vec::<RejectedInstrumentV1>::new(),
                    },
                )
            }
            ControlEvent::ProviderError { generation, error } => {
                let generation_id = self.generation(generation)?;
                if error.kind == crate::ProviderErrorKind::SubscriptionRejected {
                    let reason_code = provider_reason_code(error.kind);
                    (
                        generation_id,
                        MarketControlEventV1::SubscriptionRejected {
                            request_id: format!("alpaca-session-{generation_id}"),
                            reason_code: reason_code.to_owned(),
                        },
                    )
                } else {
                    (
                        generation_id,
                        MarketControlEventV1::ConnectionStatus {
                            state: ConnectionState::Failed,
                        },
                    )
                }
            }
            ControlEvent::UnknownMessage { generation, .. } => {
                self.generation(generation)?;
                return Err(BrokerPortError::ProtocolViolation);
            }
            ControlEvent::QuoteDiscarded { ingest, .. } => {
                self.generation(ingest.generation)?;
                return Err(BrokerPortError::ProtocolViolation);
            }
        };
        let sequence = self.next_sequence(generation)?;
        let metadata = EventMetadataV1 {
            schema_version: 1,
            source: source(feed, NumericEncodingV1::IntegerToken)?,
            generation,
            sequence,
            raw_frame_sha256: None,
            source_timestamp: None,
            received_timestamp: timestamp_now(),
        };
        metadata
            .validate()
            .map_err(|_| BrokerPortError::ProtocolViolation)?;
        let envelope = ControlEventEnvelopeV1 { metadata, control };
        envelope
            .validate()
            .map_err(|_| BrokerPortError::ProtocolViolation)?;
        Ok(Some(envelope))
    }

    fn failure(&mut self, feed: OptionFeed) -> Option<ControlEventEnvelopeV1> {
        let generation = if let Some(current) = self.generation {
            current.generation
        } else {
            let generation = allocate_generation().ok()?;
            self.generation = Some(GenerationState {
                source_generation: 0,
                generation,
                sequence: 0,
                source_frame_sequence: 0,
            });
            generation
        };
        let sequence = self.next_sequence(generation).ok()?;
        let source = source(feed, NumericEncodingV1::IntegerToken).ok()?;
        let envelope = ControlEventEnvelopeV1 {
            metadata: EventMetadataV1 {
                schema_version: 1,
                source,
                generation,
                sequence,
                raw_frame_sha256: None,
                source_timestamp: None,
                received_timestamp: timestamp_now(),
            },
            control: MarketControlEventV1::ConnectionStatus {
                state: ConnectionState::Failed,
            },
        };
        envelope.validate().ok()?;
        Some(envelope)
    }

    fn metadata(
        &mut self,
        source_generation: SessionGeneration,
        received_at: chrono::DateTime<Utc>,
        source_timestamp: Option<UtcTimestamp>,
        numeric_encoding: NumericEncodingV1,
        raw_frame_sha256: Option<String>,
        feed: OptionFeed,
    ) -> Result<EventMetadataV1, BrokerPortError> {
        let generation = self.generation(source_generation)?;
        let sequence = self.next_sequence(generation)?;
        let source = source(feed, numeric_encoding)?;
        let metadata = EventMetadataV1 {
            schema_version: 1,
            source,
            generation,
            sequence,
            raw_frame_sha256,
            source_timestamp,
            received_timestamp: timestamp_from_datetime(received_at)?,
        };
        metadata
            .validate()
            .map_err(|_| BrokerPortError::ProtocolViolation)?;
        Ok(metadata)
    }

    fn generation(&mut self, source: SessionGeneration) -> Result<u64, BrokerPortError> {
        if let Some(current) = self.generation {
            if source.get() == current.source_generation {
                return Ok(current.generation);
            }
            if source.get() < current.source_generation {
                return Err(BrokerPortError::ProtocolViolation);
            }
            if !self.pending_raw_frames.is_empty() {
                return Err(BrokerPortError::ProtocolViolation);
            }
        }
        let generation = allocate_generation()?;
        self.generation = Some(GenerationState {
            source_generation: source.get(),
            generation,
            sequence: 0,
            source_frame_sequence: 0,
        });
        Ok(generation)
    }

    fn next_sequence(&mut self, generation: u64) -> Result<u64, BrokerPortError> {
        let current = self
            .generation
            .as_mut()
            .filter(|current| current.generation == generation)
            .ok_or(BrokerPortError::ProtocolViolation)?;
        current.sequence = current
            .sequence
            .checked_add(1)
            .ok_or(BrokerPortError::LimitExceeded)?;
        Ok(current.sequence)
    }
}

fn allocate_generation() -> Result<u64, BrokerPortError> {
    NEXT_PORT_GENERATION
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| BrokerPortError::LimitExceeded)
}

fn encoding(number: MarketNumber) -> NumericEncodingV1 {
    number.encoding()
}

fn common_encoding(
    left: MarketNumber,
    right: MarketNumber,
) -> Result<NumericEncodingV1, BrokerPortError> {
    if encoding(left) != encoding(right) {
        return Err(BrokerPortError::ProtocolViolation);
    }
    Ok(encoding(left))
}

fn decimal_string(number: MarketNumber) -> Result<DecimalString, BrokerPortError> {
    number
        .decimal_string()
        .ok_or(BrokerPortError::ProtocolViolation)
}

fn provider_timestamp(
    value: &crate::ProviderTimestamp,
) -> Result<Option<UtcTimestamp>, BrokerPortError> {
    let instant = chrono::DateTime::<Utc>::from_timestamp(value.unix_seconds(), value.nanosecond())
        .ok_or(BrokerPortError::ProtocolViolation)?;
    timestamp_from_datetime(instant).map(Some)
}

fn timestamp_from_datetime(value: chrono::DateTime<Utc>) -> Result<UtcTimestamp, BrokerPortError> {
    UtcTimestamp::parse(&value.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .map_err(|_| BrokerPortError::ProtocolViolation)
}

fn timestamp_now() -> UtcTimestamp {
    timestamp_from_datetime(chrono::DateTime::<Utc>::from(SystemTime::now()))
        .expect("current UTC timestamp is valid")
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn source(
    feed: OptionFeed,
    numeric_encoding: NumericEncodingV1,
) -> Result<MarketDataSourceV1, BrokerPortError> {
    let feed = feed
        .as_str()
        .map_err(|_| BrokerPortError::UnsupportedSource)?;
    MarketDataSourceV1::new(
        "alpaca",
        feed,
        EntitlementState::Unknown,
        numeric_encoding,
        None,
    )
    .map_err(|_| BrokerPortError::ProtocolViolation)
}

fn provider_reason_code(kind: crate::ProviderErrorKind) -> &'static str {
    match kind {
        crate::ProviderErrorKind::Authentication => "authentication_rejected",
        crate::ProviderErrorKind::ConnectionLimit => "connection_limit",
        crate::ProviderErrorKind::SubscriptionRejected => "subscription_rejected",
        crate::ProviderErrorKind::MessagePackRequired => "messagepack_required",
        crate::ProviderErrorKind::SlowClient => "slow_client",
        crate::ProviderErrorKind::Other => "provider_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataFreshness, InboundRawMarketFrame, OptionContractSymbol, OptionQuote, ProviderTimestamp,
    };
    use broker_ports::{RawFrameDisposition, RawFramePayload};

    fn raw_frame(
        frame_sequence: u64,
        event_count: u32,
        symbols: Vec<String>,
        payload: RawFramePayload,
    ) -> InboundRawMarketFrame {
        InboundRawMarketFrame {
            capture_instance_id: None,
            generation: SessionGeneration::new(1),
            frame_sequence,
            received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
            wire_encoding: broker_ports::RawFrameWireEncoding::MessagePack,
            event_count,
            symbols,
            numeric_encoding: None,
            disposition: RawFrameDisposition::DecodedMarketData,
            payload,
        }
    }

    fn quote() -> OptionQuote {
        OptionQuote {
            symbol: OptionContractSymbol::new("QQQ261218C00500000")
                .expect("synthetic provider symbol is valid"),
            timestamp: ProviderTimestamp::parse("2026-10-08T14:30:00.123456789Z")
                .expect("synthetic source timestamp is valid"),
            bid_exchange: "X".to_owned(),
            bid_price: MarketNumber::Float32(1.2),
            bid_size: 3,
            ask_exchange: "Y".to_owned(),
            ask_price: MarketNumber::Float32(1.3),
            ask_size: 4,
            conditions: Vec::new(),
            raw_frame_sha256: "a1".repeat(32),
        }
    }

    #[test]
    fn float32_projection_uses_shortest_roundtrip_decimal_and_preserves_raw_hash() {
        let projected = decimal_string(MarketNumber::Float32(1.2))
            .expect("finite provider value projects to a decimal token");
        assert_eq!(projected.as_str(), "1.2");
        let mut projector = EventProjector::default();
        let (event, raw_frame) = projector
            .quote(
                QuoteUpdate {
                    quote: quote(),
                    feed: OptionFeed::Opra,
                    ingest: crate::IngestStamp {
                        generation: SessionGeneration::new(7),
                        sequence: 9,
                        raw_frame_sequence: 0,
                        raw_frame_event_ordinal: 0,
                        raw_frame_event_count: 0,
                        received_at: tokio::time::Instant::now(),
                        received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
                    },
                    freshness: DataFreshness::Fresh,
                    coalesced_updates: 0,
                },
                OptionFeed::Opra,
            )
            .expect("shared quote contract is valid");
        assert!(raw_frame.is_none());
        assert_eq!(
            event.metadata.source.numeric_encoding,
            NumericEncodingV1::BinaryFloat32ShortestDecimal
        );
        assert_eq!(
            event.metadata.raw_frame_sha256.as_deref(),
            Some("a1".repeat(32).as_str())
        );
        assert_eq!(
            event.metadata.source_timestamp.as_ref().unwrap().as_str(),
            "2026-10-08T14:30:00.123456789Z"
        );
    }

    #[test]
    fn quote_rejects_mixed_wire_number_categories() {
        let mut quote = quote();
        quote.ask_price = MarketNumber::Float64(1.3);
        let mut projector = EventProjector::default();
        let error = projector.quote(
            QuoteUpdate {
                quote,
                feed: OptionFeed::Opra,
                ingest: crate::IngestStamp {
                    generation: SessionGeneration::new(1),
                    sequence: 1,
                    raw_frame_sequence: 0,
                    raw_frame_event_ordinal: 0,
                    raw_frame_event_count: 0,
                    received_at: tokio::time::Instant::now(),
                    received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
                },
                freshness: DataFreshness::Fresh,
                coalesced_updates: 0,
            },
            OptionFeed::Opra,
        );
        assert!(matches!(error, Err(BrokerPortError::ProtocolViolation)));
    }

    #[test]
    fn quote_coalescing_cannot_be_projected_as_complete_archive_input() {
        let mut projector = EventProjector::default();
        let error = projector.quote(
            QuoteUpdate {
                quote: quote(),
                feed: OptionFeed::Opra,
                ingest: crate::IngestStamp {
                    generation: SessionGeneration::new(1),
                    sequence: 1,
                    raw_frame_sequence: 0,
                    raw_frame_event_ordinal: 0,
                    raw_frame_event_count: 0,
                    received_at: tokio::time::Instant::now(),
                    received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
                },
                freshness: DataFreshness::Fresh,
                coalesced_updates: 1,
            },
            OptionFeed::Opra,
        );
        assert_eq!(error, Err(BrokerPortError::ProtocolViolation));
    }

    #[test]
    fn unknown_provider_control_fails_closed_for_canonical_ingestion() {
        let mut projector = EventProjector::default();
        let error = projector.control(
            ControlEvent::UnknownMessage {
                generation: SessionGeneration::new(1),
                type_tag: "future".to_owned(),
            },
            OptionFeed::Opra,
        );
        assert_eq!(error, Err(BrokerPortError::ProtocolViolation));

        let error = projector.control(
            ControlEvent::QuoteDiscarded {
                symbol: OptionContractSymbol::new("QQQ261218C00500000")
                    .expect("synthetic symbol is valid"),
                provider_timestamp: ProviderTimestamp::parse("2026-10-08T14:30:00Z")
                    .expect("synthetic timestamp is valid"),
                ingest: crate::IngestStamp {
                    generation: SessionGeneration::new(1),
                    sequence: 1,
                    raw_frame_sequence: 0,
                    raw_frame_event_ordinal: 0,
                    raw_frame_event_count: 0,
                    received_at: tokio::time::Instant::now(),
                    received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
                },
                reason: crate::QuoteDiscardReason::Stale,
            },
            OptionFeed::Opra,
        );
        assert_eq!(error, Err(BrokerPortError::ProtocolViolation));
    }

    #[test]
    fn failure_control_keeps_the_active_generation_and_sequence() {
        let mut projector = EventProjector::default();
        let connected = projector
            .control(
                ControlEvent::PhaseChanged {
                    generation: SessionGeneration::new(4),
                    phase: SessionPhase::Connecting,
                    cause: None,
                },
                OptionFeed::Opra,
            )
            .expect("connecting control converts")
            .expect("connecting state is visible");
        let failed = projector
            .failure(OptionFeed::Opra)
            .expect("failure control fits the shared contract");
        assert_eq!(failed.metadata.generation, connected.metadata.generation);
        assert_eq!(failed.metadata.sequence, connected.metadata.sequence + 1);
        assert!(matches!(
            failed.control,
            MarketControlEventV1::ConnectionStatus {
                state: ConnectionState::Failed
            }
        ));
    }

    #[test]
    fn local_ack_correlators_change_with_generation() {
        let acknowledgement =
            || crate::SubscriptionAcknowledgement {
                quotes: [OptionContractSymbol::new("QQQ261218C00500000")
                    .expect("synthetic symbol is valid")]
                .into_iter()
                .collect(),
                trades: [OptionContractSymbol::new("QQQ261218C00500000")
                    .expect("synthetic symbol is valid")]
                .into_iter()
                .collect(),
            };
        let mut projector = EventProjector::default();
        let mut ids = Vec::new();
        for generation in [SessionGeneration::new(8), SessionGeneration::new(9)] {
            let envelope = projector
                .control(
                    ControlEvent::SubscriptionAcknowledged {
                        generation,
                        acknowledgement: acknowledgement(),
                    },
                    OptionFeed::Opra,
                )
                .expect("validated source acknowledgement projects")
                .expect("acknowledgement is emitted");
            let MarketControlEventV1::SubscriptionAck {
                request_id,
                subscription_id,
                acknowledged,
                ..
            } = envelope.control
            else {
                panic!("expected shared v1 subscription ack");
            };
            assert_eq!(acknowledged.len(), 1);
            ids.push((request_id, subscription_id));
        }
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn projector_keeps_only_current_generation_and_rejects_old_controls() {
        let mut projector = EventProjector::default();
        let first = projector
            .generation(SessionGeneration::new(1))
            .expect("first generation accepted");
        let second = projector
            .generation(SessionGeneration::new(2))
            .expect("new generation replaces prior state");
        assert_ne!(first, second);
        assert_eq!(
            projector.generation(SessionGeneration::new(1)),
            Err(BrokerPortError::ProtocolViolation)
        );
        assert_eq!(projector.generation.unwrap().source_generation, 2);
    }

    #[test]
    fn raw_frame_sequences_are_unique_even_when_frames_have_no_market_events() {
        let payload = RawFramePayload::capture(Vec::new()).expect("empty synthetic payload");
        let mut projector = EventProjector::default();
        projector
            .raw_frame(
                raw_frame(1, 0, Vec::new(), payload.clone()),
                OptionFeed::Opra,
            )
            .expect("first diagnostic frame is accepted");
        assert_eq!(
            projector.raw_frame(raw_frame(1, 0, Vec::new(), payload), OptionFeed::Opra,),
            Err(BrokerPortError::ProtocolViolation)
        );
    }

    #[test]
    fn capture_identity_flows_from_raw_frame_to_event_reference() {
        let mut capture_bytes = [0x51; 16];
        capture_bytes[6] = 0x45;
        capture_bytes[8] = 0x95;
        let capture_id = broker_ports::RawCaptureInstanceId::new(capture_bytes)
            .expect("synthetic UUIDv4 capture ID");
        let payload =
            RawFramePayload::capture(b"synthetic-frame".to_vec()).expect("bounded synthetic frame");
        let mut input = raw_frame(1, 1, vec!["QQQ261218C00500000".to_owned()], payload.clone());
        input.capture_instance_id = Some(capture_id);
        let mut projector = EventProjector::default();
        let projected = projector
            .raw_frame(input, OptionFeed::Opra)
            .expect("captured frame is projected");
        assert_eq!(projected.capture_instance_id, Some(capture_id));
        assert_eq!(
            projected.wire_encoding,
            broker_ports::RawFrameWireEncoding::MessagePack
        );

        let mut quote = quote();
        quote.raw_frame_sha256 = payload.sha256().to_owned();
        let (_, raw_reference) = projector
            .quote(
                QuoteUpdate {
                    quote,
                    feed: OptionFeed::Opra,
                    ingest: crate::IngestStamp {
                        generation: SessionGeneration::new(1),
                        sequence: 1,
                        raw_frame_sequence: 1,
                        raw_frame_event_ordinal: 1,
                        raw_frame_event_count: 1,
                        received_at: tokio::time::Instant::now(),
                        received_at_utc: chrono::DateTime::<Utc>::from(SystemTime::now()),
                    },
                    freshness: DataFreshness::Fresh,
                    coalesced_updates: 0,
                },
                OptionFeed::Opra,
            )
            .expect("event references the exact captured frame");
        assert_eq!(
            raw_reference.expect("raw link exists").capture_instance_id,
            Some(capture_id)
        );
    }

    #[test]
    fn raw_frame_event_count_and_pending_reference_state_are_hard_bounded() {
        let payload = RawFramePayload::capture(Vec::new()).expect("empty synthetic payload");
        let symbols = vec!["QQQ261218C00500000".to_owned()];
        let mut too_many_events = EventProjector::default();
        assert_eq!(
            too_many_events.raw_frame(
                raw_frame(
                    1,
                    u32::try_from(MAX_FRAME_MESSAGES + 1).expect("small protocol bound"),
                    symbols.clone(),
                    payload.clone(),
                ),
                OptionFeed::Opra,
            ),
            Err(BrokerPortError::ProtocolViolation)
        );

        let mut projector = EventProjector::default();
        for frame_sequence in 1..=MAX_PENDING_RAW_FRAME_REFERENCES {
            projector
                .raw_frame(
                    raw_frame(
                        u64::try_from(frame_sequence).expect("bounded frame count"),
                        1,
                        symbols.clone(),
                        payload.clone(),
                    ),
                    OptionFeed::Opra,
                )
                .expect("pending reference remains within its fixed cap");
        }
        assert_eq!(
            projector.pending_raw_frames.len(),
            MAX_PENDING_RAW_FRAME_REFERENCES
        );
        assert_eq!(
            projector.raw_frame(
                raw_frame(
                    u64::try_from(MAX_PENDING_RAW_FRAME_REFERENCES + 1)
                        .expect("bounded frame count"),
                    1,
                    symbols,
                    payload,
                ),
                OptionFeed::Opra,
            ),
            Err(BrokerPortError::LimitExceeded)
        );
    }

    #[test]
    fn control_ack_projection_preserves_ack_and_separates_connection_state() {
        let generation = SessionGeneration::new(11);
        let mut projector = EventProjector::default();
        let connecting = projector
            .control(
                ControlEvent::PhaseChanged {
                    generation,
                    phase: SessionPhase::Connecting,
                    cause: None,
                },
                OptionFeed::Opra,
            )
            .expect("connecting control maps")
            .expect("connecting status is exported");
        let acknowledgement = projector
            .control(
                ControlEvent::SubscriptionAcknowledged {
                    generation,
                    acknowledgement: crate::SubscriptionAcknowledgement {
                        quotes: [OptionContractSymbol::new("QQQ261218C00500000")
                            .expect("synthetic symbol is valid")]
                        .into_iter()
                        .collect(),
                        trades: [
                            OptionContractSymbol::new("QQQ261218C00500000")
                                .expect("same symbol may use both channels"),
                            OptionContractSymbol::new("QQQ261218P00500000")
                                .expect("synthetic symbol is valid"),
                        ]
                        .into_iter()
                        .collect(),
                    },
                },
                OptionFeed::Opra,
            )
            .expect("acknowledgement maps")
            .expect("subscription ack is exported");
        assert!(matches!(
            connecting.control,
            MarketControlEventV1::ConnectionStatus {
                state: ConnectionState::Connecting
            }
        ));
        let MarketControlEventV1::SubscriptionAck {
            acknowledged,
            rejected,
            request_id,
            subscription_id,
        } = acknowledgement.control
        else {
            panic!("expected typed subscription acknowledgement");
        };
        assert_eq!(acknowledged.len(), 2);
        assert!(rejected.is_empty());
        assert!(request_id.starts_with("alpaca-session-"));
        assert!(subscription_id.starts_with("local-subscription-"));
        assert!(acknowledgement.metadata.sequence > connecting.metadata.sequence);
    }

    #[test]
    fn shared_v1_acknowledgement_cap_counts_channel_symbol_pairs() {
        let quote_symbols = (0..=MAX_CONTROL_INSTRUMENTS)
            .map(|index| format!("QQQ261218C{index:06}"))
            .collect::<Vec<_>>();
        assert_eq!(
            validate_shared_ack_capacity(&quote_symbols, &[]),
            Err(BrokerPortError::LimitExceeded)
        );

        let same_contract = "QQQ261218C00500000".to_owned();
        assert_eq!(
            validate_shared_ack_capacity(
                std::slice::from_ref(&same_contract),
                std::slice::from_ref(&same_contract),
            ),
            Ok(())
        );

        let quotes = (0..MAX_CONTROL_INSTRUMENTS / 2)
            .map(|index| format!("QQQ261218C{index:06}"))
            .collect::<Vec<_>>();
        let trades = (0..=MAX_CONTROL_INSTRUMENTS / 2)
            .map(|index| format!("QQQ261218P{index:06}"))
            .collect::<Vec<_>>();
        assert_eq!(
            validate_shared_ack_capacity(&quotes, &trades),
            Err(BrokerPortError::LimitExceeded)
        );

        let overlapping = (0..MAX_CONTROL_INSTRUMENTS / 2)
            .map(|index| format!("QQQ261218C{index:06}"))
            .collect::<Vec<_>>();
        assert_eq!(
            validate_shared_ack_capacity(&overlapping, &overlapping),
            Ok(())
        );
        let over_limit = (0..=MAX_CONTROL_INSTRUMENTS / 2)
            .map(|index| format!("QQQ261218C{index:06}"))
            .collect::<Vec<_>>();
        assert_eq!(
            validate_shared_ack_capacity(&over_limit, &overlapping),
            Err(BrokerPortError::LimitExceeded)
        );
    }

    #[test]
    fn process_wide_session_gate_rejects_a_second_active_subscription() {
        let slots = active_session_slots();
        let first = Arc::clone(&slots)
            .try_acquire_owned()
            .expect("the process-wide session slot should be initially available");
        assert!(Arc::clone(&slots).try_acquire_owned().is_err());
        drop(first);
        assert!(Arc::clone(&slots).try_acquire_owned().is_ok());
    }
}
