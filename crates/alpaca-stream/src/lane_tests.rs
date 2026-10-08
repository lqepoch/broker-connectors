use tokio::time::Instant;

use super::*;
use crate::config::{DesiredSubscriptions, StreamEnvironment};
use crate::model::MarketNumber;
use crate::state::ReconnectPolicy;

const SYMBOL: &str = "AAPL260123C00150000";

fn config() -> StreamConfig {
    let symbol = OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid");
    let subscriptions =
        DesiredSubscriptions::new([symbol.clone()], [symbol]).expect("subscriptions valid");
    StreamConfig::with_limits_and_reconnect(
        StreamEnvironment::Production,
        OptionFeed::Opra,
        subscriptions,
        crate::config::StreamLimits::default(),
        ReconnectPolicy::new(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
            1,
            0,
        )
        .expect("retry policy is bounded"),
    )
    .expect("synthetic stream config is valid")
}

fn quote_update(
    provider_timestamp: &str,
    generation: u64,
    sequence: u64,
    price: f64,
    freshness: DataFreshness,
) -> QuoteUpdate {
    QuoteUpdate {
        quote: OptionQuote {
            symbol: OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid"),
            timestamp: ProviderTimestamp::parse(provider_timestamp)
                .expect("synthetic timestamp is valid"),
            bid_exchange: "X".to_owned(),
            bid_price: MarketNumber::Float64(price),
            bid_size: 1,
            ask_exchange: "Y".to_owned(),
            ask_price: MarketNumber::Float64(price + 0.25),
            ask_size: 1,
            conditions: Vec::new(),
            raw_frame_sha256: "00".repeat(32),
        },
        feed: OptionFeed::Opra,
        ingest: IngestStamp {
            generation: SessionGeneration::new(generation),
            sequence,
            raw_frame_sequence: 0,
            raw_frame_event_ordinal: 0,
            raw_frame_event_count: 0,
            received_at: Instant::now(),
            received_at_utc: chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()),
        },
        freshness,
        coalesced_updates: 0,
    }
}

#[tokio::test]
async fn identical_trades_with_equal_provider_timestamps_remain_fifo() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    let generation = SessionGeneration::new(1);
    publishers.begin_generation(generation).await;

    let trade = OptionTrade {
        symbol: OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid"),
        timestamp: ProviderTimestamp::parse("2026-06-01T12:00:00Z")
            .expect("synthetic timestamp is valid"),
        price: MarketNumber::Float64(1.25),
        size: 3,
        exchange: "X".to_owned(),
        conditions: Vec::new(),
        raw_frame_sha256: "00".repeat(32),
    };

    for sequence in [41, 42] {
        publishers
            .trade(TradeUpdate {
                trade: trade.clone(),
                feed: OptionFeed::Opra,
                ingest: IngestStamp {
                    generation,
                    sequence,
                    raw_frame_sequence: 0,
                    raw_frame_event_ordinal: 0,
                    raw_frame_event_count: 0,
                    received_at: Instant::now(),
                    received_at_utc: chrono::DateTime::<chrono::Utc>::from(
                        std::time::SystemTime::now(),
                    ),
                },
                freshness: DataFreshness::Fresh,
            })
            .await
            .expect("synthetic trade is queued");
    }

    let first = receivers
        .trades
        .recv()
        .await
        .expect("first identical trade remains queued");
    let second = receivers
        .trades
        .recv()
        .await
        .expect("second identical trade remains queued");

    assert_eq!(first.trade, trade);
    assert_eq!(second.trade, trade);
    assert_eq!(first.trade, second.trade);
    assert_eq!([first.ingest.sequence, second.ingest.sequence], [41, 42]);
}

#[tokio::test]
async fn pending_quotes_order_by_provider_time_then_local_ingest_tie_break() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    let generation = SessionGeneration::new(1);
    publishers.begin_generation(generation).await;

    publishers
        .quote(quote_update(
            "2026-06-01T12:00:00Z",
            1,
            10,
            1.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("first quote is queued");
    publishers
        .quote(quote_update(
            "2026-06-01T11:59:59Z",
            1,
            11,
            2.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("older provider evidence is reported, not fatal");
    publishers
        .quote(quote_update(
            "2026-06-01T07:00:00-05:00",
            1,
            9,
            3.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("equal-time older local ingest is reported");
    publishers
        .quote(quote_update(
            "2026-06-01T07:00:00-05:00",
            1,
            11,
            4.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("equal provider time accepts a newer local ingest");
    publishers
        .quote(quote_update(
            "2026-06-01T12:00:01Z",
            1,
            10,
            5.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("newer provider evidence wins regardless of local sequence");

    let pending = receivers
        .quotes
        .inner
        .state
        .lock()
        .await
        .pending
        .get(&OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid"))
        .cloned()
        .expect("one coalesced quote remains pending");
    assert_eq!(pending.quote.bid_price, MarketNumber::Float64(5.0));
    assert_eq!(pending.ingest.sequence, 10);
    assert_eq!(pending.coalesced_updates, 2);

    let mut discarded = Vec::new();
    for _ in 0..2 {
        let event = receivers
            .controls
            .recv()
            .await
            .expect("discard remains observable on control lane");
        if let ControlEvent::QuoteDiscarded { reason, .. } = event {
            discarded.push(reason);
        } else {
            panic!("only discarded-quote events were expected");
        }
    }
    assert_eq!(
        discarded,
        vec![
            QuoteDiscardReason::OlderProviderTimestamp,
            QuoteDiscardReason::EqualTimestampNotNewerIngest,
        ]
    );
}

#[tokio::test]
async fn duplicate_provider_time_and_local_ingest_sequence_is_observably_rejected() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    publishers.begin_generation(SessionGeneration::new(1)).await;
    let quote = quote_update("2026-06-01T12:00:00Z", 1, 7, 1.0, DataFreshness::Fresh);

    publishers
        .quote(quote.clone())
        .await
        .expect("first quote with this timestamp and ingest sequence is queued");
    publishers
        .quote(quote)
        .await
        .expect("duplicate timestamp and ingest sequence is reported");

    let pending = receivers
        .quotes
        .inner
        .state
        .lock()
        .await
        .pending
        .get(&OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid"))
        .cloned()
        .expect("the original quote remains pending");
    assert_eq!(pending.quote.bid_price, MarketNumber::Float64(1.0));
    assert_eq!(pending.ingest.sequence, 7);
    assert_eq!(pending.coalesced_updates, 0);
    assert!(matches!(
        receivers.controls.recv().await,
        Some(ControlEvent::QuoteDiscarded {
            reason: QuoteDiscardReason::EqualTimestampNotNewerIngest,
            ingest: IngestStamp {
                generation,
                sequence: 7,
                ..
            },
            ..
        }) if generation == SessionGeneration::new(1)
    ));
}

#[tokio::test]
async fn stale_and_future_dated_quotes_are_observable_and_never_queued() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    publishers.begin_generation(SessionGeneration::new(1)).await;

    publishers
        .quote(quote_update(
            "2020-01-01T00:00:00Z",
            1,
            1,
            1.0,
            DataFreshness::Stale,
        ))
        .await
        .expect("stale quote discard is reported");
    publishers
        .quote(quote_update(
            "2099-01-01T00:00:00Z",
            1,
            2,
            2.0,
            DataFreshness::FutureDated,
        ))
        .await
        .expect("future quote discard is reported");

    assert!(receivers.quotes.inner.state.lock().await.pending.is_empty());
    let first = receivers
        .controls
        .recv()
        .await
        .expect("stale discard event");
    let second = receivers
        .controls
        .recv()
        .await
        .expect("future discard event");
    assert!(matches!(
        first,
        ControlEvent::QuoteDiscarded {
            reason: QuoteDiscardReason::Stale,
            ..
        }
    ));
    assert!(matches!(
        second,
        ControlEvent::QuoteDiscarded {
            reason: QuoteDiscardReason::FutureDated,
            ..
        }
    ));
}

#[tokio::test]
async fn older_quote_is_rejected_even_after_newer_quote_left_the_pending_lane() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    publishers.begin_generation(SessionGeneration::new(1)).await;
    publishers
        .quote(quote_update(
            "2026-06-01T12:00:00Z",
            1,
            1,
            1.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("newer provider evidence is queued");

    let delivered = receivers
        .quotes
        .recv()
        .await
        .expect("newer provider evidence reaches the consumer");
    assert_eq!(delivered.quote.bid_price, MarketNumber::Float64(1.0));

    publishers
        .quote(quote_update(
            "2026-06-01T11:59:59Z",
            1,
            2,
            2.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("older evidence is observably discarded");
    assert!(receivers.quotes.inner.state.lock().await.pending.is_empty());
    assert!(matches!(
        receivers.controls.recv().await,
        Some(ControlEvent::QuoteDiscarded {
            reason: QuoteDiscardReason::OlderProviderTimestamp,
            ..
        })
    ));
}

#[tokio::test]
async fn late_old_generation_quote_is_reported_without_changing_current_pending_data() {
    let (publishers, mut receivers, _) = create_lanes(&config());
    publishers.begin_generation(SessionGeneration::new(1)).await;
    publishers
        .quote(quote_update(
            "2026-06-01T12:00:00Z",
            1,
            1,
            1.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("generation one quote is queued");

    publishers.begin_generation(SessionGeneration::new(2)).await;
    publishers
        .quote(quote_update(
            "2026-06-01T11:00:00Z",
            2,
            1,
            2.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("generation two quote is queued");
    publishers
        .quote(quote_update(
            "2026-06-01T12:01:00Z",
            1,
            2,
            9.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("late generation one quote is reported");

    let current = receivers
        .quotes
        .recv()
        .await
        .expect("current generation quote remains available");
    assert_eq!(current.ingest.generation, SessionGeneration::new(2));
    assert_eq!(current.quote.bid_price, MarketNumber::Float64(2.0));
    assert!(matches!(
        receivers.controls.recv().await,
        Some(ControlEvent::QuoteDiscarded {
            reason: QuoteDiscardReason::OldGeneration,
            ingest: IngestStamp { generation, .. },
            ..
        }) if generation == SessionGeneration::new(1)
    ));
}

#[tokio::test]
async fn unified_receiver_preserves_both_lanes_and_finishes_when_closed() {
    let (publishers, receivers, _) = create_lanes(&config());
    let prior_generation = SessionGeneration::new(1);
    let generation = SessionGeneration::new(2);
    publishers.begin_generation(prior_generation).await;
    publishers
        .quote(quote_update(
            "2026-06-01T12:00:00Z",
            1,
            1,
            1.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("prior quote is queued before reconnect");
    publishers
        .control(ControlEvent::PhaseChanged {
            generation: prior_generation,
            phase: SessionPhase::SessionLost,
            cause: None,
        })
        .expect("prior generation loss is queued");
    publishers.begin_generation(generation).await;
    publishers
        .quote(quote_update(
            "2026-06-01T12:00:01Z",
            2,
            2,
            2.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("new-generation quote is queued alongside reconnect status");
    publishers
        .control(ControlEvent::PhaseChanged {
            generation,
            phase: SessionPhase::Connecting,
            cause: None,
        })
        .expect("new-generation connecting status is queued");
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async { Ok(crate::SessionExit::Cancelled) });
    let mut handle = receivers.into_handle(shutdown_tx, task);

    let lost = handle
        .next_update()
        .await
        .expect("prior control is delivered");
    let connecting = handle
        .next_update()
        .await
        .expect("new control is delivered");
    let quote = handle.next_update().await.expect("new quote is delivered");
    assert!(matches!(
        lost,
        StreamUpdate::Control(ControlEvent::PhaseChanged {
            generation: observed,
            phase: SessionPhase::SessionLost,
            ..
        }) if observed == prior_generation
    ));
    assert!(matches!(
        connecting,
        StreamUpdate::Control(ControlEvent::PhaseChanged {
            generation: observed,
            phase: SessionPhase::Connecting,
            ..
        }) if observed == generation
    ));
    assert!(matches!(
        quote,
        StreamUpdate::Quote(QuoteUpdate { ingest, .. }) if ingest.generation == generation
    ));

    publishers.close().await;
    drop(publishers);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), handle.next_update())
            .await
            .expect("closed lanes do not leave a pending receiver")
            .is_none()
    );
}

#[tokio::test]
async fn unified_event_receiver_preserves_quote_trade_and_control_lanes() {
    let (publishers, receivers, _) = create_lanes(&config());
    let generation = SessionGeneration::new(3);
    publishers.begin_generation(generation).await;
    publishers
        .quote(quote_update(
            "2026-10-08T14:30:00Z",
            generation.get(),
            1,
            1.0,
            DataFreshness::Fresh,
        ))
        .await
        .expect("synthetic quote is queued");
    publishers
        .trade(TradeUpdate {
            trade: OptionTrade {
                symbol: OptionContractSymbol::new(SYMBOL).expect("synthetic symbol is valid"),
                timestamp: ProviderTimestamp::parse("2026-10-08T14:30:00Z")
                    .expect("synthetic timestamp is valid"),
                price: MarketNumber::Float64(1.25),
                size: 2,
                exchange: "X".to_owned(),
                conditions: Vec::new(),
                raw_frame_sha256: "11".repeat(32),
            },
            feed: OptionFeed::Opra,
            ingest: IngestStamp {
                generation,
                sequence: 2,
                raw_frame_sequence: 0,
                raw_frame_event_ordinal: 0,
                raw_frame_event_count: 0,
                received_at: Instant::now(),
                received_at_utc: chrono::DateTime::<chrono::Utc>::from(
                    std::time::SystemTime::now(),
                ),
            },
            freshness: DataFreshness::Fresh,
        })
        .await
        .expect("synthetic trade is queued");
    publishers
        .control(ControlEvent::PhaseChanged {
            generation,
            phase: SessionPhase::Connecting,
            cause: None,
        })
        .expect("synthetic control is queued");
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async { Ok(crate::SessionExit::Cancelled) });
    let mut handle = receivers.into_handle(shutdown_tx, task);

    let mut quote_seen = false;
    let mut trade_seen = false;
    let mut control_seen = false;
    for _ in 0..3 {
        match handle.next_event().await.expect("one lane remains open") {
            StreamUpdate::Quote(update) => quote_seen = update.ingest.generation == generation,
            StreamUpdate::Trade(update) => trade_seen = update.ingest.generation == generation,
            StreamUpdate::Control(ControlEvent::PhaseChanged {
                generation: observed,
                phase: SessionPhase::Connecting,
                ..
            }) => control_seen = observed == generation,
            StreamUpdate::Control(_) => {}
        }
    }
    assert!(quote_seen && trade_seen && control_seen);

    publishers.close().await;
    drop(publishers);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), handle.next_event())
            .await
            .expect("closed lanes do not leave a pending receiver")
            .is_none()
    );
}
