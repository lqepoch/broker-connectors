include!("session_test_support.rs");

use std::fmt::Write as _;

use broker_ports::RawFrameDisposition;
use sha2::{Digest, Sha256};

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_pending_credential_load_before_deadline_and_closes_socket() {
    let authentication_timeout = Duration::from_secs(10);
    let configuration =
        config_with_authentication_timeout(4, 2, Duration::from_secs(1), authentication_timeout);
    let (connector, metrics) = connector([socket_script(vec![success("connected")], true)]);
    let (mut running, credential_observations) = start_with_pending_credentials(
        configuration,
        connector,
        SystemFreshnessClock,
        TEST_RETRY_SEED,
    );

    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), running.receivers.controls.recv())
            .await
            .expect("credential loading begins before the authentication deadline")
            .expect("control lane remains open");
        if matches!(
            event,
            ControlEvent::PhaseChanged {
                phase: SessionPhase::Authenticating,
                ..
            }
        ) {
            break;
        }
    }

    let polls_at_cancellation = credential_observations.polls.load(Ordering::SeqCst);
    assert!(polls_at_cancellation > 0);
    assert_eq!(credential_observations.calls.load(Ordering::SeqCst), 1);
    let cancellation_started = Instant::now();
    running.shutdown.send_replace(true);

    assert_eq!(
        tokio::time::timeout(authentication_timeout, &mut running.task)
            .await
            .expect("cancellation returns before the authentication deadline")
            .expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
    assert!(Instant::now().duration_since(cancellation_started) < authentication_timeout);
    assert_eq!(credential_observations.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        credential_observations.polls.load(Ordering::SeqCst),
        polls_at_cancellation
    );
    assert_eq!(credential_observations.drops.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.active_sockets.load(Ordering::SeqCst), 0);
    assert!(metrics.sends.lock().expect("fake send lock").is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_pending_connect_before_deadline_without_accepting_a_socket() {
    let connect_timeout = Duration::from_secs(30);
    let mut configuration = config(4, 2, Duration::from_secs(1));
    configuration.limits.connect_timeout = connect_timeout;
    let (connector, metrics) = connector([ConnectScript::Pending]);
    let (running, credential_calls) = start(configuration, connector);

    while metrics.connects.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let cancellation_started = Instant::now();
    running.shutdown.send_replace(true);

    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
    assert!(Instant::now().duration_since(cancellation_started) < connect_timeout);
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.active_sockets.load(Ordering::SeqCst), 0);
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 0);
    assert!(metrics.sends.lock().expect("fake send lock").is_empty());
    assert_eq!(credential_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn reconnect_invalidates_old_generation_and_requires_fresh_data_again() {
    let mut first = acknowledged_frames(true);
    first.push(frame([quote(1.0)]));
    let mut second = acknowledged_frames(true);
    second.push(frame([quote(2.0)]));
    let (connector, metrics) =
        connector([socket_script(first, false), socket_script(second, true)]);
    let (mut running, _) = start(config(4, 1, Duration::from_secs(1)), connector);

    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), running.receivers.controls.recv())
            .await
            .expect("second generation should be attempted")
            .expect("control lane remains open");
        if matches!(
            event,
            ControlEvent::PhaseChanged {
                generation,
                phase: SessionPhase::Connecting,
                ..
            } if generation.get() == 2
        ) {
            break;
        }
    }

    let update = tokio::time::timeout(Duration::from_secs(5), running.receivers.quotes.recv())
        .await
        .expect("fresh quote from replacement generation")
        .expect("quote lane remains open");
    assert_eq!(update.ingest.generation.get(), 2);
    assert_eq!(
        update.quote.bid_price,
        crate::model::MarketNumber::Float64(2.0)
    );
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.max_active_sockets.load(Ordering::SeqCst), 1);

    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn http_authentication_rejection_is_terminal_and_does_not_load_credentials() {
    let (connector, metrics) = connector([ConnectScript::Failure(
        ConnectFailure::AuthenticationRejected,
    )]);
    let (running, credential_calls) = start(config(4, 2, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::AuthenticationRejected)
    );
    assert_eq!(credential_calls.load(Ordering::SeqCst), 0);
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_partial_subscription_ack_is_terminal_and_never_marks_ready() {
    let mut frames = acknowledged_frames(false);
    frames.push(frame([quote(1.0)]));
    let (connector, metrics) = connector([socket_script(frames, true)]);
    let (running, credential_calls) = start(config(4, 2, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::SubscriptionRejected)
    );
    assert_eq!(credential_calls.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.sends.lock().expect("fake send lock").len(), 2);
}

#[tokio::test]
async fn quote_only_and_trade_only_acknowledgements_never_project_as_complete() {
    for (include_quote, include_trade) in [(true, false), (false, true)] {
        let frames = vec![
            success("connected"),
            success("authenticated"),
            subscription_ack_channels(include_quote, include_trade),
        ];
        let (connector, _) = connector([socket_script(frames, true)]);
        let (running, _) = start(config(4, 1, Duration::from_secs(1)), connector);
        assert_eq!(
            running.task.await.expect("session task joins"),
            Err(StreamError::SubscriptionRejected)
        );

        let mut acknowledged = false;
        let mut controls = running.receivers.controls;
        while let Some(control) = controls.recv().await {
            acknowledged |= matches!(control, ControlEvent::SubscriptionAcknowledged { .. });
        }
        assert!(!acknowledged, "each channel must match the full request");
    }
}

#[tokio::test(start_paused = true)]
async fn acknowledgement_timeout_reconnects_only_within_the_local_retry_bound() {
    let mut first = acknowledged_frames(true);
    first.truncate(2);
    let second = first.clone();
    let (connector, metrics) = connector([socket_script(first, true), socket_script(second, true)]);
    let (running, _) = start(config(4, 1, Duration::from_millis(5)), connector);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), running.task)
            .await
            .expect("bounded retries complete under virtual time")
            .expect("session task joins"),
        Err(StreamError::RetryLimitReached)
    );
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn authentication_handshake_timeout_is_observable_and_retry_bounded() {
    let incomplete_handshake = vec![success("connected")];
    let (connector, metrics) = connector([
        socket_script(incomplete_handshake.clone(), true),
        socket_script(incomplete_handshake, true),
    ]);
    let (mut running, credential_calls) = start_with_clock_and_seed(
        config_with_authentication_timeout(4, 1, Duration::from_secs(1), Duration::from_millis(5)),
        connector,
        SystemFreshnessClock,
        TEST_RETRY_SEED,
    );

    let result = tokio::time::timeout(Duration::from_secs(1), &mut running.task)
        .await
        .expect("authentication timeout and bounded retry finish under virtual time")
        .expect("session task joins");
    assert_eq!(result, Err(StreamError::RetryLimitReached));
    assert_eq!(credential_calls.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.sends.lock().expect("fake send lock").len(), 2);

    let mut awaiting_authentication = 0;
    let mut authentication_timeouts = 0;
    while let Some(event) = running.receivers.controls.recv().await {
        if let ControlEvent::PhaseChanged { phase, cause, .. } = event {
            if phase == SessionPhase::AwaitingAuthentication {
                awaiting_authentication += 1;
            }
            if phase == SessionPhase::SessionLost
                && cause == Some(SessionStatusCause::AuthenticationTimeout)
            {
                authentication_timeouts += 1;
            }
        }
    }
    assert_eq!(awaiting_authentication, 2);
    assert_eq!(authentication_timeouts, 2);
}

#[tokio::test(start_paused = true)]
async fn stale_and_future_dated_market_data_never_make_the_session_ready() {
    let fixed_now = UNIX_EPOCH + Duration::from_hours(500_000);
    let stale = timestamp_at(fixed_now - Duration::from_secs(60));
    let future = timestamp_at(fixed_now + Duration::from_secs(60));
    let mut frames = acknowledged_frames(true);
    frames.push(frame([
        quote_at(1.0, stale.clone()),
        quote_at(2.0, future.clone()),
        trade_at(stale),
        trade_at(future),
    ]));
    let (connector, _) = connector([socket_script(frames, true)]);
    let (mut running, _) = start_with_clock_and_seed(
        config(4, 1, Duration::from_secs(1)),
        connector,
        FixedFreshnessClock(fixed_now),
        TEST_RETRY_SEED,
    );

    let mut discard_reasons = Vec::new();
    while discard_reasons.len() < 2 {
        let event = tokio::time::timeout(Duration::from_secs(1), running.receivers.controls.recv())
            .await
            .expect("quote discard events arrive under the fixed clock")
            .expect("control lane remains open");
        match event {
            ControlEvent::QuoteDiscarded { reason, .. } => discard_reasons.push(reason),
            ControlEvent::PhaseChanged {
                phase: SessionPhase::Ready,
                ..
            } => panic!("stale or future-dated data must not make the session ready"),
            _ => {}
        }
    }
    assert_eq!(
        discard_reasons,
        vec![
            crate::lane::QuoteDiscardReason::Stale,
            crate::lane::QuoteDiscardReason::FutureDated,
        ]
    );

    let stale_trade = running
        .receivers
        .trades
        .recv()
        .await
        .expect("stale trade is preserved and freshness-labeled");
    let future_trade = running
        .receivers
        .trades
        .recv()
        .await
        .expect("future-dated trade is preserved and freshness-labeled");
    assert_eq!(stale_trade.freshness, DataFreshness::Stale);
    assert_eq!(future_trade.freshness, DataFreshness::FutureDated);

    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
    while let Some(event) = running.receivers.controls.recv().await {
        assert!(!matches!(
            event,
            ControlEvent::PhaseChanged {
                phase: SessionPhase::Ready,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn connection_limit_error_is_classified_without_hard_coded_plan_quotas() {
    let error_frame = frame([control([
        ("T", Value::from("error")),
        ("code", Value::from(406i64)),
        ("msg", Value::from("synthetic limit")),
    ])]);
    let frames = vec![success("connected"), error_frame];
    let (connector, metrics) = connector([socket_script(frames, true)]);
    let (running, _) = start(config(4, 2, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ConnectionLimitReached)
    );
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn full_trade_lane_terminates_instead_of_silently_dropping_trades() {
    let mut frames = acknowledged_frames(true);
    frames.push(frame([trade(), trade()]));
    let (connector, _metrics) = connector([socket_script(frames, true)]);
    let (mut running, _) = start(config(1, 1, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ConsumerOverloaded)
    );
    assert!(running.receivers.trades.recv().await.is_none());
}

#[tokio::test]
async fn unknown_market_frame_is_preserved_and_terminal() {
    let mut frames = acknowledged_frames(true);
    let raw_bytes = frame([
        quote(1.0),
        control([
            ("T", Value::from("future")),
            ("msg", Value::from("synthetic body is discarded")),
        ]),
        quote(2.0),
    ]);
    frames.push(raw_bytes.clone());
    let (connector, _) = connector([socket_script(frames, true)]);
    let (mut running, _) = start(config(4, 1, Duration::from_secs(1)), connector);

    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ProtocolViolation)
    );

    let mut raw_frame = None;
    let mut unknown_message = false;
    while let Some(event) = running.receivers.controls.recv().await {
        match event {
            ControlEvent::RawMarketFrame(frame) => raw_frame = Some(frame),
            ControlEvent::UnknownMessage { type_tag, .. } => {
                unknown_message = type_tag == "future";
            }
            _ => (),
        }
    }
    let raw_frame = raw_frame.expect("unknown input frame is retained");
    let digest = Sha256::digest(&raw_bytes);
    let mut expected_sha256 = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut expected_sha256, "{byte:02x}")
            .expect("writing hexadecimal digits to String cannot fail");
    }
    assert_eq!(raw_frame.payload.as_bytes(), raw_bytes);
    assert_eq!(raw_frame.payload.sha256(), expected_sha256);
    assert_eq!(raw_frame.event_count, 2);
    assert_eq!(raw_frame.symbols, [SYMBOL]);
    assert_eq!(raw_frame.disposition, RawFrameDisposition::UnknownMessage);
    assert!(unknown_message, "the unknown type is observable");
    assert!(running.receivers.quotes.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn cancelled_pending_reader_closes_its_owned_socket() {
    let frames = acknowledged_frames(true);
    let (connector, metrics) = connector([socket_script(frames, true)]);
    let (mut running, _) = start(config(4, 1, Duration::from_secs(1)), connector);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), running.receivers.controls.recv())
            .await
            .expect("handshake reaches fresh-data gate")
            .expect("control lane remains open");
        if matches!(
            event,
            ControlEvent::PhaseChanged {
                phase: SessionPhase::AwaitingFreshData,
                ..
            }
        ) {
            break;
        }
    }
    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
    assert_eq!(metrics.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn raw_market_frame_bytes_link_to_each_normalized_event() {
    let mut frames = acknowledged_frames(true);
    let raw_bytes = frame([quote(1.0), trade()]);
    frames.push(raw_bytes.clone());
    let (connector, _metrics) = connector([socket_script(frames, true)]);
    let (mut running, _) = start(config(4, 1, Duration::from_secs(1)), connector);

    let raw_frame = loop {
        let event = running
            .receivers
            .controls
            .recv()
            .await
            .expect("control lane remains open");
        if let ControlEvent::RawMarketFrame(frame) = event {
            break frame;
        }
    };
    assert_eq!(raw_frame.payload.as_bytes(), raw_bytes);
    assert_eq!(raw_frame.event_count, 2);
    assert_eq!(raw_frame.symbols, [SYMBOL]);
    assert_eq!(raw_frame.frame_sequence, 1);

    let quote = running
        .receivers
        .quotes
        .recv()
        .await
        .expect("first normalized event is delivered");
    assert_eq!(
        quote.quote.bid_price,
        crate::model::MarketNumber::Float64(1.0)
    );
    assert_eq!(quote.ingest.raw_frame_sequence, 1);
    assert_eq!(quote.ingest.raw_frame_event_ordinal, 1);
    assert_eq!(quote.ingest.raw_frame_event_count, 2);

    let trade = running
        .receivers
        .trades
        .recv()
        .await
        .expect("second normalized event is delivered");
    assert_eq!(trade.ingest.raw_frame_sequence, 1);
    assert_eq!(trade.ingest.raw_frame_event_ordinal, 2);
    assert_eq!(trade.ingest.raw_frame_event_count, 2);

    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
}

#[tokio::test]
async fn closing_a_required_consumer_stops_before_connecting() {
    let (connector, metrics) = connector([socket_script(Vec::new(), true)]);
    let (running, _) = start(config(4, 1, Duration::from_secs(1)), connector);
    drop(running.receivers.controls);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ConsumerClosed)
    );
    assert_eq!(metrics.connects.load(Ordering::SeqCst), 0);
}

#[test]
fn concurrently_allocated_sessions_receive_distinct_retry_seeds() {
    let workers: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                next_session_retry_seed().expect("session seed counter remains in range")
            })
        })
        .collect();
    let mut seeds: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("seed worker joins"))
        .collect();
    seeds.sort_unstable();
    seeds.dedup();
    assert_eq!(seeds.len(), 8);
}

mod diagnostic_tests {
    include!("session_diagnostic_tests.rs");
}
