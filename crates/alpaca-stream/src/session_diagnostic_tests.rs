use super::*;
use crate::transport::SocketFrame;
use std::time::{Duration, UNIX_EPOCH};

#[tokio::test]
async fn non_subscribed_quote_fails_closed_after_the_exact_ack() {
    let mut frames = acknowledged_frames(true);
    let mut message = quote(1.0);
    let Value::Map(fields) = &mut message else {
        panic!("synthetic quote is a map");
    };
    let symbol = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("S"))
        .map(|(_, value)| value)
        .expect("synthetic quote has a symbol");
    *symbol = Value::from("MSFT260123C00150000");
    frames.push(frame([message]));

    assert_protocol_violation(frames).await;
}

#[tokio::test]
async fn non_subscribed_trade_fails_closed_after_the_exact_ack() {
    let mut frames = acknowledged_frames(true);
    let mut message = trade();
    let Value::Map(fields) = &mut message else {
        panic!("synthetic trade is a map");
    };
    let symbol = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("S"))
        .map(|(_, value)| value)
        .expect("synthetic trade has a symbol");
    *symbol = Value::from("MSFT260123C00150000");
    frames.push(frame([message]));

    assert_protocol_violation(frames).await;
}

#[tokio::test]
async fn unexpected_active_success_or_subscription_is_not_treated_as_market_data() {
    for frame in [success("connected"), subscription_ack(true)] {
        let mut frames = acknowledged_frames(true);
        frames.push(frame);
        assert_protocol_violation(frames).await;
    }
}

#[tokio::test]
async fn malformed_binary_and_text_frames_fail_closed_after_the_exact_ack() {
    let mut malformed = acknowledged_frames(true);
    malformed.push(vec![0x91, 0xc1]);
    assert_protocol_violation(malformed).await;

    let mut text: Vec<_> = acknowledged_frames(true)
        .into_iter()
        .map(SocketFrame::Binary)
        .collect();
    text.push(SocketFrame::Text);
    let (connector, _) = connector([socket_with_frames_script(text, false)]);
    let (running, _) = start(config(4, 1, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ProtocolViolation)
    );
}

#[tokio::test]
async fn malformed_timestamp_extension_fails_closed_after_the_exact_ack() {
    let mut message = quote(1.0);
    let Value::Map(fields) = &mut message else {
        panic!("synthetic quote is a map");
    };
    let timestamp = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("t"))
        .map(|(_, value)| value)
        .expect("synthetic quote has a timestamp");
    let invalid_nanoseconds = ((1_000_000_000_u64) << 34).to_be_bytes().to_vec();
    *timestamp = Value::Ext(-1, invalid_nanoseconds);

    let mut frames = acknowledged_frames(true);
    frames.push(frame([message]));
    assert_protocol_violation(frames).await;
}

#[tokio::test(start_paused = true)]
async fn fresh_timestamp_extension_quote_reaches_ready_without_losing_nanoseconds() {
    let seconds = 1_800_000_000_u64;
    let nanosecond = 987_654_321_u32;
    let fixed_now = UNIX_EPOCH + Duration::new(seconds, nanosecond);
    let packed = (u64::from(nanosecond) << 34) | seconds;
    let mut message = quote(1.0);
    let Value::Map(fields) = &mut message else {
        panic!("synthetic quote is a map");
    };
    let timestamp = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("t"))
        .map(|(_, value)| value)
        .expect("synthetic quote has a timestamp");
    *timestamp = Value::Ext(-1, packed.to_be_bytes().to_vec());

    let mut frames = acknowledged_frames(true);
    frames.push(frame([message]));
    let (connector, _) = connector([socket_script(frames, true)]);
    let (mut running, _) = start_with_clock_and_seed(
        config(4, 1, Duration::from_secs(1)),
        connector,
        FixedFreshnessClock(fixed_now),
        TEST_RETRY_SEED,
    )
    ;

    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), running.receivers.controls.recv())
            .await
            .expect("ready phase arrives under the fixed clock")
            .expect("control lane remains open");
        if matches!(
            event,
            ControlEvent::PhaseChanged {
                phase: SessionPhase::Ready,
                ..
            }
        ) {
            break;
        }
    }

    let update = running
        .receivers
        .quotes
        .recv()
        .await
        .expect("fresh timestamped quote is delivered");
    assert_eq!(update.freshness, DataFreshness::Fresh);
    assert_eq!(
        update.quote.timestamp.unix_seconds(),
        i64::try_from(seconds).expect("synthetic seconds fit i64")
    );
    assert_eq!(update.quote.timestamp.nanosecond(), nanosecond);

    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
}

#[tokio::test]
async fn maximal_stale_quote_frame_fits_frame_plus_phase_control_budget() {
    let fixed_now = UNIX_EPOCH + Duration::from_hours(500_000);
    let stale = timestamp_at(fixed_now - Duration::from_secs(60));
    let mut configuration = config(1, 1, Duration::from_secs(1));
    configuration.limits.control_capacity = crate::MAX_FRAME_MESSAGES + 10;

    let mut frames = acknowledged_frames(true);
    frames.push(frame(
        (0..crate::MAX_FRAME_MESSAGES).map(|_| quote_at(1.0, stale.clone())),
    ));
    let (connector, _) = connector([socket_script(frames, true)]);
    let (mut running, _) = start_with_clock_and_seed(
        configuration,
        connector,
        FixedFreshnessClock(fixed_now),
        TEST_RETRY_SEED,
    )
    ;

    let mut discarded = 0;
    while discarded < crate::MAX_FRAME_MESSAGES {
        let event = tokio::time::timeout(
            Duration::from_secs(1),
            running.receivers.controls.recv(),
        )
        .await
        .expect("all stale quote controls fit in the derived lane capacity")
        .expect("control lane remains open for the maximal legal frame");
        match event {
            ControlEvent::QuoteDiscarded {
                reason: crate::lane::QuoteDiscardReason::Stale,
                ..
            } => discarded += 1,
            ControlEvent::QuoteDiscarded { reason, .. } => {
                panic!("the synthetic frame contains only stale quotes, got {reason:?}")
            }
            ControlEvent::PhaseChanged {
                phase: SessionPhase::Ready,
                ..
            } => panic!("stale quotes cannot make the session ready"),
            _ => {}
        }
    }

    running.shutdown.send_replace(true);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Ok(SessionExit::Cancelled)
    );
}

async fn assert_protocol_violation(frames: Vec<Vec<u8>>) {
    let (connector, _) = connector([socket_script(frames, false)]);
    let (running, _) = start(config(4, 1, Duration::from_secs(1)), connector);
    assert_eq!(
        running.task.await.expect("session task joins"),
        Err(StreamError::ProtocolViolation)
    );
}
