use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use broker_ports::{
    PortFuture, RawCaptureInstanceId, RawFrameCapture, RawFrameCaptureAck, RawFrameFinalization,
    RawFrameFinalizationAck, RawFramePayload, RawFrameSink, RawFrameSinkError, RawFrameSinkFactory,
};
use market_contracts::EntitlementState;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::*;

#[derive(Clone, Copy)]
enum SinkFault {
    None,
    PredecodeFailure,
    MismatchedFinalization,
}

#[derive(Default)]
struct SinkObservations {
    provider: Option<String>,
    feed: Option<String>,
    frames: Vec<Vec<u8>>,
    predecode_count: usize,
    finalization_count: usize,
}

struct MemoryFactory {
    fault: SinkFault,
    observations: Arc<Mutex<SinkObservations>>,
}

impl MemoryFactory {
    fn create(fault: SinkFault) -> (Arc<dyn RawFrameSinkFactory>, Arc<Mutex<SinkObservations>>) {
        let observations = Arc::new(Mutex::new(SinkObservations::default()));
        (
            Arc::new(Self {
                fault,
                observations: Arc::clone(&observations),
            }),
            observations,
        )
    }
}

impl RawFrameSinkFactory for MemoryFactory {
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> Result<Arc<dyn RawFrameSink>, RawFrameSinkError> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| RawFrameSinkError::Poisoned)?;
        observations.provider = Some(provider.to_owned());
        observations.feed = Some(feed.to_owned());
        drop(observations);
        let mut id_bytes = [0x42_u8; 16];
        id_bytes[6] = 0x42;
        id_bytes[8] = 0x82;
        let id = RawCaptureInstanceId::new(id_bytes).map_err(|_| RawFrameSinkError::Unavailable)?;
        Ok(Arc::new(MemorySink {
            id,
            fault: self.fault,
            observations: Arc::clone(&self.observations),
        }))
    }
}

struct MemorySink {
    id: RawCaptureInstanceId,
    fault: SinkFault,
    observations: Arc<Mutex<SinkObservations>>,
}

impl RawFrameSink for MemorySink {
    fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.id
    }

    fn persist_before_decode<'a>(
        &'a self,
        capture: &'a RawFrameCapture,
    ) -> PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>> {
        Box::pin(async move {
            if matches!(self.fault, SinkFault::PredecodeFailure) {
                return Err(RawFrameSinkError::Unavailable);
            }
            let mut observations = self
                .observations
                .lock()
                .map_err(|_| RawFrameSinkError::Poisoned)?;
            observations
                .frames
                .push(capture.payload().as_bytes().to_vec());
            observations.predecode_count += 1;
            Ok(RawFrameCaptureAck::for_capture(capture))
        })
    }

    fn finalize_after_decode<'a>(
        &'a self,
        predecode_ack: &'a RawFrameCaptureAck,
        summary: &'a RawFrameFinalization,
    ) -> PortFuture<'a, Result<RawFrameFinalizationAck, RawFrameSinkError>> {
        Box::pin(async move {
            self.observations
                .lock()
                .map_err(|_| RawFrameSinkError::Poisoned)?
                .finalization_count += 1;
            if matches!(self.fault, SinkFault::MismatchedFinalization) {
                let payload = RawFramePayload::capture(b"wrong-finalization-binding".to_vec())
                    .map_err(|_| RawFrameSinkError::Unavailable)?;
                let timestamp =
                    market_contracts::UtcTimestamp::parse("2026-10-08T12:00:00.000000000Z")
                        .map_err(|_| RawFrameSinkError::Unavailable)?;
                let wrong_capture = RawFrameCapture::new(
                    predecode_ack.capture_instance_id(),
                    "alpaca",
                    "opra",
                    EntitlementState::Unknown,
                    predecode_ack.source_generation(),
                    predecode_ack.frame_sequence(),
                    timestamp,
                    RawFrameWireEncoding::MessagePack,
                    payload,
                )
                .map_err(|_| RawFrameSinkError::Unavailable)?;
                let wrong_predecode = RawFrameCaptureAck::for_capture(&wrong_capture);
                return Ok(RawFrameFinalizationAck::for_finalization(
                    &wrong_predecode,
                    summary,
                ));
            }
            Ok(RawFrameFinalizationAck::for_finalization(
                predecode_ack,
                summary,
            ))
        })
    }
}

fn cancellation_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

#[tokio::test]
async fn reviewed_fixture_reuses_runner_and_projector_with_exact_post_auth_capture() {
    let (factory, observations) = MemoryFactory::create(SinkFault::None);
    let (_cancel_tx, cancel_rx) = cancellation_channel();
    let capture =
        capture_reviewed_fixture(ReviewedFixtureId::AlpacaOpraTradeV1, factory, cancel_rx)
            .await
            .expect("fixed in-process fixture reaches its local terminal marker");
    let receipt = capture.receipt();

    assert_eq!(receipt.fixture_id(), ReviewedFixtureId::AlpacaOpraTradeV1);
    assert_eq!(receipt.fixture_id().as_str(), "alpaca-opra-trade-v1");
    assert_eq!(receipt.fixture_sha256(), FIXTURE_SHA256);
    assert_eq!(
        receipt.fixture_freshness_clock_unix_seconds(),
        1_791_460_800
    );
    assert_eq!(receipt.runner_received_digest_sha256(), FIXTURE_SHA256);
    assert_eq!(receipt.terminal_state(), OfflineFixtureTerminal::FixtureEnd);
    assert_eq!(receipt.script_frame_count(), 4);
    assert_eq!(receipt.runner_received_frame_count(), 4);
    assert_eq!(receipt.captured_frame_count(), 2);
    assert_eq!(receipt.raw_market_frame_count(), 1);
    assert_eq!(receipt.predecode_ack_count(), 2);
    assert_eq!(receipt.finalization_ack_count(), 2);
    assert!(receipt.captured_bytes() > 0);
    assert_eq!(receipt.output_item_count() as usize, capture.items().len());

    let observations = observations
        .lock()
        .expect("synthetic sink observation lock");
    assert_eq!(observations.provider.as_deref(), Some("alpaca"));
    assert_eq!(observations.feed.as_deref(), Some("opra"));
    assert_eq!(observations.predecode_count, 2);
    assert_eq!(observations.finalization_count, 2);
    assert_eq!(observations.frames, reviewed_fixture_frames().unwrap()[2..]);
    assert!(
        capture.items().iter().any(|item| matches!(
            item,
            MarketDataItem::Event { envelope, .. }
                if envelope.metadata.source.provider == "alpaca"
                    && envelope.metadata.source.feed == "opra"
                    && envelope.metadata.source.entitlement == EntitlementState::Unknown
                    && envelope.metadata.source.source_record_id.is_none()
        )),
        "projected records: {:?}",
        capture.items()
    );
    assert_eq!(
        capture
            .items()
            .iter()
            .filter(|item| matches!(item, MarketDataItem::RawFrame(_)))
            .count(),
        2
    );
    assert_eq!(
        capture
            .items()
            .iter()
            .filter(|item| matches!(item, MarketDataItem::Event { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn fixture_end_before_protocol_completion_never_returns_receipt() {
    for marker_after_frames in [Some(0), Some(1), Some(3), None] {
        let frames = reviewed_fixture_frames().expect("reviewed provider frames encode");
        let mut script = FixtureScript::reviewed(frames.clone());
        script.marker_after_frames = marker_after_frames;
        let (factory, _) = MemoryFactory::create(SinkFault::None);
        let (_cancel_tx, cancel_rx) = cancellation_channel();
        let result = run_scripted_capture(
            ReviewedFixtureId::AlpacaOpraTradeV1,
            frames,
            script,
            factory,
            cancel_rx,
        )
        .await;
        assert!(result.is_err(), "marker placement {marker_after_frames:?}");
    }
}

#[tokio::test]
async fn missing_predecode_or_mismatched_finalization_ack_never_returns_receipt() {
    for fault in [
        SinkFault::PredecodeFailure,
        SinkFault::MismatchedFinalization,
    ] {
        let (factory, _) = MemoryFactory::create(fault);
        let (_cancel_tx, cancel_rx) = cancellation_channel();
        let result =
            capture_reviewed_fixture(ReviewedFixtureId::AlpacaOpraTradeV1, factory, cancel_rx)
                .await;
        assert_eq!(result.err(), Some(OfflineFixtureError::CaptureFailed));
    }
}

#[tokio::test]
async fn caller_cancellation_returns_no_receipt_and_runs_no_sink_capture() {
    let (factory, observations) = MemoryFactory::create(SinkFault::None);
    let (cancel_tx, cancel_rx) = cancellation_channel();
    cancel_tx.send_replace(true);
    let result =
        capture_reviewed_fixture(ReviewedFixtureId::AlpacaOpraTradeV1, factory, cancel_rx).await;
    assert_eq!(result.err(), Some(OfflineFixtureError::Cancelled));
    let observations = observations
        .lock()
        .expect("synthetic sink observation lock");
    assert_eq!(observations.frames, Vec::<Vec<u8>>::new());
    assert_eq!(observations.predecode_count, 0);
    assert_eq!(observations.finalization_count, 0);
}

struct BlockingFactory {
    started: Mutex<Option<oneshot::Sender<()>>>,
    future_dropped: Arc<AtomicBool>,
}

impl BlockingFactory {
    fn create() -> (
        Arc<dyn RawFrameSinkFactory>,
        oneshot::Receiver<()>,
        Arc<AtomicBool>,
    ) {
        let (started_tx, started_rx) = oneshot::channel();
        let future_dropped = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                started: Mutex::new(Some(started_tx)),
                future_dropped: Arc::clone(&future_dropped),
            }),
            started_rx,
            future_dropped,
        )
    }
}

impl RawFrameSinkFactory for BlockingFactory {
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> Result<Arc<dyn RawFrameSink>, RawFrameSinkError> {
        if provider != "alpaca" || feed != "opra" {
            return Err(RawFrameSinkError::Unavailable);
        }
        let started = self
            .started
            .lock()
            .map_err(|_| RawFrameSinkError::Poisoned)?
            .take();
        let mut id_bytes = [0x24_u8; 16];
        id_bytes[6] = 0x42;
        id_bytes[8] = 0x82;
        let id = RawCaptureInstanceId::new(id_bytes).map_err(|_| RawFrameSinkError::Unavailable)?;
        Ok(Arc::new(BlockingSink {
            id,
            started: Mutex::new(started),
            future_dropped: Arc::clone(&self.future_dropped),
        }))
    }
}

struct BlockingSink {
    id: RawCaptureInstanceId,
    started: Mutex<Option<oneshot::Sender<()>>>,
    future_dropped: Arc<AtomicBool>,
}

struct FutureDropObserver(Arc<AtomicBool>);

impl Drop for FutureDropObserver {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl RawFrameSink for BlockingSink {
    fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.id
    }

    fn persist_before_decode<'a>(
        &'a self,
        capture: &'a RawFrameCapture,
    ) -> PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>> {
        let started = match self.started.lock() {
            Ok(mut value) => value.take(),
            Err(_) => {
                return Box::pin(async { Err(RawFrameSinkError::Poisoned) });
            }
        };
        let future_dropped = Arc::clone(&self.future_dropped);
        Box::pin(async move {
            let _observer = FutureDropObserver(future_dropped);
            if let Some(started) = started {
                let _ = started.send(());
            }
            std::future::pending::<()>().await;
            Ok(RawFrameCaptureAck::for_capture(capture))
        })
    }

    fn finalize_after_decode<'a>(
        &'a self,
        predecode_ack: &'a RawFrameCaptureAck,
        summary: &'a RawFrameFinalization,
    ) -> PortFuture<'a, Result<RawFrameFinalizationAck, RawFrameSinkError>> {
        Box::pin(async move {
            Ok(RawFrameFinalizationAck::for_finalization(
                predecode_ack,
                summary,
            ))
        })
    }
}

#[tokio::test]
async fn cancellation_during_sink_wait_joins_the_inline_session_and_returns_no_receipt() {
    let (factory, started_rx, future_dropped) = BlockingFactory::create();
    let (cancel_tx, cancel_rx) = cancellation_channel();
    let cancel_after_sink_started = async move {
        started_rx
            .await
            .expect("the first post-auth fixture frame reaches the injected sink");
        cancel_tx.send_replace(true);
    };

    let (result, ()) = tokio::join!(
        capture_reviewed_fixture(ReviewedFixtureId::AlpacaOpraTradeV1, factory, cancel_rx),
        cancel_after_sink_started,
    );

    assert_eq!(result.err(), Some(OfflineFixtureError::Cancelled));
    assert!(future_dropped.load(Ordering::Acquire));
}
