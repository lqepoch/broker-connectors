    use rmpv::Value;
    use std::collections::VecDeque;
    use std::future::{Future, pending};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::task::{Context, Poll};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::config::{
        DesiredSubscriptions, OptionContractSymbol, OptionFeed, StreamEnvironment, StreamLimits,
    };
    use crate::credentials::AlpacaCredentials;
    use crate::lane::{ControlEvent, LaneReceivers, create_lanes};
    use crate::state::ReconnectPolicy;
    use crate::transport::{SocketFailure, SocketFrame};
    use broker_ports::{
        PortFuture, RawCaptureInstanceId, RawFrameCapture, RawFrameCaptureAck,
        RawFrameFinalization, RawFrameFinalizationAck, RawFrameSink, RawFrameSinkError,
    };

    const SYMBOL: &str = "AAPL260123C00150000";
    const TEST_RETRY_SEED: u64 = 0x236a_1aca_5eed;

    #[derive(Clone)]
    struct FakeCredentialProvider {
        calls: Arc<AtomicUsize>,
    }

    impl CredentialProvider for FakeCredentialProvider {
        fn load_credentials(
            &mut self,
        ) -> impl Future<Output = Result<AlpacaCredentials, CredentialFailure>> + Send {
            self.calls.fetch_add(1, Ordering::SeqCst);
            async {
                AlpacaCredentials::new("synthetic-key", "synthetic-secret")
                    .map_err(|_| CredentialFailure::Unavailable)
            }
        }
    }

    #[derive(Default)]
    struct PendingCredentialObservations {
        calls: AtomicUsize,
        polls: AtomicUsize,
        drops: AtomicUsize,
    }

    struct PendingCredentialProvider {
        observations: Arc<PendingCredentialObservations>,
    }

    struct PendingCredentialFuture {
        observations: Arc<PendingCredentialObservations>,
    }

    #[derive(Clone, Copy)]
    enum RawSinkFault {
        None,
        FailPredecode(u64),
        MismatchPredecode(u64),
        FailFinalization(u64),
        MismatchFinalization(u64),
        PendingPredecode(u64),
    }

    struct FakeRawFrameSink {
        id: RawCaptureInstanceId,
        fault: RawSinkFault,
        observations: Arc<StdMutex<Vec<String>>>,
    }

    impl FakeRawFrameSink {
        fn with_fault(fault: RawSinkFault) -> (Arc<dyn RawFrameSink>, Arc<StdMutex<Vec<String>>>) {
            let mut id = [0x31; 16];
            id[6] = 0x41;
            id[8] = 0x91;
            let id = RawCaptureInstanceId::new(id).expect("synthetic UUIDv4 capture ID");
            let observations = Arc::new(StdMutex::new(Vec::new()));
            let sink: Arc<dyn RawFrameSink> = Arc::new(Self {
                id,
                fault,
                observations: Arc::clone(&observations),
            });
            (sink, observations)
        }

        fn wrong_capture(capture: &RawFrameCapture) -> RawFrameCapture {
            let mut id = [0x52; 16];
            id[6] = 0x42;
            id[8] = 0x92;
            RawFrameCapture::new(
                RawCaptureInstanceId::new(id).expect("synthetic wrong UUIDv4 capture ID"),
                capture.provider(),
                capture.feed(),
                capture.entitlement(),
                capture.source_generation(),
                capture.frame_sequence(),
                capture.received_timestamp_utc().clone(),
                capture.wire_encoding(),
                capture.payload().clone(),
            )
            .expect("well-formed synthetic capture with a wrong identity")
        }
    }

    impl RawFrameSink for FakeRawFrameSink {
        fn capture_instance_id(&self) -> RawCaptureInstanceId {
            self.id
        }

        fn persist_before_decode<'a>(
            &'a self,
            capture: &'a RawFrameCapture,
        ) -> PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>> {
            Box::pin(async move {
                self.observations
                    .lock()
                    .expect("fake raw sink observations")
                    .push(format!(
                        "pre:{}:{}",
                        capture.source_generation(),
                        capture.frame_sequence()
                    ));
                match self.fault {
                    RawSinkFault::FailPredecode(sequence)
                        if capture.frame_sequence() == sequence =>
                    {
                        return Err(RawFrameSinkError::CapacityExceeded);
                    }
                    RawSinkFault::PendingPredecode(sequence)
                        if capture.frame_sequence() == sequence =>
                    {
                        pending::<()>().await;
                    }
                    RawSinkFault::MismatchPredecode(sequence)
                        if capture.frame_sequence() == sequence =>
                    {
                        return Ok(RawFrameCaptureAck::for_capture(&Self::wrong_capture(capture)));
                    }
                    _ => {}
                }
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
                    .expect("fake raw sink observations")
                    .push(format!("final:{}", summary.event_count()));
                let sequence = predecode_ack.frame_sequence();
                match self.fault {
                    RawSinkFault::FailFinalization(target) if sequence == target => {
                        return Err(RawFrameSinkError::Ambiguous);
                    }
                    RawSinkFault::MismatchFinalization(target) if sequence == target => {
                        let mut wrong_id = [0x73; 16];
                        wrong_id[6] = 0x43;
                        wrong_id[8] = 0x93;
                        let wrong = RawFrameCapture::new(
                            RawCaptureInstanceId::new(wrong_id)
                                .expect("synthetic wrong UUIDv4 capture ID"),
                            "alpaca",
                            "opra",
                            market_contracts::EntitlementState::Unknown,
                            predecode_ack.source_generation(),
                            predecode_ack.frame_sequence(),
                            market_contracts::UtcTimestamp::parse(
                                "2026-10-08T12:00:00Z",
                            )
                            .expect("fixed UTC timestamp"),
                            broker_ports::RawFrameWireEncoding::MessagePack,
                            broker_ports::RawFramePayload::capture(b"synthetic".to_vec())
                                .expect("bounded synthetic capture"),
                        )
                        .expect("synthetic wrong capture identity");
                        let wrong_ack = RawFrameCaptureAck::for_capture(&wrong);
                        return Ok(RawFrameFinalizationAck::for_finalization(
                            &wrong_ack,
                            summary,
                        ));
                    }
                    _ => {}
                }
                Ok(RawFrameFinalizationAck::for_finalization(
                    predecode_ack,
                    summary,
                ))
            })
        }
    }

    impl CredentialProvider for PendingCredentialProvider {
        fn load_credentials(
            &mut self,
        ) -> impl Future<Output = Result<AlpacaCredentials, CredentialFailure>> + Send {
            self.observations.calls.fetch_add(1, Ordering::SeqCst);
            PendingCredentialFuture {
                observations: Arc::clone(&self.observations),
            }
        }
    }

    impl Future for PendingCredentialFuture {
        type Output = Result<AlpacaCredentials, CredentialFailure>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.observations.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }

    impl Drop for PendingCredentialFuture {
        fn drop(&mut self) {
            self.observations.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    enum ConnectScript {
        Socket {
            frames: Vec<Vec<u8>>,
            hold_open: bool,
        },
        SocketWithFrames {
            frames: Vec<SocketFrame>,
            hold_open: bool,
        },
        Pending,
        Failure(ConnectFailure),
    }

    struct FakeConnector {
        scripts: Arc<StdMutex<VecDeque<ConnectScript>>>,
        connects: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        active_sockets: Arc<AtomicUsize>,
        max_active_sockets: Arc<AtomicUsize>,
        sends: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    impl SocketConnector for FakeConnector {
        type Socket = FakeSocket;

        fn connect(
            &mut self,
            _endpoint: &'static str,
        ) -> impl Future<Output = Result<Self::Socket, ConnectFailure>> + Send {
            self.connects.fetch_add(1, Ordering::SeqCst);
            let script = self.scripts.lock().expect("fake script lock").pop_front();
            let closes = Arc::clone(&self.closes);
            let active_sockets = Arc::clone(&self.active_sockets);
            let max_active_sockets = Arc::clone(&self.max_active_sockets);
            let sends = Arc::clone(&self.sends);
            async move {
                match script {
                    Some(ConnectScript::Pending) => {
                        pending::<Result<FakeSocket, ConnectFailure>>().await
                    }
                    Some(ConnectScript::Socket { frames, hold_open }) => {
                        let active = active_sockets.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active_sockets.fetch_max(active, Ordering::SeqCst);
                        Ok(FakeSocket {
                            frames: frames.into_iter().map(SocketFrame::Binary).collect(),
                            hold_open,
                            closes,
                            active_sockets,
                            sends,
                        })
                    }
                    Some(ConnectScript::SocketWithFrames { frames, hold_open }) => {
                        let active = active_sockets.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active_sockets.fetch_max(active, Ordering::SeqCst);
                        Ok(FakeSocket {
                            frames: frames.into(),
                            hold_open,
                            closes,
                            active_sockets,
                            sends,
                        })
                    }
                    Some(ConnectScript::Failure(error)) => Err(error),
                    None => Err(ConnectFailure::Retryable),
                }
            }
        }
    }

    struct FakeSocket {
        frames: VecDeque<SocketFrame>,
        hold_open: bool,
        closes: Arc<AtomicUsize>,
        active_sockets: Arc<AtomicUsize>,
        sends: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    impl StreamSocket for FakeSocket {
        #[allow(clippy::unused_async_trait_impl)] // StreamSocket requires an async send operation; the fake returns immediately.
        async fn send_binary(&mut self, payload: Zeroizing<Vec<u8>>) -> Result<(), SocketFailure> {
            self.sends
                .lock()
                .expect("fake send lock")
                .push(payload.to_vec());
            Ok(())
        }

        async fn receive(&mut self) -> Result<Option<SocketFrame>, SocketFailure> {
            if let Some(payload) = self.frames.pop_front() {
                return Ok(Some(payload));
            }
            if self.hold_open {
                pending().await
            } else {
                Ok(None)
            }
        }

        async fn close(&mut self) {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.active_sockets.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct RunningFake {
        task: JoinHandle<Result<SessionExit, StreamError>>,
        shutdown: watch::Sender<bool>,
        receivers: LaneReceivers,
    }

    fn config(
        trade_capacity: usize,
        max_retries: u8,
        acknowledgement_timeout: Duration,
    ) -> StreamConfig {
        config_with_authentication_timeout(
            trade_capacity,
            max_retries,
            acknowledgement_timeout,
            Duration::from_secs(5),
        )
    }

    fn config_with_authentication_timeout(
        trade_capacity: usize,
        max_retries: u8,
        acknowledgement_timeout: Duration,
        authentication_timeout: Duration,
    ) -> StreamConfig {
        let symbol = OptionContractSymbol::new(SYMBOL).expect("valid synthetic option symbol");
        let subscriptions = DesiredSubscriptions::new([symbol.clone()], [symbol])
            .expect("non-empty synthetic subscriptions");
        let limits = StreamLimits {
            authentication_timeout,
            acknowledgement_timeout,
            trade_capacity,
            ..StreamLimits::default()
        };
        let reconnect = ReconnectPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            max_retries,
            0,
        )
        .expect("bounded reconnect policy");
        StreamConfig::with_limits_and_reconnect(
            StreamEnvironment::Production,
            OptionFeed::Opra,
            subscriptions,
            limits,
            reconnect,
        )
        .expect("valid synthetic stream config")
    }

    fn connector(scripts: impl IntoIterator<Item = ConnectScript>) -> (FakeConnector, FakeMetrics) {
        let metrics = FakeMetrics {
            connects: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
            active_sockets: Arc::new(AtomicUsize::new(0)),
            max_active_sockets: Arc::new(AtomicUsize::new(0)),
            sends: Arc::new(StdMutex::new(Vec::new())),
        };
        let connector = FakeConnector {
            scripts: Arc::new(StdMutex::new(scripts.into_iter().collect())),
            connects: Arc::clone(&metrics.connects),
            closes: Arc::clone(&metrics.closes),
            active_sockets: Arc::clone(&metrics.active_sockets),
            max_active_sockets: Arc::clone(&metrics.max_active_sockets),
            sends: Arc::clone(&metrics.sends),
        };
        (connector, metrics)
    }

    struct FakeMetrics {
        connects: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        active_sockets: Arc<AtomicUsize>,
        max_active_sockets: Arc<AtomicUsize>,
        sends: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    fn start(
        config: StreamConfig,
        connector: FakeConnector,
    ) -> (RunningFake, Arc<AtomicUsize>) {
        start_with_clock_and_seed(config, connector, SystemFreshnessClock, TEST_RETRY_SEED)
    }

    fn start_with_clock_and_seed<W: FreshnessClock>(
        config: StreamConfig,
        connector: FakeConnector,
        freshness_clock: W,
        retry_seed: u64,
    ) -> (RunningFake, Arc<AtomicUsize>) {
        let (publishers, receivers, consumers_closed) = create_lanes(&config);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let credential_calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeCredentialProvider {
            calls: Arc::clone(&credential_calls),
        };
        let task = tokio::spawn(run_session_with_seed_and_clock(
            config,
            provider,
            connector,
            shutdown_rx,
            consumers_closed,
            publishers,
            None,
            SessionRuntime {
                retry_seed,
                freshness_clock,
            },
        ));
        (
            RunningFake {
                task,
                shutdown,
                receivers,
            },
            credential_calls,
        )
    }

    fn start_with_raw_sink(
        config: StreamConfig,
        connector: FakeConnector,
        raw_frame_sink: Arc<dyn RawFrameSink>,
    ) -> (RunningFake, Arc<AtomicUsize>) {
        let (publishers, receivers, consumers_closed) = create_lanes(&config);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let credential_calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeCredentialProvider {
            calls: Arc::clone(&credential_calls),
        };
        let task = tokio::spawn(run_session_with_seed_and_clock(
            config,
            provider,
            connector,
            shutdown_rx,
            consumers_closed,
            publishers,
            Some(raw_frame_sink),
            SessionRuntime {
                retry_seed: TEST_RETRY_SEED,
                freshness_clock: SystemFreshnessClock,
            },
        ));
        (
            RunningFake {
                task,
                shutdown,
                receivers,
            },
            credential_calls,
        )
    }

    fn start_with_pending_credentials<W: FreshnessClock>(
        config: StreamConfig,
        connector: FakeConnector,
        freshness_clock: W,
        retry_seed: u64,
    ) -> (RunningFake, Arc<PendingCredentialObservations>) {
        let (publishers, receivers, consumers_closed) = create_lanes(&config);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let observations = Arc::new(PendingCredentialObservations::default());
        let provider = PendingCredentialProvider {
            observations: Arc::clone(&observations),
        };
        let task = tokio::spawn(run_session_with_seed_and_clock(
            config,
            provider,
            connector,
            shutdown_rx,
            consumers_closed,
            publishers,
            None,
            SessionRuntime {
                retry_seed,
                freshness_clock,
            },
        ));
        (
            RunningFake {
                task,
                shutdown,
                receivers,
            },
            observations,
        )
    }

    fn wire_value(value: &Value) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, value).expect("encode fake MessagePack frame");
        bytes
    }

    fn frame(messages: impl IntoIterator<Item = Value>) -> Vec<u8> {
        let value = Value::Array(messages.into_iter().collect());
        wire_value(&value)
    }

    fn control(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
        Value::Map(
            fields
                .into_iter()
                .map(|(key, value)| (Value::from(key), value))
                .collect(),
        )
    }

    fn success(message: &'static str) -> Vec<u8> {
        frame([control([
            ("T", Value::from("success")),
            ("msg", Value::from(message)),
        ])])
    }

    fn subscription_ack(include_trade: bool) -> Vec<u8> {
        subscription_ack_channels(true, include_trade)
    }

    fn subscription_ack_channels(include_quote: bool, include_trade: bool) -> Vec<u8> {
        let quotes = include_quote
            .then(|| Value::from(SYMBOL))
            .into_iter()
            .collect::<Vec<_>>();
        let trades = include_trade
            .then(|| Value::from(SYMBOL))
            .into_iter()
            .collect::<Vec<_>>();
        frame([control([
            ("T", Value::from("subscription")),
            ("quotes", Value::Array(quotes)),
            ("trades", Value::Array(trades)),
        ])])
    }

    fn quote(price: f64) -> Value {
        quote_at(price, timestamp_now())
    }

    fn quote_at(price: f64, provider_timestamp: String) -> Value {
        control([
            ("T", Value::from("q")),
            ("S", Value::from(SYMBOL)),
            ("t", Value::from(provider_timestamp)),
            ("bx", Value::from("X")),
            ("bp", Value::F64(price)),
            ("bs", Value::from(1u64)),
            ("ax", Value::from("Y")),
            ("ap", Value::F64(price + 0.25)),
            ("as", Value::from(1u64)),
            ("c", Value::Array(Vec::new())),
        ])
    }

    fn trade() -> Value {
        trade_at(timestamp_now())
    }

    fn trade_at(provider_timestamp: String) -> Value {
        control([
            ("T", Value::from("t")),
            ("S", Value::from(SYMBOL)),
            ("t", Value::from(provider_timestamp)),
            ("p", Value::F64(1.25)),
            ("s", Value::from(1u64)),
            ("x", Value::from("X")),
            ("c", Value::Array(Vec::new())),
        ])
    }

    fn timestamp_now() -> String {
        timestamp_at(SystemTime::now())
    }

    fn timestamp_at(time: SystemTime) -> String {
        let elapsed = time
            .duration_since(UNIX_EPOCH)
            .expect("synthetic timestamp is after Unix epoch");
        let total_seconds = i64::try_from(elapsed.as_secs()).expect("test clock fits i64");
        let days = total_seconds.div_euclid(86_400);
        let day_seconds = total_seconds.rem_euclid(86_400);

        // Howard Hinnant's civil-from-days conversion keeps the test independent of chrono's clock feature.
        // Howard Hinnant 的 civil-from-days 换算让测试不依赖 chrono 的 clock feature。
        let shifted_days = days + 719_468;
        let era = shifted_days.div_euclid(146_097);
        let day_of_era = shifted_days - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_prime = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
        let month = month_prime + if month_prime < 10 { 3 } else { -9 };
        year += i64::from(month <= 2);
        let hour = day_seconds / 3_600;
        let minute = (day_seconds % 3_600) / 60;
        let second = day_seconds % 60;
        format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:09}Z",
            elapsed.subsec_nanos()
        )
    }

    fn acknowledged_frames(acknowledge_trade: bool) -> Vec<Vec<u8>> {
        vec![
            success("connected"),
            success("authenticated"),
            subscription_ack(acknowledge_trade),
        ]
    }

    fn socket_script(frames: Vec<Vec<u8>>, hold_open: bool) -> ConnectScript {
        ConnectScript::Socket { frames, hold_open }
    }

    fn socket_with_frames_script(frames: Vec<SocketFrame>, hold_open: bool) -> ConnectScript {
        ConnectScript::SocketWithFrames { frames, hold_open }
    }
