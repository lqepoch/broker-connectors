use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, pending};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::timeout;

struct MockSocket {
    incoming: mpsc::Receiver<Vec<u8>>,
    outgoing: mpsc::Sender<StreamerCommand>,
    send_failures: Arc<StdMutex<BTreeMap<StreamerService, MockSendFailure>>>,
}

struct MockPeer {
    incoming: Option<mpsc::Sender<Vec<u8>>>,
    outgoing: mpsc::Receiver<StreamerCommand>,
    send_failures: Arc<StdMutex<BTreeMap<StreamerService, MockSendFailure>>>,
}

#[derive(Clone, Copy)]
enum MockSendFailure {
    Immediate,
    PartialWriteThenPending,
}

impl MockPeer {
    async fn next_command(&mut self) -> StreamerCommand {
        self.outgoing
            .recv()
            .await
            .expect("mock runtime sends planned command")
    }

    async fn send_frame(&self, frame: impl AsRef<[u8]>) {
        self.incoming
            .as_ref()
            .expect("mock socket remains open")
            .send(frame.as_ref().to_vec())
            .await
            .expect("mock socket receives test frame");
    }

    fn close(&mut self) {
        drop(self.incoming.take());
    }

    fn fail_next_send(&self, service: StreamerService) {
        self.send_failures
            .lock()
            .expect("mock send-failure lock is available")
            .insert(service, MockSendFailure::Immediate);
    }

    fn stall_next_send_after_partial_write(&self, service: StreamerService) {
        self.send_failures
            .lock()
            .expect("mock send-failure lock is available")
            .insert(service, MockSendFailure::PartialWriteThenPending);
    }
}

fn mock_session() -> (MockSocket, MockPeer) {
    let (incoming_sender, incoming) = mpsc::channel(32);
    let (outgoing, outgoing_receiver) = mpsc::channel(32);
    let send_failures = Arc::new(StdMutex::new(BTreeMap::new()));
    (
        MockSocket {
            incoming,
            outgoing,
            send_failures: Arc::clone(&send_failures),
        },
        MockPeer {
            incoming: Some(incoming_sender),
            outgoing: outgoing_receiver,
            send_failures,
        },
    )
}

struct MockFactory {
    sockets: VecDeque<MockSocket>,
    connect_calls: Arc<AtomicUsize>,
}

impl AuthenticatedSessionFactory for MockFactory {
    type Socket = MockSocket;

    fn connect_authenticated(
        &mut self,
        _generation: ConnectionGeneration,
        _login_request_id: RequestId,
    ) -> impl Future<Output = Result<Self::Socket, PortFailure>> + Send {
        self.connect_calls.fetch_add(1, Ordering::Relaxed);
        let socket = self.sockets.pop_front();
        async move { socket.ok_or(PortFailure::ConnectFailed) }
    }
}

#[derive(Default)]
struct CancellationProbe {
    partial_socket_open: AtomicBool,
    connect_future_dropped: AtomicBool,
    task_state: Arc<AdapterTaskProbe>,
    temporary_auth_buffer: StdMutex<Vec<u8>>,
}

#[derive(Default)]
struct AdapterTaskProbe {
    active: AtomicBool,
    adapter_task_started: Notify,
    adapter_task_stopped: Notify,
}

struct CancellationFactory {
    probe: Arc<CancellationProbe>,
}

impl AuthenticatedSessionFactory for CancellationFactory {
    type Socket = MockSocket;

    fn connect_authenticated(
        &mut self,
        _generation: ConnectionGeneration,
        _login_request_id: RequestId,
    ) -> impl Future<Output = Result<Self::Socket, PortFailure>> + Send {
        let probe = Arc::clone(&self.probe);
        async move {
            probe.partial_socket_open.store(true, Ordering::Release);
            probe
                .temporary_auth_buffer
                .lock()
                .expect("test buffer lock is available")
                .extend_from_slice(b"synthetic-auth-buffer");

            // The child captures only non-sensitive lifecycle state. The
            // synthetic authentication buffer remains owned by this future's
            // cancellation guard and is never copied into the task.
            let task_probe = Arc::clone(&probe.task_state);
            let (started_sender, started_receiver) = oneshot::channel();
            let task = tokio::spawn(async move {
                task_probe.active.store(true, Ordering::Release);
                let _exit = AdapterTaskExit(Arc::clone(&task_probe));
                task_probe.adapter_task_started.notify_waiters();
                let _ = started_sender.send(());
                pending::<()>().await;
            });
            let _cancellation = PendingConnectGuard {
                probe: Arc::clone(&probe),
                task,
            };
            if started_receiver.await.is_err() {
                return Err(PortFailure::ConnectFailed);
            }

            pending::<Result<MockSocket, PortFailure>>().await
        }
    }
}

struct PendingConnectGuard {
    probe: Arc<CancellationProbe>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PendingConnectGuard {
    fn drop(&mut self) {
        self.probe
            .connect_future_dropped
            .store(true, Ordering::Release);
        self.probe
            .partial_socket_open
            .store(false, Ordering::Release);
        if let Ok(mut buffer) = self.probe.temporary_auth_buffer.lock() {
            buffer.fill(0);
        }
        self.task.abort();
    }
}

struct AdapterTaskExit(Arc<AdapterTaskProbe>);

impl Drop for AdapterTaskExit {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
        self.0.adapter_task_stopped.notify_waiters();
    }
}

impl StreamerSocket for MockSocket {
    fn send_subscription(
        &mut self,
        command: &StreamerCommand,
    ) -> impl Future<Output = Result<(), PortFailure>> + Send {
        let sender = self.outgoing.clone();
        let command = command.clone();
        let send_failures = Arc::clone(&self.send_failures);
        async move {
            let failure = send_failures
                .lock()
                .expect("mock send-failure lock is available")
                .remove(&command.service());
            if matches!(failure, Some(MockSendFailure::Immediate)) {
                return Err(PortFailure::SendFailed);
            }
            sender
                .send(command)
                .await
                .map_err(|_| PortFailure::SendFailed)?;
            if matches!(failure, Some(MockSendFailure::PartialWriteThenPending)) {
                return pending::<Result<(), PortFailure>>().await;
            }
            Ok(())
        }
    }

    async fn receive_frame(&mut self) -> Result<Option<Vec<u8>>, PortFailure> {
        Ok(self.incoming.recv().await)
    }
}

fn factory(
    sessions: Vec<(MockSocket, MockPeer)>,
) -> (MockFactory, Vec<MockPeer>, Arc<AtomicUsize>) {
    let mut sockets = VecDeque::new();
    let mut peers = Vec::new();
    for (socket, peer) in sessions {
        sockets.push_back(socket);
        peers.push(peer);
    }
    let connect_calls = Arc::new(AtomicUsize::new(0));
    (
        MockFactory {
            sockets,
            connect_calls: connect_calls.clone(),
        },
        peers,
        connect_calls,
    )
}

fn fast_config() -> SessionConfig {
    SessionConfig {
        connect_timeout: Duration::from_millis(500),
        acknowledgement_timeout: Duration::from_secs(2),
        send_timeout: Duration::from_millis(500),
        reconnect_initial_delay: Duration::from_millis(5),
        reconnect_max_delay: Duration::from_millis(20),
        ..SessionConfig::default()
    }
}

async fn next_connection_event(
    events: &mut CriticalEventReceiver,
    connected: bool,
) -> SessionEvent {
    loop {
        let event = timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("connection event arrives")
            .expect("runtime remains active");
        if matches!(
            event,
            SessionEvent::Connection {
                connected: state,
                ..
            } if state == connected
        ) {
            return event;
        }
    }
}

async fn next_service_status(
    events: &mut CriticalEventReceiver,
    service: StreamerService,
    cause: ServiceStatusCause,
) -> SessionEvent {
    loop {
        let event = timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("service event arrives")
            .expect("runtime remains active");
        if matches!(
            event,
            SessionEvent::ServiceStatus {
                service: actual_service,
                cause: actual_cause,
                ..
            } if actual_service == service && actual_cause == cause
        ) {
            return event;
        }
    }
}

async fn next_ignored_ack(events: &mut CriticalEventReceiver) -> AckIgnoreReason {
    loop {
        let event = timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("ignored-ack event arrives")
            .expect("runtime remains active");
        if let SessionEvent::IgnoredAcknowledgement(reason) = event {
            return reason;
        }
    }
}

async fn send_route_barrier(peer: &MockPeer, events: &mut CriticalEventReceiver) {
    peer.send_frame(
        r#"{"response":[{"service":"ADMIN","requestid":"999","command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"barrier"}}]}"#,
    )
    .await;
    loop {
        let event = timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("routing barrier arrives")
            .expect("runtime remains active");
        if matches!(event, SessionEvent::UnmatchedResponse) {
            return;
        }
    }
}

async fn send_ack(peer: &MockPeer, command: &StreamerCommand, code: i64) {
    let frame = format!(
        r#"{{"response":[{{"service":"{}","requestid":"{}","command":"{}","timestamp":1,"content":{{"code":{},"msg":"synthetic"}}}}]}}"#,
        command.service().manifest().name(),
        command.request_id().as_wire_value(),
        command.command().name(),
        code,
    );
    peer.send_frame(frame).await;
}

async fn acknowledge_success(peer: &MockPeer, command: &StreamerCommand) {
    let code = match command.command() {
        SubscriptionCommand::Subs => 26,
        SubscriptionCommand::Add => 28,
        SubscriptionCommand::Unsubs => 27,
        SubscriptionCommand::View => 29,
    };
    send_ack(peer, command, code).await;
}

async fn stop_runtime(
    control: &StreamerControl,
    task: tokio::task::JoinHandle<Result<(), SessionRunError>>,
) {
    control.shutdown().expect("shutdown fits the control queue");
    timeout(Duration::from_secs(1), task)
        .await
        .expect("runtime shuts down")
        .expect("runtime task does not panic")
        .expect("orderly shutdown succeeds");
}

async fn wait_for_adapter_task_state(probe: &CancellationProbe, active: bool) {
    let signal = if active {
        &probe.task_state.adapter_task_started
    } else {
        &probe.task_state.adapter_task_stopped
    };
    timeout(Duration::from_secs(1), async {
        loop {
            let notified = signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if probe.task_state.active.load(Ordering::Acquire) == active {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect("adapter handshake task reaches requested state");
}

#[tokio::test]
async fn one_socket_replays_services_and_correlates_per_service_ack() {
    let session = mock_session();
    let (factory, mut peers, connect_calls) = factory(vec![session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity key is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("option key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("one mock connection exists");
    let activity = peer.next_command().await;
    let equities = peer.next_command().await;
    let options = peer.next_command().await;
    assert_eq!(activity.service(), StreamerService::AcctActivity);
    assert_eq!(activity.command(), SubscriptionCommand::Subs);
    assert_eq!(activity.fields(), "0,1,2,3");
    assert_eq!(activity.keys_csv(), "Account Activity");
    assert_eq!(equities.service(), StreamerService::LevelOneEquities);
    assert_eq!(equities.fields(), "0,45,46,51,52");
    assert_eq!(equities.keys_csv(), "SYNTH-EQ");
    assert_eq!(options.service(), StreamerService::LevelOneOptions);
    assert_eq!(options.fields(), "0,2,3,38");
    assert_eq!(options.keys_csv(), "SYNTH-OPTION");

    acknowledge_success(peer, &activity).await;
    acknowledge_success(peer, &equities).await;
    acknowledge_success(peer, &options).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    assert_eq!(connect_calls.load(Ordering::Relaxed), 1);
    stop_runtime(&channels.control, task).await;
}

#[tokio::test]
async fn connect_timeout_cancels_handshake_and_closes_all_owned_resources() {
    let probe = Arc::new(CancellationProbe::default());
    let factory = CancellationFactory {
        probe: Arc::clone(&probe),
    };
    let config = SessionConfig {
        connect_timeout: Duration::from_millis(50),
        reconnect_initial_delay: Duration::from_secs(5),
        reconnect_max_delay: Duration::from_secs(5),
        ..SessionConfig::default()
    };
    let (runtime, mut channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    let task = tokio::spawn(runtime.run());

    wait_for_adapter_task_state(&probe, true).await;
    let disconnected = next_connection_event(&mut channels.critical, false).await;
    assert!(matches!(
        disconnected,
        SessionEvent::Connection {
            failure: Some(PortFailure::ConnectFailed),
            ..
        }
    ));
    assert!(probe.connect_future_dropped.load(Ordering::Acquire));
    assert!(!probe.partial_socket_open.load(Ordering::Acquire));
    {
        let buffer = probe
            .temporary_auth_buffer
            .lock()
            .expect("test buffer lock is available");
        assert!(!buffer.is_empty());
        assert!(buffer.iter().all(|byte| *byte == 0));
    }
    wait_for_adapter_task_state(&probe, false).await;

    channels
        .control
        .shutdown()
        .expect("shutdown fits the control queue");
    timeout(Duration::from_secs(1), task)
        .await
        .expect("runtime stops after cancellation")
        .expect("runtime task does not panic")
        .expect("orderly shutdown succeeds");

    timeout(Duration::from_secs(1), async {
        while channels.critical.recv().await.is_some() {}
    })
    .await
    .expect("critical receiver closes after queued events drain");
    assert!(
        timeout(Duration::from_secs(1), channels.market_data.recv())
            .await
            .expect("market-data receiver closes")
            .is_none()
    );
}

#[tokio::test]
async fn rejected_quote_subscription_degrades_only_that_service_and_can_retry() {
    let session = mock_session();
    let (factory, mut peers, connect_calls) = factory(vec![session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("option key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("mock connection exists");
    let activity = peer.next_command().await;
    let options = peer.next_command().await;
    acknowledge_success(peer, &activity).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    send_ack(peer, &options, 29).await;
    let rejected = next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Rejected,
    )
    .await;
    assert!(matches!(
        rejected,
        SessionEvent::ServiceStatus {
            readiness: ServiceReadiness::Degraded,
            ..
        }
    ));
    assert_eq!(connect_calls.load(Ordering::Relaxed), 1);

    channels
        .control
        .retry(StreamerService::LevelOneOptions)
        .expect("retry fits the control queue");
    let retry = peer.next_command().await;
    assert_eq!(retry.service(), StreamerService::LevelOneOptions);
    assert_eq!(retry.command(), SubscriptionCommand::Subs);
    acknowledge_success(peer, &retry).await;
    let ready = next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    assert!(matches!(
        ready,
        SessionEvent::ServiceStatus {
            readiness: ServiceReadiness::Ready,
            ..
        }
    ));
    assert_eq!(connect_calls.load(Ordering::Relaxed), 1);

    channels
        .control
        .view(StreamerService::LevelOneOptions)
        .expect("market service supports VIEW");
    let view = peer.next_command().await;
    assert_eq!(view.command(), SubscriptionCommand::View);
    assert_eq!(view.keys_csv(), "SYNTH-OPTION");
    acknowledge_success(peer, &view).await;
    let after_view = next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    assert!(matches!(
        after_view,
        SessionEvent::ServiceStatus {
            readiness: ServiceReadiness::Ready,
            ..
        }
    ));
    assert!(matches!(
        peer.outgoing.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    stop_runtime(&channels.control, task).await;
}

async fn assert_subscription_send_failure_reconnects(failure: MockSendFailure) {
    let first = mock_session();
    let second = mock_session();
    let (factory, mut peers, connect_calls) = factory(vec![first, second]);
    let mut config = fast_config();
    config.send_timeout = Duration::from_millis(30);
    let (runtime, mut channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("option key is valid");
    let first_peer = peers.first_mut().expect("first mock connection exists");
    match failure {
        MockSendFailure::Immediate => {
            first_peer.fail_next_send(StreamerService::LevelOneOptions);
        }
        MockSendFailure::PartialWriteThenPending => {
            first_peer.stall_next_send_after_partial_write(StreamerService::LevelOneOptions);
        }
    }
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let activity_first = first_peer.next_command().await;
    assert_eq!(activity_first.service(), StreamerService::AcctActivity);
    let old_generation = activity_first.connection_generation();
    if matches!(failure, MockSendFailure::PartialWriteThenPending) {
        let partially_written = first_peer.next_command().await;
        assert_eq!(
            partially_written.service(),
            StreamerService::LevelOneOptions
        );
        assert_eq!(partially_written.command(), SubscriptionCommand::Subs);
    }

    let disconnected = next_connection_event(&mut channels.critical, false).await;
    assert!(matches!(
        disconnected,
        SessionEvent::Connection {
            failure: Some(PortFailure::SendFailed),
            ..
        }
    ));
    next_connection_event(&mut channels.critical, true).await;
    assert_eq!(connect_calls.load(Ordering::Relaxed), 2);

    let second_peer = peers
        .get_mut(1)
        .expect("replacement mock connection exists");
    let activity_replay = second_peer.next_command().await;
    let options_replay = second_peer.next_command().await;
    assert_eq!(activity_replay.command(), SubscriptionCommand::Subs);
    assert_eq!(activity_replay.service(), StreamerService::AcctActivity);
    assert_eq!(options_replay.command(), SubscriptionCommand::Subs);
    assert_eq!(options_replay.keys_csv(), "SYNTH-OPTION");
    assert_ne!(activity_replay.connection_generation(), old_generation);

    acknowledge_success(second_peer, &activity_replay).await;
    acknowledge_success(second_peer, &options_replay).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    let ready = next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    assert!(matches!(
        ready,
        SessionEvent::ServiceStatus {
            readiness: ServiceReadiness::Ready,
            ..
        }
    ));
    stop_runtime(&channels.control, task).await;
}

#[tokio::test]
async fn subscription_send_error_drops_socket_and_replays_all_desired_services() {
    assert_subscription_send_failure_reconnects(MockSendFailure::Immediate).await;
}

#[tokio::test]
async fn partial_subscription_write_timeout_drops_socket_and_replays_all_desired_services() {
    assert_subscription_send_failure_reconnects(MockSendFailure::PartialWriteThenPending).await;
}

#[tokio::test]
async fn add_timeout_reconnects_with_full_subs_and_ignores_late_ack() {
    let first = mock_session();
    let second = mock_session();
    let (factory, mut peers, connect_calls) = factory(vec![first, second]);
    let mut config = fast_config();
    config.acknowledgement_timeout = Duration::from_millis(200);
    let (runtime, mut channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["A"])
        .expect("initial key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let first_peer = peers.first_mut().expect("first mock connection exists");
    let activity_first = first_peer.next_command().await;
    let options_first = first_peer.next_command().await;
    acknowledge_success(first_peer, &activity_first).await;
    acknowledge_success(first_peer, &options_first).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["A", "B"])
        .expect("new key set is valid");
    let add = first_peer.next_command().await;
    assert_eq!(add.command(), SubscriptionCommand::Add);
    assert_eq!(add.keys_csv(), "B");
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::TimedOut,
    )
    .await;
    assert_eq!(
        connect_calls.load(Ordering::Relaxed),
        1,
        "one service timeout must not tear down the shared activity socket"
    );
    acknowledge_success(first_peer, &add).await;
    assert_eq!(
        next_ignored_ack(&mut channels.critical).await,
        AckIgnoreReason::UnknownRequest
    );
    assert_eq!(connect_calls.load(Ordering::Relaxed), 1);

    first_peer.close();
    next_connection_event(&mut channels.critical, false).await;
    next_connection_event(&mut channels.critical, true).await;
    let second_peer = peers
        .get_mut(1)
        .expect("replacement mock connection exists");
    let activity_replay = second_peer.next_command().await;
    let options_replay = second_peer.next_command().await;
    assert_eq!(activity_replay.command(), SubscriptionCommand::Subs);
    assert_eq!(options_replay.command(), SubscriptionCommand::Subs);
    assert_eq!(options_replay.keys_csv(), "A,B");
    assert_ne!(
        options_replay.connection_generation(),
        add.connection_generation()
    );
    assert_ne!(options_replay.request_id(), add.request_id());

    acknowledge_success(second_peer, &activity_replay).await;
    send_ack(second_peer, &add, 28).await;
    assert_eq!(
        next_ignored_ack(&mut channels.critical).await,
        AckIgnoreReason::UnknownRequest
    );
    acknowledge_success(second_peer, &options_replay).await;
    let ready = next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    assert!(matches!(
        ready,
        SessionEvent::ServiceStatus {
            readiness: ServiceReadiness::Ready,
            ..
        }
    ));
    assert_eq!(connect_calls.load(Ordering::Relaxed), 2);
    stop_runtime(&channels.control, task).await;
}

#[tokio::test]
async fn sparse_market_deltas_coalesce_and_ignore_exact_same_timestamp_duplicates() {
    let session = mock_session();
    let (factory, mut peers, _) = factory(vec![session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("mock connection exists");
    let activity = peer.next_command().await;
    let equities = peer.next_command().await;
    acknowledge_success(peer, &activity).await;
    acknowledge_success(peer, &equities).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":10,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
    )
    .await;
    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":11,"command":"SUBS","content":[{"key":"SYNTH-EQ","46":1800000000000}]}]}"#,
    )
    .await;
    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":11,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
    )
    .await;
    // Node's existing snapshot characterization accepts the changed same-time
    // sparse row above and drops this exact repeat without renewing freshness.
    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":11,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
    )
    .await;
    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":9,"command":"SUBS","content":[{"key":"SYNTH-EQ","47":999}]}]}"#,
    )
    .await;
    peer.send_frame(
        r#"{"response":[{"service":"ADMIN","requestid":"999","command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"barrier"}}]}"#,
    )
    .await;
    loop {
        let event = timeout(Duration::from_secs(1), channels.critical.recv())
            .await
            .expect("routing barrier arrives")
            .expect("runtime remains active");
        if matches!(event, SessionEvent::UnmatchedResponse) {
            break;
        }
    }

    let update = timeout(Duration::from_secs(1), channels.market_data.recv())
        .await
        .expect("coalesced quote arrives")
        .expect("market buffer remains active");
    assert_eq!(update.service, StreamerService::LevelOneEquities);
    assert_eq!(update.key, "SYNTH-EQ");
    assert_eq!(update.source_timestamp, 11.0);
    assert_eq!(update.revision, 3);
    assert_eq!(update.coalesced_updates, 2);
    assert_eq!(update.fields.get("45"), Some(&Value::from(600)));
    assert_eq!(
        update.fields.get("46"),
        Some(&Value::from(1_800_000_000_000i64))
    );
    assert_eq!(update.field_provenance["45"].source_timestamp, 11.0);
    assert_eq!(update.field_provenance["46"].source_timestamp, 11.0);
    assert_eq!(update.field_provenance["45"].update_revision, 3);
    assert_eq!(update.field_provenance["46"].update_revision, 2);
    assert_eq!(
        update.field_provenance["45"].received_at,
        update.received_at
    );
    assert!(!update.fields.contains_key("47"));
    assert!(!update.field_provenance.contains_key("47"));
    let debug = format!("{update:?}");
    assert!(!debug.contains("SYNTH-EQ"));
    assert!(debug.contains("key: \"[REDACTED]\""));
    assert!(debug.contains("fields: \"[REDACTED]\""));
    assert!(debug.contains("field_provenance: \"[REDACTED]\""));
    stop_runtime(&channels.control, task).await;
}

#[tokio::test]
async fn fatal_market_data_capacity_discards_pending_quotes_and_closes_receivers() {
    let session = mock_session();
    let (factory, mut peers, _) = factory(vec![session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("mock connection exists");
    let activity = peer.next_command().await;
    let equities = peer.next_command().await;
    acknowledge_success(peer, &activity).await;
    acknowledge_success(peer, &equities).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":1,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
    )
    .await;
    send_route_barrier(peer, &mut channels.critical).await;

    let mut oversized_content = serde_json::Map::new();
    oversized_content.insert("key".to_owned(), Value::String("SYNTH-EQ".to_owned()));
    for index in 0..=MAX_MERGED_MARKET_FIELDS {
        oversized_content.insert(format!("field-{index:03}"), Value::from(index));
    }
    let oversized_frame = serde_json::json!({
        "data": [{
            "service": "LEVELONE_EQUITIES",
            "timestamp": 2,
            "command": "SUBS",
            "content": [Value::Object(oversized_content)]
        }]
    });
    peer.send_frame(serde_json::to_vec(&oversized_frame).expect("synthetic frame serializes"))
        .await;

    let run_error = timeout(Duration::from_secs(1), task)
        .await
        .expect("fatal capacity error terminates the runtime")
        .expect("runtime task does not panic")
        .expect_err("oversized market update fails closed");
    assert_eq!(run_error, SessionRunError::MarketDataCapacityExceeded);
    assert!(
        timeout(Duration::from_millis(50), channels.market_data.recv())
            .await
            .expect("closed market-data receiver wakes")
            .is_none(),
        "a queued pre-failure quote is never delivered after runtime failure"
    );
    timeout(Duration::from_millis(50), async {
        while channels.critical.recv().await.is_some() {}
    })
    .await
    .expect("critical receiver closes after queued events drain");
}

#[tokio::test]
async fn aborting_runtime_discards_pending_quotes_before_receiver_returns_none() {
    let session = mock_session();
    let (factory, mut peers, _) = factory(vec![session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity key is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("mock connection exists");
    let activity = peer.next_command().await;
    let equities = peer.next_command().await;
    acknowledge_success(peer, &activity).await;
    acknowledge_success(peer, &equities).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    peer.send_frame(
        r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":10,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
    )
    .await;
    send_route_barrier(peer, &mut channels.critical).await;
    let (closed, queued_rows, fences, fence_bytes) =
        channels.market_data.buffer_state_counts_for_test().await;
    assert!(!closed);
    assert_eq!(queued_rows, 1, "quote is queued before task abort");
    assert_eq!(fences, 1, "historical ordering fence is retained");
    assert!(fence_bytes > 0);

    task.abort();
    let cancelled = task.await.expect_err("abort cancels the owner task");
    assert!(cancelled.is_cancelled());

    assert!(
        timeout(Duration::from_millis(50), channels.market_data.recv())
            .await
            .expect("aborted runtime wakes the market-data receiver")
            .is_none(),
        "a pre-abort quote is not delivered"
    );
    let state = channels.market_data.buffer_state_counts_for_test().await;
    assert_eq!(state, (true, 0, 0, 0));
}

#[tokio::test]
// Keep the reconnect timeline in one deterministic integration test so each
// observed quote and generation transition has a visible predecessor.
#[allow(clippy::too_many_lines)]
async fn delivered_market_rows_keep_order_fences_across_reconnect_generations() {
    let first_session = mock_session();
    let second_session = mock_session();
    let (factory, mut peers, connect_calls) = factory(vec![first_session, second_session]);
    let (runtime, mut channels) =
        StreamerRuntime::new(factory, fast_config()).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity key is valid");
    let task = tokio::spawn(runtime.run());

    let first_connection = next_connection_event(&mut channels.critical, true).await;
    let SessionEvent::Connection {
        generation: first_generation,
        ..
    } = first_connection
    else {
        unreachable!("connection helper returns a connection event");
    };
    let first_peer = peers.first_mut().expect("first mock connection exists");
    let activity = first_peer.next_command().await;
    let equities = first_peer.next_command().await;
    acknowledge_success(first_peer, &activity).await;
    acknowledge_success(first_peer, &equities).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    first_peer
        .send_frame(
            r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":20,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
        )
        .await;
    send_route_barrier(first_peer, &mut channels.critical).await;
    let initial = timeout(Duration::from_secs(1), channels.market_data.recv())
        .await
        .expect("first quote arrives")
        .expect("market-data receiver remains active");
    assert_eq!(initial.generation, first_generation);
    assert_eq!(initial.revision, 1);
    assert_eq!(initial.source_timestamp, 20.0);

    first_peer
        .send_frame(
            r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":20,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":600}]}]}"#,
        )
        .await;
    first_peer
        .send_frame(
            r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":19,"command":"SUBS","content":[{"key":"SYNTH-EQ","46":1800000000000}]}]}"#,
        )
        .await;
    send_route_barrier(first_peer, &mut channels.critical).await;
    assert!(
        timeout(Duration::from_millis(50), channels.market_data.recv())
            .await
            .is_err()
    );

    first_peer
        .send_frame(
            r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":20,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":601}]}]}"#,
        )
        .await;
    send_route_barrier(first_peer, &mut channels.critical).await;
    let changed = timeout(Duration::from_secs(1), channels.market_data.recv())
        .await
        .expect("changed same-time quote arrives")
        .expect("market-data receiver remains active");
    assert_eq!(changed.generation, first_generation);
    assert_eq!(changed.revision, 2);
    assert_eq!(changed.source_timestamp, 20.0);
    assert_eq!(changed.fields.get("45"), Some(&Value::from(601)));
    assert!(!changed.fields.contains_key("46"));

    first_peer.close();
    let disconnected = next_connection_event(&mut channels.critical, false).await;
    assert!(matches!(
        disconnected,
        SessionEvent::Connection {
            failure: Some(PortFailure::Closed),
            ..
        }
    ));
    let second_connection = next_connection_event(&mut channels.critical, true).await;
    let SessionEvent::Connection {
        generation: second_generation,
        ..
    } = second_connection
    else {
        unreachable!("connection helper returns a connection event");
    };
    assert!(second_generation.value() > first_generation.value());

    let second_peer = peers.get_mut(1).expect("second mock connection exists");
    let activity = second_peer.next_command().await;
    let equities = second_peer.next_command().await;
    acknowledge_success(second_peer, &activity).await;
    acknowledge_success(second_peer, &equities).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneEquities,
        ServiceStatusCause::Acknowledged,
    )
    .await;
    second_peer
        .send_frame(
            r#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":1,"command":"SUBS","content":[{"key":"SYNTH-EQ","45":700}]}]}"#,
        )
        .await;
    send_route_barrier(second_peer, &mut channels.critical).await;
    let after_reconnect = timeout(Duration::from_secs(1), channels.market_data.recv())
        .await
        .expect("new-generation quote arrives")
        .expect("market-data receiver remains active");
    assert_eq!(after_reconnect.generation, second_generation);
    assert_eq!(after_reconnect.revision, 1);
    assert_eq!(after_reconnect.source_timestamp, 1.0);
    assert_eq!(after_reconnect.fields.get("45"), Some(&Value::from(700)));
    assert_eq!(connect_calls.load(Ordering::Relaxed), 2);
    stop_runtime(&channels.control, task).await;
}

#[tokio::test]
async fn critical_activity_queue_overflow_is_a_fatal_explicit_error() {
    let session = mock_session();
    let (factory, mut peers, _) = factory(vec![session]);
    let mut config = fast_config();
    config.critical_capacity = 2;
    let (runtime, mut channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    let task = tokio::spawn(runtime.run());

    next_connection_event(&mut channels.critical, true).await;
    let peer = peers.first_mut().expect("mock connection exists");
    let activity = peer.next_command().await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceStatusCause::CommandSent,
    )
    .await;
    acknowledge_success(peer, &activity).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceStatusCause::Acknowledged,
    )
    .await;

    let event = r#"{"data":[{"service":"ACCT_ACTIVITY","timestamp":1,"command":"SUBS","content":[{"key":"SYNTH-ACCOUNT","seq":1,"1":"SYNTH-ACCOUNT","2":"ORDER","3":{"id":"SYNTH-ORDER"}}]}]}"#;
    peer.send_frame(event).await;
    peer.send_frame(event).await;
    peer.send_frame(event).await;
    let result = timeout(Duration::from_secs(1), task)
        .await
        .expect("overflow terminates the owner task")
        .expect("owner task does not panic");
    assert_eq!(result, Err(SessionRunError::CriticalDeliveryOverflow));
    assert!(matches!(
        channels.critical.recv().await,
        Some(SessionEvent::Activity { .. })
    ));
    assert!(matches!(
        channels.critical.recv().await,
        Some(SessionEvent::Activity { .. })
    ));
    assert!(
        timeout(Duration::from_millis(50), channels.critical.recv())
            .await
            .expect("closed critical receiver wakes")
            .is_none()
    );
    assert!(
        timeout(Duration::from_millis(50), channels.market_data.recv())
            .await
            .expect("closed market receiver wakes")
            .is_none()
    );
}

#[test]
fn control_input_is_bounded_and_cannot_remove_activity_service() {
    let session = mock_session();
    let (factory, _, _) = factory(vec![session]);
    let mut config = fast_config();
    config.control_capacity = 1;
    let (_runtime, channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    assert_eq!(
        channels
            .control
            .set_desired(StreamerService::AcctActivity, std::iter::empty::<&str>()),
        Err(SessionControlError::MandatoryActivitySubscription)
    );
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("one item fits the mailbox");
    assert_eq!(
        channels
            .control
            .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"]),
        Err(SessionControlError::QueueFull)
    );
}
