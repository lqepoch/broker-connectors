use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};

use super::*;

type TestWebSocket = WebSocketStream<TcpStream>;

#[derive(Clone)]
struct LoopbackCredentialProvider {
    endpoint: Arc<str>,
}

impl StreamerCredentialProvider for LoopbackCredentialProvider {
    fn load_session_credentials(
        &mut self,
    ) -> impl Future<Output = Result<StreamerSessionCredentials, CredentialProviderFailure>> + Send
    {
        let endpoint = Arc::clone(&self.endpoint);
        async move {
            let secret = StreamerLoginSecret::new("synthetic-access-token".to_owned())
                .map_err(|_| CredentialProviderFailure::Unavailable)?;
            StreamerSessionCredentials::for_loopback_test(
                endpoint.to_string(),
                "synthetic-customer".to_owned(),
                "synthetic-correlation".to_owned(),
                "N9".to_owned(),
                "synthetic-function".to_owned(),
                secret,
            )
            .map_err(|_| CredentialProviderFailure::Unavailable)
        }
    }
}

async fn listen() -> (TcpListener, Arc<str>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("loopback listener binds");
    let address = listener
        .local_addr()
        .expect("loopback address is available");
    (listener, Arc::from(format!("ws://{address}")))
}

async fn accept(listener: &TcpListener) -> TestWebSocket {
    let (stream, _) = listener.accept().await.expect("loopback peer connects");
    accept_async(stream)
        .await
        .expect("loopback websocket handshake succeeds")
}

async fn next_json(socket: &mut TestWebSocket) -> Value {
    loop {
        match socket.next().await.expect("websocket frame arrives") {
            Ok(Message::Text(text)) => {
                return serde_json::from_str(text.as_str()).expect("provider message is JSON");
            }
            Ok(Message::Binary(bytes)) => {
                return serde_json::from_slice(&bytes).expect("provider message is JSON");
            }
            Ok(Message::Ping(payload)) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("test peer returns a pong");
            }
            Ok(Message::Pong(_) | Message::Frame(_)) => {}
            Ok(Message::Close(_)) => panic!("socket closed before expected JSON frame"),
            Err(error) => panic!("loopback websocket read failed: {error}"),
        }
    }
}

async fn next_request(socket: &mut TestWebSocket) -> Value {
    next_json(socket).await["requests"][0].clone()
}

async fn send_json(socket: &mut TestWebSocket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("test peer sends JSON");
}

async fn send_ack(socket: &mut TestWebSocket, request: &Value, code: i64) {
    send_json(
        socket,
        json!({
            "response": [{
                "service": request["service"],
                "requestid": request["requestid"],
                "command": request["command"],
                "timestamp": 1,
                "content": {"code": code, "msg": "synthetic"}
            }]
        }),
    )
    .await;
}

fn assert_login(request: &Value, request_id: &str) {
    assert_eq!(request["service"], "ADMIN");
    assert_eq!(request["command"], "LOGIN");
    assert_eq!(request["requestid"], request_id);
    assert_eq!(request["SchwabClientCustomerId"], "synthetic-customer");
    assert_eq!(request["SchwabClientCorrelId"], "synthetic-correlation");
    assert_eq!(
        request["parameters"]["Authorization"],
        "synthetic-access-token"
    );
    assert_eq!(request["parameters"]["SchwabClientChannel"], "N9");
    assert_eq!(
        request["parameters"]["SchwabClientFunctionId"],
        "synthetic-function"
    );
}

async fn next_connection(
    receiver: &mut CriticalEventReceiver,
    connected: bool,
) -> ConnectionGeneration {
    loop {
        let event = timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("connection state arrives")
            .expect("runtime channels remain open");
        if let SessionEvent::Connection {
            generation,
            connected: value,
            ..
        } = event
            && value == connected
        {
            return generation;
        }
    }
}

async fn next_service_status(
    receiver: &mut CriticalEventReceiver,
    service: StreamerService,
    readiness: ServiceReadiness,
) {
    loop {
        let event = timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("service state arrives")
            .expect("runtime channels remain open");
        if matches!(
            event,
            SessionEvent::ServiceStatus {
                service: current,
                readiness: value,
                ..
            } if current == service && value == readiness
        ) {
            return;
        }
    }
}

async fn next_activity(receiver: &mut CriticalEventReceiver) {
    loop {
        let event = timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("activity event arrives")
            .expect("runtime channels remain open");
        if matches!(event, SessionEvent::Activity { .. }) {
            return;
        }
    }
}

#[tokio::test]
// One fake loopback server exercises login, service-local rejection, and
// reconnect sequencing without contacting an external provider.
#[allow(clippy::too_many_lines)]
async fn one_socket_factory_login_and_runtime_keep_service_failure_local() {
    let (listener, endpoint) = listen().await;
    let (force_reconnect_sender, force_reconnect_receiver) = oneshot::channel();
    let (shutdown_server_sender, shutdown_server_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut initial_peer = accept(&listener).await;
        let login = next_request(&mut initial_peer).await;
        assert_login(&login, "1");

        let ping = b"synthetic-ping".to_vec();
        initial_peer
            .send(Message::Ping(ping.clone().into()))
            .await
            .expect("test peer sends WebSocket ping");
        let returned_frame = timeout(Duration::from_secs(1), initial_peer.next())
            .await
            .expect("client returns a bounded PONG")
            .expect("client keeps socket open")
            .expect("client PONG frame is valid");
        assert!(matches!(returned_frame, Message::Pong(payload) if payload.as_ref() == ping));

        // A stale/unrelated ADMIN ACK cannot authenticate this socket.
        send_json(
            &mut initial_peer,
            json!({"response": [{
                "service": "ADMIN",
                "requestid": "999",
                "command": "LOGIN",
                "timestamp": 1,
                "content": {"code": 0, "msg": "synthetic"}
            }]}),
        )
        .await;
        sleep(Duration::from_millis(30)).await;
        send_ack(&mut initial_peer, &login, 0).await;

        let queued_requests = [
            next_request(&mut initial_peer).await,
            next_request(&mut initial_peer).await,
            next_request(&mut initial_peer).await,
        ];
        assert_eq!(queued_requests[0]["service"], "ACCT_ACTIVITY");
        assert_eq!(queued_requests[0]["requestid"], "2");
        assert_eq!(queued_requests[0]["command"], "SUBS");
        assert_eq!(queued_requests[0]["parameters"]["keys"], "Account Activity");
        assert_eq!(queued_requests[0]["parameters"]["fields"], "0,1,2,3");
        assert_eq!(queued_requests[1]["service"], "LEVELONE_EQUITIES");
        assert_eq!(queued_requests[1]["requestid"], "3");
        assert_eq!(queued_requests[1]["parameters"]["keys"], "SYNTH-EQ");
        assert_eq!(queued_requests[1]["parameters"]["fields"], "0,45,46,51,52");
        assert_eq!(queued_requests[2]["service"], "LEVELONE_OPTIONS");
        assert_eq!(queued_requests[2]["requestid"], "4");
        assert_eq!(queued_requests[2]["parameters"]["keys"], "SYNTH-OPTION");
        assert_eq!(queued_requests[2]["parameters"]["fields"], "0,2,3,38");

        // A failed OPTIONS service does not tear down the shared socket.
        send_ack(&mut initial_peer, &queued_requests[2], 90).await;
        send_ack(&mut initial_peer, &queued_requests[0], 26).await;
        send_ack(&mut initial_peer, &queued_requests[1], 26).await;
        send_json(
            &mut initial_peer,
            json!({"data": [{
                "service": "ACCT_ACTIVITY",
                "timestamp": 2,
                "command": "SUBS",
                "content": [{"1": "synthetic-order-id", "2": "EXECUTION"}]
            }]}),
        )
        .await;
        let _ = force_reconnect_receiver.await;
        initial_peer
            .send(Message::Close(None))
            .await
            .expect("test peer closes first socket");
        drop(initial_peer);

        let mut second = accept(&listener).await;
        let login = next_request(&mut second).await;
        assert_login(&login, "5");
        send_ack(&mut second, &login, 0).await;
        let replay = [
            next_request(&mut second).await,
            next_request(&mut second).await,
            next_request(&mut second).await,
        ];
        assert_eq!(replay[0]["service"], "ACCT_ACTIVITY");
        assert_eq!(replay[0]["requestid"], "6");
        assert_eq!(replay[0]["parameters"]["keys"], "Account Activity");
        assert_eq!(replay[1]["service"], "LEVELONE_EQUITIES");
        assert_eq!(replay[1]["requestid"], "7");
        assert_eq!(replay[1]["parameters"]["keys"], "SYNTH-EQ");
        assert_eq!(replay[2]["service"], "LEVELONE_OPTIONS");
        assert_eq!(replay[2]["requestid"], "8");
        assert_eq!(replay[2]["parameters"]["keys"], "SYNTH-OPTION");
        let _ = shutdown_server_receiver.await;
        drop(second);
    });

    let provider = LoopbackCredentialProvider { endpoint };
    let factory = SchwabStreamerSessionFactory::for_loopback_test(provider);
    let config = SessionConfig {
        connect_timeout: Duration::from_secs(1),
        acknowledgement_timeout: Duration::from_millis(500),
        send_timeout: Duration::from_millis(500),
        reconnect_initial_delay: Duration::from_millis(5),
        reconnect_max_delay: Duration::from_millis(20),
        ..SessionConfig::default()
    };
    let (runtime, mut channels) = StreamerRuntime::new(factory, config).expect("config is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity subscription is valid");
    channels
        .control
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("option subscription is valid");
    let runtime_task = tokio::spawn(runtime.run());

    next_connection(&mut channels.critical, true).await;
    next_service_status(
        &mut channels.critical,
        StreamerService::LevelOneOptions,
        ServiceReadiness::Degraded,
    )
    .await;
    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceReadiness::Ready,
    )
    .await;
    next_activity(&mut channels.critical).await;
    let _ = force_reconnect_sender.send(());
    let first_generation = next_connection(&mut channels.critical, false).await;
    let second_generation = next_connection(&mut channels.critical, true).await;
    assert!(second_generation.value() > first_generation.value());

    next_service_status(
        &mut channels.critical,
        StreamerService::AcctActivity,
        ServiceReadiness::Pending,
    )
    .await;
    channels.control.shutdown().expect("shutdown is accepted");
    timeout(Duration::from_secs(2), runtime_task)
        .await
        .expect("runtime shuts down")
        .expect("runtime task joins")
        .expect("runtime closes cleanly");
    let _ = shutdown_server_sender.send(());
    timeout(Duration::from_secs(2), server)
        .await
        .expect("loopback server exits")
        .expect("loopback server task joins");
}

#[tokio::test]
async fn dropping_authentication_future_closes_its_loopback_socket() {
    let (listener, endpoint) = listen().await;
    let (login_seen_sender, login_seen_receiver) = oneshot::channel();
    let (socket_closed_sender, socket_closed_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let login = next_request(&mut socket).await;
        assert_login(&login, "1");
        let _ = login_seen_sender.send(());
        let closed = timeout(Duration::from_secs(1), socket.next()).await;
        assert!(closed.is_ok());
        let _ = socket_closed_sender.send(());
    });

    let mut factory =
        SchwabStreamerSessionFactory::for_loopback_test(LoopbackCredentialProvider { endpoint });
    let mut connect =
        Box::pin(factory.connect_authenticated(ConnectionGeneration::new(1), RequestId::new(1)));
    tokio::select! {
        result = &mut connect => panic!("authentication returned before a matching ACK: {}", if result.is_ok() { "socket returned" } else { "fixed-category error" }),
        result = login_seen_receiver => result.expect("server observes LOGIN"),
    }
    drop(connect);
    timeout(Duration::from_secs(2), socket_closed_receiver)
        .await
        .expect("cancelled adapter closes the direct-owned socket")
        .expect("server observes socket closure");
    timeout(Duration::from_secs(2), server)
        .await
        .expect("server task completes")
        .expect("server task joins");
}

#[tokio::test]
async fn rejected_login_ack_does_not_return_an_authenticated_socket() {
    let (listener, endpoint) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let login = next_request(&mut socket).await;
        assert_login(&login, "1");
        send_ack(&mut socket, &login, 90).await;
        let closed = timeout(Duration::from_secs(1), socket.next()).await;
        assert!(closed.is_ok(), "rejected LOGIN closes its socket");
    });

    let mut factory =
        SchwabStreamerSessionFactory::for_loopback_test(LoopbackCredentialProvider { endpoint });
    let result = timeout(
        Duration::from_secs(2),
        factory.connect_authenticated(ConnectionGeneration::new(1), RequestId::new(1)),
    )
    .await
    .expect("LOGIN response is handled within its deadline");
    assert!(matches!(
        result,
        Err(PortFailure::AuthenticationUnavailable)
    ));
    timeout(Duration::from_secs(2), server)
        .await
        .expect("loopback peer observes failed authentication cleanup")
        .expect("loopback server task joins");
}
