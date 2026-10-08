//! Exercises the extracted Schwab Streamer runtime through a fake socket port.
//! Numeric service fields remain opaque; this test proves protocol reuse only.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use schwab_streamer::{
    AuthenticatedSessionFactory, ConnectionGeneration, PortFailure, ServiceReadiness,
    ServiceStatusCause, SessionConfig, SessionEvent, SocketEvent, StreamerCommand, StreamerRuntime,
    StreamerService, StreamerSocket,
};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommandObservation {
    generation: u64,
    request_id: u64,
    service: &'static str,
    command: &'static str,
    fields: &'static str,
}

struct FakeSocketFactory {
    inbound_tx: mpsc::Sender<Vec<u8>>,
    inbound_rx: Option<mpsc::Receiver<Vec<u8>>>,
    commands: Arc<Mutex<Vec<CommandObservation>>>,
}

struct FakeSocket {
    inbound_tx: mpsc::Sender<Vec<u8>>,
    inbound_rx: mpsc::Receiver<Vec<u8>>,
    commands: Arc<Mutex<Vec<CommandObservation>>>,
}

impl AuthenticatedSessionFactory for FakeSocketFactory {
    type Socket = FakeSocket;

    fn connect_authenticated(
        &mut self,
        _generation: ConnectionGeneration,
        _login_request_id: schwab_streamer::RequestId,
    ) -> impl Future<Output = Result<Self::Socket, PortFailure>> + Send {
        async move {
            let inbound_rx = self.inbound_rx.take().ok_or(PortFailure::ConnectFailed)?;
            Ok(FakeSocket {
                inbound_tx: self.inbound_tx.clone(),
                inbound_rx,
                commands: Arc::clone(&self.commands),
            })
        }
    }
}

impl StreamerSocket for FakeSocket {
    fn send_subscription(
        &mut self,
        command: &StreamerCommand,
    ) -> impl Future<Output = Result<(), PortFailure>> + Send {
        let ack = format!(
            "{{\"response\":[{{\"service\":\"{}\",\"requestid\":\"{}\",\"command\":\"{}\",\"timestamp\":1,\"content\":{{\"code\":0,\"msg\":\"OK\"}}}}]}}",
            command.service().manifest().name(),
            command.request_id().as_wire_value(),
            command.command().name(),
        )
        .into_bytes();
        let data = if command.service() == StreamerService::LevelOneEquities {
            Some(
                br#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":1700000000000,"command":"SUBS","content":[{"key":"SYNTHETIC-STREAM-KEY","45":12.3450,"52":"opaque-synthetic-field"}]}]}"#
                    .to_vec(),
            )
        } else {
            None
        };
        self.commands
            .lock()
            .expect("synthetic command record lock")
            .push(CommandObservation {
                generation: command.connection_generation().value(),
                request_id: command.request_id().value(),
                service: command.service().manifest().name(),
                command: command.command().name(),
                fields: command.fields(),
            });
        let sender = self.inbound_tx.clone();
        async move {
            sender
                .send(ack)
                .await
                .map_err(|_| PortFailure::SendFailed)?;
            if let Some(data) = data {
                sender
                    .send(data)
                    .await
                    .map_err(|_| PortFailure::SendFailed)?;
            }
            Ok(())
        }
    }

    fn receive_frame(
        &mut self,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, PortFailure>> + Send {
        async move { Ok(self.inbound_rx.recv().await) }
    }

    fn receive_event(
        &mut self,
    ) -> impl Future<Output = Result<Option<SocketEvent>, PortFailure>> + Send {
        async move { Ok(self.inbound_rx.recv().await.map(SocketEvent::Frame)) }
    }
}

#[tokio::test]
async fn fake_socket_drives_the_vendored_runtime_without_field_semantic_mapping() {
    let (inbound_tx, inbound_rx) = mpsc::channel(16);
    let commands = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeSocketFactory {
        inbound_tx,
        inbound_rx: Some(inbound_rx),
        commands: Arc::clone(&commands),
    };
    let (runtime, channels) =
        StreamerRuntime::new(factory, SessionConfig::default()).expect("default config is bounded");
    channels
        .control
        .set_desired(StreamerService::LevelOneEquities, ["SYNTHETIC-STREAM-KEY"])
        .expect("one synthetic instrument key is valid");
    let task = tokio::spawn(runtime.run());
    let mut critical = channels.critical;
    let mut market_data = channels.market_data;

    let ready = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(SessionEvent::ServiceStatus {
                service: StreamerService::LevelOneEquities,
                readiness: ServiceReadiness::Ready,
                cause: ServiceStatusCause::Acknowledged,
                ..
            }) = critical.recv().await
            {
                break;
            }
        }
    })
    .await;
    assert!(
        ready.is_ok(),
        "matching synthetic ACK reaches SDK Ready state"
    );

    let update = tokio::time::timeout(Duration::from_secs(2), market_data.recv())
        .await
        .expect("synthetic market row arrives within the test deadline")
        .expect("runtime has not shut down");
    assert_eq!(update.service, StreamerService::LevelOneEquities);
    assert_eq!(update.key, "SYNTHETIC-STREAM-KEY");
    assert_eq!(update.generation.value(), 1);
    assert_eq!(update.revision, 1);
    assert_eq!(update.fields.get("45").unwrap().to_string(), "12.3450");
    assert_eq!(
        update
            .field_provenance
            .get("45")
            .expect("field provenance is retained")
            .update_revision,
        update.revision
    );
    assert_eq!(
        update
            .field_provenance
            .get("52")
            .expect("second field provenance is retained")
            .update_revision,
        update.revision
    );

    let observations = commands.lock().expect("synthetic command records").clone();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].service, "ACCT_ACTIVITY");
    assert_eq!(observations[1].service, "LEVELONE_EQUITIES");
    assert_eq!(observations[1].fields, "0,45,46,51,52");
    assert!(
        observations
            .iter()
            .all(|observation| observation.generation == 1 && observation.request_id > 0)
    );

    channels
        .control
        .shutdown()
        .expect("runtime shutdown is bounded");
    task.await
        .expect("synthetic runtime task joined")
        .expect("synthetic runtime shut down cleanly");
}
