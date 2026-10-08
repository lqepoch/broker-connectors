//! Test-only authenticated one-socket factory for the current Schwab wire flow.
//! 仅供 crate 自有合成测试使用的认证会话工厂。

use std::future::Future;
use std::time::Duration;

use tokio::time::timeout;

use crate::command::{ConnectionGeneration, RequestId};
use crate::credentials::{CredentialProviderFailure, StreamerCredentialProvider};
use crate::protocol::serialize_login;
use crate::session::{AuthenticatedSessionFactory, PortFailure, SocketEvent, StreamerSocket};
use crate::socket::SchwabStreamerSocket;
use crate::wire::{is_successful_streamer_command, parse_streamer_frame};

/// Test-only factory that owns one synthetic authenticated WebSocket at a time.
///
/// This module is compiled only for this crate's unit tests. The test-only
/// constructor accepts loopback `ws://` endpoints; all remote targets are
/// rejected because no official exact endpoint allowlist is available.
/// 中文摘要：仅用于 crate 自有测试的 loopback 假 socket；生产 library 不编译或导出该登录运行时。
pub struct SchwabStreamerSessionFactory<P> {
    credential_provider: P,
    login_ack_timeout: Duration,
    #[cfg(test)]
    allow_loopback_ws: bool,
}

impl<P> SchwabStreamerSessionFactory<P> {
    #[cfg(test)]
    pub(crate) fn for_loopback_test(credential_provider: P) -> Self {
        Self {
            credential_provider,
            login_ack_timeout: Duration::from_secs(1),
            allow_loopback_ws: true,
        }
    }
}

impl<P> AuthenticatedSessionFactory for SchwabStreamerSessionFactory<P>
where
    P: StreamerCredentialProvider,
{
    type Socket = SchwabStreamerSocket;

    async fn connect_authenticated(
        &mut self,
        _generation: ConnectionGeneration,
        login_request_id: RequestId,
    ) -> Result<Self::Socket, PortFailure> {
        let credentials = self
            .credential_provider
            .load_session_credentials()
            .await
            .map_err(map_credential_failure)?;

        // The only plaintext WebSocket exception is compiled into the
        // crate's unit-test constructor and restricted by credential
        // validation to loopback hosts.
        #[cfg(test)]
        let loopback_test = self.allow_loopback_ws;
        #[cfg(not(test))]
        let loopback_test = false;
        let (mut socket, login_payload) = connect_before_login(
            credentials.socket_url.as_str(),
            loopback_test,
            || async {
                SchwabStreamerSocket::connect(
                    credentials.socket_url.as_str(),
                    credentials.customer_id.clone(),
                    credentials.correlation_id.clone(),
                )
                .await
            },
            || serialize_login(&credentials, login_request_id),
        )
        .await?;
        socket.send_login(&login_payload).await?;

        match timeout(
            self.login_ack_timeout,
            wait_for_login_ack(&mut socket, login_request_id),
        )
        .await
        {
            Ok(Ok(true)) => Ok(socket),
            Ok(Ok(false) | Err(_)) | Err(_) => Err(PortFailure::AuthenticationUnavailable),
        }
    }
}

fn map_credential_failure(_failure: CredentialProviderFailure) -> PortFailure {
    PortFailure::AuthenticationUnavailable
}

async fn wait_for_login_ack(
    socket: &mut SchwabStreamerSocket,
    request_id: RequestId,
) -> Result<bool, PortFailure> {
    let request_id = request_id.as_wire_value();
    loop {
        match socket.receive_event().await? {
            Some(SocketEvent::Liveness) => {}
            Some(SocketEvent::Frame(bytes)) => {
                let Ok(frame) = parse_streamer_frame(&bytes) else {
                    continue;
                };
                let Some(responses) = frame.response else {
                    continue;
                };
                for response in responses {
                    if response.service != "ADMIN"
                        || response.command != "LOGIN"
                        || response.request_id != request_id
                    {
                        continue;
                    }
                    return Ok(is_successful_streamer_command(
                        &response.service,
                        &response.command,
                        response.content.code,
                    ));
                }
            }
            None => return Err(PortFailure::Closed),
        }
    }
}

fn endpoint_allowed(endpoint: &str, allow_loopback_ws: bool) -> bool {
    let Ok(url) = url::Url::parse(endpoint) else {
        return false;
    };
    allow_loopback_ws
        && url.scheme() == "ws"
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && matches!(url.host_str(), Some("127.0.0.1" | "::1" | "localhost"))
}

async fn connect_before_login<S, P, C, CFut, L>(
    endpoint: &str,
    allow_loopback_ws: bool,
    connector: C,
    serialize_login: L,
) -> Result<(S, P), PortFailure>
where
    C: FnOnce() -> CFut,
    CFut: Future<Output = Result<S, PortFailure>>,
    L: FnOnce() -> Result<P, PortFailure>,
{
    if !endpoint_allowed(endpoint, allow_loopback_ws) {
        return Err(PortFailure::AuthenticationUnavailable);
    }
    let socket = connector().await?;
    let login_payload = serialize_login()?;
    Ok((socket, login_payload))
}

#[cfg(test)]
mod endpoint_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn untrusted_remote_wss_is_rejected_before_connect_or_login_serialization() {
        let recording_connector_calls = Arc::new(AtomicUsize::new(0));
        let login_serialization_calls = Arc::new(AtomicUsize::new(0));
        let connector_count = Arc::clone(&recording_connector_calls);
        let serializer_count = Arc::clone(&login_serialization_calls);

        let result = connect_before_login(
            "wss://untrusted.example.invalid:443/provider/path?synthetic=1",
            false,
            || async move {
                connector_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, PortFailure>(())
            },
            || {
                serializer_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, PortFailure>(Vec::<u8>::new())
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(PortFailure::AuthenticationUnavailable)
        ));
        assert_eq!(recording_connector_calls.load(Ordering::SeqCst), 0);
        assert_eq!(login_serialization_calls.load(Ordering::SeqCst), 0);
    }
}
