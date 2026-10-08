//! Authenticated one-socket factory for the current Schwab Streamer wire flow.
//! 定义具体 Streamer 适配器的已认证会话工厂接线。

use std::time::Duration;

use tokio::time::timeout;

use crate::command::{ConnectionGeneration, RequestId};
use crate::credentials::{CredentialProviderFailure, StreamerCredentialProvider};
use crate::protocol::serialize_login;
use crate::session::{
    AuthenticatedSessionFactory, PortFailure, SessionConfigError, SocketEvent, StreamerSocket,
};
use crate::socket::SchwabStreamerSocket;
use crate::wire::{is_successful_streamer_command, parse_streamer_frame};

/// Concrete factory that owns exactly one authenticated WebSocket at a time.
///
/// The provider must return dynamic `StreamerInfo` and an opaque zeroizing
/// `StreamerLoginSecret`; this adapter never reads environment files, calls
/// OAuth, or obtains account metadata itself. The current #171 credential
/// layer does not yet expose a safe access-token lease, so production provider
/// construction remains blocked and must fail closed until that contract is
/// available.
/// 中文摘要：从注入的 credential provider 读取新会话材料，校验 endpoint、发送 LOGIN 并等待匹配的 ACK；超时或拒绝均失败关闭。
pub struct SchwabStreamerSessionFactory<P> {
    credential_provider: P,
    login_ack_timeout: Duration,
    #[cfg(test)]
    allow_loopback_ws: bool,
}

impl<P> SchwabStreamerSessionFactory<P> {
    /// Creates a production factory that accepts only `wss://` endpoints.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    pub fn new(credential_provider: P) -> Self {
        Self {
            credential_provider,
            login_ack_timeout: Duration::from_secs(15),
            #[cfg(test)]
            allow_loopback_ws: false,
        }
    }

    /// Bounds the provider's successful TCP/WebSocket connection's LOGIN ACK
    /// wait. `StreamerRuntime` additionally bounds the full connect/auth flow.
    /// 中文摘要：设置等待匹配 LOGIN ACK 的最长时间；零值会被拒绝，超时或拒绝时不会返回 socket。
    ///
    /// # Errors
    /// Returns [`SessionConfigError::InvalidDuration`] when the timeout is zero.
    pub fn with_login_ack_timeout(
        mut self,
        login_ack_timeout: Duration,
    ) -> Result<Self, SessionConfigError> {
        if login_ack_timeout.is_zero() {
            return Err(SessionConfigError::InvalidDuration);
        }
        self.login_ack_timeout = login_ack_timeout;
        Ok(self)
    }

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
        if !endpoint_allowed(&credentials.socket_url, loopback_test) {
            return Err(PortFailure::AuthenticationUnavailable);
        }

        let mut socket = SchwabStreamerSocket::connect(
            credentials.socket_url.as_str(),
            credentials.customer_id.clone(),
            credentials.correlation_id.clone(),
        )
        .await?;
        let login_payload = serialize_login(&credentials, login_request_id)?;
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
    if url.scheme() == "wss" {
        return true;
    }
    allow_loopback_ws
        && url.scheme() == "ws"
        && matches!(url.host_str(), Some("127.0.0.1" | "::1" | "localhost"))
}
