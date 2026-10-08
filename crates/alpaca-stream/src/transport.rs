//! TLS WebSocket adapter with binary-only `MessagePack` application frames and transport limits.
//!
//! 使用 TLS WebSocket、binary-only `MessagePack` 应用帧及传输层大小限制的适配器。

use std::future::Future;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Bytes, Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use zeroize::Zeroizing;

use crate::protocol::MAX_FRAME_BYTES;

const WRITE_BUFFER_BYTES: usize = 16 * 1024;
const MAX_WRITE_BUFFER_BYTES: usize = 2 * MAX_FRAME_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectFailure {
    AuthenticationRejected,
    EndpointRejected,
    Retryable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SocketFailure {
    Transport,
}

pub(crate) enum SocketFrame {
    Binary(Vec<u8>),
    Text,
    /// Local test-control terminal marker; it is never produced by a provider WebSocket.
    #[cfg(feature = "offline-test-support")]
    FixtureEnd,
}

pub(crate) trait StreamSocket: Send + 'static {
    fn send_binary(
        &mut self,
        payload: Zeroizing<Vec<u8>>,
    ) -> impl Future<Output = Result<(), SocketFailure>> + Send;

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<SocketFrame>, SocketFailure>> + Send;

    fn close(&mut self) -> impl Future<Output = ()> + Send;
}

pub(crate) trait SocketConnector: Send + 'static {
    type Socket: StreamSocket;

    fn connect(
        &mut self,
        endpoint: &'static str,
    ) -> impl Future<Output = Result<Self::Socket, ConnectFailure>> + Send;
}

pub(crate) struct TokioConnector;

impl SocketConnector for TokioConnector {
    type Socket = TokioSocket;

    async fn connect(&mut self, endpoint: &'static str) -> Result<Self::Socket, ConnectFailure> {
        let mut request = endpoint
            .into_client_request()
            .map_err(|_| ConnectFailure::EndpointRejected)?;
        request.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_static("application/msgpack"),
        );
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_FRAME_BYTES);
        config.max_frame_size = Some(MAX_FRAME_BYTES);
        config.write_buffer_size = WRITE_BUFFER_BYTES;
        config.max_write_buffer_size = MAX_WRITE_BUFFER_BYTES;
        let (socket, response) = connect_async_with_config(request, Some(config), false)
            .await
            .map_err(map_connect_error)?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(ConnectFailure::EndpointRejected);
        }
        Ok(TokioSocket { socket })
    }
}

pub(crate) struct TokioSocket {
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl StreamSocket for TokioSocket {
    async fn send_binary(&mut self, payload: Zeroizing<Vec<u8>>) -> Result<(), SocketFailure> {
        self.socket
            // Keep the zeroizing owner attached to the WebSocket payload until Tungstenite drops it.
            // 让 WebSocket payload 持有会清零的 owner，直至 Tungstenite 释放该 payload。
            .send(Message::Binary(Bytes::from_owner(payload)))
            .await
            .map_err(|_| SocketFailure::Transport)
    }

    async fn receive(&mut self) -> Result<Option<SocketFrame>, SocketFailure> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Binary(payload))) => {
                    return Ok(Some(SocketFrame::Binary(payload.to_vec())));
                }
                Some(Ok(Message::Text(_))) => return Ok(Some(SocketFrame::Text)),
                Some(Ok(Message::Ping(payload))) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| SocketFailure::Transport)?;
                }
                Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Err(_)) => return Err(SocketFailure::Transport),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.socket.close(None).await;
    }
}

fn map_connect_error(error: Error) -> ConnectFailure {
    match error {
        Error::Http(response) => classify_http_status(response.status()),
        _ => ConnectFailure::Retryable,
    }
}

fn classify_http_status(status: StatusCode) -> ConnectFailure {
    if status == StatusCode::UNAUTHORIZED {
        ConnectFailure::AuthenticationRejected
    } else if status.is_client_error() {
        ConnectFailure::EndpointRejected
    } else {
        ConnectFailure::Retryable
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio_tungstenite::tungstenite::protocol::{Message, Role, WebSocket};

    use super::*;

    #[test]
    fn http_401_is_classified_as_terminal_authentication_rejection() {
        assert_eq!(
            classify_http_status(StatusCode::UNAUTHORIZED),
            ConnectFailure::AuthenticationRejected
        );
        assert_eq!(
            classify_http_status(StatusCode::FORBIDDEN),
            ConnectFailure::EndpointRejected
        );
        assert_eq!(
            classify_http_status(StatusCode::SERVICE_UNAVAILABLE),
            ConnectFailure::Retryable
        );
    }

    #[test]
    fn tungstenite_reassembles_fragmented_binary_frames_into_one_application_message() {
        let payload = [0x91, 0x81, 0xa1, b'T', 0xa1, b'x'];
        let split = 3;
        let mut wire = vec![
            0x02,
            u8::try_from(split).expect("synthetic fragment offset fits u8"),
        ];
        wire.extend_from_slice(&payload[..split]);
        wire.push(0x80);
        wire.push(u8::try_from(payload.len() - split).expect("synthetic fragment length fits u8"));
        wire.extend_from_slice(&payload[split..]);
        let mut websocket = WebSocket::from_raw_socket(Cursor::new(wire), Role::Client, None);

        let message = websocket
            .read()
            .expect("complete fragmented application message");
        assert_eq!(message, Message::Binary(payload.to_vec().into()));
        assert_eq!(
            crate::protocol::decode_frame(&payload),
            Ok(vec![crate::protocol::ProviderMessage::Unknown(
                "x".to_owned()
            )])
        );
    }
}
