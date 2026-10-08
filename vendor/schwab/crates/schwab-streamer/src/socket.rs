//! Bounded Tokio WebSocket transport for one authenticated session.
//! 提供 Streamer socket 端口的 TLS WebSocket 适配器。

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};
use zeroize::Zeroizing;

use crate::command::StreamerCommand;
use crate::protocol::{LoginPayload, serialize_subscription};
use crate::session::{PortFailure, SocketEvent, StreamerSocket};
use crate::wire::MAX_WIRE_FRAME_BYTES;

type TokioWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Maximum outgoing WebSocket buffer. It allows one maximum-size command plus
/// framing slack while preventing unbounded retention after write failures.
const MAX_SOCKET_WRITE_BUFFER_BYTES: usize = MAX_WIRE_FRAME_BYTES * 2;
/// Bound the automatic WebSocket PONG write while the runtime awaits input.
const SOCKET_CONTROL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Concrete single-socket transport returned only after LOGIN succeeds.
/// 中文摘要：单个已完成 LOGIN 的 TLS WebSocket；接收帧大小受限，凭证元数据仅在 socket 生命周期内零化保存。
pub struct SchwabStreamerSocket {
    stream: TokioWebSocket,
    customer_id: Zeroizing<String>,
    correlation_id: Zeroizing<String>,
}

impl SchwabStreamerSocket {
    pub(crate) async fn connect(
        socket_url: &str,
        customer_id: Zeroizing<String>,
        correlation_id: Zeroizing<String>,
    ) -> Result<Self, PortFailure> {
        let config = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_SOCKET_WRITE_BUFFER_BYTES)
            .max_message_size(Some(MAX_WIRE_FRAME_BYTES))
            .max_frame_size(Some(MAX_WIRE_FRAME_BYTES));
        let (stream, _response) =
            connect_async_tls_with_config(socket_url, Some(config), false, None)
                .await
                .map_err(|_| PortFailure::ConnectFailed)?;
        Ok(Self {
            stream,
            customer_id,
            correlation_id,
        })
    }

    pub(crate) async fn send_login(&mut self, payload: &LoginPayload) -> Result<(), PortFailure> {
        self.send_text(payload.as_str()).await
    }

    async fn send_text(&mut self, payload: &str) -> Result<(), PortFailure> {
        self.stream
            .send(Message::Text(payload.into()))
            .await
            .map_err(|_| PortFailure::SendFailed)
    }

    async fn read_event(&mut self) -> Result<Option<SocketEvent>, PortFailure> {
        let Some(message) = self.stream.next().await else {
            return Ok(None);
        };
        match message.map_err(|_| PortFailure::ReceiveFailed)? {
            Message::Text(text) => Ok(Some(SocketEvent::Frame(text.as_bytes().to_vec()))),
            Message::Binary(bytes) => Ok(Some(SocketEvent::Frame(bytes.to_vec()))),
            Message::Ping(payload) => {
                timeout(
                    SOCKET_CONTROL_SEND_TIMEOUT,
                    self.stream.send(Message::Pong(payload)),
                )
                .await
                .map_err(|_| PortFailure::SendFailed)?
                .map_err(|_| PortFailure::SendFailed)?;
                Ok(Some(SocketEvent::Liveness))
            }
            Message::Pong(_) => Ok(Some(SocketEvent::Liveness)),
            Message::Close(_) => Ok(None),
            Message::Frame(_) => Err(PortFailure::ReceiveFailed),
        }
    }
}

impl StreamerSocket for SchwabStreamerSocket {
    async fn send_subscription(&mut self, command: &StreamerCommand) -> Result<(), PortFailure> {
        let payload = serialize_subscription(
            command,
            self.customer_id.as_str(),
            self.correlation_id.as_str(),
        )?;
        self.send_text(payload.as_str()).await
    }

    async fn receive_frame(&mut self) -> Result<Option<Vec<u8>>, PortFailure> {
        loop {
            match self.read_event().await? {
                Some(SocketEvent::Frame(bytes)) => return Ok(Some(bytes)),
                Some(SocketEvent::Liveness) => {}
                None => return Ok(None),
            }
        }
    }

    async fn receive_event(&mut self) -> Result<Option<SocketEvent>, PortFailure> {
        self.read_event().await
    }

    async fn send_ping(&mut self) -> Result<(), PortFailure> {
        self.stream
            .send(Message::Ping(Vec::new().into()))
            .await
            .map_err(|_| PortFailure::SendFailed)
    }
}
