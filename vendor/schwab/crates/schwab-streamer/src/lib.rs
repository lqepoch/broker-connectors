#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Read-only Schwab Streamer protocol decoding and subscription-state types.
//!
//! The public library surface exposes bounded JSON frame decoding, opaque wire
//! values, service manifests, and subscription-state types. Numeric field IDs
//! are not mapped to market meanings. Authenticated session, credential, socket,
//! and native LOGIN code is compiled only into this crate's own unit-test build;
//! downstream applications cannot construct or invoke that runtime through the
//! public library API. Official endpoint and field evidence remains incomplete.
//!
//! 本 crate 的公开 API 仅提供有界 JSON 解码、未解释 wire value、service manifest 与订阅状态类型。数字字段
//! ID 不映射为行情语义。认证 session、凭据、socket 和原生 LOGIN 代码仅编译进本 crate 的单元测试；下游应用
//! 无法通过公开 library API 构造或调用该 runtime。官方 endpoint 与字段证据仍不完整。

/// Maximum accepted UTF-8 byte length for a Streamer access-token field.
/// 中文摘要：Streamer access-token 字段接受的最大 UTF-8 字节数。
pub const MAX_STREAMER_TOKEN_BYTES: usize = 8 * 1024;
/// Maximum accepted UTF-8 byte length for a Streamer WebSocket URL field.
/// 中文摘要：Streamer WebSocket URL 字段接受的最大 UTF-8 字节数。
pub const MAX_STREAMER_SOCKET_URL_BYTES: usize = 4 * 1024;
/// Maximum accepted UTF-8 byte length for each Streamer metadata field.
/// 中文摘要：每个 Streamer metadata 字段接受的最大 UTF-8 字节数。
pub const MAX_STREAMER_METADATA_BYTES: usize = 512;

mod command;
#[cfg(test)]
mod credentials;
#[cfg(test)]
mod factory;
mod manifest;
#[cfg(test)]
mod protocol;
#[cfg(test)]
mod session;
#[cfg(test)]
mod socket;
mod state;
mod wire;

pub use command::{
    AckDisposition, AckIgnoreReason, CommandAcknowledgement, ConnectionGeneration, ReplayPlan,
    RequestId, ServiceReadiness, StreamerCommand, SubscriptionCommand,
};
#[cfg(test)]
pub(crate) use credentials::{
    CredentialProviderFailure, StreamerCredentialProvider, StreamerLoginSecret,
    StreamerSessionCredentials,
};
#[cfg(test)]
pub(crate) use factory::SchwabStreamerSessionFactory;
pub use manifest::{SERVICE_COUNT, SERVICE_MANIFESTS, ServiceManifest, StreamerService};
#[cfg(test)]
pub(crate) use session::{
    AuthenticatedSessionFactory, CriticalEventReceiver, MAX_MERGED_MARKET_FIELDS, PortFailure,
    ServiceStatusCause, SessionConfig, SessionControlError, SessionEvent, SessionRunError,
    StreamerControl, StreamerRuntime, StreamerSocket,
};
pub use state::{
    MAX_KEY_BYTES, MAX_KEY_INPUT_ITEMS, MAX_KEYS_PER_SERVICE, MAX_SERIALIZED_KEY_BYTES,
    ServiceStateError, ServiceSubscriptionManager,
};
pub use wire::{
    MAX_WIRE_ARRAY_ITEMS, MAX_WIRE_FRAME_BYTES, StreamerCommandResponse, StreamerDataPayload,
    StreamerDataRow, StreamerNotifyPayload, StreamerResponseContent, StreamerWireError,
    StreamerWireFrame, is_successful_streamer_command, parse_streamer_frame,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod wire_tests;

#[cfg(test)]
mod session_tests;

#[cfg(test)]
mod adapter_tests;
