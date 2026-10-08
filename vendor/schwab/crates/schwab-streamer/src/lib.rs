#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Bounded Streamer session orchestration, login, and read-only wire parsing.
//!
//! This crate implements one async owner task, service-level
//! desired/acknowledged state, bounded event delivery, a bounded JSON frame
//! codec, and a TLS WebSocket adapter for the Node-characterized LOGIN and
//! subscription envelopes. OAuth, REST `StreamerInfo` lookup, and the safe token
//! lease bridge remain outside this crate and are not yet wired for production.
//!
//! Coalesced market-data rows keep last-changed source and monotonic receive
//! provenance per wire field. The row-level timestamp fields remain the newest
//! accepted delta for compatibility; consumers that need field freshness must
//! use [`MarketDataUpdate::field_provenance`]. For each service/key in one
//! generation, an exact same-timestamp sparse delta is ignored without
//! advancing its revision or receive instant, even after delivery; changed
//! sparse content at that timestamp remains admissible.
//! This follows the Node snapshot contract: duplicate detection compares the
//! last incoming row fingerprint, not the merged row retained by the cache.
//! SHA-256 ordering fences persist after a consumer pops a row and are cleared
//! only at generation reset or runtime shutdown. The fence cap is shared by
//! equities and options: each generation retains at most
//! [`MAX_MARKET_DATA_FENCE_KEYS`] service/key fences, each key up to
//! [`MAX_MARKET_DATA_FENCE_KEY_BYTES`] bytes, under the estimated
//! [`MAX_MARKET_DATA_FENCE_BYTES`] byte bound. Reaching a fence or row capacity
//! is fatal to the runtime: `run()` returns `MarketDataCapacityExceeded`, both
//! receivers close, and queued market-data rows and ordering fences are
//! discarded before the failed runtime returns. No pending quote is delivered
//! after that failure. If Tokio aborts the owner task, the receiver's next
//! `recv()` clears any pending quote rows and fences before returning `None`.
//! 提供按连接代次隔离的只读 Streamer 协议、有界事件缓冲和单 socket runtime；凭证与 endpoint 由注入边界负责。

mod command;
mod credentials;
mod factory;
mod manifest;
mod protocol;
mod session;
mod socket;
mod state;
mod wire;

pub use command::{
    AckDisposition, AckIgnoreReason, CommandAcknowledgement, ConnectionGeneration, ReplayPlan,
    RequestId, ServiceReadiness, StreamerCommand, SubscriptionCommand,
};
pub use credentials::{
    CredentialInputError, CredentialProviderFailure, MAX_STREAMER_METADATA_BYTES,
    MAX_STREAMER_SOCKET_URL_BYTES, MAX_STREAMER_TOKEN_BYTES, StreamerCredentialProvider,
    StreamerLoginSecret, StreamerSessionCredentials,
};
pub use factory::SchwabStreamerSessionFactory;
pub use manifest::{SERVICE_COUNT, SERVICE_MANIFESTS, ServiceManifest, StreamerService};
pub use session::{
    AuthenticatedSessionFactory, CriticalEventReceiver, MAX_CONTROL_CAPACITY, MAX_CRITICAL_BYTES,
    MAX_CRITICAL_CAPACITY, MAX_MARKET_DATA_FENCE_BYTES, MAX_MARKET_DATA_FENCE_KEY_BYTES,
    MAX_MARKET_DATA_FENCE_KEYS, MAX_MARKET_DATA_KEYS, MAX_MERGED_MARKET_FIELDS,
    MAX_MERGED_MARKET_PROVENANCE_BYTES, MAX_MERGED_MARKET_ROW_BYTES, MarketDataFieldProvenance,
    MarketDataReceiver, MarketDataUpdate, PortFailure, ServiceStatusCause, SessionConfig,
    SessionConfigError, SessionControlError, SessionEvent, SessionRunError, SocketEvent,
    StreamerControl, StreamerRuntime, StreamerSessionChannels, StreamerSocket,
};
pub use socket::SchwabStreamerSocket;
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
