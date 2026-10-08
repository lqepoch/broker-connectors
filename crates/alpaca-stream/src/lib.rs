#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Bounded Alpaca options WebSocket ingestion with `MessagePack` decoding and explicit session recovery.
//!
//! This crate owns one read-only options stream. It injects credentials, accepts only allowlisted
//! endpoint/feed pairs, requires authentication and full subscription acknowledgement, bounds
//! frames and delivery lanes, and starts a new generation after every lost session. Quote updates
//! may coalesce by option contract; trade updates fail the session when their bounded lane fills.
//! It does not call REST, persist frames, calculate volatility, run strategies, or submit orders.
//!
//! 有界 Alpaca 期权 WebSocket 行情接入、`MessagePack` 解码与显式会话恢复。
//!
//! 本 crate 只拥有一个只读期权流。凭证通过 trait 注入；endpoint/feed 采用 allowlist；只有
//! 认证成功、完整订阅回执和新鲜数据都到达后才进入 ready。Frame 与交付队列均有上限；报价
//! 可以按期权合约合并，逐笔成交队列满时会结束当前 session。这里不调用 REST、不保存原始
//! 帧、不计算波动率、不运行策略，也不提交订单。

mod config;
mod credentials;
mod diagnostics;
mod lane;
mod market_port;
mod model;
mod msgpack_timestamp;
mod protocol;
mod session;
mod state;
mod transport;
mod update;

pub use config::{
    DesiredSubscriptions, MAX_ACKNOWLEDGEMENT_TIMEOUT, MAX_CONNECT_TIMEOUT,
    MAX_DESIRED_SYMBOLS_PER_CHANNEL, MAX_OPTION_SYMBOL_BYTES, MAX_READY_AGE, MAX_READY_FUTURE_SKEW,
    OptionContractSymbol, OptionFeed, StreamConfig, StreamEnvironment, StreamLimits,
};
pub use credentials::{
    AlpacaCredentials, CredentialFailure, CredentialProvider, CredentialValidationError,
    MAX_CREDENTIAL_FIELD_BYTES,
};
pub use lane::{
    ControlEvent, ControlReceiver, QuoteDiscardReason, QuoteReceiver, QuoteUpdate, StreamHandle,
    TradeReceiver, TradeUpdate,
};
pub use market_port::{AlpacaOptionsMarketDataPort, MAX_ACTIVE_ALPACA_PORT_SESSIONS};
pub use model::{
    DataFreshness, InboundRawMarketFrame, IngestStamp, MarketNumber, OptionQuote, OptionTrade,
    ProviderTimestamp, SessionGeneration, SessionPhase, SessionStatusCause,
    SubscriptionAcknowledgement,
};
pub use protocol::{
    DecodeError, MAX_ARRAY_ITEMS, MAX_CONDITIONS, MAX_DECODE_DEPTH, MAX_DECODE_NODES,
    MAX_FRAME_BYTES, MAX_FRAME_MESSAGES, MAX_LABEL_BYTES, MAX_MAP_ENTRIES, MAX_STRING_BYTES,
    ProviderError, ProviderErrorKind, ProviderMessage, SuccessMessage, decode_frame,
};
pub use session::{AlpacaOptionsStream, SessionExit, StreamError};
pub use state::{MAX_RECONNECT_ATTEMPTS, ReconnectPolicy, reconnect_delay};
pub use update::StreamUpdate;
