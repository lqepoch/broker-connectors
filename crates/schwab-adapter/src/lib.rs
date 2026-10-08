#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! Read-only Schwab adapters over the extracted SDK and shared broker ports.
//!
//! Account and position reads reuse the source SDK's fixed GET routes, bounded
//! parser, token boundary, and HTTP transport. Every request must obtain one
//! permit from the caller-injected shared admission owner. This crate does not
//! own OAuth, account state, persistence, execution, or market-data authority.
//! Schwab Streamer field IDs remain uninterpreted until an authoritative public
//! field dictionary is available.
//!
//! # 简体中文
//!
//! 本 crate 复用提取的 Schwab SDK 与共享券商端口实现只读账户和持仓读取。每个 REST 请求都必须从调用方
//! 注入的唯一共享准入 owner 获取许可。本 crate 不拥有 OAuth、账户状态、持久化、执行或行情 authority。
//! 在取得官方字段字典前，Streamer 数字字段始终保持未解释状态。

mod account_read;
mod admission;
mod streamer_gate;

pub use account_read::{
    SchwabAccountBinding, SchwabAccountBindingError, SchwabAccountSummary, SchwabBalanceSnapshot,
    SchwabPosition, SchwabPositionInstrument, SchwabReadAdapter,
};
pub use admission::{SchwabReadAdmissionOwner, SchwabReadOperation};
pub use streamer_gate::{
    SchwabStreamerBootstrapError, SchwabStreamerBootstrapLease, SchwabStreamerBootstrapPort,
    SchwabStreamerGate, SchwabStreamerGateError, TrustedWssEndpoint,
};
