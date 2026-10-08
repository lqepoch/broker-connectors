//! Validation-only boundary for Schwab Streamer bootstrap material.
//!
//! The gate does not fetch OAuth tokens or user preferences. An application
//! may inject a trusted bootstrap source backed by its existing short-lived
//! token-lease and shared read-budget owners, plus an exact full-URL WSS
//! allowlist. Validation immediately drops the candidate lease: this module
//! cannot construct a socket, serialize LOGIN, or hand credentials to a
//! transport. The public SDK runtime remains unavailable until official
//! endpoint and field evidence is complete.
//!
//! # 简体中文
//!
//! 本 gate 仅校验启动材料，不获取 OAuth token 或用户偏好，也不连接 socket 或发送 LOGIN。应用可注入既有
//! 短时 token lease 与共享读取预算 owner 支撑的可信来源，并提供完整 URL 精确 WSS 白名单。校验后立即销毁
//! 候选 lease；当前没有公开的认证运行时、生产 bootstrap 或 provider allowlist。

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use broker_ports::PortFuture;
use domain::{AccountNamespace, ExecutionBrokerId};
use zeroize::Zeroizing;

/// Exact full WSS URL configured by a trusted application composition root.
/// 由受信应用 composition root 配置的完整精确 WSS URL。
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct TrustedWssEndpoint {
    url: String,
}

impl TrustedWssEndpoint {
    /// Validates and stores one full WSS URL for exact string matching.
    /// 校验并保存一个完整 WSS URL，后续只进行完整字符串精确匹配。
    ///
    /// # Errors
    /// Returns [`SchwabStreamerGateError::InvalidAllowlist`] when the URL is
    /// malformed, insecure, contains user information or a fragment, or is
    /// outside the source SDK's fixed URL size bound.
    pub fn new(url: impl Into<String>) -> Result<Self, SchwabStreamerGateError> {
        let url = url.into();
        if !valid_wss_url(&url) {
            return Err(SchwabStreamerGateError::InvalidAllowlist);
        }
        Ok(Self { url })
    }

    fn matches_url(&self, value: &str) -> bool {
        valid_wss_url(value) && self.url == value
    }
}

impl fmt::Debug for TrustedWssEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedWssEndpoint")
            .field("url", &"[REDACTED]")
            .finish()
    }
}

/// Bounded, zeroizing Streamer bootstrap data and access-token lease.
/// 有界且 drop 清零的 Streamer 启动数据和 access-token lease。
pub struct SchwabStreamerBootstrapLease {
    socket_url: Zeroizing<String>,
    customer_id: Zeroizing<String>,
    correlation_id: Zeroizing<String>,
    channel: Zeroizing<String>,
    function_id: Zeroizing<String>,
    access_token: Zeroizing<String>,
    expires_at: SystemTime,
}

impl SchwabStreamerBootstrapLease {
    /// Takes ownership of a fresh metadata snapshot and short-lived token lease.
    /// 接管新鲜元数据快照与短时 token lease。
    ///
    /// # Errors
    /// Returns [`SchwabStreamerGateError::InvalidBootstrapMaterial`] when a
    /// field is empty, malformed, or exceeds the source SDK's fixed size bound.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket_url: impl Into<String>,
        customer_id: impl Into<String>,
        correlation_id: impl Into<String>,
        channel: impl Into<String>,
        function_id: impl Into<String>,
        access_token: Zeroizing<String>,
        expires_at: SystemTime,
    ) -> Result<Self, SchwabStreamerGateError> {
        let lease = Self {
            socket_url: Zeroizing::new(socket_url.into()),
            customer_id: Zeroizing::new(customer_id.into()),
            correlation_id: Zeroizing::new(correlation_id.into()),
            channel: Zeroizing::new(channel.into()),
            function_id: Zeroizing::new(function_id.into()),
            access_token,
            expires_at,
        };
        if lease.socket_url.is_empty()
            || lease.socket_url.len() > schwab_streamer::MAX_STREAMER_SOCKET_URL_BYTES
            || lease.socket_url.chars().any(char::is_control)
            || [
                &lease.customer_id,
                &lease.correlation_id,
                &lease.channel,
                &lease.function_id,
            ]
            .into_iter()
            .any(|value| {
                value.is_empty()
                    || value.len() > schwab_streamer::MAX_STREAMER_METADATA_BYTES
                    || value.chars().any(char::is_control)
            })
            || lease.access_token.is_empty()
            || lease.access_token.len() > schwab_streamer::MAX_STREAMER_TOKEN_BYTES
            || lease.access_token.chars().any(char::is_control)
        {
            return Err(SchwabStreamerGateError::InvalidBootstrapMaterial);
        }
        Ok(lease)
    }
}

impl fmt::Debug for SchwabStreamerBootstrapLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchwabStreamerBootstrapLease([REDACTED])")
    }
}

/// Application-supplied source for fresh, authorized Streamer bootstrap data.
///
/// Implementations must use the existing shared read-budget owner for the
/// read-only `userPreference` call, obtain the bearer from an authorized
/// short-lived token lease, and avoid detached refresh tasks. The returned
/// object remains an untrusted candidate until [`SchwabStreamerGate`] checks
/// its exact full URL and expiry; the gate then drops it without exposing a
/// credential-send interface.
///
/// 应用提供的新鲜、获准 Streamer 启动材料来源。实现必须在只读 `userPreference` 读取中复用现有共享预算
/// owner，从获准的短时 token lease 获取 bearer，不得分离刷新任务。返回值由 gate 检查完整 URL 与到期时间后
/// 直接销毁，不提供凭据发送接口。
pub trait SchwabStreamerBootstrapPort: Send + Sync + 'static {
    /// Loads fresh Streamer metadata and one bounded token lease.
    /// 加载新鲜 Streamer 元数据和一个有界 token lease。
    fn load_fresh<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
    ) -> PortFuture<'a, Result<SchwabStreamerBootstrapLease, SchwabStreamerBootstrapError>>;
}

/// Fixed source failure without provider text or credential material.
/// 不携带 provider 文本或凭据材料的固定来源错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabStreamerBootstrapError {
    /// No fresh authorized bootstrap data is available.
    /// 没有可用的新鲜授权启动数据。
    Unavailable,
}

/// Failure category for the required Streamer bootstrap gate.
/// 必需 Streamer 启动 gate 的失败分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabStreamerGateError {
    /// The namespace or explicit endpoint allowlist is invalid.
    /// 命名空间或 endpoint 白名单无效。
    InvalidAllowlist,
    /// The bootstrap source failed or returned malformed metadata.
    /// bootstrap 来源失败或返回格式错误的元数据。
    BootstrapUnavailable,
    /// The candidate URL is not an exact allowlisted WSS URL.
    /// 候选 URL 不是完整精确匹配的白名单 WSS URL。
    EndpointNotAllowed,
    /// The token lease has expired or is otherwise invalid.
    /// token lease 已过期或无效。
    LeaseExpired,
    /// A required metadata or bearer value is malformed or out of bounds.
    /// 必需元数据或 bearer 格式错误或超出上限。
    InvalidBootstrapMaterial,
}

/// Validation-only gate for a candidate bootstrap lease.
/// 仅用于校验候选 bootstrap lease 的 gate。
pub struct SchwabStreamerGate<B: SchwabStreamerBootstrapPort> {
    namespace: AccountNamespace,
    endpoints: BTreeSet<TrustedWssEndpoint>,
    bootstrap: Arc<B>,
}

impl<B: SchwabStreamerBootstrapPort> SchwabStreamerGate<B> {
    /// Requires a Schwab namespace, at least one exact full WSS URL, and an injected bootstrap source.
    /// 必须提供 Schwab 命名空间、至少一个完整精确 WSS URL 和注入的 bootstrap 来源。
    ///
    /// # Errors
    /// Returns [`SchwabStreamerGateError::InvalidAllowlist`] if the namespace
    /// is not Schwab-scoped or the endpoint allowlist is empty.
    pub fn new(
        namespace: AccountNamespace,
        endpoints: impl IntoIterator<Item = TrustedWssEndpoint>,
        bootstrap: Arc<B>,
    ) -> Result<Self, SchwabStreamerGateError> {
        if namespace.broker() != &ExecutionBrokerId::Schwab {
            return Err(SchwabStreamerGateError::InvalidAllowlist);
        }
        let endpoints: BTreeSet<_> = endpoints.into_iter().collect();
        if endpoints.is_empty() {
            return Err(SchwabStreamerGateError::InvalidAllowlist);
        }
        Ok(Self {
            namespace,
            endpoints,
            bootstrap,
        })
    }

    /// Fetches and validates one candidate lease, then drops it without
    /// creating a credential object that a socket runtime can consume.
    /// 获取并校验一个候选 lease，随后直接销毁，不创建可交给 socket runtime 的凭据对象。
    ///
    /// # Errors
    /// Returns [`SchwabStreamerGateError::BootstrapUnavailable`] when the
    /// injected source cannot provide a lease,
    /// [`SchwabStreamerGateError::EndpointNotAllowed`] when its
    /// full WSS URL is not allowlisted, or [`SchwabStreamerGateError::LeaseExpired`]
    /// for a stale lease.
    pub async fn validate_bootstrap(&self) -> Result<(), SchwabStreamerGateError> {
        let lease = self
            .bootstrap
            .load_fresh(&self.namespace)
            .await
            .map_err(|_| SchwabStreamerGateError::BootstrapUnavailable)?;
        if lease.expires_at <= SystemTime::now() {
            return Err(SchwabStreamerGateError::LeaseExpired);
        }
        if !self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.matches_url(lease.socket_url.as_str()))
        {
            return Err(SchwabStreamerGateError::EndpointNotAllowed);
        }
        drop(lease);
        Ok(())
    }
}

impl<B: SchwabStreamerBootstrapPort> fmt::Debug for SchwabStreamerGate<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerGate")
            .field("namespace", &"[REDACTED]")
            .field("endpoint_count", &self.endpoints.len())
            .field("bootstrap", &"[REDACTED]")
            .finish()
    }
}

fn valid_wss_url(value: &str) -> bool {
    if value.is_empty()
        || value.len() > schwab_streamer::MAX_STREAMER_SOCKET_URL_BYTES
        || value.chars().any(char::is_control)
        || has_invalid_authority(value)
    {
        return false;
    }
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    url.scheme() == "wss"
        && url.host_str().is_some()
        && url.port_or_known_default() != Some(0)
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

fn has_invalid_authority(value: &str) -> bool {
    value
        .split_once("://")
        .is_none_or(|(_, authority_and_path)| {
            authority_and_path
                .split(['/', '?', '#'])
                .next()
                .is_none_or(|authority| authority.is_empty() || authority.contains('@'))
        })
}

#[cfg(test)]
mod tests;
