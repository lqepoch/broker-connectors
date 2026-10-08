//! Fail-closed boundary for Schwab Streamer bootstrap material.
//!
//! This gate does not fetch OAuth tokens or user preferences. The application
//! must inject one trusted bootstrap source backed by its existing short-lived
//! token-lease and shared read-budget owners, and must pass an explicit exact
//! WSS host/port allowlist. No production bootstrap source is implemented here.
//!
//! # 简体中文
//!
//! 本模块为 Schwab Streamer 启动材料提供失败关闭边界，但不获取 OAuth token 或用户偏好。应用必须注入由
//! 既有短时 token lease 与共享读取预算 owner 支撑的可信来源，并显式传入精确 WSS host/port 白名单。
//! 当前没有生产 bootstrap 实现。

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::SystemTime;

use broker_ports::PortFuture;
use domain::{AccountNamespace, ExecutionBrokerId};
use schwab_streamer::{
    CredentialInputError, CredentialProviderFailure, StreamerCredentialProvider,
    StreamerLoginSecret, StreamerSessionCredentials,
};
use zeroize::Zeroizing;

/// Exact trusted WSS hostname and port configured by the application.
/// 由应用配置的精确可信 WSS 主机名和端口。
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct TrustedWssEndpoint {
    host: String,
    port: u16,
}

impl TrustedWssEndpoint {
    /// Validates one lowercase DNS hostname and nonzero explicit port.
    /// 校验一个小写 DNS 主机名和非零显式端口。
    ///
    /// # Errors
    /// Returns [`SchwabStreamerGateError::InvalidAllowlist`] when the hostname
    /// is not a valid lowercase DNS name or the port is zero.
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, SchwabStreamerGateError> {
        let host = host.into();
        if port == 0 || !valid_dns_host(&host) {
            return Err(SchwabStreamerGateError::InvalidAllowlist);
        }
        Ok(Self { host, port })
    }

    fn matches_url(&self, value: &str) -> bool {
        if has_userinfo_delimiter(value) {
            return false;
        }
        let Ok(url) = url::Url::parse(value) else {
            return false;
        };
        url.scheme() == "wss"
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url.host_str() == Some(self.host.as_str())
            && url.port_or_known_default() == Some(self.port)
    }
}

impl fmt::Debug for TrustedWssEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedWssEndpoint")
            .field("host", &"[REDACTED]")
            .field("port", &self.port)
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
/// object is an untrusted candidate until [`SchwabStreamerGate`] checks its
/// endpoint and expiry.
///
/// 应用提供的新鲜、获准 Streamer 启动材料来源。实现必须在只读 `userPreference` 读取中复用现有共享预算
/// owner，从获准的短时 token lease 获取 bearer，不得分离刷新任务。返回值在 gate 校验 endpoint 与到期时间前
/// 仍是未受信候选。
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
    /// 命名空间或显式 endpoint 白名单无效。
    InvalidAllowlist,
    /// The bootstrap source failed or returned malformed metadata.
    /// bootstrap 来源失败或返回格式错误的元数据。
    BootstrapUnavailable,
    /// The candidate URL is not an exact allowlisted WSS endpoint.
    /// 候选 URL 不是精确匹配的白名单 WSS endpoint。
    EndpointNotAllowed,
    /// The token lease has expired or is otherwise invalid.
    /// token lease 已过期或无效。
    LeaseExpired,
    /// A required metadata or bearer value is malformed or out of bounds.
    /// 必需元数据或 bearer 格式错误或超出上限。
    InvalidBootstrapMaterial,
}

/// Required host/port and lease gate for the vendored source Streamer runtime.
/// vendored 源 Streamer runtime 必须经过的 host/port 与 lease gate。
pub struct SchwabStreamerGate<B: SchwabStreamerBootstrapPort> {
    namespace: AccountNamespace,
    endpoints: BTreeSet<TrustedWssEndpoint>,
    bootstrap: Arc<B>,
}

impl<B: SchwabStreamerBootstrapPort> SchwabStreamerGate<B> {
    /// Requires a Schwab namespace, at least one exact WSS endpoint, and an injected bootstrap source.
    /// 必须提供 Schwab 命名空间、至少一个精确 WSS endpoint 和注入的 bootstrap 来源。
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

    async fn load_credentials(
        &self,
    ) -> Result<StreamerSessionCredentials, SchwabStreamerGateError> {
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
        let access_token = StreamerLoginSecret::from_zeroizing(lease.access_token)
            .map_err(map_credential_error)?;
        let socket_url = lease.socket_url.to_string();
        let customer_id = lease.customer_id.to_string();
        let correlation_id = lease.correlation_id.to_string();
        let channel = lease.channel.to_string();
        let function_id = lease.function_id.to_string();
        StreamerSessionCredentials::new(
            socket_url,
            customer_id,
            correlation_id,
            channel,
            function_id,
            access_token,
        )
        .map_err(map_credential_error)
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

impl<B: SchwabStreamerBootstrapPort> StreamerCredentialProvider for SchwabStreamerGate<B> {
    async fn load_session_credentials(
        &mut self,
    ) -> Result<StreamerSessionCredentials, CredentialProviderFailure> {
        self.load_credentials()
            .await
            .map_err(|_| CredentialProviderFailure::Unavailable)
    }
}

fn map_credential_error(_error: CredentialInputError) -> SchwabStreamerGateError {
    SchwabStreamerGateError::InvalidBootstrapMaterial
}

fn valid_dns_host(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 253
        || !value.is_ascii()
        || value != value.to_ascii_lowercase()
        || value.parse::<IpAddr>().is_ok()
    {
        return false;
    }
    let labels: Vec<_> = value.split('.').collect();
    if labels
        .iter()
        .all(|label| label.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    labels.into_iter().all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && label
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn has_userinfo_delimiter(value: &str) -> bool {
    value
        .split_once("://")
        .is_none_or(|(_, authority_and_path)| {
            authority_and_path
                .split(['/', '?', '#'])
                .next()
                .is_none_or(|authority| authority.contains('@'))
        })
}

#[cfg(test)]
mod tests;
