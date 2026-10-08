//! Narrow, redacted credential handoff for one Streamer LOGIN.
//! 定义包含秘密的 Streamer 登录输入和已校验会话元数据。

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;

use url::Url;
use zeroize::Zeroizing;

/// Maximum accepted access-token bytes retained for a Streamer LOGIN.
/// 中文摘要：Streamer LOGIN bearer token 接受的最大 UTF-8 字节数。
pub const MAX_STREAMER_TOKEN_BYTES: usize = 8 * 1024;
/// Maximum UTF-8 bytes accepted for a dynamic Streamer WebSocket URL.
/// 中文摘要：动态 Streamer WebSocket URL 接受的最大 UTF-8 字节数。
pub const MAX_STREAMER_SOCKET_URL_BYTES: usize = 4 * 1024;
/// Maximum UTF-8 bytes accepted for each Streamer metadata string.
/// 中文摘要：每个 Streamer LOGIN 元数据字符串的最大 UTF-8 字节数。
pub const MAX_STREAMER_METADATA_BYTES: usize = 512;

/// Secret token material that cannot be formatted, cloned, or serialized.
///
/// Production providers should construct this from an authorized token lease.
/// The bearer value is only exposed inside this crate while serializing the
/// LOGIN frame and is zeroized when this value is dropped.
/// 中文摘要：用于一次 LOGIN 的 bearer token；不支持克隆或序列化，Debug 脱敏且 drop 时清零。
pub struct StreamerLoginSecret(Zeroizing<String>);

impl StreamerLoginSecret {
    /// Takes ownership of a bearer value, validates its bounded RFC 6750 token
    /// alphabet, and stores it in a zeroizing buffer.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    pub fn new(value: impl Into<String>) -> Result<Self, CredentialInputError> {
        Self::from_zeroizing(Zeroizing::new(value.into()))
    }

    /// Takes ownership of an already-zeroizing bearer value without making an
    /// additional application-level copy.
    /// 中文摘要：校验已启用 zeroize 的 bearer 缓冲区并接管所有权，不额外创建应用层副本。
    pub fn from_zeroizing(value: Zeroizing<String>) -> Result<Self, CredentialInputError> {
        if !is_valid_bearer(value.as_str()) {
            return Err(CredentialInputError::InvalidBearer);
        }
        Ok(Self(value))
    }

    pub(crate) fn expose_to_login_serializer(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for StreamerLoginSecret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("StreamerLoginSecret([REDACTED])")
    }
}

/// Validated dynamic Streamer URL and login metadata supplied by the runtime
/// credential/bootstrap layer.
///
/// This type intentionally has no `Debug`, `Clone`, or serialization support:
/// customer identifiers and endpoint query data must not enter diagnostics or
/// fixture snapshots. The current credential implementation has no public
/// access-token lease, so production construction remains an explicit upstream
/// integration boundary.
/// 中文摘要：已校验的动态 WebSocket URL、LOGIN 元数据与 token；不实现 Debug/Clone/序列化以避免凭证泄漏。
pub struct StreamerSessionCredentials {
    pub(crate) socket_url: Zeroizing<String>,
    pub(crate) customer_id: Zeroizing<String>,
    pub(crate) correlation_id: Zeroizing<String>,
    pub(crate) channel: Zeroizing<String>,
    pub(crate) function_id: Zeroizing<String>,
    pub(crate) access_token: StreamerLoginSecret,
}

impl StreamerSessionCredentials {
    /// Validates an HTTPS-protected dynamic Streamer endpoint and the five
    /// fields proven by the current Node `StreamerInfoSchema`/LOGIN request.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    pub fn new(
        socket_url: impl Into<String>,
        customer_id: impl Into<String>,
        correlation_id: impl Into<String>,
        channel: impl Into<String>,
        function_id: impl Into<String>,
        access_token: StreamerLoginSecret,
    ) -> Result<Self, CredentialInputError> {
        Self::new_with_policy(
            socket_url.into(),
            customer_id.into(),
            correlation_id.into(),
            channel.into(),
            function_id.into(),
            access_token,
            false,
        )
    }

    fn new_with_policy(
        socket_url: String,
        customer_id: String,
        correlation_id: String,
        channel: String,
        function_id: String,
        access_token: StreamerLoginSecret,
        allow_loopback_ws: bool,
    ) -> Result<Self, CredentialInputError> {
        let socket_url = Zeroizing::new(socket_url);
        let customer_id = Zeroizing::new(customer_id);
        let correlation_id = Zeroizing::new(correlation_id);
        let channel = Zeroizing::new(channel);
        let function_id = Zeroizing::new(function_id);
        validate_socket_url(&socket_url, allow_loopback_ws)?;
        for value in [&customer_id, &correlation_id, &channel, &function_id] {
            validate_metadata(value)?;
        }

        Ok(Self {
            socket_url,
            customer_id,
            correlation_id,
            channel,
            function_id,
            access_token,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_loopback_test(
        socket_url: String,
        customer_id: String,
        correlation_id: String,
        channel: String,
        function_id: String,
        access_token: StreamerLoginSecret,
    ) -> Result<Self, CredentialInputError> {
        Self::new_with_policy(
            socket_url,
            customer_id,
            correlation_id,
            channel,
            function_id,
            access_token,
            true,
        )
    }
}

/// Async source for fresh authorization material and dynamic StreamerInfo.
///
/// A production implementation must obtain the token from the credential
/// lease and metadata from the authorized read-only user-preferences flow. It
/// must not retain detached tasks, log values, or return unbounded strings.
/// The `schwab-secrets` v2 API does not currently expose a safe access-token
/// lease, so the production provider remains blocked pending #171's contract.
///
/// The returned future is cancelled when the runtime's connect timeout expires.
/// Providers must not detach lookup/refresh tasks; they must directly own
/// intermediate credentials and zeroizing buffers so drop clears them. Any
/// child task must retain no authorization material and its handle must be
/// retained so drop can request abort. Tokio abort is not synchronous.
/// 中文摘要：定义 Streamer凭证提供器 的注入边界；具体实现仍须遵守类型说明中的安全约束。
pub trait StreamerCredentialProvider: Send + 'static {
    /// Loads one bounded session context. Reconnect calls this again so the
    /// provider can refresh access material while retaining its reviewed
    /// StreamerInfo caching policy.
    /// 中文摘要：为一次连接尝试加载新的有界凭证；重连会再次调用提供器，连接期限到达时取消该 future。
    fn load_session_credentials(
        &mut self,
    ) -> impl Future<Output = Result<StreamerSessionCredentials, CredentialProviderFailure>> + Send;
}

/// Fixed credential-source failure; upstream messages and secret values are
/// intentionally discarded.
/// 中文摘要：凭证源只返回固定失败类别，不携带 token、账户标识或上游错误文本。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialProviderFailure {
    /// No valid credential/session metadata is currently available.
    /// 凭证提供器当前不可用。
    Unavailable,
}

impl Display for CredentialProviderFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("STREAMER_CREDENTIALS_UNAVAILABLE")
    }
}

impl Error for CredentialProviderFailure {}

/// Validation errors contain only stable categories, never the offending
/// endpoint, identifier, or bearer value.
/// 中文摘要：凭证或 LOGIN 元数据未通过长度、字符集或安全 endpoint 校验时返回的固定错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialInputError {
    /// The access token is empty, oversized, or outside the bearer alphabet.
    /// 登录 bearer token 为空、格式错误或过长。
    InvalidBearer,
    /// The Streamer URL is invalid or violates secure-endpoint policy.
    /// socket URL 不符合必需的 wss URL 语法。
    InvalidSocketUrl,
    /// A required metadata value is empty, oversized, or contains controls.
    /// 必需的 Streamer 元数据缺失或超出大小上限。
    InvalidMetadata,
}

impl Display for CredentialInputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidBearer => "invalid Streamer bearer value",
            Self::InvalidSocketUrl => "invalid or insecure Streamer endpoint",
            Self::InvalidMetadata => "invalid Streamer login metadata",
        })
    }
}

impl Error for CredentialInputError {}

fn is_valid_bearer(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_STREAMER_TOKEN_BYTES {
        return false;
    }
    let bytes = value.as_bytes();
    let first_padding = bytes.iter().position(|byte| *byte == b'=');
    let token_part = first_padding.map_or(bytes, |index| &bytes[..index]);
    let padding_part = first_padding.map_or(&[][..], |index| &bytes[index..]);
    !token_part.is_empty()
        && token_part.iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
        && padding_part.iter().all(|byte| *byte == b'=')
}

fn validate_metadata(value: &str) -> Result<(), CredentialInputError> {
    if value.is_empty()
        || value.len() > MAX_STREAMER_METADATA_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(CredentialInputError::InvalidMetadata);
    }
    Ok(())
}

fn validate_socket_url(value: &str, allow_loopback_ws: bool) -> Result<(), CredentialInputError> {
    if value.is_empty() || value.len() > MAX_STREAMER_SOCKET_URL_BYTES {
        return Err(CredentialInputError::InvalidSocketUrl);
    }
    let url = Url::parse(value).map_err(|_| CredentialInputError::InvalidSocketUrl)?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(CredentialInputError::InvalidSocketUrl);
    }
    if url.scheme() == "wss" {
        return Ok(());
    }
    let is_loopback = matches!(url.host_str(), Some("127.0.0.1" | "::1" | "localhost"));
    if allow_loopback_ws && url.scheme() == "ws" && is_loopback {
        return Ok(());
    }
    Err(CredentialInputError::InvalidSocketUrl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_and_session_debug_never_reveal_synthetic_material() {
        let secret_marker = "synthetic-secret-marker";
        let customer_marker = "synthetic-customer-marker";
        let secret = StreamerLoginSecret::new(secret_marker.to_owned())
            .expect("synthetic bearer value is valid");
        assert!(!format!("{secret:?}").contains(secret_marker));

        let credentials = StreamerSessionCredentials::new(
            "wss://streamer.example.invalid/private-query",
            customer_marker,
            "synthetic-correlation",
            "N9",
            "synthetic-function",
            secret,
        )
        .expect("synthetic session context is valid");
        let error_text = CredentialProviderFailure::Unavailable.to_string();
        assert!(!error_text.contains(secret_marker));
        assert!(!error_text.contains(customer_marker));
        assert_eq!(error_text, "STREAMER_CREDENTIALS_UNAVAILABLE");
        drop(credentials);
    }

    #[test]
    fn rejects_plaintext_remote_urls_and_invalid_bearer_material() {
        let secret = StreamerLoginSecret::new("synthetic-access-token".to_owned())
            .expect("test bearer is valid");
        assert!(matches!(
            StreamerSessionCredentials::new(
                "ws://streamer.example.invalid",
                "customer",
                "correlation",
                "channel",
                "function",
                secret,
            ),
            Err(CredentialInputError::InvalidSocketUrl)
        ));
        assert!(matches!(
            StreamerLoginSecret::new("bad token".to_owned()),
            Err(CredentialInputError::InvalidBearer)
        ));

        let oversized_endpoint = format!("wss://example.invalid/{}", "x".repeat(4096));
        assert!(matches!(
            StreamerSessionCredentials::new(
                oversized_endpoint,
                "synthetic-customer",
                "synthetic-correlation",
                "N9",
                "synthetic-function",
                StreamerLoginSecret::new("synthetic-access-token".to_owned())
                    .expect("synthetic bearer is valid"),
            ),
            Err(CredentialInputError::InvalidSocketUrl)
        ));
    }
}
