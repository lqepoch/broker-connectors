//! Injected Alpaca credentials with fixed diagnostics and redacted formatting.
//!
//! 通过注入边界提供 Alpaca 凭证，诊断固定分类且格式化脱敏。

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;

use zeroize::Zeroizing;

/// Maximum accepted UTF-8 byte length for either injected credential field.
/// 注入凭证字段接受的最大 UTF-8 字节长度。
pub const MAX_CREDENTIAL_FIELD_BYTES: usize = 1_024;

/// Alpaca API key and secret owned only for one connection attempt.
/// 仅为一次连接尝试持有的 Alpaca API key 和 secret。
pub struct AlpacaCredentials {
    key_id: Zeroizing<String>,
    secret: Zeroizing<String>,
}

impl AlpacaCredentials {
    /// Validates and stores credentials in zeroizing strings.
    /// 校验凭证并使用 drop 时清零的字符串保存。
    ///
    /// # Errors
    ///
    /// Returns [`CredentialValidationError::InvalidCredential`] when either field is empty,
    /// oversized, or contains a control character.
    pub fn new(
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, CredentialValidationError> {
        let key_id = Zeroizing::new(key_id.into());
        let secret = Zeroizing::new(secret.into());
        if !valid_field(&key_id) || !valid_field(&secret) {
            return Err(CredentialValidationError::InvalidCredential);
        }
        Ok(Self { key_id, secret })
    }

    pub(crate) fn key_id(&self) -> &str {
        self.key_id.as_str()
    }

    pub(crate) fn secret(&self) -> &str {
        self.secret.as_str()
    }
}

impl Debug for AlpacaCredentials {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("AlpacaCredentials([REDACTED])")
    }
}

fn valid_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CREDENTIAL_FIELD_BYTES
        && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

/// Injected credential source used for each new WebSocket session.
/// 每个新 WebSocket 会话使用的凭证注入来源。
pub trait CredentialProvider: Send + 'static {
    /// Loads one bounded credential pair; the future is cancelled when the session is cancelled.
    /// 加载一组有界凭证；会话取消时会取消该 future。
    fn load_credentials(
        &mut self,
    ) -> impl Future<Output = Result<AlpacaCredentials, CredentialFailure>> + Send;
}

/// Fixed credential-source failure that never carries provider text or secret values.
/// 不携带 provider 文本或秘密值的固定凭证来源错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialFailure {
    /// No credentials are currently available.
    /// 当前没有可用凭证。
    Unavailable,
}

impl Display for CredentialFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("ALPACA_CREDENTIALS_UNAVAILABLE")
    }
}

impl Error for CredentialFailure {}

/// Safe validation category for injected credentials.
/// 注入凭证校验失败的固定类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialValidationError {
    /// A credential field is empty, oversized, or contains non-visible ASCII.
    /// 凭证字段为空、过长或包含不可见 ASCII 字符。
    InvalidCredential,
}

impl Display for CredentialValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid Alpaca credential input")
    }
}

impl Error for CredentialValidationError {}
