//! Typed read-admission namespace and provenance shared by catalog and account reads.
//!
//! This module defines evidence carried across a port boundary; it does not own the
//! runtime budget, issue permits, or establish that a caller is authorized. A
//! composition root must supply an evidence implementation backed by its one
//! trusted admission owner.
//!
//! # 简体中文
//!
//! 本模块定义合约目录和账户读取共用的类型化读取准入命名空间与来源记录。
//!
//! 此模块只传递端口边界上的证据，不拥有运行时预算、不签发准入凭证，也不证明调用方已获授权。
//! 系统装配根必须提供由唯一可信准入 owner 支撑的 evidence 实现。

use domain::{AccountNamespace, MetadataSource};
use std::fmt;

const MAX_PROVENANCE_ID_BYTES: usize = 128;

/// Maximum UTF-8 byte length for one request correlation identifier.
/// 单个请求关联标识允许的最大 UTF-8 字节数。
pub const MAX_READ_REQUEST_ID_BYTES: usize = MAX_PROVENANCE_ID_BYTES;

/// Namespace to which a read-admission decision applies.
/// 读取准入决策所适用的命名空间。
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum ReadAdmissionNamespace {
    /// Read of one provider's instrument catalog.
    /// 读取一个供应商的合约目录。
    InstrumentCatalog(MetadataSource),
    /// Read of one explicitly bound broker account.
    /// 读取一个显式绑定的券商账户。
    Account(AccountNamespace),
}

/// Stable identifying provenance for one runtime admission decision.
/// 一次运行时准入决策的稳定来源标识。
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ReadAdmissionProvenance {
    policy_id: String,
    policy_revision: String,
    decision_id: String,
}

/// Opaque correlation identifier for one read operation.
/// 一次读取操作的不透明关联标识。
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ReadRequestId(String);

/// Invalid identifying value in admission provenance.
/// 准入来源标识中的值无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadAdmissionProvenanceError {
    /// An identifier is empty, too long, has edge whitespace, or contains a control character.
    /// 标识为空、过长、首尾含空白或含控制字符。
    InvalidIdentifier,
}

impl ReadAdmissionProvenance {
    /// Create provenance with explicit policy, policy revision, and admission decision IDs.
    /// 创建显式包含策略、策略修订及准入决策 ID 的来源记录。
    ///
    /// # Errors
    ///
    /// Returns [`ReadAdmissionProvenanceError::InvalidIdentifier`] if any ID is empty,
    /// over 128 UTF-8 bytes, padded with edge whitespace, or contains a control character.
    ///
    /// # 错误
    ///
    /// 任一 ID 为空、超过 128 UTF-8 字节、含首尾空白或控制字符时，返回
    /// [`ReadAdmissionProvenanceError::InvalidIdentifier`]。
    pub fn new(
        policy_id: impl Into<String>,
        policy_revision: impl Into<String>,
        decision_id: impl Into<String>,
    ) -> Result<Self, ReadAdmissionProvenanceError> {
        let policy_id = policy_id.into();
        let policy_revision = policy_revision.into();
        let decision_id = decision_id.into();
        if [&policy_id, &policy_revision, &decision_id]
            .into_iter()
            .any(|value| !valid_id(value))
        {
            return Err(ReadAdmissionProvenanceError::InvalidIdentifier);
        }
        Ok(Self {
            policy_id,
            policy_revision,
            decision_id,
        })
    }

    /// Return the identifier of the policy that evaluated this read.
    /// 返回评估本次读取的策略标识。
    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    /// Return the immutable policy revision used for this decision.
    /// 返回本次决策使用的不可变策略修订。
    #[must_use]
    pub fn policy_revision(&self) -> &str {
        &self.policy_revision
    }

    /// Return the unique decision identifier supplied by the admission owner.
    /// 返回准入 owner 提供的唯一决策标识。
    #[must_use]
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }
}

impl fmt::Debug for ReadAdmissionNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstrumentCatalog(source) => formatter
                .debug_tuple("InstrumentCatalog")
                .field(source)
                .finish(),
            Self::Account(_) => formatter
                .debug_tuple("Account")
                .field(&"[REDACTED]")
                .finish(),
        }
    }
}

impl fmt::Debug for ReadAdmissionProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadAdmissionProvenance")
            .field("policy_id", &"[REDACTED]")
            .field("policy_revision", &"[REDACTED]")
            .field("decision_id", &"[REDACTED]")
            .finish()
    }
}

impl ReadRequestId {
    /// Create a bounded read correlation identifier.
    /// 创建有界读取关联标识。
    ///
    /// # Errors
    ///
    /// Returns [`ReadAdmissionProvenanceError::InvalidIdentifier`] when the identifier
    /// fails the shared non-empty, 128-byte, whitespace, or control-character checks.
    ///
    /// # 错误
    ///
    /// 标识未通过统一的非空、128 字节、空白字符或控制字符校验时，返回
    /// [`ReadAdmissionProvenanceError::InvalidIdentifier`]。
    pub fn new(value: impl Into<String>) -> Result<Self, ReadAdmissionProvenanceError> {
        let value = value.into();
        if !valid_id(&value) {
            return Err(ReadAdmissionProvenanceError::InvalidIdentifier);
        }
        Ok(Self(value))
    }

    /// Return the identifier for explicit request correlation.
    /// 返回用于显式请求关联的标识。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ReadRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ReadRequestId")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Evidence issued by the one trusted runtime read-admission owner.
/// 由唯一可信运行时读取准入 owner 签发的证据。
///
/// Implementations must retain the exact authorization namespace and policy
/// provenance. A boolean such as `admitted = true` is insufficient. Implementing
/// this trait is an application trust boundary; the type alone does not prove
/// that an implementation is honest.
///
/// 实现必须保留精确的授权命名空间与策略来源。`admitted = true` 之类的布尔值不充分。实现本 trait
/// 属于应用信任边界；仅凭类型本身无法证明实现可信。
pub trait ReadAdmissionEvidence: Send + Sync {
    /// Return the exact account or provider-catalog authorization namespace.
    /// 返回精确的账户或供应商目录授权命名空间。
    fn namespace(&self) -> &ReadAdmissionNamespace;

    /// Return the policy and decision provenance retained by the admission owner.
    /// 返回准入 owner 保留的策略和决策来源记录。
    fn provenance(&self) -> &ReadAdmissionProvenance;
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROVENANCE_ID_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::{ReadAdmissionProvenance, ReadAdmissionProvenanceError, ReadRequestId};

    #[test]
    fn provenance_requires_bounded_nonempty_identifiers() {
        let provenance = ReadAdmissionProvenance::new("minute-read", "sha256:abc", "decision-1")
            .expect("synthetic provenance is valid");
        assert_eq!(provenance.policy_id(), "minute-read");
        assert_eq!(provenance.policy_revision(), "sha256:abc");
        assert_eq!(provenance.decision_id(), "decision-1");

        for invalid in ["", " leading", "trailing ", "line\nbreak"] {
            assert_eq!(
                ReadAdmissionProvenance::new(invalid, "v1", "decision-1"),
                Err(ReadAdmissionProvenanceError::InvalidIdentifier)
            );
        }
        let too_long = "x".repeat(129);
        assert_eq!(
            ReadAdmissionProvenance::new(too_long, "v1", "decision-1"),
            Err(ReadAdmissionProvenanceError::InvalidIdentifier)
        );
    }

    #[test]
    fn request_identifier_debug_is_redacted() {
        let request_id = ReadRequestId::new("synthetic-private-correlation")
            .expect("synthetic request ID is valid");
        assert_eq!(request_id.as_str(), "synthetic-private-correlation");
        assert!(!format!("{request_id:?}").contains("synthetic-private-correlation"));

        let provenance = ReadAdmissionProvenance::new(
            "synthetic-policy-secret",
            "synthetic-policy-revision",
            "synthetic-decision-secret",
        )
        .unwrap();
        let debug = format!("{provenance:?}");
        assert!(!debug.contains("synthetic-policy-secret"));
        assert!(!debug.contains("synthetic-policy-revision"));
        assert!(!debug.contains("synthetic-decision-secret"));
    }
}
