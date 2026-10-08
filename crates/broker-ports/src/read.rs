//! Provider-neutral, account-namespaced read requests and observations.
//!
//! The adapter supplies associated row types so this port does not create a
//! second account-state authority. Pages preserve provider and local receive
//! evidence; exhausting a cursor is not a durable snapshot or completeness
//! guarantee.
//!
//! # 简体中文
//!
//! 本模块定义供应商中立且带账户命名空间的只读请求和观察结果。
//!
//! 行类型由 adapter 通过关联类型提供，因此本端口不会创建第二套账户状态权威。分页保留供应商和
//! 本地接收证据；游标耗尽不构成持久快照或完整性保证。

use crate::{
    BrokerPortError, OpaquePageCursor, PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace,
    ReadRequestId,
};
use domain::{AccountNamespace, ExecutionBrokerId, ProviderRecordId};
use market_contracts::UtcTimestamp;
use std::fmt;

/// Maximum requested rows in one broker-account page.
/// 单页券商账户读取允许请求的最大行数。
pub const MAX_READ_PAGE_SIZE: u16 = 1_000;

/// Account-scoped read request with explicit correlation and admission evidence.
/// 携带显式关联标识和准入证据的账户范围只读请求。
pub struct AccountReadRequest<A> {
    namespace: AccountNamespace,
    request_id: ReadRequestId,
    admission: A,
}

/// Invalid account-read request or admission linkage.
/// 账户只读请求或准入关联无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountReadRequestError {
    /// The admission evidence belongs to another account namespace.
    /// 准入证据属于其他账户命名空间。
    AdmissionNamespaceMismatch,
}

impl<A: ReadAdmissionEvidence> AccountReadRequest<A> {
    /// Bind a read request to an exact account namespace and runtime admission token.
    /// 将读取请求绑定到精确账户命名空间和运行时准入令牌。
    ///
    /// # Errors
    ///
    /// Returns [`AccountReadRequestError::AdmissionNamespaceMismatch`] if the
    /// admission evidence names another account namespace.
    ///
    /// # 错误
    ///
    /// 准入证据指向其他账户命名空间时，返回
    /// [`AccountReadRequestError::AdmissionNamespaceMismatch`]。
    pub fn new(
        namespace: AccountNamespace,
        request_id: ReadRequestId,
        admission: A,
    ) -> Result<Self, AccountReadRequestError> {
        if admission.namespace() != &ReadAdmissionNamespace::Account(namespace.clone()) {
            return Err(AccountReadRequestError::AdmissionNamespaceMismatch);
        }
        Ok(Self {
            namespace,
            request_id,
            admission,
        })
    }

    /// Return the exact broker, environment, and account scope requested.
    /// 返回请求的精确券商、环境和账户范围。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }

    /// Return this read operation's correlation identifier.
    /// 返回本次读取操作的关联标识。
    #[must_use]
    pub const fn request_id(&self) -> &ReadRequestId {
        &self.request_id
    }

    /// Borrow the exact admission evidence passed to the read port.
    /// 借用传入只读端口的精确准入证据。
    #[must_use]
    pub const fn admission(&self) -> &A {
        &self.admission
    }
}

/// One bounded account-page request using an opaque provider cursor.
/// 使用不透明供应商游标的单页有界账户请求。
pub struct BrokerReadPageRequest<A> {
    base: AccountReadRequest<A>,
    page_size: u16,
    cursor: Option<OpaquePageCursor>,
}

impl<A: ReadAdmissionEvidence> BrokerReadPageRequest<A> {
    /// Create a page request with an explicit bound and optional provider cursor.
    /// 使用显式上限和可选供应商游标创建分页请求。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerReadError::InvalidRequest`] when `page_size` is zero or
    /// exceeds [`MAX_READ_PAGE_SIZE`].
    ///
    /// # 错误
    ///
    /// `page_size` 为零或超过 [`MAX_READ_PAGE_SIZE`] 时，返回
    /// [`BrokerReadError::InvalidRequest`]。
    pub fn new(
        base: AccountReadRequest<A>,
        page_size: u16,
        cursor: Option<OpaquePageCursor>,
    ) -> Result<Self, BrokerReadError> {
        if page_size == 0 || page_size > MAX_READ_PAGE_SIZE {
            return Err(BrokerReadError::InvalidRequest);
        }
        Ok(Self {
            base,
            page_size,
            cursor,
        })
    }

    /// Borrow the underlying namespaced request.
    /// 借用底层带命名空间的请求。
    #[must_use]
    pub const fn base(&self) -> &AccountReadRequest<A> {
        &self.base
    }

    /// Return the maximum number of requested rows.
    /// 返回请求的最大行数。
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Borrow the provider cursor, if this request continues a prior page.
    /// 若此请求延续上一页，则借用供应商游标。
    #[must_use]
    pub const fn cursor(&self) -> Option<&OpaquePageCursor> {
        self.cursor.as_ref()
    }
}

/// Timestamp and provider-record evidence attached to one read response.
/// 附加到一次读取响应上的时间戳和供应商记录证据。
#[derive(Clone, Eq, PartialEq)]
pub struct ReadEvidence {
    broker: ExecutionBrokerId,
    provider_record: Option<ProviderRecordId>,
    provider_timestamp: Option<UtcTimestamp>,
    received_at: UtcTimestamp,
}

impl fmt::Debug for ReadEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadEvidence")
            .field("broker", &self.broker)
            .field(
                "provider_record",
                &self.provider_record.as_ref().map(|_| "[REDACTED]"),
            )
            .field("provider_timestamp", &self.provider_timestamp)
            .field("received_at", &self.received_at)
            .finish()
    }
}

impl ReadEvidence {
    /// Retain provider identity, optional provider time, and local receipt time.
    /// 保留供应商身份、可选供应商时间和本地接收时间。
    #[must_use]
    pub const fn new(
        broker: ExecutionBrokerId,
        provider_record: Option<ProviderRecordId>,
        provider_timestamp: Option<UtcTimestamp>,
        received_at: UtcTimestamp,
    ) -> Self {
        Self {
            broker,
            provider_record,
            provider_timestamp,
            received_at,
        }
    }

    /// Return the provider whose endpoint supplied the response.
    /// 返回提供响应的供应商。
    #[must_use]
    pub const fn broker(&self) -> &ExecutionBrokerId {
        &self.broker
    }

    /// Borrow the provider record identity, when supplied.
    /// 若供应商提供了记录标识，则借用该标识。
    #[must_use]
    pub const fn provider_record(&self) -> Option<&ProviderRecordId> {
        self.provider_record.as_ref()
    }

    /// Borrow the provider timestamp, when supplied.
    /// 若供应商提供了时间戳，则借用该时间戳。
    #[must_use]
    pub const fn provider_timestamp(&self) -> Option<&UtcTimestamp> {
        self.provider_timestamp.as_ref()
    }

    /// Return the local time at which the adapter received the response.
    /// 返回 adapter 收到响应的本地时间。
    #[must_use]
    pub const fn received_at(&self) -> &UtcTimestamp {
        &self.received_at
    }
}

/// One account read value with its explicit namespace and response evidence.
/// 一个带显式命名空间和响应证据的账户读取结果。
#[derive(Clone, Eq, PartialEq)]
pub struct ObservedRead<T> {
    namespace: AccountNamespace,
    request_id: ReadRequestId,
    admission_provenance: crate::ReadAdmissionProvenance,
    value: T,
    evidence: ReadEvidence,
}

impl<T> fmt::Debug for ObservedRead<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservedRead")
            .field("namespace", &"[REDACTED]")
            .field("request_id", &self.request_id)
            .field("admission_provenance", &"[REDACTED]")
            .field("value", &"[REDACTED]")
            .field("evidence", &self.evidence)
            .finish()
    }
}

impl<T> ObservedRead<T> {
    /// Bind a response to its request and reject a mismatched broker identity.
    /// 将响应绑定到原请求，并拒绝券商身份不匹配的结果。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerReadError::IdentityMismatch`] if the response evidence
    /// names a broker other than the request namespace.
    ///
    /// # 错误
    ///
    /// 响应证据标识的券商与请求命名空间不一致时，返回
    /// [`BrokerReadError::IdentityMismatch`]。
    pub fn new<A>(
        request: &AccountReadRequest<A>,
        value: T,
        evidence: ReadEvidence,
    ) -> Result<Self, BrokerReadError>
    where
        A: ReadAdmissionEvidence,
    {
        if evidence.broker() != request.namespace().broker() {
            return Err(BrokerReadError::IdentityMismatch);
        }
        Ok(Self {
            namespace: request.namespace().clone(),
            request_id: request.request_id().clone(),
            admission_provenance: request.admission().provenance().clone(),
            value,
            evidence,
        })
    }

    /// Return the account namespace used for this request.
    /// 返回本次请求使用的账户命名空间。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }

    /// Return the request correlation identifier.
    /// 返回请求关联标识。
    #[must_use]
    pub const fn request_id(&self) -> &ReadRequestId {
        &self.request_id
    }

    /// Borrow the admission policy and decision provenance for this response.
    /// 借用本响应的准入策略和决策来源记录。
    #[must_use]
    pub const fn admission_provenance(&self) -> &crate::ReadAdmissionProvenance {
        &self.admission_provenance
    }

    /// Borrow the read value without upgrading it to account-state authority.
    /// 借用读取值，但不将其升级为账户状态权威。
    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Borrow source and timestamp evidence for this response.
    /// 借用本响应的来源和时间戳证据。
    #[must_use]
    pub const fn evidence(&self) -> &ReadEvidence {
        &self.evidence
    }
}

/// One provider page of account rows with cursor and response evidence.
/// 一页账户记录及其游标和响应证据。
#[derive(Clone, Eq, PartialEq)]
pub struct BrokerReadPage<T> {
    namespace: AccountNamespace,
    request_id: ReadRequestId,
    admission_provenance: crate::ReadAdmissionProvenance,
    rows: Vec<T>,
    next_cursor: Option<OpaquePageCursor>,
    evidence: ReadEvidence,
}

impl<T> fmt::Debug for BrokerReadPage<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerReadPage")
            .field("namespace", &"[REDACTED]")
            .field("request_id", &self.request_id)
            .field("admission_provenance", &"[REDACTED]")
            .field("row_count", &self.rows.len())
            .field("next_cursor", &self.next_cursor)
            .field("evidence", &self.evidence)
            .finish()
    }
}

/// Invalid account read page returned by a broker adapter.
/// 券商 adapter 返回的账户分页无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerReadPageError {
    /// The adapter returned more rows than the requested bound.
    /// adapter 返回的行数超过请求上限。
    TooManyRows,
    /// The response identity does not match the requested broker.
    /// 响应身份与请求的券商不匹配。
    IdentityMismatch,
}

impl<T> BrokerReadPage<T> {
    /// Validate row count and provider identity before returning one page.
    /// 返回单页前校验行数和供应商身份。
    ///
    /// # Errors
    ///
    /// Returns [`BrokerReadPageError::TooManyRows`] when the row count exceeds
    /// the request bound, or [`BrokerReadPageError::IdentityMismatch`] when the
    /// response evidence names another broker.
    ///
    /// # 错误
    ///
    /// 行数超过请求上限时返回 [`BrokerReadPageError::TooManyRows`]；响应证据标识的券商
    /// 不匹配时返回 [`BrokerReadPageError::IdentityMismatch`]。
    pub fn new<A>(
        request: &BrokerReadPageRequest<A>,
        rows: Vec<T>,
        next_cursor: Option<OpaquePageCursor>,
        evidence: ReadEvidence,
    ) -> Result<Self, BrokerReadPageError>
    where
        A: ReadAdmissionEvidence,
    {
        if rows.len() > usize::from(request.page_size()) {
            return Err(BrokerReadPageError::TooManyRows);
        }
        if evidence.broker() != request.base().namespace().broker() {
            return Err(BrokerReadPageError::IdentityMismatch);
        }
        Ok(Self {
            namespace: request.base().namespace().clone(),
            request_id: request.base().request_id().clone(),
            admission_provenance: request.base().admission().provenance().clone(),
            rows,
            next_cursor,
            evidence,
        })
    }

    /// Return the explicit broker/environment/account namespace.
    /// 返回显式券商/环境/账户命名空间。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }

    /// Return the request identifier for this page.
    /// 返回本页对应的请求标识。
    #[must_use]
    pub const fn request_id(&self) -> &ReadRequestId {
        &self.request_id
    }

    /// Borrow the admission policy and decision provenance for this page.
    /// 借用本页的准入策略和决策来源记录。
    #[must_use]
    pub const fn admission_provenance(&self) -> &crate::ReadAdmissionProvenance {
        &self.admission_provenance
    }

    /// Borrow rows in provider order.
    /// 按供应商返回顺序借用数据行。
    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// Borrow a provider continuation cursor when one was returned.
    /// 若供应商返回续页游标，则借用该游标。
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&OpaquePageCursor> {
        self.next_cursor.as_ref()
    }

    /// Borrow provider and local receipt evidence.
    /// 借用供应商与本地接收证据。
    #[must_use]
    pub const fn evidence(&self) -> &ReadEvidence {
        &self.evidence
    }
}

/// Safe read-only broker operation failures without raw provider diagnostics.
/// 不包含供应商原始诊断信息的安全只读券商错误分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerReadError {
    /// The request has invalid paging values.
    /// 分页请求参数无效。
    InvalidRequest,
    /// The request admission token belongs to another namespace.
    /// 请求准入令牌属于其他命名空间。
    NamespaceMismatch,
    /// The provider response identified another broker.
    /// 供应商响应标识了不同的券商。
    IdentityMismatch,
    /// The provider denied access or account visibility.
    /// 供应商拒绝访问或账户可见性。
    Unauthorized,
    /// The provider rejected this query for a reason that is not safely classifiable.
    /// 供应商拒绝了查询，但无法安全细分拒绝原因。
    ProviderRejected,
    /// The provider rate-limited the read.
    /// 供应商对读取进行了限流。
    RateLimited,
    /// The local bounded delivery or concurrency resource is saturated.
    /// 本地有界交付或并发资源已饱和。
    Overloaded,
    /// The provider could not complete the read.
    /// 供应商无法完成读取。
    Unavailable,
    /// The response violated the adapter's normalized contract.
    /// 响应违反 adapter 的规范化合同。
    InvalidResponse,
    /// The adapter does not implement this read operation.
    /// adapter 未实现该读取操作。
    Unsupported,
    /// The caller's bounded read deadline expired.
    /// 调用方的有界读取 deadline 已到期。
    DeadlineExceeded,
}

impl From<BrokerPortError> for BrokerReadError {
    fn from(error: BrokerPortError) -> Self {
        match error {
            BrokerPortError::InvalidRequest | BrokerPortError::LimitExceeded => {
                Self::InvalidRequest
            }
            BrokerPortError::UnsupportedSource => Self::Unsupported,
            BrokerPortError::Transport => Self::Unavailable,
            BrokerPortError::ProviderRejected => Self::ProviderRejected,
            BrokerPortError::Overloaded => Self::Overloaded,
            BrokerPortError::ProtocolViolation => Self::InvalidResponse,
        }
    }
}

/// Provider-neutral, read-only account port with explicit associated data rows.
/// 通过显式关联数据行提供供应商中立只读能力的账户端口。
///
/// Associated row types belong to the adapter boundary and must not be raw SDK
/// DTOs. The consuming engine maps these observations into its one account-state
/// authority. Implementations reject a request whose complete namespace differs
/// from `account_namespace()` before calling the provider. No port method
/// mutates broker state, and this trait does not define order submission.
///
/// 关联行类型属于 adapter 边界，不能直接使用 SDK DTO。消费方 engine 将这些观察映射到唯一账户
/// 状态权威。本端口所有方法均不修改券商状态，也不定义订单提交。
pub trait BrokerReadPort: Send + Sync {
    /// Runtime admission token from the trusted shared read-budget owner.
    /// 来自可信共享读取预算 owner 的运行时准入令牌。
    type Admission: ReadAdmissionEvidence;

    /// Normalized account summary row supplied by the adapter boundary.
    /// adapter 边界提供的规范化账户摘要行。
    type AccountSummary: Send + Sync + 'static;

    /// Normalized position row supplied by the adapter boundary.
    /// adapter 边界提供的规范化持仓行。
    type Position: Send + Sync + 'static;

    /// Normalized open-order observation supplied by the adapter boundary.
    /// adapter 边界提供的规范化未结订单观察。
    type OpenOrder: Send + Sync + 'static;

    /// Normalized cumulative fill observation supplied by the adapter boundary.
    /// adapter 边界提供的规范化累计成交观察。
    type Fill: Send + Sync + 'static;

    /// Return the adapter's fixed complete account namespace.
    /// 返回 adapter 固定的完整账户命名空间。
    fn account_namespace(&self) -> &AccountNamespace;

    /// Read one account summary using a request from the same namespace.
    /// 使用来自同一命名空间的请求读取账户摘要。
    fn read_account(
        &self,
        request: AccountReadRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<ObservedRead<Self::AccountSummary>, BrokerReadError>>;

    /// Read one bounded position page.
    /// 读取一页有界持仓。
    fn read_positions(
        &self,
        request: BrokerReadPageRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<BrokerReadPage<Self::Position>, BrokerReadError>>;

    /// Read one bounded open-order page.
    /// 读取一页有界未结订单。
    fn read_open_orders(
        &self,
        request: BrokerReadPageRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<BrokerReadPage<Self::OpenOrder>, BrokerReadError>>;

    /// Read one bounded cumulative-fill page.
    /// 读取一页有界累计成交。
    fn read_fills(
        &self,
        request: BrokerReadPageRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<BrokerReadPage<Self::Fill>, BrokerReadError>>;
}

#[cfg(test)]
mod tests {
    use super::{
        AccountReadRequest, AccountReadRequestError, BrokerReadError, BrokerReadPage,
        BrokerReadPageError, BrokerReadPageRequest, BrokerReadPort, MAX_READ_PAGE_SIZE,
        ObservedRead, ReadEvidence,
    };
    use crate::{
        PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace, ReadAdmissionProvenance,
        ReadRequestId,
    };
    use domain::{AccountNamespace, AccountScope, BrokerEnvironment, ExecutionBrokerId};
    use market_contracts::UtcTimestamp;

    struct FakeAdmission {
        namespace: ReadAdmissionNamespace,
        provenance: ReadAdmissionProvenance,
    }

    impl ReadAdmissionEvidence for FakeAdmission {
        fn namespace(&self) -> &ReadAdmissionNamespace {
            &self.namespace
        }

        fn provenance(&self) -> &ReadAdmissionProvenance {
            &self.provenance
        }
    }

    fn namespace() -> AccountNamespace {
        AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            BrokerEnvironment::Paper,
            AccountScope::new("synthetic-account").unwrap(),
        )
    }

    fn admission(namespace: &AccountNamespace) -> FakeAdmission {
        FakeAdmission {
            namespace: ReadAdmissionNamespace::Account(namespace.clone()),
            provenance: ReadAdmissionProvenance::new("test-policy", "rev-1", "decision-1").unwrap(),
        }
    }

    fn request() -> AccountReadRequest<FakeAdmission> {
        let namespace = namespace();
        AccountReadRequest::new(
            namespace.clone(),
            ReadRequestId::new("synthetic-read-1").unwrap(),
            admission(&namespace),
        )
        .unwrap()
    }

    fn evidence(broker: ExecutionBrokerId) -> ReadEvidence {
        ReadEvidence::new(
            broker,
            None,
            Some(UtcTimestamp::parse("2026-10-08T14:30:00Z").unwrap()),
            UtcTimestamp::parse("2026-10-08T14:30:01Z").unwrap(),
        )
    }

    #[test]
    fn account_request_requires_exact_admission_namespace() {
        let requested = namespace();
        let other = AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            BrokerEnvironment::Live,
            AccountScope::new("synthetic-account").unwrap(),
        );
        assert_eq!(
            AccountReadRequest::new(
                requested,
                ReadRequestId::new("synthetic-read-2").unwrap(),
                admission(&other),
            )
            .err(),
            Some(AccountReadRequestError::AdmissionNamespaceMismatch)
        );
    }

    #[test]
    fn broker_page_enforces_bound_and_broker_identity() {
        let base = request();
        let page_request = BrokerReadPageRequest::new(
            base,
            2,
            Some(crate::OpaquePageCursor::new("synthetic-cursor").unwrap()),
        )
        .unwrap();
        let page = BrokerReadPage::new(
            &page_request,
            vec!["position-a".to_owned()],
            None,
            evidence(ExecutionBrokerId::Alpaca),
        )
        .unwrap();
        assert_eq!(page.rows(), ["position-a"]);
        assert_eq!(page.admission_provenance().decision_id(), "decision-1");
        assert!(page.next_cursor().is_none());
        let debug = format!("{page:?}");
        assert!(!debug.contains("synthetic-account"));
        assert!(!debug.contains("position-a"));
        assert!(!debug.contains("decision-1"));

        assert_eq!(
            BrokerReadPage::new(
                &page_request,
                vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                None,
                evidence(ExecutionBrokerId::Alpaca),
            ),
            Err(BrokerReadPageError::TooManyRows)
        );
        assert_eq!(
            BrokerReadPage::<String>::new(
                &page_request,
                vec![],
                None,
                evidence(ExecutionBrokerId::Schwab),
            ),
            Err(BrokerReadPageError::IdentityMismatch)
        );
    }

    #[test]
    fn page_request_has_fixed_upper_bound() {
        assert!(BrokerReadPageRequest::new(request(), MAX_READ_PAGE_SIZE, None).is_ok());
        assert!(matches!(
            BrokerReadPageRequest::new(request(), MAX_READ_PAGE_SIZE + 1, None),
            Err(BrokerReadError::InvalidRequest)
        ));
        assert!(matches!(
            BrokerReadPageRequest::new(request(), 0, None),
            Err(BrokerReadError::InvalidRequest)
        ));
    }

    #[test]
    fn summary_response_rejects_wrong_broker_identity() {
        assert!(matches!(
            ObservedRead::new(
                &request(),
                "synthetic-summary",
                evidence(ExecutionBrokerId::Schwab)
            ),
            Err(BrokerReadError::IdentityMismatch)
        ));
    }

    #[tokio::test]
    async fn offline_fake_implements_only_read_operations() {
        let fake = FakeReadPort {
            namespace: namespace(),
        };
        let result = fake.read_account(request()).await.unwrap();
        assert_eq!(result.value(), "synthetic-account-summary");
        assert_eq!(result.admission_provenance().decision_id(), "decision-1");
        assert_eq!(result.evidence().broker(), &ExecutionBrokerId::Alpaca);
        let debug = format!("{result:?}");
        assert!(!debug.contains("synthetic-account"));
        assert!(!debug.contains("synthetic-account-summary"));
        assert!(!debug.contains("decision-1"));
    }

    struct FakeReadPort {
        namespace: AccountNamespace,
    }

    impl BrokerReadPort for FakeReadPort {
        type Admission = FakeAdmission;
        type AccountSummary = String;
        type Position = String;
        type OpenOrder = String;
        type Fill = String;

        fn account_namespace(&self) -> &AccountNamespace {
            &self.namespace
        }

        fn read_account(
            &self,
            request: AccountReadRequest<Self::Admission>,
        ) -> PortFuture<'_, Result<ObservedRead<Self::AccountSummary>, BrokerReadError>> {
            Box::pin(async move {
                if request.namespace() != &self.namespace {
                    return Err(BrokerReadError::NamespaceMismatch);
                }
                ObservedRead::new(
                    &request,
                    "synthetic-account-summary".to_owned(),
                    evidence(ExecutionBrokerId::Alpaca),
                )
            })
        }

        fn read_positions(
            &self,
            _request: BrokerReadPageRequest<Self::Admission>,
        ) -> PortFuture<'_, Result<BrokerReadPage<Self::Position>, BrokerReadError>> {
            Box::pin(async { Err(BrokerReadError::Unsupported) })
        }

        fn read_open_orders(
            &self,
            _request: BrokerReadPageRequest<Self::Admission>,
        ) -> PortFuture<'_, Result<BrokerReadPage<Self::OpenOrder>, BrokerReadError>> {
            Box::pin(async { Err(BrokerReadError::Unsupported) })
        }

        fn read_fills(
            &self,
            _request: BrokerReadPageRequest<Self::Admission>,
        ) -> PortFuture<'_, Result<BrokerReadPage<Self::Fill>, BrokerReadError>> {
            Box::pin(async { Err(BrokerReadError::Unsupported) })
        }
    }
}
