//! Read-only option-catalog queries that preserve candidate evidence and admission provenance.
//!
//! Catalog results remain `OptionInstrumentCandidate` values. A parsed symbol is
//! not upgraded to a qualified contract unless every required provider term is
//! present and validated by the shared domain model.
//!
//! # 简体中文
//!
//! 本模块定义保留候选证据和准入来源的只读期权目录查询。
//!
//! 目录结果保持为 `OptionInstrumentCandidate`。只有共享领域模型验证了所有必需供应商条款后，
//! 才能将解析出的标识升级为完整合约。

use crate::{
    BrokerPortError, PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace, ReadRequestId,
};
use domain::{ExpirationDate, MetadataSource, OptionInstrumentCandidate, Underlying};
use market_contracts::UtcTimestamp;
use std::fmt;
use std::num::NonZeroU16;

/// Maximum number of option candidates requested in one catalog page.
/// 单页期权目录允许请求的最大候选数量。
pub const MAX_CATALOG_PAGE_SIZE: u16 = 1_000;

/// Maximum byte length of an opaque provider pagination cursor.
/// 不透明供应商分页游标允许的最大字节数。
pub const MAX_CURSOR_BYTES: usize = 1_024;

/// Opaque provider pagination token. Its debug form never reveals the raw cursor.
/// 不透明供应商分页令牌；`Debug` 输出不会暴露原始游标。
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct OpaquePageCursor(String);

/// Invalid provider pagination cursor.
/// 供应商分页游标无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpaquePageCursorError {
    /// The cursor is empty, oversized, edge-padded, or contains a control character.
    /// 游标为空、过长、首尾含空白或包含控制字符。
    Invalid,
}

impl OpaquePageCursor {
    /// Create a bounded opaque provider cursor.
    /// 创建有界不透明供应商游标。
    ///
    /// # Errors
    ///
    /// Returns [`OpaquePageCursorError::Invalid`] for an empty, oversized, padded,
    /// or control-containing cursor.
    ///
    /// # 错误
    ///
    /// 游标为空、过长、带首尾空白或含控制字符时，返回 [`OpaquePageCursorError::Invalid`]。
    pub fn new(value: impl Into<String>) -> Result<Self, OpaquePageCursorError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_CURSOR_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(OpaquePageCursorError::Invalid);
        }
        Ok(Self(value))
    }

    /// Return the cursor for an explicit adapter request.
    /// 返回供 adapter 显式请求使用的游标。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OpaquePageCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaquePageCursor")
            .field("len_bytes", &self.0.len())
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// A bounded option-catalog query with an exact runtime admission token.
/// 携带精确运行时准入令牌的有界期权目录查询。
pub struct OptionCatalogQuery<A> {
    source: MetadataSource,
    underlying: Underlying,
    expiration: Option<ExpirationDate>,
    page_size: NonZeroU16,
    cursor: Option<OpaquePageCursor>,
    request_id: ReadRequestId,
    admission: A,
}

/// Invalid option-catalog query or admission linkage.
/// 期权目录查询或准入关联无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionCatalogQueryError {
    /// The requested page size is zero or exceeds its fixed bound.
    /// 请求页大小为零或超过固定上限。
    InvalidPageSize,
    /// The admission evidence belongs to a different provider namespace.
    /// 准入证据属于不同供应商命名空间。
    AdmissionNamespaceMismatch,
}

impl<A: ReadAdmissionEvidence> OptionCatalogQuery<A> {
    /// Create an explicit source-bound query after acquiring its read-admission evidence.
    /// 取得读取准入证据后，创建显式绑定来源的目录查询。
    ///
    /// # Errors
    ///
    /// Returns [`OptionCatalogQueryError::InvalidPageSize`] outside `1..=MAX_CATALOG_PAGE_SIZE`,
    /// or [`OptionCatalogQueryError::AdmissionNamespaceMismatch`] when the evidence is not
    /// bound to the requested source.
    ///
    /// # 错误
    ///
    /// 页大小不在 `1..=MAX_CATALOG_PAGE_SIZE` 时返回
    /// [`OptionCatalogQueryError::InvalidPageSize`]；准入证据未绑定所请求来源时返回
    /// [`OptionCatalogQueryError::AdmissionNamespaceMismatch`]。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: MetadataSource,
        underlying: Underlying,
        expiration: Option<ExpirationDate>,
        page_size: u16,
        cursor: Option<OpaquePageCursor>,
        request_id: ReadRequestId,
        admission: A,
    ) -> Result<Self, OptionCatalogQueryError> {
        if page_size == 0 || page_size > MAX_CATALOG_PAGE_SIZE {
            return Err(OptionCatalogQueryError::InvalidPageSize);
        }
        let expected_namespace = ReadAdmissionNamespace::InstrumentCatalog(source.clone());
        if admission.namespace() != &expected_namespace {
            return Err(OptionCatalogQueryError::AdmissionNamespaceMismatch);
        }
        Ok(Self {
            source,
            underlying,
            expiration,
            page_size: NonZeroU16::new(page_size)
                .ok_or(OptionCatalogQueryError::InvalidPageSize)?,
            cursor,
            request_id,
            admission,
        })
    }

    /// Return the provider or broker that owns the requested catalog.
    /// 返回所请求目录的供应商或券商来源。
    #[must_use]
    pub const fn source(&self) -> &MetadataSource {
        &self.source
    }

    /// Return the requested underlying asset.
    /// 返回请求的标的资产。
    #[must_use]
    pub const fn underlying(&self) -> &Underlying {
        &self.underlying
    }

    /// Return the optional exact expiration filter.
    /// 返回可选的精确到期日筛选条件。
    #[must_use]
    pub const fn expiration(&self) -> Option<ExpirationDate> {
        self.expiration
    }

    /// Return the bounded page size.
    /// 返回有界页大小。
    #[must_use]
    pub const fn page_size(&self) -> NonZeroU16 {
        self.page_size
    }

    /// Borrow the provider cursor, if the previous page supplied one.
    /// 若上一页提供了供应商游标，则借用该游标。
    #[must_use]
    pub const fn cursor(&self) -> Option<&OpaquePageCursor> {
        self.cursor.as_ref()
    }

    /// Return the read request correlation identifier.
    /// 返回读取请求关联标识。
    #[must_use]
    pub const fn request_id(&self) -> &ReadRequestId {
        &self.request_id
    }

    /// Borrow the exact admission evidence used for this query.
    /// 借用本查询使用的精确准入证据。
    #[must_use]
    pub const fn admission(&self) -> &A {
        &self.admission
    }
}

/// One bounded page of source-attributed option candidates.
/// 一页有界且带来源归属的期权候选结果。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionCatalogPage {
    source: MetadataSource,
    request_id: ReadRequestId,
    admission_provenance: crate::ReadAdmissionProvenance,
    candidates: Vec<OptionInstrumentCandidate>,
    next_cursor: Option<OpaquePageCursor>,
    received_at: UtcTimestamp,
}

/// Invalid catalog page returned by a provider adapter.
/// 供应商 adapter 返回的目录页无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionCatalogPageError {
    /// The page has more candidates than the requested bound.
    /// 目录页候选数量超过请求上限。
    TooManyCandidates,
}

impl OptionCatalogPage {
    /// Validate and retain one provider page without claiming complete catalog coverage.
    /// 校验并保留一个供应商目录页，但不宣称目录覆盖完整。
    ///
    /// # Errors
    ///
    /// Returns [`OptionCatalogPageError::TooManyCandidates`] when the provider returned
    /// more rows than the request allowed.
    ///
    /// # 错误
    ///
    /// 供应商返回行数超过请求上限时，返回
    /// [`OptionCatalogPageError::TooManyCandidates`]。
    pub fn new<A: ReadAdmissionEvidence>(
        query: &OptionCatalogQuery<A>,
        candidates: Vec<OptionInstrumentCandidate>,
        next_cursor: Option<OpaquePageCursor>,
        received_at: UtcTimestamp,
    ) -> Result<Self, OptionCatalogPageError> {
        if candidates.len() > usize::from(query.page_size().get()) {
            return Err(OptionCatalogPageError::TooManyCandidates);
        }
        Ok(Self {
            source: query.source().clone(),
            request_id: query.request_id().clone(),
            admission_provenance: query.admission().provenance().clone(),
            candidates,
            next_cursor,
            received_at,
        })
    }

    /// Return the source boundary that produced this page.
    /// 返回产生本页数据的来源边界。
    #[must_use]
    pub const fn source(&self) -> &MetadataSource {
        &self.source
    }

    /// Return the request correlation identifier used for this page.
    /// 返回本页使用的请求关联标识。
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

    /// Return candidate rows with known, unavailable, and unknown terms preserved.
    /// 返回保留已知、不可用和未知条款的候选行。
    #[must_use]
    pub fn candidates(&self) -> &[OptionInstrumentCandidate] {
        &self.candidates
    }

    /// Borrow the next source cursor, if the provider reported one.
    /// 若供应商报告了下一页游标，则借用该游标。
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&OpaquePageCursor> {
        self.next_cursor.as_ref()
    }

    /// Return when the local adapter received this page.
    /// 返回本地 adapter 收到本页的时间。
    #[must_use]
    pub const fn received_at(&self) -> &UtcTimestamp {
        &self.received_at
    }
}

/// Read-only, provider-neutral option contract catalog port.
/// 只读、供应商中立的期权合约目录端口。
///
/// The associated admission type must come from the composition root's trusted
/// shared read-budget owner. Cancellation of `list_options` does not prove the
/// provider did not receive or process a request. Results are page observations;
/// an absent next cursor is not by itself a provider completeness guarantee.
///
/// 关联准入类型必须来自系统装配根的可信共享读取预算 owner。取消 `list_options` 不证明供应商
/// 未收到或处理请求。结果只是分页观察；没有下一页游标本身不构成供应商完整性保证。
pub trait InstrumentCatalogPort: Send + Sync {
    /// Trusted runtime admission evidence required for every catalog query.
    /// 每次目录查询都必须携带的可信运行时准入证据。
    type Admission: ReadAdmissionEvidence;

    /// Return one bounded page of source-attributed option candidates.
    /// 返回一页有界且带来源归属的期权候选项。
    fn list_options(
        &self,
        query: OptionCatalogQuery<Self::Admission>,
    ) -> PortFuture<'_, Result<OptionCatalogPage, BrokerPortError>>;
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CATALOG_PAGE_SIZE, MAX_CURSOR_BYTES, OpaquePageCursor, OptionCatalogPage,
        OptionCatalogPageError, OptionCatalogQuery, OptionCatalogQueryError,
    };
    use crate::{
        ReadAdmissionEvidence, ReadAdmissionNamespace, ReadAdmissionProvenance, ReadRequestId,
    };
    use domain::{MarketDataProviderId, MetadataSource, Underlying};
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

    fn admission(source: MetadataSource) -> FakeAdmission {
        FakeAdmission {
            namespace: ReadAdmissionNamespace::InstrumentCatalog(source),
            provenance: ReadAdmissionProvenance::new("test-policy", "rev-1", "decision-1")
                .expect("synthetic provenance is valid"),
        }
    }

    #[test]
    fn query_preserves_admission_namespace_and_bounded_page_semantics() {
        let source = MetadataSource::MarketData(MarketDataProviderId::Alpaca);
        let query = OptionCatalogQuery::new(
            source.clone(),
            Underlying::new("QQQ").expect("valid synthetic underlying"),
            None,
            MAX_CATALOG_PAGE_SIZE,
            None,
            ReadRequestId::new("synthetic-request-1").expect("valid request ID"),
            admission(source.clone()),
        )
        .expect("matching admission namespace is required");
        assert_eq!(query.source(), &source);
        assert_eq!(query.page_size().get(), MAX_CATALOG_PAGE_SIZE);
        assert_eq!(query.admission().provenance().decision_id(), "decision-1");
    }

    #[test]
    fn query_rejects_wrong_namespace_and_out_of_range_page_sizes() {
        let source = MetadataSource::MarketData(MarketDataProviderId::Alpaca);
        let wrong_source = MetadataSource::MarketData(MarketDataProviderId::Schwab);
        let request_id = ReadRequestId::new("synthetic-request-2").expect("valid request ID");
        assert_eq!(
            OptionCatalogQuery::new(
                source.clone(),
                Underlying::new("QQQ").unwrap(),
                None,
                10,
                None,
                request_id.clone(),
                admission(wrong_source),
            )
            .err(),
            Some(OptionCatalogQueryError::AdmissionNamespaceMismatch)
        );
        for invalid in [0, MAX_CATALOG_PAGE_SIZE + 1] {
            assert_eq!(
                OptionCatalogQuery::new(
                    source.clone(),
                    Underlying::new("QQQ").unwrap(),
                    None,
                    invalid,
                    None,
                    request_id.clone(),
                    admission(source.clone()),
                )
                .err(),
                Some(OptionCatalogQueryError::InvalidPageSize)
            );
        }
    }

    #[test]
    fn provider_cursor_is_bounded_and_debug_redacted() {
        let cursor =
            OpaquePageCursor::new("synthetic-secret-cursor").expect("synthetic cursor is valid");
        assert_eq!(cursor.as_str(), "synthetic-secret-cursor");
        assert!(!format!("{cursor:?}").contains("synthetic-secret-cursor"));
        assert!(OpaquePageCursor::new("x".repeat(MAX_CURSOR_BYTES + 1)).is_err());
        assert!(OpaquePageCursor::new("bad\ncursor").is_err());
    }

    #[test]
    fn catalog_page_enforces_request_bound_and_keeps_receive_time() {
        let source = MetadataSource::MarketData(MarketDataProviderId::Alpaca);
        let timestamp = UtcTimestamp::parse("2026-10-08T14:30:00Z").unwrap();
        let query = OptionCatalogQuery::new(
            source.clone(),
            Underlying::new("QQQ").expect("valid synthetic underlying"),
            None,
            1,
            None,
            ReadRequestId::new("synthetic-page-request").unwrap(),
            admission(source.clone()),
        )
        .unwrap();
        let page = OptionCatalogPage::new(&query, vec![], None, timestamp.clone())
            .expect("empty page is a valid provider observation");
        assert_eq!(page.source(), &source);
        assert_eq!(page.request_id().as_str(), "synthetic-page-request");
        assert_eq!(page.admission_provenance().decision_id(), "decision-1");
        assert_eq!(page.received_at(), &timestamp);
        assert!(page.next_cursor().is_none());

        let too_many = vec![candidate(), candidate()];
        assert_eq!(
            OptionCatalogPage::new(&query, too_many, None, timestamp,),
            Err(OptionCatalogPageError::TooManyCandidates)
        );
    }

    fn candidate() -> domain::OptionInstrumentCandidate {
        domain::OptionInstrumentCandidate::from_symbol_candidate(
            domain::OptionSymbol::parse("QQQ   261218C00500000").unwrap(),
        )
    }
}
