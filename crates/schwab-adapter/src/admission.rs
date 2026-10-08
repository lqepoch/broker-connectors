//! Adapter bridge to the application's single trusted bounded-read owner.
//!
//! # 简体中文
//!
//! 将 Schwab SDK 的请求级 admission 回调桥接到应用唯一可信的有界读取 owner。

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker_ports::{PortFuture, ReadAdmissionEvidence, ReadRequestId};
use domain::AccountNamespace;
use schwab_sdk::{ReadAdmissionError, ReadAdmissionPort, ReadPriority};

/// One Schwab REST operation that must be covered by the shared read budget.
/// 必须由共享读取预算覆盖的一种 Schwab REST 操作。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SchwabReadOperation {
    /// Read one account summary.
    /// 读取一份账户摘要。
    AccountSummary,
    /// Read one account's positions.
    /// 读取一个账户的持仓。
    Positions,
    /// Read Streamer bootstrap metadata through `userPreference`.
    /// 通过 `userPreference` 读取 Streamer 启动元数据。
    StreamerBootstrap,
}

/// Application-owned interface to the one trusted, bounded read-admission owner.
///
/// The owner validates the supplied evidence and account namespace, applies its
/// existing policy and budget, and returns a permit that remains held for the
/// complete REST operation. Implementations must not create an adapter-local
/// quota or treat the evidence object itself as a permit.
///
/// 应用唯一可信、有界的读取准入 owner 接口。owner 必须校验来源证据和账户命名空间，复用既有策略与预算，
/// 并返回一个在整个 REST 操作期间持有的许可。本接口不创建 adapter 本地配额，也不把 evidence 当作许可。
pub trait SchwabReadAdmissionOwner: Send + Sync + 'static {
    /// Evidence type carried by shared broker-port requests.
    /// 共享 broker-port 请求携带的准入证据类型。
    type Evidence: ReadAdmissionEvidence + Clone + Send + Sync + 'static;

    /// Permit returned by the existing shared budget owner.
    /// 由既有共享预算 owner 返回的许可类型。
    type Permit: Send + 'static;

    /// Validates evidence and acquires one bounded permit for a fixed operation.
    /// 校验 evidence 并为固定操作获取一个有界许可。
    fn acquire<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
        evidence: &'a Self::Evidence,
        request_id: &'a ReadRequestId,
        operation: SchwabReadOperation,
        priority: ReadPriority,
        maximum_wait: Duration,
    ) -> PortFuture<'a, Result<Self::Permit, ReadAdmissionError>>;

    /// Passes bounded 429 metadata to that same shared owner.
    /// 将有界 429 元数据交给同一个共享 owner。
    fn observe_rate_limit_headers<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
        evidence: &'a Self::Evidence,
        request_id: &'a ReadRequestId,
        operation: SchwabReadOperation,
        values: &'a [&'a [u8]],
    ) -> PortFuture<'a, Result<(), ReadAdmissionError>>;
}

/// Per-request adapter presented to the source SDK's admission port.
pub(crate) struct AdmissionBridge<A: SchwabReadAdmissionOwner> {
    owner: Arc<A>,
    namespace: AccountNamespace,
    evidence: A::Evidence,
    request_id: ReadRequestId,
    operation: SchwabReadOperation,
    permit: Mutex<Option<A::Permit>>,
}

impl<A: SchwabReadAdmissionOwner> AdmissionBridge<A> {
    pub(crate) fn new(
        owner: Arc<A>,
        namespace: AccountNamespace,
        evidence: A::Evidence,
        request_id: ReadRequestId,
        operation: SchwabReadOperation,
    ) -> Self {
        Self {
            owner,
            namespace,
            evidence,
            request_id,
            operation,
            permit: Mutex::new(None),
        }
    }
}

impl<A: SchwabReadAdmissionOwner> ReadAdmissionPort for AdmissionBridge<A> {
    fn admit(
        &self,
        priority: ReadPriority,
        maximum_wait: Duration,
    ) -> schwab_sdk::BoxFuture<'_, Result<(), ReadAdmissionError>> {
        Box::pin(async move {
            let permit = self
                .owner
                .acquire(
                    &self.namespace,
                    &self.evidence,
                    &self.request_id,
                    self.operation,
                    priority,
                    maximum_wait,
                )
                .await?;
            let mut held = self
                .permit
                .lock()
                .map_err(|_| ReadAdmissionError::FailClosed)?;
            if held.is_some() {
                return Err(ReadAdmissionError::FailClosed);
            }
            *held = Some(permit);
            Ok(())
        })
    }

    fn observe_rate_limit_headers<'a>(
        &'a self,
        values: &'a [&'a [u8]],
    ) -> schwab_sdk::BoxFuture<'a, Result<(), ReadAdmissionError>> {
        Box::pin(async move {
            self.owner
                .observe_rate_limit_headers(
                    &self.namespace,
                    &self.evidence,
                    &self.request_id,
                    self.operation,
                    values,
                )
                .await
        })
    }
}

impl<A: SchwabReadAdmissionOwner> fmt::Debug for AdmissionBridge<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionBridge")
            .field("namespace", &"[REDACTED]")
            .field("evidence", &"[REDACTED]")
            .field("request_id", &self.request_id)
            .field("operation", &self.operation)
            .field("permit", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Shared token-provider wrapper that lets one adapter retain an injected owner.
pub(crate) struct SharedTokenProvider<P>(pub(crate) Arc<P>);

impl<P: schwab_sdk::AccessTokenProvider> schwab_sdk::AccessTokenProvider
    for SharedTokenProvider<P>
{
    fn access_token(
        &self,
    ) -> schwab_sdk::BoxFuture<'_, Result<schwab_sdk::AccessToken, schwab_sdk::TokenProviderError>>
    {
        self.0.access_token()
    }
}

/// Shared HTTP-transport wrapper that lets one adapter retain an injected owner.
pub(crate) struct SharedHttpTransport<T>(pub(crate) Arc<T>);

impl<T: schwab_sdk::HttpTransport> schwab_sdk::HttpTransport for SharedHttpTransport<T> {
    fn send(
        &self,
        request: schwab_sdk::HttpRequest,
    ) -> schwab_sdk::BoxFuture<'_, Result<schwab_sdk::HttpResponse, schwab_sdk::HttpTransportError>>
    {
        self.0.send(request)
    }
}
