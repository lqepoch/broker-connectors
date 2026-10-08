//! Schwab's read-only account-port implementation.
//!
//! # 简体中文
//!
//! 通过 vendored `schwab-sdk` 实现只读账户和单页持仓读取。

use broker_ports::{
    AccountReadRequest, BrokerReadError, BrokerReadPage, BrokerReadPageRequest, BrokerReadPort,
    ObservedRead, PortFuture, ReadAdmissionEvidence, ReadAdmissionNamespace, ReadEvidence,
    ReadRequestId,
};
use chrono::{SecondsFormat, Utc};
use domain::{AccountNamespace, ExactDecimal, ExecutionBrokerId};
use market_contracts::UtcTimestamp;
use schwab_sdk::{
    AccessTokenProvider, AccountResponse, AccountsQuery, BalanceSnapshot, HttpTransport, QueryText,
    ReadAdmissionError, ReadApiError, ReadRequestError, RestError, SchwabSdk, SecuritiesAccount,
    TraderReadResponse, TypedReadResponse, WireNumber,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use zeroize::Zeroizing;

use crate::admission::{
    AdmissionBridge, SchwabReadAdmissionOwner, SchwabReadOperation, SharedHttpTransport,
    SharedTokenProvider,
};

/// Explicit Schwab account namespace and opaque account-hash route binding.
///
/// The namespace is descriptive and grants no read or execution authority. The
/// hash must come from the application's trusted account-binding flow; it is
/// held in a zeroizing buffer and omitted from diagnostics.
///
/// 显式 Schwab 账户命名空间与不透明账户 hash 路由绑定。命名空间仅用于描述，不授予读取或执行权限。hash
/// 必须来自应用可信账户绑定流程，保存在清零缓冲区中且不会进入诊断输出。
pub struct SchwabAccountBinding {
    namespace: AccountNamespace,
    account_hash: Zeroizing<String>,
}

impl SchwabAccountBinding {
    /// Validates and binds one Schwab account namespace to its broker hash.
    /// 校验并将 Schwab 账户命名空间绑定到 broker hash。
    ///
    /// # Errors
    /// Returns [`SchwabAccountBindingError::WrongBroker`] when the namespace is
    /// not scoped to Schwab, or [`SchwabAccountBindingError::InvalidAccountHash`]
    /// when the opaque route identifier is malformed or outside its bounds.
    pub fn new(
        namespace: AccountNamespace,
        account_hash: impl Into<String>,
    ) -> Result<Self, SchwabAccountBindingError> {
        if namespace.broker() != &ExecutionBrokerId::Schwab {
            return Err(SchwabAccountBindingError::WrongBroker);
        }
        let account_hash = Zeroizing::new(account_hash.into());
        let value = account_hash.as_str();
        if value.is_empty()
            || value.trim() != value
            || value.len() > 256
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(SchwabAccountBindingError::InvalidAccountHash);
        }
        // Reuse the source SDK's path-segment validation. It only validates the
        // opaque identifier; it does not grant account access.
        schwab_sdk::PathIdentifier::new(value)
            .map_err(|_| SchwabAccountBindingError::InvalidAccountHash)?;
        Ok(Self {
            namespace,
            account_hash,
        })
    }

    /// Return the descriptive broker, environment, and local account scope.
    /// 返回描述性的券商、环境和本地账户 scope。
    #[must_use]
    pub const fn namespace(&self) -> &AccountNamespace {
        &self.namespace
    }
}

impl fmt::Debug for SchwabAccountBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabAccountBinding")
            .field("namespace", &"[REDACTED]")
            .field("account_hash", &"[REDACTED]")
            .finish()
    }
}

/// Invalid explicit Schwab account binding.
/// 显式 Schwab 账户绑定无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabAccountBindingError {
    /// The namespace names another execution broker.
    /// 命名空间指向其他执行券商。
    WrongBroker,
    /// The opaque account hash is empty, padded, oversized, or contains controls.
    /// 不透明账户 hash 为空、带首尾空白、过长或包含控制字符。
    InvalidAccountHash,
}

/// Exact Schwab balance values projected without floating-point conversion.
/// 不经浮点转换而投影的 Schwab 精确余额。
pub struct SchwabBalanceSnapshot {
    values: BTreeMap<String, ExactDecimal>,
}

impl SchwabBalanceSnapshot {
    /// Borrow exact balance values by their Schwab field names.
    /// 按 Schwab 字段名借用精确余额。
    #[must_use]
    pub const fn values(&self) -> &BTreeMap<String, ExactDecimal> {
        &self.values
    }
}

impl fmt::Debug for SchwabBalanceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabBalanceSnapshot")
            .field("value_count", &self.values.len())
            .field("values", &"[REDACTED]")
            .finish()
    }
}

/// Syntactically validated account observation; it is not account-state authority.
/// 通过结构校验的账户观察；它不构成账户状态 authority。
pub struct SchwabAccountSummary {
    account_type: Option<String>,
    round_trips: Option<ExactDecimal>,
    is_day_trader: Option<bool>,
    is_closing_only_restricted: Option<bool>,
    pfcb_flag: Option<bool>,
    initial_balances: Option<SchwabBalanceSnapshot>,
    current_balances: Option<SchwabBalanceSnapshot>,
    projected_balances: Option<SchwabBalanceSnapshot>,
}

impl SchwabAccountSummary {
    /// Return the broker-reported account type when present.
    /// 若存在则返回 broker 报告的账户类型。
    #[must_use]
    pub fn account_type(&self) -> Option<&str> {
        self.account_type.as_deref()
    }

    /// Return the exact reported round-trip count when present.
    /// 若存在则返回精确的已报告往返次数。
    #[must_use]
    pub const fn round_trips(&self) -> Option<ExactDecimal> {
        self.round_trips
    }

    /// Return the broker-reported day-trader flag without inferring missing values.
    /// 返回 broker 报告的日内交易者标记，不推断缺失值。
    #[must_use]
    pub const fn is_day_trader(&self) -> Option<bool> {
        self.is_day_trader
    }

    /// Return the broker-reported closing-only restriction flag.
    /// 返回 broker 报告的仅可平仓限制标记。
    #[must_use]
    pub const fn is_closing_only_restricted(&self) -> Option<bool> {
        self.is_closing_only_restricted
    }

    /// Return the broker-reported portfolio-cash flag.
    /// 返回 broker 报告的 portfolio-cash 标记。
    #[must_use]
    pub const fn pfcb_flag(&self) -> Option<bool> {
        self.pfcb_flag
    }

    /// Return initial balance observations when supplied.
    /// 若 broker 提供则返回初始余额观察。
    #[must_use]
    pub const fn initial_balances(&self) -> Option<&SchwabBalanceSnapshot> {
        self.initial_balances.as_ref()
    }

    /// Return current balance observations when supplied.
    /// 若 broker 提供则返回当前余额观察。
    #[must_use]
    pub const fn current_balances(&self) -> Option<&SchwabBalanceSnapshot> {
        self.current_balances.as_ref()
    }

    /// Return projected balance observations when supplied.
    /// 若 broker 提供则返回预测余额观察。
    #[must_use]
    pub const fn projected_balances(&self) -> Option<&SchwabBalanceSnapshot> {
        self.projected_balances.as_ref()
    }
}

impl fmt::Debug for SchwabAccountSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabAccountSummary")
            .field(
                "account_type",
                &self.account_type.as_ref().map(|_| "[REDACTED]"),
            )
            .field("round_trips", &self.round_trips.map(|_| "[REDACTED]"))
            .field("is_day_trader", &self.is_day_trader.is_some())
            .field(
                "is_closing_only_restricted",
                &self.is_closing_only_restricted.is_some(),
            )
            .field("pfcb_flag", &self.pfcb_flag.is_some())
            .field("initial_balances", &self.initial_balances.is_some())
            .field("current_balances", &self.current_balances.is_some())
            .field("projected_balances", &self.projected_balances.is_some())
            .finish()
    }
}

/// Position instrument identifiers retained as provider-labelled observations.
/// 以 provider 标签保留的持仓标的标识观察。
pub struct SchwabPositionInstrument {
    asset_type: Option<String>,
    symbol: Option<String>,
    instrument_type: Option<String>,
}

impl SchwabPositionInstrument {
    /// Return the provider-reported asset family when present.
    /// 若存在则返回 provider 报告的资产类别。
    #[must_use]
    pub fn asset_type(&self) -> Option<&str> {
        self.asset_type.as_deref()
    }

    /// Return the provider symbol when present.
    /// 若存在则返回 provider 报告的 symbol。
    #[must_use]
    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    /// Return the provider instrument type when present.
    /// 若存在则返回 provider 报告的标的类型。
    #[must_use]
    pub fn instrument_type(&self) -> Option<&str> {
        self.instrument_type.as_deref()
    }
}

impl fmt::Debug for SchwabPositionInstrument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPositionInstrument")
            .field("asset_type", &self.asset_type.is_some())
            .field("symbol", &self.symbol.as_ref().map(|_| "[REDACTED]"))
            .field("instrument_type", &self.instrument_type.is_some())
            .finish()
    }
}

/// Exact position values projected from one account response.
/// 从一份账户响应投影出的精确持仓值。
pub struct SchwabPosition {
    instrument: Option<SchwabPositionInstrument>,
    long_quantity: Option<ExactDecimal>,
    short_quantity: Option<ExactDecimal>,
    average_price: Option<ExactDecimal>,
    market_value: Option<ExactDecimal>,
    current_day_profit_loss: Option<ExactDecimal>,
}

impl SchwabPosition {
    /// Return provider-labelled instrument identity when present.
    /// 若存在则返回带 provider 标签的标的身份。
    #[must_use]
    pub const fn instrument(&self) -> Option<&SchwabPositionInstrument> {
        self.instrument.as_ref()
    }

    /// Return the reported long quantity without numeric coercion.
    /// 返回无数值强制转换的已报告多头数量。
    #[must_use]
    pub const fn long_quantity(&self) -> Option<ExactDecimal> {
        self.long_quantity
    }

    /// Return the reported short quantity without numeric coercion.
    /// 返回无数值强制转换的已报告空头数量。
    #[must_use]
    pub const fn short_quantity(&self) -> Option<ExactDecimal> {
        self.short_quantity
    }

    /// Return the reported average price without numeric coercion.
    /// 返回无数值强制转换的已报告平均价格。
    #[must_use]
    pub const fn average_price(&self) -> Option<ExactDecimal> {
        self.average_price
    }

    /// Return the reported market value without numeric coercion.
    /// 返回无数值强制转换的已报告市场价值。
    #[must_use]
    pub const fn market_value(&self) -> Option<ExactDecimal> {
        self.market_value
    }

    /// Return the reported current-day profit/loss without numeric coercion.
    /// 返回无数值强制转换的已报告当日盈亏。
    #[must_use]
    pub const fn current_day_profit_loss(&self) -> Option<ExactDecimal> {
        self.current_day_profit_loss
    }
}

impl fmt::Debug for SchwabPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPosition")
            .field("instrument", &self.instrument.is_some())
            .field("long_quantity", &self.long_quantity.is_some())
            .field("short_quantity", &self.short_quantity.is_some())
            .field("average_price", &self.average_price.is_some())
            .field("market_value", &self.market_value.is_some())
            .field(
                "current_day_profit_loss",
                &self.current_day_profit_loss.is_some(),
            )
            .finish()
    }
}

/// Read-only Schwab adapter over the extracted SDK and shared broker ports.
/// 通过提取的 SDK 和共享 broker-port 提供只读 Schwab adapter。
pub struct SchwabReadAdapter<P, T, A>
where
    A: SchwabReadAdmissionOwner,
{
    binding: SchwabAccountBinding,
    token_provider: Arc<P>,
    transport: Arc<T>,
    admission_owner: Arc<A>,
}

impl<P, T, A> SchwabReadAdapter<P, T, A>
where
    P: AccessTokenProvider + 'static,
    T: HttpTransport + 'static,
    A: SchwabReadAdmissionOwner,
{
    /// Creates the adapter with an explicit account binding and shared admission owner.
    /// 使用显式账户绑定与共享准入 owner 创建 adapter。
    #[must_use]
    pub fn new(
        binding: SchwabAccountBinding,
        token_provider: Arc<P>,
        transport: Arc<T>,
        admission_owner: Arc<A>,
    ) -> Self {
        Self {
            binding,
            token_provider,
            transport,
            admission_owner,
        }
    }

    fn sdk(
        &self,
        read_admission: Arc<AdmissionBridge<A>>,
    ) -> SchwabSdk<SharedTokenProvider<P>, SharedHttpTransport<T>> {
        let read_admission: Arc<dyn schwab_sdk::ReadAdmissionPort> = read_admission;
        SchwabSdk::builder(
            SharedTokenProvider(Arc::clone(&self.token_provider)),
            SharedHttpTransport(Arc::clone(&self.transport)),
            read_admission,
        )
        .build()
    }

    fn request_bridge(
        &self,
        evidence: &A::Evidence,
        request_id: &ReadRequestId,
        operation: SchwabReadOperation,
    ) -> Result<Arc<AdmissionBridge<A>>, BrokerReadError> {
        if evidence.namespace() != &ReadAdmissionNamespace::Account(self.binding.namespace.clone())
        {
            return Err(BrokerReadError::NamespaceMismatch);
        }
        Ok(Arc::new(AdmissionBridge::new(
            Arc::clone(&self.admission_owner),
            self.binding.namespace.clone(),
            evidence.clone(),
            request_id.clone(),
            operation,
        )))
    }

    fn ensure_request_namespace(
        &self,
        namespace: &AccountNamespace,
        evidence: &A::Evidence,
    ) -> Result<(), BrokerReadError> {
        if namespace != &self.binding.namespace {
            return Err(BrokerReadError::NamespaceMismatch);
        }
        if evidence.namespace() != &ReadAdmissionNamespace::Account(self.binding.namespace.clone())
        {
            return Err(BrokerReadError::NamespaceMismatch);
        }
        Ok(())
    }
}

impl<P, T, A> fmt::Debug for SchwabReadAdapter<P, T, A>
where
    A: SchwabReadAdmissionOwner,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabReadAdapter")
            .field("binding", &self.binding)
            .field("token_provider", &"[REDACTED]")
            .field("transport", &"[REDACTED]")
            .field("admission_owner", &"[REDACTED]")
            .finish()
    }
}

impl<P, T, A> BrokerReadPort for SchwabReadAdapter<P, T, A>
where
    P: AccessTokenProvider + 'static,
    T: HttpTransport + 'static,
    A: SchwabReadAdmissionOwner,
{
    type Admission = A::Evidence;
    type AccountSummary = SchwabAccountSummary;
    type Position = SchwabPosition;
    type OpenOrder = ();
    type Fill = ();

    fn account_namespace(&self) -> &AccountNamespace {
        &self.binding.namespace
    }

    fn read_account(
        &self,
        request: AccountReadRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<ObservedRead<Self::AccountSummary>, BrokerReadError>> {
        Box::pin(async move {
            self.ensure_request_namespace(request.namespace(), request.admission())?;
            let bridge = self.request_bridge(
                request.admission(),
                request.request_id(),
                SchwabReadOperation::AccountSummary,
            )?;
            let sdk = self.sdk(bridge);
            let response = sdk
                .trader()
                .account(self.binding.account_hash.as_str(), AccountsQuery::default())
                .await
                .map_err(map_read_error)?;
            let account = account_model(&response)?;
            let summary = project_account_summary(&account.securities_account)?;
            let evidence = read_evidence()?;
            ObservedRead::new(&request, summary, evidence)
        })
    }

    fn read_positions(
        &self,
        request: BrokerReadPageRequest<Self::Admission>,
    ) -> PortFuture<'_, Result<BrokerReadPage<Self::Position>, BrokerReadError>> {
        Box::pin(async move {
            self.ensure_request_namespace(request.base().namespace(), request.base().admission())?;
            if request.cursor().is_some() {
                return Err(BrokerReadError::Unsupported);
            }
            let bridge = self.request_bridge(
                request.base().admission(),
                request.base().request_id(),
                SchwabReadOperation::Positions,
            )?;
            let query = AccountsQuery {
                fields: Some(QueryText::new("positions").map_err(map_request_error)?),
            };
            let sdk = self.sdk(bridge);
            let response = sdk
                .trader()
                .account(self.binding.account_hash.as_str(), query)
                .await
                .map_err(map_read_error)?;
            let account = account_model(&response)?;
            let source_positions = account
                .securities_account
                .positions
                .as_ref()
                .ok_or(BrokerReadError::InvalidResponse)?;
            if source_positions.len() > usize::from(request.page_size()) {
                return Err(BrokerReadError::InvalidResponse);
            }
            let rows = source_positions
                .iter()
                .map(project_position)
                .collect::<Result<Vec<_>, _>>()?;
            let evidence = read_evidence()?;
            BrokerReadPage::new(&request, rows, None, evidence)
                .map_err(|_| BrokerReadError::InvalidResponse)
        })
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

fn account_model(response: &TypedReadResponse) -> Result<&AccountResponse, BrokerReadError> {
    match response.parsed().trader_model() {
        Some(TraderReadResponse::Account(account)) => Ok(account),
        _ => Err(BrokerReadError::InvalidResponse),
    }
}

fn project_account_summary(
    account: &SecuritiesAccount,
) -> Result<SchwabAccountSummary, BrokerReadError> {
    Ok(SchwabAccountSummary {
        account_type: account.account_type.clone(),
        round_trips: project_wire_number(account.round_trips.as_ref())?,
        is_day_trader: account.is_day_trader,
        is_closing_only_restricted: account.is_closing_only_restricted,
        pfcb_flag: account.pfcb_flag,
        initial_balances: project_balance(account.initial_balances.as_ref())?,
        current_balances: project_balance(account.current_balances.as_ref())?,
        projected_balances: project_balance(account.projected_balances.as_ref())?,
    })
}

fn project_position(position: &schwab_sdk::Position) -> Result<SchwabPosition, BrokerReadError> {
    Ok(SchwabPosition {
        instrument: position
            .instrument
            .as_ref()
            .map(|instrument| SchwabPositionInstrument {
                asset_type: instrument.asset_type.clone(),
                symbol: instrument.symbol.clone(),
                instrument_type: instrument.instrument_type.clone(),
            }),
        long_quantity: project_wire_number(position.long_quantity.as_ref())?,
        short_quantity: project_wire_number(position.short_quantity.as_ref())?,
        average_price: project_wire_number(position.average_price.as_ref())?,
        market_value: project_wire_number(position.market_value.as_ref())?,
        current_day_profit_loss: project_wire_number(position.current_day_profit_loss.as_ref())?,
    })
}

fn project_balance(
    balance: Option<&BalanceSnapshot>,
) -> Result<Option<SchwabBalanceSnapshot>, BrokerReadError> {
    balance
        .map(|balance| {
            balance
                .values()
                .map(|(key, value)| {
                    Ok((
                        key.to_owned(),
                        ExactDecimal::parse_json_number(value.as_str())
                            .map_err(|_| BrokerReadError::InvalidResponse)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, BrokerReadError>>()
                .map(|values| SchwabBalanceSnapshot { values })
        })
        .transpose()
}

fn project_wire_number(
    value: Option<&WireNumber>,
) -> Result<Option<ExactDecimal>, BrokerReadError> {
    value
        .map(|value| {
            ExactDecimal::parse_json_number(value.as_str())
                .map_err(|_| BrokerReadError::InvalidResponse)
        })
        .transpose()
}

fn read_evidence() -> Result<ReadEvidence, BrokerReadError> {
    let received_at = chrono::DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(SecondsFormat::Nanos, true);
    let received_at =
        UtcTimestamp::parse(&received_at).map_err(|_| BrokerReadError::Unavailable)?;
    Ok(ReadEvidence::new(
        ExecutionBrokerId::Schwab,
        None,
        None,
        received_at,
    ))
}

fn map_request_error(_error: ReadRequestError) -> BrokerReadError {
    BrokerReadError::InvalidRequest
}

fn map_read_error(error: ReadApiError) -> BrokerReadError {
    match error {
        ReadApiError::Request(_) | ReadApiError::Rest(RestError::Request(_)) => {
            BrokerReadError::InvalidRequest
        }
        ReadApiError::Response { .. } => BrokerReadError::InvalidResponse,
        ReadApiError::Rest(RestError::Admission(error)) => map_admission_error(error),
        ReadApiError::Rest(RestError::Token(_)) => BrokerReadError::Unauthorized,
        ReadApiError::Rest(RestError::Transport(error)) => match error {
            schwab_sdk::HttpTransportError::Timeout => BrokerReadError::DeadlineExceeded,
            _ => BrokerReadError::Unavailable,
        },
        ReadApiError::Rest(RestError::HttpStatus(response)) => match response.status() {
            401 | 403 => BrokerReadError::Unauthorized,
            408 | 504 => BrokerReadError::DeadlineExceeded,
            429 => BrokerReadError::RateLimited,
            500..=599 => BrokerReadError::Unavailable,
            _ => BrokerReadError::ProviderRejected,
        },
    }
}

fn map_admission_error(error: ReadAdmissionError) -> BrokerReadError {
    match error {
        ReadAdmissionError::QueueFull => BrokerReadError::Overloaded,
        ReadAdmissionError::PolicyRejected => BrokerReadError::Unauthorized,
        ReadAdmissionError::FailClosed | ReadAdmissionError::RuntimeUnavailable => {
            BrokerReadError::Unavailable
        }
        ReadAdmissionError::RequestDeadlineExpired => BrokerReadError::DeadlineExceeded,
    }
}
