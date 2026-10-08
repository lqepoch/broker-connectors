use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker_ports::{
    AccountNamespace, AccountReadRequest, BrokerEnvironment, BrokerReadError,
    BrokerReadPageRequest, BrokerReadPort, ExecutionBrokerId, PortFuture, ReadAdmissionEvidence,
    ReadAdmissionNamespace, ReadAdmissionProvenance, ReadRequestId,
};
use domain::{AccountScope, ExactDecimal};
use schwab_adapter::{
    SchwabAccountBinding, SchwabReadAdapter, SchwabReadAdmissionOwner, SchwabReadOperation,
};
use schwab_sdk::{
    AccessToken, AccessTokenProvider, BoxFuture, HttpMethod, HttpRequest, HttpResponse,
    HttpTransport, HttpTransportError, ReadAdmissionError, ReadPriority, RedirectPolicy,
    TokenProviderError,
};

const ACCOUNT_RESPONSE: &str = include_str!("fixtures/account_response.json");

#[derive(Clone)]
struct FakeEvidence {
    namespace: ReadAdmissionNamespace,
    provenance: ReadAdmissionProvenance,
}

impl FakeEvidence {
    fn new(namespace: &AccountNamespace) -> Self {
        Self {
            namespace: ReadAdmissionNamespace::Account(namespace.clone()),
            provenance: ReadAdmissionProvenance::new(
                "synthetic-read-policy",
                "synthetic-revision-1",
                "synthetic-decision-1",
            )
            .expect("synthetic provenance is valid"),
        }
    }
}

impl ReadAdmissionEvidence for FakeEvidence {
    fn namespace(&self) -> &ReadAdmissionNamespace {
        &self.namespace
    }

    fn provenance(&self) -> &ReadAdmissionProvenance {
        &self.provenance
    }
}

struct FakePermit(Arc<Mutex<usize>>);

impl Drop for FakePermit {
    fn drop(&mut self) {
        *self.0.lock().expect("fake permit counter lock") += 1;
    }
}

#[derive(Clone)]
struct FakeAdmissionOwner {
    calls: Arc<Mutex<Vec<(String, SchwabReadOperation, ReadPriority, Duration)>>>,
    observations: Arc<Mutex<usize>>,
    permit_drops: Arc<Mutex<usize>>,
    reject_requests: bool,
}

impl FakeAdmissionOwner {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            observations: Arc::new(Mutex::new(0)),
            permit_drops: Arc::new(Mutex::new(0)),
            reject_requests: false,
        }
    }

    fn rejecting() -> Self {
        Self {
            reject_requests: true,
            ..Self::new()
        }
    }
}

impl SchwabReadAdmissionOwner for FakeAdmissionOwner {
    type Evidence = FakeEvidence;
    type Permit = FakePermit;

    fn acquire<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
        evidence: &'a Self::Evidence,
        request_id: &'a ReadRequestId,
        operation: SchwabReadOperation,
        priority: ReadPriority,
        maximum_wait: Duration,
    ) -> PortFuture<'a, Result<Self::Permit, ReadAdmissionError>> {
        Box::pin(async move {
            assert_eq!(
                evidence.namespace(),
                &ReadAdmissionNamespace::Account(namespace.clone())
            );
            self.calls.lock().expect("fake owner call lock").push((
                request_id.as_str().to_owned(),
                operation,
                priority,
                maximum_wait,
            ));
            if self.reject_requests {
                return Err(ReadAdmissionError::PolicyRejected);
            }
            Ok(FakePermit(Arc::clone(&self.permit_drops)))
        })
    }

    fn observe_rate_limit_headers<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
        evidence: &'a Self::Evidence,
        _request_id: &'a ReadRequestId,
        _operation: SchwabReadOperation,
        values: &'a [&'a [u8]],
    ) -> PortFuture<'a, Result<(), ReadAdmissionError>> {
        Box::pin(async move {
            assert_eq!(
                evidence.namespace(),
                &ReadAdmissionNamespace::Account(namespace.clone())
            );
            assert!(values.iter().any(|value| *value == b"2"));
            *self
                .observations
                .lock()
                .expect("fake observation counter lock") += 1;
            Ok(())
        })
    }
}

struct FakeTokenProvider;

impl AccessTokenProvider for FakeTokenProvider {
    fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>> {
        Box::pin(async { AccessToken::new("synthetic-access-token") })
    }
}

#[derive(Clone)]
struct FakeTransport {
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeTransport {
    fn account_response() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: ACCOUNT_RESPONSE.as_bytes().to_vec(),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn rate_limited() -> Self {
        Self {
            status: 429,
            headers: vec![("retry-after".to_owned(), b"2".to_vec())],
            body: b"{}".to_vec(),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl HttpTransport for FakeTransport {
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
        Box::pin(async move {
            assert_eq!(request.method(), HttpMethod::Get);
            assert_eq!(request.redirect_policy(), RedirectPolicy::Disabled);
            assert!(!request.timeout().is_zero());
            request.with_bearer_token(|token| assert_eq!(token, "synthetic-access-token"));
            self.calls
                .lock()
                .expect("fake transport call lock")
                .push(request.endpoint().route_name().to_owned());
            if self.status == 429 {
                let retry_after = self
                    .headers
                    .iter()
                    .filter(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                    .map(|(_, value)| value.as_slice())
                    .collect::<Vec<_>>();
                request
                    .observe_response_head(self.status, &retry_after)
                    .await
                    .map_err(|_| HttpTransportError::RateLimitObservation)?;
            }
            HttpResponse::new(self.status, self.headers.clone(), self.body.clone())
                .map_err(|_| HttpTransportError::InvalidResponse)
        })
    }
}

fn namespace(account: &str) -> AccountNamespace {
    AccountNamespace::new(
        ExecutionBrokerId::Schwab,
        BrokerEnvironment::Paper,
        AccountScope::new(account).expect("synthetic local account scope is valid"),
    )
}

fn adapter(
    namespace: &AccountNamespace,
    transport: Arc<FakeTransport>,
    owner: Arc<FakeAdmissionOwner>,
) -> SchwabReadAdapter<FakeTokenProvider, FakeTransport, FakeAdmissionOwner> {
    SchwabReadAdapter::new(
        SchwabAccountBinding::new(namespace.clone(), "synthetic-account-hash")
            .expect("synthetic binding is valid"),
        Arc::new(FakeTokenProvider),
        transport,
        owner,
    )
}

fn request(namespace: &AccountNamespace) -> AccountReadRequest<FakeEvidence> {
    AccountReadRequest::new(
        namespace.clone(),
        ReadRequestId::new("synthetic-read-001").expect("synthetic request ID is valid"),
        FakeEvidence::new(namespace),
    )
    .expect("synthetic admission scope matches request")
}

#[tokio::test]
async fn account_read_uses_sdk_get_and_preserves_exact_balances_without_exposing_raw_dto() {
    let namespace = namespace("synthetic-account-scope");
    let transport = Arc::new(FakeTransport::account_response());
    let owner = Arc::new(FakeAdmissionOwner::new());
    let adapter = adapter(&namespace, Arc::clone(&transport), Arc::clone(&owner));

    let result = adapter
        .read_account(request(&namespace))
        .await
        .expect("synthetic account response is projected");

    assert_eq!(result.namespace(), &namespace);
    assert_eq!(result.value().account_type(), Some("CASH"));
    assert_eq!(
        *result
            .value()
            .current_balances()
            .unwrap()
            .values()
            .get("cashBalance")
            .expect("synthetic cash balance is present"),
        ExactDecimal::parse_json_number("100.2500").expect("exact decimal fixture")
    );
    assert_eq!(result.evidence().broker(), &ExecutionBrokerId::Schwab);
    assert_eq!(
        transport.calls.lock().expect("transport calls").as_slice(),
        &[String::from("trader-account")]
    );
    assert_eq!(owner.calls.lock().expect("owner calls").len(), 1);
    assert_eq!(*owner.permit_drops.lock().expect("permit drops"), 1);
    let debug = format!("{:?}", result.value());
    assert!(!debug.contains("100.2500"));
    assert!(!debug.contains("SYNTHETIC-ACCOUNT-001"));
}

#[tokio::test]
async fn positions_use_the_same_account_route_and_reject_unknown_pagination() {
    let namespace = namespace("synthetic-account-scope");
    let transport = Arc::new(FakeTransport::account_response());
    let owner = Arc::new(FakeAdmissionOwner::new());
    let adapter = adapter(&namespace, Arc::clone(&transport), Arc::clone(&owner));
    let page_request = BrokerReadPageRequest::new(request(&namespace), 1, None)
        .expect("one synthetic position fits the bounded request");

    let page = adapter
        .read_positions(page_request)
        .await
        .expect("synthetic positions are projected");

    assert_eq!(page.rows().len(), 1);
    assert_eq!(
        page.rows()[0].instrument().unwrap().symbol(),
        Some("SYNTHETIC-QQQ-CALL")
    );
    assert_eq!(
        page.rows()[0].market_value(),
        Some(ExactDecimal::parse_json_number("26.9000").expect("exact decimal fixture"))
    );
    assert!(page.next_cursor().is_none());
    assert_eq!(
        transport.calls.lock().expect("transport calls").as_slice(),
        &[String::from("trader-account")]
    );
    assert_eq!(
        owner.calls.lock().expect("owner calls")[0].1,
        SchwabReadOperation::Positions
    );
}

#[tokio::test]
async fn unsupported_order_and_fill_reads_do_not_acquire_budget_or_send_requests() {
    let namespace = namespace("synthetic-account-scope");
    let transport = Arc::new(FakeTransport::account_response());
    let owner = Arc::new(FakeAdmissionOwner::new());
    let adapter = adapter(&namespace, Arc::clone(&transport), Arc::clone(&owner));
    let open_orders = BrokerReadPageRequest::new(request(&namespace), 10, None)
        .expect("synthetic request bound is valid");
    let fills = BrokerReadPageRequest::new(request(&namespace), 10, None)
        .expect("synthetic request bound is valid");

    assert_eq!(
        adapter.read_open_orders(open_orders).await,
        Err(BrokerReadError::Unsupported)
    );
    assert_eq!(
        adapter.read_fills(fills).await,
        Err(BrokerReadError::Unsupported)
    );
    assert!(transport.calls.lock().expect("transport calls").is_empty());
    assert!(owner.calls.lock().expect("owner calls").is_empty());
}

#[tokio::test]
async fn namespace_mismatch_fails_before_admission_or_transport() {
    let bound_namespace = namespace("synthetic-bound-account");
    let other_namespace = namespace("synthetic-other-account");
    let transport = Arc::new(FakeTransport::account_response());
    let owner = Arc::new(FakeAdmissionOwner::new());
    let adapter = adapter(&bound_namespace, Arc::clone(&transport), Arc::clone(&owner));

    assert!(matches!(
        adapter.read_account(request(&other_namespace)).await,
        Err(BrokerReadError::NamespaceMismatch)
    ));
    assert!(transport.calls.lock().expect("transport calls").is_empty());
    assert!(owner.calls.lock().expect("owner calls").is_empty());
}

#[tokio::test]
async fn shared_admission_rejection_fails_before_transport_without_a_fallback_permit() {
    let namespace = namespace("synthetic-account-scope");
    let transport = Arc::new(FakeTransport::account_response());
    let owner = Arc::new(FakeAdmissionOwner::rejecting());
    let adapter = adapter(&namespace, Arc::clone(&transport), Arc::clone(&owner));

    assert!(matches!(
        adapter.read_account(request(&namespace)).await,
        Err(BrokerReadError::Unauthorized)
    ));
    assert_eq!(owner.calls.lock().expect("owner calls").len(), 1);
    assert_eq!(*owner.permit_drops.lock().expect("permit drops"), 0);
    assert!(transport.calls.lock().expect("transport calls").is_empty());
}

#[tokio::test]
async fn rate_limit_metadata_returns_to_the_same_shared_owner() {
    let namespace = namespace("synthetic-account-scope");
    let transport = Arc::new(FakeTransport::rate_limited());
    let owner = Arc::new(FakeAdmissionOwner::new());
    let adapter = adapter(&namespace, transport, Arc::clone(&owner));

    assert!(matches!(
        adapter.read_account(request(&namespace)).await,
        Err(BrokerReadError::RateLimited)
    ));
    assert_eq!(*owner.observations.lock().expect("429 observations"), 1);
    assert_eq!(*owner.permit_drops.lock().expect("permit drops"), 1);
}
