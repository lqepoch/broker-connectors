//! Offline checks for the public read-only Schwab SDK facade.
//!
//! # 简体中文
//!
//! Schwab 只读 SDK facade 的离线检查。

#![forbid(unsafe_code)]

use schwab_sdk::{
    AccessToken, AccessTokenProvider, BoxFuture, HttpMethod, HttpRequest, HttpResponse,
    HttpTransport, HttpTransportError, ReadAdmissionError, ReadAdmissionPort, ReadPriority,
    SchwabHttpsTransport, SchwabSdk, TokenProviderError,
};
use schwab_sdk::{CsvValues, QuotesQuery};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TOKEN: &str = "synthetic-sdk-token-never-a-credential";

fn assert_http_transport<T: HttpTransport>() {}

#[test]
fn reexported_https_transport_implements_the_port_without_network_access() {
    assert_http_transport::<SchwabHttpsTransport>();
}

struct FakeTokenProvider {
    calls: Arc<AtomicUsize>,
}

impl AccessTokenProvider for FakeTokenProvider {
    fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { AccessToken::new(TOKEN) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedRequest {
    method: HttpMethod,
    route: &'static str,
    token_matches: bool,
}

struct FakeTransport {
    calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<ObservedRequest>>>,
    status: u16,
    body: Vec<u8>,
}

impl HttpTransport for FakeTransport {
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed
            .lock()
            .expect("synthetic observed-request lock")
            .push(ObservedRequest {
                method: request.method(),
                route: request.endpoint().route_name(),
                token_matches: request.with_bearer_token(|token| token == TOKEN),
            });

        let status = self.status;
        let body = self.body.clone();
        Box::pin(async move {
            let response = HttpResponse::new(
                status,
                vec![
                    ("Retry-After".to_owned(), b"9".to_vec()),
                    ("X-Synthetic-Metadata".to_owned(), b"preserved".to_vec()),
                ],
                body,
            )
            .map_err(|_| HttpTransportError::InvalidResponse)?;
            request
                .observe_response_head(status, &[b"9"])
                .await
                .map_err(|_| HttpTransportError::RateLimitObservation)?;
            Ok(response)
        })
    }
}

struct TestSdk {
    sdk: SchwabSdk<FakeTokenProvider, FakeTransport>,
    token_calls: Arc<AtomicUsize>,
    transport_calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<ObservedRequest>>>,
}

struct AllowAdmission;

impl ReadAdmissionPort for AllowAdmission {
    fn admit(
        &self,
        _priority: ReadPriority,
        _maximum_wait: std::time::Duration,
    ) -> BoxFuture<'_, Result<(), ReadAdmissionError>> {
        Box::pin(async { Ok(()) })
    }

    fn observe_rate_limit_headers<'a>(
        &'a self,
        _values: &'a [&'a [u8]],
    ) -> BoxFuture<'a, Result<(), ReadAdmissionError>> {
        Box::pin(async { Ok(()) })
    }
}

fn test_sdk(status: u16, body: &str) -> TestSdk {
    let token_calls = Arc::new(AtomicUsize::new(0));
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let admission = Arc::new(AllowAdmission);
    let sdk = SchwabSdk::builder(
        FakeTokenProvider {
            calls: Arc::clone(&token_calls),
        },
        FakeTransport {
            calls: Arc::clone(&transport_calls),
            observed: Arc::clone(&observed),
            status,
            body: body.as_bytes().to_vec(),
        },
        admission,
    )
    .build();

    TestSdk {
        sdk,
        token_calls,
        transport_calls,
        observed,
    }
}

fn assert_one_get(test_sdk: &TestSdk, expected_route: &'static str) {
    assert_eq!(test_sdk.token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(test_sdk.transport_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        test_sdk
            .observed
            .lock()
            .expect("synthetic observed-request lock")
            .as_slice(),
        &[ObservedRequest {
            method: HttpMethod::Get,
            route: expected_route,
            token_matches: true,
        }]
    );
}

#[tokio::test]
async fn trader_facade_delegates_typed_get_and_keeps_response_metadata() {
    let test_sdk = test_sdk(
        200,
        r#"[{"accountNumber":"synthetic-account","hashValue":"synthetic-hash"}]"#,
    );

    let result = test_sdk
        .sdk
        .trader()
        .account_numbers()
        .await
        .expect("synthetic Trader response validates");

    assert_one_get(&test_sdk, "trader-account-numbers");
    assert_eq!(
        result.response().endpoint().route_name(),
        "trader-account-numbers"
    );
    assert_eq!(result.response().status(), 200);
    assert!(
        result
            .response()
            .headers()
            .any(|(name, value)| { name == "x-synthetic-metadata" && value == b"preserved" })
    );
}

#[tokio::test]
async fn market_data_facade_delegates_normalized_get_and_preserves_metadata() {
    let test_sdk = test_sdk(
        200,
        r#"{"QQQ   260814P00740000":{"assetMainType":"OPTION","symbol":"QQQ   260814P00740000","realtime":true,"reference":{"underlying":"QQQ","contractType":"PUT","strikePrice":740,"expirationYear":2026,"expirationMonth":8,"expirationDay":14},"quote":{"bidPrice":29.9,"askPrice":30.1,"quoteTime":1700000000000}}}"#,
    );

    let result = test_sdk
        .sdk
        .market_data()
        .normalized_option_quotes(["QQQ   260814P00740000"], None, 1_700_000_001_000)
        .await
        .expect("synthetic Market Data response normalizes");

    assert_one_get(&test_sdk, "market-option-quotes");
    assert_eq!(
        result.response().endpoint().route_name(),
        "market-option-quotes"
    );
    assert_eq!(result.response().status(), 200);
    assert_eq!(result.quotes().len(), 1);
    assert!(
        result
            .response()
            .headers()
            .any(|(name, value)| { name == "x-synthetic-metadata" && value == b"preserved" })
    );
}

#[tokio::test]
async fn market_data_429_error_preserves_metadata_and_is_not_retried() {
    let test_sdk = test_sdk(429, "synthetic rate limit response");
    let query = QuotesQuery {
        symbols: CsvValues::from_values(["QQQ"]).expect("valid synthetic symbol"),
        fields: None,
        indicative: None,
    };

    let error = test_sdk
        .sdk
        .market_data()
        .quotes(query)
        .await
        .expect_err("429 remains a caller-visible read error");

    assert_one_get(&test_sdk, "market-quotes");
    assert_eq!(error.code(), "REST_HTTP_STATUS");
    let response = error.response().expect("HTTP error retains its response");
    assert_eq!(response.status(), 429);
    assert_eq!(response.attempts(), 1);
    assert!(
        response
            .headers()
            .any(|(name, value)| { name == "retry-after" && value == b"9" })
    );
    assert!(
        response
            .headers()
            .any(|(name, value)| { name == "x-synthetic-metadata" && value == b"preserved" })
    );
}
