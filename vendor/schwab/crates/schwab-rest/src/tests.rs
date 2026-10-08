use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SYNTHETIC_TOKEN: &str = "synthetic-token-never-a-credential";
const SYNTHETIC_BODY_MARKER: &[u8] = b"synthetic-response-marker";
const SYNTHETIC_HEADER_MARKER: &[u8] = b"synthetic-header-marker";

#[derive(Clone, Copy)]
enum TokenResult {
    Available,
    Unavailable,
    Invalid,
}

struct FakeTokenProvider {
    result: TokenResult,
    calls: Arc<AtomicUsize>,
}

impl FakeTokenProvider {
    fn available(calls: Arc<AtomicUsize>) -> Self {
        Self {
            result: TokenResult::Available,
            calls,
        }
    }

    fn unavailable(calls: Arc<AtomicUsize>) -> Self {
        Self {
            result: TokenResult::Unavailable,
            calls,
        }
    }

    fn invalid(calls: Arc<AtomicUsize>) -> Self {
        Self {
            result: TokenResult::Invalid,
            calls,
        }
    }
}

impl AccessTokenProvider for FakeTokenProvider {
    fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match self.result {
                TokenResult::Available => AccessToken::new(SYNTHETIC_TOKEN),
                TokenResult::Unavailable => Err(TokenProviderError::Unavailable),
                TokenResult::Invalid => AccessToken::new("bad\r\nauthorization"),
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestCheck {
    MethodIsGet,
    RouteIsFixed,
    TokenMatches,
    TimeoutIsBounded,
    RedirectsDisabled,
    AcceptsJson,
    ResponseBodyLimitIsFixed,
}

#[derive(Debug, Eq, PartialEq)]
struct RequestObservation {
    passed: Vec<RequestCheck>,
}

struct FakeTransport {
    result: Mutex<Option<Result<HttpResponse, HttpTransportError>>>,
    calls: Arc<AtomicUsize>,
    observation: Arc<Mutex<Option<RequestObservation>>>,
}

impl FakeTransport {
    fn response(
        response: HttpResponse,
        calls: Arc<AtomicUsize>,
        observation: Arc<Mutex<Option<RequestObservation>>>,
    ) -> Self {
        Self {
            result: Mutex::new(Some(Ok(response))),
            calls,
            observation,
        }
    }

    fn failure(
        failure: HttpTransportError,
        calls: Arc<AtomicUsize>,
        observation: Arc<Mutex<Option<RequestObservation>>>,
    ) -> Self {
        Self {
            result: Mutex::new(Some(Err(failure))),
            calls,
            observation,
        }
    }
}

impl HttpTransport for FakeTransport {
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let passed = [
            (
                request.method() == HttpMethod::Get,
                RequestCheck::MethodIsGet,
            ),
            (
                request.url() == ReadEndpoint::AccountNumbers.url()
                    || request.url() == ReadEndpoint::UserPreferences.url(),
                RequestCheck::RouteIsFixed,
            ),
            (
                request.with_bearer_token(|value| value == SYNTHETIC_TOKEN),
                RequestCheck::TokenMatches,
            ),
            (
                request.timeout() == std::time::Duration::from_secs(15),
                RequestCheck::TimeoutIsBounded,
            ),
            (
                request.redirect_policy() == RedirectPolicy::Disabled,
                RequestCheck::RedirectsDisabled,
            ),
            (
                request.accept() == "application/json",
                RequestCheck::AcceptsJson,
            ),
            (
                request.max_response_body_bytes() == MAX_RESPONSE_BODY_BYTES,
                RequestCheck::ResponseBodyLimitIsFixed,
            ),
        ]
        .into_iter()
        .filter_map(|(condition, check)| condition.then_some(check))
        .collect();
        let observation = RequestObservation { passed };
        *self.observation.lock().expect("test observation lock") = Some(observation);
        let result = self.result.lock().expect("test result lock").take();
        Box::pin(async move {
            match result.unwrap_or(Err(HttpTransportError::Send)) {
                Ok(response) => {
                    request.observe_synthetic_response_head(&response).await?;
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        })
    }
}

fn response(status: u16, headers: Vec<(String, Vec<u8>)>, body: Vec<u8>) -> HttpResponse {
    HttpResponse::new(status, headers, body).expect("bounded synthetic response")
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("synthetic Tokio runtime")
        .block_on(future)
}

#[derive(Clone, Copy, Debug, Default)]
struct AllowReadAdmission;

impl ReadAdmissionPort for AllowReadAdmission {
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

pub(crate) fn test_read_admission_gate() -> Arc<dyn ReadAdmissionPort> {
    Arc::new(AllowReadAdmission)
}

fn observations() -> Arc<Mutex<Option<RequestObservation>>> {
    Arc::new(Mutex::new(None))
}

fn assert_safe_diagnostics(value: &str) {
    assert!(!value.contains(SYNTHETIC_TOKEN));
    assert!(!value.contains("bad\r\nauthorization"));
    assert!(!value.contains("synthetic-response-marker"));
    assert!(!value.contains("synthetic-header-marker"));
}

#[test]
fn account_numbers_uses_fixed_get_route_and_preserves_bounded_response_metadata() {
    let token_calls = Arc::new(AtomicUsize::new(0));
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let observation = observations();
    let fake = FakeTransport::response(
        response(
            200,
            vec![
                ("X-Request-Id".to_owned(), b"synthetic-request-id".to_vec()),
                ("Retry-After".to_owned(), b"30".to_vec()),
            ],
            br#"[{"accountNumber":"synthetic","hashValue":"synthetic-hash"}]"#.to_vec(),
        ),
        Arc::clone(&transport_calls),
        Arc::clone(&observation),
    );
    let client = SchwabRestClient::new(
        FakeTokenProvider::available(Arc::clone(&token_calls)),
        fake,
        test_read_admission_gate(),
    );

    let result = block_on(client.account_numbers()).expect("successful synthetic GET");
    let observed = observation.lock().expect("test observation lock");
    let observed = observed.as_ref().expect("request observation");
    assert_eq!(
        observed.passed,
        vec![
            RequestCheck::MethodIsGet,
            RequestCheck::RouteIsFixed,
            RequestCheck::TokenMatches,
            RequestCheck::TimeoutIsBounded,
            RequestCheck::RedirectsDisabled,
            RequestCheck::AcceptsJson,
            RequestCheck::ResponseBodyLimitIsFixed,
        ]
    );
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.method(), HttpMethod::Get);
    assert_eq!(result.endpoint(), &ReadEndpoint::AccountNumbers);
    assert_eq!(
        result.url(),
        "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"
    );
    assert_eq!(result.attempts(), 1);
    assert_eq!(result.status(), 200);
    assert_eq!(
        result.body(),
        br#"[{"accountNumber":"synthetic","hashValue":"synthetic-hash"}]"#
    );
    assert!(
        result
            .headers()
            .any(|(name, value)| name == "retry-after" && value == b"30")
    );
    assert!(
        result
            .headers()
            .any(|(name, value)| name == "x-request-id" && value == b"synthetic-request-id")
    );
}

#[test]
fn user_preferences_uses_its_other_fixed_read_route() {
    let token_calls = Arc::new(AtomicUsize::new(0));
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let observation = observations();
    let client = SchwabRestClient::new(
        FakeTokenProvider::available(token_calls),
        FakeTransport::response(
            response(204, Vec::new(), Vec::new()),
            Arc::clone(&transport_calls),
            Arc::clone(&observation),
        ),
        test_read_admission_gate(),
    );

    let result = block_on(client.user_preferences()).expect("successful synthetic GET");
    assert_eq!(result.endpoint(), &ReadEndpoint::UserPreferences);
    assert_eq!(
        result.url(),
        "https://api.schwabapi.com/trader/v1/userPreference"
    );
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    let observed = observation.lock().expect("test observation lock");
    assert!(
        observed
            .as_ref()
            .expect("request observation")
            .passed
            .contains(&RequestCheck::RouteIsFixed)
    );
}

#[test]
fn provider_failure_or_invalid_token_stops_before_transport() {
    for (provider_result, expected_code) in [
        (TokenResult::Unavailable, "REST_TOKEN_UNAVAILABLE"),
        (TokenResult::Invalid, "REST_TOKEN_INVALID"),
    ] {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let transport_calls = Arc::new(AtomicUsize::new(0));
        let observation = observations();
        let transport = FakeTransport::response(
            response(200, Vec::new(), Vec::new()),
            Arc::clone(&transport_calls),
            observation,
        );
        let provider = match provider_result {
            TokenResult::Unavailable => FakeTokenProvider::unavailable(Arc::clone(&token_calls)),
            TokenResult::Invalid => FakeTokenProvider::invalid(Arc::clone(&token_calls)),
            TokenResult::Available => unreachable!("case table excludes available"),
        };
        let client = SchwabRestClient::new(provider, transport, test_read_admission_gate());
        let error = block_on(client.account_numbers()).expect_err("provider must fail closed");
        assert_eq!(error.code(), expected_code);
        assert_eq!(error.attempts(), 0);
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_calls.load(Ordering::SeqCst), 0);
        assert_safe_diagnostics(&format!("{error:?} {error}"));
    }
}

#[test]
fn non_success_response_retains_status_headers_and_body_without_logging_them() {
    let token_calls = Arc::new(AtomicUsize::new(0));
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let client = SchwabRestClient::new(
        FakeTokenProvider::available(token_calls),
        FakeTransport::response(
            response(
                429,
                vec![
                    ("Retry-After".to_owned(), b"30".to_vec()),
                    ("X-Diagnostic".to_owned(), SYNTHETIC_HEADER_MARKER.to_vec()),
                ],
                SYNTHETIC_BODY_MARKER.to_vec(),
            ),
            Arc::clone(&transport_calls),
            observations(),
        ),
        test_read_admission_gate(),
    );

    let error = block_on(client.account_numbers()).expect_err("429 remains a status error");
    assert_eq!(error.code(), "REST_HTTP_STATUS");
    assert_eq!(
        error.request_dispatch_certainty(),
        RequestDispatchCertainty::MayHaveBeenSent
    );
    assert_eq!(error.attempts(), 1);
    let response = error.response().expect("status response retained");
    assert_eq!(response.status(), 429);
    assert_eq!(response.attempts(), 1);
    assert!(
        response
            .headers()
            .any(|(name, value)| name == "retry-after" && value == b"30")
    );
    assert!(
        response
            .headers()
            .any(|(name, value)| name == "x-diagnostic" && value == SYNTHETIC_HEADER_MARKER)
    );
    assert_eq!(response.body(), SYNTHETIC_BODY_MARKER);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    assert_safe_diagnostics(&format!("{error:?} {error}"));
}

#[test]
fn transport_failure_is_not_retried() {
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let client = SchwabRestClient::new(
        FakeTokenProvider::available(Arc::new(AtomicUsize::new(0))),
        FakeTransport::failure(
            HttpTransportError::Receive,
            Arc::clone(&transport_calls),
            observations(),
        ),
        test_read_admission_gate(),
    );

    let error = block_on(client.account_numbers()).expect_err("transport failure is preserved");
    assert_eq!(error.code(), "REST_TRANSPORT_RECEIVE_FAILED");
    assert_eq!(error.attempts(), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    assert_safe_diagnostics(&format!("{error:?} {error}"));
}

#[test]
fn rest_errors_expose_conservative_request_dispatch_certainty() {
    use RequestDispatchCertainty::{DefinitelyNotSent, MayHaveBeenSent};

    for error in [
        RestError::Request(ReadRequestError::InvalidQueryValue),
        RestError::Token(TokenProviderError::Unavailable),
        RestError::Transport(HttpTransportError::Connect),
        RestError::Transport(HttpTransportError::Configuration),
    ] {
        assert_eq!(error.request_dispatch_certainty(), DefinitelyNotSent);
    }

    for transport_error in [
        HttpTransportError::Send,
        HttpTransportError::Receive,
        HttpTransportError::Timeout,
        HttpTransportError::Redirect,
        HttpTransportError::HeaderLimit,
        HttpTransportError::BodyLimit,
        HttpTransportError::InvalidResponse,
    ] {
        let error = RestError::Transport(transport_error);
        assert_eq!(error.request_dispatch_certainty(), MayHaveBeenSent);
    }
}

#[test]
fn every_read_request_error_has_a_stable_sanitized_rest_code() {
    let cases = [
        (
            ReadRequestError::InvalidPathIdentifier,
            "REST_READ_PATH_IDENTIFIER_INVALID",
        ),
        (
            ReadRequestError::InvalidNumericIdentifier,
            "REST_READ_NUMERIC_IDENTIFIER_INVALID",
        ),
        (
            ReadRequestError::InvalidQueryKey,
            "REST_READ_QUERY_KEY_INVALID",
        ),
        (
            ReadRequestError::InvalidQueryValue,
            "REST_READ_QUERY_VALUE_INVALID",
        ),
        (ReadRequestError::InvalidList, "REST_READ_LIST_INVALID"),
        (
            ReadRequestError::DuplicateQueryParameter,
            "REST_READ_QUERY_PARAMETER_DUPLICATE",
        ),
        (
            ReadRequestError::ConflictingQueryParameter,
            "REST_READ_QUERY_PARAMETER_CONFLICT",
        ),
        (
            ReadRequestError::TooManyQueryParameters,
            "REST_READ_QUERY_PARAMETER_LIMIT",
        ),
        (ReadRequestError::TargetTooLong, "REST_READ_TARGET_TOO_LONG"),
    ];

    for (request_error, expected_code) in cases {
        let error = RestError::Request(request_error);
        assert_eq!(error.code(), expected_code);
        assert_eq!(error.to_string(), expected_code);
        assert!(!format!("{error:?} {error}").contains("QQQ"));
    }
}

#[test]
fn response_constructor_enforces_body_header_count_and_header_syntax_bounds() {
    let exact_size_body = vec![b'x'; MAX_RESPONSE_BODY_BYTES];
    let exact_size_response = HttpResponse::new(200, Vec::new(), exact_size_body)
        .expect("exact response body limit is accepted");
    assert_eq!(exact_size_response.body().len(), MAX_RESPONSE_BODY_BYTES);

    let oversized_body = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    assert_eq!(
        HttpResponse::new(200, Vec::new(), oversized_body).err(),
        Some(ResponseLimitError::BodyTooLarge)
    );

    let too_many_headers = (0..=MAX_HEADER_COUNT)
        .map(|index| (format!("x-test-{index}"), Vec::new()))
        .collect::<Vec<_>>();
    assert_eq!(
        HttpResponse::new(200, too_many_headers, Vec::new()).err(),
        Some(ResponseLimitError::TooManyHeaders)
    );

    let exact_header_count = (0..MAX_HEADER_COUNT)
        .map(|index| (format!("x-{index}"), Vec::new()))
        .collect::<Vec<_>>();
    assert!(HttpResponse::new(200, exact_header_count, Vec::new()).is_ok());

    assert_eq!(
        HttpResponse::new(
            200,
            vec![("bad\r\nname".to_owned(), Vec::new())],
            Vec::new(),
        )
        .err(),
        Some(ResponseLimitError::InvalidHeaderName)
    );
    assert_eq!(
        HttpResponse::new(
            200,
            vec![("x-test".to_owned(), b"bad\r\nvalue".to_vec())],
            Vec::new()
        )
        .err(),
        Some(ResponseLimitError::InvalidHeaderValue)
    );

    let headers_over_total_limit = (0..4)
        .map(|index| (format!("x{index}"), vec![b'a'; MAX_HEADER_VALUE_BYTES]))
        .collect::<Vec<_>>();
    assert_eq!(
        HttpResponse::new(200, headers_over_total_limit, Vec::new()).err(),
        Some(ResponseLimitError::HeadersTooLarge)
    );

    assert_eq!(
        HttpResponse::new(
            200,
            vec![("x-test".to_owned(), vec![b'a'; MAX_HEADER_VALUE_BYTES + 1])],
            Vec::new(),
        )
        .err(),
        Some(ResponseLimitError::InvalidHeaderValue)
    );

    assert_eq!(
        HttpResponse::new(99, Vec::new(), Vec::new()).err(),
        Some(ResponseLimitError::InvalidStatus)
    );
}

#[test]
fn token_debug_and_response_debug_redact_synthetic_sensitive_values() {
    let token = AccessToken::new(SYNTHETIC_TOKEN).expect("synthetic token is valid");
    assert_safe_diagnostics(&format!("{token:?}"));
    let response = response(
        200,
        vec![("x-private".to_owned(), SYNTHETIC_HEADER_MARKER.to_vec())],
        SYNTHETIC_BODY_MARKER.to_vec(),
    );
    assert_safe_diagnostics(&format!("{response:?}"));
    assert_safe_diagnostics(&format!(
        "{:?}",
        HttpRequest::get(
            ReadEndpoint::AccountNumbers,
            token,
            test_read_admission_gate(),
        )
    ));
}

#[test]
fn access_token_accepts_bearer_token_chars_and_rejects_header_injection() {
    let accepted = AccessToken::new("Abc-._~+/0==").expect("RFC 6750 token characters");
    assert!(format!("{accepted:?}").contains("[REDACTED]"));

    for invalid in ["", "has space", "bad\rvalue", "bad\nvalue", "not-☃"] {
        let error = AccessToken::new(invalid).expect_err("invalid bearer token rejected");
        assert_eq!(error, TokenProviderError::InvalidToken);
        assert_safe_diagnostics(&format!("{error:?} {error}"));
    }
}
