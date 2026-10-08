use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use reqwest::{Certificate, Url};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::client::{
    AccessToken, HttpRequest, HttpTransport, HttpTransportError, MAX_HEADER_VALUE_BYTES,
    MAX_RESPONSE_BODY_BYTES, ReadEndpoint,
};
use crate::{CsvValues, QuotesQuery, ReadRequest, SchwabHttpsTransport};

const SYNTHETIC_TOKEN: &str = "synthetic-transport-token";
const SYNTHETIC_BODY_MARKER: &[u8] = b"synthetic-response-private-marker";
const SYNTHETIC_HEADER_MARKER: &[u8] = b"synthetic-header-private-marker";
const HYPER_H1_DEFAULT_MAX_READ_BUFFER_BYTES: usize = 8_192 + 4_096 * 100;
const LARGE_HEADER_TEST_BYTES: usize = 256 * 1024;

struct TlsMaterial {
    server_config: Arc<ServerConfig>,
    trusted_certificate: Certificate,
}

struct TestServer {
    origin: String,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn finish(self) -> Vec<Vec<u8>> {
        self.task.await.expect("local test server task completes");
        self.requests
            .lock()
            .expect("request observation lock")
            .clone()
    }

    async fn abort_without_waiting_for_request(self) -> Vec<Vec<u8>> {
        self.task.abort();
        let _ = self.task.await;
        self.requests
            .lock()
            .expect("request observation lock")
            .clone()
    }
}

fn make_tls_material() -> TlsMaterial {
    let mut ca_parameters =
        CertificateParams::new(Vec::<String>::new()).expect("test CA parameters are valid");
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_parameters.key_usages.push(KeyUsagePurpose::KeyCertSign);
    let ca_key_pair = KeyPair::generate().expect("ephemeral test CA key is generated");
    let ca_certificate = ca_parameters
        .self_signed(&ca_key_pair)
        .expect("ephemeral test CA certificate is generated");
    let issuer = Issuer::new(ca_parameters, ca_key_pair);

    let mut leaf_parameters = CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .expect("loopback leaf certificate parameters are valid");
    leaf_parameters.use_authority_key_identifier_extension = true;
    leaf_parameters
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let leaf_key_pair = KeyPair::generate().expect("ephemeral server key is generated");
    let leaf_certificate = leaf_parameters
        .signed_by(&leaf_key_pair, &issuer)
        .expect("ephemeral server certificate is signed by the local test CA");
    let server_certificate = leaf_certificate.der().clone();
    let trusted_certificate =
        Certificate::from_der(ca_certificate.der().as_ref()).expect("test CA certificate parses");
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key_pair.serialize_der()));
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![server_certificate], private_key)
        .expect("ephemeral certificate and key form a TLS server identity");

    TlsMaterial {
        server_config: Arc::new(server_config),
        trusted_certificate,
    }
}

async fn spawn_server(
    server_config: Arc<ServerConfig>,
    first_response: Vec<u8>,
    follow_up_response: Option<Vec<u8>>,
    delay_before_first_response: Duration,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds an ephemeral port");
    let address: SocketAddr = listener.local_addr().expect("listener has a local address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let task_requests = Arc::clone(&requests);
    let task = tokio::spawn(async move {
        let acceptor = TlsAcceptor::from(server_config);
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut stream) = acceptor.accept(tcp).await else {
            return;
        };
        let Some(first_request) = read_request(&mut stream).await else {
            return;
        };
        task_requests
            .lock()
            .expect("request observation lock")
            .push(first_request);
        if !delay_before_first_response.is_zero() {
            tokio::time::sleep(delay_before_first_response).await;
        }
        let _ = stream.write_all(&first_response).await;
        let _ = stream.flush().await;

        let Some(follow_up_response) = follow_up_response else {
            return;
        };
        let Ok(Ok((tcp, _))) = timeout(Duration::from_millis(200), listener.accept()).await else {
            return;
        };
        let Ok(mut stream) = acceptor.accept(tcp).await else {
            return;
        };
        let Some(request) = read_request(&mut stream).await else {
            return;
        };
        task_requests
            .lock()
            .expect("request observation lock")
            .push(request);
        let _ = stream.write_all(&follow_up_response).await;
        let _ = stream.flush().await;
    });

    TestServer {
        origin: format!("https://{address}"),
        requests,
        task,
    }
}

async fn read_request(stream: &mut tokio_rustls::server::TlsStream<TcpStream>) -> Option<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer).await.ok()?;
        if bytes_read == 0 {
            return None;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() > 16 * 1024 {
            return None;
        }
    }
    Some(request)
}

fn test_request(timeout: Duration) -> HttpRequest {
    test_request_with_gate(timeout, crate::tests::test_read_admission_gate())
}

fn test_request_with_gate(
    timeout: Duration,
    gate: Arc<dyn crate::ReadAdmissionPort>,
) -> HttpRequest {
    HttpRequest::get_with_timeout(
        ReadEndpoint::AccountNumbers,
        AccessToken::new(SYNTHETIC_TOKEN).expect("synthetic token is valid"),
        gate,
        timeout,
    )
}

#[derive(Default)]
struct RecordingAdmission(Mutex<Vec<Vec<u8>>>);

impl crate::ReadAdmissionPort for RecordingAdmission {
    fn admit(
        &self,
        _priority: crate::ReadPriority,
        _maximum_wait: Duration,
    ) -> crate::BoxFuture<'_, Result<(), crate::ReadAdmissionError>> {
        Box::pin(async { Ok(()) })
    }

    fn observe_rate_limit_headers<'a>(
        &'a self,
        values: &'a [&'a [u8]],
    ) -> crate::BoxFuture<'a, Result<(), crate::ReadAdmissionError>> {
        let copied = values
            .iter()
            .map(|value| value.to_vec())
            .collect::<Vec<_>>();
        Box::pin(async move {
            self.0
                .lock()
                .expect("recorded admission lock")
                .extend(copied);
            Ok(())
        })
    }
}

fn assert_retry_after_observed(admission: &RecordingAdmission) {
    assert_eq!(
        admission
            .0
            .lock()
            .expect("recorded admission lock")
            .as_slice(),
        &[b"3600".to_vec()]
    );
}

#[tokio::test]
async fn cross_origin_allowlisted_target_is_rejected_before_bearer_is_sent() {
    let tls = make_tls_material();
    let origin_server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[], b"synthetic-origin-response"),
        None,
        Duration::ZERO,
    )
    .await;
    let foreign_server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[], b"synthetic-foreign-response"),
        None,
        Duration::ZERO,
    )
    .await;
    let foreign_port = foreign_server
        .origin
        .rsplit(':')
        .next()
        .expect("loopback origin contains an explicit port");
    let transport =
        SchwabHttpsTransport::for_loopback_test(&origin_server.origin, tls.trusted_certificate)
            .expect("test transport uses the primary local origin");
    let request = HttpRequest::get(
        ReadEndpoint::allowlisted(
            "malicious-test-route",
            format!("//127.0.0.1:{foreign_port}/capture"),
        ),
        AccessToken::new(SYNTHETIC_TOKEN).expect("synthetic bearer is valid"),
        crate::tests::test_read_admission_gate(),
    );

    let error = transport
        .send(request)
        .await
        .expect_err("cross-origin target must be rejected before transport");
    assert_eq!(error, HttpTransportError::Configuration);
    assert_safe_error(&error);
    assert!(
        origin_server
            .abort_without_waiting_for_request()
            .await
            .is_empty()
    );
    assert!(
        foreign_server
            .abort_without_waiting_for_request()
            .await
            .is_empty()
    );
}

fn response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status} synthetic\r\n");
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let mut wire = response.into_bytes();
    wire.extend_from_slice(body);
    wire
}

fn incomplete_oversized_h1_header(value_len: usize) -> Vec<u8> {
    let mut wire = b"HTTP/1.1 200 synthetic\r\nX-Synthetic-Oversized: ".to_vec();
    wire.extend(std::iter::repeat_n(b'x', value_len));
    wire
}

fn chunked_response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status} synthetic\r\n");
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
    response.push_str(&format!("{:x}\r\n", body.len()));
    let mut wire = response.into_bytes();
    wire.extend_from_slice(body);
    wire.extend_from_slice(b"\r\n0\r\n\r\n");
    wire
}

#[tokio::test]
async fn production_adapter_sends_fixed_get_with_sensitive_bearer_over_verified_tls() {
    let tls = make_tls_material();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(
            200,
            &[("X-Request-Id", "synthetic-request-id")],
            b"synthetic-read-body",
        ),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin uses a trusted local test certificate");

    let result = transport.send(test_request(Duration::from_secs(2))).await;
    let requests = server.finish().await;
    assert_eq!(
        requests.len(),
        1,
        "TLS completed and one HTTP request was received"
    );
    let received = result.expect("local verified HTTPS GET succeeds");
    assert_eq!(received.status(), 200);
    assert_eq!(received.body(), b"synthetic-read-body");
    assert!(
        received
            .headers()
            .any(|(name, value)| { name == "x-request-id" && value == b"synthetic-request-id" })
    );

    let request = String::from_utf8(requests[0].clone()).expect("HTTP request is ASCII");
    assert!(request.starts_with("GET /trader/v1/accounts/accountNumbers HTTP/1.1\r\n"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept: application/json\r\n")
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: bearer synthetic-transport-token\r\n")
    );
}

#[tokio::test]
async fn typed_quote_query_reaches_only_configured_loopback_origin() {
    let tls = make_tls_material();
    let configured_server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[], b"synthetic-quote-response"),
        None,
        Duration::ZERO,
    )
    .await;
    let foreign_server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[], b"synthetic-foreign-response"),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&configured_server.origin, tls.trusted_certificate)
            .expect("test-only adapter trusts only the local test CA");

    let read_request = ReadRequest::Quotes(QuotesQuery {
        symbols: CsvValues::from_values(["QQQ", "SPY"])
            .expect("synthetic quote symbols form a valid bounded list"),
        fields: Some(
            CsvValues::from_values(["quote", "reference"])
                .expect("synthetic quote fields form a valid bounded list"),
        ),
        indicative: Some(false),
    });
    let endpoint = read_request
        .endpoint()
        .expect("typed quote request produces an allowlisted endpoint");
    let target = endpoint.path().to_owned();
    assert_eq!(
        target,
        "/marketdata/v1/quotes?symbols=QQQ%2CSPY&fields=quote%2Creference&indicative=false"
    );
    let parsed_target = Url::parse(&format!("https://api.schwabapi.com{target}"))
        .expect("allowlisted target is a valid HTTPS URL");
    let decoded_query: Vec<_> = parsed_target.query_pairs().into_owned().collect();
    assert_eq!(
        decoded_query,
        [
            ("symbols".to_owned(), "QQQ,SPY".to_owned()),
            ("fields".to_owned(), "quote,reference".to_owned()),
            ("indicative".to_owned(), "false".to_owned()),
        ]
    );

    let response = transport
        .send(HttpRequest::get(
            endpoint,
            AccessToken::new(SYNTHETIC_TOKEN).expect("synthetic bearer is valid"),
            crate::tests::test_read_admission_gate(),
        ))
        .await
        .expect("normal allowlisted query is sent over verified local TLS");
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"synthetic-quote-response");

    let configured_requests = configured_server.finish().await;
    assert_eq!(configured_requests.len(), 1);
    let request =
        String::from_utf8(configured_requests[0].clone()).expect("captured HTTP request is ASCII");
    assert!(request.starts_with(
        "GET /marketdata/v1/quotes?symbols=QQQ%2CSPY&fields=quote%2Creference&indicative=false HTTP/1.1\r\n"
    ));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: bearer synthetic-transport-token\r\n")
    );
    assert!(
        foreign_server
            .abort_without_waiting_for_request()
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn redirect_is_rejected_without_following_location() {
    let tls = make_tls_material();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(
            302,
            &[
                ("Location", "/followed"),
                (
                    "X-Private",
                    std::str::from_utf8(SYNTHETIC_HEADER_MARKER)
                        .expect("synthetic header marker is ASCII"),
                ),
            ],
            b"synthetic-redirect-body",
        ),
        Some(response(200, &[], b"synthetic-followed-body")),
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("redirect must not be followed or surfaced as a successful response");
    assert_eq!(error, HttpTransportError::Redirect);
    assert_safe_error(&error);

    let requests = server.finish().await;
    assert_eq!(requests.len(), 1, "redirect target was never requested");
}

#[tokio::test]
async fn chunked_response_body_is_rejected_before_oversized_body_is_retained() {
    let tls = make_tls_material();
    let body = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        chunked_response(200, &[], &body),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("streamed body above the bound is rejected");
    assert_eq!(error, HttpTransportError::BodyLimit);
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn rate_limit_head_is_observed_before_oversized_body_rejection() {
    let tls = make_tls_material();
    let admission = Arc::new(RecordingAdmission::default());
    let body = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(429, &[("Retry-After", "3600")], &body),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request_with_gate(
            Duration::from_secs(3),
            Arc::clone(&admission) as Arc<dyn crate::ReadAdmissionPort>,
        ))
        .await
        .expect_err("oversized 429 body is rejected");
    assert_eq!(error, HttpTransportError::BodyLimit);
    assert_retry_after_observed(&admission);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn rate_limit_head_is_observed_before_truncated_body_read_failure() {
    let tls = make_tls_material();
    let admission = Arc::new(RecordingAdmission::default());
    let wire = b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 3600\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort"
        .to_vec();
    let server = spawn_server(Arc::clone(&tls.server_config), wire, None, Duration::ZERO).await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request_with_gate(
            Duration::from_secs(3),
            Arc::clone(&admission) as Arc<dyn crate::ReadAdmissionPort>,
        ))
        .await
        .expect_err("truncated 429 body read fails closed");
    assert_eq!(error, HttpTransportError::Receive);
    assert_retry_after_observed(&admission);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn aggregate_and_count_header_overlimits_are_rejected_with_fixed_errors() {
    let tls = make_tls_material();
    let headers = (0..5)
        .map(|index| {
            (
                format!("X-Synthetic-{index}"),
                "x".repeat(MAX_HEADER_VALUE_BYTES),
            )
        })
        .collect::<Vec<_>>();
    let header_refs = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &header_refs, b""),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("aggregate response headers above the contract bound are rejected");
    assert_eq!(error, HttpTransportError::HeaderLimit);
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);

    let tls = make_tls_material();
    let oversized_value = "x".repeat(MAX_HEADER_VALUE_BYTES + 1);
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[("X-Synthetic-Large", &oversized_value)], b""),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("an individual response header above its bound is rejected");
    assert_eq!(error, HttpTransportError::HeaderLimit);
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);

    let tls = make_tls_material();
    let many_headers = (0..crate::client::MAX_HEADER_COUNT - 1)
        .map(|index| (format!("X-Synthetic-{index}"), "v".to_owned()))
        .collect::<Vec<_>>();
    let many_header_refs = many_headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &many_header_refs, b""),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("response header count above the contract bound is rejected");
    assert_eq!(error, HttpTransportError::HeaderLimit);
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn single_256_kib_header_is_rejected_by_post_parse_contract_check() {
    let tls = make_tls_material();
    let oversized_value = "x".repeat(LARGE_HEADER_TEST_BYTES);
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[("X-Synthetic-Large", &oversized_value)], b""),
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(3)))
        .await
        .expect_err("a parseable 256 KiB value must fail the REST header contract");
    assert_eq!(error, HttpTransportError::HeaderLimit);
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn incomplete_http1_header_above_hyper_default_read_buffer_fails_closed() {
    let tls = make_tls_material();
    let response =
        incomplete_oversized_h1_header(HYPER_H1_DEFAULT_MAX_READ_BUFFER_BYTES.saturating_add(1));
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response,
        None,
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_secs(3)))
        .await
        .expect_err("an incomplete HTTP/1 header beyond Hyper's read-buffer limit must fail");
    // Reqwest 0.13.5 wraps Hyper exchange/parser errors as request errors;
    // they are not the `Decode` kind used for response-body decoding.
    assert_eq!(error, HttpTransportError::Send);
    assert_eq!(
        error.request_dispatch_certainty(),
        crate::RequestDispatchCertainty::MayHaveBeenSent
    );
    assert_safe_error(&error);
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn total_request_timeout_and_diagnostics_are_fixed_and_redacted() {
    let tls = make_tls_material();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(200, &[], SYNTHETIC_BODY_MARKER),
        None,
        Duration::from_millis(350),
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let error = transport
        .send(test_request(Duration::from_millis(100)))
        .await
        .expect_err("request deadline covers response receipt");
    assert_eq!(error, HttpTransportError::Timeout);
    assert_eq!(
        error.request_dispatch_certainty(),
        crate::RequestDispatchCertainty::MayHaveBeenSent
    );
    assert_safe_error(&error);
    let _ = server.finish().await;
}

#[tokio::test]
async fn untrusted_tls_certificate_fails_before_any_http_request() {
    let server_tls = make_tls_material();
    let different_trust = make_tls_material();
    let server = spawn_server(
        Arc::clone(&server_tls.server_config),
        response(200, &[], SYNTHETIC_BODY_MARKER),
        None,
        Duration::ZERO,
    )
    .await;
    let transport = SchwabHttpsTransport::for_loopback_test(
        &server.origin,
        different_trust.trusted_certificate,
    )
    .expect("the test transport is configured with an explicit local trust root");

    let error = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect_err("a certificate outside the configured trust root is rejected");
    assert_eq!(error, HttpTransportError::Connect);
    assert_eq!(
        error.request_dispatch_certainty(),
        crate::RequestDispatchCertainty::DefinitelyNotSent
    );
    assert_safe_error(&error);
    assert!(server.finish().await.is_empty());
}

#[tokio::test]
async fn http_5xx_is_not_retried_inside_the_transport() {
    let tls = make_tls_material();
    let server = spawn_server(
        Arc::clone(&tls.server_config),
        response(503, &[("Retry-After", "0")], SYNTHETIC_BODY_MARKER),
        Some(response(200, &[], b"synthetic-retry-body")),
        Duration::ZERO,
    )
    .await;
    let transport =
        SchwabHttpsTransport::for_loopback_test(&server.origin, tls.trusted_certificate)
            .expect("test-only loopback origin is accepted");

    let response = transport
        .send(test_request(Duration::from_secs(2)))
        .await
        .expect("transport returns bounded status responses to caller policy");
    assert_eq!(response.status(), 503);
    assert_eq!(response.body(), SYNTHETIC_BODY_MARKER);
    let requests = server.finish().await;
    assert_eq!(
        requests.len(),
        1,
        "one GET produces one physical HTTP request"
    );
}

#[test]
fn test_origin_override_accepts_only_explicit_https_loopback_urls() {
    let tls = make_tls_material();
    let result = SchwabHttpsTransport::for_loopback_test(
        "https://example.invalid:443",
        tls.trusted_certificate,
    );
    let error = match result {
        Ok(_) => panic!("test origin override accepted a non-loopback host"),
        Err(error) => error,
    };
    assert_eq!(error, HttpTransportError::Configuration);
    assert_safe_error(&error);
}

fn assert_safe_error(error: &HttpTransportError) {
    let diagnostics = format!("{error:?} {error}");
    assert!(!diagnostics.contains(SYNTHETIC_TOKEN));
    assert!(!diagnostics.contains("synthetic-response-private-marker"));
    assert!(!diagnostics.contains("synthetic-header-private-marker"));
    assert!(!diagnostics.contains("127.0.0.1"));
}
