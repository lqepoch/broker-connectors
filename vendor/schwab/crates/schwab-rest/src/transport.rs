//! Fixed-origin HTTPS adapter for bounded, one-attempt GET requests.
//! 提供固定来源的 HTTPS 适配器，执行有界且单次尝试的 GET 请求。

use std::time::Duration;

#[cfg(test)]
use reqwest::Certificate;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderName, HeaderValue};
use reqwest::{Client, ClientBuilder, Url, redirect::Policy};
use zeroize::Zeroizing;

use crate::client::{
    BoxFuture, HttpRequest, HttpResponse, HttpTransport, HttpTransportError, MAX_HEADER_COUNT,
    MAX_HEADER_NAME_BYTES, MAX_HEADER_VALUE_BYTES, MAX_RESPONSE_HEADER_BYTES, SCHWAB_API_ROOT,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HTTP2_HEADER_LIST_BYTES: usize = MAX_RESPONSE_HEADER_BYTES + MAX_HEADER_COUNT * 32;

/// Production read-only HTTPS adapter. It owns a client configured for the
/// fixed Schwab API origin and exposes no origin or request-method override.
/// 中文摘要：固定 Schwab HTTPS origin 的只读传输适配器；只发送 GET，关闭重定向并限制响应体与响应头。
pub struct SchwabHttpsTransport {
    client: Client,
    origin: Url,
}

impl SchwabHttpsTransport {
    /// Creates the production adapter with platform-verified rustls roots.
    /// Failure to initialize the TLS verifier or HTTP client fails closed.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    ///
    /// # Errors
    /// Returns [`HttpTransportError`] if the fixed-origin HTTPS client cannot be configured.
    pub fn new() -> Result<Self, HttpTransportError> {
        let origin = Url::parse(SCHWAB_API_ROOT).map_err(|_| HttpTransportError::Configuration)?;
        let client = build_client()?;
        Ok(Self { client, origin })
    }

    #[cfg(test)]
    pub(super) fn for_loopback_test(
        origin: &str,
        trusted_certificate: Certificate,
    ) -> Result<Self, HttpTransportError> {
        let origin = Url::parse(origin).map_err(|_| HttpTransportError::Configuration)?;
        if !is_test_loopback_origin(&origin) {
            return Err(HttpTransportError::Configuration);
        }
        let client = build_test_client(trusted_certificate)?;
        Ok(Self { client, origin })
    }
}

impl HttpTransport for SchwabHttpsTransport {
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
        Box::pin(async move {
            // Resolve and bind the target to the configured authority before
            // touching the bearer token. A leading `//host/path` in any
            // allowlisted route must never be able to redirect credentials.
            let url = resolve_request_url(&self.origin, request.endpoint().path())?;

            let authorization =
                request.with_bearer_token(|token| Zeroizing::new(format!("Bearer {token}")));
            let mut authorization_header = HeaderValue::from_bytes(authorization.as_bytes())
                .map_err(|_| HttpTransportError::InvalidResponse)?;
            authorization_header.set_sensitive(true);

            let mut response = self
                .client
                .get(url)
                .timeout(request.timeout())
                .header(ACCEPT, request.accept())
                .header(AUTHORIZATION, authorization_header)
                .send()
                .await
                .map_err(|error| classify_send_error(&error))?;

            if response.status().is_redirection() {
                return Err(HttpTransportError::Redirect);
            }

            let status = response.status().as_u16();
            let retry_after_values = response
                .headers()
                .get_all("retry-after")
                .iter()
                .take(MAX_HEADER_COUNT + 1)
                .map(reqwest::header::HeaderValue::as_bytes)
                .collect::<Vec<_>>();
            request
                .observe_response_head(status, &retry_after_values)
                .await
                .map_err(|_| HttpTransportError::RateLimitObservation)?;

            let max_body_bytes = request.max_response_body_bytes();
            let headers = bounded_headers(response.headers())?;
            if response
                .content_length()
                .is_some_and(|length| length > max_body_bytes as u64)
            {
                return Err(HttpTransportError::BodyLimit);
            }

            let capacity = response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(max_body_bytes);
            let mut body = Vec::with_capacity(capacity);
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| classify_receive_error(&error))?
            {
                let new_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(HttpTransportError::BodyLimit)?;
                if new_length > max_body_bytes {
                    return Err(HttpTransportError::BodyLimit);
                }
                body.try_reserve(chunk.len())
                    .map_err(|_| HttpTransportError::BodyLimit)?;
                body.extend_from_slice(&chunk);
            }

            HttpResponse::new(status, headers, body).map_err(|error| match error {
                crate::client::ResponseLimitError::BodyTooLarge => HttpTransportError::BodyLimit,
                crate::client::ResponseLimitError::InvalidStatus => {
                    HttpTransportError::InvalidResponse
                }
                crate::client::ResponseLimitError::TooManyHeaders
                | crate::client::ResponseLimitError::InvalidHeaderName
                | crate::client::ResponseLimitError::InvalidHeaderValue
                | crate::client::ResponseLimitError::HeadersTooLarge => {
                    HttpTransportError::HeaderLimit
                }
            })
        })
    }
}

fn resolve_request_url(origin: &Url, path: &str) -> Result<Url, HttpTransportError> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path
            .bytes()
            .any(|byte| byte <= b' ' || byte == b'\\' || byte == 0x7f)
    {
        return Err(HttpTransportError::Configuration);
    }
    let target = origin
        .join(path)
        .map_err(|_| HttpTransportError::Configuration)?;
    if origin.scheme() != "https"
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
        || target.scheme() != origin.scheme()
        || target.host_str() != origin.host_str()
        || target.port_or_known_default() != origin.port_or_known_default()
        || !target.username().is_empty()
        || target.password().is_some()
        || target.fragment().is_some()
    {
        return Err(HttpTransportError::Configuration);
    }
    Ok(target)
}

fn client_builder() -> Result<ClientBuilder, HttpTransportError> {
    let max_http2_header_list_bytes = u32::try_from(MAX_HTTP2_HEADER_LIST_BYTES)
        .map_err(|_| HttpTransportError::Configuration)?;
    Ok(Client::builder()
        .use_rustls_tls()
        .https_only(true)
        .no_proxy()
        .redirect(Policy::none())
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(15))
        .connect_timeout(CONNECT_TIMEOUT)
        // One sentinel header is parsed so the contract layer can classify an
        // exact one-over-limit header count. Reqwest exposes no HTTP/1 header
        // byte-size setting; `bounded_headers` checks byte limits after parse.
        // Hyper's pinned HTTP/1 parser has its own default maximum read buffer,
        // but that is not the REST contract's byte limit.
        .http1_max_headers(MAX_HEADER_COUNT + 1)
        // HTTP/2 accounts for 32 bytes of per-field overhead in addition to
        // names and values; keep the parser close to the REST contract limit.
        .http2_max_header_list_size(max_http2_header_list_bytes))
}

fn build_client() -> Result<Client, HttpTransportError> {
    client_builder()?
        .build()
        .map_err(|_| HttpTransportError::Configuration)
}

#[cfg(test)]
fn build_test_client(trusted_certificate: Certificate) -> Result<Client, HttpTransportError> {
    // Tests trust only the ephemeral local certificate. Certificate-chain
    // and hostname verification remain enabled; invalid certificates are
    // never accepted by this adapter.
    client_builder()?
        .tls_certs_only([trusted_certificate])
        .build()
        .map_err(|_| HttpTransportError::Configuration)
}

fn bounded_headers(
    headers: &reqwest::header::HeaderMap,
) -> Result<Vec<(String, Vec<u8>)>, HttpTransportError> {
    if headers.len() > MAX_HEADER_COUNT {
        return Err(HttpTransportError::HeaderLimit);
    }

    let mut total_bytes = 0usize;
    for (name, value) in headers {
        let name_bytes = name.as_str().as_bytes();
        let value_bytes = value.as_bytes();
        if name_bytes.is_empty() || name_bytes.len() > MAX_HEADER_NAME_BYTES {
            return Err(HttpTransportError::HeaderLimit);
        }
        if value_bytes.len() > MAX_HEADER_VALUE_BYTES {
            return Err(HttpTransportError::HeaderLimit);
        }
        total_bytes = total_bytes
            .checked_add(name_bytes.len())
            .and_then(|length| length.checked_add(value_bytes.len()))
            .ok_or(HttpTransportError::HeaderLimit)?;
        if total_bytes > MAX_RESPONSE_HEADER_BYTES {
            return Err(HttpTransportError::HeaderLimit);
        }
    }

    let mut bounded = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        // HeaderMap only contains syntactically valid HTTP names. Reparse here
        // to keep the public response boundary independent of reqwest types.
        let parsed_name = HeaderName::from_bytes(name.as_str().as_bytes())
            .map_err(|_| HttpTransportError::HeaderLimit)?;
        let parsed_value = HeaderValue::from_bytes(value.as_bytes())
            .map_err(|_| HttpTransportError::HeaderLimit)?;
        bounded.push((
            parsed_name.as_str().to_owned(),
            parsed_value.as_bytes().to_vec(),
        ));
    }
    Ok(bounded)
}

fn classify_send_error(error: &reqwest::Error) -> HttpTransportError {
    // Reqwest can mark a connection-establishment timeout as both a timeout
    // and a connect failure. Preserve the pre-dispatch classification first;
    // timeouts outside connection establishment remain potentially sent.
    if error.is_connect() {
        HttpTransportError::Connect
    } else if error.is_timeout() {
        HttpTransportError::Timeout
    } else if error.is_decode() {
        // Reqwest's Decode kind describes response-body decoding. It does not
        // identify HTTP/1 header-limit failures, so do not infer HeaderLimit.
        HttpTransportError::InvalidResponse
    } else if error.is_redirect() {
        HttpTransportError::Redirect
    } else {
        HttpTransportError::Send
    }
}

fn classify_receive_error(error: &reqwest::Error) -> HttpTransportError {
    if error.is_timeout() {
        HttpTransportError::Timeout
    } else {
        HttpTransportError::Receive
    }
}

#[cfg(test)]
fn is_test_loopback_origin(origin: &Url) -> bool {
    origin.scheme() == "https"
        && origin.host_str() == Some("127.0.0.1")
        && origin.port().is_some()
        && origin.username().is_empty()
        && origin.password().is_none()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none()
}

#[cfg(test)]
mod origin_tests {
    use super::{HttpTransportError, SCHWAB_API_ROOT, Url, resolve_request_url};

    #[test]
    fn resolved_request_url_stays_bound_to_https_origin() {
        let origin = Url::parse(SCHWAB_API_ROOT).expect("fixed origin is a valid URL");
        let normal = resolve_request_url(&origin, "/trader/v1/accounts")
            .expect("allowlisted path remains on fixed origin");
        assert_eq!(
            normal.as_str(),
            "https://api.schwabapi.com/trader/v1/accounts"
        );

        let query = resolve_request_url(
            &origin,
            "/marketdata/v1/quotes?symbols=QQQ%2CSPY&fields=quote%2Creference&indicative=false",
        )
        .expect("typed allowlisted request query remains on fixed origin");
        assert_eq!(
            query.as_str(),
            "https://api.schwabapi.com/marketdata/v1/quotes?symbols=QQQ%2CSPY&fields=quote%2Creference&indicative=false"
        );

        for path in [
            "//foreign.example/capture",
            "//api.schwabapi.com:444/capture",
            "//user@api.schwabapi.com/capture",
            "/\\foreign.example/capture",
            "/trader/v1/accounts\r\nHost: foreign.example",
            "/trader/v1/accounts#synthetic",
            "/trader/v1/accounts?fields=positions#synthetic",
            "http://api.schwabapi.com/trader/v1/accounts",
        ] {
            assert_eq!(
                resolve_request_url(&origin, path),
                Err(HttpTransportError::Configuration),
                "path {path:?} must be rejected before adding authorization"
            );
        }

        for invalid_origin in [
            "https://api.schwabapi.com/?token=synthetic",
            "https://api.schwabapi.com/#synthetic",
        ] {
            let invalid_origin = Url::parse(invalid_origin).expect("test origin is a valid URL");
            assert_eq!(
                resolve_request_url(&invalid_origin, "/trader/v1/accounts"),
                Err(HttpTransportError::Configuration),
                "configured origin {invalid_origin:?} must not carry query or fragment data"
            );
        }
    }
}
