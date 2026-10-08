use std::sync::Arc;
use std::time::{Duration, Instant};

use alpaca_core::BaseUrl;
use reqwest::{
    StatusCode,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue},
};
use serde::de::DeserializeOwned;

use crate::Error;
use crate::auth::Authenticator;
use crate::meta::{ErrorMeta, HttpResponse, ResponseMeta};
use crate::observer::{
    ErrorEvent, NoopObserver, RequestStart, ResponseEvent, RetryEvent, TransportObserver,
};
use crate::rate_limit::ConcurrencyLimit;
use crate::request::{NoContent, RequestBody, RequestParts};
use crate::retry::{RetryConfig, RetryDecision};

/// Maximum response bytes read and retained by this transport.
/// This applies to both fixed-length and chunked transfer encoding.
pub const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;

enum ResponseBodyError {
    TooLarge,
    InvalidUtf8,
    Transport(reqwest::Error),
}

#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    default_headers: HeaderMap,
    request_id_header_name: HeaderName,
    retry_config: RetryConfig,
    observer: Arc<dyn TransportObserver>,
    concurrency_limit: ConcurrencyLimit,
}

#[derive(Clone)]
pub struct HttpClientBuilder {
    reqwest_client: Option<reqwest::Client>,
    timeout: Duration,
    default_headers: HeaderMap,
    request_id_header_name: HeaderName,
    retry_config: RetryConfig,
    observer: Arc<dyn TransportObserver>,
    concurrency_limit: ConcurrencyLimit,
}

struct ResponseParts {
    meta: ResponseMeta,
    body: String,
}

impl HttpClient {
    #[must_use]
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::default()
    }

    pub async fn send_json<T>(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<T>, Error>
    where
        T: DeserializeOwned,
    {
        let response = self.send(base_url, &request, authenticator).await?;
        let parsed = serde_json::from_str(&response.body).map_err(|error| {
            let meta = ErrorMeta::from_response_meta(response.meta.clone(), response.body.clone());
            let error = Error::Deserialize {
                message: error.to_string(),
                meta: Some(meta.clone()),
            };
            self.observer.on_error(&ErrorEvent { meta: Some(meta) });
            error
        })?;

        self.observer.on_response(&ResponseEvent {
            meta: response.meta.clone(),
        });
        Ok(HttpResponse::new(parsed, response.meta))
    }

    pub async fn send_json_expected<T>(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        expected_status: StatusCode,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<T>, Error>
    where
        T: DeserializeOwned,
    {
        let response = self.send(base_url, &request, authenticator).await?;
        let response = self.require_status(response, expected_status)?;
        let parsed = serde_json::from_str(&response.body).map_err(|error| {
            let meta = ErrorMeta::from_response_meta(response.meta.clone(), response.body.clone());
            let error = Error::Deserialize {
                message: error.to_string(),
                meta: Some(meta.clone()),
            };
            self.observer.on_error(&ErrorEvent { meta: Some(meta) });
            error
        })?;

        self.observer.on_response(&ResponseEvent {
            meta: response.meta.clone(),
        });
        Ok(HttpResponse::new(parsed, response.meta))
    }

    pub async fn send_json_or_empty_expected<T>(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        expected_status: StatusCode,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<Option<T>>, Error>
    where
        T: DeserializeOwned,
    {
        let response = self.send(base_url, &request, authenticator).await?;
        let response = self.require_status(response, expected_status)?;
        let parsed = if response.body.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&response.body).map_err(|error| {
                let meta =
                    ErrorMeta::from_response_meta(response.meta.clone(), response.body.clone());
                let error = Error::Deserialize {
                    message: error.to_string(),
                    meta: Some(meta.clone()),
                };
                self.observer.on_error(&ErrorEvent { meta: Some(meta) });
                error
            })?)
        };

        self.observer.on_response(&ResponseEvent {
            meta: response.meta.clone(),
        });
        Ok(HttpResponse::new(parsed, response.meta))
    }

    pub async fn send_text(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<String>, Error> {
        let response = self.send(base_url, &request, authenticator).await?;
        self.observer.on_response(&ResponseEvent {
            meta: response.meta.clone(),
        });
        Ok(HttpResponse::new(response.body, response.meta))
    }

    pub async fn send_no_content(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<NoContent>, Error> {
        self.send_empty_expected(base_url, request, StatusCode::NO_CONTENT, authenticator)
            .await
    }

    pub async fn send_empty_expected(
        &self,
        base_url: &BaseUrl,
        request: RequestParts,
        expected_status: StatusCode,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<HttpResponse<NoContent>, Error> {
        let response = self.send(base_url, &request, authenticator).await?;
        let response = self.require_status(response, expected_status)?;
        if !response.body.is_empty() {
            let meta = ErrorMeta::from_response_meta(response.meta, response.body);
            let error = Error::Deserialize {
                message: format!(
                    "expected an empty response body for HTTP {}",
                    expected_status.as_u16()
                ),
                meta: Some(meta.clone()),
            };
            self.observer.on_error(&ErrorEvent {
                meta: Some(meta.clone()),
            });
            return Err(error);
        }

        self.observer.on_response(&ResponseEvent {
            meta: response.meta.clone(),
        });
        Ok(HttpResponse::new(NoContent, response.meta))
    }

    fn require_status(
        &self,
        response: ResponseParts,
        expected_status: StatusCode,
    ) -> Result<ResponseParts, Error> {
        if response.meta.status() == expected_status.as_u16() {
            return Ok(response);
        }

        let meta = ErrorMeta::from_response_meta(response.meta, response.body);
        let error = Error::HttpStatus(meta.clone());
        self.observer.on_error(&ErrorEvent { meta: Some(meta) });
        Err(error)
    }

    async fn send(
        &self,
        base_url: &BaseUrl,
        request: &RequestParts,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<ResponseParts, Error> {
        let _permit = self.concurrency_limit.acquire().await?;
        let url = base_url.join_path(request.path());
        let mut attempt = 0;
        let started_at = Instant::now();

        loop {
            let observed_url = url_with_query(&url, request.query());
            self.observer.on_request_start(&RequestStart {
                operation: request.operation().map(ToOwned::to_owned),
                method: request.method(),
                url: observed_url,
            });

            let request_builder = self.build_request(&url, request, authenticator)?;
            let response = match request_builder.send().await {
                Ok(response) => response,
                Err(error) => {
                    match self.retry_config.classify_transport_error(
                        &request.method(),
                        attempt,
                        started_at.elapsed(),
                    ) {
                        RetryDecision::RetryAfter(wait) => {
                            self.observer.on_retry(&RetryEvent {
                                operation: request.operation().map(ToOwned::to_owned),
                                method: request.method(),
                                url: url.clone(),
                                attempt: attempt + 1,
                                status: None,
                                wait,
                            });
                            tokio::time::sleep(wait).await;
                            attempt += 1;
                            continue;
                        }
                        RetryDecision::DoNotRetry => {
                            let error = Error::from_reqwest(error, None);
                            self.observer.on_error(&ErrorEvent { meta: None });
                            return Err(error);
                        }
                    }
                }
            };

            let status = response.status();
            let headers = response.headers().clone();
            let meta = ResponseMeta::from_response_parts(
                request.operation().map(ToOwned::to_owned),
                url.clone(),
                status,
                &headers,
                &self.request_id_header_name,
                attempt + 1,
                started_at.elapsed(),
            );
            let body = match read_bounded_response(response).await {
                Ok(body) => body,
                Err(ResponseBodyError::TooLarge) => {
                    let error_meta = ErrorMeta::from_response_meta(meta, String::new());
                    let error = Error::ResponseBodyTooLarge(error_meta.clone());
                    self.observer.on_error(&ErrorEvent {
                        meta: Some(error_meta),
                    });
                    return Err(error);
                }
                Err(ResponseBodyError::InvalidUtf8) => {
                    let error_meta = ErrorMeta::from_response_meta(meta, String::new());
                    let error = Error::InvalidResponseEncoding(error_meta.clone());
                    self.observer.on_error(&ErrorEvent {
                        meta: Some(error_meta),
                    });
                    return Err(error);
                }
                Err(ResponseBodyError::Transport(error)) => {
                    match self.retry_config.classify_transport_error(
                        &request.method(),
                        attempt,
                        started_at.elapsed(),
                    ) {
                        RetryDecision::RetryAfter(wait) => {
                            self.observer.on_retry(&RetryEvent {
                                operation: request.operation().map(ToOwned::to_owned),
                                method: request.method(),
                                url: url.clone(),
                                attempt: attempt + 1,
                                status: Some(status),
                                wait,
                            });
                            tokio::time::sleep(wait).await;
                            attempt += 1;
                            continue;
                        }
                        RetryDecision::DoNotRetry => {
                            let error_meta =
                                ErrorMeta::from_response_meta(meta.clone(), String::new());
                            let error = Error::from_reqwest(error, Some(error_meta.clone()));
                            self.observer.on_error(&ErrorEvent {
                                meta: Some(error_meta),
                            });
                            return Err(error);
                        }
                    }
                }
            };

            match self.retry_config.classify_response(
                &request.method(),
                status,
                attempt,
                meta.retry_after(),
                started_at.elapsed(),
            ) {
                RetryDecision::RetryAfter(wait) => {
                    self.observer.on_retry(&RetryEvent {
                        operation: request.operation().map(ToOwned::to_owned),
                        method: request.method(),
                        url: url.clone(),
                        attempt: attempt + 1,
                        status: Some(status),
                        wait,
                    });
                    tokio::time::sleep(wait).await;
                    attempt += 1;
                    continue;
                }
                RetryDecision::DoNotRetry => {}
            }

            if status.is_success() {
                return Ok(ResponseParts { meta, body });
            }

            let error_meta = ErrorMeta::from_response_meta(meta, body);
            let error = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                Error::RateLimited(error_meta.clone())
            } else {
                Error::HttpStatus(error_meta.clone())
            };
            self.observer.on_error(&ErrorEvent {
                meta: Some(error_meta),
            });
            return Err(error);
        }
    }

    fn build_request(
        &self,
        url: &str,
        request: &RequestParts,
        authenticator: Option<&dyn Authenticator>,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let mut headers = self.default_headers.clone();
        headers.extend(request.headers().clone());
        if let Some(authenticator) = authenticator {
            authenticator.apply(&mut headers)?;
        }

        let mut builder = self
            .client
            .request(request.method(), url)
            .headers(headers)
            .query(request.query());

        builder = match request.body() {
            RequestBody::Empty => builder,
            RequestBody::Json(value) => builder.json(value),
            RequestBody::Text(value) => builder.body(value.clone()),
            RequestBody::Bytes(value) => builder.body(value.clone()),
        };

        if matches!(request.body(), RequestBody::Text(_))
            && !request.headers().contains_key(CONTENT_TYPE)
            && !self.default_headers.contains_key(CONTENT_TYPE)
        {
            builder = builder.header(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        }

        Ok(builder)
    }
}

fn url_with_query(url: &str, query: &[(String, String)]) -> String {
    if query.is_empty() {
        return url.to_owned();
    }
    let Ok(mut parsed) = reqwest::Url::parse(url) else {
        return url.to_owned();
    };
    parsed
        .query_pairs_mut()
        .extend_pairs(query.iter().map(|(key, value)| (key, value)));
    parsed.into()
}

async fn read_bounded_response(
    mut response: reqwest::Response,
) -> Result<String, ResponseBodyError> {
    if !declared_length_within_limit(response.content_length()) {
        return Err(ResponseBodyError::TooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(ResponseBodyError::Transport)?
    {
        append_bounded_chunk(&mut body, &chunk)?;
    }

    String::from_utf8(body).map_err(|_| ResponseBodyError::InvalidUtf8)
}

fn declared_length_within_limit(length: Option<u64>) -> bool {
    length.is_none_or(|length| length <= MAX_RESPONSE_BODY_BYTES as u64)
}

fn append_bounded_chunk(body: &mut Vec<u8>, chunk: &[u8]) -> Result<(), ResponseBodyError> {
    if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
        return Err(ResponseBodyError::TooLarge);
    }
    body.extend_from_slice(chunk);
    Ok(())
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            reqwest_client: None,
            timeout: Duration::from_secs(30),
            default_headers: HeaderMap::new(),
            request_id_header_name: HeaderName::from_static("x-request-id"),
            retry_config: RetryConfig::default(),
            observer: Arc::new(NoopObserver),
            concurrency_limit: ConcurrencyLimit::default(),
        }
    }
}

impl HttpClientBuilder {
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn reqwest_client(mut self, client: reqwest::Client) -> Self {
        self.reqwest_client = Some(client);
        self
    }

    pub fn default_header(mut self, name: &str, value: &str) -> Result<Self, Error> {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            Error::InvalidRequest(format!("invalid default header name: {error}"))
        })?;
        let value = HeaderValue::from_str(value).map_err(|error| {
            Error::InvalidRequest(format!("invalid default header value: {error}"))
        })?;
        self.default_headers.insert(name, value);
        Ok(self)
    }

    pub fn request_id_header_name(mut self, name: &str) -> Result<Self, Error> {
        self.request_id_header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            Error::InvalidRequest(format!("invalid request id header name: {error}"))
        })?;
        Ok(self)
    }

    #[must_use]
    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    #[must_use]
    pub fn observer(mut self, observer: Arc<dyn TransportObserver>) -> Self {
        self.observer = observer;
        self
    }

    #[must_use]
    pub fn concurrency_limit(mut self, concurrency_limit: ConcurrencyLimit) -> Self {
        self.concurrency_limit = concurrency_limit;
        self
    }

    pub fn build(self) -> Result<HttpClient, Error> {
        let client = match self.reqwest_client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .timeout(self.timeout)
                .build()
                .map_err(|error| Error::from_reqwest(error, None))?,
        };

        Ok(HttpClient {
            client,
            default_headers: self.default_headers,
            request_id_header_name: self.request_id_header_name,
            retry_config: self.retry_config,
            observer: self.observer,
            concurrency_limit: self.concurrency_limit,
        })
    }
}

#[cfg(test)]
mod bounded_response_tests {
    use super::{
        MAX_RESPONSE_BODY_BYTES, ResponseBodyError, append_bounded_chunk,
        declared_length_within_limit,
    };

    #[test]
    fn declared_lengths_are_checked_before_reading() {
        assert!(declared_length_within_limit(None));
        assert!(declared_length_within_limit(Some(
            MAX_RESPONSE_BODY_BYTES as u64
        )));
        assert!(!declared_length_within_limit(Some(
            MAX_RESPONSE_BODY_BYTES as u64 + 1
        )));
    }

    #[test]
    fn response_chunks_are_capped_at_the_limit() {
        let mut body = Vec::new();
        let chunk = vec![b'x'; MAX_RESPONSE_BODY_BYTES];
        assert!(append_bounded_chunk(&mut body, &chunk).is_ok());
        assert_eq!(body.len(), MAX_RESPONSE_BODY_BYTES);
        assert!(matches!(
            append_bounded_chunk(&mut body, b"x"),
            Err(ResponseBodyError::TooLarge)
        ));
    }

    #[test]
    fn cumulative_chunk_limit_rejects_chunked_overflow() {
        let mut body = vec![b'x'; MAX_RESPONSE_BODY_BYTES - 2];
        assert!(append_bounded_chunk(&mut body, b"ab").is_ok());
        assert!(matches!(
            append_bounded_chunk(&mut body, b"c"),
            Err(ResponseBodyError::TooLarge)
        ));
        assert_eq!(body.len(), MAX_RESPONSE_BODY_BYTES);
    }
}
