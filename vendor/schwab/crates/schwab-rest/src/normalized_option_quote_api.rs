//! Client-level normalized option-quote result and read operation.
//! 定义客户端级规范化期权报价结果和读取操作。

use std::fmt;

use crate::routes::normalize_option_quote_symbols;
use crate::{
    AccessTokenProvider, CsvValues, HttpTransport, NormalizedOptionQuote, ReadApiError,
    ReadRequest, RestResponse, SchwabRestClient,
};

/// One bounded option-quote GET plus its structural quote projection.
///
/// The raw response remains available for rate-limit and request metadata.
/// Debug output contains only the route, status, attempts, and result count.
/// 中文摘要：保留原 REST 响应并提供标准化期权报价列表；标准化不替代行情新鲜度判断。
pub struct NormalizedOptionQuoteReadResponse {
    response: RestResponse,
    quotes: Vec<NormalizedOptionQuote>,
}

impl NormalizedOptionQuoteReadResponse {
    /// Borrows the bounded REST response and its status/rate-limit metadata.
    /// 借用有界 REST 响应及其状态和限流元数据。
    #[must_use]
    pub const fn response(&self) -> &RestResponse {
        &self.response
    }

    /// Borrows normalized quote rows in request order; no freshness or tradability decision is made.
    /// 按请求顺序借用归一化报价；不作 freshness 或 tradability 判断。
    #[must_use]
    pub fn quotes(&self) -> &[NormalizedOptionQuote] {
        &self.quotes
    }

    /// Consumes the result and returns its raw REST response and normalized quote rows.
    /// 消费结果并拆出原始 REST 响应与归一化报价列表。
    #[must_use]
    pub fn into_parts(self) -> (RestResponse, Vec<NormalizedOptionQuote>) {
        (self.response, self.quotes)
    }
}

impl fmt::Debug for NormalizedOptionQuoteReadResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedOptionQuoteReadResponse")
            .field("route", &self.response.endpoint().route_name())
            .field("status", &self.response.status())
            .field("attempts", &self.response.attempts())
            .field("quote_count", &self.quotes.len())
            .finish()
    }
}

impl<P, T> SchwabRestClient<P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Fetches and structurally normalizes option quotes with one GET.
    ///
    /// `fields: None` keeps the Node SDK default of `quote,reference`; an
    /// explicit empty `CsvValues` sends `fields=`. `observed_at_ms` is supplied
    /// by the caller so quote-age diagnostics remain deterministic. The
    /// returned projection does not establish freshness or tradeability.
    /// 中文摘要：读取规范化期权报价并同时保留原始响应元数据；不判定 freshness 或 tradability。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn normalized_option_quotes<I, S>(
        &self,
        symbols: I,
        fields: Option<CsvValues>,
        observed_at_ms: i64,
    ) -> Result<NormalizedOptionQuoteReadResponse, ReadApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let normalized_symbols = normalize_option_quote_symbols(symbols)?;
        let request = ReadRequest::option_quotes_with_fields(
            normalized_symbols.iter().map(String::as_str),
            fields,
        )?;
        let requested_symbols = normalized_symbols
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let typed = self.read_typed(request).await?;
        let quotes = match typed
            .parsed()
            .option_quotes(&requested_symbols, observed_at_ms)
        {
            Ok(quotes) => quotes,
            Err(source) => {
                return Err(ReadApiError::Response {
                    response: typed.into_response(),
                    source,
                });
            }
        };
        Ok(NormalizedOptionQuoteReadResponse {
            response: typed.into_response(),
            quotes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccessToken, BoxFuture, HttpMethod, HttpRequest, HttpResponse, HttpTransportError,
        ReadResponseError, TokenProviderError,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const TOKEN: &str = "synthetic-normalized-option-token";
    const LONG: &str = "QQQ   260814P00740000";
    const SHORT: &str = "QQQ   260814P00739000";
    const OBSERVED_AT_MS: i64 = 1_700_000_001_000;

    struct FakeTokens(Arc<AtomicUsize>);

    impl AccessTokenProvider for FakeTokens {
        fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { AccessToken::new(TOKEN) })
        }
    }

    struct FakeTransport {
        calls: Arc<AtomicUsize>,
        target: Arc<Mutex<Vec<String>>>,
        body: Vec<u8>,
    }

    impl HttpTransport for FakeTransport {
        fn send(
            &self,
            request: HttpRequest,
        ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.target
                .lock()
                .expect("synthetic request lock")
                .push(request.endpoint().path().to_owned());
            assert_eq!(request.method(), HttpMethod::Get);
            assert!(request.with_bearer_token(|token| token == TOKEN));
            let body = self.body.clone();
            Box::pin(async move {
                let response = HttpResponse::new(
                    200,
                    vec![
                        ("Retry-After".to_owned(), b"17".to_vec()),
                        ("X-RateLimit-Remaining".to_owned(), b"42".to_vec()),
                    ],
                    body,
                )
                .map_err(|_| HttpTransportError::InvalidResponse)?;
                request.observe_synthetic_response_head(&response).await?;
                Ok(response)
            })
        }
    }

    type TestClient = SchwabRestClient<FakeTokens, FakeTransport>;
    type TestClientParts = (
        TestClient,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
    );

    fn fake_client(body: &str) -> TestClientParts {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let transport_calls = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(Mutex::new(Vec::new()));
        let client = SchwabRestClient::new(
            FakeTokens(Arc::clone(&token_calls)),
            FakeTransport {
                calls: Arc::clone(&transport_calls),
                target: Arc::clone(&target),
                body: body.as_bytes().to_vec(),
            },
            crate::tests::test_read_admission_gate(),
        );
        (client, token_calls, transport_calls, target)
    }

    const QUOTES: &str = r#"{
        "QQQ   260814P00740000": {
            "assetMainType": "OPTION",
            "symbol": "QQQ   260814P00740000",
            "realtime": true,
            "reference": {
                "underlying": "QQQ",
                "contractType": "PUT",
                "strikePrice": 740,
                "expirationYear": 2026,
                "expirationMonth": 8,
                "expirationDay": 14
            },
            "quote": {"bidPrice": 29.9, "askPrice": 30.1, "quoteTime": 1700000000000}
        },
        "QQQ   260814P00739000": {
            "assetMainType": "OPTION",
            "symbol": "QQQ   260814P00739000",
            "realtime": true,
            "reference": {
                "underlying": "QQQ",
                "contractType": "PUT",
                "strikePrice": 739,
                "expirationYear": 2026,
                "expirationMonth": 8,
                "expirationDay": 14
            },
            "quote": {"bidPrice": 28.95, "askPrice": 29.05, "quoteTime": 1700000000000}
        }
    }"#;

    #[tokio::test]
    async fn normalized_option_quotes_preserve_the_single_get_contract_and_metadata() {
        let (client, token_calls, transport_calls, targets) = fake_client(QUOTES);
        let result = client
            .normalized_option_quotes(
                [format!("{LONG}  "), "   ".to_owned(), format!("{SHORT}\t")],
                None,
                OBSERVED_AT_MS,
            )
            .await
            .expect("synthetic quote read");

        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            targets.lock().expect("synthetic request lock").as_slice(),
            &[
                "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000%2CQQQ+++260814P00739000&fields=quote%2Creference"
            ]
        );
        assert_eq!(
            result.response().endpoint().route_name(),
            "market-option-quotes"
        );
        assert_eq!(result.response().attempts(), 1);
        assert!(
            result.response().headers().any(|(name, value)| {
                name.eq_ignore_ascii_case("retry-after") && value == b"17"
            })
        );
        assert!(result.response().headers().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-ratelimit-remaining") && value == b"42"
        }));

        let quotes = result.quotes();
        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes[0].symbol, LONG);
        assert_eq!(quotes[0].underlying.as_deref(), Some("QQQ"));
        assert_eq!(quotes[0].contract_type.as_deref(), Some("PUT"));
        assert_eq!(quotes[0].expiration.as_deref(), Some("2026-08-14"));
        assert_eq!(
            quotes[0].mid.as_ref().map(ToString::to_string).as_deref(),
            Some("30")
        );
        assert_eq!(
            quotes[0]
                .spread
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("0.2")
        );
        assert_eq!(
            quotes[0]
                .quote_age_ms
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("1000")
        );
        assert_eq!(quotes[1].symbol, SHORT);
        assert_eq!(
            quotes[1].mid.as_ref().map(ToString::to_string).as_deref(),
            Some("29")
        );

        let debug = format!("{result:?}");
        assert!(!debug.contains(LONG));
        assert!(!debug.contains("29.9"));
        assert!(!debug.contains("1700000000000"));
    }

    #[tokio::test]
    async fn quote_projection_error_keeps_the_successful_rest_response() {
        let (client, _, transport_calls, _) = fake_client("{}");
        let error = client
            .normalized_option_quotes([LONG], None, OBSERVED_AT_MS)
            .await
            .expect_err("missing quote row must fail closed");

        assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
        assert_eq!(error.code(), "REST_OPTION_QUOTE_UNAVAILABLE");
        let response = error
            .response()
            .expect("successful HTTP response is retained");
        assert_eq!(response.status(), 200);
        assert!(
            response.headers().any(|(name, value)| {
                name.eq_ignore_ascii_case("retry-after") && value == b"17"
            })
        );
        assert_eq!(
            error.response_error(),
            Some(ReadResponseError::QuoteUnavailable)
        );
    }

    #[tokio::test]
    async fn explicit_empty_fields_remain_present_in_the_query() {
        let (client, _, transport_calls, targets) = fake_client(QUOTES);
        let result = client
            .normalized_option_quotes([LONG], Some(CsvValues::empty()), OBSERVED_AT_MS)
            .await
            .expect("synthetic quote read with explicit empty fields");

        assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.quotes().len(), 1);
        assert_eq!(
            targets.lock().expect("synthetic request lock").as_slice(),
            &["/marketdata/v1/quotes?symbols=QQQ+++260814P00740000&fields="]
        );
    }
}
