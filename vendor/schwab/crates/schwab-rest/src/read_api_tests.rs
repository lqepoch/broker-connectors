use super::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FIXTURE: &str = include_str!("../../../test/fixtures/rust-v2/sdk_read_surface.json");
const QUERY_EXTENSION_FIXTURE: &str = include_str!("../fixtures/query_extensions.json");
const FAKE_TOKEN: &str = "synthetic-read-token-never-a-credential";
const ACCOUNT_HASH: &str = "synthetic-hash";
const BODY_MARKER: &[u8] = b"synthetic-private-response-body";

const FIXTURE_IDS: &[&str] = &[
    "trader-account-number-hash-list",
    "trader-accounts-positions-field",
    "trader-account-hash-path-encoding",
    "trader-orders-query-preserves-false-zero-and-omits-undefined",
    "trader-order-id-path",
    "trader-cross-account-orders-query",
    "trader-transactions-query",
    "trader-transaction-object-response",
    "trader-transaction-array-convenience-selects-first",
    "trader-empty-transaction-array-throws-not-found",
    "trader-user-preference-object-shape",
    "trader-streamer-info-array-preference-selects-first",
    "trader-streamer-info-missing-preference-fails",
    "market-batch-quotes-joins-symbols-and-fields",
    "market-single-symbol-quote-path-encoding",
    "market-option-quote-normalizes-contract-and-nbbo",
    "market-vertical-option-quote-derives-two-leg-market",
    "market-option-chain-forwards-all-supported-fields",
    "market-option-chain-deprecated-include-quotes-alias",
    "market-option-expiration-chain-query",
    "market-price-history-query-preserves-zero-and-booleans",
    "market-movers-encodes-path-symbol",
    "market-batch-hours-joins-market-list",
    "market-single-hours-date-query",
    "market-instrument-search-joins-symbols",
    "market-instrument-cusip-path-encoding",
    "trader-204-empty-account-response-bypasses-schema",
    "market-200-empty-body-bypasses-required-history-schema",
    "trader-invalid-success-envelope-fails-schema-validation",
    "market-invalid-success-envelope-fails-schema-validation",
    "trader-http-400-preserves-broker-error-status-and-body",
    "market-http-429-retries-only-through-read-retry-policy",
    "trader-account-numbers-body-wrapper",
    "trader-accounts-body-wrapper-forwards-fields",
    "trader-account-body-wrapper-forwards-hash-and-fields",
    "trader-orders-body-wrapper-forwards-range-and-status",
    "trader-order-body-wrapper-forwards-id",
    "trader-cross-account-orders-body-wrapper-forwards-query",
    "trader-transactions-body-wrapper-forwards-query",
    "trader-user-preferences-body-wrapper",
    "market-batch-quotes-body-wrapper-forwards-query",
    "market-single-quote-body-wrapper-forwards-fields",
    "market-batch-option-quotes-body-wrapper-normalizes-each-contract",
    "market-option-chain-body-wrapper-forwards-filters",
    "market-option-expiration-body-wrapper-forwards-filters",
    "market-price-history-body-wrapper-forwards-all-fields",
    "market-movers-body-wrapper-forwards-sort-frequency",
    "market-batch-hours-body-wrapper-joins-market-list",
    "market-single-hours-body-wrapper-forwards-date",
    "market-instrument-search-body-wrapper-joins-symbol-list",
    "market-cusip-body-wrapper-encodes-path",
];

struct FakeTokens {
    calls: Arc<AtomicUsize>,
}

impl AccessTokenProvider for FakeTokens {
    fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { AccessToken::new(FAKE_TOKEN) })
    }
}

#[derive(Clone)]
struct FakeTransport {
    calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<ObservedRequest>>>,
    status: u16,
    response_body: Vec<u8>,
}

#[derive(Clone, Debug)]
struct ObservedRequest {
    method: HttpMethod,
    target: String,
    token_matches: bool,
}

impl HttpTransport for FakeTransport {
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed
            .lock()
            .expect("test request mutex")
            .push(ObservedRequest {
                method: request.method(),
                target: request.endpoint().path().to_owned(),
                token_matches: request.with_bearer_token(|token| token == FAKE_TOKEN),
            });
        let status = self.status;
        let response_body = self.response_body.clone();
        Box::pin(async move {
            let response = HttpResponse::new(
                status,
                vec![
                    ("Retry-After".to_owned(), b"17".to_vec()),
                    ("X-RateLimit-Remaining".to_owned(), b"42".to_vec()),
                ],
                response_body,
            )
            .map_err(|_| HttpTransportError::InvalidResponse)?;
            request.observe_synthetic_response_head(&response).await?;
            Ok(response)
        })
    }
}

type FakeClient = SchwabRestClient<FakeTokens, FakeTransport>;
type FakeClientParts = (
    FakeClient,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<ObservedRequest>>>,
);

fn fake_client(status: u16) -> FakeClientParts {
    fake_client_with_body(status, BODY_MARKER.to_vec())
}

fn fake_client_with_body(status: u16, response_body: Vec<u8>) -> FakeClientParts {
    fake_client_with_gate(
        status,
        response_body,
        crate::tests::test_read_admission_gate(),
    )
}

fn fake_client_with_gate(
    status: u16,
    response_body: Vec<u8>,
    read_admission: Arc<dyn ReadAdmissionPort>,
) -> FakeClientParts {
    let token_calls = Arc::new(AtomicUsize::new(0));
    let transport_calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let client = SchwabRestClient::new(
        FakeTokens {
            calls: Arc::clone(&token_calls),
        },
        FakeTransport {
            calls: Arc::clone(&transport_calls),
            observed: Arc::clone(&observed),
            status,
            response_body,
        },
        read_admission,
    );
    (client, token_calls, transport_calls, observed)
}

#[derive(Clone, Default)]
struct PriorityRecorder(Arc<Mutex<Vec<ReadPriority>>>);

impl ReadAdmissionPort for PriorityRecorder {
    fn admit(
        &self,
        priority: ReadPriority,
        _maximum_wait: Duration,
    ) -> BoxFuture<'_, Result<(), ReadAdmissionError>> {
        self.0
            .lock()
            .expect("priority recorder lock")
            .push(priority);
        Box::pin(async { Ok(()) })
    }

    fn observe_rate_limit_headers<'a>(
        &'a self,
        _values: &'a [&'a [u8]],
    ) -> BoxFuture<'a, Result<(), ReadAdmissionError>> {
        Box::pin(async { Ok(()) })
    }
}

fn priority_test_gate() -> Arc<PriorityRecorder> {
    Arc::new(PriorityRecorder::default())
}

async fn expect_urgent_typed_read<F>(read: F)
where
    F: Future<Output = Result<TypedReadResponse, ReadApiError>>,
{
    let result = tokio::time::timeout(Duration::from_millis(250), read).await;
    assert!(
        matches!(result, Ok(Ok(_))),
        "authority read must use Urgent"
    );
}

fn q(value: &str) -> QueryText {
    QueryText::new(value).expect("static fixture query text")
}

fn path(value: &str) -> PathIdentifier {
    PathIdentifier::new(value).expect("static fixture path identifier")
}

fn broker_id(value: &str) -> BrokerIdentifier {
    BrokerIdentifier::new(value).expect("static fixture broker identifier")
}

fn csv(values: &[&str]) -> CsvValues {
    CsvValues::from_values(values.iter().copied()).expect("static fixture CSV values")
}

fn order_query(
    from: &str,
    to: &str,
    status: Option<&str>,
    max_results: Option<i64>,
) -> OrdersQuery {
    OrdersQuery {
        from_entered_time: q(from),
        to_entered_time: q(to),
        max_results,
        status: status.map(q),
    }
}

fn transaction_query(
    start: &str,
    end: &str,
    types: &str,
    symbol: Option<&str>,
) -> TransactionsQuery {
    TransactionsQuery {
        start_date: q(start),
        end_date: q(end),
        types: q(types),
        symbol: symbol.map(q),
    }
}

// Keep the exhaustive fixture-ID to request mapping together for review.
#[allow(clippy::too_many_lines)]
fn request_for_case(id: &str) -> ReadRequest {
    match id {
        "trader-account-number-hash-list"
        | "trader-account-numbers-body-wrapper"
        | "trader-http-400-preserves-broker-error-status-and-body"
        | "trader-invalid-success-envelope-fails-schema-validation" => ReadRequest::AccountNumbers,
        "trader-accounts-positions-field" | "trader-accounts-body-wrapper-forwards-fields" => {
            ReadRequest::Accounts(AccountsQuery {
                fields: Some(q("positions")),
            })
        }
        "trader-204-empty-account-response-bypasses-schema" => {
            ReadRequest::Accounts(AccountsQuery::default())
        }
        "trader-account-hash-path-encoding" => ReadRequest::Account {
            account_hash: path(" synthetic/hash+ "),
            query: AccountsQuery {
                fields: Some(q("positions")),
            },
        },
        "trader-account-body-wrapper-forwards-hash-and-fields" => ReadRequest::Account {
            account_hash: path(ACCOUNT_HASH),
            query: AccountsQuery {
                fields: Some(q("positions")),
            },
        },
        "trader-orders-query-preserves-false-zero-and-omits-undefined" => {
            let mut extensions = QueryExtensions::new();
            extensions
                .push_optional_bool("includeSomething", Some(false))
                .expect("valid synthetic boolean extension");
            extensions
                .push_optional_text("omitThis", None)
                .expect("undefined/null extension is omitted");
            ReadRequest::orders_with_extensions(
                ACCOUNT_HASH,
                order_query(
                    "2026-08-12T09:30:00-04:00",
                    "2026-08-12T16:00:00-04:00",
                    Some("WORKING"),
                    Some(0),
                ),
                extensions,
            )
            .expect("valid account orders request")
        }
        "trader-orders-body-wrapper-forwards-range-and-status" => ReadRequest::Orders {
            account_hash: path(ACCOUNT_HASH),
            query: order_query("start", "end", Some("WORKING"), Some(10)),
        },
        "trader-order-id-path" => ReadRequest::Order {
            account_hash: path(ACCOUNT_HASH),
            order_id: broker_id("9007199254740993"),
        },
        "trader-order-body-wrapper-forwards-id" => ReadRequest::Order {
            account_hash: path(ACCOUNT_HASH),
            order_id: broker_id("42"),
        },
        "trader-cross-account-orders-query"
        | "trader-cross-account-orders-body-wrapper-forwards-query" => {
            ReadRequest::OrdersAcrossAccounts(order_query("start", "end", Some("FILLED"), None))
        }
        "trader-transactions-query" => ReadRequest::Transactions {
            account_hash: path(ACCOUNT_HASH),
            query: transaction_query("2026-08-12", "2026-08-13", "TRADE", Some("QQQ")),
        },
        "trader-transactions-body-wrapper-forwards-query" => ReadRequest::Transactions {
            account_hash: path(ACCOUNT_HASH),
            query: transaction_query("start", "end", "TRADE", Some("QQQ")),
        },
        "trader-transaction-object-response"
        | "trader-transaction-array-convenience-selects-first"
        | "trader-empty-transaction-array-throws-not-found" => ReadRequest::Transaction {
            account_hash: path(ACCOUNT_HASH),
            transaction_id: broker_id(match id {
                "trader-transaction-object-response" => "43",
                "trader-transaction-array-convenience-selects-first" => "44",
                _ => "46",
            }),
        },
        "trader-user-preference-object-shape"
        | "trader-streamer-info-array-preference-selects-first"
        | "trader-streamer-info-missing-preference-fails"
        | "trader-user-preferences-body-wrapper" => ReadRequest::UserPreferences,
        "market-batch-quotes-joins-symbols-and-fields" => ReadRequest::Quotes(QuotesQuery {
            symbols: csv(&["QQQ", "QQQ   260814P00740000"]),
            fields: Some(csv(&["quote", "fundamental"])),
            indicative: Some(true),
        }),
        "market-batch-quotes-body-wrapper-forwards-query" => ReadRequest::Quotes(QuotesQuery {
            symbols: csv(&["QQQ", "SPY"]),
            fields: Some(csv(&["quote", "reference"])),
            indicative: Some(false),
        }),
        "market-http-429-retries-only-through-read-retry-policy" => {
            ReadRequest::Quotes(QuotesQuery {
                symbols: csv(&["QQQ"]),
                fields: None,
                indicative: None,
            })
        }
        "market-single-symbol-quote-path-encoding" => ReadRequest::Quote {
            symbol: path("QQQ   260814P00740000"),
            fields: Some(csv(&["quote", "reference"])),
        },
        "market-single-quote-body-wrapper-forwards-fields" => ReadRequest::Quote {
            symbol: path("QQQ"),
            fields: Some(csv(&["quote", "fundamental"])),
        },
        "market-option-quote-normalizes-contract-and-nbbo" => ReadRequest::OptionQuotes {
            symbols: csv(&["QQQ   260814P00740000"]),
            fields: Some(csv(&["quote", "reference"])),
        },
        "market-batch-option-quotes-body-wrapper-normalizes-each-contract" => {
            ReadRequest::OptionQuotes {
                symbols: csv(&["QQQ   260814P00740000", "QQQ   260814P00739000"]),
                fields: Some(csv(&["quote", "reference"])),
            }
        }
        "market-vertical-option-quote-derives-two-leg-market" => ReadRequest::VerticalOptionQuote {
            long_symbol: q("QQQ   260814P00740000"),
            short_symbol: q("QQQ   260814P00739000"),
        },
        "market-option-chain-forwards-all-supported-fields" => {
            ReadRequest::OptionChains(Box::new(OptionChainQuery {
                symbol: q("QQQ"),
                contract_type: Some(q("PUT")),
                include_underlying_quote: Some(true),
                include_quotes: None,
                strategy: Some(q("VERTICAL")),
                interval: Some(1),
                strike_count: Some(10),
                strike: Some(DecimalQuery::new("400").expect("decimal fixture")),
                range: Some(q("OTM")),
                from_date: Some(q("2026-08-12")),
                to_date: Some(q("2026-08-13")),
                volatility: Some(DecimalQuery::new("0.2").expect("decimal fixture")),
                underlying_price: Some(DecimalQuery::new("400").expect("decimal fixture")),
                interest_rate: Some(DecimalQuery::new("0.05").expect("decimal fixture")),
                days_to_expiration: Some(0),
                exp_month: Some(q("AUG")),
                option_type: Some(q("ALL")),
                entitlement: Some(q("NP")),
            }))
        }
        "market-option-chain-deprecated-include-quotes-alias" => {
            ReadRequest::OptionChains(Box::new(OptionChainQuery {
                symbol: q("QQQ"),
                contract_type: None,
                include_underlying_quote: None,
                include_quotes: Some(true),
                strategy: None,
                interval: None,
                strike_count: None,
                strike: None,
                range: None,
                from_date: None,
                to_date: None,
                volatility: None,
                underlying_price: None,
                interest_rate: None,
                days_to_expiration: None,
                exp_month: None,
                option_type: None,
                entitlement: None,
            }))
        }
        "market-option-chain-body-wrapper-forwards-filters" => {
            ReadRequest::OptionChains(Box::new(OptionChainQuery {
                symbol: q("QQQ"),
                contract_type: Some(q("PUT")),
                include_underlying_quote: None,
                include_quotes: Some(false),
                strategy: Some(q("SINGLE")),
                interval: None,
                strike_count: None,
                strike: None,
                range: Some(q("OTM")),
                from_date: None,
                to_date: None,
                volatility: None,
                underlying_price: None,
                interest_rate: None,
                days_to_expiration: Some(0),
                exp_month: None,
                option_type: None,
                entitlement: None,
            }))
        }
        "market-option-expiration-chain-query" => {
            ReadRequest::OptionExpirationChain(OptionExpirationQuery {
                symbol: q("QQQ"),
                contract_type: Some(q("ALL")),
                exp_month: Some(q("AUG")),
                option_type: Some(q("ALL")),
            })
        }
        "market-option-expiration-body-wrapper-forwards-filters" => {
            ReadRequest::OptionExpirationChain(OptionExpirationQuery {
                symbol: q("QQQ"),
                contract_type: Some(q("PUT")),
                exp_month: Some(q("AUG")),
                option_type: Some(q("ALL")),
            })
        }
        "market-price-history-query-preserves-zero-and-booleans" => {
            ReadRequest::PriceHistory(PriceHistoryQuery {
                symbol: q("QQQ"),
                period_type: Some(q("day")),
                period: Some(0),
                frequency_type: Some(q("minute")),
                frequency: Some(5),
                start_date: Some(0),
                end_date: Some(2),
                need_extended_hours_data: Some(false),
                need_previous_close: Some(true),
            })
        }
        "market-200-empty-body-bypasses-required-history-schema" => {
            ReadRequest::PriceHistory(PriceHistoryQuery {
                symbol: q("QQQ"),
                period_type: None,
                period: None,
                frequency_type: None,
                frequency: None,
                start_date: None,
                end_date: None,
                need_extended_hours_data: None,
                need_previous_close: None,
            })
        }
        "market-price-history-body-wrapper-forwards-all-fields" => {
            ReadRequest::PriceHistory(PriceHistoryQuery {
                symbol: q("QQQ"),
                period_type: Some(q("day")),
                period: Some(1),
                frequency_type: Some(q("minute")),
                frequency: Some(5),
                start_date: Some(100),
                end_date: Some(200),
                need_extended_hours_data: Some(true),
                need_previous_close: Some(true),
            })
        }
        "market-movers-encodes-path-symbol" => ReadRequest::Movers {
            symbol: path("$DJI"),
            query: MoversQuery {
                sort: Some(q("VOLUME")),
                frequency: Some(30),
            },
        },
        "market-invalid-success-envelope-fails-schema-validation" => ReadRequest::Movers {
            symbol: path("$DJI"),
            query: MoversQuery::default(),
        },
        "market-movers-body-wrapper-forwards-sort-frequency" => ReadRequest::Movers {
            symbol: path("$DJI"),
            query: MoversQuery {
                sort: Some(q("PERCENT_CHANGE_DOWN")),
                frequency: Some(60),
            },
        },
        "market-batch-hours-joins-market-list"
        | "market-batch-hours-body-wrapper-joins-market-list" => {
            ReadRequest::Markets(MarketsQuery {
                markets: csv(&["EQUITY", "OPTION"]),
                date: Some(q("2026-08-12")),
            })
        }
        "market-single-hours-date-query" | "market-single-hours-body-wrapper-forwards-date" => {
            ReadRequest::MarketHours {
                market: path("OPTION"),
                query: MarketHoursQuery {
                    date: Some(q("2026-08-12")),
                },
            }
        }
        "market-instrument-search-joins-symbols"
        | "market-instrument-search-body-wrapper-joins-symbol-list" => {
            ReadRequest::SearchInstruments(InstrumentSearchQuery {
                symbols: csv(&["QQQ", "SPY"]),
                projection: q("SYMBOL_SEARCH"),
            })
        }
        "market-instrument-cusip-path-encoding" | "market-cusip-body-wrapper-encodes-path" => {
            ReadRequest::InstrumentByCusip(path("12 345"))
        }
        _ => panic!("unmapped read-surface fixture case: {id}"),
    }
}

#[test]
fn every_node_read_surface_vector_builds_one_allowlisted_get_target() {
    assert_eq!(FIXTURE.matches("\"id\": \"").count(), FIXTURE_IDS.len());
    for id in FIXTURE_IDS {
        let (expected_path, expected_query) = fixture_expected_request(id);
        let (client, token_calls, transport_calls, observed) = fake_client(200);
        let result = block_on(client.read(request_for_case(id))).expect("synthetic GET response");
        assert_eq!(result.method(), HttpMethod::Get, "fixture {id}");
        assert_eq!(result.attempts(), 1, "fixture {id}");
        assert_eq!(token_calls.load(Ordering::SeqCst), 1, "fixture {id}");
        assert_eq!(transport_calls.load(Ordering::SeqCst), 1, "fixture {id}");
        let observations = observed.lock().expect("test request mutex");
        assert_eq!(observations.len(), 1, "fixture {id}");
        let request = &observations[0];
        assert_eq!(request.method, HttpMethod::Get, "fixture {id}");
        assert!(request.token_matches, "fixture {id}");

        let url = reqwest::Url::parse(&format!("https://fixture.invalid{}", request.target))
            .expect("valid builder target");
        assert_eq!(url.path(), expected_path, "fixture {id}");
        let actual_query = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_query, expected_query, "fixture {id}");
        assert!(!request.target.contains(FAKE_TOKEN), "fixture {id}");
        assert!(!request.target.contains("omitThis"), "fixture {id}");
    }
}

#[test]
fn every_characterized_route_has_an_attached_response_family() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).expect("read-surface fixture");
    let cases = fixture["cases"].as_array().expect("fixture cases");
    for id in FIXTURE_IDS {
        let request = request_for_case(id);
        let endpoint = request.endpoint().expect("fixture route");
        let case = cases
            .iter()
            .find(|case| case["id"] == *id)
            .unwrap_or_else(|| panic!("missing fixture case {id}"));
        let method = case["method"].as_str().expect("fixture method");
        assert!(
            ReadResponseKind::for_endpoint(&endpoint).is_some(),
            "no response family for fixture {id} route {}",
            endpoint.route_name()
        );
        assert_eq!(
            ReadResponseKind::for_endpoint(&endpoint),
            Some(kind_for_fixture_method(method)),
            "route-aware response family differs from Node read vector {id}"
        );
    }
}

#[test]
fn trader_query_extensions_match_node_value_and_urlsearchparams_serialization() {
    let fixture: Value =
        serde_json::from_str(QUERY_EXTENSION_FIXTURE).expect("query extension fixture JSON");
    let case = &fixture["cases"][0];
    let mut orders_extensions = QueryExtensions::new();
    for extension in case["extensions"]
        .as_array()
        .expect("fixture extension rows")
    {
        let key = extension["key"].as_str().expect("fixture extension key");
        match extension["kind"].as_str().expect("fixture extension kind") {
            "boolean" => orders_extensions
                .push_optional_bool(key, extension["value"].as_bool())
                .expect("valid boolean extension"),
            "decimal" => orders_extensions
                .push_optional_number(key, extension["value"].as_str())
                .expect("valid decimal extension"),
            "text" => orders_extensions
                .push_optional_text(key, extension["value"].as_str())
                .expect("valid text extension"),
            "omit" => orders_extensions
                .push_optional_text(key, None)
                .expect("undefined/null extension is omitted"),
            kind => panic!("unmapped query extension fixture kind: {kind}"),
        }
    }
    let expected_extension_query = case["expectedQuery"]
        .as_str()
        .expect("fixture expected extension query");

    let mut cross_account_extensions = QueryExtensions::new();
    cross_account_extensions
        .push_optional_text("cursor", Some("next/page"))
        .expect("valid cross-account extension");

    let mut transaction_extensions = QueryExtensions::new();
    transaction_extensions
        .push_optional_bool("includeDetails", Some(true))
        .expect("valid transaction extension");

    let (client, token_calls, transport_calls, observed) =
        fake_client_with_body(200, b"[]".to_vec());
    block_on(async {
        client
            .orders_with_query_extensions(
                ACCOUNT_HASH,
                order_query("start time", "end+time", Some("WORKING"), Some(0)),
                orders_extensions,
            )
            .await
            .expect("account orders response");
        client
            .orders_across_accounts_with_query_extensions(
                order_query("start", "end", None, None),
                cross_account_extensions,
            )
            .await
            .expect("cross-account orders response");
        client
            .transactions_with_query_extensions(
                ACCOUNT_HASH,
                transaction_query("start", "end", "TRADE", None),
                transaction_extensions,
            )
            .await
            .expect("transactions response");
    });

    assert_eq!(token_calls.load(Ordering::SeqCst), 3);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 3);
    let observed = observed.lock().expect("test request mutex");
    assert_eq!(
        observed[0].target,
        format!(
            "/trader/v1/accounts/synthetic-hash/orders?fromEnteredTime=start+time&toEnteredTime=end%2Btime&maxResults=0&status=WORKING&{expected_extension_query}"
        )
    );
    assert_eq!(
        observed[1].target,
        "/trader/v1/orders?fromEnteredTime=start&toEnteredTime=end&cursor=next%2Fpage"
    );
    assert_eq!(
        observed[2].target,
        "/trader/v1/accounts/synthetic-hash/transactions?startDate=start&endDate=end&types=TRADE&includeDetails=true"
    );
    assert!(
        observed
            .iter()
            .all(|request| request.method == HttpMethod::Get)
    );
    assert!(observed.iter().all(|request| request.token_matches));
}

#[test]
fn query_extensions_are_bounded_redacted_and_cannot_override_the_fixed_route_contract() {
    let mut extensions = QueryExtensions::new();
    extensions
        .push_optional_text("diagnostic", Some("synthetic-private-query-marker"))
        .expect("valid extension");
    assert_eq!(
        extensions.push_optional_bool("diagnostic", Some(false)),
        Err(ReadRequestError::DuplicateQueryParameter)
    );
    assert_eq!(
        extensions.push_optional_text("", Some("value")),
        Err(ReadRequestError::InvalidQueryKey)
    );
    assert_eq!(
        extensions.push_optional_text("bad\nkey", Some("value")),
        Err(ReadRequestError::InvalidQueryKey)
    );
    assert_eq!(
        extensions.push_optional_number("notFinite", Some("NaN")),
        Err(ReadRequestError::InvalidQueryValue)
    );
    let oversized = "x".repeat(4_097);
    assert_eq!(
        extensions.push_optional_text("oversized", Some(&oversized)),
        Err(ReadRequestError::InvalidQueryValue)
    );
    let debug = format!("{extensions:?}");
    assert!(!debug.contains("diagnostic"));
    assert!(!debug.contains("synthetic-private-query-marker"));

    let mut reserved = QueryExtensions::new();
    reserved
        .push_optional_text("status", Some("FILLED"))
        .expect("query extension can be built before route-specific validation");
    let request = ReadRequest::orders_with_extensions(
        ACCOUNT_HASH,
        order_query("start", "end", Some("WORKING"), None),
        reserved,
    )
    .expect("valid account orders request");
    assert_eq!(
        request.endpoint().unwrap_err(),
        ReadRequestError::ConflictingQueryParameter
    );

    let mut too_many = QueryExtensions::new();
    for index in 0..100 {
        too_many
            .push_optional_bool(format!("extra{index}"), Some(false))
            .expect("within extension builder limit");
    }
    assert_eq!(
        too_many.push_optional_bool("extra-over-limit", Some(false)),
        Err(ReadRequestError::TooManyQueryParameters)
    );
    let request = ReadRequest::orders_with_extensions(
        ACCOUNT_HASH,
        order_query("start", "end", None, None),
        too_many,
    )
    .expect("account orders request before combined limit check");
    assert_eq!(
        request.endpoint().unwrap_err(),
        ReadRequestError::TooManyQueryParameters
    );
}

fn kind_for_fixture_method(method: &str) -> ReadResponseKind {
    match method {
        "getAccountNumbers" | "getAccountNumbersWithResponse" => ReadResponseKind::AccountNumbers,
        "getAccounts" | "getAccountsWithResponse" => ReadResponseKind::Accounts,
        "getAccount" | "getAccountWithResponse" => ReadResponseKind::Account,
        "getOrders"
        | "getOrdersWithResponse"
        | "getOrdersAcrossAccounts"
        | "getOrdersAcrossAccountsWithResponse" => ReadResponseKind::Orders,
        "getOrder" | "getOrderWithResponse" => ReadResponseKind::Order,
        "getTransactions" | "getTransactionsWithResponse" => ReadResponseKind::Transactions,
        "getTransaction" | "getTransactionWithResponse" => ReadResponseKind::Transaction,
        "getUserPreferences" | "getUserPreferencesWithResponse" | "getStreamerInfo" => {
            ReadResponseKind::UserPreferences
        }
        "getQuotes"
        | "getQuotesWithResponse"
        | "getOptionQuote"
        | "getOptionQuotes"
        | "getVerticalOptionQuote" => ReadResponseKind::Quotes,
        "getQuote" | "getQuoteWithResponse" => ReadResponseKind::SingleQuote,
        "getOptionChains" | "getOptionChainsWithResponse" => ReadResponseKind::OptionChain,
        "getOptionExpirationChain" | "getOptionExpirationChainWithResponse" => {
            ReadResponseKind::OptionExpirationChain
        }
        "getPriceHistory" | "getPriceHistoryWithResponse" => ReadResponseKind::PriceHistory,
        "getMovers" | "getMoversWithResponse" => ReadResponseKind::Movers,
        "getMarkets"
        | "getMarketsWithResponse"
        | "getMarketHours"
        | "getMarketHoursWithResponse" => ReadResponseKind::MarketHours,
        "searchInstruments" | "searchInstrumentsWithResponse" => {
            ReadResponseKind::InstrumentsSearch
        }
        "getInstrumentByCusip" | "getInstrumentByCusipWithResponse" => {
            ReadResponseKind::InstrumentDetail
        }
        _ => panic!("unmapped fixture method: {method}"),
    }
}

#[test]
fn route_aware_response_parser_selects_and_validates_the_account_number_dto() {
    let body = br#"[{"accountNumber":"synthetic-account","hashValue":"synthetic-hash"}]"#;
    let (client, token_calls, transport_calls, _) = fake_client_with_body(200, body.to_vec());
    let response =
        block_on(client.read(ReadRequest::AccountNumbers)).expect("synthetic account-number GET");
    let parsed = ParsedReadResponse::from_endpoint_response(&response)
        .expect("route-selected account-number schema");

    assert_eq!(parsed.kind(), ReadResponseKind::AccountNumbers);
    assert_eq!(
        parsed
            .json()
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    let Some(TraderReadResponse::AccountNumbers(rows)) = parsed.trader_model() else {
        panic!("route-selected account-number DTO");
    };
    assert_eq!(rows[0].account_number, "synthetic-account");
    assert_eq!(rows[0].hash_value, "synthetic-hash");
    assert!(!format!("{parsed:?} {:?}", parsed.trader_model()).contains("synthetic-hash"));
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn route_aware_response_parser_projects_user_preferences_and_typed_streamer_info() {
    let body = br#"{"streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.invalid/ws","schwabClientCustomerId":"synthetic-customer","schwabClientCorrelId":"synthetic-correlation","schwabClientChannel":"N9","schwabClientFunctionId":"synthetic-function","futureField":"preserved"}],"futurePreferenceField":true}"#;
    let (client, token_calls, transport_calls, _) = fake_client_with_body(200, body.to_vec());
    let response =
        block_on(client.read(ReadRequest::UserPreferences)).expect("synthetic preference GET");
    let parsed = ParsedReadResponse::from_endpoint_response(&response)
        .expect("route-selected user-preference schema and DTO");

    assert_eq!(parsed.kind(), ReadResponseKind::UserPreferences);
    let preference = parsed
        .user_preferences_model()
        .and_then(UserPreferencesResponse::first)
        .expect("typed preference");
    assert_eq!(
        preference.unknown_fields.get("futurePreferenceField"),
        Some(&serde_json::Value::Bool(true))
    );
    let streamer = parsed
        .streamer_info_model()
        .expect("typed first streamer info");
    assert_eq!(
        streamer.streamer_socket_url,
        "wss://streamer.example.invalid/ws"
    );
    assert_eq!(streamer.schwab_client_customer_id, "synthetic-customer");
    assert_eq!(
        streamer.unknown_fields.get("futureField"),
        Some(&serde_json::Value::String("preserved".to_owned()))
    );
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn route_aware_response_parser_projects_market_hours_and_instrument_detail() {
    let hours_body = br#"{"EQUITY":{"equity":{"date":"2026-09-28","marketType":"EQUITY","product":"EQUITY","isOpen":false,"sessionHours":{"regularMarket":[]}}}}"#;
    let (client, _, _, _) = fake_client_with_body(200, hours_body.to_vec());
    let response = block_on(client.read(ReadRequest::MarketHours {
        market: path("EQUITY"),
        query: MarketHoursQuery::default(),
    }))
    .expect("synthetic single-market-hours GET");
    let parsed = ParsedReadResponse::from_endpoint_response(&response)
        .expect("route-selected market-hours schema and DTO");
    assert!(matches!(
        parsed.market_model(),
        Some(MarketReadResponse::MarketHours(hours))
            if hours.markets["EQUITY"]["equity"].product == "EQUITY"
    ));

    let instrument_body =
        br#"{"cusip":"synthetic-cusip","symbol":"SYNTH","futureField":"preserved"}"#;
    let (client, _, _, _) = fake_client_with_body(200, instrument_body.to_vec());
    let response = block_on(client.read(ReadRequest::InstrumentByCusip(path("synthetic-cusip"))))
        .expect("synthetic instrument-detail GET");
    let parsed = ParsedReadResponse::from_endpoint_response(&response)
        .expect("route-selected instrument-detail schema and DTO");
    assert!(matches!(
        parsed.market_model(),
        Some(MarketReadResponse::InstrumentDetail(instrument))
            if instrument.symbol.as_deref() == Some("SYNTH")
                && instrument.unknown_fields.get("futureField")
                    == Some(&serde_json::Value::String("preserved".to_owned()))
    ));
}

#[test]
#[allow(clippy::too_many_lines)]
fn route_errors_and_diagnostics_fail_closed_without_revealing_identifiers_or_payloads() {
    assert_eq!(
        PathIdentifier::new(" \t ").unwrap_err(),
        ReadRequestError::InvalidPathIdentifier
    );
    assert_eq!(
        PathIdentifier::new("account\nhash").unwrap_err(),
        ReadRequestError::InvalidPathIdentifier
    );
    assert_eq!(
        PathIdentifier::new("x".repeat(257)).unwrap_err(),
        ReadRequestError::InvalidPathIdentifier
    );
    assert!(BrokerIdentifier::new("9007199254740993").is_ok());
    assert!(BrokerIdentifier::new(i64::MAX.to_string()).is_ok());
    assert_eq!(
        BrokerIdentifier::new("9223372036854775808").unwrap_err(),
        ReadRequestError::InvalidNumericIdentifier
    );
    assert_eq!(
        BrokerIdentifier::new("0").unwrap_err(),
        ReadRequestError::InvalidNumericIdentifier
    );
    assert_eq!(
        QueryText::new("bad\nquery").unwrap_err(),
        ReadRequestError::InvalidQueryValue
    );
    assert_eq!(
        DecimalQuery::new("NaN").unwrap_err(),
        ReadRequestError::InvalidQueryValue
    );
    assert_eq!(
        CsvValues::from_values(Vec::<String>::new()).unwrap_err(),
        ReadRequestError::InvalidList
    );
    assert_eq!(
        CsvValues::from_values(["QQQ,SPY"]).unwrap_err(),
        ReadRequestError::InvalidList,
        "BUG_IN_LEGACY: Node array helpers join embedded commas, ambiguously turning one element into multiple query values; Rust rejects it"
    );
    let csv_string = ReadRequest::Quotes(QuotesQuery {
        symbols: CsvValues::from_csv("QQQ,SPY").expect("explicit CSV string"),
        fields: None,
        indicative: None,
    });
    assert!(
        csv_string
            .endpoint()
            .expect("explicit CSV query")
            .path()
            .contains("symbols=QQQ%2CSPY"),
        "the Node string overload deliberately retains an explicit CSV value"
    );
    assert_eq!(
        ReadRequest::Quotes(QuotesQuery {
            symbols: CsvValues::empty(),
            fields: None,
            indicative: None,
        })
        .endpoint()
        .unwrap_err(),
        ReadRequestError::InvalidList,
        "the explicit empty-field sentinel cannot become an empty required symbol list"
    );

    let too_long = QueryText::new("x".repeat(4_000)).expect("bounded field value");
    let request = ReadRequest::OptionChains(Box::new(OptionChainQuery {
        symbol: too_long.clone(),
        contract_type: Some(too_long.clone()),
        include_underlying_quote: None,
        include_quotes: None,
        strategy: Some(too_long.clone()),
        interval: None,
        strike_count: None,
        strike: None,
        range: Some(too_long.clone()),
        from_date: Some(too_long.clone()),
        to_date: Some(too_long.clone()),
        volatility: None,
        underlying_price: None,
        interest_rate: None,
        days_to_expiration: None,
        exp_month: Some(too_long.clone()),
        option_type: Some(too_long.clone()),
        entitlement: Some(too_long),
    }));
    let (client, token_calls, transport_calls, _) = fake_client(200);
    let error = block_on(client.read(request)).expect_err("oversized target must fail");
    assert_eq!(error.code(), "REST_READ_TARGET_TOO_LONG");
    assert_eq!(error.attempts(), 0);
    assert_eq!(token_calls.load(Ordering::SeqCst), 0);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 0);

    let request = ReadRequest::Account {
        account_hash: path(ACCOUNT_HASH),
        query: AccountsQuery::default(),
    };
    let (client, _, _, _) = fake_client(400);
    let debug = format!("{request:?}");
    assert!(!debug.contains(ACCOUNT_HASH));
    let error = block_on(client.read(request)).expect_err("fake response is successful");
    assert_eq!(error.code(), "REST_HTTP_STATUS");
    let response = error.response().expect("HTTP response retained");
    assert_eq!(response.status(), 400);
    assert!(
        response
            .headers()
            .any(|(name, value)| name == "retry-after" && value == b"17")
    );
    assert!(
        response
            .headers()
            .any(|(name, value)| name == "x-ratelimit-remaining" && value == b"42")
    );
    assert_eq!(response.body(), BODY_MARKER);
    let error_debug = format!("{error:?}");
    assert!(!error_debug.contains(ACCOUNT_HASH));
    assert!(!error_debug.contains(FAKE_TOKEN));
    assert!(!error_debug.contains("synthetic-private-response-body"));
}

#[test]
fn option_quote_constructors_match_node_padding_blank_duplicate_and_default_rules() {
    let symbol = "QQQ   260814P00740000";
    let request =
        ReadRequest::option_quotes(["QQQ   260814P00740000   ", " \t", "QQQ   260814P00739000\t"])
            .expect("valid option quote symbols after Node-compatible normalization");
    let endpoint = request.endpoint().expect("bounded option-quote endpoint");
    assert_eq!(
        endpoint.path(),
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000%2CQQQ+++260814P00739000&fields=quote%2Creference"
    );

    let single = ReadRequest::option_quote("QQQ   260814P00740000   ")
        .expect("single option quote trims only trailing padding");
    assert_eq!(
        single.endpoint().expect("single quote endpoint").path(),
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000&fields=quote%2Creference"
    );

    let bom_padding = ReadRequest::option_quote(format!("{symbol}\u{FEFF}"))
        .expect("ECMAScript trimEnd removes BOM padding");
    assert_eq!(
        bom_padding
            .endpoint()
            .expect("BOM-padded quote endpoint")
            .path(),
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000&fields=quote%2Creference"
    );
    let next_line_suffix = ReadRequest::option_quote(format!("{symbol}\u{0085}"))
        .expect("ECMAScript trimEnd preserves NEXT LINE");
    assert!(
        next_line_suffix
            .endpoint()
            .expect("NEXT LINE quote endpoint")
            .path()
            .contains("%C2%85")
    );

    let empty_fields =
        ReadRequest::option_quotes_with_fields(["QQQ   260814P00740000"], Some(CsvValues::empty()))
            .expect("Node's explicit empty fields array is retained as an empty query value");
    assert_eq!(
        empty_fields
            .endpoint()
            .expect("empty fields remain a bounded endpoint")
            .path(),
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000&fields="
    );

    assert_eq!(
        ReadRequest::option_quotes([" ", "\t"]).unwrap_err(),
        ReadRequestError::InvalidList,
        "Node drops blanks and fails before a request if no symbols remain"
    );
    assert_eq!(
        ReadRequest::option_quotes(["QQQ   260814P00740000", "QQQ   260814P00740000   ",])
            .unwrap_err(),
        ReadRequestError::InvalidList,
        "Node rejects duplicate symbols after trailing whitespace normalization"
    );
    assert_eq!(
        ReadRequest::option_quotes(["QQQ   260814P00740000,SPY   260814P00740000"]).unwrap_err(),
        ReadRequestError::InvalidList,
        "BUG_IN_LEGACY: Node joins an array element containing a comma into multiple symbols; Rust fails closed instead of requesting an ambiguous symbol list"
    );
    assert_eq!(
        ReadRequest::vertical_option_quote("QQQ   260814P00740000   ", "QQQ   260814P00739000\t",)
            .expect("vertical request")
            .endpoint()
            .expect("vertical endpoint")
            .path(),
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000%2CQQQ+++260814P00739000&fields=quote%2Creference"
    );
}

#[test]
fn option_quote_constructor_makes_one_read_only_attempt_with_node_default_fields() {
    let request =
        ReadRequest::option_quotes(["QQQ   260814P00740000   "]).expect("option quote request");
    let (client, token_calls, transport_calls, observed) = fake_client(200);
    let response = block_on(client.read(request)).expect("synthetic response");
    assert_eq!(response.attempts(), 1);
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    let requests = observed.lock().expect("test request mutex");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, HttpMethod::Get);
    assert!(requests[0].token_matches);
    assert_eq!(
        requests[0].target,
        "/marketdata/v1/quotes?symbols=QQQ+++260814P00740000&fields=quote%2Creference"
    );
}

#[test]
fn decimal_query_validation_is_exact_bounded_and_keeps_the_original_lexeme() {
    let request = ReadRequest::OptionChains(Box::new(OptionChainQuery {
        symbol: q("QQQ"),
        contract_type: None,
        include_underlying_quote: None,
        include_quotes: None,
        strategy: None,
        interval: None,
        strike_count: None,
        strike: Some(DecimalQuery::new("9007199254740993").expect("exact large integer")),
        range: None,
        from_date: None,
        to_date: None,
        volatility: Some(DecimalQuery::new("1.0000000000000001").expect("exact precision")),
        underlying_price: None,
        interest_rate: None,
        days_to_expiration: None,
        exp_month: None,
        option_type: None,
        entitlement: None,
    }));
    let endpoint = request.endpoint().expect("bounded option-chain endpoint");
    assert!(endpoint.path().contains("strike=9007199254740993"));
    assert!(endpoint.path().contains("volatility=1.0000000000000001"));

    assert!(DecimalQuery::new("1e-18").is_ok());
    assert_eq!(
        DecimalQuery::new("1e-19").unwrap_err(),
        ReadRequestError::InvalidQueryValue,
        "FAIL_CLOSED_DECIMAL_BOUND: a finite Node number beyond the Rust exact scale is not sent"
    );
    for invalid in ["NaN", "Infinity", "1e100", ".5", "1.", "1..0"] {
        assert_eq!(
            DecimalQuery::new(invalid).unwrap_err(),
            ReadRequestError::InvalidQueryValue,
            "invalid exact decimal query: {invalid}"
        );
    }
}

#[test]
fn encoded_host_and_fragment_injection_remain_path_or_query_data() {
    let request = ReadRequest::Account {
        account_hash: path("//attacker.example/orders#fragment"),
        query: AccountsQuery {
            fields: Some(q("https://attacker.example/#fragment")),
        },
    };
    let endpoint = request.endpoint().expect("typed read route");
    assert!(
        endpoint
            .path()
            .starts_with("/trader/v1/accounts/%2F%2Fattacker.example")
    );
    assert!(endpoint.path().contains("%23fragment"));
    assert!(!endpoint.path().starts_with("//"));
    assert!(!endpoint.path().contains('#'));

    let resolved = reqwest::Url::parse(SCHWAB_API_ROOT)
        .expect("fixed API origin")
        .join(endpoint.path())
        .expect("encoded relative route");
    assert_eq!(resolved.scheme(), "https");
    assert_eq!(resolved.host_str(), Some("api.schwabapi.com"));
    assert_eq!(resolved.port(), None);
    assert!(PathIdentifier::new("orders\r\nHost: attacker.example").is_err());
}

#[test]
fn node_http_error_fixtures_keep_raw_status_headers_and_body_without_retry() {
    for (id, expected_status) in [
        (
            "trader-http-400-preserves-broker-error-status-and-body",
            400,
        ),
        (
            "market-http-429-retries-only-through-read-retry-policy",
            429,
        ),
    ] {
        let (client, token_calls, transport_calls, observed) = fake_client(expected_status);
        let error = block_on(client.read(request_for_case(id))).expect_err("HTTP error expected");
        assert_eq!(error.code(), "REST_HTTP_STATUS");
        assert_eq!(
            error.attempts(),
            1,
            "Rust adapter makes one attempt; budget policy is external"
        );
        let response = error.response().expect("HTTP status response is preserved");
        assert_eq!(response.status(), expected_status);
        assert!(response.headers().any(|(name, _)| name == "retry-after"));
        assert!(
            response
                .headers()
                .any(|(name, _)| name == "x-ratelimit-remaining")
        );
        assert_eq!(response.body(), BODY_MARKER);
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.lock().expect("test request mutex").len(), 1);
        let debug = format!("{error:?}");
        assert!(!debug.contains(FAKE_TOKEN));
        assert!(!debug.contains("synthetic-private-response-body"));
    }
}

fn fixture_expected_request(id: &str) -> (String, BTreeMap<String, String>) {
    let marker = format!("\"id\": \"{id}\"");
    let case_start = FIXTURE
        .find(&marker)
        .unwrap_or_else(|| panic!("missing fixture case {id}"));
    let expected_start = FIXTURE[case_start..]
        .find("\"expectedRequest\"")
        .map_or_else(
            || panic!("missing expectedRequest for {id}"),
            |index| case_start + index,
        );
    let open = FIXTURE[expected_start..].find('{').map_or_else(
        || panic!("missing expectedRequest object for {id}"),
        |index| expected_start + index,
    );
    let close = matching_object_end(FIXTURE.as_bytes(), open).expect("closed fixture object");
    let object = &FIXTURE[open..=close];
    let path = string_field(object, "path").expect("fixture request path");
    let query = string_object_field(object, "query").expect("fixture request query");
    (path, query)
}

fn matching_object_end(input: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in input.iter().copied().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn string_field(object: &str, key: &str) -> Option<String> {
    let mut cursor = field_value_start(object.as_bytes(), key)?;
    parse_json_string(object.as_bytes(), &mut cursor)
}

fn string_object_field(object: &str, key: &str) -> Option<BTreeMap<String, String>> {
    let bytes = object.as_bytes();
    let mut cursor = field_value_start(bytes, key)?;
    skip_ascii_whitespace(bytes, &mut cursor);
    if bytes.get(cursor) != Some(&b'{') {
        return None;
    }
    cursor += 1;
    let mut values = BTreeMap::new();
    loop {
        skip_ascii_whitespace(bytes, &mut cursor);
        match bytes.get(cursor)? {
            b'}' => return Some(values),
            b',' => cursor += 1,
            b'"' => {
                let key = parse_json_string(bytes, &mut cursor)?;
                skip_ascii_whitespace(bytes, &mut cursor);
                if bytes.get(cursor) != Some(&b':') {
                    return None;
                }
                cursor += 1;
                skip_ascii_whitespace(bytes, &mut cursor);
                let value = parse_json_string(bytes, &mut cursor)?;
                if values.insert(key, value).is_some() {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

fn field_value_start(input: &[u8], key: &str) -> Option<usize> {
    let mut cursor = 0usize;
    while cursor < input.len() {
        if input[cursor] != b'"' {
            cursor += 1;
            continue;
        }
        let name = parse_json_string(input, &mut cursor)?;
        skip_ascii_whitespace(input, &mut cursor);
        if input.get(cursor) != Some(&b':') {
            return None;
        }
        cursor += 1;
        skip_ascii_whitespace(input, &mut cursor);
        if name == key {
            return Some(cursor);
        }
        skip_json_value(input, &mut cursor)?;
    }
    None
}

fn skip_json_value(input: &[u8], cursor: &mut usize) -> Option<()> {
    match input.get(*cursor)? {
        b'"' => {
            parse_json_string(input, cursor)?;
        }
        b'{' => {
            *cursor = matching_object_end(input, *cursor)?.checked_add(1)?;
        }
        b'[' => {
            let mut depth = 0usize;
            let mut in_string = false;
            let mut escaped = false;
            while let Some(byte) = input.get(*cursor).copied() {
                *cursor += 1;
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        in_string = false;
                    }
                    continue;
                }
                match byte {
                    b'"' => in_string = true,
                    b'[' => depth += 1,
                    b']' => {
                        depth = depth.checked_sub(1)?;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {
            while input
                .get(*cursor)
                .is_some_and(|byte| !matches!(byte, b',' | b'}' | b']'))
            {
                *cursor += 1;
            }
        }
    }
    Some(())
}

fn parse_json_string(input: &[u8], cursor: &mut usize) -> Option<String> {
    if input.get(*cursor) != Some(&b'"') {
        return None;
    }
    *cursor += 1;
    let mut output = String::new();
    while let Some(byte) = input.get(*cursor).copied() {
        *cursor += 1;
        match byte {
            b'"' => return Some(output),
            b'\\' => {
                let escape = input.get(*cursor).copied()?;
                *cursor += 1;
                match escape {
                    b'"' => output.push('"'),
                    b'\\' => output.push('\\'),
                    b'/' => output.push('/'),
                    b'b' => output.push('\u{0008}'),
                    b'f' => output.push('\u{000c}'),
                    b'n' => output.push('\n'),
                    b'r' => output.push('\r'),
                    b't' => output.push('\t'),
                    _ => return None,
                }
            }
            0..=0x1f => return None,
            0x20..=0x7f => output.push(char::from(byte)),
            _ => {
                let rest = std::str::from_utf8(&input[*cursor - 1..]).ok()?;
                let character = rest.chars().next()?;
                output.push(character);
                *cursor += character.len_utf8() - 1;
            }
        }
    }
    None
}

fn skip_ascii_whitespace(input: &[u8], cursor: &mut usize) {
    while input.get(*cursor).is_some_and(u8::is_ascii_whitespace) {
        *cursor += 1;
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("synthetic Tokio runtime")
        .block_on(future)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn typed_client_facade_dispatches_each_node_get_through_the_allowlisted_transport() {
    let (client, token_calls, transport_calls, observed) = fake_client_with_body(204, Vec::new());

    macro_rules! assert_read {
        ($future:expr, $expected:expr) => {{
            let result = $future.await;
            let response = result.expect("one synthetic typed GET");
            let expected = $expected.endpoint().expect("expected allowlisted route");
            assert_eq!(response.response().method(), HttpMethod::Get);
            assert_eq!(
                response.response().endpoint().route_name(),
                expected.route_name()
            );
            assert_eq!(response.response().attempts(), 1);
            assert_eq!(response.response().status(), 204);
            assert_eq!(response.parsed().status(), 204);
            assert!(response.parsed().json().is_none());
            assert!(
                response
                    .response()
                    .headers()
                    .any(|(name, value)| { name == "retry-after" && value == b"17" })
            );
            let requests = observed.lock().expect("test request mutex");
            let request = requests.last().expect("request reached fake transport");
            assert_eq!(request.method, HttpMethod::Get);
            assert!(request.token_matches);
            assert_eq!(request.target, expected.path());
        }};
    }

    assert_read!(client.account_numbers_typed(), ReadRequest::AccountNumbers);
    assert_read!(
        client.accounts(AccountsQuery {
            fields: Some(q("positions")),
        }),
        ReadRequest::Accounts(AccountsQuery {
            fields: Some(q("positions")),
        })
    );
    assert_read!(
        client.account(ACCOUNT_HASH, AccountsQuery::default()),
        ReadRequest::Account {
            account_hash: path(ACCOUNT_HASH),
            query: AccountsQuery::default(),
        }
    );
    assert_read!(
        client.orders(
            ACCOUNT_HASH,
            order_query("start", "end", Some("WORKING"), Some(10))
        ),
        ReadRequest::Orders {
            account_hash: path(ACCOUNT_HASH),
            query: order_query("start", "end", Some("WORKING"), Some(10)),
        }
    );
    assert_read!(
        client.order(ACCOUNT_HASH, "9007199254740993"),
        ReadRequest::Order {
            account_hash: path(ACCOUNT_HASH),
            order_id: broker_id("9007199254740993"),
        }
    );
    assert_read!(
        client.orders_across_accounts(order_query("start", "end", None, Some(25))),
        ReadRequest::OrdersAcrossAccounts(order_query("start", "end", None, Some(25)))
    );
    assert_read!(
        client.transactions(
            ACCOUNT_HASH,
            transaction_query("start", "end", "TRADE", None)
        ),
        ReadRequest::Transactions {
            account_hash: path(ACCOUNT_HASH),
            query: transaction_query("start", "end", "TRADE", None),
        }
    );
    assert_read!(
        client.transaction(ACCOUNT_HASH, "43"),
        ReadRequest::Transaction {
            account_hash: path(ACCOUNT_HASH),
            transaction_id: broker_id("43"),
        }
    );
    assert_read!(
        client.user_preferences_typed(),
        ReadRequest::UserPreferences
    );
    assert_read!(
        client.quotes(QuotesQuery {
            symbols: csv(&["QQQ", "SPY"]),
            fields: Some(csv(&["quote", "reference"])),
            indicative: Some(false),
        }),
        ReadRequest::Quotes(QuotesQuery {
            symbols: csv(&["QQQ", "SPY"]),
            fields: Some(csv(&["quote", "reference"])),
            indicative: Some(false),
        })
    );
    assert_read!(
        client.quote("QQQ", Some(csv(&["quote"]))),
        ReadRequest::Quote {
            symbol: path("QQQ"),
            fields: Some(csv(&["quote"])),
        }
    );
    assert_read!(
        client.option_quote("QQQ   260814P00740000"),
        ReadRequest::option_quote("QQQ   260814P00740000").unwrap()
    );
    assert_read!(
        client.option_quotes(["QQQ   260814P00740000", "QQQ   260814P00739000"]),
        ReadRequest::option_quotes(["QQQ   260814P00740000", "QQQ   260814P00739000"]).unwrap()
    );
    assert_read!(
        client.vertical_option_quote("QQQ   260814P00740000", "QQQ   260814P00739000"),
        ReadRequest::vertical_option_quote("QQQ   260814P00740000", "QQQ   260814P00739000")
            .unwrap()
    );
    assert_read!(
        client.option_chains(OptionChainQuery {
            symbol: q("QQQ"),
            contract_type: Some(q("PUT")),
            include_underlying_quote: Some(true),
            include_quotes: None,
            strategy: None,
            interval: None,
            strike_count: None,
            strike: None,
            range: None,
            from_date: None,
            to_date: None,
            volatility: None,
            underlying_price: None,
            interest_rate: None,
            days_to_expiration: None,
            exp_month: None,
            option_type: None,
            entitlement: None,
        }),
        ReadRequest::OptionChains(Box::new(OptionChainQuery {
            symbol: q("QQQ"),
            contract_type: Some(q("PUT")),
            include_underlying_quote: Some(true),
            include_quotes: None,
            strategy: None,
            interval: None,
            strike_count: None,
            strike: None,
            range: None,
            from_date: None,
            to_date: None,
            volatility: None,
            underlying_price: None,
            interest_rate: None,
            days_to_expiration: None,
            exp_month: None,
            option_type: None,
            entitlement: None,
        }))
    );
    assert_read!(
        client.option_expiration_chain(OptionExpirationQuery {
            symbol: q("QQQ"),
            contract_type: Some(q("PUT")),
            exp_month: Some(q("AUG")),
            option_type: Some(q("ALL")),
        }),
        ReadRequest::OptionExpirationChain(OptionExpirationQuery {
            symbol: q("QQQ"),
            contract_type: Some(q("PUT")),
            exp_month: Some(q("AUG")),
            option_type: Some(q("ALL")),
        })
    );
    assert_read!(
        client.price_history(PriceHistoryQuery {
            symbol: q("QQQ"),
            period_type: Some(q("day")),
            period: Some(0),
            frequency_type: Some(q("minute")),
            frequency: Some(5),
            start_date: Some(0),
            end_date: Some(2),
            need_extended_hours_data: Some(false),
            need_previous_close: Some(true),
        }),
        ReadRequest::PriceHistory(PriceHistoryQuery {
            symbol: q("QQQ"),
            period_type: Some(q("day")),
            period: Some(0),
            frequency_type: Some(q("minute")),
            frequency: Some(5),
            start_date: Some(0),
            end_date: Some(2),
            need_extended_hours_data: Some(false),
            need_previous_close: Some(true),
        })
    );
    assert_read!(
        client.movers(
            "$DJI",
            MoversQuery {
                sort: Some(q("VOLUME")),
                frequency: Some(30),
            }
        ),
        ReadRequest::Movers {
            symbol: path("$DJI"),
            query: MoversQuery {
                sort: Some(q("VOLUME")),
                frequency: Some(30),
            },
        }
    );
    assert_read!(
        client.markets(MarketsQuery {
            markets: csv(&["EQUITY", "OPTION"]),
            date: Some(q("2026-08-12")),
        }),
        ReadRequest::Markets(MarketsQuery {
            markets: csv(&["EQUITY", "OPTION"]),
            date: Some(q("2026-08-12")),
        })
    );
    assert_read!(
        client.market_hours(
            "OPTION",
            MarketHoursQuery {
                date: Some(q("2026-08-12")),
            }
        ),
        ReadRequest::MarketHours {
            market: path("OPTION"),
            query: MarketHoursQuery {
                date: Some(q("2026-08-12")),
            },
        }
    );
    assert_read!(
        client.search_instruments(InstrumentSearchQuery {
            symbols: csv(&["QQQ", "SPY"]),
            projection: q("SYMBOL_SEARCH"),
        }),
        ReadRequest::SearchInstruments(InstrumentSearchQuery {
            symbols: csv(&["QQQ", "SPY"]),
            projection: q("SYMBOL_SEARCH"),
        })
    );
    assert_read!(
        client.instrument_by_cusip("12 345"),
        ReadRequest::InstrumentByCusip(path("12 345"))
    );

    assert_eq!(token_calls.load(Ordering::SeqCst), 22);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 22);
    assert_eq!(observed.lock().expect("test request mutex").len(), 22);
}

#[test]
fn account_order_and_transaction_reads_default_to_urgent_admission() {
    let admission = priority_test_gate();
    let (client, _, transport_calls, _) = fake_client_with_gate(
        204,
        Vec::new(),
        Arc::clone(&admission) as Arc<dyn ReadAdmissionPort>,
    );

    block_on(async {
        client
            .read_with_priority(ReadRequest::UserPreferences, ReadPriority::Urgent)
            .await
            .expect("synthetic explicit-priority read");
        expect_urgent_typed_read(client.account_numbers_typed()).await;
        expect_urgent_typed_read(client.accounts(AccountsQuery::default())).await;
        expect_urgent_typed_read(client.account(ACCOUNT_HASH, AccountsQuery::default())).await;
        expect_urgent_typed_read(
            client.orders(ACCOUNT_HASH, order_query("start", "end", None, Some(10))),
        )
        .await;
        expect_urgent_typed_read(client.order(ACCOUNT_HASH, "43")).await;
        expect_urgent_typed_read(client.orders_across_accounts(order_query(
            "start",
            "end",
            None,
            Some(10),
        )))
        .await;
        expect_urgent_typed_read(client.transactions(
            ACCOUNT_HASH,
            transaction_query("start", "end", "TRADE", None),
        ))
        .await;
        expect_urgent_typed_read(client.transaction(ACCOUNT_HASH, "43")).await;
        expect_urgent_typed_read(client.read_typed(ReadRequest::OrdersAcrossAccounts(
            order_query("start", "end", None, Some(10)),
        )))
        .await;
        let raw = client
            .read(ReadRequest::Transaction {
                account_hash: path(ACCOUNT_HASH),
                transaction_id: broker_id("43"),
            })
            .await;
        assert!(raw.is_ok(), "raw authority read must use Urgent");
    });

    assert_eq!(transport_calls.load(Ordering::SeqCst), 11);
    let priorities = admission.0.lock().expect("priority recorder lock");
    assert_eq!(priorities.len(), 11);
    assert!(
        priorities
            .iter()
            .all(|priority| *priority == ReadPriority::Urgent)
    );
}

#[tokio::test]
async fn default_priority_is_forwarded_as_an_sdk_owned_ordering_hint() {
    let admission = priority_test_gate();
    let (client, _, transport_calls, _) = fake_client_with_gate(
        204,
        Vec::new(),
        Arc::clone(&admission) as Arc<dyn ReadAdmissionPort>,
    );
    client
        .quotes(QuotesQuery {
            symbols: csv(&["QQQ"]),
            fields: None,
            indicative: None,
        })
        .await
        .expect("synthetic refresh read");
    client
        .orders(
            ACCOUNT_HASH,
            order_query("start", "end", Some("WORKING"), Some(10)),
        )
        .await
        .expect("synthetic authority read");

    assert_eq!(transport_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        admission
            .0
            .lock()
            .expect("priority recorder lock")
            .as_slice(),
        &[ReadPriority::Refresh, ReadPriority::Urgent]
    );
}

#[test]
fn typed_read_error_keeps_retry_after_and_does_not_retry_or_hide_schema_failure() {
    let (client, token_calls, transport_calls, _) =
        fake_client_with_body(429, b"synthetic-broker-error".to_vec());
    let error = block_on(client.quotes(QuotesQuery {
        symbols: csv(&["QQQ"]),
        fields: None,
        indicative: None,
    }))
    .expect_err("429 remains a caller-controlled status error");
    assert_eq!(error.code(), "REST_HTTP_STATUS");
    assert_eq!(
        error.request_dispatch_certainty(),
        crate::RequestDispatchCertainty::MayHaveBeenSent
    );
    let response = error.response().expect("status response is retained");
    assert_eq!(response.status(), 429);
    assert_eq!(response.attempts(), 1);
    assert!(
        response
            .headers()
            .any(|(name, value)| { name == "retry-after" && value == b"17" })
    );
    assert_eq!(response.body(), b"synthetic-broker-error");
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);

    let (client, token_calls, transport_calls, _) = fake_client_with_body(200, b"[]".to_vec());
    let error = block_on(client.quotes(QuotesQuery {
        symbols: csv(&["QQQ"]),
        fields: None,
        indicative: None,
    }))
    .expect_err("invalid successful envelope fails schema validation");
    assert_eq!(error.code(), "REST_READ_RESPONSE_SCHEMA_INVALID");
    assert_eq!(
        error.request_dispatch_certainty(),
        crate::RequestDispatchCertainty::MayHaveBeenSent
    );
    assert_eq!(
        error.response_error(),
        Some(ReadResponseError::SchemaViolation { field: "quotes" })
    );
    let response = error
        .response()
        .expect("invalid response remains available");
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"[]");
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn streamer_info_api_selects_first_typed_preference_from_existing_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).expect("read-surface fixture");
    let case = fixture["cases"]
        .as_array()
        .expect("fixture cases")
        .iter()
        .find(|case| case["id"] == "trader-streamer-info-array-preference-selects-first")
        .expect("streamer-info fixture case");
    let body =
        serde_json::to_vec(&case["transport"]["body"]).expect("synthetic streamer info serializes");
    let (client, token_calls, transport_calls, observed) = fake_client_with_body(200, body);

    let info = block_on(client.streamer_info()).expect("first streamer info is projected");
    assert_eq!(info.schwab_client_customer_id, "synthetic-customer");
    assert_eq!(info.schwab_client_correl_id, "synthetic-correlation");
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    let request = observed
        .lock()
        .expect("test request mutex")
        .first()
        .cloned()
        .expect("request reached fake transport");
    assert_eq!(request.target, "/trader/v1/userPreference");
    assert_eq!(request.method, HttpMethod::Get);
    assert!(request.token_matches);
}

#[test]
fn typed_quote_api_parses_fixture_body_after_the_fake_transport_call() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).expect("read-surface fixture");
    let case = fixture["cases"]
        .as_array()
        .expect("fixture cases")
        .iter()
        .find(|case| case["id"] == "market-batch-quotes-joins-symbols-and-fields")
        .expect("batch quote fixture case");
    let body = serde_json::to_vec(&case["transport"]["body"])
        .expect("synthetic quote response serializes");
    let (client, token_calls, transport_calls, observed) = fake_client_with_body(200, body);

    let typed = block_on(client.quotes(QuotesQuery {
        symbols: csv(&["QQQ"]),
        fields: Some(csv(&["quote"])),
        indicative: None,
    }))
    .expect("fixture quote response validates");
    assert_eq!(typed.parsed().kind(), ReadResponseKind::Quotes);
    let Some(MarketReadResponse::Quotes(quotes)) = typed.parsed().market_model() else {
        panic!("typed quote family expected");
    };
    assert_eq!(
        quotes.items["QQQ"].unknown_fields.get("futureQuoteField"),
        Some(&serde_json::json!({"value": 1}))
    );
    assert!(
        typed
            .response()
            .headers()
            .any(|(name, value)| { name == "retry-after" && value == b"17" })
    );
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.lock().expect("test request mutex")[0].target,
        "/marketdata/v1/quotes?symbols=QQQ&fields=quote"
    );
}

#[test]
fn invalid_typed_path_input_fails_before_token_or_transport() {
    let (client, token_calls, transport_calls, _) = fake_client(204);
    let error = block_on(client.account("\n", AccountsQuery::default()))
        .expect_err("blank account hash fails request construction");
    assert_eq!(error.code(), "REST_READ_PATH_IDENTIFIER_INVALID");
    assert!(error.response().is_none());
    assert_eq!(token_calls.load(Ordering::SeqCst), 0);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 0);
}
