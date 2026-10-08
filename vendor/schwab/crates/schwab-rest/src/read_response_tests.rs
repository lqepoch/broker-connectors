use super::*;
use std::collections::BTreeMap;

use serde_json::{Value, json};

const FIXTURE: &str = include_str!("../../../test/fixtures/rust-v2/sdk_read_surface.json");
const RESPONSE_MODELS_FIXTURE: &str =
    include_str!("../../../test/fixtures/rust-v2/sdk_response_models.json");
const BODY_LIMIT: usize = MAX_RESPONSE_BODY_BYTES;

fn account_number_hash_response(row_count: usize) -> String {
    let mut body = String::from("[");
    for index in 0..row_count {
        if index > 0 {
            body.push(',');
        }
        body.push_str("{\"accountNumber\":\"synthetic-account-");
        body.push_str(&index.to_string());
        body.push_str("\",\"hashValue\":\"synthetic-hash-");
        body.push_str(&index.to_string());
        body.push_str("\"}");
    }
    body.push(']');
    body
}

#[test]
#[allow(clippy::too_many_lines)]
fn applicable_golden_read_responses_follow_node_schema_or_documented_rust_boundary() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("synthetic JSON fixture");
    let cases = fixture["cases"].as_array().expect("case list");
    assert_eq!(cases.len(), 51);
    let mut parsed = 0usize;
    let mut schema_rejections = 0usize;
    let mut no_body = 0usize;
    let mut normalized_quotes = 0usize;
    let mut legacy_verticals = 0usize;
    let mut retry_parity_not_run = 0usize;

    for case in cases {
        let id = case["id"].as_str().expect("fixture case id");
        let method = case["method"].as_str().expect("fixture method");
        let kind = kind_for_method(method);
        if id == "market-http-429-retries-only-through-read-retry-policy" {
            let scripted = case["transport"]["responses"]
                .as_array()
                .expect("429 retry response sequence");
            assert_eq!(case["expectedFetchCount"], 2);
            assert_eq!(scripted.first().expect("429 response")["status"], 429);
            assert_eq!(scripted.get(1).expect("retried success")["status"], 200);
            // This response parser has no retry policy. Do not parse the second
            // scripted reply as if Rust had made Node's second fetch.
            retry_parity_not_run += 1;
            continue;
        }
        let response = fixture_response(&case["transport"]);
        let expected = &case["expectedOutcome"];
        let expected_kind = expected["kind"].as_str().expect("expected outcome kind");

        if !(200..300).contains(&response.status) {
            let error = ParsedReadResponse::parse(kind, response.status, &response.body)
                .expect_err("non-success status must remain a transport/API error");
            assert_eq!(error, ReadResponseError::HttpStatus, "case {id}");
            continue;
        }

        match ParsedReadResponse::parse(kind, response.status, &response.body) {
            Err(error) => {
                assert_eq!(
                    expected_kind, "error",
                    "unexpected schema rejection in {id}"
                );
                assert_eq!(expected["name"], "ZodError", "case {id}");
                assert!(
                    matches!(error, ReadResponseError::SchemaViolation { .. }),
                    "case {id}"
                );
                if id == "trader-invalid-success-envelope-fails-schema-validation" {
                    assert_eq!(error.field(), Some("accountNumbers"));
                } else if id == "market-invalid-success-envelope-fails-schema-validation" {
                    assert_eq!(error.field(), Some("movers.screeners"));
                }
                let diagnostic = format!("{error:?} {error}");
                assert!(!diagnostic.contains("synthetic"), "case {id}");
                schema_rejections += 1;
            }
            Ok(response) => {
                parsed += 1;
                if response.json().is_none() {
                    no_body += 1;
                }
                let diagnostic = format!("{response:?}");
                assert!(!diagnostic.contains("synthetic"), "case {id}");
                match expected_kind {
                    "response" => {
                        if expected["bodyState"] == "undefined" {
                            assert!(response.json().is_none(), "case {id}");
                        } else {
                            assert_eq!(
                                response.json(),
                                Some(&response_json(&case["transport"])),
                                "case {id}"
                            );
                        }
                    }
                    "body" => {
                        let actual = match method {
                            "getTransaction" => response.transaction_convenience(),
                            "getStreamerInfo" => response.streamer_info(),
                            _ => response.json().ok_or(ReadResponseError::InvalidJson),
                        };
                        let actual = match actual {
                            Ok(value) => value,
                            Err(error) => {
                                assert_eq!(expected["kind"], "error", "case {id}");
                                panic!("unexpected convenience error in {id}: {error}");
                            }
                        };
                        assert_eq!(actual, &expected["value"], "case {id}");
                        if method == "getStreamerInfo" {
                            let typed = response.streamer_info_model().unwrap_or_else(|error| {
                                panic!("typed selection failed in {id}: {error}")
                            });
                            assert_eq!(
                                typed.streamer_socket_url,
                                expected["value"]["streamerSocketUrl"]
                            );
                            assert_eq!(
                                typed.schwab_client_customer_id,
                                expected["value"]["schwabClientCustomerId"]
                            );
                            assert_eq!(
                                typed.schwab_client_correl_id,
                                expected["value"]["schwabClientCorrelId"]
                            );
                            assert_eq!(
                                typed.schwab_client_channel,
                                expected["value"]["schwabClientChannel"]
                            );
                            assert_eq!(
                                typed.schwab_client_function_id,
                                expected["value"]["schwabClientFunctionId"]
                            );
                        }
                    }
                    "error" => match method {
                        "getTransaction" => assert_eq!(
                            response.transaction_convenience().unwrap_err(),
                            ReadResponseError::TransactionNotFound,
                            "case {id}"
                        ),
                        "getStreamerInfo" => {
                            assert_eq!(
                                response.streamer_info().unwrap_err(),
                                ReadResponseError::StreamerInfoUnavailable,
                                "case {id}"
                            );
                            assert_eq!(
                                response.streamer_info_model().unwrap_err(),
                                ReadResponseError::StreamerInfoUnavailable,
                                "typed case {id}"
                            );
                        }
                        _ => panic!("unexpected fixture error with a schema-valid response: {id}"),
                    },
                    "quote" => {
                        let symbols = [case["args"][0]
                            .as_str()
                            .expect("getOptionQuote symbol argument")];
                        let quotes = response
                            .option_quotes(&symbols, 1_700_000_000_000)
                            .expect("option quote normalization");
                        assert_eq!(quotes.len(), 1, "case {id}");
                        compare_quote_projection(&quotes[0], &expected["value"], id);
                        normalized_quotes += 1;
                    }
                    "quotes" => {
                        let symbols = case["args"][0]
                            .as_array()
                            .expect("getOptionQuotes symbol list")
                            .iter()
                            .map(|symbol| symbol.as_str().expect("symbol"))
                            .collect::<Vec<_>>();
                        let quotes = response
                            .option_quotes(&symbols, 1_700_000_000_000)
                            .expect("option quote list normalization");
                        let expected_quotes = expected["value"].as_array().expect("quote list");
                        assert_eq!(quotes.len(), expected_quotes.len(), "case {id}");
                        for (quote, expected) in quotes.iter().zip(expected_quotes) {
                            compare_quote_projection(quote, expected, id);
                        }
                        normalized_quotes += quotes.len();
                    }
                    "vertical" => {
                        assert_eq!(case["classification"], "BUG_IN_LEGACY", "case {id}");
                        let long_symbol = case["args"][0].as_str().expect("long option symbol");
                        let short_symbol = case["args"][1].as_str().expect("short option symbol");
                        let legs = response
                            .vertical_quote_legs(long_symbol, short_symbol, 1_700_000_000_000)
                            .expect("raw vertical leg normalization");
                        assert_eq!(legs.long.symbol, long_symbol);
                        assert_eq!(legs.short.symbol, short_symbol);
                        assert_eq!(
                            legs.long
                                .bid
                                .as_ref()
                                .map(ExactDecimal::as_string)
                                .as_deref(),
                            Some("29.9")
                        );
                        assert_eq!(
                            legs.long
                                .ask
                                .as_ref()
                                .map(ExactDecimal::as_string)
                                .as_deref(),
                            Some("30.1")
                        );
                        assert_eq!(
                            legs.short
                                .bid
                                .as_ref()
                                .map(ExactDecimal::as_string)
                                .as_deref(),
                            Some("28.95")
                        );
                        assert_eq!(
                            legs.short
                                .ask
                                .as_ref()
                                .map(ExactDecimal::as_string)
                                .as_deref(),
                            Some("29.05")
                        );
                        assert!(!format!("{legs:?}").contains("29.9"));
                        // The Node fixture records the legacy f64 synthetic result. Rust
                        // deliberately returns exact legs and delegates spread pricing.
                        assert_eq!(expected["value"]["derivedMid"], 1.0);
                        legacy_verticals += 1;
                    }
                    _ => panic!("unexpected expectedOutcome kind {expected_kind} in {id}"),
                }
            }
        }
    }

    assert_eq!(parsed, 47);
    assert_eq!(schema_rejections, 2);
    assert_eq!(no_body, 2);
    assert_eq!(normalized_quotes, 3);
    assert_eq!(legacy_verticals, 1);
    assert_eq!(retry_parity_not_run, 1);
}

#[test]
fn golden_429_case_records_node_retry_expectation_without_claiming_rust_parity() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("synthetic JSON fixture");
    let case = fixture["cases"]
        .as_array()
        .expect("case list")
        .iter()
        .find(|case| case["id"] == "market-http-429-retries-only-through-read-retry-policy")
        .expect("429 retry golden");
    assert_eq!(case["expectedFetchCount"], 2);
    assert_eq!(case["transport"]["responses"][0]["status"], 429);
    assert_eq!(case["transport"]["responses"][1]["status"], 200);
    // Rust read transport performs one attempt and does not own #176's shared
    // request budget/retry policy. This test does not exercise a Rust retry.
}

#[test]
fn exact_decimal_option_normalization_avoids_legacy_binary_spread_artifact() {
    let response = option_quote_response();
    let quotes = response
        .option_quotes(&["QQQ   260814P00740000"], 1_700_000_000_000)
        .expect("fixture quote");
    let quote = &quotes[0];
    assert_eq!(
        quote.bid.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("29.9")
    );
    assert_eq!(
        quote.ask.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("30.1")
    );
    assert_eq!(
        quote.mid.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("30")
    );
    assert_eq!(
        quote
            .spread
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("0.2")
    );
    let ratio = quote.spread_percent_of_mid.as_ref().expect("exact ratio");
    assert_eq!(ratio.numerator().as_string(), "20");
    assert_eq!(ratio.denominator().as_string(), "30");
    assert_eq!(quote.expiration.as_deref(), Some("2026-08-14"));
    assert_eq!(
        quote
            .strike
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("740")
    );
    assert_eq!(quote.contract_type.as_deref(), Some("PUT"));
}

#[test]
fn wire_decimal_lexeme_is_preserved_and_quote_age_uses_only_positive_quote_time() {
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        br#"{"QQQ   260814P00740000":{"assetMainType":"OPTION","symbol":"QQQ   260814P00740000","quote":{"bidPrice":1.0000000000000001,"askPrice":1.0000000000000002,"quoteTime":0,"tradeTime":1699999999000,"privateField":"synthetic-private"}}}"#,
    )
    .expect("synthetic option quote with an exact JSON decimal");
    let quote = response
        .option_quotes(&["QQQ   260814P00740000"], 1_700_000_000_000)
        .expect("option quote projection")
        .remove(0);
    assert_eq!(
        quote.bid.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("1.0000000000000001")
    );
    assert_eq!(
        quote.ask.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("1.0000000000000002")
    );
    assert_eq!(quote.quote_time, None);
    assert_eq!(
        quote
            .trade_time
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("1699999999000")
    );
    assert_eq!(quote.quote_age_ms, None);
    let debug = format!("{quote:?}");
    assert!(!debug.contains("QQQ"));
    assert!(!debug.contains("1.0000000000000001"));
    assert!(!debug.contains("synthetic-private"));
}

#[test]
fn option_quote_identity_accepts_trailing_padding_but_rejects_another_contract() {
    let requested_symbol = "QQQ   260814P00740000";
    let padded = json!({
        "QQQ   260814P00740000   ": {
            "assetMainType": "OPTION",
            "symbol": "QQQ   260814P00740000   ",
            "quote": { "bidPrice": 1.0, "askPrice": 1.1 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&padded).expect("padded synthetic quote encodes"),
    )
    .expect("bounded padded quote response parses");
    let quote = response
        .option_quotes(&[requested_symbol], 1_700_000_000_000)
        .expect("Node-compatible trailing padding is accepted");
    assert_eq!(quote[0].symbol, "QQQ   260814P00740000   ");

    let mismatched = json!({
        "QQQ   260814P00740000": {
            "assetMainType": "OPTION",
            "symbol": "QQQ   260814P00739000",
            "quote": { "bidPrice": 1.0, "askPrice": 1.1 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&mismatched).expect("mismatched synthetic quote encodes"),
    )
    .expect("mismatched quote response remains bounded JSON");
    let error = response
        .option_quotes(&[requested_symbol], 1_700_000_000_000)
        .expect_err("a provider row for another option must fail closed");
    assert_eq!(error, ReadResponseError::QuoteIdentityMismatch);
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains("QQQ"));
    assert!(!diagnostic.contains("260814P00740000"));
    assert!(!diagnostic.contains("260814P00739000"));
}

#[test]
fn option_quote_lookup_matches_full_index_alias_order_and_error_precedence() {
    let symbol = "QQQ   260814P00740000";
    let alias_collision = json!({
        "AAA   ": {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "quote": { "bidPrice": 1.01, "askPrice": 1.11 }
        },
        "BBB": {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "quote": { "bidPrice": 2.02, "askPrice": 2.12 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&alias_collision).expect("synthetic alias collision encodes"),
    )
    .expect("synthetic alias collision validates");
    let selected = response
        .option_quotes(&[symbol], 1_700_000_000_000)
        .expect("the first compatibility alias resolves");
    assert_eq!(
        selected[0]
            .bid
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("1.01"),
        "alias collisions follow the prior response-map and per-row order"
    );

    let direct_key_wins = json!({
        (symbol): {
            "assetMainType": "OPTION",
            "symbol": "QQQ   260814P00739000",
            "quote": { "bidPrice": 1.01, "askPrice": 1.11 }
        },
        "ZZZ   ": {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "quote": { "bidPrice": 2.02, "askPrice": 2.12 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&direct_key_wins).expect("synthetic direct-key precedence encodes"),
    )
    .expect("synthetic direct-key precedence response validates structurally");
    assert_eq!(
        response
            .option_quotes(&[symbol], 1_700_000_000_000)
            .unwrap_err(),
        ReadResponseError::QuoteIdentityMismatch,
        "an exact raw response key must not be replaced by a valid alias"
    );

    for (body, requested) in [
        (b"{}".as_slice(), vec!["missing", "missing   "]),
        (b"{}".as_slice(), vec!["", "missing"]),
        (b"{}".as_slice(), vec!["missing", ""]),
    ] {
        let response = ParsedReadResponse::parse(ReadResponseKind::Quotes, 200, body)
            .expect("empty synthetic quote object validates");
        assert_eq!(
            quote_result_signature(crate::read_response::option_quotes_indexed_reference(
                &response,
                &requested,
                1_700_000_000_000,
            )),
            quote_result_signature(response.option_quotes(&requested, 1_700_000_000_000,)),
            "optimized and full-index resolvers preserve which error is observed first"
        );
    }
}

#[test]
fn option_quote_lookup_differentially_matches_reference_for_direct_alias_and_duplicate_inputs() {
    let requested = [
        "QQQ   260814P00740000",
        "SPY   260814P00700000",
        "IWM   260814P00200000",
        "DIA   260814P00400000",
    ];
    let mut quotes = serde_json::Map::new();
    for (index, symbol) in requested.iter().enumerate() {
        let decimal_offset =
            f64::from(u32::try_from(index).expect("synthetic fixture index fits u32")) / 100.0;
        let row_symbol = if index == 2 {
            format!("{symbol}   ")
        } else {
            (*symbol).to_owned()
        };
        let key = if index == 2 {
            format!("{symbol}   ")
        } else {
            (*symbol).to_owned()
        };
        quotes.insert(
            key,
            json!({
                "assetMainType": "OPTION",
                "symbol": row_symbol,
                "reference": { "underlying": symbol.get(..3).unwrap_or("QQQ"), "contractType": "PUT" },
                "quote": {
                    "bidPrice": 1.0 + decimal_offset,
                    "askPrice": 1.1 + decimal_offset,
                    "bidSize": index + 1,
                    "askSize": index + 2,
                    "quoteTime": 1_699_999_999_999_i64,
                    "delta": -0.25
                }
            }),
        );
    }
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&Value::Object(quotes)).expect("synthetic quote map encodes"),
    )
    .expect("synthetic quote map validates");
    let request_cases: &[&[&str]] = &[
        &[requested[0]],
        &[requested[2]],
        &[requested[3], requested[0], requested[1]],
        &["QQQ   260814P00740000   "],
        &[requested[0], "QQQ   260814P00740000   "],
        &["absent", "absent   "],
        &["absent", ""],
        &["", "absent"],
    ];
    for symbols in request_cases {
        assert_eq!(
            quote_result_signature(crate::read_response::option_quotes_indexed_reference(
                &response,
                symbols,
                1_700_000_000_000,
            )),
            quote_result_signature(response.option_quotes(symbols, 1_700_000_000_000)),
            "request {symbols:?}"
        );
    }
}

#[test]
#[ignore = "offline old/new option quote lookup microbenchmark"]
#[allow(clippy::too_many_lines)]
fn ignored_option_quote_lookup_microbenchmark() {
    use std::hint::black_box;

    const ITERATIONS: usize = 40;
    const ROUNDS: usize = 31;
    const OBSERVED_AT_MS: i64 = 1_700_000_000_000;

    let make_response = |row_count: usize, request_count: usize, alias_first: bool| {
        let mut quotes = serde_json::Map::new();
        let mut requested = Vec::with_capacity(request_count);
        for index in 0..row_count {
            let strike = 400_000 + index * 1_000;
            let symbol = format!("QQQ   260814P{strike:08}");
            let padded = alias_first && index == 0;
            let row_symbol = if padded {
                format!("{symbol}   ")
            } else {
                symbol.clone()
            };
            let key = if padded {
                format!("{symbol}   ")
            } else {
                symbol.clone()
            };
            if index < request_count {
                requested.push(symbol.clone());
            }
            quotes.insert(
                key,
                json!({
                    "assetMainType": "OPTION",
                    "symbol": row_symbol,
                    "reference": { "underlying": "QQQ", "contractType": "PUT" },
                    "quote": {
                        "bidPrice": 0.90,
                        "askPrice": 1.00,
                        "bidSize": 10,
                        "askSize": 12,
                        "quoteTime": OBSERVED_AT_MS - 1
                    }
                }),
            );
        }
        let response = ParsedReadResponse::parse(
            ReadResponseKind::Quotes,
            200,
            &serde_json::to_vec(&Value::Object(quotes)).expect("synthetic quote map encodes"),
        )
        .expect("synthetic quote map validates");
        (response, requested)
    };

    for (name, rows, requests, alias_first) in [
        ("vertical_two_leg", 2, 2, false),
        ("vertical_two_leg_alias", 2, 2, true),
        ("max_request_100", 100, 100, false),
        ("max_request_100_alias", 100, 100, true),
        ("oversized_response", 1_024, 2, false),
    ] {
        let (response, requested_owned) = make_response(rows, requests, alias_first);
        let requested = requested_owned
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            quote_result_signature(crate::read_response::option_quotes_indexed_reference(
                &response,
                &requested,
                OBSERVED_AT_MS,
            )),
            quote_result_signature(response.option_quotes(&requested, OBSERVED_AT_MS)),
            "benchmark case output must match the pre-optimization implementation"
        );

        let mut old_samples = Vec::with_capacity(ROUNDS);
        let mut new_samples = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            let measure = |reference: bool| {
                let started = std::time::Instant::now();
                for _ in 0..ITERATIONS {
                    let result = if reference {
                        crate::read_response::option_quotes_indexed_reference(
                            &response,
                            &requested,
                            OBSERVED_AT_MS,
                        )
                    } else {
                        response.option_quotes(&requested, OBSERVED_AT_MS)
                    };
                    black_box(result).expect("benchmark inputs remain valid");
                }
                started.elapsed().as_nanos()
            };
            if round % 2 == 0 {
                old_samples.push(measure(true));
                new_samples.push(measure(false));
            } else {
                new_samples.push(measure(false));
                old_samples.push(measure(true));
            }
        }
        let summarize = |samples: &[u128]| {
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            let nearest_rank = |percentile: usize| {
                let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
                sorted[rank.saturating_sub(1)]
            };
            (
                nearest_rank(50),
                nearest_rank(95),
                nearest_rank(99),
                sorted[0],
                sorted[sorted.len() - 1],
            )
        };
        let old_summary = summarize(&old_samples);
        let new_summary = summarize(&new_samples);
        println!(
            "OPTION_QUOTE_LOOKUP_BENCH workload={name} rows={rows} requests={} alias_first={alias_first} paired_rounds={ROUNDS} iterations_per_sample={ITERATIONS} old_ns={old_samples:?} new_ns={new_samples:?} old_stats_ns=(p50={},p95={},p99={},min={},max={}) new_stats_ns=(p50={},p95={},p99={},min={},max={})",
            requested.len(),
            old_summary.0,
            old_summary.1,
            old_summary.2,
            old_summary.3,
            old_summary.4,
            new_summary.0,
            new_summary.1,
            new_summary.2,
            new_summary.3,
            new_summary.4,
        );
    }
}

#[test]
fn option_quote_projection_maps_node_missing_non_option_and_duplicate_rows_to_fixed_errors() {
    let requested = "QQQ   260814P00740000";

    let missing = ParsedReadResponse::parse(ReadResponseKind::Quotes, 200, b"{}")
        .expect("an empty quote map has the valid Node quotes envelope");
    let error = missing
        .option_quotes(&[requested], 1_700_000_000_000)
        .expect_err("a missing requested option fails closed");
    assert_eq!(error, ReadResponseError::QuoteUnavailable);

    let equity = json!({
        (requested): {
            "assetMainType": "EQUITY",
            "symbol": requested,
            "quote": { "bidPrice": 1.0, "askPrice": 1.1 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&equity).expect("synthetic non-option quote encodes"),
    )
    .expect("quote response conforms to the Node response schema");
    let error = response
        .option_quotes(&[requested], 1_700_000_000_000)
        .expect_err("an equity row cannot satisfy an option quote request");
    assert_eq!(error, ReadResponseError::QuoteNotOption);

    let option = json!({
        (requested): {
            "assetMainType": "OPTION",
            "symbol": requested,
            "quote": { "bidPrice": 1.0, "askPrice": 1.1 }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&option).expect("synthetic option quote encodes"),
    )
    .expect("option response conforms to the Node response schema");
    let error = response
        .option_quotes(&[requested, "QQQ   260814P00740000   "], 1_700_000_000_000)
        .expect_err("trailing-padding aliases identify one business symbol");
    assert_eq!(error, ReadResponseError::DuplicateQuoteSymbol);

    for error in [
        ReadResponseError::QuoteUnavailable,
        ReadResponseError::QuoteNotOption,
        ReadResponseError::DuplicateQuoteSymbol,
    ] {
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("QQQ"));
        assert!(!diagnostic.contains("260814P00740000"));
    }
}

#[test]
fn option_quote_projection_is_structural_not_a_freshness_or_nbbo_gate() {
    let symbol = "QQQ   260814P00740000";
    let stale_crossed = json!({
        (symbol): {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "realtime": false,
            "quote": {
                "bidPrice": 1.5,
                "askPrice": 1.0,
                "quoteTime": 1_699_999_950_000_i64
            }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&stale_crossed).expect("stale synthetic quote encodes"),
    )
    .expect("structural quote response parses");
    let quote = response
        .option_quotes(&[symbol], 1_700_000_000_000)
        .expect("REST normalization does not claim to apply the market-data gate")
        .remove(0);
    assert_eq!(
        quote
            .quote_age_ms
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("50000")
    );
    assert_eq!(
        quote.bid.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("1.5")
    );
    assert_eq!(
        quote.ask.as_ref().map(ExactDecimal::as_string).as_deref(),
        Some("1")
    );

    let future = json!({
        (symbol): {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "quote": {
                "bidPrice": 1.0,
                "askPrice": 1.1,
                "quoteTime": 1_700_000_000_100_i64
            }
        }
    });
    let response = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        &serde_json::to_vec(&future).expect("future synthetic quote encodes"),
    )
    .expect("future quote response remains parseable");
    let quote = response
        .option_quotes(&[symbol], 1_700_000_000_000)
        .expect("structural normalization preserves the signed future age")
        .remove(0);
    assert_eq!(
        quote
            .quote_age_ms
            .as_ref()
            .map(ExactDecimal::as_string)
            .as_deref(),
        Some("-100")
    );
}

#[test]
fn decimal_parser_handles_exponents_and_rejects_unbounded_or_non_finite_values() {
    for (input, expected) in [
        ("29.900", "29.9"),
        ("3e-3", "0.003"),
        ("1.25e2", "125"),
        ("-0.20", "-0.2"),
        ("0", "0"),
    ] {
        assert_eq!(
            ExactDecimal::parse(input)
                .expect("exact decimal")
                .as_string(),
            expected
        );
    }
    for input in ["", "NaN", "inf", "1e-19", "1e100", "1..0", ".5", "--1"] {
        assert_eq!(
            ExactDecimal::parse(input),
            Err(ReadResponseError::DecimalOutOfRange),
            "{input}"
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn optimized_decimal_parser_matches_the_legacy_parser_at_syntax_and_range_boundaries() {
    fn legacy_parse(value: &str) -> Result<String, ReadResponseError> {
        const MAX_BYTES: usize = 64;
        const MAX_SCALE: u32 = 18;

        if value.is_empty() || value.len() > MAX_BYTES {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        let exponent_at = unsigned.find(['e', 'E']);
        let (mantissa, exponent) = match exponent_at {
            Some(index) => {
                let exponent = unsigned[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| ReadResponseError::DecimalOutOfRange)?;
                (&unsigned[..index], exponent)
            }
            None => (unsigned, 0),
        };
        if mantissa.is_empty() {
            return Err(ReadResponseError::DecimalOutOfRange);
        }

        let mut digits = String::with_capacity(mantissa.len());
        let mut fractional_digits = 0i32;
        let mut integer_digits = 0usize;
        let mut decimal_seen = false;
        for byte in mantissa.bytes() {
            match byte {
                b'0'..=b'9' => {
                    digits.push(char::from(byte));
                    if decimal_seen {
                        fractional_digits += 1;
                    } else {
                        integer_digits += 1;
                    }
                }
                b'.' if !decimal_seen => decimal_seen = true,
                _ => return Err(ReadResponseError::DecimalOutOfRange),
            }
        }
        if digits.is_empty() || integer_digits == 0 || (decimal_seen && fractional_digits == 0) {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        let mut scale = fractional_digits
            .checked_sub(exponent)
            .ok_or(ReadResponseError::DecimalOutOfRange)?;
        if scale < 0 {
            let zeros =
                usize::try_from(-scale).map_err(|_| ReadResponseError::DecimalOutOfRange)?;
            if digits.len().saturating_add(zeros) > MAX_BYTES {
                return Err(ReadResponseError::DecimalOutOfRange);
            }
            digits.extend(std::iter::repeat_n('0', zeros));
            scale = 0;
        }
        let mut scale = u32::try_from(scale).map_err(|_| ReadResponseError::DecimalOutOfRange)?;
        if scale > MAX_SCALE {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        while scale > 0 && digits.ends_with('0') {
            digits.pop();
            scale -= 1;
        }
        let magnitude = digits
            .parse::<i128>()
            .map_err(|_| ReadResponseError::DecimalOutOfRange)?;
        let coefficient = if negative {
            magnitude
                .checked_neg()
                .ok_or(ReadResponseError::DecimalOutOfRange)?
        } else {
            magnitude
        };
        let mut output = coefficient.unsigned_abs().to_string();
        if scale > 0 {
            let scale = scale as usize;
            if output.len() <= scale {
                output.insert_str(0, &"0".repeat(scale + 1 - output.len()));
            }
            output.insert(output.len() - scale, '.');
        }
        if coefficient < 0 {
            output.insert(0, '-');
        }
        Ok(output)
    }

    let mut cases = vec![
        String::new(),
        "0".to_owned(),
        "-0".to_owned(),
        "+0.000".to_owned(),
        ".5".to_owned(),
        "1.".to_owned(),
        "1e".to_owned(),
        "1e+".to_owned(),
        "1e2147483647".to_owned(),
        "1e-2147483648".to_owned(),
        "0e2147483647".to_owned(),
        "0e-2147483648".to_owned(),
        "0e-2".to_owned(),
        "0e-64".to_owned(),
        "0.0e-2".to_owned(),
        "0.000e-8".to_owned(),
        "1e18".to_owned(),
        "1e19".to_owned(),
        "1e37".to_owned(),
        "1e38".to_owned(),
        "1e40".to_owned(),
        "170141183460469231731687303715884105727".to_owned(),
        "170141183460469231731687303715884105728".to_owned(),
        "-170141183460469231731687303715884105727".to_owned(),
        "-170141183460469231731687303715884105728".to_owned(),
        format!("0.{}", "0".repeat(18)),
        format!("0.{}", "0".repeat(19)),
        "0".repeat(64),
        "0".repeat(65),
        "é".to_owned(),
    ];

    let alphabet = b"019.+-eE";
    for length in 0..=5 {
        let combinations = alphabet
            .len()
            .pow(u32::try_from(length).expect("synthetic string length fits u32"));
        for mut encoded in 0..combinations {
            let mut bytes = vec![b'0'; length];
            for byte in &mut bytes {
                *byte = alphabet[encoded % alphabet.len()];
                encoded /= alphabet.len();
            }
            cases.push(String::from_utf8(bytes).expect("ASCII generation"));
        }
    }

    let mut seed = 0x5eed_1234_u64;
    for _ in 0..20_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let length = usize::try_from(seed % 65).expect("remainder fits usize") + 1;
        let mut value = String::with_capacity(length);
        for _ in 0..length {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let byte = match seed % 8 {
                0..=5 => b'0' + u8::try_from((seed >> 8) % 10).expect("remainder fits one digit"),
                6 => b'.',
                _ => b"eE+-"[usize::try_from((seed >> 8) % 4).expect("remainder fits usize")],
            };
            value.push(char::from(byte));
        }
        cases.push(value);
    }

    for input in cases {
        let expected = legacy_parse(&input);
        let actual = ExactDecimal::parse(&input).map(|decimal| decimal.as_string());
        assert_eq!(actual, expected, "input bytes: {:?}", input.as_bytes());
    }
}

#[test]
fn optimized_occ_strike_parser_matches_legacy_identity_and_date_behavior() {
    fn legacy_parse(symbol: &str) -> Option<(String, String, String, String)> {
        fn valid_date(year: u32, month: u32, day: u32) -> bool {
            if !(1..=12).contains(&month) || day == 0 {
                return false;
            }
            let leap =
                year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
            let days = match month {
                2 if leap => 29,
                2 => 28,
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            day <= days
        }

        let bytes = symbol.as_bytes();
        if bytes.len() != 21
            || !bytes[6..12].iter().all(u8::is_ascii_digit)
            || !matches!(bytes[12], b'C' | b'P')
            || !bytes[13..21].iter().all(u8::is_ascii_digit)
        {
            return None;
        }
        let underlying = std::str::from_utf8(&bytes[..6]).ok()?.trim_end().to_owned();
        if underlying.is_empty() {
            return None;
        }
        let compact = std::str::from_utf8(&bytes[6..12]).ok()?;
        let year = 2_000 + compact[0..2].parse::<u32>().ok()?;
        let month = compact[2..4].parse::<u32>().ok()?;
        let day = compact[4..6].parse::<u32>().ok()?;
        if !valid_date(year, month, day) {
            return None;
        }
        let strike_raw = std::str::from_utf8(&bytes[13..21]).ok()?;
        let strike = ExactDecimal::parse(&format!("{}.{}", &strike_raw[..5], &strike_raw[5..]))
            .ok()?
            .as_string();
        Some((
            underlying,
            format!("{year:04}-{month:02}-{day:02}"),
            if bytes[12] == b'C' { "CALL" } else { "PUT" }.to_owned(),
            strike,
        ))
    }

    let mut symbols = vec![
        "QQQ   260814P00740000".to_owned(),
        "SPY   260814C00650999".to_owned(),
        "ABCDEF260814P00000001".to_owned(),
        "X     260814C99999999".to_owned(),
        "QQQ   270101P00000000".to_owned(),
        "QQQ   270228P00001000".to_owned(),
        "QQQ   270229P00001000".to_owned(),
        "QQQ   240229P00001000".to_owned(),
        "QQQ   260814X00740000".to_owned(),
        "      260814P00740000".to_owned(),
        "QQQ   260814P0074000".to_owned(),
        "QQQ   260814P007400000".to_owned(),
        "QQQ   260814P0074A000".to_owned(),
    ];
    let mut seed = 0x0cc5_7a1e_u32;
    for _ in 0..10_000 {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        symbols.push(format!("QQQ   260814P{:08}", seed % 100_000_000));
    }

    for symbol in symbols {
        assert_eq!(
            crate::read_response::parse_occ_symbol_fields_for_test(&symbol),
            legacy_parse(&symbol),
            "symbol bytes: {:?}",
            symbol.as_bytes()
        );
    }
}

#[test]
fn parser_limits_json_body_and_tree_complexity_before_schema_walk() {
    let too_large = vec![b' '; BODY_LIMIT + 1];
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Quotes, 200, &too_large).unwrap_err(),
        ReadResponseError::BodyTooLarge
    );
    let nested = format!("{}null{}", "[".repeat(100), "]".repeat(100));
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Quotes, 200, nested.as_bytes()).unwrap_err(),
        ReadResponseError::JsonTooComplex
    );
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Quotes, 200, b"{bad json").unwrap_err(),
        ReadResponseError::InvalidJson
    );
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Quotes, 429, b"{}").unwrap_err(),
        ReadResponseError::HttpStatus
    );
}

#[test]
fn account_number_hash_rows_accept_the_limit_and_reject_one_over_with_fixed_error() {
    let at_limit = account_number_hash_response(MAX_ACCOUNT_NUMBER_HASH_ROWS);
    assert!(at_limit.len() <= MAX_RESPONSE_BODY_BYTES);
    let parsed =
        ParsedReadResponse::parse(ReadResponseKind::AccountNumbers, 200, at_limit.as_bytes())
            .expect("the exact account mapping limit is accepted");
    let Some(TraderReadResponse::AccountNumbers(rows)) = parsed.trader_model() else {
        panic!("account number route returns account-number models");
    };
    assert_eq!(rows.len(), MAX_ACCOUNT_NUMBER_HASH_ROWS);

    let over_limit = account_number_hash_response(MAX_ACCOUNT_NUMBER_HASH_ROWS + 1);
    assert!(over_limit.len() <= MAX_RESPONSE_BODY_BYTES);
    let error =
        ParsedReadResponse::parse(ReadResponseKind::AccountNumbers, 200, over_limit.as_bytes())
            .expect_err("one row over the account mapping limit must fail closed");
    assert_eq!(error, ReadResponseError::AccountNumberRowsTooMany);
    assert_eq!(error.code(), "REST_ACCOUNT_NUMBER_HASH_ROWS_TOO_MANY");
    assert_eq!(error.to_string(), "REST_ACCOUNT_NUMBER_HASH_ROWS_TOO_MANY");
    assert!(!format!("{error:?} {error}").contains("synthetic-account-0"));
    assert!(!format!("{error:?} {error}").contains("synthetic-hash-0"));
}

#[test]
fn debug_redacts_unknown_fields_streamer_ids_and_quote_payloads() {
    let source = json!({
        "accounts": [],
        "streamerInfo": [{
            "streamerSocketUrl": "wss://streamer.example.invalid",
            "schwabClientCustomerId": "synthetic-customer-secret",
            "schwabClientCorrelId": "synthetic-correlation-secret",
            "schwabClientChannel": "N9",
            "schwabClientFunctionId": "synthetic-function-secret",
            "unknownPrivateField": "synthetic-unknown-secret"
        }],
        "futureField": {"opaque": "synthetic-future-secret"}
    });
    let parsed = ParsedReadResponse::parse(
        ReadResponseKind::UserPreferences,
        200,
        &serde_json::to_vec(&source).expect("fixture JSON encoding"),
    )
    .expect("passthrough preference");
    assert_eq!(parsed.json(), Some(&source));
    let debug = format!("{parsed:?}");
    for secret in [
        "synthetic-customer-secret",
        "synthetic-correlation-secret",
        "synthetic-function-secret",
        "synthetic-unknown-secret",
        "synthetic-future-secret",
    ] {
        assert!(!debug.contains(secret));
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn response_model_goldens_preserve_exact_values_and_passthrough_fields() {
    let fixture: Value =
        serde_json::from_str(RESPONSE_MODELS_FIXTURE).expect("synthetic response model golden");
    let cases = fixture["cases"].as_array().expect("golden cases");
    assert_eq!(cases.len(), 12);

    for case in cases {
        let id = case["id"].as_str().expect("golden case id");
        let kind = match case["kind"].as_str().expect("response kind") {
            "AccountNumbers" => ReadResponseKind::AccountNumbers,
            "Account" => ReadResponseKind::Account,
            "Orders" => ReadResponseKind::Orders,
            "Transaction" | "TransactionArray" => ReadResponseKind::Transaction,
            "UserPreferencesObject" | "UserPreferencesArray" => ReadResponseKind::UserPreferences,
            "MarketHoursBatch" | "MarketHoursSingle" => ReadResponseKind::MarketHours,
            "InstrumentsSearch" => ReadResponseKind::InstrumentsSearch,
            "InstrumentDetail" => ReadResponseKind::InstrumentDetail,
            _ => panic!("unmapped synthetic response model case: {id}"),
        };
        let body = serde_json::to_vec(&case["body"]).expect("fixture body encoding");
        let parsed = ParsedReadResponse::parse(kind, 200, &body)
            .unwrap_or_else(|error| panic!("golden case {id} rejected: {error}"));
        if kind == ReadResponseKind::UserPreferences {
            let model = parsed
                .user_preferences_model()
                .unwrap_or_else(|| panic!("missing typed preferences model in {id}"));
            assert_eq!(
                model.is_array(),
                case["kind"] == "UserPreferencesArray",
                "case {id}"
            );
        } else if matches!(
            kind,
            ReadResponseKind::MarketHours
                | ReadResponseKind::InstrumentsSearch
                | ReadResponseKind::InstrumentDetail
        ) {
            assert!(parsed.market_model().is_some(), "case {id}");
        } else {
            assert!(parsed.trader_model().is_some(), "case {id}");
        }
    }

    let object_case = cases
        .iter()
        .find(|case| case["kind"] == "UserPreferencesObject")
        .expect("object preference golden");
    let object_body = serde_json::to_vec(&object_case["body"]).expect("object preference body");
    let object = ParsedReadResponse::parse(ReadResponseKind::UserPreferences, 200, &object_body)
        .expect("object preference response");
    let preferences = object.user_preferences_model().expect("typed preferences");
    let preference = preferences.first().expect("single preference");
    let account = &preference.accounts.as_ref().expect("account list")[0];
    assert_eq!(account.account_number, "synthetic-preference-account");
    assert_eq!(account.primary_account, Some(true));
    assert_eq!(account.account_type.as_deref(), Some("MARGIN"));
    assert_eq!(account.nickname.as_deref(), Some("Synthetic"));
    assert_eq!(account.account_color.as_deref(), Some("BLUE"));
    assert_eq!(account.display_account_id.as_deref(), Some("8421"));
    assert_eq!(account.auto_position_effect, Some(false));
    assert_eq!(
        account.unknown_fields.get("futureAccountPreference"),
        Some(&json!("preserved"))
    );
    let offer = &preference.offers.as_ref().expect("offers")[0];
    assert_eq!(offer.level2_permissions, Some(true));
    assert_eq!(offer.market_data_permission.as_deref(), Some("REALTIME"));
    assert_eq!(
        offer.unknown_fields.get("futureOfferField"),
        Some(&json!("preserved"))
    );
    assert_eq!(
        preference.unknown_fields.get("futurePreferenceField"),
        Some(&json!({"opaque": "synthetic-preference-extra"}))
    );
    let streamer = object
        .streamer_info_model()
        .expect("first typed streamer info");
    assert_eq!(
        streamer.streamer_socket_url,
        "wss://streamer.example.invalid/ws"
    );
    assert_eq!(
        streamer.schwab_client_customer_id,
        "synthetic-customer-secret"
    );
    assert_eq!(
        streamer.schwab_client_correl_id,
        "synthetic-correlation-secret"
    );
    assert_eq!(streamer.schwab_client_channel, "N9");
    assert_eq!(
        streamer.schwab_client_function_id,
        "synthetic-function-secret"
    );
    assert_eq!(
        streamer.unknown_fields.get("futureStreamerField"),
        Some(&json!({"opaque": "synthetic-streamer-extra"}))
    );

    let array_case = cases
        .iter()
        .find(|case| case["kind"] == "UserPreferencesArray")
        .expect("array preference golden");
    let array_body = serde_json::to_vec(&array_case["body"]).expect("array preference body");
    let array = ParsedReadResponse::parse(ReadResponseKind::UserPreferences, 200, &array_body)
        .expect("array preference response");
    assert!(
        array
            .user_preferences_model()
            .expect("typed array")
            .is_array()
    );
    assert_eq!(
        array
            .streamer_info_model()
            .expect("first preference and first streamer selection")
            .schwab_client_customer_id,
        "synthetic-first-customer"
    );
    assert_eq!(
        array.streamer_info().expect("compatible raw accessor"),
        &array_case["body"][0]["streamerInfo"][0]
    );

    let accepted_urls = fixture["acceptedUrlSyntaxCases"]
        .as_array()
        .expect("accepted URL syntax list");
    for url in accepted_urls {
        let mut body = object_case["body"].clone();
        body["streamerInfo"][0]["streamerSocketUrl"] = url.clone();
        let bytes = serde_json::to_vec(&body).expect("accepted URL response body");
        ParsedReadResponse::parse(ReadResponseKind::UserPreferences, 200, &bytes).unwrap_or_else(
            |error| panic!("Node-accepted URL syntax was rejected: {url}: {error}"),
        );
    }

    for invalid in fixture["invalidPreferenceCases"]
        .as_array()
        .expect("invalid preference cases")
    {
        let id = invalid["id"].as_str().expect("invalid case id");
        let bytes = serde_json::to_vec(&invalid["body"]).expect("invalid preference body");
        assert!(
            matches!(
                ParsedReadResponse::parse(ReadResponseKind::UserPreferences, 200, &bytes),
                Err(ReadResponseError::SchemaViolation { .. })
            ),
            "case {id}"
        );
    }

    for diagnostic in [
        format!("{object:?}"),
        format!("{:?}", object.user_preferences_model()),
        format!("{:?}", object.streamer_info_model()),
        format!("{array:?}"),
        format!("{:?}", array.user_preferences_model()),
        format!("{:?}", array.streamer_info_model()),
    ] {
        for secret in [
            "synthetic-preference-account",
            "synthetic-customer-secret",
            "synthetic-correlation-secret",
            "synthetic-function-secret",
            "synthetic-unknown-secret",
            "synthetic-preference-extra",
            "synthetic-streamer-extra",
        ] {
            assert!(
                !diagnostic.contains(secret),
                "leaked {secret}: {diagnostic}"
            );
        }
    }

    let account_case = &cases[1];
    let account = ParsedReadResponse::parse(
        ReadResponseKind::Account,
        200,
        &serde_json::to_vec(&account_case["body"]).expect("account body encoding"),
    )
    .expect("synthetic account response");
    let TraderReadResponse::Account(account_model) = account.trader_model().expect("typed account")
    else {
        panic!("account route returns account model");
    };
    assert_eq!(
        account_model.unknown_fields.get("responseFutureField"),
        Some(&json!("synthetic-account-extra"))
    );
    let security = &account_model.securities_account;
    assert_eq!(
        security.round_trips.as_ref().map(WireNumber::as_str),
        Some("2.00")
    );
    let position = &security.positions.as_ref().expect("positions")[0];
    assert_eq!(
        position.short_quantity.as_ref().map(WireNumber::as_str),
        Some("0.125000")
    );
    assert_eq!(
        position.average_price.as_ref().map(WireNumber::as_str),
        Some("1.0000000000000001")
    );
    assert_eq!(
        position
            .instrument
            .as_ref()
            .and_then(|instrument| instrument.instrument_id.as_ref())
            .map(WireNumber::as_str),
        Some("9007199254740993")
    );
    assert!(position.unknown_fields.get("futurePositionField").is_some());
    assert_eq!(
        security
            .initial_balances
            .as_ref()
            .and_then(|balances| balances.get("cashBalance"))
            .map(WireNumber::as_str),
        Some("1000.0000")
    );
    assert!(
        security
            .current_balances
            .as_ref()
            .expect("current balances")
            .unknown_fields()
            .get("futureCurrentBalance")
            .is_some()
    );
    assert!(
        security
            .projected_balances
            .as_ref()
            .expect("projected balances")
            .unknown_fields()
            .get("futureProjectedBalance")
            .is_some()
    );

    let accounts_body =
        serde_json::to_vec(&json!([&account_case["body"]])).expect("account list body encoding");
    let accounts = ParsedReadResponse::parse(ReadResponseKind::Accounts, 200, &accounts_body)
        .expect("synthetic account list");
    assert!(matches!(
        accounts.trader_model(),
        Some(TraderReadResponse::Accounts(rows)) if rows.len() == 1
    ));

    let order_case = &cases[2];
    let orders = ParsedReadResponse::parse(
        ReadResponseKind::Orders,
        200,
        &serde_json::to_vec(&order_case["body"]).expect("orders body encoding"),
    )
    .expect("synthetic orders response");
    let TraderReadResponse::Orders(orders_model) = orders.trader_model().expect("typed orders")
    else {
        panic!("orders route returns order models");
    };
    let order = &orders_model[0];
    assert_eq!(
        order.order_id.as_ref().map(WireNumber::as_str),
        Some("9007199254740993")
    );
    assert_eq!(order.price.as_ref().map(WireNumber::as_str), Some("0.8300"));
    assert!(matches!(
        &order.account_number,
        Some(BrokerAccountNumber::Number(value)) if value.as_str() == "9007199254740995"
    ));
    assert_eq!(
        order
            .order_activity_collection
            .as_ref()
            .expect("activities")[0]
            .execution_legs
            .as_ref()
            .expect("execution legs")[0]
            .price
            .as_ref()
            .map(WireNumber::as_str),
        Some("0.410000")
    );
    assert_eq!(
        order.child_order_strategies.as_ref().expect("child order")[0]
            .order_id
            .as_ref()
            .map(WireNumber::as_str),
        Some("9007199254740999")
    );
    let single_order_body =
        serde_json::to_vec(&order_case["body"][0]).expect("single order body encoding");
    let single_order = ParsedReadResponse::parse(ReadResponseKind::Order, 200, &single_order_body)
        .expect("synthetic single order");
    assert!(matches!(
        single_order.trader_model(),
        Some(TraderReadResponse::Order(order))
            if order.order_id.as_ref().map(WireNumber::as_str) == Some("9007199254740993")
    ));

    let transaction_case = &cases[3];
    let transaction = ParsedReadResponse::parse(
        ReadResponseKind::Transaction,
        200,
        &serde_json::to_vec(&transaction_case["body"]).expect("transaction body encoding"),
    )
    .expect("synthetic transaction response");
    let TraderReadResponse::Transaction(TransactionResponse::One(transaction_model)) =
        transaction.trader_model().expect("typed transaction")
    else {
        panic!("transaction object shape is retained");
    };
    assert_eq!(
        transaction_model
            .activity_id
            .as_ref()
            .map(WireNumber::as_str),
        Some("9007199254740993")
    );
    assert_eq!(
        transaction_model
            .net_amount
            .as_ref()
            .map(WireNumber::as_str),
        Some("-0.830000")
    );
    assert_eq!(
        transaction_model
            .transfer_items
            .as_ref()
            .expect("transfer items")[0]
            .cost
            .as_ref()
            .map(WireNumber::as_str),
        Some("-0.8300")
    );
    assert!(
        transaction_model
            .unknown_fields
            .get("futureTransactionField")
            .is_some()
    );
    let transactions_body = serde_json::to_vec(&json!([&transaction_case["body"]]))
        .expect("transactions body encoding");
    let transactions =
        ParsedReadResponse::parse(ReadResponseKind::Transactions, 200, &transactions_body)
            .expect("synthetic transaction list");
    assert!(matches!(
        transactions.trader_model(),
        Some(TraderReadResponse::Transactions(rows))
            if rows.len() == 1
                && rows[0].activity_id.as_ref().map(WireNumber::as_str)
                    == Some("9007199254740993")
    ));

    let array_case = &cases[4];
    let transaction_array = ParsedReadResponse::parse(
        ReadResponseKind::Transaction,
        200,
        &serde_json::to_vec(&array_case["body"]).expect("transaction array encoding"),
    )
    .expect("synthetic transaction array");
    assert_eq!(
        transaction_array
            .transaction_model_convenience()
            .expect("first transaction")
            .activity_id
            .as_ref()
            .map(WireNumber::as_str),
        Some("41")
    );
    let empty_case = &cases[5];
    let empty = ParsedReadResponse::parse(
        ReadResponseKind::Transaction,
        200,
        &serde_json::to_vec(&empty_case["body"]).expect("empty transaction array encoding"),
    )
    .expect("empty transaction array is a schema-valid Node response");
    assert_eq!(
        empty.transaction_model_convenience().unwrap_err(),
        ReadResponseError::TransactionNotFound
    );

    let market_batch_case = cases
        .iter()
        .find(|case| case["kind"] == "MarketHoursBatch")
        .expect("market-hours batch golden");
    let market_batch = ParsedReadResponse::parse(
        ReadResponseKind::MarketHours,
        200,
        &serde_json::to_vec(&market_batch_case["body"]).expect("market-hours batch body"),
    )
    .expect("market-hours batch shape");
    let Some(MarketReadResponse::MarketHours(hours)) = market_batch.market_model() else {
        panic!("batch route projects market-hours record");
    };
    let product = &hours.markets["EQUITY"]["equity"];
    assert_eq!(product.date, "2026-09-28");
    assert_eq!(product.market_type, "EQUITY");
    assert_eq!(product.product_name.as_deref(), Some("Equities"));
    assert!(!product.is_open);
    assert_eq!(
        product.unknown_fields.get("futureProductField"),
        Some(&json!({"note": "synthetic market hours"}))
    );
    let session = &product.session_hours["regularMarket"][0];
    assert_eq!(session.start, "2026-09-28T09:30:00-04:00");
    assert_eq!(
        session.unknown_fields.get("futureSessionTimeField"),
        Some(&json!("preserved-in-rust"))
    );
    let market_debug = format!("{market_batch:?} {:?}", market_batch.market_model());
    assert!(!market_debug.contains("synthetic market hours"));
    assert!(!market_debug.contains("preserved-in-rust"));

    let market_single_case = cases
        .iter()
        .find(|case| case["kind"] == "MarketHoursSingle")
        .expect("market-hours single golden");
    let market_single = ParsedReadResponse::parse(
        ReadResponseKind::MarketHours,
        200,
        &serde_json::to_vec(&market_single_case["body"]).expect("market-hours single body"),
    )
    .expect("single-market-hours response uses the same record shape");
    let Some(MarketReadResponse::MarketHours(hours)) = market_single.market_model() else {
        panic!("single route projects market-hours record");
    };
    assert_eq!(hours.markets["OPTION"]["option"].product_name, None);
    assert!(hours.markets["OPTION"]["option"].is_open);

    let search_case = cases
        .iter()
        .find(|case| case["kind"] == "InstrumentsSearch")
        .expect("instrument search golden");
    let search = ParsedReadResponse::parse(
        ReadResponseKind::InstrumentsSearch,
        200,
        &serde_json::to_vec(&search_case["body"]).expect("instrument search body"),
    )
    .expect("instrument search response");
    let Some(MarketReadResponse::InstrumentsSearch(search_model)) = search.market_model() else {
        panic!("search route projects instruments wrapper");
    };
    assert_eq!(search_model.instruments.len(), 2);
    assert_eq!(search_model.instruments[0].symbol.as_deref(), Some("SYNTH"));
    assert_eq!(search_model.instruments[1].symbol, None);
    assert_eq!(
        search_model.unknown_fields.get("futureSearchField"),
        Some(&json!("preserved"))
    );
    assert_eq!(
        search_model.instruments[0]
            .unknown_fields
            .get("futureInstrumentField"),
        Some(&json!({"source": "synthetic"}))
    );

    let detail_case = cases
        .iter()
        .find(|case| case["kind"] == "InstrumentDetail")
        .expect("instrument detail golden");
    let detail = ParsedReadResponse::parse(
        ReadResponseKind::InstrumentDetail,
        200,
        &serde_json::to_vec(&detail_case["body"]).expect("instrument detail body"),
    )
    .expect("instrument detail permits omitted optional fields");
    let Some(MarketReadResponse::InstrumentDetail(detail_model)) = detail.market_model() else {
        panic!("detail route projects one instrument summary");
    };
    assert_eq!(detail_model.symbol, None);
    assert_eq!(
        detail_model
            .unknown_fields
            .get("futureNumericToken")
            .and_then(Value::as_number)
            .map(ToString::to_string)
            .as_deref(),
        Some("9007199254740993")
    );
    assert_eq!(
        detail_model.unknown_fields.get("futureInstrumentField"),
        Some(&json!({"note": "synthetic"}))
    );
    assert!(!format!("{detail:?} {:?}", detail.market_model()).contains("synthetic"));

    for invalid in fixture["invalidMarketCases"]
        .as_array()
        .expect("invalid market DTO goldens")
    {
        let kind = match invalid["kind"].as_str().expect("invalid kind") {
            "MarketHoursBatch" | "MarketHoursSingle" => ReadResponseKind::MarketHours,
            "InstrumentsSearch" => ReadResponseKind::InstrumentsSearch,
            "InstrumentDetail" => ReadResponseKind::InstrumentDetail,
            other => panic!("unknown invalid market fixture kind: {other}"),
        };
        let bytes = serde_json::to_vec(&invalid["body"]).expect("invalid DTO body");
        assert!(
            matches!(
                ParsedReadResponse::parse(kind, 200, &bytes),
                Err(ReadResponseError::SchemaViolation { .. })
            ),
            "case {}",
            invalid["id"]
        );
    }
}

#[test]
fn typed_trader_projection_rejects_unrepresentable_numbers_and_redacts_debug() {
    let high_precision = br#"{"securitiesAccount":{"accountNumber":"synthetic-account","positions":[{"averagePrice":0.0000000000000000001}]}}"#;
    let precise = ParsedReadResponse::parse(ReadResponseKind::Account, 200, high_precision)
        .expect("finite high-precision wire token is retained");
    let Some(TraderReadResponse::Account(account)) = precise.trader_model() else {
        panic!("account route returns account model");
    };
    let price = account
        .securities_account
        .positions
        .as_ref()
        .expect("position")[0]
        .average_price
        .as_ref()
        .expect("average price");
    assert_eq!(price.as_str(), "0.0000000000000000001");
    assert_eq!(
        price.exact_decimal(),
        Err(ReadResponseError::DecimalOutOfRange)
    );

    let overflow = br#"{"securitiesAccount":{"accountNumber":"synthetic-account","positions":[{"averagePrice":1e309}]}}"#;
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Account, 200, overflow).unwrap_err(),
        ReadResponseError::SchemaViolation {
            field: "securitiesAccount.positions[].number"
        }
    );

    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Account, 200, b"{broken").unwrap_err(),
        ReadResponseError::InvalidJson
    );
    let oversized = vec![b' '; BODY_LIMIT + 1];
    assert_eq!(
        ParsedReadResponse::parse(ReadResponseKind::Account, 200, &oversized).unwrap_err(),
        ReadResponseError::BodyTooLarge
    );

    let source = br#"[{"accountNumber":"synthetic-account-secret","hashValue":"synthetic-hash-secret","unknownPayload":{"id":"synthetic-order-id-secret","price":0.8300}}]"#;
    let parsed = ParsedReadResponse::parse(ReadResponseKind::AccountNumbers, 200, source)
        .expect("synthetic secret-bearing response");
    let TraderReadResponse::AccountNumbers(accounts) = parsed.trader_model().expect("typed rows")
    else {
        panic!("account number route returns account-number models");
    };
    assert_eq!(accounts[0].hash_value, "synthetic-hash-secret");
    let unknown_number = accounts[0]
        .unknown_fields
        .get("unknownPayload")
        .and_then(|value| value.get("price"))
        .and_then(Value::as_number)
        .expect("unknown numeric token");
    assert_eq!(unknown_number.to_string(), "0.8300");
    for diagnostic in [
        format!("{parsed:?}"),
        format!("{:?}", parsed.trader_model()),
    ] {
        for secret in [
            "synthetic-account-secret",
            "synthetic-hash-secret",
            "synthetic-order-id-secret",
            "0.8300",
        ] {
            assert!(!diagnostic.contains(secret));
        }
    }
    let model_debug = format!("{:?}", parsed.trader_model());
    assert!(!model_debug.contains("synthetic"));
}

fn fixture_response(transport: &Value) -> FixtureResponse {
    let selected = transport
        .get("responses")
        .and_then(Value::as_array)
        .and_then(|responses| responses.last())
        .unwrap_or(transport);
    let status = u16::try_from(selected["status"].as_u64().expect("fixture status"))
        .expect("fixture status fits u16");
    let body = if selected["emptyBody"] == true || selected.get("body").is_none() {
        Vec::new()
    } else {
        serde_json::to_vec(&selected["body"]).expect("fixture response JSON")
    };
    FixtureResponse { status, body }
}

fn response_json(transport: &Value) -> Value {
    let selected = transport
        .get("responses")
        .and_then(Value::as_array)
        .and_then(|responses| responses.last())
        .unwrap_or(transport);
    selected.get("body").cloned().unwrap_or(Value::Null)
}

struct FixtureResponse {
    status: u16,
    body: Vec<u8>,
}

fn kind_for_method(method: &str) -> ReadResponseKind {
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

fn option_quote_response() -> ParsedReadResponse {
    let cases: Value = serde_json::from_str(FIXTURE).expect("fixture JSON");
    let case = cases["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|case| case["id"] == "market-option-quote-normalizes-contract-and-nbbo")
        .expect("option quote golden");
    let response = fixture_response(&case["transport"]);
    ParsedReadResponse::parse(ReadResponseKind::Quotes, response.status, &response.body)
        .expect("valid option quote fixture")
}

fn compare_quote_projection(actual: &NormalizedOptionQuote, expected: &Value, case_id: &str) {
    let expected_map = expected.as_object().expect("expected normalized quote");
    for (key, value) in expected_map {
        match key.as_str() {
            "symbol" => assert_eq!(
                actual.symbol,
                value.as_str().expect("expected string"),
                "{case_id}"
            ),
            "underlying" => assert_eq!(actual.underlying.as_deref(), value.as_str(), "{case_id}"),
            "contractType" => {
                assert_eq!(actual.contract_type.as_deref(), value.as_str(), "{case_id}");
            }
            "expiration" => assert_eq!(actual.expiration.as_deref(), value.as_str(), "{case_id}"),
            "realtime" => assert_eq!(actual.realtime, value.as_bool(), "{case_id}"),
            "strike" => assert_decimal_matches(actual.strike.as_ref(), value, case_id),
            "bid" => assert_decimal_matches(actual.bid.as_ref(), value, case_id),
            "ask" => assert_decimal_matches(actual.ask.as_ref(), value, case_id),
            "mid" => assert_decimal_matches(actual.mid.as_ref(), value, case_id),
            "quoteTime" => assert_decimal_matches(actual.quote_time.as_ref(), value, case_id),
            "delta" => assert_decimal_matches(actual.delta.as_ref(), value, case_id),
            "spread" => {
                let exact_spread = actual
                    .spread
                    .as_ref()
                    .map(ExactDecimal::as_string)
                    .expect("bid and ask produce exact spread");
                let exact_from_legs = match (
                    actual.bid.as_ref().map(ExactDecimal::as_string).as_deref(),
                    actual.ask.as_ref().map(ExactDecimal::as_string).as_deref(),
                ) {
                    (Some("29.9"), Some("30.1")) => "0.2",
                    (Some("28.95"), Some("29.05")) => "0.1",
                    _ => panic!("unclassified exact Decimal quote in {case_id}"),
                };
                assert_eq!(exact_spread, exact_from_legs, "{case_id}");
                assert_ne!(
                    exact_spread,
                    value.as_number().expect("Node spread number").to_string(),
                    "LEGACY_BINARY_FLOAT_NOISE: retain Node golden as observed; Rust Decimal uses exact legs"
                );
            }
            key => panic!("unexpected Node quote projection field {key} in {case_id}"),
        }
    }
}

fn assert_decimal_matches(actual: Option<&ExactDecimal>, expected: &Value, case_id: &str) {
    let expected = expected
        .as_number()
        .expect("expected JSON number")
        .to_string();
    let expected = ExactDecimal::parse(&expected).expect("bounded expected decimal");
    assert_eq!(actual, Some(&expected), "{case_id}");
}

#[test]
fn market_read_fixtures_have_route_selected_wire_dtos() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("read-surface fixture");
    let cases = fixture["cases"].as_array().expect("fixture cases");
    let expected = [
        (
            "market-batch-quotes-joins-symbols-and-fields",
            ReadResponseKind::Quotes,
        ),
        (
            "market-single-symbol-quote-path-encoding",
            ReadResponseKind::SingleQuote,
        ),
        (
            "market-option-chain-forwards-all-supported-fields",
            ReadResponseKind::OptionChain,
        ),
        (
            "market-option-expiration-chain-query",
            ReadResponseKind::OptionExpirationChain,
        ),
        (
            "market-price-history-query-preserves-zero-and-booleans",
            ReadResponseKind::PriceHistory,
        ),
        (
            "market-movers-encodes-path-symbol",
            ReadResponseKind::Movers,
        ),
    ];

    for (id, kind) in expected {
        let case = cases
            .iter()
            .find(|case| case["id"] == id)
            .unwrap_or_else(|| panic!("missing Node fixture {id}"));
        let body = serde_json::to_vec(&case["transport"]["body"])
            .expect("fixture market response serializes");
        let parsed = ParsedReadResponse::parse(kind, 200, &body)
            .unwrap_or_else(|error| panic!("market response {id} rejected: {error}"));
        let model = parsed
            .market_model()
            .unwrap_or_else(|| panic!("market DTO missing for {id}"));
        match (kind, model) {
            (ReadResponseKind::Quotes, MarketReadResponse::Quotes(rows)) => {
                let quote = rows.items.get("QQQ").expect("quote row");
                assert_eq!(quote.symbol, "QQQ");
                assert_eq!(quote.asset_main_type.as_deref(), Some("EQUITY"));
                assert_eq!(
                    quote.unknown_fields.get("futureQuoteField"),
                    Some(&json!({"value": 1}))
                );
            }
            (ReadResponseKind::SingleQuote, MarketReadResponse::SingleQuote(quote)) => {
                assert_eq!(quote.symbol.as_deref(), Some("QQQ   260814P00740000"));
                assert_eq!(
                    quote.previous_close.as_ref().map(WireNumber::as_str),
                    Some("500")
                );
                assert_eq!(quote.candles.as_ref().map(Vec::len), Some(0));
            }
            (ReadResponseKind::OptionChain, MarketReadResponse::OptionChain(chain)) => {
                assert_eq!(chain.symbol.as_deref(), Some("QQQ"));
                assert_eq!(chain.is_delayed, Some(false));
                assert_eq!(chain.put_exp_date_map.as_ref().map(BTreeMap::len), Some(0));
                assert_eq!(
                    chain.unknown_fields.get("futureBrokerField"),
                    Some(&json!(7))
                );
            }
            (
                ReadResponseKind::OptionExpirationChain,
                MarketReadResponse::OptionExpirationChain(chain),
            ) => {
                assert_eq!(chain.symbol.as_deref(), Some("QQQ"));
                assert_eq!(chain.expiration_list.as_ref().map(Vec::len), Some(0));
            }
            (ReadResponseKind::PriceHistory, MarketReadResponse::PriceHistory(history)) => {
                assert_eq!(history.symbol.as_deref(), Some("QQQ"));
                assert_eq!(history.empty, Some(true));
                assert_eq!(history.candles.len(), 0);
            }
            (ReadResponseKind::Movers, MarketReadResponse::Movers(movers)) => {
                assert_eq!(movers.screeners.len(), 0);
            }
            _ => panic!("route selected a mismatched market DTO for {id}"),
        }
    }
}

#[test]
fn market_wire_dtos_keep_number_lexemes_dynamic_keys_and_additive_values() {
    let quotes = ParsedReadResponse::parse(
        ReadResponseKind::Quotes,
        200,
        br#"{"QQQ":{"symbol":"QQQ","quote":{"bidPrice":0.0100,"askPrice":0.0200,"quoteTime":1790480000000,"futureQuoteField":{"source":"synthetic"}},"futureRootField":true}}"#,
    )
    .expect("valid synthetic quote");
    let Some(MarketReadResponse::Quotes(quotes)) = quotes.market_model() else {
        panic!("quotes DTO expected");
    };
    let quote = quotes.items.get("QQQ").expect("quote row");
    let detail = quote.quote.as_ref().expect("quote detail");
    assert_eq!(
        detail.bid_price.as_ref().map(WireNumber::as_str),
        Some("0.0100")
    );
    assert_eq!(
        detail.ask_price.as_ref().map(WireNumber::as_str),
        Some("0.0200")
    );
    assert_eq!(
        detail.unknown_fields.get("futureQuoteField"),
        Some(&json!({"source": "synthetic"}))
    );
    assert_eq!(
        quote.unknown_fields.get("futureRootField"),
        Some(&json!(true))
    );

    let chain = ParsedReadResponse::parse(
        ReadResponseKind::OptionChain,
        200,
        br#"{"symbol":"QQQ","putExpDateMap":{"2026-09-27:0":{"100.0000":{"putCall":"PUT","symbol":"SYNTH  260927P00100000","bidPrice":0.0100,"strikePrice":100.0000,"delta":0.125}}}}"#,
    )
    .expect("valid synthetic option chain");
    let Some(MarketReadResponse::OptionChain(chain)) = chain.market_model() else {
        panic!("option chain DTO expected");
    };
    let contract = &chain.put_exp_date_map.as_ref().expect("put map")["2026-09-27:0"]["100.0000"];
    assert_eq!(
        contract.strike_price.as_ref().map(WireNumber::as_str),
        Some("100.0000")
    );
    assert_eq!(contract.unknown_fields.get("delta"), Some(&json!(0.125)));
    assert!(!format!("{chain:?}").contains("SYNTH"));
}

fn quote_result_signature(
    result: Result<Vec<NormalizedOptionQuote>, ReadResponseError>,
) -> Result<Vec<Value>, ReadResponseError> {
    fn decimal(value: Option<&ExactDecimal>) -> Option<String> {
        value.map(ExactDecimal::as_string)
    }

    result.map(|quotes| {
        quotes
            .iter()
            .map(|quote| {
                let ratio = quote.spread_percent_of_mid.as_ref().map(|ratio| {
                    [
                        ratio.numerator().as_string(),
                        ratio.denominator().as_string(),
                    ]
                });
                json!({
                    "symbol": quote.symbol,
                    "underlying": quote.underlying,
                    "contractType": quote.contract_type,
                    "expiration": quote.expiration,
                    "strike": decimal(quote.strike.as_ref()),
                    "realtime": quote.realtime,
                    "quoteType": quote.quote_type,
                    "bid": decimal(quote.bid.as_ref()),
                    "ask": decimal(quote.ask.as_ref()),
                    "bidSize": decimal(quote.bid_size.as_ref()),
                    "askSize": decimal(quote.ask_size.as_ref()),
                    "mark": decimal(quote.mark.as_ref()),
                    "last": decimal(quote.last.as_ref()),
                    "mid": decimal(quote.mid.as_ref()),
                    "spread": decimal(quote.spread.as_ref()),
                    "spreadPercentOfMid": ratio,
                    "quoteTime": decimal(quote.quote_time.as_ref()),
                    "tradeTime": decimal(quote.trade_time.as_ref()),
                    "quoteAgeMs": decimal(quote.quote_age_ms.as_ref()),
                    "delta": decimal(quote.delta.as_ref()),
                    "gamma": decimal(quote.gamma.as_ref()),
                    "theta": decimal(quote.theta.as_ref()),
                    "vega": decimal(quote.vega.as_ref()),
                    "rho": decimal(quote.rho.as_ref()),
                    "volatility": decimal(quote.volatility.as_ref()),
                    "openInterest": decimal(quote.open_interest.as_ref()),
                    "totalVolume": decimal(quote.total_volume.as_ref()),
                    "underlyingPrice": decimal(quote.underlying_price.as_ref()),
                    "theoreticalOptionValue": decimal(quote.theoretical_option_value.as_ref()),
                    "timeValue": decimal(quote.time_value.as_ref()),
                    "intrinsicValue": decimal(quote.intrinsic_value.as_ref())
                })
            })
            .collect()
    })
}
