use std::collections::HashMap;

use alpaca_data::stocks::{Bar, BarsResponse, Currency};
use market_contracts::{DecimalString, EntitlementState, UtcTimestamp};

use super::{
    AlpacaStockBarsPage, AlpacaStockBarsRequest, MAX_STOCK_BARS_PAGE_SIZE, RequestedStockBarsFeed,
    StockBarsCursor, StockBarsSort, StockBarsTimeframe, typed_bars_response_sha256,
};
use crate::AlpacaRestError;

const SYMBOL: &str = "QQQ";
const START: &str = "2026-10-08T13:30:00Z";
const END: &str = "2026-10-08T13:32:00Z";

fn request() -> AlpacaStockBarsRequest {
    AlpacaStockBarsRequest::new(
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid synthetic start"),
        UtcTimestamp::parse(END).expect("valid synthetic end"),
        100,
    )
    .expect("valid synthetic query")
}

fn response(bars: Vec<Bar>, next_page_token: Option<&str>) -> BarsResponse {
    BarsResponse {
        bars: HashMap::from([(SYMBOL.to_owned(), bars)]),
        next_page_token: next_page_token.map(str::to_owned),
        currency: Some(Currency::usd()),
    }
}

fn bar(timestamp: &str) -> Bar {
    Bar {
        t: Some(timestamp.to_owned()),
        o: Some("10.00".parse().expect("valid synthetic decimal")),
        h: Some("11.00".parse().expect("valid synthetic decimal")),
        l: Some("9.00".parse().expect("valid synthetic decimal")),
        c: Some("10.50".parse().expect("valid synthetic decimal")),
        v: Some(100),
        n: Some(3),
        vw: Some("10.25".parse().expect("valid synthetic decimal")),
    }
}

fn received_at() -> UtcTimestamp {
    UtcTimestamp::parse("2026-10-09T00:00:00Z").expect("valid synthetic receive time")
}

#[test]
fn request_fixes_default_sip_query_and_validates_boundaries() {
    let request = request();
    let sdk = request.to_sdk_request();
    assert_eq!(sdk.symbols, vec![SYMBOL.to_owned()]);
    assert_eq!(sdk.timeframe.as_str(), "1Min");
    assert_eq!(sdk.start.as_deref(), Some(START));
    assert_eq!(sdk.end.as_deref(), Some(END));
    assert_eq!(sdk.limit, Some(100));
    assert_eq!(sdk.feed.expect("feed is fixed").to_string(), "sip");
    assert_eq!(
        sdk.adjustment.expect("adjustment is fixed").to_string(),
        "raw"
    );
    assert_eq!(sdk.sort.expect("sort is fixed").to_string(), "asc");
    assert_eq!(sdk.currency.expect("currency is fixed").as_str(), "USD");
    assert_eq!(sdk.asof.as_deref(), Some("-"));
    assert_eq!(request.requested_feed(), RequestedStockBarsFeed::Sip);

    for invalid in ["qqq", " QQQ", "QQQ/../SPY", "QQQ\n", "BRKB"] {
        assert!(
            AlpacaStockBarsRequest::new(
                invalid,
                StockBarsTimeframe::Minute1,
                UtcTimestamp::parse(START).expect("valid time"),
                UtcTimestamp::parse(END).expect("valid time"),
                1
            )
            .is_err()
        );
    }
    assert!(
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute1,
            UtcTimestamp::parse(END).expect("valid time"),
            UtcTimestamp::parse(START).expect("valid time"),
            1
        )
        .is_err()
    );
    assert!(
        AlpacaStockBarsRequest::new(
            "BRK.B",
            StockBarsTimeframe::Minute1,
            UtcTimestamp::parse(START).expect("valid time"),
            UtcTimestamp::parse(END).expect("valid time"),
            1
        )
        .is_ok()
    );
    assert!(
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute1,
            UtcTimestamp::parse(START).expect("valid time"),
            UtcTimestamp::parse(END).expect("valid time"),
            MAX_STOCK_BARS_PAGE_SIZE + 1
        )
        .is_err()
    );
}

#[test]
fn boats_request_is_explicit_and_query_getters_are_available() {
    let request = AlpacaStockBarsRequest::new_with_feed(
        RequestedStockBarsFeed::Boats,
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid time"),
        UtcTimestamp::parse(END).expect("valid time"),
        100,
    )
    .expect("valid synthetic BOATS query");
    let sdk = request.to_sdk_request();
    assert_eq!(sdk.feed.expect("feed is explicit").to_string(), "boats");
    assert_eq!(request.requested_feed(), RequestedStockBarsFeed::Boats);
    assert_eq!(request.requested_feed().as_str(), "boats");
    assert_eq!(request.timeframe(), StockBarsTimeframe::Minute1);
    assert_eq!(request.start().as_str(), START);
    assert_eq!(request.end().as_str(), END);
    assert_eq!(request.page_size(), 100);
    assert_eq!(request.sort(), StockBarsSort::Asc);
}

#[test]
fn descending_request_sends_desc_and_validates_latest_first_pages() {
    let descending = AlpacaStockBarsRequest::new_with_feed_and_sort(
        RequestedStockBarsFeed::Sip,
        StockBarsSort::Desc,
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid start"),
        UtcTimestamp::parse(END).expect("valid end"),
        100,
    )
    .expect("valid descending query");
    let sdk = descending.to_sdk_request();
    assert_eq!(sdk.sort.expect("sort is explicit").to_string(), "desc");
    assert_eq!(descending.sort(), StockBarsSort::Desc);
    assert_eq!(descending.sort().as_str(), "desc");

    let mut page = AlpacaStockBarsPage::from_provider(
        &descending,
        response(
            vec![
                bar("2026-10-08T13:32:00Z"),
                bar("2026-10-08T13:31:00Z"),
                bar(START),
            ],
            Some("synthetic-desc-token"),
        ),
        received_at(),
    )
    .expect("descending provider order validates");
    assert_eq!(
        page.bars()
            .iter()
            .map(|bar| bar.timestamp().as_str())
            .collect::<Vec<_>>(),
        ["2026-10-08T13:32:00Z", "2026-10-08T13:31:00Z", START]
    );
    let cursor = page.take_next_cursor().expect("provider has a next page");
    let encoded = serde_json::to_string(&cursor).expect("cursor serializes");
    assert!(encoded.contains("\"sort\":\"desc\""));
    let decoded: StockBarsCursor =
        serde_json::from_str(&encoded).expect("descending cursor deserializes");
    assert!(descending.clone().with_cursor(decoded.clone()).is_ok());
    assert_eq!(
        request().with_cursor(decoded),
        Err(AlpacaRestError::InvalidRequest)
    );

    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &descending,
            response(vec![bar(START), bar("2026-10-08T13:31:00Z")], None),
            received_at(),
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
}

#[test]
fn provider_bars_preserve_exact_values_and_unknown_effective_source() {
    let page = AlpacaStockBarsPage::from_provider(
        &request(),
        response(vec![bar(START)], None),
        received_at(),
    )
    .expect("synthetic page validates");
    assert_eq!(page.bars().len(), 1);
    let bar = &page.bars()[0];
    assert_eq!(bar.symbol(), SYMBOL);
    assert_eq!(bar.open().as_str(), "10.00");
    assert_eq!(bar.high().as_str(), "11.00");
    assert_eq!(bar.low().as_str(), "9.00");
    assert_eq!(bar.close().as_str(), "10.50");
    assert_eq!(bar.volume(), 100);
    assert_eq!(bar.trade_count(), Some(3));
    assert_eq!(
        bar.volume_weighted_price().map(DecimalString::as_str),
        Some("10.25")
    );
    assert_eq!(bar.requested_feed(), RequestedStockBarsFeed::Sip);
    assert_eq!(bar.source().provider, "alpaca");
    assert_eq!(bar.source().feed, "unknown");
    assert_eq!(bar.source().entitlement, EntitlementState::Unknown);
    assert_eq!(
        bar.source().numeric_encoding,
        market_contracts::NumericEncodingV1::DecimalToken
    );
    assert_eq!(page.response_observed_at().as_str(), "2026-10-09T00:00:00Z");
    assert!(!page.has_next_page());
    assert!(page.query_access_witness().is_none());
}

#[test]
fn synthetic_empty_page_has_no_query_access_witness_or_completeness_claim() {
    let page =
        AlpacaStockBarsPage::from_provider(&request(), response(Vec::new(), None), received_at())
            .expect("empty synthetic provider page is structurally valid");

    assert!(page.bars().is_empty());
    assert!(!page.has_next_page());
    assert!(page.query_access_witness().is_none());
}

#[test]
fn typed_response_digest_is_stable_and_binds_rows_and_cursor() {
    let original = response(vec![bar(START)], Some("synthetic-page-token"));
    let same_typed_response = response(vec![bar(START)], Some("synthetic-page-token"));
    let changed_cursor = response(vec![bar(START)], Some("different-synthetic-token"));
    let changed_row = response(
        vec![bar("2026-10-08T13:31:00Z")],
        Some("synthetic-page-token"),
    );

    let digest = typed_bars_response_sha256(&original);
    assert_eq!(digest, typed_bars_response_sha256(&same_typed_response));
    assert_ne!(digest, typed_bars_response_sha256(&changed_cursor));
    assert_ne!(digest, typed_bars_response_sha256(&changed_row));
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
    assert!(!digest.contains("synthetic-page-token"));
}

#[test]
fn normalized_sdk_response_fixture_keeps_decimal_values_exact() {
    let response: BarsResponse = serde_json::from_str(
        r#"{
            "bars": {
                "QQQ": [{
                    "t": "2026-10-08T13:30:00Z",
                    "o": 10.00000000000000000001,
                    "h": 11.00000000000000000001,
                    "l": 9.00000000000000000001,
                    "c": 10.50000000000000000001,
                    "v": 100,
                    "n": 3,
                    "vw": 10.25000000000000000001
                }]
            },
            "next_page_token": null,
            "currency": "USD"
        }"#,
    )
    .expect("synthetic normalized SDK response fixture parses through the pinned model");
    let page = AlpacaStockBarsPage::from_provider(&request(), response, received_at())
        .expect("synthetic normalized SDK bars validate");
    assert_eq!(page.bars()[0].open().as_str(), "10.00000000000000000001");
    assert_eq!(page.bars()[0].close().as_str(), "10.50000000000000000001");
    assert_eq!(
        page.bars()[0]
            .volume_weighted_price()
            .map(DecimalString::as_str),
        Some("10.25000000000000000001")
    );
}

#[test]
fn continuation_is_redacted_and_bound_to_the_full_query() {
    let mut page = AlpacaStockBarsPage::from_provider(
        &request(),
        response(vec![bar(START)], Some("synthetic-page-token")),
        received_at(),
    )
    .expect("synthetic page validates");
    let cursor = page.take_next_cursor().expect("continuation returned");
    assert!(!format!("{cursor:?}").contains("synthetic-page-token"));
    assert!(request().with_cursor(cursor.clone()).is_ok());

    let different_symbol = AlpacaStockBarsRequest::new(
        "SPY",
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid time"),
        UtcTimestamp::parse(END).expect("valid time"),
        100,
    )
    .expect("valid synthetic query");
    assert_eq!(
        different_symbol.with_cursor(cursor),
        Err(AlpacaRestError::InvalidRequest)
    );
}

#[test]
fn continuation_rejects_every_mutable_query_identity_field() {
    let mut page = AlpacaStockBarsPage::from_provider(
        &request(),
        response(vec![bar(START)], Some("synthetic-page-token")),
        received_at(),
    )
    .expect("synthetic page validates");
    let cursor = page.take_next_cursor().expect("continuation returned");
    let start = UtcTimestamp::parse(START).expect("valid start");
    let end = UtcTimestamp::parse(END).expect("valid end");
    let alternatives = [
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute5,
            start.clone(),
            end.clone(),
            100,
        ),
        AlpacaStockBarsRequest::new_with_feed(
            RequestedStockBarsFeed::Boats,
            SYMBOL,
            StockBarsTimeframe::Minute1,
            UtcTimestamp::parse(START).expect("valid start"),
            UtcTimestamp::parse(END).expect("valid end"),
            100,
        ),
        AlpacaStockBarsRequest::new_with_feed_and_sort(
            RequestedStockBarsFeed::Sip,
            StockBarsSort::Desc,
            SYMBOL,
            StockBarsTimeframe::Minute1,
            start.clone(),
            end.clone(),
            100,
        ),
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute1,
            start.clone(),
            end.clone(),
            99,
        ),
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute1,
            UtcTimestamp::parse("2026-10-08T13:30:01Z").expect("valid start"),
            end.clone(),
            100,
        ),
        AlpacaStockBarsRequest::new(
            SYMBOL,
            StockBarsTimeframe::Minute1,
            start,
            UtcTimestamp::parse("2026-10-08T13:32:01Z").expect("valid end"),
            100,
        ),
    ];
    for alternative in alternatives {
        assert_eq!(
            alternative
                .expect("valid alternate query")
                .with_cursor(cursor.clone()),
            Err(AlpacaRestError::InvalidRequest)
        );
    }
}

#[test]
fn continuation_cursor_round_trips_as_versioned_opaque_state() {
    let mut page = AlpacaStockBarsPage::from_provider(
        &request(),
        response(vec![bar(START)], Some("synthetic-page-token")),
        received_at(),
    )
    .expect("synthetic page validates");
    let cursor = page.take_next_cursor().expect("continuation returned");
    let encoded = serde_json::to_string(&cursor).expect("cursor serializes");
    assert!(!format!("{cursor:?}").contains("synthetic-page-token"));

    let decoded: StockBarsCursor =
        serde_json::from_str(&encoded).expect("versioned cursor deserializes");
    assert_eq!(decoded, cursor);
    assert!(request().with_cursor(decoded).is_ok());

    let mut malformed: serde_json::Value =
        serde_json::from_str(&encoded).expect("serialized cursor is JSON");
    malformed["version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<StockBarsCursor>(malformed).is_err());
}

#[test]
fn provider_page_rows_and_cursor_are_bounded_and_validated() {
    let one_bar_request = AlpacaStockBarsRequest::new(
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid start"),
        UtcTimestamp::parse(END).expect("valid end"),
        1,
    )
    .expect("valid one-row synthetic query");
    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &one_bar_request,
            response(vec![bar(START), bar("2026-10-08T13:31:00Z")], None),
            received_at()
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &request(),
            response(vec![bar("2026-10-08T13:33:00Z")], None),
            received_at()
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &request(),
            response(vec![bar(START), bar(START)], None),
            received_at()
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &request(),
            response(
                vec![bar(START)],
                Some("synthetic-page-token".repeat(40).as_str())
            ),
            received_at()
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
    assert_eq!(
        StockBarsCursor::from_provider(
            request().identity,
            "synthetic-page-token".to_owned(),
            None,
            Some("synthetic-page-token")
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );
}

#[test]
fn malformed_provider_bars_fail_closed() {
    let mut malformed = bar(START);
    malformed.h = Some("8".parse().expect("valid synthetic decimal"));
    assert_eq!(
        AlpacaStockBarsPage::from_provider(
            &request(),
            response(vec![malformed], None),
            received_at()
        ),
        Err(AlpacaRestError::ProtocolViolation)
    );

    let mut zero = bar(START);
    zero.c = Some("0.00".parse().expect("valid synthetic zero"));
    assert_eq!(
        AlpacaStockBarsPage::from_provider(&request(), response(vec![zero], None), received_at()),
        Err(AlpacaRestError::ProtocolViolation)
    );

    let mut wrong_currency = response(vec![bar(START)], None);
    wrong_currency.currency = Some(Currency::from("EUR"));
    assert_eq!(
        AlpacaStockBarsPage::from_provider(&request(), wrong_currency, received_at()),
        Err(AlpacaRestError::ProtocolViolation)
    );
}
