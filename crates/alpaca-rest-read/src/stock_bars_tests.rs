use std::collections::HashMap;

use alpaca_data::stocks::{Bar, BarsResponse, Currency};
use market_contracts::{DecimalString, EntitlementState, UtcTimestamp};

use super::{
    AlpacaStockBarsPage, AlpacaStockBarsRequest, MAX_STOCK_BARS_PAGE_SIZE, RequestedStockBarsFeed,
    StockBarsCursor, StockBarsTimeframe,
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
fn request_fixes_sip_query_and_validates_boundaries() {
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
