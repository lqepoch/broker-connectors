use std::collections::HashMap;

use alpaca_data::stocks::{Bar, BarsResponse, Currency};
use market_contracts::UtcTimestamp;

use super::{StockBarsPageProgress, StockBarsWindowBuilder};
use crate::{
    AlpacaRestError, AlpacaStockBarsPage, AlpacaStockBarsRequest, AlpacaStockBarsWindowRequest,
    MAX_STOCK_BARS_PAGES_PER_WINDOW, RequestedStockBarsFeed, StockBarsSort, StockBarsTimeframe,
};

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

fn descending_request() -> AlpacaStockBarsRequest {
    AlpacaStockBarsRequest::new_with_feed_and_sort(
        RequestedStockBarsFeed::Sip,
        StockBarsSort::Desc,
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid synthetic start"),
        UtcTimestamp::parse(END).expect("valid synthetic end"),
        100,
    )
    .expect("valid descending synthetic query")
}

fn response(timestamp: &str, token: Option<&str>) -> BarsResponse {
    response_bars(vec![bar(timestamp)], token)
}

fn response_bars(bars: Vec<Bar>, token: Option<&str>) -> BarsResponse {
    BarsResponse {
        bars: HashMap::from([(SYMBOL.to_owned(), bars)]),
        next_page_token: token.map(str::to_owned),
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

fn page(
    request: &AlpacaStockBarsRequest,
    timestamp: &str,
    token: Option<&str>,
) -> AlpacaStockBarsPage {
    AlpacaStockBarsPage::from_provider(
        request,
        response(timestamp, token),
        UtcTimestamp::parse("2026-10-09T00:00:00Z").expect("valid synthetic receive time"),
    )
    .expect("synthetic provider page validates")
}

#[test]
fn window_request_rejects_unbounded_page_limits() {
    assert_eq!(
        AlpacaStockBarsWindowRequest::new(request(), 0),
        Err(AlpacaRestError::InvalidRequest)
    );
    assert_eq!(
        AlpacaStockBarsWindowRequest::new(request(), MAX_STOCK_BARS_PAGES_PER_WINDOW + 1),
        Err(AlpacaRestError::InvalidRequest)
    );
    assert!(AlpacaStockBarsWindowRequest::new(request(), 1).is_ok());
}

#[test]
fn provider_end_returns_one_page_without_claiming_archive_completeness() {
    let mut builder = StockBarsWindowBuilder::new(4, None);
    assert!(matches!(
        builder.push(page(&request(), START, None)),
        Ok(StockBarsPageProgress::Stop)
    ));
    let mut window = builder.finish();
    assert_eq!(window.pages_read(), 1);
    assert_eq!(window.bar_count(), 1);
    assert!(!window.has_more_pages());
    assert!(window.take_next_cursor().is_none());
}

#[test]
fn page_limit_returns_a_partial_window_with_a_serializable_continuation() {
    let mut builder = StockBarsWindowBuilder::new(2, None);
    let first = request();
    let cursor = match builder
        .push(page(&first, START, Some("synthetic-token-a")))
        .expect("first page advances")
    {
        StockBarsPageProgress::Continue(cursor) => *cursor,
        StockBarsPageProgress::Stop => panic!("first page has a continuation"),
    };

    let second = first.with_cursor(cursor).expect("cursor matches query");
    assert!(matches!(
        builder.push(page(
            &second,
            "2026-10-08T13:31:00Z",
            Some("synthetic-token-b")
        )),
        Ok(StockBarsPageProgress::Stop)
    ));

    let mut window = builder.finish();
    assert_eq!(window.pages_read(), 2);
    assert_eq!(window.bar_count(), 2);
    assert!(window.has_more_pages());
    let continuation = window.take_next_cursor().expect("window was capped");
    let encoded = serde_json::to_string(&continuation).expect("cursor serializes");
    let decoded = serde_json::from_str(&encoded).expect("cursor deserializes");
    assert_eq!(continuation, decoded);
}

#[test]
fn provider_cursor_cycle_fails_closed_across_pages() {
    let mut builder = StockBarsWindowBuilder::new(4, None);
    let first = request();
    let cursor_a = match builder
        .push(page(&first, START, Some("synthetic-token-a")))
        .expect("first page advances")
    {
        StockBarsPageProgress::Continue(cursor) => *cursor,
        StockBarsPageProgress::Stop => panic!("first page has a continuation"),
    };

    let second = first
        .clone()
        .with_cursor(cursor_a)
        .expect("cursor matches query");
    let cursor_b = match builder
        .push(page(
            &second,
            "2026-10-08T13:31:00Z",
            Some("synthetic-token-b"),
        ))
        .expect("second page advances")
    {
        StockBarsPageProgress::Continue(cursor) => *cursor,
        StockBarsPageProgress::Stop => panic!("second page has a continuation"),
    };

    let third = first.with_cursor(cursor_b).expect("cursor matches query");
    assert_eq!(
        builder.push(page(
            &third,
            "2026-10-08T13:32:00Z",
            Some("synthetic-token-a")
        )),
        Err(AlpacaRestError::ProtocolViolation)
    );
}

#[test]
fn resumed_window_seeds_cursor_cycle_detection_with_input_cursor() {
    let initial = request();
    let first = page(&initial, START, Some("synthetic-token-a"));
    let cursor_a = first
        .clone()
        .take_next_cursor()
        .expect("first provider page has a cursor");
    let resumed = initial
        .clone()
        .with_cursor(cursor_a)
        .expect("cursor matches query");
    let mut builder = StockBarsWindowBuilder::new(4, resumed.cursor());

    let cursor_b = match builder
        .push(page(
            &resumed,
            "2026-10-08T13:31:00Z",
            Some("synthetic-token-b"),
        ))
        .expect("second provider page advances")
    {
        StockBarsPageProgress::Continue(cursor) => *cursor,
        StockBarsPageProgress::Stop => panic!("page has a continuation"),
    };
    let third = initial.with_cursor(cursor_b).expect("cursor matches query");

    assert_eq!(
        builder.push(page(
            &third,
            "2026-10-08T13:32:00Z",
            Some("synthetic-token-a")
        )),
        Err(AlpacaRestError::ProtocolViolation)
    );
}

#[test]
fn query_feed_remains_explicit_across_paginated_continuation() {
    let base = AlpacaStockBarsRequest::new_with_feed(
        RequestedStockBarsFeed::Boats,
        SYMBOL,
        StockBarsTimeframe::Minute1,
        UtcTimestamp::parse(START).expect("valid start"),
        UtcTimestamp::parse(END).expect("valid end"),
        100,
    )
    .expect("valid BOATS query");
    let mut builder = StockBarsWindowBuilder::new(1, None);
    assert!(matches!(
        builder.push(page(&base, START, Some("synthetic-boats-token"))),
        Ok(StockBarsPageProgress::Stop)
    ));
    let window = builder.finish();
    assert_eq!(
        window
            .bars()
            .next()
            .map(crate::AlpacaStockBarObservation::requested_feed),
        Some(RequestedStockBarsFeed::Boats)
    );
}

#[test]
fn descending_window_continues_from_recent_bars_to_older_bars() {
    let initial = descending_request();
    let first_page = AlpacaStockBarsPage::from_provider(
        &initial,
        response_bars(
            vec![bar("2026-10-08T13:32:00Z"), bar("2026-10-08T13:31:00Z")],
            Some("synthetic-desc-token-a"),
        ),
        UtcTimestamp::parse("2026-10-09T00:00:00Z").expect("valid synthetic receive time"),
    )
    .expect("descending first page validates");
    let mut builder = StockBarsWindowBuilder::new(4, None);
    let cursor = match builder.push(first_page).expect("first page advances") {
        StockBarsPageProgress::Continue(cursor) => *cursor,
        StockBarsPageProgress::Stop => panic!("first descending page has a continuation"),
    };
    let continued = initial
        .with_cursor(cursor)
        .expect("descending cursor retains query order");
    let last_page = AlpacaStockBarsPage::from_provider(
        &continued,
        response_bars(vec![bar(START)], None),
        UtcTimestamp::parse("2026-10-09T00:00:00Z").expect("valid synthetic receive time"),
    )
    .expect("descending continuation validates older page");
    assert_eq!(builder.push(last_page), Ok(StockBarsPageProgress::Stop));

    let window = builder.finish();
    assert_eq!(window.pages_read(), 2);
    assert_eq!(
        window
            .bars()
            .map(|bar| bar.timestamp().as_str())
            .collect::<Vec<_>>(),
        ["2026-10-08T13:32:00Z", "2026-10-08T13:31:00Z", START]
    );
}
