#[path = "support/fake_gateway.rs"]
mod fake_gateway;

use std::time::Duration;

use domain::OptionSymbol;
use fake_gateway::{CANCEL_CONTRACT_DATA, ContractFixture, FakeGateway, Observation, ResponsePlan};
use ibkr_read::{
    IbkrCatalogAdapter, IbkrCatalogError, IbkrCatalogTimeouts, IbkrOptionCatalogQuery,
};

fn query() -> IbkrOptionCatalogQuery {
    IbkrOptionCatalogQuery::new(
        OptionSymbol::parse("AAPL  270115C00150000").expect("valid synthetic OCC symbol"),
        "CBOE",
        "USD",
    )
    .expect("explicit catalog scope")
}

fn timeouts(request: Duration) -> IbkrCatalogTimeouts {
    IbkrCatalogTimeouts::new(
        Duration::from_secs(2),
        request,
        Duration::from_millis(100),
        Duration::from_millis(500),
    )
    .expect("non-zero bounded timeouts")
}

async fn connect(gateway: &FakeGateway, request_timeout: Duration) -> IbkrCatalogAdapter {
    IbkrCatalogAdapter::connect(gateway.address, 17, timeouts(request_timeout))
        .await
        .expect("connect to loopback synthetic gateway")
}

fn assert_read_only_requests(observation: &Observation) {
    assert!(
        observation.outbound_ids.contains(&71),
        "expected TWS StartApi handshake"
    );
    assert!(
        observation.outbound_ids.contains(&9),
        "expected only contract-details lookup"
    );
    assert!(
        observation
            .outbound_ids
            .iter()
            .all(|id| matches!(*id, 71 | 9 | CANCEL_CONTRACT_DATA)),
        "adapter emitted an unexpected SDK operation: {:?}",
        observation.outbound_ids
    );
}

#[tokio::test]
async fn exact_catalog_row_is_returned_only_after_native_end() {
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(vec![ContractFixture::exact(
        123_456,
    )]))
    .await
    .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    let entry = adapter
        .lookup_option(&query())
        .await
        .expect("exact catalog result");
    assert_eq!(entry.provider_identity().contract_id(), 123_456);
    assert_eq!(entry.provider_identity().exchange(), "CBOE");
    assert_eq!(entry.provider_identity().currency().as_str(), "USD");
    assert!(
        entry.candidate().qualify().is_err(),
        "missing economics must block qualification"
    );

    adapter.disconnect().await.expect("bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
    let search = observation
        .search
        .expect("contract-details search captured");
    assert_eq!(
        search.contract_id, 0,
        "zero is only the SDK's search sentinel"
    );
    assert_eq!(search.symbol, "AAPL");
    assert_eq!(search.security_type, "OPT");
    assert_eq!(search.expiration, "20270115");
    assert_eq!(search.strike, 150.0);
    assert_eq!(search.right, "C");
    assert_eq!(search.exchange, "CBOE");
    assert_eq!(search.currency, "USD");
    assert_eq!(search.local_symbol, "AAPL  270115C00150000");
}

#[tokio::test]
async fn empty_native_end_returns_no_match() {
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(Vec::new()))
        .await
        .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::NoMatch)
    );

    adapter.disconnect().await.expect("bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
}

#[tokio::test]
async fn multiple_exact_rows_fail_closed_after_native_end() {
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(vec![
        ContractFixture::exact(123_456),
        ContractFixture::exact(123_457),
    ]))
    .await
    .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::AmbiguousMatch)
    );

    adapter.disconnect().await.expect("bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
}

#[tokio::test]
async fn mismatched_provider_identity_is_rejected_after_native_end() {
    let mut wrong_exchange = ContractFixture::exact(123_456);
    wrong_exchange.exchange = "SMART".to_string();
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(vec![wrong_exchange]))
        .await
        .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::InvalidProviderRow)
    );

    adapter.disconnect().await.expect("bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
}

#[tokio::test]
async fn row_limit_cancels_but_still_requires_native_end() {
    let rows = (1..=17).map(ContractFixture::exact).collect();
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(rows))
        .await
        .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::TooManyRows)
    );

    adapter.disconnect().await.expect("bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
    assert!(
        observation.outbound_ids.contains(&CANCEL_CONTRACT_DATA),
        "row cap must issue native cancel"
    );
}

#[tokio::test]
async fn cancel_is_confirmed_only_after_native_end_arrives() {
    let rows = (1..=17).map(ContractFixture::exact).collect();
    let gateway = FakeGateway::start(ResponsePlan::RowsThenEndAfterCancel(rows))
        .await
        .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::TooManyRows)
    );
    adapter.disconnect().await.expect("bounded disconnect");

    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
    assert!(observation.outbound_ids.contains(&CANCEL_CONTRACT_DATA));
}

#[tokio::test]
async fn explicit_disconnect_rejects_later_lookup() {
    let gateway = FakeGateway::start(ResponsePlan::RowsAndEnd(Vec::new()))
        .await
        .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;
    adapter.disconnect().await.expect("bounded disconnect");

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::SessionPoisoned)
    );
    let observation = gateway.finish().await.expect("finish local fake server");
    assert!(observation.outbound_ids.contains(&71));
    assert!(observation.outbound_ids.iter().all(|id| *id == 71));
}

#[tokio::test]
async fn non_loopback_endpoints_are_rejected_before_connecting() {
    let endpoint: std::net::SocketAddr = "192.0.2.1:4001".parse().expect("test address");
    assert!(matches!(
        IbkrCatalogAdapter::connect(endpoint, 17, timeouts(Duration::from_secs(1))).await,
        Err(IbkrCatalogError::InvalidEndpoint)
    ));

    let endpoint: std::net::SocketAddr = "127.0.0.1:0".parse().expect("test address");
    assert!(matches!(
        IbkrCatalogAdapter::connect(endpoint, 17, timeouts(Duration::from_secs(1))).await,
        Err(IbkrCatalogError::InvalidEndpoint)
    ));
}

#[tokio::test]
async fn connect_timeout_closes_unresponsive_loopback_gateway() {
    let gateway = FakeGateway::start(ResponsePlan::NoHandshakeResponse)
        .await
        .expect("start unresponsive synthetic gateway");
    assert!(matches!(
        IbkrCatalogAdapter::connect(gateway.address, 17, timeouts(Duration::from_millis(50)),)
            .await,
        Err(IbkrCatalogError::ConnectTimeout)
    ));

    let observation = gateway.finish().await.expect("finish local fake server");
    assert!(observation.outbound_ids.is_empty());
}

#[tokio::test]
async fn missing_end_times_out_cancels_and_poisons_the_session() {
    let gateway = FakeGateway::start(ResponsePlan::RowsWithoutEnd(vec![ContractFixture::exact(
        123_456,
    )]))
    .await
    .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_millis(80)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::SessionPoisoned)
    );
    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::SessionPoisoned)
    );

    adapter
        .disconnect()
        .await
        .expect("idempotent bounded disconnect");
    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
    assert_eq!(
        observation
            .outbound_ids
            .iter()
            .filter(|id| **id == 9)
            .count(),
        1,
        "poisoned session must not issue another contract query"
    );
    assert!(
        observation.outbound_ids.contains(&CANCEL_CONTRACT_DATA),
        "timeout must issue native cancel"
    );
}

#[tokio::test]
async fn peer_close_without_end_poisons_the_session() {
    let gateway = FakeGateway::start(ResponsePlan::CloseAfterRows(vec![ContractFixture::exact(
        123_456,
    )]))
    .await
    .expect("start fake loopback gateway");
    let adapter = connect(&gateway, Duration::from_secs(2)).await;

    assert_eq!(
        adapter.lookup_option(&query()).await,
        Err(IbkrCatalogError::SessionPoisoned)
    );
    adapter
        .disconnect()
        .await
        .expect("close the poisoned SDK client");

    let observation = gateway.finish().await.expect("finish local fake server");
    assert_read_only_requests(&observation);
}
