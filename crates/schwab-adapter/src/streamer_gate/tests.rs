use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use broker_ports::PortFuture;
use domain::{AccountNamespace, AccountScope, BrokerEnvironment, ExecutionBrokerId};
use zeroize::Zeroizing;

use super::{
    SchwabStreamerBootstrapError, SchwabStreamerBootstrapLease, SchwabStreamerBootstrapPort,
    SchwabStreamerGate, SchwabStreamerGateError, TrustedWssEndpoint,
};

const ALLOWLISTED_URL: &str = "wss://streamer.example.invalid:443/provided/path?session=synthetic";

struct FakeBootstrap {
    namespace: AccountNamespace,
    lease: Mutex<Option<SchwabStreamerBootstrapLease>>,
}

impl SchwabStreamerBootstrapPort for FakeBootstrap {
    fn load_fresh<'a>(
        &'a self,
        namespace: &'a AccountNamespace,
    ) -> PortFuture<'a, Result<SchwabStreamerBootstrapLease, SchwabStreamerBootstrapError>> {
        Box::pin(async move {
            assert_eq!(namespace, &self.namespace);
            self.lease
                .lock()
                .expect("fake bootstrap lease lock")
                .take()
                .ok_or(SchwabStreamerBootstrapError::Unavailable)
        })
    }
}

fn namespace() -> AccountNamespace {
    AccountNamespace::new(
        ExecutionBrokerId::Schwab,
        BrokerEnvironment::Paper,
        AccountScope::new("synthetic-account-scope").expect("synthetic scope is valid"),
    )
}

fn lease(socket_url: &str, expires_at: SystemTime) -> SchwabStreamerBootstrapLease {
    SchwabStreamerBootstrapLease::new(
        socket_url,
        "synthetic-customer",
        "synthetic-correlation",
        "synthetic-channel",
        "synthetic-function",
        Zeroizing::new("synthetic-access-token".to_owned()),
        expires_at,
    )
    .expect("synthetic bootstrap material is bounded")
}

fn gate(socket_url: &str, expires_at: SystemTime) -> SchwabStreamerGate<FakeBootstrap> {
    let namespace = namespace();
    let bootstrap = Arc::new(FakeBootstrap {
        namespace: namespace.clone(),
        lease: Mutex::new(Some(lease(socket_url, expires_at))),
    });
    SchwabStreamerGate::new(
        namespace,
        [TrustedWssEndpoint::new(ALLOWLISTED_URL).expect("synthetic exact endpoint is valid")],
        bootstrap,
    )
    .expect("explicit allowlist and source are required")
}

#[test]
fn endpoint_allowlist_requires_a_full_wss_url_without_userinfo_or_fragment() {
    for invalid in [
        "",
        "ws://streamer.example.invalid/path",
        "wss:///path",
        "wss://user@streamer.example.invalid/path",
        "wss://@streamer.example.invalid/path",
        "wss://streamer.example.invalid/path#fragment",
        "wss://streamer.example.invalid:0/path",
        "https://streamer.example.invalid/path",
    ] {
        assert_eq!(
            TrustedWssEndpoint::new(invalid),
            Err(SchwabStreamerGateError::InvalidAllowlist),
            "rejected endpoint candidate: {invalid}"
        );
    }
}

#[tokio::test]
async fn validation_returns_no_credential_object_and_drops_a_matching_lease() {
    let gate = gate(ALLOWLISTED_URL, SystemTime::now() + Duration::from_secs(30));
    let result: Result<(), SchwabStreamerGateError> = gate.validate_bootstrap().await;
    assert_eq!(result, Ok(()));
    assert!(matches!(
        gate.validate_bootstrap().await,
        Err(SchwabStreamerGateError::BootstrapUnavailable)
    ));
    assert!(!format!("{gate:?}").contains("synthetic"));
}

#[tokio::test]
async fn exact_url_allowlist_rejects_host_port_path_query_case_and_scheme_variants() {
    for candidate in [
        "wss://unexpected.example.invalid:443/provided/path?session=synthetic",
        "wss://streamer.example.invalid.attacker.test:443/provided/path?session=synthetic",
        "wss://streamer.example.invalid:8443/provided/path?session=synthetic",
        "wss://streamer.example.invalid:443/other/path?session=synthetic",
        "wss://streamer.example.invalid:443/provided/path?session=other",
        "wss://STREAMER.example.invalid:443/provided/path?session=synthetic",
        "wss://stréamer.example.invalid:443/provided/path?session=synthetic",
        "ws://streamer.example.invalid:443/provided/path?session=synthetic",
        "wss://user@streamer.example.invalid:443/provided/path?session=synthetic",
        "wss://@streamer.example.invalid:443/provided/path?session=synthetic",
        "wss://streamer.example.invalid:443/provided/path?session=synthetic#fragment",
    ] {
        let gate = gate(candidate, SystemTime::now() + Duration::from_secs(30));
        assert!(matches!(
            gate.validate_bootstrap().await,
            Err(SchwabStreamerGateError::EndpointNotAllowed)
        ));
    }
}

#[tokio::test]
async fn expired_lease_fails_closed_before_endpoint_acceptance() {
    let gate = gate(ALLOWLISTED_URL, SystemTime::now() - Duration::from_secs(1));
    assert!(matches!(
        gate.validate_bootstrap().await,
        Err(SchwabStreamerGateError::LeaseExpired)
    ));
}

#[test]
fn bootstrap_material_debug_is_redacted() {
    let material = lease(ALLOWLISTED_URL, SystemTime::now() + Duration::from_secs(30));
    let debug = format!("{material:?}");
    assert!(!debug.contains("synthetic-access-token"));
    assert!(!debug.contains("streamer.example.invalid"));
    assert!(!debug.contains("synthetic-customer"));
}
