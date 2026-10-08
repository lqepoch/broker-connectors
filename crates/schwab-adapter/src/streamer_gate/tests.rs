use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use broker_ports::{AccountNamespace, BrokerEnvironment, ExecutionBrokerId, PortFuture};
use domain::AccountScope;
use zeroize::Zeroizing;

use super::{
    SchwabStreamerBootstrapError, SchwabStreamerBootstrapLease, SchwabStreamerBootstrapPort,
    SchwabStreamerGate, SchwabStreamerGateError, TrustedWssEndpoint,
};

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
        [TrustedWssEndpoint::new("streamer.example.invalid", 443)
            .expect("synthetic exact endpoint is valid")],
        bootstrap,
    )
    .expect("explicit allowlist and source are required")
}

#[test]
fn endpoint_allowlist_rejects_empty_wildcard_ip_and_noncanonical_hosts() {
    assert_eq!(
        TrustedWssEndpoint::new("*.example.invalid", 443),
        Err(SchwabStreamerGateError::InvalidAllowlist)
    );
    assert_eq!(
        TrustedWssEndpoint::new("127.0.0.1", 443),
        Err(SchwabStreamerGateError::InvalidAllowlist)
    );
    assert_eq!(
        TrustedWssEndpoint::new("Streamer.example.invalid", 443),
        Err(SchwabStreamerGateError::InvalidAllowlist)
    );
}

#[tokio::test]
async fn source_sdk_credential_provider_requires_exact_wss_host_and_live_lease() {
    use schwab_streamer::StreamerCredentialProvider;

    let mut gate = gate(
        "wss://streamer.example.invalid:443/private?synthetic=opaque",
        SystemTime::now() + Duration::from_secs(30),
    );
    let credentials = gate
        .load_session_credentials()
        .await
        .expect("fake credential source and explicit endpoint gate accept the synthetic lease");
    assert!(!format!("{:?}", gate).contains("synthetic"));
    drop(credentials);
}

#[tokio::test]
async fn wrong_host_scheme_port_and_expired_lease_fail_closed() {
    let wrong_host = gate(
        "wss://unexpected.example.invalid:443/private?synthetic=opaque",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        wrong_host.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let wrong_scheme = gate(
        "ws://streamer.example.invalid:443/private",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        wrong_scheme.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let wrong_port = gate(
        "wss://streamer.example.invalid:8443/private",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        wrong_port.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let userinfo = gate(
        "wss://user@streamer.example.invalid:443/private",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        userinfo.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let empty_userinfo = gate(
        "wss://@streamer.example.invalid:443/private",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        empty_userinfo.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let fragment = gate(
        "wss://streamer.example.invalid:443/private#unexpected",
        SystemTime::now() + Duration::from_secs(30),
    );
    assert!(matches!(
        fragment.load_credentials().await,
        Err(SchwabStreamerGateError::EndpointNotAllowed)
    ));

    let expired = gate(
        "wss://streamer.example.invalid/private",
        SystemTime::now() - Duration::from_secs(1),
    );
    assert!(matches!(
        expired.load_credentials().await,
        Err(SchwabStreamerGateError::LeaseExpired)
    ));
}

#[test]
fn bootstrap_material_debug_is_redacted() {
    let material = lease(
        "wss://streamer.example.invalid/private?synthetic=opaque",
        SystemTime::now() + Duration::from_secs(30),
    );
    let debug = format!("{material:?}");
    assert!(!debug.contains("synthetic-access-token"));
    assert!(!debug.contains("streamer.example.invalid"));
    assert!(!debug.contains("synthetic-customer"));
}
