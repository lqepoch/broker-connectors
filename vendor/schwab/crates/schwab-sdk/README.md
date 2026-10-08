# `schwab-sdk` read-only REST facade

`schwab-sdk` is the public read-only facade over `schwab-rest`. It groups the
existing typed Trader and Market Data GET operations under `SchwabSdk::trader()`
and `SchwabSdk::market_data()`. Request construction, transport, response
parsing, decimal normalization, metadata, and error classification remain
owned by `schwab-rest`.

SDK01 completes only a partial dependency-boundary slice; broader SDK productization remains PARTIAL. SDK02 will decide whether stable account and wire adapter APIs should be exposed by this read-only facade.

## Construction and safety boundary

Construction requires an `AccessTokenProvider`, an `HttpTransport`, and an
explicit SDK-owned admission port:

~~~rust,ignore
let transport = SchwabHttpsTransport::new()?;
let admission: Arc<dyn ReadAdmissionPort> = shared_admission_port;
let sdk = SchwabSdk::builder(token_provider, transport, admission).build();
~~~

`ReadAdmissionPort` accepts a bounded urgency hint, maximum wait, and bounded
429 metadata. It does not set local limits or assert a Schwab quota. Existing
project callers can use `broker_schwab::RequestBudgetReadAdmission` to adapt the
project's `request-budget` policy. Share one port implementation when clients
must use the same local budget. A caller that only needs ordering can provide a
separate policy implementation. The SDK does not retry requests automatically.

`SchwabHttpsTransport` is re-exported from this crate. It uses the fixed Schwab
REST origin, emits only GET requests, disables redirects, and does not retry.
The `AccessTokenProvider` remains injected; this crate contains no OAuth
exchange, refresh lifecycle, credential persistence, or production provider
bridge. The facade has no raw URL/method/body entry point and no POST/PUT/DELETE,
preview, place, replace, or cancel capability. It is not wired into the
production runtime and does not establish account authority, quote freshness,
entitlement, or tradeability.

## Streamer is out of scope

This crate does not include or connect the Streamer. The extracted public
`schwab-streamer` API contains a bounded frame decoder, opaque wire values,
service manifests, and subscription acknowledgement state. Credential,
LOGIN-serialization, socket, factory, and authenticated-session modules are
compiled only for `schwab-streamer`'s own unit tests and are not a downstream
runtime API. Its synthetic socket tests do not establish provider connectivity.

A URL returned by REST preferences does not establish endpoint authority. This
extraction has no official endpoint allowlist, production bootstrap, or
provider field dictionary; production Streamer connectivity remains blocked.

## Project adapters

Domain-to-wire order conversion, account-hash mapping, and project read-budget
policy live in `broker-schwab`.
The adapter currently consumes `schwab-rest` and `schwab-contracts` directly
because this facade exposes read-only Trader/Market Data operations and does
not expose order-wire constructors or the account-hash source bridge. Deciding
whether those stable adapter APIs should be surfaced by `schwab-sdk` is a
follow-up SDK02 boundary; it must not introduce mutation transport.

## Migration status

Issue [#172](https://github.com/lqepoch/schwab_auto_bot/issues/172) remains
**PARTIAL**. This facade groups existing Rust typed GETs, but
does not complete OAuth/provider integration, all Node/Rust contract parity
and error acceptance, production runtime wiring, or provider verification.
Tests use injected fake providers and synthetic responses; they make no
network request.
