# Schwab read adapter

This crate adapts the extracted read-only Schwab SDK to `broker-ports` without
exposing SDK DTOs to consumers. It implements one account-summary GET and one
bounded, single-page position read. Exact decimal tokens are parsed into the
shared `domain::ExactDecimal`; absent fields stay absent, raw account numbers
are discarded, and Debug output omits account and balance values.

Every REST operation requires an injected `Arc<SchwabReadAdmissionOwner>`.
That bridge validates the request's account namespace and evidence, acquires
one permit from the application's existing budget owner, holds it through the
request, and sends bounded 429 metadata back to the same owner. The adapter
owns no scheduler, quota, or fallback permit. A local `AccountScope` is only a
partition label; `SchwabAccountBinding` requires a separately supplied opaque
broker hash and does not authorize reads by itself.

Schwab's account response has no continuation cursor in this source contract.
Position reads reject a caller cursor, reject a response larger than the
requested page bound, and never truncate. Open-order and fill reads return
`Unsupported`: the source query types do not establish a complete bounded
cursor contract. This crate does not convert REST quotes into stream events.

The Streamer gate requires an injected bootstrap source, a nonempty exact
lowercase DNS-host/port allowlist, a fresh zeroizing token lease, and a WSS URL
matching that allowlist. This crate does not implement OAuth, token storage, or
the `userPreference` bootstrap source. No `BrokerEventPort` or market-event
converter is enabled. Numeric Schwab field IDs remain opaque until an
authoritative mapping is available. The protocol integration test supplies a
fake socket to the extracted `schwab-streamer` runtime and checks ACK,
generation, sparse-field revision, and raw field preservation without assigning
field meanings.

See [`docs/schwab-adapter.md`](../../docs/schwab-adapter.md) for source pin,
license, protocol evidence, blockers, and validation status.
