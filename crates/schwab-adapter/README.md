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

The Streamer gate only validates a fresh zeroizing candidate lease against an
explicit exact full-URL WSS allowlist and then drops the lease. It does not
return credentials or connect a socket. The public `schwab-streamer` library
exports its bounded decoder and subscription-state types; the extracted
credential, LOGIN, socket, and session runtime is compiled only for that crate's
own loopback unit tests. No production Streamer runtime, OAuth bootstrap,
`userPreference` source, `BrokerEventPort`, or market-event converter is
available. Numeric Schwab field IDs remain opaque until authoritative evidence
is available. The adapter integration tests exercise only public decoding and
local subscription-state transitions; they do not test a provider connection.

See [`docs/schwab-adapter.md`](../../docs/schwab-adapter.md) for source pin,
license, protocol evidence, blockers, and validation status.
