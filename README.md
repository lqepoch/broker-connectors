# broker-connectors

This public Rust workspace owns broker protocol adapters and provider-neutral read ports. It starts with the audited Alpaca options WebSocket protocol and a bounded market-data port. The provider stream emits validated contracts from the frozen [`trading-core`](https://github.com/lqepoch/trading-core) revision `0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8`.

The current slice is read-only. It does not read credentials from dotenv, connect to Alpaca during tests, call OAuth, qualify provider symbols as complete contracts, compute Greeks, or submit orders. `opra` and `indicative` are explicit options WebSocket feeds; this slice has no stock SIP or historical REST adapter. The market-data port exposes one bounded, ordered `MarketDataItem` lane, so controls and market records cannot be reordered by a downstream two-queue `select!`. Its sequence is adapter delivery order, not an upstream provider sequence or a promise of raw cross-channel wire order. A successful socket send is not a subscription acknowledgement, and local protocol tests do not establish account entitlement or provider availability.

The shared v1 subscription ACK is channel-agnostic and caps each request at 32 `(channel, symbol)` pairs. Quote/trade overlap counts twice. The Alpaca adapter compares the provider's quote and trade ACK sets independently against the full request before emitting that projection; an incomplete ACK is terminal. Alpaca's wire ACK carries no request ID, so the shared request/subscription IDs are local generation correlators. The adapter reports entitlement as `unknown`. Any quote coalescing, quote discard, or unknown provider message fails the canonical stream, since the source stream cannot claim a complete archive after those conditions. Larger subscriptions need a versioned bounded ACK contract, not chunking or partial success.

The ordered market-data lane can also carry the exact bytes of each received
MessagePack application frame and link normalizable quote/trade events to that
frame by SHA-256, generation, frame sequence, and 1-based event ordinal/count.
Capture excludes outbound authentication and subscription frames. A frame is
limited to 1 MiB; outstanding frame leases are limited to 16 MiB and 1,024
records process-wide. Exceeding a bound terminates that generation with a fixed
failure. Unknown, malformed, provider-error, or dropped/coalesced input remains
diagnostic raw evidence and cannot qualify a complete event archive. This
workspace transports raw frames in memory only; durable Parquet storage and
research qualification belong to the separately versioned market-data
pipeline, not this adapter.

The initial implementation reuses the audited source `schwab_auto_bot@c907d18bc31790ede4cf36a4312a6813467506f0` for `alpaca-stream` protocol/session mechanics, with source-level provenance in `SOURCE-MANIFEST.json`. New public package files use the project-authorized `MIT OR Apache-2.0` license; upstream dependency license terms remain separate and are recorded in the manifest/SBOM.

## Local checks

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --workspace --locked
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --workspace --all-targets --locked -- -D warnings
```

These checks use synthetic inputs. Real Alpaca entitlement, OPRA service behavior, SIP service behavior (not implemented here), remote network operation, durable frame storage, and native Windows/macOS runtime remain `NOT RUN` in this repository task.
