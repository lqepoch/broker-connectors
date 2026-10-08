# broker-connectors

This public Rust workspace owns broker protocol adapters and provider-neutral ports. It starts with the audited Alpaca options WebSocket protocol and bounded market-data contracts. Shared domain types come from the frozen [`trading-core`](https://github.com/lqepoch/trading-core) revision `0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8`.

`broker-ports` now defines four read-only boundaries: market data, instrument catalog, account reads, and account events. The existing Alpaca options stream is the only concrete port adapter in this slice; the catalog, account-read, and account-event contracts do not yet have provider implementations. Catalog, account-read, and event-subscription requests carry typed admission namespace and policy provenance, but those values are not themselves an authorization grant: the composition root must supply them from the single trusted read-budget owner. Account rows remain adapter-associated types and must be mapped into the consuming engine's one account-state authority.

No broker writer or provider execution adapter is present. The separate `broker-execution` crate defines a provider-neutral software port for typed submit/replace/cancel requests and Accepted/DefinitelyNotSent/Rejected/Unknown outcomes. Replace acceptance requires a typed predecessor-to-replacement identity link: the provider may retain the known identity or return a new identity, but a new identity is accepted only when its reported predecessor exactly matches the known order in the same account namespace. Missing or mismatched linkage becomes `Unknown`; cancel acceptance still requires the same provider identity. Its opt-in `offline-fake` feature is synthetic-only and bounded by both queue counts and a 1 MiB aggregate accounted-data budget. Downstream code can consume `Accepted` results but cannot synthesize them; the crate-private acceptance builder means an external `ExecutionPort` implementer cannot currently produce `Accepted` until a reviewed in-crate adapter or validated factory is added. This contract does not grant execution authority or make Paper/Live available: the consuming engine retains admission, risk, funds, account authority, durable intent/outbox, and reconciliation; Live requests are rejected and Unknown outcomes require reconciliation without automatic retry. Tests never read credentials, connect to Alpaca, call OAuth, qualify provider symbols as complete contracts, compute Greeks, or submit orders. `opra` and `indicative` are explicit options WebSocket feeds; this slice has no stock SIP or historical REST adapter. The market-data port exposes one bounded, ordered `MarketDataItem` lane, so controls and market records cannot be reordered by a downstream two-queue `select!`. Its sequence is adapter delivery order, not an upstream provider sequence or a promise of raw cross-channel wire order. A successful socket send is not a subscription acknowledgement, and local protocol tests do not establish account entitlement or provider availability.

`broker-execution` is intentionally separate from `broker-ports`. A minimal engine resolver probe rejected the combined dependency graph because `market-contracts` at core revision `0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8` requires `serde_json=1.0.151` while the pinned Schwab persistence source requires `serde_json=1.0.149`; the exact command and failure are recorded in [`DEPENDENCY-RESOLUTION.md`](DEPENDENCY-RESOLUTION.md). The new crate depends only on core `domain`; `scripts/check_broker_execution_dependency_firewall.py` verifies its full normal dependency graph, including all features, remains `broker-execution`, `domain`, and `exact-decimal`.

The pinned upstream/community [`alpaca-rust` v0.33.3](https://github.com/wmzhai/alpaca-rust/tree/d91be382e3e9d25c52c24e626e78702532d24ba2) candidate is not an Alpaca-maintained or Alpaca-supported official SDK, and it is not a production REST client in this workspace. Alpaca lists it under [Community-Made SDKs](https://docs.alpaca.markets/us/docs/sdks-and-tools); the upstream README names individual maintainer Weiming Zhai. At that exact revision, [`alpaca-rest-http` buffers response text without a byte cap](https://github.com/wmzhai/alpaca-rust/blob/d91be382e3e9d25c52c24e626e78702532d24ba2/crates/alpaca-http/src/client.rs), and the high-level data client has no custom zeroizing authenticator path. The workspace metadata declares `MIT OR Apache-2.0`; the upstream [`MIT`](https://github.com/wmzhai/alpaca-rust/blob/d91be382e3e9d25c52c24e626e78702532d24ba2/LICENSE-MIT) and [`Apache-2.0`](https://github.com/wmzhai/alpaca-rust/blob/d91be382e3e9d25c52c24e626e78702532d24ba2/LICENSE-APACHE) files were checked, although the GitHub repository API reports `NOASSERTION`. No Alpaca REST SDK dependency or credential-bearing client is added. A later integration needs a narrowly reviewed, pinned upstream patch with bounded response-body reads, redirects disabled, and zeroization for long-lived credential storage; transient transport copies must still be described separately.

This workspace has no IBKR SDK integration. The evaluated pinned Rust client [`wboayue/rust-ibapi`](https://github.com/wboayue/rust-ibapi/tree/3e73f2f1cfac151c10e403a3e7d779272134445f) identifies itself as an unofficial community client; the [official TWS API documentation](https://www.interactivebrokers.com/docs/tws-api/doc/orders/modifying-orders) is the protocol reference, not evidence that IBKR maintains or supports that Rust SDK.

The shared v1 subscription ACK is channel-agnostic and caps each request at 32 `(channel, symbol)` pairs. Quote/trade overlap counts twice. The Alpaca adapter compares the provider's quote and trade ACK sets independently against the full request before emitting that projection; an incomplete ACK is terminal. Alpaca's wire ACK carries no request ID, so the shared request/subscription IDs are local generation correlators. The adapter reports entitlement as `unknown`. Any quote coalescing, quote discard, or unknown provider message fails the canonical stream, since the source stream cannot claim a complete archive after those conditions. Larger subscriptions need a versioned bounded ACK contract, not chunking or partial success.

The ordered market-data lane can also carry the exact bytes of each received
MessagePack application frame and link normalizable quote/trade events to that
frame by SHA-256, generation, frame sequence, and 1-based event ordinal/count.
Capture excludes outbound authentication and subscription frames. A frame is
limited to 1 MiB; outstanding frame leases are limited to 16 MiB and 1,024
records process-wide. Exceeding a bound terminates that generation with a fixed
failure. Unknown/provider-error frames that reach the lane retain diagnostic raw
bytes and cannot qualify a complete event archive. In active market-data and
subscription-handshake capture modes, a frame that fails decoding is also
published as a `DecodeFailure` raw record. Capture currently decodes and
analyzes the frame before publishing that in-memory record; it has no awaited
durable capture sink or persistence acknowledgement. Durable Parquet storage and
research qualification belong to the separately versioned market-data
pipeline, not this adapter.

The initial implementation reuses the audited source `schwab_auto_bot@c907d18bc31790ede4cf36a4312a6813467506f0` for `alpaca-stream` protocol/session mechanics, with source-level provenance in `SOURCE-MANIFEST.json`. New public package files use the project-authorized `MIT OR Apache-2.0` license; upstream dependency license terms remain separate and are recorded in the manifest/SBOM.

## Local checks

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.98.1 test -p broker-execution --locked --offline
CARGO_BUILD_JOBS=2 cargo +1.98.1 test -p broker-execution --features offline-fake --locked --offline
python3 scripts/check_broker_execution_dependency_firewall.py
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --workspace --locked
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --workspace --all-targets --locked -- -D warnings
```

These checks use synthetic inputs. Real Alpaca entitlement, OPRA service behavior, SIP service behavior (not implemented here), remote network operation, durable frame storage, and native Windows/macOS runtime remain `NOT RUN` in this repository task.
