# broker-connectors

This public Rust workspace owns broker protocol adapters and provider-neutral ports. It starts with the audited Alpaca options WebSocket protocol and bounded market-data contracts. Shared domain types come from the frozen [`trading-core`](https://github.com/lqepoch/trading-core) revision `0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8`.

`broker-ports` now defines four read-only boundaries: market data, instrument catalog, account reads, and account events. The Alpaca options stream is the only implementation of those generic ports in this slice. IBKR has a provider-specific exact option-catalog wrapper, but it does not implement the generic catalog port because that port's query cannot carry the full OCC identity and explicit exchange/currency needed for a safe request. The account-read and account-event contracts do not yet have provider implementations. Catalog, account-read, and event-subscription requests carry typed admission namespace and policy provenance, but those values are not themselves an authorization grant: the composition root must supply them from the single trusted read-budget owner. Account rows remain adapter-associated types and must be mapped into the consuming engine's one account-state authority.

No broker writer or provider execution adapter is present. The separate `broker-execution` crate defines a provider-neutral software port for typed submit/replace/cancel requests and Accepted/DefinitelyNotSent/Rejected/Unknown outcomes. Replace acceptance requires a typed predecessor-to-replacement identity link: the provider may retain the known identity or return a new identity, but a new identity is accepted only when its reported predecessor exactly matches the known order in the same account namespace. Missing or mismatched linkage becomes `Unknown`; cancel acceptance still requires the same provider identity. Its opt-in `offline-fake` feature is synthetic-only and bounded by both queue counts and a 1 MiB aggregate accounted-data budget. Downstream code can consume `Accepted` results but cannot synthesize them; the crate-private acceptance builder means an external `ExecutionPort` implementer cannot currently produce `Accepted` until a reviewed in-crate adapter or validated factory is added. This contract does not grant execution authority or make Paper/Live available: the consuming engine retains admission, risk, funds, account authority, durable intent/outbox, and reconciliation; Live requests are rejected and Unknown outcomes require reconciliation without automatic retry. Tests never read credentials, connect to Alpaca, call OAuth, qualify provider symbols as complete contracts, compute Greeks, or submit orders. `opra` and `indicative` are explicit options WebSocket feeds; this slice has no stock SIP or historical REST adapter. The market-data port exposes one bounded, ordered `MarketDataItem` lane, so controls and market records cannot be reordered by a downstream two-queue `select!`. Its sequence is adapter delivery order, not an upstream provider sequence or a promise of raw cross-channel wire order. A successful socket send is not a subscription acknowledgement, and local protocol tests do not establish account entitlement or provider availability.

`broker-execution` is intentionally separate from `broker-ports`. A minimal engine resolver probe rejected the combined dependency graph because `market-contracts` at core revision `0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8` requires `serde_json=1.0.151` while the pinned Schwab persistence source requires `serde_json=1.0.149`; the exact command and failure are recorded in [`DEPENDENCY-RESOLUTION.md`](DEPENDENCY-RESOLUTION.md). The new crate depends only on core `domain`; `scripts/check_broker_execution_dependency_firewall.py` verifies its full normal dependency graph, including all features, remains `broker-execution`, `domain`, and `exact-decimal`.

The pinned community [`alpaca-rust` v0.33.3](https://github.com/wmzhai/alpaca-rust/tree/d91be382e3e9d25c52c24e626e78702532d24ba2) is not maintained or supported by Alpaca. Alpaca lists it under [Community-Made SDKs](https://docs.alpaca.markets/us/docs/sdks-and-tools); the pinned upstream README names Weiming Zhai as maintainer. The workspace vendors only `alpaca-core`, `alpaca-data`, and `alpaca-rest-http` under `vendor/alpaca-rust/`, retaining the upstream `MIT OR Apache-2.0` license files. The exact source tree, package paths, removed live API tests, security patches, and patch hash are recorded in [`vendor/alpaca-rust/UPSTREAM.md`](vendor/alpaca-rust/UPSTREAM.md) and `SOURCE-MANIFEST.json`.

The [`alpaca-rest-read`](crates/alpaca-rest-read/README.md) crate is a narrow, read-only adapter over the pinned community SDK. It uses shared `market-contracts` quote/trade payloads; only quote, trade, and snapshot reads are exposed. Credentials are explicitly injected and owned in zeroizing buffers, there is no environment-variable lookup, the HTTPS origin is fixed, redirects are disabled, response bodies are capped at 8 MiB, and request/page/retry budgets are finite. HTTP request construction necessarily creates transient header copies that this patch cannot zeroize. The adapter keeps requested `opra`/`indicative` separate from effective source and entitlement, which remain `unknown`. Historical option bars/trades and trusted watermarks remain unsupported. No Alpaca provider request or OAuth flow was run.

The [`ibkr-read`](crates/ibkr-read/README.md) crate wraps the pinned source snapshot of [`wboayue/rust-ibapi`](https://github.com/wboayue/rust-ibapi/tree/3e73f2f1cfac151c10e403a3e7d779272134445f) for one exact, read-only contract-details query. The Rust client identifies itself as an unofficial community SDK; it is not an IBKR-maintained or supported SDK. Its adapter accepts only loopback `SocketAddr`s, keeps the SDK client private, limits each network phase to 60 seconds, requires a complete OCC symbol and explicit non-`SMART` exchange/currency, and returns the shared option candidate with missing deliverable/exercise/settlement fields left `Unknown`. A caller-aborted lookup poisons the session and releases the SDK owner because the SDK's subscription `Drop` only schedules cancel and does not confirm native END. It does not implement the generic catalog port, account reads/events, market-data subscriptions, historical reads, or any execution port. Connection and error diagnostics report fixed classifications, message IDs and byte counts without raw payloads. Production connections keep both `MessageRecorder` and `RawFrameTap` disabled, and neither `IBAPI_RECORDING_DIR` nor `IBAPI_RAW_CAPTURE_DIR` can enable persistence; explicit temporary-directory capture is test-only and uses synthetic fixtures. The official [TWS API contract-details documentation](https://www.interactivebrokers.com/docs/tws-api/sync-doc/contract-details) is the protocol reference. The pinned source commit is not the `v5.0.0` tag commit; the immutable source tree, archive digest, MIT license and complete local source-change record are recorded in [`vendor/ibapi/UPSTREAM.md`](vendor/ibapi/UPSTREAM.md) and `SOURCE-MANIFEST.json`.

The shared v1 subscription ACK is channel-agnostic and caps each request at 32 `(channel, symbol)` pairs. Quote/trade overlap counts twice. The Alpaca adapter compares the provider's quote and trade ACK sets independently against the full request before emitting that projection; an incomplete ACK is terminal. Alpaca's wire ACK carries no request ID, so the shared request/subscription IDs are local generation correlators. The adapter reports entitlement as `unknown`. Any quote coalescing, quote discard, or unknown provider message fails the canonical stream, since the source stream cannot claim a complete archive after those conditions. Larger subscriptions need a versioned bounded ACK contract, not chunking or partial success.

The ordered market-data lane can also carry the exact bytes of each received
MessagePack application frame and link normalizable quote/trade events to that
frame by SHA-256, canonical generation, frame sequence, and 1-based event ordinal/count.
Capture excludes outbound authentication/subscription frames and starts only
after authentication, for inbound subscription ACK and market application
frames. A frame is limited to 1 MiB; outstanding frame leases are limited to 16
MiB and 1,024 records process-wide. Exceeding a bound terminates that generation
with a fixed failure. Unknown/provider-error frames retain diagnostic bytes and
cannot qualify a complete event archive. They may retain parsed market-event
counts and symbols, but the whole frame is quarantined and none of those events
are published. Decode failures retain the exact bytes and are finalized with
zero events, no symbols, and no numeric encoding. Mixed numeric encodings remain
valid with no homogeneous encoding value. When a trusted
`RawFrameSink` is injected, the runner awaits a matching pre-decode ACK, decodes,
then awaits a matching post-decode finalization ACK before publishing the raw
frame or normalized events. Both ACKs bind one `RawFrameCaptureKey` containing
the capture UUID, source-local generation, sequence, and exact frame SHA-256;
finalization also binds a canonical bounded summary hash. The `generation` on a
published raw frame/event reference is the canonical port generation; its
`capture_key` preserves the separate source-local lineage used by the MDP spool.
This distinction keeps reconnect frames unique when sequence numbers and bytes
repeat. Sink failure, timeout, cancellation, or ACK mismatch ends the
generation without retry, decode, or event publication past that frame. ACK
constructors only express the sink implementation's promise and do not prove
`fsync`, entitlement, completeness, or Drive publication. `AlpacaOptionsMarketDataPort`
uses `with_raw_frame_sink_factory` to request a distinct sink for each logical
subscription. The trusted factory owns one UUIDv4 per subscription, stable across
reconnect generations and replaced after restart. A crash between ACK phases
leaves an unfinalized capture for the sink owner to quarantine. Without an
injected sink, sessions remain explicitly in-memory diagnostic mode. This
workspace does not yet implement the production MDP spool; durable Parquet
storage and research qualification remain MDP responsibilities.

The initial implementation reuses the audited source `schwab_auto_bot@c907d18bc31790ede4cf36a4312a6813467506f0` for `alpaca-stream` protocol/session mechanics, with source-level provenance in `SOURCE-MANIFEST.json`. New public package files use the project-authorized `MIT OR Apache-2.0` license; upstream dependency license terms remain separate and are recorded in the manifest/SBOM.

## Local checks

```sh
CARGO_BUILD_JOBS=2 cargo +1.99.0 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --locked --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --features offline-fake --locked --offline
python3 scripts/check_broker_execution_dependency_firewall.py
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p alpaca-rest-read --locked --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy -p alpaca-rest-read --all-targets --locked --offline -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p ibkr-read --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy -p ibkr-read --all-targets --locked -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --workspace --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy --workspace --all-targets --locked -- -D warnings
python3 scripts/generate_spdx_sbom.py
python3 scripts/update_source_manifest_hashes.py
python3 scripts/check_vendor_provenance.py
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --manifest-path vendor/alpaca-rust/Cargo.toml -p alpaca-core -p alpaca-rest-http -p alpaca-data --lib --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --manifest-path vendor/alpaca-rust/Cargo.toml -p alpaca-rest-http --test client_retry --offline
```

The workspace toolchain is pinned to Rust 1.99.0 because the selected Alpaca SDK requires that compiler; shared workspace crates such as `broker-execution` and `ibkr-read` retain MSRV 1.98.1. Consumers of `alpaca-rest-read` need Rust 1.99.0. These checks use synthetic inputs or localhost-only fake transport. Real Alpaca entitlement, OPRA service behavior, SIP service behavior (not implemented here), real IBKR TWS/Gateway access, account operations, order operations, provider network operation, durable frame storage, and native Windows/macOS runtime remain `NOT RUN` in this repository task. The loopback IBKR test endpoint verifies the wrapper protocol path only; it does not validate real-provider readiness or remote gateway deployments.
