# Contributing

Use the pinned Rust toolchain from `rust-toolchain.toml` and keep dependencies
exactly pinned in Cargo.toml and Cargo.lock. Rust 1.99.0 is required to build
the workspace because `alpaca-rest-read` consumes the selected SDK; shared
packages such as `broker-execution` retain MSRV 1.98.1. Tests must use synthetic
provider frames and fake transports. Do not add credentials, broker/OAuth calls, account
state, order-send paths, private trading traces, or non-public market data.

For Rust changes, run:

```sh
CARGO_BUILD_JOBS=2 cargo +1.99.0 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --locked --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --features offline-fake --locked --offline
python3 scripts/check_broker_execution_dependency_firewall.py
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p alpaca-rest-read --locked --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy -p alpaca-rest-read --all-targets --locked --offline -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --workspace --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy --workspace --all-targets --locked -- -D warnings
python3 scripts/generate_spdx_sbom.py
python3 scripts/update_source_manifest_hashes.py
python3 scripts/check_vendor_provenance.py
```

Update `SOURCE-MANIFEST.json` when an extracted source file or its adapted form
changes. Keep provider entitlement, completeness, source exactness, and local
test evidence distinct in code and documentation. A socket write or connection
state does not confirm a subscription.

Keep Clippy's conversion and safety diagnostics enabled. A temporary allowance
for inherited documentation/style warnings must be attached only to the
affected item or module and documented in `AGENTS.md`; do not add a workspace-
wide pedantic suppression. Raw market frames stay in the ordered bounded lane,
exclude outbound auth data, and retain exact bytes plus event correlation. They
are not yet persisted or research-qualified by this workspace.

The optional `broker-ports::RawFrameSink` contract is not a production storage
implementation. Pre-decode capture ACKs bind the capture UUID, source generation,
frame sequence, and exact byte hash. Post-decode finalization ACKs additionally
bind a canonical bounded summary hash. A sink must await local durability of both
the exact frame plus recovery metadata and the derived summary; it must not ACK
ambiguous writes. The Alpaca adapter must not decode a frame until the pre-decode
ACK matches, and must not publish correlated events until finalization matches.
Sink failures, cancellation, saturation, or ACK mismatches poison the generation
and stop it without retrying past the missing frame. Synthetic sink tests are
contract tests only; they do not establish filesystem durability.

`broker-execution` is a software contract only. Keep `offline-fake` opt-in and
synthetic, preserve explicit bounds on both scripted outcomes and recorded
commands, and do not treat a Paper route as permission or `Unknown` as retryable.
The dependency firewall is expected to list only `broker-execution`, core
`domain`, and `exact-decimal` for all normal dependencies and enabled features.

`alpaca-rest-read` uses the pinned, vendored community SDK documented in
`vendor/alpaca-rust/UPSTREAM.md`. Keep provider calls out of tests. The wrapper
must use explicit credential injection, the fixed HTTPS origin, no redirects,
the 8 MiB streamed-body limit, finite page/deadline/retry budgets, and shared
market-contract event types. Requested feed values are not evidence of effective
feed or entitlement. Historical option bars/trades and trusted watermarks remain
unsupported until a reviewed implementation carries explicit feed evidence and
satisfies the bounded-read contract.
