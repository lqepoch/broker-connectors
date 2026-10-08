# Vendored Alpaca Rust SDK provenance

This directory contains a scoped copy of three crates from the community Rust
project [`wmzhai/alpaca-rust`](https://github.com/wmzhai/alpaca-rust):

- Upstream commit: `d91be382e3e9d25c52c24e626e78702532d24ba2`
- Upstream Git tree: `faba69ee9eaacdafff334699a03b4dd45223b004`
- Upstream version: `0.33.3`
- Upstream release: [`v0.33.3`](https://github.com/wmzhai/alpaca-rust/releases/tag/v0.33.3),
  which points to the pinned commit and was listed as the latest release when
  checked on `2026-10-08`.
- Upstream edition and MSRV: Rust 2024, Rust `1.99.0`
- Upstream workspace license: `MIT OR Apache-2.0`
- Selected packages: `alpaca-core`, `alpaca-data`, and `alpaca-rest-http`

Alpaca's [SDKs and Tools page](https://docs.alpaca.markets/us/docs/sdks-and-tools)
lists `alpaca-rust` in its “Community-Made SDKs” section, separately from
official SDKs. This is a community Rust SDK for Alpaca APIs, not an Alpaca
maintained or supported SDK. The pinned upstream README names Weiming Zhai as
the project maintainer. The upstream `LICENSE-MIT` and `LICENSE-APACHE` files
are retained without modification; their upstream blob IDs and local hashes
are recorded in `SOURCE-MANIFEST.json`.

Only the three data-read packages are copied. The vendor workspace deliberately
omits `alpaca-trade`, `alpaca-mock`, `alpaca-time`, `alpaca-option`, and
`alpaca-facade`. The workspace manifest also omits unrelated dependency entries
and adds the `zeroize` dependency used by the credential patch. The three
upstream live API integration tests (`stocks_real_api.rs`,
`crypto_real_api.rs`, and `corporate_actions_real_api.rs`) are omitted, as is
their `dotenvy` dev dependency. This prevents a routine test command from
loading local credentials or making provider calls.

`LOCAL-PATCHES.patch` is the reviewable file-level diff against the selected
upstream files. Its hash and the source/adapted file hashes are in
`SOURCE-MANIFEST.json`. The patches:

- Store long-lived SDK credentials in `Zeroizing<String>` values and redact
  credential-bearing `Debug` implementations.
- Keep credentials out of a retained `HeaderMap`; HTTP request construction
  still creates short-lived header and request copies that cannot be zeroized.
- Reject noncanonical market-data origins before constructing authenticated
  requests, require HTTPS, and disable redirects on the SDK client.
- Limit response bodies to 8 MiB using a declared-length precheck and an
  incremental chunk counter. Oversized data fails before full body buffering.
- Add a finite total retry-wait budget to the HTTP retry policy.
- Add synthetic unit coverage for credential redaction, origin rejection,
  response-size limits, and retry budgets.

The root `Cargo.lock` is the canonical workspace/runtime dependency lock. The
nested `vendor/alpaca-rust/Cargo.lock` exists only to make the standalone
vendored-source unit and localhost integration tests reproducible; those test
commands can resolve a different compatible transitive version than the root
workspace uses. Production consumers resolve the path packages through the
root lockfile.

The wrapper exposes only latest option quotes, latest option trades, and finite
option snapshots. It does not use `_all` convenience methods. Snapshot pagination
is capped at four pages, each page is limited to at most 1,000 points, and each
request has a fixed deadline. Historical option bars/trades and trusted
watermarks are reported unsupported because the [historical bars](https://docs.alpaca.markets/us/v1.4.2/reference/optionbars)
and [historical trades](https://docs.alpaca.markets/us/reference/optiontrades)
API references and pinned request models provide no explicit feed selector for
those history requests, and the reviewed REST responses provide no continuity
receipt. Requested `opra`/`indicative` values never upgrade source feed or
entitlement from `unknown`.

Validation is offline. The Rust unit tests use synthetic values. The sole
integration test starts a loopback server bound to localhost; it does not call
Alpaca. Real provider, OAuth, account, feed entitlement, and market-data checks
are `NOT RUN`.

Re-run the vendored tests from the workspace root with at most two build jobs:

```sh
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --manifest-path vendor/alpaca-rust/Cargo.toml -p alpaca-core -p alpaca-rest-http -p alpaca-data --lib --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --manifest-path vendor/alpaca-rust/Cargo.toml -p alpaca-rest-http --test client_retry --offline
```
