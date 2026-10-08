# Schwab SDK extraction and adapter boundary

## Source and scope

The adapter reuses project-owned Rust source from the immutable
`schwab_auto_bot@c907d18bc31790ede4cf36a4312a6813467506f0` pin. The selected
packages are `schwab-sdk`, `schwab-rest`, and `schwab-streamer`, plus five
reviewed synthetic fixtures. The exact source tree is recorded in
[`vendor/schwab/SOURCE-MANIFEST.json`](../vendor/schwab/SOURCE-MANIFEST.json),
which includes each source Git blob, source SHA-256, target path, and target
SHA-256. The extraction has 66 files. It excludes `broker-schwab`, account
configuration, credentials, private account state, real market data, and
provider traces.

These Rust packages are project code, not an upstream third-party SDK. The
source repository has no root license declaration for these packages. The
project owner explicitly authorized this selected public extraction under the
target project's `MIT OR Apache-2.0` license; this does not change the private
source repository's license. The existing target `LICENSE-MIT` and
`LICENSE-APACHE` files govern the extracted target copy. The target does not
claim to grant Schwab API access, account authority, or market-data rights.

The source packages retain their fixed HTTPS GET routes, injected token and
transport ports, bounded response parser, no-auto-retry behavior, and source
SDK error categories. Schwab Streamer's public target library exposes only the
bounded decoder, opaque protocol values, fixed service manifests, and local
subscription-state types. Authentication, credential, socket, and session
runtime modules compile only in the `schwab-streamer` crate's own unit-test
build. The three Cargo
packages inherit the target's authorized `MIT OR Apache-2.0` metadata. The
workspace pins `serde_json=1.0.151` with `arbitrary_precision`; `schwab-streamer`
pins `sha2=0.11.0` directly while the existing Alpaca stream keeps its
`sha2=0.10.9` workspace pin. `schwab-rest` retains exact `reqwest=0.13.5`
with rustls and localhost-only TLS test dependencies `rcgen=0.14.10` and
`tokio-rustls=0.26.5`. All target adaptations and source hashes are recorded
per file in both manifests.

## Capability report

| Capability | Status in this workspace | Evidence and boundary |
| --- | --- | --- |
| Account summary read | Available as a bounded read-only adapter call | Uses the pinned SDK's account `GET`, an explicit opaque broker hash, and the injected shared read-budget owner; account numbers are discarded. |
| Positions read | Available as one bounded page | Rejects caller cursors and oversized responses; it does not invent continuation or completeness evidence. |
| Open orders and fills | Unsupported | The selected source query contract has no cursor/completeness proof, so the adapter returns `Unsupported` without sending a request. |
| Schwab Streamer protocol | Public decoder and state types only | Adapter tests decode synthetic opaque fields and check local subscription-state transitions. No public socket or authenticated runtime is exported. |
| Stream-to-market-event conversion | Blocked | Numeric field IDs remain opaque; no quote/Greek mapping, SIP/OPRA label, or timestamp unit is inferred. |
| Stream bootstrap validation | Blocked without a provider | An injected source and an exact full-URL WSS allowlist can validate and immediately drop a candidate zeroizing lease. This gate does not return credentials or connect a socket. No production bootstrap owner or official endpoint allowlist is present. |
| Stream provider connection and LOGIN | Unavailable in the public library | Native credential, LOGIN, socket, and session modules are test-only; no production code path can construct them through the SDK API. |
| Order placement, replace, cancel, or OAuth | Not implemented | No write facade, OAuth flow, or broker call is included in this extraction. |
| Generic account-event port | No provider implementation | The shared contract exists, but this adapter does not publish account events. |

These statuses describe local adapter capabilities, not Schwab provider
readiness. Synthetic tests do not establish remote compatibility, account
authority, entitlement, or live safety.

## REST read behavior

`SchwabReadAdapter` implements `BrokerReadPort::read_account` and
`read_positions` by calling `SchwabSdk::trader().account(...)`. Both calls use
the source SDK's fixed account GET route and the explicit opaque hash carried
by `SchwabAccountBinding`; the adapter never derives a broker hash from a local
`AccountScope`. The summary projection omits raw account numbers and stores
balance and position numbers as shared `ExactDecimal` values. It makes no
account-state authority claim.

Admission evidence is not a permit. The adapter constructor requires a shared
`Arc<SchwabReadAdmissionOwner>`; that owner validates evidence and namespace,
applies the existing application budget, and returns a permit held until the
REST call ends. It receives 429 `Retry-After` values through the same bridge.
The crate creates no local limiter, default permit, or second budget.

The source order and transaction APIs do not provide a cursor/completeness
proof matching `BrokerReadPort` page semantics. `read_open_orders` and
`read_fills` therefore return `Unsupported` without acquiring a permit or
sending a request. Positions have no broker cursor either: a cursor request is
unsupported, and a response beyond the caller's page bound fails closed instead
of truncating.

## Streamer evidence and blockers

The vendored source code declares fixed field lists for
`LEVELONE_EQUITIES` (`0,45,46,51,52`) and `LEVELONE_OPTIONS` (`0,2,3,38`), but
the pinned source documentation does not assign public meanings to those
numeric fields. Searches of the publicly visible Schwab Developer Portal for
these service names, field IDs, and bid/ask labels returned no field dictionary;
the accessible [official user guide](https://developer.schwab.com/user-guides/get-started/authenticate-with-oauth)
covers OAuth rather than the Streamer field schema. This is insufficient
evidence to map IDs to price, size, or Greeks. No numeric field is translated
into a market quote, no event is labeled SIP/OPRA, and no provider timestamp
unit is inferred.

`SchwabStreamerGate` requires an application-supplied fresh
`userPreference`/access-token-lease source and an explicit allowlist of exact
full WSS URL strings. The URL's scheme, host, user information, and fragment
are validated; path and query are compared only as part of the complete exact
URL, without assuming a fixed provider path or host. The gate rejects expired
leases and drops accepted candidates without returning a credential object.
There is no production bootstrap implementation, official endpoint allowlist,
or public authenticated runtime in this repository. Provider streaming remains
**BLOCKED**. No real provider connection, OAuth call, or token load was run.

`schwab-streamer`'s own unit tests still exercise the pinned source runtime
against localhost-only synthetic sockets. Those private module paths compile
only in the crate's unit-test build; they are not in the downstream public
library API. Adapter integration tests use only the public decoder and local
subscription-state manager. These are protocol-only synthetic checks, not
provider compatibility or market-data acceptance evidence.

## Validation status

The final integrated workspace uses Core `domain`, `market-contracts`, and
`exact-decimal` from the published trading-core revision
`23a87d5b5a549e4489c1a2844c4132c43148fe6b` (tree
`f7b0c789bdc16c4695730d349cd519d2196900c7`). The full nine-package workspace
passes these offline gates with the Rust 1.99.0 toolchain:

```text
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/root/.cache/lqepoch/broker-connectors-target \
  cargo +1.99.0 test --workspace --all-features --locked --offline
313 passed; 4 ignored; 0 failed

CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/root/.cache/lqepoch/broker-connectors-target \
  cargo +1.99.0 clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
passed

cargo +1.99.0 deny --all-features --locked --offline check all
passed

cargo +1.99.0 fmt --all -- --check
passed

python3 scripts/generate_spdx_sbom.py
python3 scripts/update_source_manifest_hashes.py
python3 scripts/check_vendor_provenance.py
passed
```

Static source-hash checks also matched all 88 root imported-source entries and
66 selected Schwab source entries to their recorded target hashes; all 66
selected source hashes and Git blobs matched the immutable source pin. Final
selected source hashes and Git blobs matched the immutable source pin. Gitleaks
range and working-tree scans are recorded separately for the frozen commit.
Real Schwab REST/Streamer access, OAuth, live accounts, Windows/macOS execution,
and market-data entitlement are **NOT RUN**.
