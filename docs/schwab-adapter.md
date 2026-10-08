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
transport ports, bounded response parser, no-auto-retry behavior, source SDK
error categories, and native Streamer protocol/runtime. Any manifest version
adaptation is recorded in the per-file provenance manifest and will be folded
into the root source manifest after the serialized Cargo/SBOM integration
stage.

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

`SchwabStreamerGate` requires all three inputs before the extracted SDK
credential trait can produce a session: an application-supplied fresh
`userPreference`/access-token-lease source, a zeroizing lease, and a nonempty
exact host/port allowlist. It rejects expired leases, insecure schemes, and
host/port mismatches. There is no production bootstrap implementation or
provider allowlist in this repository, so production streaming remains
**BLOCKED**. No real provider connection, OAuth call, or token load was run.

The target fake-socket test drives the extracted `StreamerRuntime` and its
`StreamerSocket`/`AuthenticatedSessionFactory` traits. It confirms matching ACK
readiness, connection generation, sparse field revision, and uninterpreted
wire keys. It is protocol-only synthetic evidence, not provider compatibility
or market-data acceptance evidence.

## Validation status

No Cargo build or test has been run for this target slice yet because the
workspace dependency and lockfile integration is serialized with the Alpaca
and IBKR owners. The first permitted local validation after that integration
will include the complete `schwab-sdk`, `schwab-rest`, `schwab-streamer`, and
`schwab-adapter` test suites, formatting, Clippy, dependency/SBOM checks,
source-manifest hash validation, and secret scanning. Real Schwab REST/Streamer
access, OAuth, live accounts, Windows/macOS execution, and market-data
entitlement are **NOT RUN**.
