# Dependency resolver evidence

## Security-relevant runtime patch pins

The public workspace pins Tokio `1.53.2` and zeroize `1.9.1` exactly. Keep the
lockfile aligned with these workspace pins rather than lowering them to satisfy
a downstream resolver: Tokio `1.53.2` includes fixes to bounded-channel,
semaphore, broadcast wakeup, and runtime behavior; zeroize `1.9.1` fixes a
potential uninitialized-memory read in the `optimization_barrier` fallback and
makes `Zeroizing` debug output opaque. These upstream fixes are recorded in the
[Tokio 1.53.2 release notes](https://github.com/tokio-rs/tokio/releases/tag/tokio-1.53.2),
[zeroize PR #1551](https://github.com/RustCrypto/utils/pull/1551), and
[zeroize PR #1497](https://github.com/RustCrypto/utils/pull/1497). This rationale
does not depend on whether an advisory database has assigned a RUSTSEC ID.

The engine integration isolated the pinned public dependency graph in
`/tmp/lq-resolver-probe` and ran this command:

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 metadata --manifest-path /tmp/lq-resolver-probe/Cargo.toml --format-version 1 > /tmp/lq-resolver-probe/metadata.json
```

The command exited with status 101. Cargo reported that
`market-contracts v0.1.0` from `trading-core@0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8`
requires `serde_json =1.0.151`, while the previously selected
`serde_json v1.0.149` is required by the pinned Schwab persistence dependency
from `c907d18bc31790ede4cf36a4312a6813467506f0`; Cargo ended with
`failed to select a version for serde_json which could resolve this conflict`.

This probe used the pinned dependency manifests only. It did not inspect private
Git history, change the Schwab dependency, or exercise a provider connection.
`broker-execution` therefore stays separate from `broker-ports` and depends only
on core `domain`; `scripts/check_broker_execution_dependency_firewall.py`
checks that boundary on the public workspace. This is historical resolver
evidence from the engine probe and intentionally retains its original Rust
1.98.1 command; this broker workspace now pins Rust 1.99.0 because the separate
`alpaca-rest-read` consumer uses the pinned SDK, while `broker-execution` itself
still declares MSRV 1.98.1.

The current public Schwab extraction is a narrower package set and no longer
uses the source workspace's former `serde_json=1.0.149` selection. Its target
workspace uses exact `serde_json=1.0.151` with `arbitrary_precision`, matching
the pinned `market-contracts` dependency and preserving decimal lexical
precision. The source-to-target version adaptation is recorded in
`vendor/schwab/SOURCE-MANIFEST.json`.

The current workspace pins `domain` and `market-contracts`, and their transitive
`exact-decimal` package, to the same immutable trading-core PR 11 revision
`23a87d5b5a549e4489c1a2844c4132c43148fe6b` (tree
`f7b0c789bdc16c4695730d349cd519d2196900c7`). Cargo.lock records one shared
revision so adapters and consumers use the same Rust domain and market-contract
types. `SOURCE-MANIFEST.json` records that pin for all three packages.

The pinned Alpaca REST stack's target-union dependency graph includes
`webpki-root-certs 1.0.9`: the SDK enables reqwest 0.13.5's `rustls` feature,
which includes `rustls-platform-verifier 0.7.1`, whose wasm32 target dependency
is `webpki-root-certs`. This target-specific edge is retained in the SBOM even
though it is not the root store used by the native Linux transport. Cargo
declares the package license as `CDLA-Permissive-2.0`; its package `LICENSE`
text, SHA-256, registry checksum, and target-specific dependency path are
recorded in `NOTICE`, `SOURCE-MANIFEST.json`, and the SPDX package metadata.
The deny policy contains an exact crate/version exception rather than a global
license allowance. These Mozilla root certificate data do not attest broker
identity, provider entitlement, or market-data source.
