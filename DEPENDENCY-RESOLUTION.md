# Dependency resolver evidence

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
`exact-decimal` package, to the same immutable RawCore7 revision
`ebfe606b381011b4a7ad3dfdc673c8e8f6651a08` (tree
`64f964908d00960740a33f29369b702b83f38526`). Cargo.lock records one shared
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
