# Vendored rust-ibapi source

This directory contains the upstream `src/` tree from `wboayue/rust-ibapi` at
commit `3e73f2f1cfac151c10e403a3e7d779272134445f`. The upstream package reports
version `5.0.0`, MIT license, Rust MSRV `1.88`, and describes itself as an
unofficial Tokio/blocking client for the IBKR TWS API. The `v5.0.0` tag points
to a different commit (`f43c64682ab24b822dcd2892ff4208eac81813dc`); this vendor
snapshot is pinned to the inspected `main` commit, not that tag.

Source archive digest for the upstream commit's `Cargo.toml`, `LICENSE`, and
`src/` paths (from `git archive --format=tar`):

```text
SHA-256 063f22b4b640fd2fddcddb100eaa02f39031e81c1ee09500769bfa5e44cf8647
```

`Cargo.toml.upstream` preserves the exact upstream manifest. The active local
`Cargo.toml` is a dependency-only manifest: it removes upstream workspace
members, development dependencies, examples, and docs.rs packaging metadata so
this repository builds only the vendored library. The pinned upstream `src/`
tree is retained. Trailing whitespace was removed from five upstream source
files (`accounts/common/test_tables.rs`, `client/async.rs`,
`orders/builder/async_impl.rs`, `orders/builder/sync_impl.rs`, and
`orders/mod.rs`) so repository whitespace checks pass; no protocol or SDK
behavior was changed. The archive digest above covers the unmodified upstream
archive, not this whitespace-normalized working copy. The manifest records
hashes for all 302 files in the pinned `Cargo.toml`, `LICENSE`, and `src/`
closure, including the exact source path for each file. `LOCAL-CHANGES.json`
binds each of the five normalized files to its upstream and target hash and
lists the exact 1-based line numbers; the record is descriptive and is not
presented as an apply-ready patch.

The adapter crate keeps the SDK client private and exposes only the bounded,
read-only option-catalog API. Its dependency disables default features and
enables only the upstream `async` feature. It does not expose SDK account,
order, market-data subscription, or historical-data methods. The SDK itself
contains a broader public API; the adapter boundary is enforced in
`crates/ibkr-read` and its fake-wire tests. The wrapper accepts only loopback
addresses for this phase; remote Gateway deployments have not been reviewed.
