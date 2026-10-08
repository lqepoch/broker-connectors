# Vendored rust-ibapi source

This directory contains the upstream `src/` tree from `wboayue/rust-ibapi` at
commit `3e73f2f1cfac151c10e403a3e7d779272134445f`. The upstream package reports
version `5.0.0`, MIT license, Rust MSRV `1.88`, and describes itself as an
unofficial Tokio/blocking client for the IBKR TWS API. The `v5.0.0` tag points
to a different commit (`f43c64682ab24b822dcd2892ff4208eac81813dc`); this vendor
snapshot is pinned to the inspected `main` commit, not that tag.

Source archive digest for the upstream commit's `Cargo.toml`, `LICENSE`,
`rustfmt.toml`, and `src/` paths (from `git archive --format=tar`):

```text
SHA-256 91b49f2c57e423258d20b1f427682d622b78de011eacbb75fcb4296ceacb3532
```

The upstream `rustfmt.toml` is copied unchanged to this directory so Cargo's
workspace-wide format check uses the vendored source's pinned formatter settings
without rewriting those sources. Its Git blob is
`0a4312322c11cec08f41dd1ffc976fb3e4b1c395` and its file SHA-256 is
`ebdb640eee8312fc86d59249c5b43fb150f62375358f0216c2fa1bb7ef9514d2`. The
existing upstream MIT license applies to this configuration file as part of the
pinned repository snapshot.

`Cargo.toml.upstream` preserves the exact upstream manifest. The active local
`Cargo.toml` is a dependency-only manifest: it removes upstream workspace
members, development dependencies, examples, and docs.rs packaging metadata so
this repository builds only the vendored library. The pinned upstream `src/`
tree is retained with five whitespace normalizations and narrowly scoped
privacy changes, all bound to pinned source hashes and adapted target hashes in
`LOCAL-CHANGES.json` and `SOURCE-MANIFEST.json`.

The five whitespace-only edits affect `accounts/common/test_tables.rs`,
`client/async.rs`, `orders/builder/async_impl.rs`,
`orders/builder/sync_impl.rs`, and `orders/mod.rs`. The local privacy edits
change only diagnostics, test assertions, and recorder construction:
`connection/sync.rs` and `connection/async.rs` log outbound request and
handshake byte counts rather than raw frames; `connection/common.rs` logs
message IDs and inbound byte counts rather than decoded text payloads;
`common/test_utils.rs` uses stable result classes and redacted size diagnostics
rather than printing error values or protobuf bodies; a request-body mismatch
uses a fixed classification with no body-derived diagnostics. Transport, subscription and handshake
diagnostics use fixed error classes, message IDs and payload lengths rather
than wire content. Both `transport/recorder.rs` and `transport/raw_capture.rs`
keep their production capture paths disabled. Neither `IBAPI_RECORDING_DIR`
nor `IBAPI_RAW_CAPTURE_DIR` enables persistence. Tests may opt into capture
only through test-only constructors with explicit temporary directories and
synthetic fixtures. No real provider/account traffic is captured.

The source archive digest above covers the unmodified upstream
archive, not this locally adapted working copy. The manifest records hashes
for all 303 files in the pinned `Cargo.toml`, `LICENSE`, `rustfmt.toml`, and
`src/` closure, including the exact source path for each file. The versioned
local change record binds each changed file to its upstream and target hashes
and records the operation and source lines; it is descriptive and is not
presented as an apply-ready patch.

The adapter crate keeps the SDK client private and exposes only the bounded,
read-only option-catalog API. Its dependency disables default features and
enables only the upstream `async` feature. It does not expose SDK account,
order, market-data subscription, or historical-data methods. The SDK itself
contains a broader public API; the adapter boundary is enforced in
`crates/ibkr-read` and its fake-wire tests. The wrapper accepts only loopback
addresses for this phase; remote Gateway deployments have not been reviewed.
