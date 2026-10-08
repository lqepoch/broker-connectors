# Contributing

Use the pinned Rust toolchain from `rust-toolchain.toml` and keep dependencies
exactly pinned in Cargo.toml and Cargo.lock. Tests must use synthetic provider
frames and fake transports. Do not add credentials, broker/OAuth calls, account
state, order-send paths, private trading traces, or non-public market data.

For Rust changes, run:

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --workspace --locked
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --workspace --all-targets --locked -- -D warnings
python3 scripts/generate_spdx_sbom.py
python3 scripts/update_source_manifest_hashes.py
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
