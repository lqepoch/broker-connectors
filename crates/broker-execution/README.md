# broker-execution

`broker-execution` defines a provider-neutral software port for submit, replace, and cancel
commands. It reuses the frozen `trading-core@0d23c2e9f9e29c97d3f386c48cc47b9fd7909d7b`
`domain` types, including `OptionComboIntent`, `ExecutionRoute`, `RoutedOrderIdentity`, and
`Revision`. It does not define a second order model, account ledger, risk engine, or outbox.

This crate contains no broker writer or network dependency. The port rejects Live routes and
carries typed `Accepted`, `DefinitelyNotSent`, `Rejected`, and `Unknown` outcomes. Accepted results
are checked against the command identity, route, account namespace, and checked next revision.
Replace acknowledgements carry an explicit predecessor-to-replacement provider identity link. A
provider may preserve the current identity or allocate a new one; for a new identity the adapter
must supply a provider-reported predecessor exactly equal to the known current identity and in the
same account namespace. Missing or mismatched linkage is `Unknown`/protocol-invalid. Cancel
acknowledgements must retain the current provider identity. An `Unknown` outcome requires
reconciliation and cannot be retried automatically. The route and account namespace are
descriptive data, not authorization. The consuming application owns admission, risk, funds,
durable intent/outbox, account authority, and reconciliation.

The `offline-fake` feature is opt-in and non-default. It provides a synthetic-only fake with
explicit queue-count limits and a 1 MiB aggregate accounted-data budget across queued outcomes and
recorded commands. The byte budget includes typed structure sizes and variable identifier, route,
and option-leg strings; it is a deterministic retained-data accounting limit, not an OS RSS
measurement. Capacity exhaustion is explicit and does not drop an existing entry. It never reads
credentials, opens sockets, or contacts a provider. Its tests do not establish Paper or Live
execution capability. Downstream code can consume an `Accepted` result but cannot construct one:
the acceptance builder is crate-private, so an external `ExecutionPort` implementer cannot
currently produce `Accepted` until a reviewed in-crate adapter or validated factory is added.

The crate is kept separate from `broker-ports` so an engine can consume execution contracts
without also importing the read-port crate's `market-contracts` dependency and its
`serde_json=1.0.151` pin. The current Schwab persistence source pins `serde_json=1.0.149`; the
engine integration reported that Cargo rejected the combined dependency graph with both exact
versions. This crate does not change the Schwab pin to work around it. This is a dependency
boundary, not a claim that the combined engine workspace has already built. Run
`python3 scripts/check_broker_execution_dependency_firewall.py` at the repository root to verify
the full normal dependency graph remains limited to this crate, core `domain`, and `exact-decimal`.

This crate has no Alpaca REST or IBKR execution SDK dependency. The pinned
[`wmzhai/alpaca-rust@d91be382e3e9d25c52c24e626e78702532d24ba2`](https://github.com/wmzhai/alpaca-rust/tree/d91be382e3e9d25c52c24e626e78702532d24ba2)
is a community Rust SDK candidate listed by Alpaca as community-made, not an Alpaca-supported
official SDK. The evaluated [`wboayue/rust-ibapi@3e73f2f1cfac151c10e403a3e7d779272134445f`](https://github.com/wboayue/rust-ibapi/tree/3e73f2f1cfac151c10e403a3e7d779272134445f)
describes itself as an unofficial community client. Official Alpaca API and IBKR TWS API docs are
protocol references only; they do not establish vendor support for these community Rust SDKs.

## Local checks

```sh
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p broker-execution --features offline-fake --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 tree -p broker-execution --all-features --edges all
```
