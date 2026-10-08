# broker-execution

`broker-execution` defines a provider-neutral software port for submit, replace, and cancel
commands. It reuses the frozen `trading-core@0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8`
`domain` types, including `OptionComboIntent`, `ExecutionRoute`, `RoutedOrderIdentity`, and
`Revision`. It does not define a second order model, account ledger, risk engine, or outbox.

This crate contains no broker writer or network dependency. The port rejects Live routes, carries
typed `Accepted`, `DefinitelyNotSent`, `Rejected`, and `Unknown` outcomes, and validates accepted
identity and revision fields against the command. An `Unknown` outcome requires reconciliation
and cannot be retried automatically. The route and account namespace are descriptive data, not
authorization. The consuming application owns admission, risk, funds, durable intent/outbox,
account authority, and reconciliation.

The `offline-fake` feature is opt-in and non-default. It provides a synthetic-only fake with
explicitly bounded result and command queues. It never reads credentials, opens sockets, or
contacts a provider. Its tests do not establish Paper or Live execution capability.
Downstream code can consume an `Accepted` result but cannot construct one; the acceptance builder
is crate-private and the fake reaches it only after validating the scripted identity and revision.

The crate is kept separate from `broker-ports` so an engine can consume execution contracts
without also importing the read-port crate's `market-contracts` dependency and its
`serde_json=1.0.151` pin. The current Schwab persistence source pins `serde_json=1.0.149`; the
engine integration reported that Cargo rejected the combined dependency graph with both exact
versions. This crate does not change the Schwab pin to work around it. This is a dependency
boundary, not a claim that the combined engine workspace has already built. Run
`python3 scripts/check_broker_execution_dependency_firewall.py` at the repository root to verify
the full normal dependency graph remains limited to this crate, core `domain`, and `exact-decimal`.

## Local checks

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 test -p broker-execution --locked
CARGO_BUILD_JOBS=2 cargo +1.98.1 test -p broker-execution --features offline-fake --locked
CARGO_BUILD_JOBS=2 cargo +1.98.1 tree -p broker-execution --all-features --edges all
```
