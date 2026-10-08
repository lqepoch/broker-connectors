# Schwab project-source extraction

This directory contains a selected extraction from the private project repository
`lqepoch/schwab_auto_bot` at immutable commit
`c907d18bc31790ede4cf36a4312a6813467506f0` and source tree
`d0e08a6a7ef030edcd032157375395926514125b`. It includes only the `schwab-rest`,
`schwab-sdk`, and `schwab-streamer` packages plus five reviewed synthetic
contract fixtures. `SOURCE-MANIFEST.json` records every selected path, source Git
blob, source SHA-256, target SHA-256, package scope, and target-only adaptation.

The selected private source packages do not carry a repository-wide open-source
license. The repository owner explicitly authorized these listed files for
publication in this target under `MIT OR Apache-2.0`; this notice does not change
the license or ownership of the source repository or any unlisted path. The
project's MIT and Apache license texts remain at the repository root.

The target-only package manifest adaptations expose the authorized target
license to Cargo metadata. `schwab-streamer` moves credential/socket/session
dependencies to dev-only scope because those modules compile only in its unit
tests. Its test-only dependency pins `sha2 = 0.11.0` so the existing Alpaca
integration retains its separate `sha2 = 0.10.9` pin.
The workspace resolves `serde_json = 1.0.151` with `arbitrary_precision`; the
source's former `1.0.149` dependency is not retained. `schwab-rest` keeps exact
`reqwest = 0.13.5` with rustls and its local TLS tests keep exact `rcgen =
0.14.10` and `tokio-rustls = 0.26.5` pins. These dependency adaptations are
recorded in the per-file manifest and the root source manifest.

The extracted `schwab-sdk` to `schwab-rest` path dependency and the adapter's
`schwab-sdk`/`schwab-streamer` path dependencies require exact workspace
versions (`=0.1.0`). This preserves the local package boundary under the
workspace's wildcard-dependency deny policy without changing the dependency
graph.

Of the 66 selected files, 19 remain byte-for-byte copies of the pinned source
and 47 contain target adaptations. By per-file category, 31 include code
changes, 24 documentation changes, 12 test changes, and 3 manifest changes;
categories overlap. The nested and root source manifests record each changed
file's source and target hash, categories, and a specific per-file summary.
Adaptations cover workspace/license metadata, facade re-exports, API/error
documentation and must-use annotations, checked numeric conversions, test-only
readability changes, and the runtime API restriction described below. No
selected JSON fixture was rewritten.

No raw credentials, account configuration, private account state, actual market
payloads, or provider traces are included. The committed request and stream
fixtures are synthetic. Source tests use fake transports or localhost-only TLS
sockets; this target does not run OAuth or call a broker. Real provider REST,
streamer, OAuth, account, and market-data behavior remain **NOT RUN**.

The target-only strict-Clippy changes include API error documentation, checked
conversions, explicit imports, and local readability fixes. Item-scoped
`too_many_lines` allowances retain ordered buffer, connection, and frame state
transitions plus synthetic tests as contiguous review units; no numeric or
safety lint is suppressed.

The public `schwab-streamer` API contains the bounded decoder, opaque protocol
values, fixed service manifests, and subscription-state types only. Its
credentials, LOGIN serializer, native socket, factory, and authenticated
session modules are compiled only into the crate's own unit-test build. The
test factory accepts loopback `ws://` targets only; its remote-WSS rejection
test records that connector and LOGIN serialization callbacks are not called.
The adapter's `SchwabStreamerGate` validates a fresh lease against an explicit
full-URL WSS allowlist, then drops it without returning a credential object.
There is no production bootstrap, official endpoint allowlist, public
credential-send runtime, OAuth call, or provider connection in this
repository. Production streaming remains **BLOCKED**.

The extracted crate's own test build still drives its internal fake-socket
runtime over localhost and checks LOGIN/ACK and subscription lifecycle
protocol behavior. The downstream adapter integration test instead checks the
public decoder and local subscription-state manager. These are synthetic
protocol evidence, not provider compatibility, quote interpretation, or
market-data acceptance evidence.
