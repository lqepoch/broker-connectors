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
license to Cargo metadata. `schwab-streamer` pins `sha2 = 0.11.0` directly so
that the existing Alpaca integration retains its separate `sha2 = 0.10.9` pin.
The workspace resolves `serde_json = 1.0.151` with `arbitrary_precision`; the
source's former `1.0.149` dependency is not retained. `schwab-rest` keeps exact
`reqwest = 0.13.5` with rustls and its local TLS tests keep exact `rcgen =
0.14.10` and `tokio-rustls = 0.26.5` pins. These dependency adaptations are
recorded in the per-file manifest and the root source manifest.

No raw credentials, account configuration, private account state, actual market
payloads, or provider traces are included. The committed request and stream
fixtures are synthetic. Source tests use fake transports or localhost-only TLS
sockets; this target does not run OAuth or call a broker. Real provider REST,
streamer, OAuth, account, and market-data behavior remain **NOT RUN**.

The target-only strict-Clippy changes are limited to API error documentation,
checked conversions, and local readability fixes. Item-scoped
`too_many_lines` allowances retain ordered buffer, connection, and frame state
transitions plus synthetic tests as contiguous review units; no numeric or
safety lint is suppressed.
