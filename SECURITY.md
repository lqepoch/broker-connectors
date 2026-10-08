# Security policy

Report a suspected vulnerability privately through GitHub's private vulnerability
reporting for this repository if it is enabled, or through a verified private
maintainer contact. Do not include credentials, account identifiers, private
market data, or exploit payloads in public issues.

The current library is read-only. Its tests use synthetic inputs and fake
transports; they do not authenticate with a broker or establish entitlement.
Do not use this repository as evidence that a broker account, live market feed,
or trading workflow is ready for production.

Security fixes should include a bounded regression test and describe the
affected versions and any required consumer action. Credential-bearing network
requests must use fixed provider origins and reject cross-origin redirects.
