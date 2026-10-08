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

## Secret scan exception

Gitleaks 8.30.1's `generic-api-key` rule produced one reviewed false positive
in commit `995ee0817f144491f9ce9028495da9dcd9ca7a2f`,
`vendor/schwab/crates/schwab-rest/README.md:6`. The original ordinary
documentation sentence only limited the review scope; it contained no
credential, token, account identifier, or access material. The initial
`origin/main..HEAD` scan exited 1 with that single finding. The selected target
README now uses equivalent wording that does not trigger the rule. The exact
historical sentence is preserved with an HTML character reference in
`SOURCE-MANIFEST.json` for source and scan provenance. A directory-mode scan of
the prior target wording found that same line because it has no commit context
for the narrow historical exception. The current full-directory scan passes;
the exception remains limited to the historical commit.

The exact Gitleaks fingerprint is listed in `.gitleaksignore` and
`SOURCE-MANIFEST.json`. The exception is limited to this commit, file, rule, and
line; do not replace it with a rule-wide or path-wide exclusion. After adding
the exact fingerprint, the branch-range and current pre-commit diff scans both
exited 0. Re-run both on the final frozen head before publication.
