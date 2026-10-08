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
checks that boundary on the public workspace.
