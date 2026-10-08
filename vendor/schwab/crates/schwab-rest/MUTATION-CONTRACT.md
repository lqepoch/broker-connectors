# Node SDK mutation contract evidence

This document records a test-only contract slice for #172. The corresponding
Rust code is compiled only in `#[cfg(test)]` and cannot send HTTP requests,
read credentials, or be imported as a production API. It does not claim broker
acceptance or a live execution path.

## Current Node surface mapping

| Node method | Node source behavior | Rust test-only coverage | Evidence |
| --- | --- | --- | --- |
| `TraderApiClient.placeOrder` | `POST /accounts/{account}/orders`, JSON order body | `MutationOperation::Place` | `src/clients/trader.ts`; fixture cases `place-400-preserves-explicit-api-error-body` and Node SDK POST vectors |
| `TraderApiClient.replaceOrder` | `PUT /accounts/{account}/orders/{id}`, JSON order body | `MutationOperation::Replace` | `src/clients/trader.ts`; `sdk_mutations.json` Replace vectors and `sdk_mutation_errors_172.json` |
| `TraderApiClient.cancelOrder` | `DELETE /accounts/{account}/orders/{id}`, optional `CancelOrderRequest` body | `MutationOperation::Cancel` | `src/clients/trader.ts`, `src/types/trader.ts`; synthetic no-body Cancel vectors |
| Mutation request policy | `applyMutationOverrides` sets `maxRetries: 0` and `retryConfig.maxRetries: 0` | one physical attempt; automatic replay disallowed | `src/clients/trader.ts`, `src/utils/httpClient.ts`; fake-fetch vectors assert one request and zero retry events |
| Successful Place/Replace metadata | Requires a non-empty `Location` whose final path segments are `orders/{digits}`; otherwise throws `UnknownOutcomeError`. The current parser checks only the path and accepts an absolute foreign origin. | The cfg(test)-only mirror retains this behavior solely to characterize Node; the external-origin vector is marked `BUG_IN_LEGACY`. | `src/clients/trader.ts`; synthetic success, unusable-Location, and external-origin vectors |
| Successful Cancel metadata | A 2xx Cancel may return without a Location/order ID | Returned without either value | `src/clients/trader.ts`; `cancel-204-without-location-is-known-success` |
| Ambiguous outcomes | Network errors, 5xx, status 0, and response-body read errors become `UnknownOutcomeError` | Unknown; replay remains disallowed | `src/clients/trader.ts`, `src/utils/httpClient.ts`; response-lost, 503, missing Location, and body-read fixtures |
| Explicit 4xx / rate metadata | Remains `SchwabApiError`, preserving body and response/rate headers | Status/header/body assertions; classification does not claim that a side effect was impossible | `src/clients/trader.ts`, `src/utils/httpClient.ts`, `src/utils/responseMetadata.ts`; 400/409/429 fixture vectors |

Account path encoding and numeric order-ID validation reuse the existing
read-route validators. The tests compare the percent-encoded synthetic account
path and exact Node-captured JSON bytes. Debug output redacts account/order
identifiers, payloads, response headers, and bodies.

## Safety boundary and blocked evidence

The Node SDK itself does not expose whether a rejected fetch occurred before
DNS resolution, connection, TLS, request-body transmission, or broker
acceptance. Therefore transport rejection stays `UNKNOWN`; no phase-based
"definitely unsent" outcome is inferred. A response-body read failure also
stays `UNKNOWN`. A 5xx response stays `UNKNOWN`. The 4xx classification mirrors
the SDK's typed error mapping only and is not proof that the broker applied no
side effect. The external-origin `Location` vector exposes a separate
`BUG_IN_LEGACY`: Node parses the ID from `pathname` without checking URL origin
or matching the Location path to the request. The unit-test mirror is not a
production policy. A future Rust production parser must validate the fixed
trusted Schwab origin and expected request/account/order path before accepting
an order ID.

These DTOs are not connected to the Rust execution coordinator. They do not
provide a production write client, writer epoch, WAL, risk reservation,
account-authority gate, final validation, audit record, or reconciliation.
Any future live mutation implementation must be wired only through the
execution-owned single writer and its durable WAL/final gate; unknown outcomes
must be resolved by read-only authoritative reconciliation and never by blind
replay. The automation contract has no Preview request. This test file does
not implement an alternative execution route.

No provider route or acceptance claim is made here. The route, body, and
classification observations are based on the checked-in Node implementation
and sanitized synthetic fixtures:

- `src/clients/trader.ts`
- `src/utils/httpClient.ts`
- `src/utils/errors.ts`
- `src/types/trader.ts`
- `src/utils/responseMetadata.ts`
- `test/fixtures/rust-v2/sdk.json`
- `test/fixtures/rust-v2/sdk_mutations.json`
- `test/fixtures/rust-v2/sdk_mutation_errors_172.json`

The `replace-absolute-external-location-is-accepted` vector uses a reserved
`.invalid` hostname through fake fetch only. It records Node's local return
value and is not broker evidence.

`#172` remains incomplete. Official provider contract verification, the full
public SDK capability surface, production execution integration, broker-side
acceptance, and end-to-end WAL/reconciliation behavior remain outstanding.
