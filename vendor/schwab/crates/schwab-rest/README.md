# `schwab-rest` read-surface map

This file is an implementation review map for the bounded Rust read-only REST
slice in #172. It records which current Node GET helpers have an equivalent
request builder and response family. It does not claim that the full public
SDK, OAuth, retry/admission policy, or execution path has migrated.

## Node GET helpers mapped in this crate

All request builders below produce a fixed GET route. Every request passes
through the SDK-owned ReadAdmissionPort before token acquisition and
transport. The port accepts only a bounded urgency hint, a maximum wait, and
bounded 429 metadata; it defines no quota or local admission policy. The
project's request-budget policy is adapted in broker-schwab through
RequestBudgetReadAdmission. Callers that need shared admission must inject and
share the same port implementation. After admission, each GET performs
exactly one token-provider call and one transport call. Typed facade methods
use the same port and route allowlist, validate the response family selected by
the route, and return bounded raw bytes and the DTO projection. Non-2xx
responses retain bounded status, body, and headers, including Retry-After, in
RestError::HttpStatus; the project adapter may observe 429 metadata, but no
request is automatically retried. DTOs describe response syntax and do not
establish snapshot authority.

Dependency boundary: `schwab-rest` has no workspace dependency on `domain`,
`account-state`, or `request-budget`. `broker_schwab::SchwabAccountHashSource`
and the request-budget adapter live in `broker-schwab`; REST owns only Schwab
route, response, transport, and admission-port contracts.
Generic `read` and `read_typed` calls classify account-number, account,
order, and transaction routes as `Urgent` by default; other generic routes
default to `Refresh`. The corresponding account/order/transaction typed
convenience methods use that same default. User-preference and streamer-info
convenience methods remain explicitly `Urgent`; market-data convenience
methods remain `Refresh`. `read_with_priority` and
`read_typed_with_priority` allow an explicit application override. These
priorities only order local admission and reserve configured local headroom;
they do not describe or change Schwab quotas.

| Current Node surface | Rust request / route | Response parser kind | Evidence / limit |
| --- | --- | --- | --- |
| `TraderApiClient.getAccountNumbers[WithResponse]` | `AccountNumbers` → `/trader/v1/accounts/accountNumbers` | `AccountNumbers` | `TraderReadResponse::AccountNumbers`; `broker-schwab::SchwabAccountHashSource` maps validated rows to the #169 read-only source port; at most 4,096 rows |
| `TraderApiClient.getAccounts[WithResponse]` | `Accounts(AccountsQuery)` → `/trader/v1/accounts` | `Accounts` | `TraderReadResponse::Accounts`; `fields=positions` is preserved |
| `TraderApiClient.getAccount[WithResponse]`; runtime position reader | `Account { account_hash, query }` → `/trader/v1/accounts/{hash}` | `Account` | `TraderReadResponse::Account`; position and balance numeric tokens are exact and bounded |
| `TraderApiClient.getOrders[WithResponse]`; runtime order snapshot pages | `Orders { account_hash, query }` → `/trader/v1/accounts/{hash}/orders`; `orders_with_query_extensions` preserves bounded additive fields | `Orders` | `TraderReadResponse::Orders`; nested legs, activities, child orders, and query fields are typed |
| `TraderApiClient.getOrder[WithResponse]`; runtime exact-order read | `Order { account_hash, order_id }` → `/trader/v1/accounts/{hash}/orders/{id}` | `Order` | `TraderReadResponse::Order`; response order IDs retain exact numeric tokens |
| `TraderApiClient.getOrdersAcrossAccounts[WithResponse]` | `OrdersAcrossAccounts(query)` → `/trader/v1/orders`; `orders_across_accounts_with_query_extensions` preserves bounded additive fields | `Orders` | Explicit query DTO |
| `TraderApiClient.getTransactions[WithResponse]` | `Transactions { account_hash, query }` → `/trader/v1/accounts/{hash}/transactions`; `transactions_with_query_extensions` preserves bounded additive fields | `Transactions` | `TraderReadResponse::Transactions`; transfer items and user fields are typed |
| `TraderApiClient.getTransaction[WithResponse]` | `Transaction { account_hash, transaction_id }` → `/trader/v1/accounts/{hash}/transactions/{id}` | `Transaction` | Object-or-array shape is retained; typed convenience selects the first row and fails on an empty array |
| `TraderApiClient.getUserPreferences[WithResponse]`; runtime streamer bootstrap | `UserPreferences` → `/trader/v1/userPreference` | `UserPreferences` plus `user_preferences_model()` | Typed object-or-array preference envelope; account, streamer, and offer rows preserve additive fields |
| `TraderApiClient.getStreamerInfo` | `StreamerInfo` → same `/trader/v1/userPreference` request | `UserPreferences` plus `streamer_info_model()` | Typed convenience selects the first preference and first `streamerInfo`; legacy `streamer_info()` still returns the validated source JSON |
| `MarketDataApiClient.getQuotes[WithResponse]` | `quotes(QuotesQuery)` → `/marketdata/v1/quotes` | `Quotes` | Typed quote rows, nested quote/reference fields, and additive fields |
| `MarketDataApiClient.getOptionQuote(s)` | `option_quote(s)` request plus `SchwabRestClient::normalized_option_quotes(...)` → same `/marketdata/v1/quotes` route | `Quotes` plus `NormalizedOptionQuoteReadResponse` | One GET; matches trailing-padding trim, blank filtering, normalized-duplicate rejection, default `quote,reference` fields, and an explicitly empty fields array; response metadata and exact-decimal quote projections are both retained |
| `MarketDataApiClient.getVerticalOptionQuote` | `vertical_option_quote` → same `/marketdata/v1/quotes` route | `Quotes` plus `vertical_quote_legs()` | Returns exact-decimal legs; the legacy synthetic `f64` spread is classified `BUG_IN_LEGACY` and is not the Rust price authority |
| `MarketDataApiClient.getQuote[WithResponse]` | `quote(symbol, fields)` → `/marketdata/v1/{symbol}/quotes` | `SingleQuote` | Typed history-shaped fields; route shape is evidenced by current Node source/golden only and provider response behavior remains unverified |
| `MarketDataApiClient.getOptionChains[WithResponse]` | `option_chains(OptionChainQuery)` → `/marketdata/v1/chains` | `OptionChain` | Typed expiration/strike maps; includes `includeQuotes` alias precedence |
| `MarketDataApiClient.getOptionExpirationChain[WithResponse]` | `option_expiration_chain(query)` → `/marketdata/v1/expirationchain` | `OptionExpirationChain` | Typed expiration list and explicit query DTO |
| `MarketDataApiClient.getPriceHistory[WithResponse]` | `price_history(query)` → `/marketdata/v1/pricehistory` | `PriceHistory` | Typed OHLCV rows; preserves zero and `false` query values |
| `MarketDataApiClient.getMovers[WithResponse]` | `movers(symbol, query)` → `/marketdata/v1/movers/{symbol}` | `Movers` | Typed screener rows; symbol encoded as one path segment |
| `MarketDataApiClient.getMarkets[WithResponse]` | `Markets(query)` → `/marketdata/v1/markets` | `MarketHours` | `MarketReadResponse::MarketHours`; market list remains comma-separated after query decoding |
| `MarketDataApiClient.getMarketHours[WithResponse]` | `MarketHours { market, query }` → `/marketdata/v1/markets/{market}` | `MarketHours` | Same typed record shape as batch; market encoded as one path segment |
| `MarketDataApiClient.searchInstruments[WithResponse]` | `SearchInstruments(query)` → `/marketdata/v1/instruments` | `InstrumentsSearch` | `MarketReadResponse::InstrumentsSearch`; symbol list and projection are explicit |
| `MarketDataApiClient.getInstrumentByCusip[WithResponse]` | `InstrumentByCusip(cusip)` → `/marketdata/v1/instruments/{cusip}` | `InstrumentDetail` | `MarketReadResponse::InstrumentDetail`; CUSIP encoded as one path segment |

The `SchwabGateway` account, order, and quote methods are compositions of the
rows above. Account-number-to-hash cache/refresh ownership belongs to the
`account-state` layer from #169 and is not duplicated in this REST crate. The
current runtime's direct GET callers for account bootstrap, user-preference
streamer bootstrap, position reads, order-range pages, and exact-order reads
map to the same request rows.

The Node Trader `OrdersQuery` and `TransactionsParams` types accept additive
string, number, and boolean keys. The fixed Rust query DTOs remain the normal
typed path; their `*_with_query_extensions` counterparts preserve this
additive Node surface without accepting caller-selected methods, paths, or
bodies. `QueryExtensions` omits `None`, retains `false` and decimal zero,
retains exact finite decimal text, and serializes query keys and values with
the same form encoding as Node's `URLSearchParams`. It rejects empty, over-
4,096-byte, or ASCII-control keys; control-containing or over-4,096-byte
values; duplicate keys; and keys that collide with fields already represented
by the typed query. Decimal values must fit `ExactDecimal`'s i128 coefficient
and 18-place scale. The builder accepts at most 100 extension items and the
complete request at most 100 query items; the existing 16-KiB request-target
cap also applies. These explicit bounds are Rust-side safety limits on the
dynamic Node contract.

The account-number/hash response accepts at most
`MAX_ACCOUNT_NUMBER_HASH_ROWS` (4,096) rows, matching the hard snapshot limit
in `account-state`. The parser checks this row count after decoding the
already byte-bounded JSON body, but before the general tree walk, per-row
schema validation, or typed `Vec<AccountNumberHash>` projection. The existing
1 MiB response-body limit remains independently enforced by the transport and
parser. This bounds projected mapping work; the JSON value itself is still
materialized within that byte limit. The REST parser owns the row-count bound, while the project mapping adapter
lives in broker-schwab. The SchwabAccountHashSource converts the typed
account_numbers_typed() response to ReadOnlyAccountHashSource; it rejects
missing DTOs, empty snapshots, malformed rows, invalid identifiers/hashes, and
duplicate normalized account numbers. HTTP 401/403 map to Unauthorized, 429 to
RateLimited, and transport or other HTTP failures to Unavailable. The adapter
exposes stable source codes and, for 429 only, a bounded raw Retry-After hint.
It performs no sleep or retry and discards provider response bodies,
identifiers, URL, and token. Fake-provider and fake-transport tests cover the
mapping without a live Schwab request. Runtime startup/bootstrap wiring
remains out of scope.

## Typed Trader response projection

`ParsedReadResponse::trader_model()` returns `TraderReadResponse` for the
account-number, account, position, order, and transaction response families
listed above. `Order` includes recursive child strategies and execution-leg
activity; account positions and balance groups are represented alongside
their enclosing account. `TransactionResponse` retains whether the broker
returned one object or an array, and `first()` mirrors the Node helper.

Known numeric fields use `WireNumber`, which stores the source JSON number
token instead of converting through `f64`; this retains significant trailing
zeros and identifiers beyond JavaScript's safe integer range. Conversion to
`ExactDecimal` is bounded to the existing i128 coefficient and 18-place scale;
when a caller requests arithmetic outside that bound, conversion returns a
fixed error while the DTO retains the original number token. The workspace
enables `serde_json`'s `arbitrary_precision` feature so parsed `Number` values
preserve that token. Non-finite numeric values fail response validation.
Unknown fields at each modeled object remain accessible through
`UnknownFields`, and the original parsed tree remains available through
`json()`. DTO and payload `Debug` output is redacted. The shared synthetic golden
`test/fixtures/rust-v2/sdk_response_models.json` is checked by Node's current
Zod schemas and the Rust DTO tests.

These DTOs establish only that a bounded successful response conforms to the
local schema. They do not prove that order, account, position, or transaction
data is a current authoritative snapshot. No OAuth bridge, production account
reader, or broker-backed validation is wired by this projection.

User preferences preserve whether Node returned one object or an array. The
typed projection includes account preferences, streamer identifiers, offer
permissions, and additive unknown fields; its `Debug` output is redacted. The
streamer URL check mirrors the current Node `z.string().url()` syntax contract,
which accepts schemes beyond `wss` (including `https`, `mailto`, and
`http://localhost`). It is not destination authorization and must never be used
as permission to connect. The #173 socket adapter must apply its own
authenticated Schwab-endpoint allowlist and fail-closed checks. No socket or
network connection is made by these DTOs.

`ParsedReadResponse::market_model()` returns typed projections for quotes,
single quotes, option chains and expirations, price history, movers, market
hours, and instrument search/detail. Quote and option-chain numeric wire fields
use `WireNumber` to preserve source number tokens; dynamic expiration/strike
keys remain map keys, and additive passthrough values remain available through
`UnknownFields`. Batch and single market-hours
routes share the current Node record shape: dynamic market/product/session keys
remain map keys, while required product/session fields are typed. Instrument
search and detail fields are optional strings as in the current Zod schemas.
The schemas declare no numeric fields in these families; additive unknown
values remain raw JSON and are not converted through `f64`. Rust retains
unknown session-time fields in `UnknownFields` for lossless access, although
the current Node session-time Zod object accepts and strips those fields. The
synthetic golden records this projection difference explicitly. All DTO Debug
output is redacted, and projections are not market-status or instrument
authority.

For option-quote input normalization, the source contract is
`src/clients/marketData.ts:105-119`: Node applies `trimEnd`, removes empty
symbols, checks normalized duplicates before `getQuotes`, and supplies
`['quote', 'reference']` only when `fields` is nullish. The #167
`market-batch-option-quotes-body-wrapper-normalizes-each-contract` golden
checks the default field query. The offline Rust constructor tests additionally
cover trailing padding, blank filtering, duplicate aliases, and `fields: []`
(`fields=`); they are source-derived cases because the existing Node golden
does not include those edge inputs. The vertical helper delegates to the same
option-quote flow. Rust rejects a missing/duplicate vertical leg before I/O,
which is an explicit fail-closed boundary.

`SchwabRestClient::normalized_option_quotes` composes that existing route and
response projection into one client call. It accepts an explicit observation
timestamp for deterministic `quote_age_ms`, and returns both the normalized
quotes and the original `RestResponse` so callers retain `Retry-After` and
rate-limit metadata. Its result is structural quote data only; it does not
establish freshness, non-future time, uncrossed NBBO, entitlement, or
tradeability.

Generic Node arrays are joined with commas by `normalizeList` and
`normalizeFields` (`src/clients/marketData.ts`); explicit CSV strings are
forwarded as strings. An array element such as `"QQQ,SPY"` becomes
indistinguishable from two values. Rust `CsvValues::from_values` rejects an
embedded comma to fail closed, while `CsvValues::from_csv` preserves an
explicit CSV-string input. This is a deliberate safe divergence from the
ambiguous legacy behavior, recorded as `BUG_IN_LEGACY` in the crate tests.
Option-quote symbol normalization remains source-specific: it trims only
trailing ECMAScript whitespace, drops blank entries, and rejects duplicates
after trimming; it does not trim leading whitespace.

Financial numeric query values (`strike`, `volatility`, `underlyingPrice`, and
`interestRate`) use bounded exact decimal validation and keep their original
text for URL encoding. The validator does not parse through binary floating
point. Its current representable scale is limited to 18 decimal places and
the response parser's bounded coefficient range. In the current Node source,
`buildQuery` forwards non-null numeric values without finite/range validation
and `HttpClient.buildUrl` applies `String(value)`; this permits values such as
`NaN`, infinities, and numeric precision outside the Rust bound to reach the
URL builder. Rust rejects these values before transport. This legacy behavior
is classified `BUG_IN_LEGACY`; the stricter exact bound is intentional and
must not be relaxed to reproduce unsafe floating-point query handling.

## Current Node package exports outside this REST slice

The root `package.json` exports additional non-REST capabilities. Their absence
from this crate is intentional and must not be read as migration completion:

| Node export | Rust owner / status |
| --- | --- |
| `.` | Rust application composition across `apps/*`; no single root SDK facade is claimed here |
| `./accounts` | `account-state` resolver from #169; not a REST-owned cache |
| `./orders`, `./options` | Strong domain/order-building APIs from #169 |
| `./normalized-quotes` | `market-data` and `pricing`; REST only parses raw quote rows and provides structural decimal projections |
| `./streamer-fields`, `./streamer-market-data`, `./streamer-contracts`, `./streamer-snapshot` | Streamer and market-data crates; not implemented by this REST crate |
| `./gateway` | Application-level read-only composition; current Rust route primitives are available, but no full public gateway facade is claimed |
| `./token-store` | `secrets` / #171; OAuth/token acquisition is not part of this client |
| `./contract-manifest` | Contract and market-data crates; not implemented by this REST crate |
| `./automation` | Rust app/runtime/execution crates; not implemented by this REST crate |

## Partial migration and blocked contract evidence

- Request routes and parameters in this slice are grounded in the checked-in
  current Node client methods and the synthetic #167 read-surface golden. The
  official [Schwab Developer Portal](https://developer.schwab.com/) is reachable,
  but its current endpoint-level API reference was not anonymously available
  for independent route verification. No new provider route is inferred from
  naming. This is local Node compatibility evidence, not provider acceptance
  evidence; production endpoint conformance remains **BLOCKED** pending
  authoritative current Schwab API reference material.
- #171 remains open. This crate accepts a validated, injected bearer token;
  it does not acquire, refresh, rotate, persist, or revoke credentials.
- **BLOCKED — production wiring:** `SchwabRestClient::read` and
  `ParsedReadResponse::from_endpoint_response` compose inside this crate, but
  no application runtime or OAuth bridge currently consumes these Trader DTOs.
  This parser does not establish an authoritative broker snapshot.
- Node's read transport can retry a characterized 429 scenario. Rust preserves
  bounded Retry-After response metadata and never retries the failed request.
  Local cooldown, date parsing, and any fallback wait are application policy
  supplied through the admission port; this REST crate does not choose them.
  The project adapter's date and cooldown checks use synthetic responses and
  make no Schwab request.
- Node quote convenience values expose raw numeric Greek/IV fields without
  unit conversion in `MarketDataApiClient.normalizeOptionQuote`. Rust retains
  exact decimal values for those fields but does not attach unit/source/quality
  metadata. Current provider unit semantics were not independently verified;
  that metadata contract is **BLOCKED**, and consumers must not infer units or
  use this parser as a freshness/tradeability gate.
- Typed request and response DTOs cover the listed Trader and Market Data GET
  families. Read admission is process-local and is not cross-process or durable
  accounting. It does not implement mutation budgets. Exact durable mutation
  reservation counts, WAL ordering, single-writer authority, and production
  write admission remain **BLOCKED** on the #174/#175 gates; `WriterSession::open`
  remains fail-closed. Official endpoint evidence, runtime OAuth/account-reader
  composition, high-level account/order authority integration, and broker
  replay acceptance remain outstanding. No native OS transport acceptance is
  claimed; local checks cover only the current host.

Offline tests use synthetic tokens, synthetic response bodies, and fake
transports or local ephemeral TLS. They do not call Schwab or operate a bot.
