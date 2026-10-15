# alpaca-rest-read

`alpaca-rest-read` adapts the pinned community Alpaca Rust SDK to the shared
`market-contracts` quote/trade payloads. It is a read-only software adapter;
constructing a client does not issue a request. A caller must explicitly
invoke a supported read method.

The implementation supports latest option quotes, latest option trades, finite
option snapshot windows, one-page stock-bars reads, and finite stock-bars page
windows.
Options requests require an explicit `opra` or `indicative` choice and at most
100 validated OCC symbol candidates. Snapshot pages are limited to 1,000
points, four pages per operation, and 512 bytes per continuation cursor. The
stock-bars facade defaults feed intent to `sip` and also accepts explicit
`boats`; adjustment is fixed to `raw`, sort defaults to `asc`, currency is fixed
to `USD`, and `asof` to `-`. A request accepts one uppercase symbol, one of five supported
timeframes, a UTC inclusive start/end range, and 1 through 1,000 bars per page.
Use `new_with_feed_and_sort` to request `asc` or `desc`; the existing
constructors continue to default to `asc`. Pages and windows retain provider
order, and the cursor binds the selected sort direction. A descending query
therefore starts from recent bars and continues toward older bars; consumers
that display chronological rows should reorder the bounded result by timestamp.
The single-page method returns one provider page. The window method reads at
most four pages and returns a continuation cursor when that cap is reached.
The versioned cursor can be serialized for caller round-tripping, is query-bound
on use, redacted in `Debug`, and validated on deserialization. It carries
provider continuation state, not source or completeness evidence.

Request deadlines default to 8 seconds, a complete finite page window defaults
to 20 seconds, and retry waiting defaults to 500 ms with at most one retry.
Hard limits are 20 seconds per request, 60 seconds per window, 2 seconds of
retry wait, and two in-flight requests per client. A 429 is retried once within
the configured budget and remains `RateLimited` if the retry is exhausted.
There is no per-client quota counter; shared market-data admission belongs to
the consuming read-budget owner. The patched HTTP transport
rejects declared response lengths above 8 MiB and incrementally rejects chunked
responses above 8 MiB before allocating the complete body or deserializing JSON.
SDK JSON number and string decimals are parsed exactly; values that would
require rounding, underflow, or exceed the decimal coefficient range are
rejected before mapping.

Credentials are supplied explicitly with `AlpacaRestCredentials`; this crate
does not read environment variables or load dotenv files. Long-lived owned
credential strings are held in zeroizing buffers and their `Debug` output is
redacted. HTTP request construction creates transient `HeaderValue` and request
copies that cannot be zeroized by this adapter. The vendored SDK pins the
production origin to `https://data.alpaca.markets` and disables redirects so
credentials are never forwarded to an arbitrary configured host.

Option observations use the shared `MarketEventV1` quote or trade payload and
keep provider timestamp separate from local receive time. The stock-bars page
returns a bounded adapter observation with exact decimal OHLC/VWAP and integer
volume; it is not the shared MDP minute-bar schema. Requested feed remains
separate from source evidence: `MarketDataSourceV1.feed` and entitlement stay
`unknown` because a request parameter does not prove which feed was returned
or the account's entitlement. A page returned by the read client after SDK and
page validation also carries an opaque, in-process
`AlpacaStockBarsQueryAccessWitness` for its exact API selector and typed response
observation. Its getters expose the contract ID, fixed HTTPS origin and path,
symbol, requested feed, raw adjustment, timeframe, inclusive range, sort, page
limit, fixed currency/as-of selectors, incoming and next-cursor SHA-256, local
observation time, row count, and typed-response SHA-256. Parser-created fixture
pages do not carry it. The witness does not
echo or verify the effective feed, account plan, entitlement, license, or history
completeness; an empty page records query success with zero rows, and a missing
next-page cursor is not a completeness claim. Its SHA-256 covers a stable
SDK-typed response projection, not the HTTP wire body. This SDK wrapper does not
expose the exact HTTP status or request-id. HTTP 401, 403, and 429 map to distinct fixed
authentication, access-denied, and rate-limit errors. Other provider 4xx errors
remain generic because status alone cannot identify whether a request failed
due to plan, time range, or another rule. No error triggers a fallback to IEX
or another feed. Local response-observation time is not historical point-in-time
availability. These REST observations do not carry a provider continuity
sequence, trusted watermark, or completion receipt. The adapter
does not qualify an OCC candidate as a complete broker instrument.

The `documented_stock_bars_access` profile describes official plan rules but
never inspects credentials or verifies the active account subscription. For
Basic, SIP history can be requested only when `end` is at least 15 minutes old,
and historical BOATS data is 15-minute delayed. Algo Trader Plus removes the
documented 15-minute historical restriction for SIP and BOATS. The historical
bars endpoint accepts `sip` and `boats`; it does not accept `overnight`. Alpaca
documents `overnight` for some latest-data requests and instructs callers to
use `boats` for historical overnight data. Returned source and entitlement
remain `unknown` even after a successful request. These rules are described in
the official [Market Data API plans](https://docs.alpaca.markets/us/docs/about-market-data-api),
[historical stock bars reference](https://docs.alpaca.markets/us/reference/stockbars),
[24/5 trading](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api), and
[Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq).

Historical option bars and trades remain unsupported here. Alpaca's
[historical option bars](https://docs.alpaca.markets/us/v1.4.2/reference/optionbars)
and [historical option trades](https://docs.alpaca.markets/us/reference/optiontrades)
request structs do not provide an explicit feed selector for those operations,
so this crate will not label an unqualified result OPRA. The stock facade uses
Alpaca's historical stock bars endpoint for explicit single pages and bounded
windows. A window with a continuation cursor is partial. Absence of a
next-page token means only that traversal reached the end of provider response
pages; it is not an archive, entitlement, or research-qualification receipt.
The existing Go market-data ingestor remains the stock historical collection
and publication owner. Trusted watermarks are also unsupported:
the reviewed REST responses provide no continuity evidence for one. The
capability report returns these unsupported states explicitly.

The decimal-preservation test uses a synthetic normalized `BarsResponse`
fixture from the pinned SDK model. It does not claim coverage of the
single-symbol endpoint's raw HTTP response shape, and the parser-only fixture
path does not mint a query-access witness.

The upstream is [`wmzhai/alpaca-rust` v0.33.3 at commit
`d91be382e3e9d25c52c24e626e78702532d24ba2`](https://github.com/wmzhai/alpaca-rust/tree/d91be382e3e9d25c52c24e626e78702532d24ba2).
Alpaca classifies it as a community-made SDK, not an Alpaca-maintained or
Alpaca-supported SDK. It is licensed `MIT OR Apache-2.0`; exact package paths,
source tree, changes, and license hashes are recorded in
[`vendor/alpaca-rust/UPSTREAM.md`](../../vendor/alpaca-rust/UPSTREAM.md) and the
root `SOURCE-MANIFEST.json`. Rust 1.99.0 is required by the SDK. Other workspace
packages retain their 1.98.1 MSRV declarations.

Tests use synthetic typed responses and localhost-only fake transport. No
provider request, OAuth flow, account call, or real market data was used.
