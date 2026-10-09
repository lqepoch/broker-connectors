# alpaca-rest-read

`alpaca-rest-read` adapts the pinned community Alpaca Rust SDK to the shared
`market-contracts` quote/trade payloads. It is a read-only software adapter;
constructing a client does not issue a request. A caller must explicitly
invoke a supported read method.

The implementation supports latest option quotes, latest option trades, finite
option snapshot windows, and exactly one page per explicit stock-bars call.
Options requests require an explicit `opra` or `indicative` choice and at most
100 validated OCC symbol candidates. Snapshot pages are limited to 1,000
points, four pages per operation, and 512 bytes per continuation cursor. The
stock-bars facade fixes feed intent to `sip`, adjustment to `raw`, sort to
`asc`, currency to `USD`, and `asof` to `-`; it accepts one uppercase symbol,
one of five supported timeframes, a UTC inclusive start/end range, and 1 through
1,000 bars. A returned page token is opaque, redacted, capped at 512 bytes, and
bound to those exact query parameters. The facade calls the SDK's single-page
`stocks().bars` method and never calls `bars_all` or loops over pages.

Request deadlines default to 8 seconds, a complete finite options page window
defaults to 20 seconds, and retry waiting defaults to 500 ms with at most one
retry. Hard limits are 20 seconds per request, 60 seconds per window, 2 seconds
of retry wait, and two in-flight requests per client. The patched HTTP transport
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
or the account's entitlement. A 403 is reported as provider rejection and a
429 remains rate limited after the SDK's bounded retry; neither condition
falls back to IEX or another feed. Local response-observation time is not
historical point-in-time availability. These REST observations do not carry a provider
continuity sequence, trusted watermark, or completion receipt. The adapter
does not qualify an OCC candidate as a complete broker instrument.

Historical option bars and trades remain unsupported here. Alpaca's
[historical option bars](https://docs.alpaca.markets/us/v1.4.2/reference/optionbars)
and [historical option trades](https://docs.alpaca.markets/us/reference/optiontrades)
request structs do not provide an explicit feed selector for those operations,
so this crate will not label an unqualified result OPRA. The stock facade uses
Alpaca's [historical stock bars endpoint](https://docs.alpaca.markets/us/v1.4.2/reference/stockbarsingle-1)
only for one explicitly requested page at a time; absence of a next-page token
is not a provider completeness, entitlement, or research-qualification receipt.
The existing Go market-data ingestor remains the stock historical collection
and publication owner. Trusted watermarks are also unsupported:
the reviewed REST responses provide no continuity evidence for one. The
capability report returns these unsupported states explicitly.

The decimal-preservation test uses a synthetic normalized `BarsResponse`
fixture from the pinned SDK model. It does not claim coverage of the
single-symbol endpoint's raw HTTP response shape.

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
