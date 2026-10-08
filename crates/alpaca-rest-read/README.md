# alpaca-rest-read

`alpaca-rest-read` adapts the pinned community Alpaca Rust SDK to the shared
`market-contracts` quote/trade payloads. It is a read-only software adapter;
constructing a client does not issue a request. A caller must explicitly
invoke a supported read method.

The implementation supports latest option quotes, latest option trades, and
finite option snapshot windows. Requests require an explicit `opra` or
`indicative` choice and at most 100 validated OCC symbol candidates. Snapshot
pages are limited to 1,000 points, four pages per operation, and 512 bytes per
continuation cursor. Request deadlines default to 8 seconds, a complete page
window defaults to 20 seconds, and retry waiting defaults to 500 ms with at
most one retry. Hard limits are 20 seconds per request, 60 seconds per window,
2 seconds of retry wait, and two in-flight requests per client. The patched
HTTP transport rejects response bodies larger than 8 MiB, including chunked
responses. SDK JSON number and string decimals are parsed exactly; values that
would require rounding, underflow, or exceed the decimal coefficient range are
rejected before mapping.

Credentials are supplied explicitly with `AlpacaRestCredentials`; this crate
does not read environment variables or load dotenv files. Long-lived owned
credential strings are held in zeroizing buffers and their `Debug` output is
redacted. HTTP request construction creates transient `HeaderValue` and request
copies that cannot be zeroized by this adapter. The vendored SDK pins the
production origin to `https://data.alpaca.markets` and disables redirects so
credentials are never forwarded to an arbitrary configured host.

Every observation uses the shared `MarketEventV1` quote or trade payload and
keeps provider timestamp separate from local receive time. The requested feed
is retained as request metadata; `MarketDataSourceV1.feed` and entitlement stay
`unknown` because a request parameter does not prove the feed returned or the
account's entitlement. These REST observations do not carry a provider
continuity sequence, a trusted watermark, or a complete-stream receipt. The
adapter does not qualify an OCC candidate as a complete broker instrument.

Historical option bars and trades are unsupported here. Alpaca's [historical
bars](https://docs.alpaca.markets/us/v1.4.2/reference/optionbars) and
[historical trades](https://docs.alpaca.markets/us/reference/optiontrades)
endpoint references and the selected SDK request structs do not provide an
explicit feed selector for those operations, so this crate will not label an
unqualified result OPRA. Trusted watermarks are also unsupported:
the reviewed REST responses provide no continuity evidence for one. The
capability report returns these unsupported states explicitly.

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
