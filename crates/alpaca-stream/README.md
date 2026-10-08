# `alpaca-stream`

`alpaca-stream` is a read-only options market-data adapter. It opens one
allowlisted Alpaca WebSocket per task, uses binary MessagePack frames, injects
credentials through `CredentialProvider`, and separates coalesced quotes,
non-coalesced trades, and protected control events into bounded consumer lanes.
`AlpacaOptionsMarketDataPort` projects those updates into frozen
`trading-core::market-contracts` v1 envelopes. MessagePack float prices use a
shortest-round-tripping decimal projection, are marked with the corresponding
binary-float encoding, and carry the SHA-256 of the raw frame. The adapter does
not claim that the projection is an exact provider decimal token.

The canonical `MarketDataItem` lane can also carry each received application
frame once, with exact bytes and a `ControlMessage`, `DecodedMarketData`,
`UnknownMessage`, `ProviderError`, or `DecodeFailure` disposition. Capture starts
only after successful authentication and includes inbound subscription ACKs and
market application frames; outbound authentication/subscription messages and
authentication diagnostics are never captured. A frame is at most 1 MiB;
outstanding frame leases are bounded to 16 MiB and 1,024 records process-wide.
Normalized events refer to the canonical port generation, frame sequence, exact
frame hash, and 1-based ordinal/count. A durable capture also carries one
`RawFrameCaptureKey` with `(capture UUID, source-local generation, frame
sequence, exact SHA-256)`. Canonical generation and source-local generation are
separate lineages: the former sequences public port records, while the latter
identifies the raw spool frame before projection. A reconnect can reuse a frame
sequence and byte-identical payload, so a durable capture is keyed by all four
source fields rather than by canonical generation alone.

For `DecodedMarketData`, the summary count describes successfully decoded
quote/trade messages. `ControlMessage` and `DecodeFailure` summaries require
zero events, no symbols, and no numeric encoding. A mixed numeric frame is valid
with no homogeneous numeric encoding. `UnknownMessage` and `ProviderError`
summaries may retain successfully parsed market counts and symbols, but the
adapter quarantines the entire frame and emits none of its normalized events.
These records are diagnostics, not complete archive input.

`AlpacaOptionsStream::new` and `AlpacaOptionsMarketDataPort::new` keep the
existing in-memory diagnostic mode. To require pre-decode persistence, inject a
trusted `RawFrameSink` or configure
`AlpacaOptionsMarketDataPort::with_raw_frame_sink_factory`. The runner awaits a
matching pre-decode ACK before decoding and a matching post-decode finalization
ACK before publishing the raw frame or its normalized events. These ACKs bind
the same `RawFrameCaptureKey`, including source-local generation; finalization also
binds the bounded event count, sorted symbols, numeric encoding, disposition,
and its canonical summary hash. Any sink failure, timeout, cancellation, or
ACK mismatch ends the generation with no retry or later frame. Decode failures
are finalized with the original bytes and produce no normalized event. A
factory-owned UUIDv4 is stable across reconnect generations and new after
process restart. An unfinalized record after a crash remains unknown/quarantined.
The ACK types only express the sink implementation's promise: they do not prove
that bytes were fsynced, that the source is entitled or complete, or that Drive
publication occurred. This workspace defines the seam and runner ordering but
does not yet contain the production MDP spool; persistence and recovery remain
the sink owner's responsibility.

The shared v1 subscription control is channel-agnostic and allows at most 32
requested `(channel, symbol)` pairs. Quote/trade overlap counts twice toward
that limit; 33 pairs fail before a socket is opened. Before projection, the
provider's full quote and trade acknowledgement sets are independently compared
with the exact request. The shared v1 `acknowledged` list is emitted only after
both sets match, so a quote-only or trade-only response cannot confirm a request
for both channels. The provider wire message has no request or subscription ID;
the adapter's `alpaca-session-*` and `local-subscription-*` values are local
correlators scoped to the validated local generation, not provider-issued IDs.
Wire bytes and decoder outputs never appear in `Debug` or diagnostics. Raw-frame
leases hold both byte and record budget permits until the final consumer drops
the frame; exceeding a bound terminates the active generation.

This adapter supports only option WebSocket feeds `opra` and `indicative`. It
does not implement stock SIP or historical REST, does not establish the account's
OPRA entitlement, and does not qualify an option symbol or binary-float price as
source-exact. Larger subscriptions require a separately versioned bounded ACK
contract; they are not split into chunks or represented as partial success.

The `offline-test-support` feature is default-off. It exposes one fixed reviewed
MessagePack fixture through `capture_reviewed_fixture`; the in-process script
drives the same session runner, raw sink ACK path, and `EventProjector` used by
the live adapter. Its public input is a closed one-variant fixture enum, a
`RawFrameSinkFactory`, and a cancellation receiver:

```rust
pub async fn capture_reviewed_fixture(
    fixture_id: ReviewedFixtureId,
    sink_factory: std::sync::Arc<dyn broker_ports::RawFrameSinkFactory>,
    cancellation: tokio::sync::watch::Receiver<bool>,
) -> Result<OfflineFixtureCapture, OfflineFixtureError>
```

It accepts no provider URL, credentials, frame bytes, or arbitrary fixture
identifier. `OfflineFixtureCapture` returns a slice of the existing
`broker_ports::MarketDataItem` records and a private-field
`OfflineFixtureReceipt` with read-only getters for fixture identity/input seal,
fixed freshness-clock seconds, local terminal state, runner counts/input digest,
capture counts/bytes, raw frame/finalization digests, and output count. The
freshness-clock getter identifies only the fixed replay classifier reference;
received timestamps remain wall-clock values. The fixture records its true
`alpaca`/`opra` protocol identity and `Unknown` entitlement. Its local
`FixtureEnd` marker is not a provider END, watermark, successful live stream,
or completeness statement. The harness supplies a private fixed freshness clock
matching the fixture trade timestamp so the same projector output is stable on
different host dates; it changes neither provider timestamps nor received times,
and does not establish live freshness.

The receipt is constructed by the SDK and has private fields. It binds the four
scripted provider frames (including the two pre-auth control frames, which never
enter the raw sink) to the frames actually received, plus the two post-auth raw
captures (subscription ACK and trade) and each matching pre-decode/finalization
ACK. Cancellation, EOF, timeout, an early/missing terminal marker, a seal
mismatch, or any missing/mismatched sink ACK returns an error without a receipt.
For the fixed four-frame case, the local marker is emitted only after the
existing projector has delivered the synthetic trade item to the bounded
collector, so session shutdown cannot discard a still-pending trade.
The run and cleanup are inline and bounded; it does not detach a worker. Input,
capture, and output counts/bytes are capped. The receipt digests use
domain-separated SHA-256 with big-endian `u32` counts and lengths. The input
seal uses `eqoboard.alpaca.offline-fixture-input.v1\0`, frame count, then each
frame's length and bytes. The ordered raw-frame digest uses
`eqoboard.alpaca.offline-fixture-raw-frames.v1\0`, capture count, then each
capture UUID, source generation, frame sequence, payload length, and exact
payload. The finalization rollup uses
`eqoboard.alpaca.offline-fixture-finalization.v1\0`, capture count, then each
capture UUID, generation, frame sequence, raw 32-byte frame SHA-256, and raw
32-byte matching finalization-summary SHA-256. The input seal covers connected,
authenticated, subscription-ACK, and trade frames; it excludes outbound auth
and subscription frames and the local `FixtureEnd` marker. Sink ACKs remain the sink's
promise: they do not independently prove `fsync`, provider completeness, or
entitlement. This feature exists only for local synthetic replay tooling.

`alpaca-stream` 是只读期权行情适配器。每个任务只打开一条 allowlist 内的 Alpaca
WebSocket，使用 binary MessagePack frame，通过 `CredentialProvider` 注入凭证，并将
合并报价、不可合并成交和受保护控制事件放入有界消费者队列。

## Protocol boundary / 协议边界

- Alpaca documents the option stream as MessagePack on the wire and says its
  JSON examples are for readability. In addition to the existing bounded
  RFC 3339 string form, quote/trade timestamps decode the standard MessagePack
  Timestamp extension type `-1` using its 4-, 8-, or 12-byte payload format.
- The options endpoint does not support JSON text frames. Any WebSocket text
  frame after authentication is a terminal protocol violation: the generation
  closes without decoding or capturing that frame, retrying, or consuming later
  binary messages. When a raw sink is configured, every post-auth binary
  application message passed to the MessagePack decoder is captured and
  finalized before it can be published; pre-authentication traffic is excluded.
- The configured feed is explicit: `Opra` or `Indicative` is carried on every
  event. `Delayed` remains a distinct enum value but is rejected because the
  reviewed options endpoint does not define a delayed feed path. There is no
  automatic feed downgrade.
- `Connected`, authenticated, and the exact full desired subscription set are
  required before waiting for data. `Ready` is emitted only after a desired
  quote or trade has a provider timestamp within the configured freshness
  window.
- The session compares quote and trade ACK sets separately against the request,
  including when the same option symbol is requested on both channels. A partial
  channel ACK is terminal. The broker port then projects that already-validated
  result into the channel-agnostic shared v1 list. That schema's 32-item cap is
  applied to channel-symbol pairs before connecting; overlaps count twice and
  33 pairs are rejected. The provider protocol has no request ID; shared IDs are
  local generation correlators only. Stale sockets are closed at reconnect and
  cannot acknowledge a later generation.
- Accepted quotes within one local generation are ordered by provider instant,
  including after a quote has left the pending lane. An older provider timestamp
  cannot replace or follow newer evidence. Equal provider timestamps use the
  greater local ingest sequence as a tie-break; this sequence records local
  decode order and is not a provider sequence. Stale, future-dated, older,
  equal-time non-newer, and old-generation quotes are rejected from the quote
  lane and reported through bounded `QuoteDiscarded` control events.
- Stale and future-dated trades remain in the non-coalescing trade lane with
  their freshness label, but neither can move the session to `Ready`. No
  per-contract readiness state is exported by this adapter.
- Every lost socket invalidates pending quote and trade data plus quote timestamp
  watermarks before a retry. Each retry obtains a new local generation,
  reauthenticates, sends the complete desired subscriptions, and waits for a new
  exact acknowledgement and fresh data.
- Quote/trade `t` accepts the existing bounded RFC 3339 string form and the
  standard MessagePack Timestamp extension (`ext` type `-1`, payload length 4,
  8, or 12). Nanoseconds are range-checked and retained exactly. Binary source
  timestamps have no original text: `ProviderTimestamp::as_rfc3339()` returns
  an empty string for them; use `unix_seconds()` and `nanosecond()` for the
  source instant. Freshness and chronology use the numeric instant for both
  wire forms.
- Local generation, local ingest sequence, monotonic receive time, and the
  provider timestamp are separate fields. This adapter does not claim lossless
  upstream resume or that all upstream losses can be detected.
- Quotes coalesce by unique option symbol. Trades never coalesce; a full trade
  lane ends the session with `ConsumerOverloaded`. A full control lane also
  ends the session so control/error events cannot be silently displaced by
  market data.
- Each stream task has at most one open or opening socket. Provider plan limits
  vary and are not hard-coded; provider error code `406` is classified as a
  terminal connection-limit result.
- Connect/authentication/subscription deadlines, queue capacities, MessagePack
  frame size, container sizes, node count, and nesting depth have local caps.
  Retry count and jitter are bounded. Cancellation and consumer closure interrupt
  connect, credential loading, sends, reads, and retry waits; the owned socket is
  closed with a local close deadline.
- A rejected text frame, MessagePack decode failure, unexpected active control
  message, or non-subscribed quote/trade emits one fixed `ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION`
  diagnostic with feed, generation, phase, and a low-cardinality reason code.
  Quote/trade timestamp schema failures also report only the fixed message
  family, field name, MessagePack value kind, and validation problem; they do
  not expose the value or change the existing public decode error for malformed
  frames.
  It never logs frame bytes, decoded values, symbols, unknown type tags, headers,
  provider messages, or credentials. Diagnostics do not change the acceptance,
  acknowledgement, freshness, or retry rules.
- Lane overloads and receiver closures emit `ALPACA_OPTIONS_STREAM_LANE_FAILURE` with
  one fixed lane reason code. The public `ConsumerOverloaded` and `ConsumerClosed`
  results remain unchanged, and a full lane still ends the session.

- 配置必须显式选择 `Opra` 或 `Indicative`，feed 身份会随每条事件传递。`Delayed`
  保留为独立枚举值，但当前审阅的期权 endpoint 没有定义 delayed feed 路径，因此会拒绝；
  不会自动降级 feed。
- 在等待行情前必须收到 `Connected`、认证成功以及与完整期望订阅完全一致的回执。期望
  报价或成交的 provider 时间戳处于配置的新鲜度窗口内后，才会发送 `Ready`。
- 同一代次内，所有已接收报价均按 provider 时间点排序，已从待处理队列取走的报价也保留时序
  水位。更早的 provider 时间戳不能覆盖或晚于更新证据。provider 时间相等时，使用更大的本地
  接收序号作为 tie-break；该序号记录本地解码顺序，不是 provider sequence。过期、未来时间、
  较旧时间、相同时间但本地序号未更新、旧代次的报价都不会进入 quote lane，并通过有界
  `QuoteDiscarded` 控制事件报告。
- 过期或未来时间的成交仍保留在不合并的 trade lane 并附带新鲜度标签，但都不能使会话进入
  `Ready`。本适配器不提供逐合约 readiness 状态。
- 每次 socket 丢失时，都会先清除报价和成交待处理数据及报价时间水位再重试。每次重试都会
  取得新本地代次、重新认证、发送完整期望订阅，并等待新的精确回执和新鲜数据。
- 报价/成交的 `t` 同时接受原有有界 RFC 3339 字符串和标准 MessagePack Timestamp 扩展
  （`ext` 类型 `-1`，载荷长度为 4、8 或 12）。纳秒分量会严格校验并原样保留。二进制来源
  没有原始文本，因此 `ProviderTimestamp::as_rfc3339()` 对此返回空字符串；应用应使用
  `unix_seconds()` 与 `nanosecond()` 读取来源时间。两种 wire 格式的 freshness 与 chronology
  均使用精确数值时间点。
- 本地代次、本地接收序号、单调时钟接收时间和 provider 时间点分别保存。本适配器不声称上游
  无损续传，也不声称能检测全部上游丢失。
- Alpaca 文档说明期权流在线路上使用 MessagePack，JSON 示例仅为便于阅读。除原有有界
  RFC 3339 字符串外，报价/成交时间字段还会按 MessagePack Timestamp 标准解码 `-1` 扩展及其
  4、8、12 字节载荷格式。
- 期权 endpoint 不支持 JSON text frame。认证后的任何 WebSocket text frame 都是终止性协议错误：
  当前代次关闭，不解码或捕获该帧、不重试，也不消费后续 binary 消息。配置 raw sink 时，所有
  传给 MessagePack decoder 的认证后 binary 应用消息都会先捕获并完成定稿，再允许发布；认证前流量不纳入捕获。
- 报价按唯一期权代码合并。成交不会合并；成交 lane 满时以 `ConsumerOverloaded` 结束会话。
  控制 lane 满时也结束会话，确保控制/错误事件不会被行情静默挤出。
- 每个流任务最多持有一条打开或正在建立的 socket。Provider 套餐限制会变化，因此不硬编码
  额度；provider 错误码 `406` 会分类为终止性连接上限结果。
- 连接/认证/订阅期限、队列容量、MessagePack frame/容器/节点/深度均有本地上限。重试
  次数和抖动有界。取消或消费者关闭会中断连接、凭证加载、发送、读取和重试等待；任务会在
  本地关闭期限内关闭其拥有的 socket。
- 被拒绝的 text frame、MessagePack 解码失败、活跃流中的意外控制消息或未订阅的报价/成交，
  会输出一条固定 `ALPACA_OPTIONS_STREAM_PROTOCOL_VIOLATION` 诊断，包含 feed、代次、阶段和
  低基数原因码。报价/成交时间字段的数据结构错误还会报告固定的消息类型、字段名、MessagePack
  值类型和校验问题；日志不会输出字段值或改变 malformed frame 的公开解码错误。日志不会输出 frame 字节、
  解码值、合约代码、未知 type tag、headers、provider 原文或凭据。这些诊断不会改变消息接纳、
  订阅回执、新鲜度或重试规则。
- 行情队列过载或消费者关闭时会输出固定 `ALPACA_OPTIONS_STREAM_LANE_FAILURE` 及 lane 原因码。
  公共 `ConsumerOverloaded` / `ConsumerClosed` 结果不变，队列满仍会结束会话。

## Decoder dependency review / Decoder 依赖审查

The decoder is pinned to `rmpv = 1.3.1` in the workspace manifest and lockfile.
The reviewed crates.io registry archive SHA-256 and published checksum are both
`7a4e1d4b9b938a26d2996af33229f0ca0956c652c1375067f0b45291c1df8417`. The package
declares `MIT`, Rust `1.85`, and is not yanked. Its source repository is
[`3Hren/msgpack-rust`](https://github.com/3Hren/msgpack-rust). The parser first
walks the byte stream without allocating, enforcing this crate's bounds, then
decodes with an explicit depth ceiling. This is a source/version/checksum review,
not a claim that every downstream license policy or security audit passes.

Decoder 固定到 workspace manifest 与 lockfile 中的 `rmpv = 1.3.1`。已审查的 crates.io
registry archive SHA-256 与发布 checksum 一致，均为
`7a4e1d4b9b938a26d2996af33229f0ca0956c652c1375067f0b45291c1df8417`。该 package 声明
`MIT`、Rust `1.85`，当前未被 yanked。源码仓库为
[`3Hren/msgpack-rust`](https://github.com/3Hren/msgpack-rust)。解析器先无分配遍历输入字节并
实施本 crate 的边界，再用显式深度上限解码。该记录说明了来源、版本和 checksum 审查，
不代表所有下游 license policy 或安全审计都会通过。

## Upstream SDK boundary / 上游 SDK 边界

This crate owns the WebSocket protocol boundary because the pinned Alpaca SDK
does not include options streaming. It has no REST client or Alpaca SDK
dependency. The REST adapter is a separate crate in this workspace and must use
the pinned SDK's low-level transport with redirects disabled.

本 crate 负责 WebSocket 协议边界，因为固定版本 Alpaca SDK 不包含期权行情流。本 crate 不含 REST
客户端或 Alpaca SDK 依赖。REST adapter 位于本 workspace 的独立 crate，并必须使用固定 SDK 的低层
transport 且关闭重定向。

## Verification boundary / 验证边界

Tests use synthetic MessagePack frames, fake sockets/sinks, and Tokio's paused time.
They do not contact Alpaca, invoke REST, read local credentials, perform actual
durable persistence, submit orders, calculate volatility, or run strategy code. A passing
unit suite is local protocol/adapter evidence only; production entitlement,
provider behavior, and native Windows/macOS operation remain unverified.

测试只使用合成 MessagePack frame、假 socket/sink 和 Tokio paused time。它们不会连接 Alpaca、调用
REST、读取本地凭证、证明真实耐久存储、提交订单、计算波动率或运行策略。没有注入可信 sink 时，原始
frame 只在内存中沿有序 lane 传递；注入 sink 时，runner 会等待解码前 ACK 和解码后定稿 ACK。它不捕获
发出的认证/订阅消息及认证诊断；frame 上限为 1 MiB，全局未释放预算为 16 MiB / 1,024 条。公开
`generation` 是 canonical port 代次；耐久 `capture_key` 另保留 `(capture UUID, 来源本地代次, 帧序号,
SHA-256)`，因此 reconnect 后即使帧序号和字节相同也不会与 spool 原始记录冲突。`DecodedMarketData`
的 `event_count` 记录成功解码的 quote/trade；`ControlMessage` 和 `DecodeFailure` 必须为零事件、无 symbol、无数值编码。
混合数值编码可用 `None` 表示。`UnknownMessage` / `ProviderError` 可能保留已解析行情数量和 symbol，但整帧进入 quarantine，且不发布任何规范化事件。单元测试通过只构成本地
协议/适配器证据；生产 entitlement、provider 行为和原生 Windows/macOS 运行仍未验证。

`offline-test-support` feature 默认关闭，只回放一个固定、已审阅的 MessagePack fixture，并通过
同一 session runner、raw sink 两阶段 ACK 和 `EventProjector`。API 不接收 URL、凭证、任意帧或任意
fixture ID；其参数是闭合的 `ReviewedFixtureId`（当前只有 `AlpacaOpraTradeV1`）、
`Arc<dyn broker_ports::RawFrameSinkFactory>` 和 `watch::Receiver<bool>`。返回值只包含现有
`broker_ports::MarketDataItem` 与 SDK 私有字段构造、没有公开构造/反序列化入口的
`OfflineFixtureReceipt`；receipt 仅提供只读 getter，
读取固定 fixture 标识/输入封印、回放 freshness clock 秒数、本地终止状态、runner 收帧数和摘要、捕获帧数/字节、
原始帧/定稿摘要及输出数。该 clock 仅用于固定 provider 时间的陈旧分类；received time 仍来自 wall clock。
provider/feed 仍是 `alpaca`/`opra`，entitlement 保留 `Unknown`。本地 `FixtureEnd` 只是
测试控制标记，不是 provider END、水位、真实行情完成或 entitlement 证据。receipt 只在四个固定输入帧
全部由 runner 收到、两条认证后捕获帧的两阶段 ACK 均匹配且摘要一致时返回；取消、EOF、超时、提前或
缺失终止标记、seal/ACK 不匹配都只返回错误。正常四帧回放时，本地 marker 还会等待现有 projector 将合成
成交写入有界 collector，避免会话关闭丢弃尚未消费的成交。会话与清理在同一 future 内有界完成，不 detach worker。
离线 runner 使用与 fixture 成交时间相同的私有固定 freshness clock，避免宿主机日期改变投影结果；它不改写
provider 时间戳或接收时间，也不构成真实实时性证据。

摘要可跨 crate 复算：所有摘要先写入各自 ASCII 域分隔符（含末尾 `NUL`），计数和长度都使用无符号
32 位大端序。输入域 `eqoboard.alpaca.offline-fixture-input.v1\0` 后依次写入帧数，再为每帧写入字节长度和
原始 MessagePack 字节。捕获域 `eqoboard.alpaca.offline-fixture-raw-frames.v1\0` 后写入捕获数，再按捕获顺序
写入 UUID 16 字节、source generation 64 位大端序、frame sequence 64 位大端序、payload 长度和原始
payload。定稿域 `eqoboard.alpaca.offline-fixture-finalization.v1\0` 后写入捕获数，再按相同顺序写入 UUID、
generation、sequence、frame SHA-256 原始 32 字节及匹配的定稿摘要 SHA-256 原始 32 字节。三者均对整个编码
序列计算 SHA-256；十六进制格式只用于 receipt 展示，不参与后两种摘要的编码。

## References / 参考资料

- [Alpaca real-time option data](https://docs.alpaca.markets/us/docs/real-time-option-data)
- [Alpaca WebSocket market-data stream](https://docs.alpaca.markets/us/docs/streaming-market-data)
- [MessagePack Timestamp extension specification](https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type)
- [`rmpv` 1.3.1 on crates.io](https://crates.io/crates/rmpv/1.3.1)

- [Alpaca 实时期权行情](https://docs.alpaca.markets/us/docs/real-time-option-data)
- [Alpaca WebSocket 行情流](https://docs.alpaca.markets/us/docs/streaming-market-data)
- [MessagePack Timestamp 扩展规范](https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type)
- [crates.io 上的 `rmpv` 1.3.1](https://crates.io/crates/rmpv/1.3.1)
