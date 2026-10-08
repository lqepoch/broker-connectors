# IBKR 只读期权合约目录

本 crate 提供受界限保护的 IBKR 合约目录查询。它将完整 OCC 期权标识、明确指定的非 `SMART` exchange 和明确指定的 currency 组成 provider 查询；收到完整结果流并确认 `ContractDataEnd` 后，返回共享 `OptionInstrumentCandidate` 和 adapter-owned IBKR provider identity。

provider `conId` 是来源记录身份，不能替代 `OptionInstrumentKey`，也不代表账户权限、行情权限或订单路由资格。缺失的 deliverable、exercise style 和 settlement type 保持 `Unknown`，因此候选合约不能通过 `qualify()`。

## 能力边界

- 仅实现精确 OCC 期权 contract-details catalog lookup。
- 该 provider-specific API 当前不实现通用 `InstrumentCatalogPort`：共享 query 尚无完整 OCC/right/strike 与显式 exchange/currency 字段，因此本 crate 不枚举宽泛期权链，也不推断路由默认值。
- SDK client 是 crate 私有字段，不会通过公共类型或方法返回。
- 查找按单个 session 串行执行；SDK 自动重连关闭。
- API endpoint 必须是 loopback `SocketAddr`，不允许由此 adapter 连接任意远端主机。
- connect、lookup、cancel/drain、disconnect 每个阶段的 timeout 都必须大于零且不超过 60 秒；lookup 和 drain 的 deadline 使用 checked arithmetic。
- 请求流逐行处理，最多接受 16 条响应记录，内部未读队列上限为 32 条。
- 成功结果必须在 `ContractDataEnd` 后确认；多条结果、无效 provider identity、超限、断链或未确认取消均失败关闭。
- 未能确认请求结束时，adapter 会 poison 并在配置时限内停止和释放 SDK client；该实例不再接受查询。调用方取消 lookup 时，SDK 的订阅 Drop 只会排队发送取消而不能证明 native END；RAII guard 会同步 poison 并释放 adapter 持有的 SDK client，使连接结束且不能被复用。
- 不实现 `MarketDataPort`、`BrokerReadPort`、`BrokerEventPort`、`ExecutionPort`、账户读取、symbol 行情订阅或历史行情读取。
- SDK request、handshake、notice、错误和路由诊断只输出固定分类、消息 ID 与字节数，不打印原始 payload。生产连接同时禁用双向 `MessageRecorder` 和 inbound `RawFrameTap`；`IBAPI_RECORDING_DIR` 与 `IBAPI_RAW_CAPTURE_DIR` 都不能启用持久化。仅合成单测可通过显式 `TempDir` 构造器记录 synthetic bytes。

查询只能使用有效完整 OCC symbol；exchange 和 currency 都必须显式传入。`SMART` 与隐式 `USD` 不会被 adapter 使用。SDK 搜索请求中的 `conId = 0` 是 IBKR 搜索 sentinel，不能作为返回身份。返回行需精确匹配 OCC local symbol、标的、到期、right、strike、exchange、currency，并包含正数 provider `conId`。

loopback-only 是本阶段目录适配器的连接边界，不代表支持或验证了 IBKR 的全部部署方式。远程网关接入需要单独审查可信网络配置与访问权限。

取消路径依据 pinned SDK 的实现：`Client::drop` 调用 `request_shutdown_sync`（`vendor/ibapi/src/client/async.rs`），消息总线设置 shutdown signal，`process_messages` dispatcher 监听该 signal 并退出（`vendor/ibapi/src/transport/async.rs`）。SDK 没有独立的常驻 writer task；请求在调用路径中通过 `AsyncTcpSocket` 的 writer mutex 直接写入（`vendor/ibapi/src/transport/async/io.rs`）。fake-wire abort 测试确认释放 SDK owner 后 loopback peer 收到 EOF；该结果不构成真实网关测试。

## 上游 SDK

vendored `ibapi` 的源码来自 `wboayue/rust-ibapi` commit
`3e73f2f1cfac151c10e403a3e7d779272134445f`，package metadata 为 `5.0.0`、MIT、MSRV 1.88；该源码 commit 与 `v5.0.0` tag `f43c64682ab24b822dcd2892ff4208eac81813dc` 不同。源码 hash 与局部 manifest 处理记录在 [`vendor/ibapi/UPSTREAM.md`](../../vendor/ibapi/UPSTREAM.md)。

SDK 官方语言支持列表不包含 Rust，因此此处使用的是社区维护的非官方 TWS API 客户端。vendor `src/` 以可审计局部修改记录绑定上游与目标 hash：五个上游文件只去除尾部空白；连接、错误和路由诊断只输出固定分类、消息 ID 或长度；生产 `MessageRecorder` 与 `RawFrameTap` 均固定禁用且忽略对应环境变量；单测仅通过显式临时目录处理合成字节。局部 Cargo manifest 去掉上游 workspace、开发依赖和示例目标。

## 离线验证

`tests/catalog_fake_wire.rs` 只连接到同进程启动的 `127.0.0.1:0` synthetic gateway。它验证 protobuf search terms、provider row identity、native END、歧义拒绝、row cap、收到取消后的 END 确认、超时、调用方取消后 peer EOF/poison、显式断开和断链处理。fake handshake 中的账户字符串为 `SYNTHETIC_ACCOUNT`，没有真实 TWS/Gateway、账户或订单操作。

合并到 workspace 后运行：

```bash
CARGO_BUILD_JOBS=2 cargo +1.99.0 test -p ibkr-read --locked
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy -p ibkr-read --all-targets --locked -- -D warnings
```

代码和 fake-wire 结果不证明真实 IBKR 端点、权限、provider 目录完整性或 Paper/Live 能力。真实 TWS/Gateway 连接、账户操作、订单操作和 provider 行情均未运行。
