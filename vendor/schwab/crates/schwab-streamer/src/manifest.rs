//! Fixed service and field manifests for the currently supported Schwab feeds.
//! 定义静态服务清单和受支持的 Streamer 服务集合。

/// The fixed number of Streamer services modeled by this offline core.
/// 中文摘要：当前固定 Streamer manifest 中服务枚举项的数量。
pub const SERVICE_COUNT: usize = 3;

/// Services with independent desired, acknowledged, and readiness state.
/// 中文摘要：仅包含固定 manifest 中独立维护期望 key、ACK 和 readiness 的三个服务。
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StreamerService {
    /// Account order/activity hints.
    /// 账户活动服务，用于订单与活动提示订阅。
    AcctActivity,
    /// Equity level-one quotes.
    /// 美股 Level One 行情订阅服务。
    LevelOneEquities,
    /// Option level-one quotes.
    /// 期权 Level One 行情订阅服务。
    LevelOneOptions,
}

/// Stable order used for deterministic replay plans.
/// 中文摘要：所有受支持 Streamer 服务的固定 manifest，顺序也定义确定性重放顺序。
pub const SERVICE_MANIFESTS: [ServiceManifest; SERVICE_COUNT] = [
    ServiceManifest {
        service: StreamerService::AcctActivity,
        name: "ACCT_ACTIVITY",
        fields: "0,1,2,3",
    },
    ServiceManifest {
        service: StreamerService::LevelOneEquities,
        name: "LEVELONE_EQUITIES",
        fields: "0,45,46,51,52",
    },
    ServiceManifest {
        service: StreamerService::LevelOneOptions,
        name: "LEVELONE_OPTIONS",
        fields: "0,2,3,38",
    },
];

/// Wire name and field set for one Streamer service.
/// 中文摘要：一个服务的固定 wire 名称、字段集合和服务枚举值，用于确定性命令编码与重放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceManifest {
    service: StreamerService,
    name: &'static str,
    fields: &'static str,
}

impl ServiceManifest {
    /// Returns the service represented by this manifest.
    /// 中文摘要：返回该 manifest 描述的 Streamer 服务。
    #[must_use]
    pub const fn service(self) -> StreamerService {
        self.service
    }

    /// Returns the exact service name used in Streamer command frames.
    /// 中文摘要：返回该 Streamer 服务或命令的 wire 名称。
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Returns the fixed comma-separated field list for this service.
    /// 中文摘要：返回该服务固定的逗号分隔 wire 字段列表。
    #[must_use]
    pub const fn fields(self) -> &'static str {
        self.fields
    }
}

impl StreamerService {
    /// Returns the service's fixed manifest.
    /// 中文摘要：返回该服务的编译期固定 manifest 项。
    #[must_use]
    pub const fn manifest(self) -> ServiceManifest {
        SERVICE_MANIFESTS[self.index()]
    }

    /// Returns this service's deterministic index in [`SERVICE_MANIFESTS`].
    /// 中文摘要：返回访问该服务 manifest 项所用的稳定数组索引。
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::AcctActivity => 0,
            Self::LevelOneEquities => 1,
            Self::LevelOneOptions => 2,
        }
    }
}
