//! User-preference and Streamer bootstrap response projections.
//! 定义用户偏好和 Streamer 启动响应投影。

use serde_json::Value;

use super::ReadResponseError;
use super::common::{
    UnknownFields, boolean, object, optional_array, required_string, string, unknown_fields,
};

/// Object or array shape returned by `GET /userPreference`.
/// 中文摘要：保持 /userPreference 返回单对象或数组的原始外层形态，并保留数组顺序。
#[derive(Clone, PartialEq)]
pub enum UserPreferencesResponse {
    /// A single preference object.
    /// broker 返回单个用户偏好对象。
    One(Box<UserPreference>),
    /// A preference array, retained in response order.
    /// broker 返回用户偏好数组。
    Many(Vec<UserPreference>),
}

impl UserPreferencesResponse {
    /// Returns whether the source response used the array form.
    /// 中文摘要：报告已校验响应采用数组形式还是单对象形式。
    pub const fn is_array(&self) -> bool {
        matches!(self, Self::Many(_))
    }

    /// Returns the first preference, matching Node's convenience selection.
    /// 中文摘要：单对象时返回该偏好，数组时返回首项，与兼容便捷选择规则一致。
    pub fn first(&self) -> Option<&UserPreference> {
        match self {
            Self::One(preference) => Some(preference.as_ref()),
            Self::Many(preferences) => preferences.first(),
        }
    }
}

/// One user preference object with additive fields retained opaquely.
/// 中文摘要：/userPreference 的偏好对象，包含账户、Streamer 启动元数据和市场数据权限。
#[derive(Clone, PartialEq)]
pub struct UserPreference {
    /// Preference account rows.
    /// 中文摘要：用户偏好返回的账户信息。
    pub accounts: Option<Vec<UserPreferenceAccount>>,
    /// Streamer connection parameters.
    /// 中文摘要：Stream­er 启动所需元数据；该值不单独授权连接目标。
    pub streamer_info: Option<Vec<StreamerInfo>>,
    /// Market-data offer permissions.
    /// 中文摘要：该用户偏好的市场数据权限条目列表。
    pub offers: Option<Vec<OfferInfo>>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Account preference metadata; the account number is sensitive and Debug is redacted.
/// 中文摘要：偏好响应中的账户显示信息；账号数据属于敏感信息，Debug 输出会脱敏。
#[derive(Clone, PartialEq)]
pub struct UserPreferenceAccount {
    /// Account number as returned by the broker.
    /// 中文摘要：broker 返回的账户编号。
    pub account_number: String,
    /// Whether this is the primary account.
    /// 中文摘要：该账户是否为当前用户的主账户；缺失不表示否。
    pub primary_account: Option<bool>,
    /// Broker account type.
    /// 中文摘要：账户类型。
    pub account_type: Option<String>,
    /// User-facing account nickname.
    /// 中文摘要：用户界面显示的账户昵称。
    pub nickname: Option<String>,
    /// User-facing account color.
    /// 中文摘要：用户界面使用的账户颜色值。
    pub account_color: Option<String>,
    /// Display account identifier.
    /// 中文摘要：用户界面显示的账户标识；不替代账户哈希或账户授权。
    pub display_account_id: Option<String>,
    /// Automatic position-effect setting.
    /// 中文摘要：用户偏好中的自动持仓作用设置；它不是订单 authority。
    pub auto_position_effect: Option<bool>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Required connection values returned inside a preference.
///
/// URL validation mirrors the current Node `z.string().url()` syntax check. It
/// does not authorize a destination or establish transport safety; any future
/// socket adapter must independently restrict the endpoint before connecting.
/// 中文摘要：Streamer LOGIN 所需的 URL 与关联元数据；URL 语法有效不代表网络目标已获授权。
#[derive(Clone, PartialEq)]
pub struct StreamerInfo {
    /// Streamer socket URL accepted by Node's URL syntax schema. This does not
    /// establish that it is an authorized or safe network destination.
    /// 中文摘要：Streamer 连接 URL 的语法校验结果；此字段不授权网络目标，连接前仍需独立限制 endpoint。
    pub streamer_socket_url: String,
    /// Schwab client customer identifier.
    /// 中文摘要：Streamer LOGIN 使用的客户 ID。
    pub schwab_client_customer_id: String,
    /// Schwab client correlation identifier.
    /// 中文摘要：Streamer 命令关联标识。
    pub schwab_client_correl_id: String,
    /// Schwab client channel identifier.
    /// 中文摘要：Streamer channel 标识。
    pub schwab_client_channel: String,
    /// Schwab client function identifier.
    /// 中文摘要：Streamer function 标识。
    pub schwab_client_function_id: String,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Market-data permissions returned with user preferences.
/// 中文摘要：broker 返回的市场数据权限标记与说明，不单独授予本地访问权限。
#[derive(Clone, PartialEq)]
pub struct OfferInfo {
    /// Level 2 permission flag.
    /// 中文摘要：broker 报告的 Level 2 行情权限标记；缺失不表示已获授权。
    pub level2_permissions: Option<bool>,
    /// Market-data permission description.
    /// 中文摘要：broker 返回的市场数据权限说明字符串。
    pub market_data_permission: Option<String>,
    /// Additive broker fields retained without interpretation.
    /// 中文摘要：保留未建模的附加响应字段；这些字段不会建立快照 authority。
    pub unknown_fields: UnknownFields,
}

/// Builds the typed object/array preference DTO after schema validation.
pub(super) fn project_user_preferences(
    value: &Value,
) -> Result<UserPreferencesResponse, ReadResponseError> {
    match value {
        Value::Object(_) => Ok(UserPreferencesResponse::One(Box::new(
            project_user_preference(value)?,
        ))),
        Value::Array(preferences) => Ok(UserPreferencesResponse::Many(
            preferences
                .iter()
                .map(project_user_preference)
                .collect::<Result<_, _>>()?,
        )),
        _ => Err(ReadResponseError::SchemaViolation {
            field: "userPreference",
        }),
    }
}

fn project_user_preference(value: &Value) -> Result<UserPreference, ReadResponseError> {
    let fields = object(value, "userPreference")?;
    Ok(UserPreference {
        accounts: optional_array(
            fields,
            "accounts",
            "userPreference.accounts",
            project_user_preference_account,
        )?,
        streamer_info: optional_array(
            fields,
            "streamerInfo",
            "userPreference.streamerInfo",
            project_streamer_info,
        )?,
        offers: optional_array(
            fields,
            "offers",
            "userPreference.offers",
            project_offer_info,
        )?,
        unknown_fields: unknown_fields(fields, &["accounts", "streamerInfo", "offers"]),
    })
}

fn project_user_preference_account(
    value: &Value,
) -> Result<UserPreferenceAccount, ReadResponseError> {
    let fields = object(value, "userPreference.accounts[]")?;
    Ok(UserPreferenceAccount {
        account_number: required_string(
            fields,
            "accountNumber",
            "userPreference.accounts[].accountNumber",
        )?,
        primary_account: boolean(
            fields,
            "primaryAccount",
            "userPreference.accounts[].primaryAccount",
        )?,
        account_type: string(fields, "type", "userPreference.accounts[].type")?,
        nickname: string(fields, "nickName", "userPreference.accounts[].nickName")?,
        account_color: string(
            fields,
            "accountColor",
            "userPreference.accounts[].accountColor",
        )?,
        display_account_id: string(
            fields,
            "displayAcctId",
            "userPreference.accounts[].displayAcctId",
        )?,
        auto_position_effect: boolean(
            fields,
            "autoPositionEffect",
            "userPreference.accounts[].autoPositionEffect",
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "accountNumber",
                "primaryAccount",
                "type",
                "nickName",
                "accountColor",
                "displayAcctId",
                "autoPositionEffect",
            ],
        ),
    })
}

fn project_streamer_info(value: &Value) -> Result<StreamerInfo, ReadResponseError> {
    let fields = object(value, "userPreference.streamerInfo[]")?;
    Ok(StreamerInfo {
        streamer_socket_url: required_string(
            fields,
            "streamerSocketUrl",
            "streamerInfo.streamerSocketUrl",
        )?,
        schwab_client_customer_id: required_string(
            fields,
            "schwabClientCustomerId",
            "streamerInfo.schwabClientCustomerId",
        )?,
        schwab_client_correl_id: required_string(
            fields,
            "schwabClientCorrelId",
            "streamerInfo.schwabClientCorrelId",
        )?,
        schwab_client_channel: required_string(
            fields,
            "schwabClientChannel",
            "streamerInfo.schwabClientChannel",
        )?,
        schwab_client_function_id: required_string(
            fields,
            "schwabClientFunctionId",
            "streamerInfo.schwabClientFunctionId",
        )?,
        unknown_fields: unknown_fields(
            fields,
            &[
                "streamerSocketUrl",
                "schwabClientCustomerId",
                "schwabClientCorrelId",
                "schwabClientChannel",
                "schwabClientFunctionId",
            ],
        ),
    })
}

fn project_offer_info(value: &Value) -> Result<OfferInfo, ReadResponseError> {
    let fields = object(value, "userPreference.offers[]")?;
    Ok(OfferInfo {
        level2_permissions: boolean(
            fields,
            "level2Permissions",
            "userPreference.offers[].level2Permissions",
        )?,
        market_data_permission: string(
            fields,
            "mktDataPermission",
            "userPreference.offers[].mktDataPermission",
        )?,
        unknown_fields: unknown_fields(fields, &["level2Permissions", "mktDataPermission"]),
    })
}
