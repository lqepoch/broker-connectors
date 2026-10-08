//! Fixed-route HTTP ports, bounded response types, and the read-only REST client.
//! 定义固定路由的 HTTP 端口、有界响应类型和只读 REST 客户端。

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

use crate::routes::ReadRequestError;

/// Fixed Schwab REST origin. Production transports must not accept an
/// operator-supplied origin; tests may use a private loopback transport later.
/// 中文摘要：固定生产 Schwab API origin；请求端点不能由调用方替换。
pub const SCHWAB_API_ROOT: &str = "https://api.schwabapi.com";

/// Maximum response body accepted by the client contract.
/// 中文摘要：单个 REST 响应 body 的最大保留字节数；超限响应会被拒绝。
pub const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;
/// Maximum response header count accepted by the client contract.
/// 中文摘要：REST 响应可接受的最大 header 数量。
pub const MAX_HEADER_COUNT: usize = 64;
/// Maximum response header-name length accepted by the client contract.
/// 中文摘要：单个 REST 响应 header 名称的最大字节数。
pub const MAX_HEADER_NAME_BYTES: usize = 128;
/// Maximum individual response header-value length accepted by the client contract.
/// 中文摘要：单个 REST 响应 header 值的最大字节数。
pub const MAX_HEADER_VALUE_BYTES: usize = 4_096;
/// Maximum combined response header name/value bytes accepted by the contract.
/// 中文摘要：REST 响应所有 header 名和值合计的最大字节数。
pub const MAX_RESPONSE_HEADER_BYTES: usize = 16_384;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const TOKEN_MAX_BYTES: usize = 8_192;
const ACCOUNT_NUMBERS_PATH: &str = "/trader/v1/accounts/accountNumbers";
const USER_PREFERENCES_PATH: &str = "/trader/v1/userPreference";

/// Boxed future used by the narrow token and HTTP ports without a runtime dependency.
/// 中文摘要：供 token 与 HTTP 注入端口共享的异步返回类型，不绑定特定 runtime。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Application-supplied read ordering hint. This value expresses relative
/// urgency only; it does not define a broker quota or admission policy.
/// 应用提供的读取排序提示；只表达相对紧急程度，不定义券商配额或准入策略。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadPriority {
    /// Account-authority or similarly time-sensitive reads.
    /// 账户 authority 或类似时效敏感读取。
    Urgent,
    /// Follow-up reads that should precede ordinary refresh work.
    /// 应优先于普通刷新任务的后续读取。
    Followup,
    /// Sell-side reads that should precede ordinary refresh work.
    /// 应优先于普通刷新任务的卖出侧读取。
    Sell,
    /// Ordinary refresh reads.
    /// 普通刷新读取。
    Refresh,
}

/// Stable admission failures exposed by the read-admission port.
/// 读取准入端口公开的稳定失败类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadAdmissionError {
    /// The adapter's bounded waiter queue has no free slot.
    /// 适配器的有界等待队列已满。
    QueueFull,
    /// The adapter's local policy rejected the request.
    /// 适配器的本地策略拒绝请求。
    PolicyRejected,
    /// The adapter entered a sticky fail-closed state.
    /// 适配器进入 sticky fail-closed 状态。
    FailClosed,
    /// The adapter's owner task or runtime is unavailable.
    /// 适配器的 owner task 或 runtime 不可用。
    RuntimeUnavailable,
    /// The maximum caller wait expired.
    /// 调用方允许的最大等待时间已过。
    RequestDeadlineExpired,
}

impl ReadAdmissionError {
    /// Returns the stable machine-readable error code.
    /// 返回稳定的机器可读错误代码。
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::QueueFull => "READ_ADMISSION_QUEUE_FULL",
            Self::PolicyRejected => "READ_ADMISSION_POLICY_REJECTED",
            Self::FailClosed => "READ_ADMISSION_FAIL_CLOSED",
            Self::RuntimeUnavailable => "READ_ADMISSION_RUNTIME_UNAVAILABLE",
            Self::RequestDeadlineExpired => "READ_ADMISSION_DEADLINE_EXPIRED",
        }
    }
}

impl fmt::Display for ReadAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ReadAdmissionError {}

/// SDK-owned boundary for caller-provided local admission policy.
///
/// Implementations must reject before token acquisition when admission fails.
/// They may inspect only the bounded `Retry-After` values supplied after a
/// 429 response; they must never retry the request that received that response.
/// 由调用方提供的本地准入策略端口；失败时必须在获取 token 前拒绝，也不得重试已收到 429 的请求。
pub trait ReadAdmissionPort: Send + Sync {
    /// Waits for one admission for at most `maximum_wait`.
    /// 在 `maximum_wait` 内等待一次读取准入。
    fn admit(
        &self,
        priority: ReadPriority,
        maximum_wait: Duration,
    ) -> BoxFuture<'_, Result<(), ReadAdmissionError>>;

    /// Records bounded rate-limit metadata from a 429 response.
    /// 记录 429 响应中的有界限流元数据。
    fn observe_rate_limit_headers<'a>(
        &'a self,
        values: &'a [&'a [u8]],
    ) -> BoxFuture<'a, Result<(), ReadAdmissionError>>;
}

/// Opaque bearer token with a redacted formatter.
///
/// The owned string is zeroized on drop. This does not clear copies made by
/// providers, HTTP libraries, or callers; callers must not log the value.
/// 中文摘要：只在内存中持有并脱敏的 bearer token。
pub struct AccessToken(Zeroizing<String>);

impl AccessToken {
    /// Validates the RFC 6750 `b64token` character set and length.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    ///
    /// # Errors
    /// Returns [`TokenProviderError::InvalidToken`] when the token is empty, over its byte limit, or contains a disallowed character.
    pub fn new(value: impl Into<String>) -> Result<Self, TokenProviderError> {
        let value = Zeroizing::new(value.into());
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > TOKEN_MAX_BYTES {
            return Err(TokenProviderError::InvalidToken);
        }

        let first_padding = bytes.iter().position(|byte| *byte == b'=');
        let token_part = first_padding.map_or(bytes, |index| &bytes[..index]);
        let padding_part = first_padding.map_or(&[][..], |index| &bytes[index..]);
        let token_chars_valid = token_part.iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        });
        let padding_valid = padding_part.iter().all(|byte| *byte == b'=');

        if !token_chars_valid || !padding_valid || token_part.is_empty() {
            return Err(TokenProviderError::InvalidToken);
        }

        Ok(Self(value))
    }

    fn authorization_value(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessToken([REDACTED])")
    }
}

/// Fixed token-provider failure categories; source errors are never retained.
/// 中文摘要：token 提供器只暴露不可用或 token 无效两类脱敏失败，不保留上游错误文本。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenProviderError {
    /// The token source could not provide a token.
    /// 凭证提供器当前不可用。
    Unavailable,
    /// The token source returned a token outside the accepted bearer format.
    /// 提供器返回的 token 格式无效。
    InvalidToken,
}

impl fmt::Display for TokenProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "REST_TOKEN_UNAVAILABLE",
            Self::InvalidToken => "REST_TOKEN_INVALID",
        })
    }
}

impl std::error::Error for TokenProviderError {}

/// Narrow asynchronous token source. This port does not define OAuth refresh,
/// persistence, or authorization lifecycle semantics.
/// 中文摘要：定义 accesstoken提供器 的注入边界；具体实现仍须遵守类型说明中的安全约束。
pub trait AccessTokenProvider: Send + Sync {
    /// Loads one bearer token for a request; refresh and persistence remain the provider’s responsibility.
    /// 每次请求获取一个 bearer token；刷新与持久化由该端口之外的 provider 负责。
    fn access_token(&self) -> BoxFuture<'_, Result<AccessToken, TokenProviderError>>;
}

/// The only HTTP verb exposed by this read-only client slice.
/// 中文摘要：当前客户端唯一支持的 HTTP method；仅 GET 可进入只读传输边界。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpMethod {
    /// The sole HTTP method admitted by this read-only client.
    /// 只允许的只读 GET method。
    Get,
}

impl HttpMethod {
    /// Returns `GET`, the only HTTP verb admitted by this client.
    /// 返回该已校验值的文本表示。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
        }
    }
}

/// A fixed read route with a prevalidated, bounded request target. Its
/// formatter reveals the route family but never dynamic path/query values.
/// 中文摘要：固定 allowlist 中的只读路由；格式化输出隐藏动态路径和查询值。
#[derive(Clone, Eq, PartialEq)]
pub struct ReadEndpoint {
    route_name: &'static str,
    target: Cow<'static, str>,
}

impl ReadEndpoint {
    #[allow(non_upper_case_globals)]
    /// Fixed allow-listed route for reading account-number to account-hash mappings.
    /// 读取账户编号到账户哈希映射的固定白名单路由。
    pub const AccountNumbers: Self = Self::static_route("account-numbers", ACCOUNT_NUMBERS_PATH);
    #[allow(non_upper_case_globals)]
    /// Fixed allow-listed route for reading the authenticated user’s preferences.
    /// 读取已认证用户偏好的固定白名单路由。
    pub const UserPreferences: Self = Self::static_route("user-preferences", USER_PREFERENCES_PATH);

    const fn static_route(route_name: &'static str, target: &'static str) -> Self {
        Self {
            route_name,
            target: Cow::Borrowed(target),
        }
    }

    pub(crate) fn allowlisted(route_name: &'static str, target: String) -> Self {
        Self {
            route_name,
            target: Cow::Owned(target),
        }
    }

    pub(crate) fn path(&self) -> &str {
        self.target.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn url(&self) -> String {
        format!("{SCHWAB_API_ROOT}{}", self.path())
    }

    /// Returns the stable route label used in diagnostics without exposing dynamic path or query values.
    /// 返回用于诊断的稳定路由标签，不暴露动态路径或查询值。
    #[must_use]
    pub fn route_name(&self) -> &'static str {
        self.route_name
    }
}

impl fmt::Debug for ReadEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadEndpoint")
            .field("route", &self.route_name)
            .field("target", &"[REDACTED]")
            .finish()
    }
}

/// Redirect behavior required from every transport implementation.
/// 中文摘要：传输必须禁用 HTTP 自动重定向，避免固定 origin 的只读请求转向其他目标。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedirectPolicy {
    /// Automatic redirects are disabled to keep requests on the fixed allowlisted origin.
    /// 禁用自动重定向，避免请求被转发到未授权目标。
    Disabled,
}

/// One bounded-contract request. It contains no user-selected URL or mutation
/// payload, and its formatter redacts the bearer credential.
/// 中文摘要：绑定 allowlisted GET、bearer token 与读取准入 gate 的请求；Debug 会脱敏凭证。
pub struct HttpRequest {
    endpoint: ReadEndpoint,
    token: AccessToken,
    read_admission: Arc<dyn ReadAdmissionPort>,
    timeout: Duration,
}

impl HttpRequest {
    #[cfg(test)]
    pub(crate) fn get(
        endpoint: ReadEndpoint,
        token: AccessToken,
        read_admission: Arc<dyn ReadAdmissionPort>,
    ) -> Self {
        Self::get_with_timeout(endpoint, token, read_admission, REQUEST_TIMEOUT)
    }

    pub(crate) fn get_with_timeout(
        endpoint: ReadEndpoint,
        token: AccessToken,
        read_admission: Arc<dyn ReadAdmissionPort>,
        timeout: Duration,
    ) -> Self {
        Self {
            endpoint,
            token,
            read_admission,
            timeout,
        }
    }

    /// Returns the fixed `GET` method bound to this read request/response.
    /// 返回该只读请求绑定的固定 `GET` 方法。
    #[must_use]
    pub const fn method(&self) -> HttpMethod {
        HttpMethod::Get
    }

    /// Returns the fixed allowlisted route selected for this request.
    /// 返回该请求选择的固定 allowlist 端点。
    #[must_use]
    pub const fn endpoint(&self) -> &ReadEndpoint {
        &self.endpoint
    }

    #[cfg(test)]
    pub(crate) fn url(&self) -> String {
        self.endpoint.url()
    }

    /// Returns the finite deadline applied to this transport request.
    /// 返回应用于该传输请求的有限期限。
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns `Disabled`; transports must reject redirects instead of following them.
    /// 返回 `Disabled`；传输实现必须拒绝重定向，不能跟随至其他目标。
    #[must_use]
    pub const fn redirect_policy(&self) -> RedirectPolicy {
        RedirectPolicy::Disabled
    }

    /// Returns the fixed `application/json` response media type.
    /// 返回固定的 `application/json` 响应媒体类型。
    #[must_use]
    pub const fn accept(&self) -> &'static str {
        "application/json"
    }

    /// Provides the validated bearer token only for the duration of a
    /// transport callback. The callback must not retain or log it.
    /// 中文摘要：仅在闭包执行期间借用 bearer token；调用方不得记录或保留副本。
    pub fn with_bearer_token<T>(&self, callback: impl for<'a> FnOnce(&'a str) -> T) -> T {
        callback(self.token.authorization_value())
    }

    /// Returns the maximum response-body size the transport may retain.
    /// 返回传输实现可保留的响应正文最大字节数。
    #[must_use]
    pub const fn max_response_body_bytes(&self) -> usize {
        MAX_RESPONSE_BODY_BYTES
    }

    /// Records a 429 response head before the transport inspects or reads its
    /// body. Only Retry-After values are supplied; the gate retains neither
    /// credentials nor raw header/body data.
    /// 中文摘要：仅对 HTTP 429，在处理正文前把 Retry-After 值交给共享准入 gate；其他状态不操作，也不保留凭证或正文。
    ///
    /// # Errors
    /// Returns the shared admission gate error if it rejects the bounded rate-limit metadata.
    pub async fn observe_response_head(
        &self,
        status: u16,
        retry_after_values: &[&[u8]],
    ) -> Result<(), ReadAdmissionError> {
        if status != 429 {
            return Ok(());
        }
        self.read_admission
            .observe_rate_limit_headers(retry_after_values)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn observe_synthetic_response_head(
        &self,
        response: &HttpResponse,
    ) -> Result<(), HttpTransportError> {
        let retry_after_values = response
            .headers()
            .filter(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        self.observe_response_head(response.status(), &retry_after_values)
            .await
            .map_err(|_| HttpTransportError::RateLimitObservation)
    }
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("method", &self.method())
            .field("route", &self.endpoint.route_name())
            .field("timeout", &self.timeout())
            .field("redirect_policy", &self.redirect_policy())
            .field("bearer_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Transport failure stage without retaining a provider's raw error string.
/// 中文摘要：传输失败的固定分类，不携带响应 body、凭证或底层错误字符串。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpTransportError {
    /// The transport could not establish the connection.
    /// 底层传输无法建立连接。
    Connect,
    /// The request could not be sent through the transport.
    /// 底层传输无法发送请求。
    Send,
    /// The response could not be read from the transport.
    /// 底层传输无法读取响应。
    Receive,
    /// A bounded connect or request deadline expired.
    /// 连接或请求的有界期限已到。
    Timeout,
    /// The server attempted a redirect, which this client refuses.
    /// 服务端尝试重定向，而客户端会拒绝跟随。
    Redirect,
    /// The response exceeded a configured header bound.
    /// 响应超出配置的响应头上限。
    HeaderLimit,
    /// The response body exceeded the configured byte bound.
    /// 响应正文超出配置的字节上限。
    BodyLimit,
    /// The transport returned a response that failed bounded validation.
    /// 传输返回的响应未通过有界校验。
    InvalidResponse,
    /// The response rate-limit metadata could not be recorded safely.
    /// 无法安全记录响应中的限流元数据。
    RateLimitObservation,
    /// The resolved target did not satisfy the fixed-origin request policy.
    /// 解析后的目标不符合固定 origin 请求策略。
    Configuration,
}

/// Conservative classification of whether an HTTP request could have reached
/// the provider. Only failures known to happen before request dispatch are
/// classified as definitely unsent; all later or ambiguous failures must be
/// reconciled by callers before retrying a mutation.
/// 中文摘要：区分请求是否已交给传输端；不确定结果不能被当作未发送。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestDispatchCertainty {
    /// The local transport can prove that the request was not sent.
    /// 传输边界确认请求未发送。
    DefinitelyNotSent,
    /// The available transport evidence cannot prove whether the request was sent.
    /// 当前证据无法确认请求是否已发送。
    MayHaveBeenSent,
}

impl HttpTransportError {
    /// Returns a stable machine-readable error code without exposing upstream error text.
    /// 返回稳定的机器可读错误代码，不暴露上游错误文本。
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Connect => "REST_TRANSPORT_CONNECT_FAILED",
            Self::Send => "REST_TRANSPORT_SEND_FAILED",
            Self::Receive => "REST_TRANSPORT_RECEIVE_FAILED",
            Self::Timeout => "REST_TRANSPORT_TIMEOUT",
            Self::Redirect => "REST_TRANSPORT_REDIRECT_REJECTED",
            Self::HeaderLimit => "REST_RESPONSE_HEADERS_REJECTED",
            Self::BodyLimit => "REST_RESPONSE_BODY_TOO_LARGE",
            Self::InvalidResponse => "REST_RESPONSE_INVALID",
            Self::RateLimitObservation => "REST_RATE_LIMIT_ADMISSION_FAILED",
            Self::Configuration => "REST_TRANSPORT_CONFIGURATION_FAILED",
        }
    }

    /// Reports the most conservative send state supported by the transport
    /// failure category. Connection establishment and local configuration
    /// failures occur before an HTTP request can be sent.
    /// 中文摘要：报告本地是否能确定请求未发送。
    #[must_use]
    pub const fn request_dispatch_certainty(self) -> RequestDispatchCertainty {
        match self {
            Self::Connect | Self::Configuration => RequestDispatchCertainty::DefinitelyNotSent,
            Self::Send
            | Self::Receive
            | Self::Timeout
            | Self::Redirect
            | Self::HeaderLimit
            | Self::BodyLimit
            | Self::RateLimitObservation
            | Self::InvalidResponse => RequestDispatchCertainty::MayHaveBeenSent,
        }
    }
}

impl fmt::Display for HttpTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for HttpTransportError {}

/// A single transport attempt. Implementations must enforce the body bound
/// while streaming, before allocating an unbounded response body. The response
/// constructor is a second line of defense, not a replacement for that rule.
/// After receiving response headers, implementations must call
/// [`HttpRequest::observe_response_head`] exactly once before applying header
/// policy or reading the body, passing at most `MAX_HEADER_COUNT + 1`
/// Retry-After values. This preserves the shared 429 cooldown even if later
/// response validation or body receipt fails.
/// 中文摘要：定义 HTTP传输 的注入边界；具体实现仍须遵守类型说明中的安全约束。
pub trait HttpTransport: Send + Sync {
    /// Performs one bounded attempt for the allowlisted GET. Implementations enforce body limits while streaming and observe 429 headers before reading the body.
    /// 执行一次有界 allowlist GET；实现须在流式接收时限制正文，并在读取正文前记录 429 响应头。
    fn send(&self, request: HttpRequest)
    -> BoxFuture<'_, Result<HttpResponse, HttpTransportError>>;
}

/// Bounded raw HTTP response. Header values and body are deliberately omitted
/// from Debug output while remaining available to the caller.
/// 中文摘要：传输端返回的状态、受限响应头和有界 body；构造或保留时须遵守字节上限。
pub struct HttpResponse {
    status: u16,
    headers: Vec<ResponseHeader>,
    body: Vec<u8>,
}

impl HttpResponse {
    /// Validates response metadata and payload bounds. A real streaming
    /// transport must apply these caps during receipt as well as here.
    /// 中文摘要：校验输入并构造该类型的值；具体格式、大小上限和脱敏边界见类型说明。
    ///
    /// # Errors
    /// Returns [`ResponseLimitError`] when the status, headers, or body violate the response bounds.
    pub fn new(
        status: u16,
        headers: impl IntoIterator<Item = (String, Vec<u8>)>,
        body: Vec<u8>,
    ) -> Result<Self, ResponseLimitError> {
        if !(100..=599).contains(&status) {
            return Err(ResponseLimitError::InvalidStatus);
        }
        if body.len() > MAX_RESPONSE_BODY_BYTES {
            return Err(ResponseLimitError::BodyTooLarge);
        }

        let mut parsed_headers = Vec::new();
        let mut total_header_bytes = 0usize;
        for (name, value) in headers {
            if parsed_headers.len() == MAX_HEADER_COUNT {
                return Err(ResponseLimitError::TooManyHeaders);
            }
            if name.is_empty() || name.len() > MAX_HEADER_NAME_BYTES || !is_header_name(&name) {
                return Err(ResponseLimitError::InvalidHeaderName);
            }
            if value.len() > MAX_HEADER_VALUE_BYTES || !is_header_value(&value) {
                return Err(ResponseLimitError::InvalidHeaderValue);
            }
            total_header_bytes = total_header_bytes
                .checked_add(name.len())
                .and_then(|length| length.checked_add(value.len()))
                .ok_or(ResponseLimitError::HeadersTooLarge)?;
            if total_header_bytes > MAX_RESPONSE_HEADER_BYTES {
                return Err(ResponseLimitError::HeadersTooLarge);
            }

            parsed_headers.push(ResponseHeader {
                name: name.to_ascii_lowercase(),
                value,
            });
        }

        Ok(Self {
            status,
            headers: parsed_headers,
            body,
        })
    }

    /// Returns the broker HTTP status code.
    /// 返回 broker 报告的 HTTP 状态码。
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Iterates response headers after count, name, value, and total-byte validation.
    /// 迭代通过数量、名称、值和总字节数校验的响应头。
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_slice()))
    }

    /// Borrows the response body, capped by `MAX_RESPONSE_BODY_BYTES`.
    /// 借用受 `MAX_RESPONSE_BODY_BYTES` 限制的响应正文。
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .field("header_values", &"[REDACTED]")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

struct ResponseHeader {
    name: String,
    value: Vec<u8>,
}

/// Stable response-construction errors that never include payload data.
/// 中文摘要：响应头或响应体违反固定大小限制时返回的安全错误分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseLimitError {
    /// The response status is outside the supported HTTP status range.
    /// 响应状态码超出支持的 HTTP 状态范围。
    InvalidStatus,
    /// The response contains more headers than the configured count bound.
    /// 响应头数量超过配置上限。
    TooManyHeaders,
    /// A response header name is invalid or too long.
    /// 响应头名称无效或过长。
    InvalidHeaderName,
    /// A response header value is invalid or too long.
    /// 响应头值无效或过长。
    InvalidHeaderValue,
    /// Combined response header bytes exceed the configured bound.
    /// 响应头名称和值的总字节数超过配置上限。
    HeadersTooLarge,
    /// The response body exceeds the configured byte bound.
    /// 响应正文超过配置的字节上限。
    BodyTooLarge,
}

impl fmt::Display for ResponseLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidStatus => "REST_RESPONSE_INVALID_STATUS",
            Self::TooManyHeaders => "REST_RESPONSE_TOO_MANY_HEADERS",
            Self::InvalidHeaderName => "REST_RESPONSE_INVALID_HEADER_NAME",
            Self::InvalidHeaderValue => "REST_RESPONSE_INVALID_HEADER_VALUE",
            Self::HeadersTooLarge => "REST_RESPONSE_HEADERS_TOO_LARGE",
            Self::BodyTooLarge => "REST_RESPONSE_BODY_TOO_LARGE",
        })
    }
}

impl std::error::Error for ResponseLimitError {}

/// Caller-facing response with the fixed endpoint and one-attempt metadata.
/// 中文摘要：带端点、请求次数、HTTP 状态及受限响应数据的只读结果。
pub struct RestResponse {
    endpoint: ReadEndpoint,
    response: HttpResponse,
}

impl RestResponse {
    /// Returns the fixed `GET` method bound to this read request/response.
    /// 返回该只读请求绑定的固定 `GET` 方法。
    #[must_use]
    pub const fn method(&self) -> HttpMethod {
        HttpMethod::Get
    }

    /// Returns the fixed allowlisted route selected for this request.
    /// 返回该请求选择的固定 allowlist 端点。
    #[must_use]
    pub const fn endpoint(&self) -> &ReadEndpoint {
        &self.endpoint
    }

    #[cfg(test)]
    pub(crate) fn url(&self) -> String {
        self.endpoint.url()
    }

    /// Returns the number of transport attempts; this client never retries automatically.
    /// 返回传输尝试次数；该 client 不自动重试。
    #[must_use]
    pub const fn attempts(&self) -> u8 {
        1
    }

    /// Returns the broker HTTP status code.
    /// 返回 broker 报告的 HTTP 状态码。
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.response.status()
    }

    /// Iterates response headers after count, name, value, and total-byte validation.
    /// 迭代通过数量、名称、值和总字节数校验的响应头。
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.response.headers()
    }

    /// Borrows the response body, capped by `MAX_RESPONSE_BODY_BYTES`.
    /// 借用受 `MAX_RESPONSE_BODY_BYTES` 限制的响应正文。
    #[must_use]
    pub fn body(&self) -> &[u8] {
        self.response.body()
    }
}

impl fmt::Debug for RestResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestResponse")
            .field("method", &self.method())
            .field("route", &self.endpoint.route_name())
            .field("attempts", &self.attempts())
            .field("response", &self.response)
            .finish()
    }
}

/// Fixed REST failure categories. Non-2xx responses retain their bounded
/// status, headers (including Retry-After), and body for explicit caller policy.
/// 中文摘要：REST 请求、传输、HTTP 状态及响应限制错误的脱敏分类。
pub enum RestError {
    /// The typed read request failed validation.
    /// 类型化只读请求未通过校验。
    Request(ReadRequestError),
    /// The shared local read-admission gate rejected the request.
    /// 共享本地读取准入 gate 拒绝了该请求。
    Admission(ReadAdmissionError),
    /// The injected token provider did not return a valid bearer token.
    /// 注入的 token 提供器未返回有效 bearer token。
    Token(TokenProviderError),
    /// The bounded HTTP transport failed before a usable response was returned.
    /// 有界 HTTP 传输未能返回可用响应。
    Transport(HttpTransportError),
    /// The server returned a non-success status with bounded response metadata.
    /// 服务端返回非成功状态及有界响应元数据。
    HttpStatus(RestResponse),
}

impl RestError {
    /// Reports whether the failed operation could have reached Schwab. A
    /// non-2xx response is deliberately conservative: its status does not
    /// prove that a mutation had no side effect.
    /// 中文摘要：报告本地是否能确定请求未发送。
    #[must_use]
    pub const fn request_dispatch_certainty(&self) -> RequestDispatchCertainty {
        match self {
            Self::Request(_) | Self::Admission(_) | Self::Token(_) => {
                RequestDispatchCertainty::DefinitelyNotSent
            }
            Self::Transport(error) => error.request_dispatch_certainty(),
            Self::HttpStatus(_) => RequestDispatchCertainty::MayHaveBeenSent,
        }
    }

    /// Returns a stable machine-readable error code without exposing upstream error text.
    /// 返回稳定的机器可读错误代码，不暴露上游错误文本。
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Admission(error) => error.code(),
            Self::Request(ReadRequestError::InvalidPathIdentifier) => {
                "REST_READ_PATH_IDENTIFIER_INVALID"
            }
            Self::Request(ReadRequestError::InvalidNumericIdentifier) => {
                "REST_READ_NUMERIC_IDENTIFIER_INVALID"
            }
            Self::Request(ReadRequestError::InvalidQueryKey) => "REST_READ_QUERY_KEY_INVALID",
            Self::Request(ReadRequestError::InvalidQueryValue) => "REST_READ_QUERY_VALUE_INVALID",
            Self::Request(ReadRequestError::InvalidList) => "REST_READ_LIST_INVALID",
            Self::Request(ReadRequestError::DuplicateQueryParameter) => {
                "REST_READ_QUERY_PARAMETER_DUPLICATE"
            }
            Self::Request(ReadRequestError::ConflictingQueryParameter) => {
                "REST_READ_QUERY_PARAMETER_CONFLICT"
            }
            Self::Request(ReadRequestError::TooManyQueryParameters) => {
                "REST_READ_QUERY_PARAMETER_LIMIT"
            }
            Self::Request(ReadRequestError::TargetTooLong) => "REST_READ_TARGET_TOO_LONG",
            Self::Token(TokenProviderError::Unavailable) => "REST_TOKEN_UNAVAILABLE",
            Self::Token(TokenProviderError::InvalidToken) => "REST_TOKEN_INVALID",
            Self::Transport(HttpTransportError::Connect) => "REST_TRANSPORT_CONNECT_FAILED",
            Self::Transport(HttpTransportError::Send) => "REST_TRANSPORT_SEND_FAILED",
            Self::Transport(HttpTransportError::Receive) => "REST_TRANSPORT_RECEIVE_FAILED",
            Self::Transport(HttpTransportError::Timeout) => "REST_TRANSPORT_TIMEOUT",
            Self::Transport(HttpTransportError::Redirect) => "REST_TRANSPORT_REDIRECT_REJECTED",
            Self::Transport(HttpTransportError::HeaderLimit) => "REST_RESPONSE_HEADERS_REJECTED",
            Self::Transport(HttpTransportError::BodyLimit) => "REST_RESPONSE_BODY_TOO_LARGE",
            Self::Transport(HttpTransportError::InvalidResponse) => "REST_RESPONSE_INVALID",
            Self::Transport(HttpTransportError::RateLimitObservation) => {
                "REST_RATE_LIMIT_ADMISSION_FAILED"
            }
            Self::Transport(HttpTransportError::Configuration) => {
                "REST_TRANSPORT_CONFIGURATION_FAILED"
            }
            Self::HttpStatus(_) => "REST_HTTP_STATUS",
        }
    }

    /// Returns the raw REST response when the result type contains one.
    /// 返回该错误中保留的原始 REST 响应（若错误类别包含响应）。
    #[must_use]
    pub fn response(&self) -> Option<&RestResponse> {
        match self {
            Self::HttpStatus(response) => Some(response),
            Self::Request(_) | Self::Admission(_) | Self::Token(_) | Self::Transport(_) => None,
        }
    }

    /// Returns the number of transport attempts; this client never retries automatically.
    /// 返回传输尝试次数；该 client 不自动重试。
    #[must_use]
    pub const fn attempts(&self) -> u8 {
        match self {
            Self::Request(_) | Self::Admission(_) | Self::Token(_) => 0,
            Self::Transport(_) | Self::HttpStatus(_) => 1,
        }
    }
}

impl fmt::Debug for RestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => formatter
                .debug_tuple("RestError::Admission")
                .field(error)
                .finish(),
            Self::Request(error) => formatter
                .debug_tuple("RestError::Request")
                .field(error)
                .finish(),
            Self::Token(error) => formatter
                .debug_tuple("RestError::Token")
                .field(error)
                .finish(),
            Self::Transport(error) => formatter
                .debug_tuple("RestError::Transport")
                .field(error)
                .finish(),
            Self::HttpStatus(response) => formatter
                .debug_tuple("RestError::HttpStatus")
                .field(response)
                .finish(),
        }
    }
}

impl fmt::Display for RestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpStatus(response) => {
                write!(formatter, "{}:{}", self.code(), response.status())
            }
            _ => formatter.write_str(self.code()),
        }
    }
}

impl std::error::Error for RestError {}

/// Concrete read-only Schwab client over explicit token and transport ports.
///
/// The client makes exactly one transport call per operation. It exposes no
/// Preview, place, replace, cancel, or other mutation operation.
/// 中文摘要：通过固定 GET 路由和共享 admission gate 执行只读 REST 请求的客户端。
pub struct SchwabRestClient<P, T> {
    token_provider: P,
    transport: T,
    read_admission: Arc<dyn ReadAdmissionPort>,
}

impl<P, T> SchwabRestClient<P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Creates a read-only client from injected token and transport ports and the shared local read-admission gate.
    /// 校验输入并构造该类型的值；具体长度、格式和脱敏边界见类型说明。
    pub fn new(
        token_provider: P,
        transport: T,
        read_admission: Arc<dyn ReadAdmissionPort>,
    ) -> Self {
        Self {
            token_provider,
            transport,
            read_admission,
        }
    }

    /// Fetches account-number/hash data as a raw bounded response; use the typed read facade for schema validation.
    /// 读取账户编号映射响应。
    ///
    /// # Errors
    /// Returns [`RestError`] when request validation, admission, token lookup, transport, or the HTTP response fails.
    pub async fn account_numbers(&self) -> Result<RestResponse, RestError> {
        self.get_endpoint(ReadEndpoint::AccountNumbers, ReadPriority::Urgent)
            .await
    }

    /// Fetches user-preference data as a raw bounded response; use the typed read facade for schema validation.
    /// 读取用户偏好 DTO，不从该响应直接授权网络目标。
    ///
    /// # Errors
    /// Returns [`RestError`] when request validation, admission, token lookup, transport, or the HTTP response fails.
    pub async fn user_preferences(&self) -> Result<RestResponse, RestError> {
        self.get_endpoint(ReadEndpoint::UserPreferences, ReadPriority::Urgent)
            .await
    }

    pub(crate) async fn get_endpoint(
        &self,
        endpoint: ReadEndpoint,
        priority: ReadPriority,
    ) -> Result<RestResponse, RestError> {
        self.get_endpoint_with_timeout(endpoint, priority, REQUEST_TIMEOUT)
            .await
    }

    pub(crate) async fn get_endpoint_with_timeout(
        &self,
        endpoint: ReadEndpoint,
        priority: ReadPriority,
        timeout: Duration,
    ) -> Result<RestResponse, RestError> {
        let deadline =
            tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or(RestError::Admission(
                    ReadAdmissionError::RequestDeadlineExpired,
                ))?;
        let prepared = tokio::time::timeout_at(deadline, async {
            self.read_admission
                .admit(priority, timeout)
                .await
                .map_err(RestError::Admission)?;
            self.token_provider
                .access_token()
                .await
                .map_err(RestError::Token)
        })
        .await
        .map_err(|_| RestError::Admission(ReadAdmissionError::RequestDeadlineExpired))??;
        let request = HttpRequest::get_with_timeout(
            endpoint.clone(),
            prepared,
            Arc::clone(&self.read_admission),
            timeout,
        );
        let response = tokio::time::timeout_at(deadline, self.transport.send(request))
            .await
            .map_err(|_| RestError::Transport(HttpTransportError::Timeout))?
            .map_err(RestError::Transport)?;
        let result = RestResponse { endpoint, response };
        if (200..300).contains(&result.status()) {
            Ok(result)
        } else {
            Err(RestError::HttpStatus(result))
        }
    }
}

fn is_header_name(name: &str) -> bool {
    name.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

fn is_header_value(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| *byte == b'\t' || (0x20..=0x7e).contains(byte) || (0x80..=0xff).contains(byte))
}
