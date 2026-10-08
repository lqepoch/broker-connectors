use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use alpaca_core::{BaseUrl, Credentials, env};
use alpaca_http::{
    ConcurrencyLimit, HttpClient, HttpResponse, NoContent, RequestParts, RetryConfig,
    StaticHeaderAuthenticator, TransportObserver,
};
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use crate::{
    Error, corporate_actions::CorporateActionsClient, crypto::CryptoClient, news::NewsClient,
    options::OptionsClient, stocks::StocksClient,
};

pub const DATA_API_KEY_ENV: &str = "ALPACA_DATA_API_KEY";
pub const DATA_SECRET_KEY_ENV: &str = "ALPACA_DATA_SECRET_KEY";
const DEFAULT_DATA_BASE_URL: &str = "https://data.alpaca.markets";
const APCA_API_KEY_HEADER: &str = "APCA-API-KEY-ID";
const APCA_API_SECRET_HEADER: &str = "APCA-API-SECRET-KEY";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_IN_FLIGHT: usize = 50;
const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 50;
const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

#[allow(dead_code)]
pub(crate) struct ClientInner {
    http: HttpClient,
    auth: StaticHeaderAuthenticator,
    base_url: BaseUrl,
}

#[derive(Clone, Default)]
pub struct ClientBuilder {
    api_key: Option<Zeroizing<String>>,
    secret_key: Option<Zeroizing<String>>,
    base_url: Option<BaseUrl>,
    timeout: Option<Duration>,
    observer: Option<Arc<dyn TransportObserver>>,
    retry_config: RetryConfig,
    max_in_flight: Option<usize>,
}

impl Client {
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn new(credentials: Credentials) -> Result<Self, Error> {
        Self::builder().credentials(credentials).build()
    }

    pub fn from_env() -> Result<Self, Error> {
        Self::builder().credentials_from_env()?.build()
    }

    #[must_use]
    pub fn base_url(&self) -> &BaseUrl {
        &self.inner.base_url
    }

    #[must_use]
    pub fn stocks(&self) -> StocksClient {
        StocksClient::new(self.inner.clone())
    }

    #[must_use]
    pub fn crypto(&self) -> CryptoClient {
        CryptoClient::new(self.inner.clone())
    }

    #[must_use]
    pub fn options(&self) -> OptionsClient {
        OptionsClient::new(self.inner.clone())
    }

    #[must_use]
    pub fn news(&self) -> NewsClient {
        NewsClient::new(self.inner.clone())
    }

    #[must_use]
    pub fn corporate_actions(&self) -> CorporateActionsClient {
        CorporateActionsClient::new(self.inner.clone())
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("base_url", self.base_url())
            .field("http", &"HttpClient")
            .field("auth", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for ClientInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientInner")
            .field("base_url", &self.base_url)
            .field("http", &"HttpClient")
            .field("auth", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("api_key", &redacted_option(&self.api_key))
            .field("secret_key", &redacted_option(&self.secret_key))
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .field(
                "observer",
                &self.observer.as_ref().map(|_| "TransportObserver"),
            )
            .field("retry_config", &self.retry_config)
            .field("max_in_flight", &self.max_in_flight)
            .finish()
    }
}

impl ClientBuilder {
    #[must_use]
    pub fn credentials(self, credentials: Credentials) -> Self {
        self.credentials_zeroizing(
            Zeroizing::new(credentials.api_key().to_owned()),
            Zeroizing::new(credentials.secret_key().to_owned()),
        )
    }

    /// Set credentials from zeroizing buffers without storing plain `String` values.
    #[must_use]
    pub fn credentials_zeroizing(
        mut self,
        api_key: Zeroizing<String>,
        secret_key: Zeroizing<String>,
    ) -> Self {
        self.api_key = Some(api_key);
        self.secret_key = Some(secret_key);
        self
    }

    #[must_use]
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(Zeroizing::new(api_key.into()));
        self
    }

    #[must_use]
    pub fn secret_key(mut self, secret_key: impl Into<String>) -> Self {
        self.secret_key = Some(Zeroizing::new(secret_key.into()));
        self
    }

    #[must_use]
    pub fn base_url(mut self, base_url: BaseUrl) -> Self {
        self.base_url = Some(base_url);
        self
    }

    pub fn base_url_str(mut self, base_url: impl AsRef<str>) -> Result<Self, Error> {
        self.base_url = Some(BaseUrl::new(base_url.as_ref())?);
        Ok(self)
    }

    pub fn credentials_from_env(self) -> Result<Self, Error> {
        self.credentials_from_env_names(DATA_API_KEY_ENV, DATA_SECRET_KEY_ENV)
    }

    pub fn credentials_from_env_names(
        mut self,
        api_key_var: &str,
        secret_key_var: &str,
    ) -> Result<Self, Error> {
        if let Some(credentials) = env::credentials_from_env_names(api_key_var, secret_key_var)? {
            return Ok(self.credentials(credentials));
        }

        if let Some(credentials) = env::credentials_from_env()? {
            self = self.credentials(credentials);
        }

        Ok(self)
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn observer(mut self, observer: Arc<dyn TransportObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    #[must_use]
    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    #[must_use]
    pub fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = Some(max_in_flight);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        let base_url = match self.base_url {
            Some(base_url) => base_url,
            None => BaseUrl::new(DEFAULT_DATA_BASE_URL)?,
        };
        if base_url.as_str() != DEFAULT_DATA_BASE_URL {
            return Err(Error::InvalidConfiguration(
                "base_url must be the fixed HTTPS Alpaca market-data origin".to_owned(),
            ));
        }

        let credentials = match (self.api_key, self.secret_key) {
            (Some(api_key), Some(secret_key)) => Credentials::new_zeroizing(api_key, secret_key)?,
            (None, None) => return Err(Error::MissingCredentials),
            _ => {
                return Err(Error::InvalidConfiguration(
                    "api_key and secret_key must be paired".to_owned(),
                ));
            }
        };

        let auth = StaticHeaderAuthenticator::from_pairs([
            (APCA_API_KEY_HEADER, credentials.api_key()),
            (APCA_API_SECRET_HEADER, credentials.secret_key()),
        ])?;

        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let reqwest_client = Self::build_reqwest_client(timeout)?;

        let mut http_builder = HttpClient::builder()
            .retry_config(self.retry_config)
            .reqwest_client(reqwest_client);
        if let Some(observer) = self.observer {
            http_builder = http_builder.observer(observer);
        }
        http_builder = http_builder.concurrency_limit(ConcurrencyLimit::new(Some(
            self.max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT),
        )));

        let http = http_builder.build()?;

        Ok(Client {
            inner: Arc::new(ClientInner {
                http,
                auth,
                base_url,
            }),
        })
    }

    fn build_reqwest_client(timeout: Duration) -> Result<reqwest::Client, Error> {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .pool_max_idle_per_host(DEFAULT_POOL_MAX_IDLE_PER_HOST)
            .pool_idle_timeout(DEFAULT_POOL_IDLE_TIMEOUT)
            .tcp_keepalive(DEFAULT_TCP_KEEPALIVE)
            .timeout(timeout)
            .http1_only()
            .build()
            .map_err(|error| alpaca_http::Error::from_reqwest(error, None).into())
    }
}

impl ClientInner {
    #[allow(dead_code)]
    pub(crate) async fn send_json<T>(&self, request: RequestParts) -> Result<HttpResponse<T>, Error>
    where
        T: DeserializeOwned,
    {
        self.http
            .send_json(&self.base_url, request, Some(&self.auth))
            .await
            .map_err(Error::from)
    }

    #[allow(dead_code)]
    pub(crate) async fn send_text(
        &self,
        request: RequestParts,
    ) -> Result<HttpResponse<String>, Error> {
        self.http
            .send_text(&self.base_url, request, Some(&self.auth))
            .await
            .map_err(Error::from)
    }

    #[allow(dead_code)]
    pub(crate) async fn send_no_content(
        &self,
        request: RequestParts,
    ) -> Result<HttpResponse<NoContent>, Error> {
        self.http
            .send_no_content(&self.base_url, request, Some(&self.auth))
            .await
            .map_err(Error::from)
    }
}

fn redacted_option<T>(value: &Option<T>) -> &'static str {
    match value {
        Some(_) => "[REDACTED]",
        None => "None",
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, Error};

    #[test]
    fn client_builder_debug_redacts_credentials() {
        let builder = Client::builder()
            .api_key("synthetic-test-key")
            .secret_key("synthetic-test-secret");
        let debug = format!("{builder:?}");

        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("synthetic-test-key"));
        assert!(!debug.contains("synthetic-test-secret"));
    }

    #[test]
    fn fixed_origin_is_validated_before_credentials_or_request_construction() {
        let fixed_origin_without_credentials = Client::builder().build();
        assert!(matches!(
            fixed_origin_without_credentials,
            Err(Error::MissingCredentials)
        ));

        let untrusted_origin = Client::builder()
            .base_url_str("https://other.example")
            .expect("syntactically valid URL")
            .api_key("synthetic-test-key")
            .secret_key("synthetic-test-secret")
            .build();
        let Err(error) = untrusted_origin else {
            panic!("untrusted origin must be rejected");
        };
        let debug = format!("{error:?}");

        assert!(matches!(error, Error::InvalidConfiguration(_)));
        assert!(!debug.contains("other.example"));
        assert!(!debug.contains("synthetic-test-key"));
        assert!(!debug.contains("synthetic-test-secret"));
    }
}
