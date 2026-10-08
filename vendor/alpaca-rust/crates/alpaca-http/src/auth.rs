use std::fmt;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use zeroize::Zeroizing;

use crate::Error;

pub trait Authenticator: Send + Sync {
    fn apply(&self, headers: &mut HeaderMap) -> Result<(), Error>;
}

#[derive(Clone, Default)]
pub struct StaticHeaderAuthenticator {
    headers: Vec<(HeaderName, Zeroizing<String>)>,
}

impl StaticHeaderAuthenticator {
    pub fn from_pairs<I, K, V>(pairs: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut headers = Vec::new();

        for (name, value) in pairs {
            let name = HeaderName::from_bytes(name.as_ref().as_bytes())
                .map_err(|error| Error::Authentication(format!("invalid header name: {error}")))?;
            let value = value.as_ref();
            HeaderValue::from_str(value)
                .map_err(|_| Error::Authentication("invalid header value".to_owned()))?;
            headers.push((name, Zeroizing::new(value.to_owned())));
        }

        Ok(Self { headers })
    }

    pub fn apply(&self, headers: &mut HeaderMap) -> Result<(), Error> {
        <Self as Authenticator>::apply(self, headers)
    }
}

impl Authenticator for StaticHeaderAuthenticator {
    fn apply(&self, headers: &mut HeaderMap) -> Result<(), Error> {
        for (name, value) in &self.headers {
            let value = HeaderValue::from_str(value.as_str())
                .map_err(|_| Error::Authentication("invalid header value".to_owned()))?;
            headers.insert(name.clone(), value);
        }
        Ok(())
    }
}

impl fmt::Debug for StaticHeaderAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticHeaderAuthenticator")
            .field("header_count", &self.headers.len())
            .field("values", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::StaticHeaderAuthenticator;

    #[test]
    fn static_authenticator_debug_redacts_header_values() {
        let authenticator =
            StaticHeaderAuthenticator::from_pairs([("authorization", "synthetic-test-secret")])
                .expect("synthetic header is valid");
        let debug = format!("{authenticator:?}");

        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("synthetic-test-secret"));
    }
}
