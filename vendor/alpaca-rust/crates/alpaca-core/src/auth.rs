use std::fmt;
use zeroize::Zeroizing;

use crate::{Error, validate};

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    api_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
}

impl Credentials {
    pub fn new(api_key: impl Into<String>, secret_key: impl Into<String>) -> Result<Self, Error> {
        Self::new_zeroizing(
            Zeroizing::new(api_key.into()),
            Zeroizing::new(secret_key.into()),
        )
    }

    /// Constructs credentials that are zeroized when their owner is dropped.
    pub fn new_zeroizing(
        api_key: Zeroizing<String>,
        secret_key: Zeroizing<String>,
    ) -> Result<Self, Error> {
        if api_key.trim().is_empty() {
            return Err(Error::InvalidConfiguration(
                "api_key must not be empty or whitespace".to_owned(),
            ));
        }
        if secret_key.trim().is_empty() {
            return Err(Error::InvalidConfiguration(
                "secret_key must not be empty or whitespace".to_owned(),
            ));
        }
        validate::valid_header_value("api_key", &api_key)?;
        validate::valid_header_value("secret_key", &secret_key)?;

        Ok(Self {
            api_key,
            secret_key,
        })
    }

    #[must_use]
    pub fn api_key(&self) -> &str {
        self.api_key.as_str()
    }

    #[must_use]
    pub fn secret_key(&self) -> &str {
        self.secret_key.as_str()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("api_key", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Credentials;

    #[test]
    fn credentials_debug_redacts_values() {
        let credentials = Credentials::new("synthetic-test-key", "synthetic-test-secret")
            .expect("synthetic credentials are valid");
        let debug = format!("{credentials:?}");

        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("synthetic-test-key"));
        assert!(!debug.contains("synthetic-test-secret"));
    }
}
