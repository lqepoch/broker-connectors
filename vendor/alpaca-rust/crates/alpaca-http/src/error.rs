use thiserror::Error;

use crate::meta::ErrorMeta;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("authentication error: {0}")]
    Authentication(String),
    #[error("concurrency limit error: {0}")]
    ConcurrencyLimit(String),
    #[error("transport error: {message}")]
    Transport {
        message: String,
        meta: Option<Box<ErrorMeta>>,
    },
    #[error("deserialize error: {message}")]
    Deserialize {
        message: String,
        meta: Option<Box<ErrorMeta>>,
    },
    #[error("http status error")]
    HttpStatus(Box<ErrorMeta>),
    #[error("rate limited")]
    RateLimited(Box<ErrorMeta>),
    #[error("response body exceeds configured size limit")]
    ResponseBodyTooLarge(Box<ErrorMeta>),
    #[error("response body is not valid UTF-8")]
    InvalidResponseEncoding(Box<ErrorMeta>),
}

impl Error {
    #[must_use]
    pub fn meta(&self) -> Option<&ErrorMeta> {
        match self {
            Self::Transport { meta, .. } | Self::Deserialize { meta, .. } => meta.as_deref(),
            Self::HttpStatus(meta)
            | Self::RateLimited(meta)
            | Self::ResponseBodyTooLarge(meta)
            | Self::InvalidResponseEncoding(meta) => Some(meta.as_ref()),
            Self::InvalidRequest(_) | Self::Authentication(_) | Self::ConcurrencyLimit(_) => None,
        }
    }

    #[must_use]
    pub fn from_reqwest(error: reqwest::Error, meta: Option<ErrorMeta>) -> Self {
        Self::Transport {
            message: error.to_string(),
            meta: meta.map(Box::new),
        }
    }
}
