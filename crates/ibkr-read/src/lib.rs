//! Bounded, read-only Interactive Brokers option-catalog lookup.
//!
//! The adapter keeps the pinned SDK client private and returns a shared
//! [`domain::OptionInstrumentCandidate`] alongside an adapter-owned provider
//! identity record. A provider contract id is diagnostic catalog identity;
//! it does not qualify an option or grant account or order authority.
//!
//! Symbol-based market-data subscriptions, account reads, order operations,
//! and historical-data reads are not implemented by this crate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;
mod entry;
mod error;
mod query;

pub use adapter::{IbkrCatalogAdapter, IbkrCatalogTimeouts};
pub use entry::{IbkrOptionCatalogEntry, IbkrProviderContractIdentity};
pub use error::IbkrCatalogError;
pub use query::IbkrOptionCatalogQuery;
