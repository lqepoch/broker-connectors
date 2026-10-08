use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use ibapi::subscriptions::{Drained, Subscription, SubscriptionItem};
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout, timeout_at};

use crate::entry::map_contract_details;
use crate::query::sdk_contract;
use crate::{IbkrCatalogError, IbkrOptionCatalogEntry, IbkrOptionCatalogQuery};

const MAX_RESULT_ROWS: usize = 16;
const REQUEST_BUFFER_LIMIT: usize = 32;
const TASK_YIELD_INTERVAL: usize = 8;

/// Explicit finite time budgets for connect, request, cancel/drain, and shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IbkrCatalogTimeouts {
    connect: Duration,
    request: Duration,
    cancel_drain: Duration,
    disconnect: Duration,
}

impl IbkrCatalogTimeouts {
    /// Creates time budgets; zero durations are rejected so every network phase is bounded.
    pub fn new(
        connect: Duration,
        request: Duration,
        cancel_drain: Duration,
        disconnect: Duration,
    ) -> Result<Self, IbkrCatalogError> {
        if connect.is_zero() || request.is_zero() || cancel_drain.is_zero() || disconnect.is_zero()
        {
            return Err(IbkrCatalogError::InvalidTimeout);
        }
        Ok(Self {
            connect,
            request,
            cancel_drain,
            disconnect,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::IbkrCatalogError;

    use super::IbkrCatalogTimeouts;

    #[test]
    fn every_network_phase_requires_a_nonzero_budget() {
        assert_eq!(
            IbkrCatalogTimeouts::new(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(IbkrCatalogError::InvalidTimeout)
        );
        assert!(
            IbkrCatalogTimeouts::new(
                Duration::from_millis(1),
                Duration::from_millis(1),
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .is_ok()
        );
    }
}

/// Read-only IBKR contract-catalog adapter backed by the pinned async SDK.
///
/// The SDK client is private. This adapter serializes lookups and permanently
/// rejects new requests after a session loses terminal request evidence.
pub struct IbkrCatalogAdapter {
    client: ibapi::Client,
    timeouts: IbkrCatalogTimeouts,
    request_lock: Mutex<()>,
    poisoned: AtomicBool,
}

impl IbkrCatalogAdapter {
    /// Connects to a loopback TWS or IB Gateway API address.
    ///
    /// Non-loopback addresses and port zero are rejected. This method performs
    /// no account read and exposes no SDK client. Use a synthetic local endpoint
    /// in tests; real provider access is outside the evidence supplied by this
    /// crate's offline tests.
    pub async fn connect(
        address: SocketAddr,
        client_id: i32,
        timeouts: IbkrCatalogTimeouts,
    ) -> Result<Self, IbkrCatalogError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(IbkrCatalogError::InvalidEndpoint);
        }
        let connect = ibapi::Client::builder()
            .address(address.to_string())
            .client_id(client_id)
            .max_reconnect_attempts(0)
            .channel_capacity(REQUEST_BUFFER_LIMIT);
        let client = match timeout(timeouts.connect, connect.connect()).await {
            Ok(Ok(client)) => client,
            Ok(Err(_)) => return Err(IbkrCatalogError::ConnectFailed),
            Err(_) => return Err(IbkrCatalogError::ConnectTimeout),
        };

        Ok(Self {
            client,
            timeouts,
            request_lock: Mutex::new(()),
            poisoned: AtomicBool::new(false),
        })
    }

    /// Resolves an exact OCC symbol under explicit exchange and currency scope.
    ///
    /// The method consumes the whole stream and accepts a result only after the
    /// SDK confirms IBKR's native `ContractDataEnd`. Multiple matches are
    /// ambiguous. Rows and unread buffers are bounded; an unconfirmed cancel or
    /// disconnect poisons this adapter instance.
    pub async fn lookup_option(
        &self,
        query: &IbkrOptionCatalogQuery,
    ) -> Result<IbkrOptionCatalogEntry, IbkrCatalogError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(IbkrCatalogError::SessionPoisoned);
        }

        let deadline = Instant::now() + self.timeouts.request;
        let _request_guard = match timeout_at(deadline, self.request_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => return Err(IbkrCatalogError::RequestTimeout),
        };
        if self.poisoned.load(Ordering::Acquire) {
            return Err(IbkrCatalogError::SessionPoisoned);
        }

        let contract = sdk_contract(query);
        let builder = self
            .client
            .contract_details_stream(&contract)
            .buffer_limit(REQUEST_BUFFER_LIMIT);
        let mut subscription = match timeout_at(deadline, builder.subscribe()).await {
            Ok(Ok(subscription)) => subscription,
            Ok(Err(_)) => return Err(self.poison_and_disconnect().await),
            Err(_) => return Err(self.poison_and_disconnect().await),
        };

        let mut row_count = 0usize;
        let mut first_entry = None;
        let mut invalid_first_row = false;

        loop {
            match timeout_at(deadline, subscription.next()).await {
                Err(_) => {
                    return Err(self
                        .cancel_for_error(subscription, IbkrCatalogError::RequestTimeout)
                        .await);
                }
                Ok(Some(Ok(SubscriptionItem::Data(details)))) => {
                    row_count += 1;
                    if row_count > MAX_RESULT_ROWS {
                        return Err(self
                            .cancel_for_error(subscription, IbkrCatalogError::TooManyRows)
                            .await);
                    }
                    if row_count == 1 {
                        match map_contract_details(query, details) {
                            Ok(entry) => first_entry = Some(entry),
                            Err(_) => invalid_first_row = true,
                        }
                    }
                    if row_count % TASK_YIELD_INTERVAL == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                Ok(Some(Ok(SubscriptionItem::Notice(_)))) => {
                    return Err(self
                        .cancel_for_error(subscription, IbkrCatalogError::ProviderRejected)
                        .await);
                }
                Ok(Some(Err(_))) => {
                    return Err(self
                        .cancel_for_error(subscription, IbkrCatalogError::SdkRequestFailed)
                        .await);
                }
                Ok(None) => {
                    match self.cancel_and_confirm(subscription).await {
                        Ok(RequestTerminal::Ended) => {}
                        Ok(RequestTerminal::Rejected) => {
                            return Err(IbkrCatalogError::ProviderRejected);
                        }
                        Err(error) => return Err(error),
                    }

                    if row_count == 0 {
                        return Err(IbkrCatalogError::NoMatch);
                    }
                    if row_count > 1 {
                        return Err(IbkrCatalogError::AmbiguousMatch);
                    }
                    if invalid_first_row {
                        return Err(IbkrCatalogError::InvalidProviderRow);
                    }
                    return first_entry.ok_or(IbkrCatalogError::InvalidProviderRow);
                }
            }
        }
    }

    /// Closes the SDK session within the configured shutdown budget.
    pub async fn disconnect(&self) -> Result<(), IbkrCatalogError> {
        self.poisoned.store(true, Ordering::Release);
        match timeout(self.timeouts.disconnect, self.client.disconnect()).await {
            Ok(()) => Ok(()),
            Err(_) => Err(IbkrCatalogError::DisconnectTimeout),
        }
    }

    async fn cancel_for_error(
        &self,
        subscription: Subscription<ibapi::contracts::ContractDetails>,
        request_error: IbkrCatalogError,
    ) -> IbkrCatalogError {
        match self.cancel_and_confirm(subscription).await {
            Ok(RequestTerminal::Ended) => request_error,
            Ok(RequestTerminal::Rejected) => IbkrCatalogError::ProviderRejected,
            Err(session_error) => session_error,
        }
    }

    async fn cancel_and_confirm(
        &self,
        subscription: Subscription<ibapi::contracts::ContractDetails>,
    ) -> Result<RequestTerminal, IbkrCatalogError> {
        let deadline = Instant::now() + self.timeouts.cancel_drain;
        match timeout_at(deadline, subscription.cancel_and_drain(deadline)).await {
            Ok(Ok(Drained::Ended)) => Ok(RequestTerminal::Ended),
            Ok(Ok(Drained::Rejected(_))) => Ok(RequestTerminal::Rejected),
            Ok(Ok(Drained::Unconfirmed)) | Ok(Err(_)) | Err(_) => {
                Err(self.poison_and_disconnect().await)
            }
        }
    }

    async fn poison_and_disconnect(&self) -> IbkrCatalogError {
        self.poisoned.store(true, Ordering::Release);
        match timeout(self.timeouts.disconnect, self.client.disconnect()).await {
            Ok(()) => IbkrCatalogError::SessionPoisoned,
            Err(_) => IbkrCatalogError::DisconnectTimeout,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestTerminal {
    Ended,
    Rejected,
}
