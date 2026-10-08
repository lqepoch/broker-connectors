use std::net::SocketAddr;
use std::sync::Arc;
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

/// Maximum timeout for any individual connect, request, cancel, or shutdown phase.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(60);

/// Explicit finite time budgets for connect, request, cancel/drain, and shutdown.
/// Every phase must use a positive timeout of at most [`MAX_TIMEOUT`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IbkrCatalogTimeouts {
    connect: Duration,
    request: Duration,
    cancel_drain: Duration,
    disconnect: Duration,
}

impl IbkrCatalogTimeouts {
    /// Creates time budgets; zero or over-60-second durations are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`IbkrCatalogError::InvalidTimeout`] if any phase is zero or
    /// exceeds [`MAX_TIMEOUT`].
    pub fn new(
        connect: Duration,
        request: Duration,
        cancel_drain: Duration,
        disconnect: Duration,
    ) -> Result<Self, IbkrCatalogError> {
        if [connect, request, cancel_drain, disconnect]
            .iter()
            .any(|timeout| timeout.is_zero() || *timeout > MAX_TIMEOUT)
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
    fn every_network_phase_requires_a_bounded_nonzero_budget() {
        let valid = [Duration::from_secs(1); 4];
        let construct = |timeouts: [Duration; 4]| {
            IbkrCatalogTimeouts::new(timeouts[0], timeouts[1], timeouts[2], timeouts[3])
        };
        for (index, _) in valid.iter().enumerate() {
            let mut timeouts = valid;
            timeouts[index] = Duration::ZERO;
            assert_eq!(
                construct(timeouts),
                Err(IbkrCatalogError::InvalidTimeout),
                "network phase {index} must reject zero"
            );

            timeouts[index] = Duration::from_secs(61);
            assert_eq!(
                construct(timeouts),
                Err(IbkrCatalogError::InvalidTimeout),
                "network phase {index} must reject over-limit budgets"
            );

            timeouts[index] = Duration::MAX;
            assert_eq!(
                construct(timeouts),
                Err(IbkrCatalogError::InvalidTimeout),
                "network phase {index} must reject Duration::MAX"
            );
        }
        assert!(construct([Duration::from_millis(1); 4]).is_ok());
    }
}

/// Read-only IBKR contract-catalog adapter backed by the pinned async SDK.
///
/// The SDK client is private. This adapter serializes lookups and permanently
/// rejects new requests after a session loses terminal request evidence.
pub struct IbkrCatalogAdapter {
    client: std::sync::Mutex<Option<Arc<ibapi::Client>>>,
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
    ///
    /// # Errors
    ///
    /// Returns [`IbkrCatalogError::InvalidEndpoint`] for non-loopback or
    /// zero-port addresses, or a connection error when the SDK handshake
    /// fails or exceeds the configured connect budget.
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
            client: std::sync::Mutex::new(Some(Arc::new(client))),
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
    /// disconnect poisons and releases the SDK session. If the caller cancels
    /// this future, its drop guard poisons the adapter and drops its SDK owner;
    /// the next lookup is rejected.
    ///
    /// # Errors
    ///
    /// Returns a classified adapter error when the provider rejects the query,
    /// the response is empty/ambiguous/invalid, the request exceeds its budget,
    /// cancellation cannot be confirmed, or the SDK session is already poisoned.
    pub async fn lookup_option(
        &self,
        query: &IbkrOptionCatalogQuery,
    ) -> Result<IbkrOptionCatalogEntry, IbkrCatalogError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(IbkrCatalogError::SessionPoisoned);
        }

        let deadline =
            deadline_after(self.timeouts.request).ok_or(IbkrCatalogError::InvalidTimeout)?;
        let Ok(_request_guard) = timeout_at(deadline, self.request_lock.lock()).await else {
            return Err(IbkrCatalogError::RequestTimeout);
        };
        if self.poisoned.load(Ordering::Acquire) {
            return Err(IbkrCatalogError::SessionPoisoned);
        }
        let client = self
            .clone_client()
            .ok_or(IbkrCatalogError::SessionPoisoned)?;
        // If the caller drops this future after the SDK may have sent a request,
        // Subscription::drop only schedules cancellation; it does not confirm
        // the native end marker. Poison the client so no later query can reuse it.
        let mut poison_on_drop = RequestPoisonGuard::new(&self.poisoned, &self.client);

        let contract = sdk_contract(query);
        let builder = client
            .contract_details_stream(&contract)
            .buffer_limit(REQUEST_BUFFER_LIMIT);
        let Ok(Ok(mut subscription)) = timeout_at(deadline, builder.subscribe()).await else {
            return Err(self.poison_and_disconnect().await);
        };

        let mut row_count = 0usize;
        let mut first_entry = None;
        let mut invalid_first_row = false;

        loop {
            match timeout_at(deadline, subscription.next()).await {
                Err(_) => {
                    return Err(self
                        .cancel_for_error(
                            subscription,
                            IbkrCatalogError::RequestTimeout,
                            &mut poison_on_drop,
                        )
                        .await);
                }
                Ok(Some(Ok(SubscriptionItem::Data(details)))) => {
                    row_count += 1;
                    if row_count > MAX_RESULT_ROWS {
                        return Err(self
                            .cancel_for_error(
                                subscription,
                                IbkrCatalogError::TooManyRows,
                                &mut poison_on_drop,
                            )
                            .await);
                    }
                    if row_count == 1 {
                        match map_contract_details(query, details) {
                            Ok(entry) => first_entry = Some(entry),
                            Err(_) => invalid_first_row = true,
                        }
                    }
                    if row_count.is_multiple_of(TASK_YIELD_INTERVAL) {
                        tokio::task::yield_now().await;
                    }
                }
                Ok(Some(Ok(SubscriptionItem::Notice(_)))) => {
                    return Err(self
                        .cancel_for_error(
                            subscription,
                            IbkrCatalogError::ProviderRejected,
                            &mut poison_on_drop,
                        )
                        .await);
                }
                Ok(Some(Err(_))) => {
                    return Err(self
                        .cancel_for_error(
                            subscription,
                            IbkrCatalogError::SdkRequestFailed,
                            &mut poison_on_drop,
                        )
                        .await);
                }
                Ok(None) => {
                    match self.cancel_and_confirm(subscription).await {
                        Ok(RequestTerminal::Ended) => poison_on_drop.confirm(),
                        Ok(RequestTerminal::Rejected) => {
                            poison_on_drop.confirm();
                            return Err(IbkrCatalogError::ProviderRejected);
                        }
                        Err(error) => return Err(error),
                    }

                    return result_after_native_end(row_count, invalid_first_row, first_entry);
                }
            }
        }
    }

    /// Closes and releases the SDK session within the configured shutdown budget.
    ///
    /// # Errors
    ///
    /// Returns [`IbkrCatalogError::DisconnectTimeout`] if the SDK's shutdown
    /// does not finish within its configured budget.
    pub async fn disconnect(&self) -> Result<(), IbkrCatalogError> {
        self.poisoned.store(true, Ordering::Release);
        let Some(client) = self.take_client() else {
            return Ok(());
        };
        match timeout(self.timeouts.disconnect, client.disconnect()).await {
            Ok(()) => Ok(()),
            Err(_) => Err(IbkrCatalogError::DisconnectTimeout),
        }
    }

    async fn cancel_for_error(
        &self,
        subscription: Subscription<ibapi::contracts::ContractDetails>,
        request_error: IbkrCatalogError,
        guard: &mut RequestPoisonGuard<'_>,
    ) -> IbkrCatalogError {
        match self.cancel_and_confirm(subscription).await {
            Ok(RequestTerminal::Ended) => {
                guard.confirm();
                request_error
            }
            Ok(RequestTerminal::Rejected) => {
                guard.confirm();
                IbkrCatalogError::ProviderRejected
            }
            Err(session_error) => session_error,
        }
    }

    async fn cancel_and_confirm(
        &self,
        subscription: Subscription<ibapi::contracts::ContractDetails>,
    ) -> Result<RequestTerminal, IbkrCatalogError> {
        let Some(deadline) = deadline_after(self.timeouts.cancel_drain) else {
            return Err(self.poison_and_disconnect().await);
        };
        match timeout_at(deadline, subscription.cancel_and_drain(deadline)).await {
            Ok(Ok(Drained::Ended)) => Ok(RequestTerminal::Ended),
            Ok(Ok(Drained::Rejected(_))) => Ok(RequestTerminal::Rejected),
            Ok(Ok(Drained::Unconfirmed) | Err(_)) | Err(_) => {
                Err(self.poison_and_disconnect().await)
            }
        }
    }

    async fn poison_and_disconnect(&self) -> IbkrCatalogError {
        self.poisoned.store(true, Ordering::Release);
        let Some(client) = self.take_client() else {
            return IbkrCatalogError::SessionPoisoned;
        };
        match timeout(self.timeouts.disconnect, client.disconnect()).await {
            Ok(()) => IbkrCatalogError::SessionPoisoned,
            Err(_) => IbkrCatalogError::DisconnectTimeout,
        }
    }

    fn take_client(&self) -> Option<Arc<ibapi::Client>> {
        self.client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn clone_client(&self) -> Option<Arc<ibapi::Client>> {
        self.client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
    }
}

fn deadline_after(duration: Duration) -> Option<Instant> {
    Instant::now().checked_add(duration)
}

fn result_after_native_end(
    row_count: usize,
    invalid_first_row: bool,
    first_entry: Option<IbkrOptionCatalogEntry>,
) -> Result<IbkrOptionCatalogEntry, IbkrCatalogError> {
    if row_count == 0 {
        return Err(IbkrCatalogError::NoMatch);
    }
    if row_count > 1 {
        return Err(IbkrCatalogError::AmbiguousMatch);
    }
    if invalid_first_row {
        return Err(IbkrCatalogError::InvalidProviderRow);
    }
    first_entry.ok_or(IbkrCatalogError::InvalidProviderRow)
}

struct RequestPoisonGuard<'a> {
    poisoned: &'a AtomicBool,
    client: &'a std::sync::Mutex<Option<Arc<ibapi::Client>>>,
    armed: bool,
}

impl<'a> RequestPoisonGuard<'a> {
    fn new(
        poisoned: &'a AtomicBool,
        client: &'a std::sync::Mutex<Option<Arc<ibapi::Client>>>,
    ) -> Self {
        Self {
            poisoned,
            client,
            armed: true,
        }
    }

    fn confirm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestPoisonGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
            self.client
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestTerminal {
    Ended,
    Rejected,
}
