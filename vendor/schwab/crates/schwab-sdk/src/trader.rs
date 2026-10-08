//! Typed Trader GET facade. Each method delegates to one `schwab-rest` API.
//! 基于现有类型化 REST 客户端提供只读 Trader facade。

use schwab_rest::{AccessTokenProvider, HttpTransport, SchwabRestClient};

pub use schwab_rest::{
    AccountsQuery, BrokerIdentifier, OrdersQuery, PathIdentifier, QueryExtensions, QueryText,
    TransactionsQuery,
};

use crate::{ReadApiError, TypedReadResponse};

/// Borrowed Trader GET operations over the SDK's shared REST client.
/// 中文摘要：对固定 Trader GET 路由的轻量借用 facade。
pub struct Trader<'a, P, T> {
    client: &'a SchwabRestClient<P, T>,
}

impl<'a, P, T> Trader<'a, P, T> {
    pub(crate) const fn new(client: &'a SchwabRestClient<P, T>) -> Self {
        Self { client }
    }
}

impl<P, T> Trader<'_, P, T>
where
    P: AccessTokenProvider,
    T: HttpTransport,
{
    /// Reads the account-number to account-hash mapping.
    /// 中文摘要：读取账户编号映射响应。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn account_numbers(&self) -> Result<TypedReadResponse, ReadApiError> {
        self.client.account_numbers_typed().await
    }

    /// Reads the account list, optionally including positions.
    /// 中文摘要：按已校验查询参数读取账户列表。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn accounts(&self, query: AccountsQuery) -> Result<TypedReadResponse, ReadApiError> {
        self.client.accounts(query).await
    }

    /// Reads one account and its requested fields.
    /// 中文摘要：读取指定账户的类型化响应。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn account(
        &self,
        account_hash: impl AsRef<str>,
        query: AccountsQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.account(account_hash, query).await
    }

    /// Reads orders for one account.
    /// 中文摘要：按账户和时间范围读取订单。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn orders(
        &self,
        account_hash: impl AsRef<str>,
        query: OrdersQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.orders(account_hash, query).await
    }

    /// Reads account orders while preserving validated additive query fields.
    /// 中文摘要：读取订单并保留受限、无冲突的附加查询项。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn orders_with_query_extensions(
        &self,
        account_hash: impl AsRef<str>,
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client
            .orders_with_query_extensions(account_hash, query, extensions)
            .await
    }

    /// Reads one order by account and broker order identifier.
    /// 中文摘要：按账户及 broker ID 读取单笔订单。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn order(
        &self,
        account_hash: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.order(account_hash, order_id).await
    }

    /// Reads orders across accounts.
    /// 中文摘要：按时间范围读取跨账户订单。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn orders_across_accounts(
        &self,
        query: OrdersQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.orders_across_accounts(query).await
    }

    /// Reads orders across accounts with validated additive query fields.
    /// 中文摘要：读取跨账户订单并保留受限附加查询项。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn orders_across_accounts_with_query_extensions(
        &self,
        query: OrdersQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client
            .orders_across_accounts_with_query_extensions(query, extensions)
            .await
    }

    /// Reads transactions for one account.
    /// 中文摘要：按账户和日期范围读取交易记录。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn transactions(
        &self,
        account_hash: impl AsRef<str>,
        query: TransactionsQuery,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.transactions(account_hash, query).await
    }

    /// Reads account transactions with validated additive query fields.
    /// 中文摘要：读取交易记录并保留受限附加查询项。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn transactions_with_query_extensions(
        &self,
        account_hash: impl AsRef<str>,
        query: TransactionsQuery,
        extensions: QueryExtensions,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client
            .transactions_with_query_extensions(account_hash, query, extensions)
            .await
    }

    /// Reads one transaction by account and broker transaction identifier.
    /// 中文摘要：按账户及 broker ID 读取单条交易记录。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn transaction(
        &self,
        account_hash: impl AsRef<str>,
        transaction_id: impl AsRef<str>,
    ) -> Result<TypedReadResponse, ReadApiError> {
        self.client.transaction(account_hash, transaction_id).await
    }

    /// Reads typed user preferences. This does not establish Streamer readiness.
    /// 中文摘要：读取用户偏好 DTO；该响应不直接授权网络目标。
    ///
    /// # Errors
    /// Returns [`ReadApiError`] when request validation, admission, transport, or response projection fails.
    pub async fn user_preferences(&self) -> Result<TypedReadResponse, ReadApiError> {
        self.client.user_preferences_typed().await
    }
}
