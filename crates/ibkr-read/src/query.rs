use domain::{ContractCurrency, OptionSymbol};
use ibapi::contracts::{Contract, Currency, Exchange, OptionRight, SecurityType, Symbol};

use crate::IbkrCatalogError;

/// Exact OCC option terms and explicit IBKR exchange/currency search scope.
///
/// Constructing a query does not establish that IBKR has a matching contract.
/// The adapter rejects an empty exchange and `SMART`; it never supplies an
/// exchange or currency default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IbkrOptionCatalogQuery {
    symbol: OptionSymbol,
    exchange: String,
    currency: ContractCurrency,
}

impl IbkrOptionCatalogQuery {
    /// Creates a query from a validated OCC symbol and explicit provider scope.
    ///
    /// # Errors
    ///
    /// Returns [`IbkrCatalogError::InvalidExchange`] for empty, malformed, or
    /// `SMART` exchange input, and [`IbkrCatalogError::InvalidCurrency`] when
    /// the explicit currency is invalid.
    pub fn new(
        symbol: OptionSymbol,
        exchange: &str,
        currency: &str,
    ) -> Result<Self, IbkrCatalogError> {
        let exchange = normalize_exchange(exchange)?;
        let currency =
            ContractCurrency::new(currency).map_err(|_| IbkrCatalogError::InvalidCurrency)?;
        Ok(Self {
            symbol,
            exchange,
            currency,
        })
    }

    /// Returns the exact OCC option symbol used by the lookup.
    #[must_use]
    pub const fn symbol(&self) -> &OptionSymbol {
        &self.symbol
    }

    /// Returns the explicit exchange code used by the lookup.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Returns the explicit currency used by the lookup.
    #[must_use]
    pub const fn currency(&self) -> &ContractCurrency {
        &self.currency
    }
}

pub(crate) fn sdk_contract(query: &IbkrOptionCatalogQuery) -> Contract {
    // Zero is the TWS contract-search sentinel. It is not a returned provider id.
    let expiration = query.symbol.expiration();
    Contract {
        contract_id: 0,
        symbol: Symbol::from(query.symbol.underlying().as_str()),
        security_type: SecurityType::Option,
        last_trade_date_or_contract_month: format!(
            "{:04}{:02}{:02}",
            expiration.year(),
            expiration.month(),
            expiration.day()
        ),
        strike: f64::from(query.symbol.strike().mills()) / 1_000.0,
        right: Some(match query.symbol.right() {
            domain::OptionRight::Call => OptionRight::Call,
            domain::OptionRight::Put => OptionRight::Put,
        }),
        exchange: Exchange::from(query.exchange.as_str()),
        currency: Currency::from(query.currency.as_str()),
        local_symbol: query.symbol.format(),
        ..Contract::default()
    }
}

fn normalize_exchange(value: &str) -> Result<String, IbkrCatalogError> {
    let value = value.trim().to_ascii_uppercase();
    if value.is_empty()
        || value.len() > 32
        || value == "SMART"
        || !value.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
        })
    {
        return Err(IbkrCatalogError::InvalidExchange);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use domain::OptionSymbol;

    use super::{IbkrCatalogError, IbkrOptionCatalogQuery, sdk_contract};

    fn occ() -> OptionSymbol {
        OptionSymbol::parse("AAPL  270115C00150000").expect("valid synthetic OCC symbol")
    }

    #[test]
    fn query_requires_explicit_non_smart_exchange_and_currency() {
        assert_eq!(
            IbkrOptionCatalogQuery::new(occ(), " SMART ", "USD"),
            Err(IbkrCatalogError::InvalidExchange)
        );
        assert_eq!(
            IbkrOptionCatalogQuery::new(occ(), "CBOE", ""),
            Err(IbkrCatalogError::InvalidCurrency)
        );
        let query = IbkrOptionCatalogQuery::new(occ(), " cboe ", "usd").expect("explicit scope");
        assert_eq!(query.exchange(), "CBOE");
        assert_eq!(query.currency().as_str(), "USD");
    }

    #[test]
    fn sdk_search_contract_has_no_provider_identity_or_defaults() {
        let query = IbkrOptionCatalogQuery::new(occ(), "CBOE", "USD").expect("query");
        let contract = sdk_contract(&query);

        assert_eq!(contract.contract_id, 0, "zero is search-only sentinel");
        assert_eq!(contract.symbol.as_str(), "AAPL");
        assert_eq!(
            contract.security_type,
            ibapi::contracts::SecurityType::Option
        );
        assert_eq!(contract.exchange.as_str(), "CBOE");
        assert_eq!(contract.currency.as_str(), "USD");
        assert_eq!(contract.local_symbol, "AAPL  270115C00150000");
        assert_eq!(contract.primary_exchange.as_str(), "");
        assert_eq!(contract.multiplier, "");
        assert_eq!(contract.trading_class, "");
    }
}
