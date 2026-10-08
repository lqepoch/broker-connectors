use std::num::NonZeroI32;

use domain::{
    ContractCurrency, ContractMultiplier, MarketDataProviderId, MetadataSource,
    OptionInstrumentCandidate, OptionSymbol, ProviderMetadataKind, ProviderMetadataRef,
    ProviderRecordId, ProviderValue, TradingClass,
};
use ibapi::contracts::{ContractDetails, OptionRight, SecurityType};

use crate::{IbkrCatalogError, IbkrOptionCatalogQuery};

/// An IBKR contract record retained alongside the shared unqualified candidate.
///
/// This provider identity is useful for catalog diagnostics and follow-up
/// provider lookups. It is not a cross-broker instrument key, account identity,
/// market-data entitlement, or order-routing authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IbkrOptionCatalogEntry {
    candidate: OptionInstrumentCandidate,
    provider_identity: IbkrProviderContractIdentity,
}

impl IbkrOptionCatalogEntry {
    /// Returns the shared candidate; unknown economic terms remain unknown.
    #[must_use]
    pub const fn candidate(&self) -> &OptionInstrumentCandidate {
        &self.candidate
    }

    /// Returns the adapter-owned provider record identity.
    #[must_use]
    pub const fn provider_identity(&self) -> &IbkrProviderContractIdentity {
        &self.provider_identity
    }
}

/// Provider-specific contract identity retained from one validated IBKR row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IbkrProviderContractIdentity {
    contract_id: NonZeroI32,
    exchange: String,
    currency: ContractCurrency,
    local_symbol: String,
    record: ProviderMetadataRef,
}

impl IbkrProviderContractIdentity {
    /// Returns the positive IBKR contract id reported by the provider.
    #[must_use]
    pub const fn contract_id(&self) -> i32 {
        self.contract_id.get()
    }

    /// Returns the provider-reported exchange after exact query matching.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Returns the provider-reported currency after exact query matching.
    #[must_use]
    pub const fn currency(&self) -> &ContractCurrency {
        &self.currency
    }

    /// Returns the exact provider OCC local symbol.
    #[must_use]
    pub fn local_symbol(&self) -> &str {
        &self.local_symbol
    }

    /// Returns the typed source reference for this provider record.
    #[must_use]
    pub const fn record(&self) -> &ProviderMetadataRef {
        &self.record
    }
}

pub(crate) fn map_contract_details(
    query: &IbkrOptionCatalogQuery,
    details: ContractDetails,
) -> Result<IbkrOptionCatalogEntry, IbkrCatalogError> {
    let contract = details.contract;
    let expected_expiration = query.symbol().expiration();
    let expected_date = format!(
        "{:04}{:02}{:02}",
        expected_expiration.year(),
        expected_expiration.month(),
        expected_expiration.day()
    );
    let expected_right = match query.symbol().right() {
        domain::OptionRight::Call => OptionRight::Call,
        domain::OptionRight::Put => OptionRight::Put,
    };
    let expected_strike = f64::from(query.symbol().strike().mills()) / 1_000.0;

    let returned_symbol = OptionSymbol::parse(&contract.local_symbol)
        .map_err(|_| IbkrCatalogError::InvalidProviderRow)?;
    if contract.contract_id <= 0
        || contract.security_type != SecurityType::Option
        || contract.symbol.as_str() != query.symbol().underlying().as_str()
        || contract.last_trade_date_or_contract_month != expected_date
        || contract.right != Some(expected_right)
        || !contract.strike.is_finite()
        || contract.strike.to_bits() != expected_strike.to_bits()
        || contract.exchange.as_str() != query.exchange()
        || contract.currency.as_str() != query.currency().as_str()
        || returned_symbol != *query.symbol()
    {
        return Err(IbkrCatalogError::InvalidProviderRow);
    }

    let contract_id =
        NonZeroI32::new(contract.contract_id).ok_or(IbkrCatalogError::InvalidProviderRow)?;
    let provider_record_id = ProviderRecordId::new(format!("IBKR_CONID_{}", contract_id.get()))
        .map_err(|_| IbkrCatalogError::InvalidProviderRow)?;
    let record = ProviderMetadataRef::new(
        MetadataSource::MarketData(MarketDataProviderId::InteractiveBrokers),
        ProviderMetadataKind::Contract,
        provider_record_id,
    );

    let trading_class = TradingClass::new(&contract.trading_class).map_or_else(
        |_| ProviderValue::Unknown(record.clone()),
        ProviderValue::Known,
    );
    let currency = ContractCurrency::new(contract.currency.as_str()).map_or_else(
        |_| ProviderValue::Unknown(record.clone()),
        ProviderValue::Known,
    );
    let multiplier = validated_multiplier(&contract.multiplier).map_or_else(
        || ProviderValue::Unknown(record.clone()),
        ProviderValue::Known,
    );

    let candidate = OptionInstrumentCandidate::new(
        returned_symbol,
        trading_class,
        currency,
        multiplier,
        ProviderValue::Unknown(record.clone()),
        ProviderValue::Unknown(record.clone()),
        ProviderValue::Unknown(record.clone()),
    );
    let provider_identity = IbkrProviderContractIdentity {
        contract_id,
        exchange: contract.exchange.as_str().to_string(),
        currency: ContractCurrency::new(contract.currency.as_str())
            .map_err(|_| IbkrCatalogError::InvalidProviderRow)?,
        local_symbol: contract.local_symbol,
        record,
    };

    Ok(IbkrOptionCatalogEntry {
        candidate,
        provider_identity,
    })
}

fn validated_multiplier(value: &str) -> Option<ContractMultiplier> {
    // The SDK renders its decoded numeric multiplier as a decimal string. Accept
    // only an integer representation; ContractMultiplier enforces the shared
    // safe-integer bound. Fractional or non-canonical provider values stay Unknown.
    ContractMultiplier::new(value.parse::<u64>().ok()?).ok()
}

#[cfg(test)]
mod tests {
    use domain::{ContractMultiplier, ProviderValue};
    use ibapi::contracts::{
        Contract, ContractDetails, Currency, Exchange, OptionRight, SecurityType, Symbol,
    };

    use super::{IbkrCatalogError, map_contract_details};
    use crate::IbkrOptionCatalogQuery;

    fn query() -> IbkrOptionCatalogQuery {
        IbkrOptionCatalogQuery::new(
            domain::OptionSymbol::parse("AAPL  270115C00150000").expect("valid synthetic OCC"),
            "CBOE",
            "USD",
        )
        .expect("valid query")
    }

    fn valid_details(contract_id: i32, multiplier: &str) -> ContractDetails {
        ContractDetails {
            contract: Contract {
                contract_id,
                symbol: Symbol::from("AAPL"),
                security_type: SecurityType::Option,
                last_trade_date_or_contract_month: "20270115".to_string(),
                strike: 150.0,
                right: Some(OptionRight::Call),
                multiplier: multiplier.to_string(),
                exchange: Exchange::from("CBOE"),
                currency: Currency::from("USD"),
                local_symbol: "AAPL  270115C00150000".to_string(),
                trading_class: "AAPL".to_string(),
                ..Contract::default()
            },
            ..ContractDetails::default()
        }
    }

    #[test]
    fn valid_provider_row_keeps_identity_and_unreported_economics_unknown() {
        let entry = map_contract_details(&query(), valid_details(123_456, "100"))
            .expect("valid provider row");
        let candidate = entry.candidate();

        assert_eq!(entry.provider_identity().contract_id(), 123_456);
        assert_eq!(entry.provider_identity().exchange(), "CBOE");
        assert_eq!(entry.provider_identity().currency().as_str(), "USD");
        assert_eq!(
            entry.provider_identity().local_symbol(),
            "AAPL  270115C00150000"
        );
        assert!(
            matches!(candidate.multiplier_evidence(), ProviderValue::Known(value) if *value == ContractMultiplier::new(100).unwrap())
        );
        assert!(matches!(
            candidate.deliverable_evidence(),
            ProviderValue::Unknown(_)
        ));
        assert!(matches!(
            candidate.exercise_style_evidence(),
            ProviderValue::Unknown(_)
        ));
        assert!(matches!(
            candidate.settlement_type_evidence(),
            ProviderValue::Unknown(_)
        ));
        assert!(
            candidate.qualify().is_err(),
            "catalog identity must not qualify incomplete economics"
        );
    }

    #[test]
    fn identity_mismatch_and_nonintegral_multiplier_do_not_become_known() {
        let mut wrong_route = valid_details(123_456, "100");
        wrong_route.contract.exchange = Exchange::from("SMART");
        assert_eq!(
            map_contract_details(&query(), wrong_route),
            Err(IbkrCatalogError::InvalidProviderRow)
        );

        let entry = map_contract_details(&query(), valid_details(123_456, "100.5"))
            .expect("identity still valid");
        assert!(matches!(
            entry.candidate().multiplier_evidence(),
            ProviderValue::Unknown(_)
        ));
    }
}
