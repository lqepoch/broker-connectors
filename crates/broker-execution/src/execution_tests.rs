use super::{
    ExecutionAcceptanceError, ExecutionCancelRequest, ExecutionCommand, ExecutionOutcome,
    ExecutionOutcomeCategory, ExecutionOutcomeReason, ExecutionReplaceRequest,
    ExecutionRequestError, ExecutionSubmitRequest, ProviderOrderAcknowledgement,
    ProviderOrderReplacementLink, validate_intent,
};
use domain::{
    AccountNamespace, AccountScope, BrokerEnvironment, BrokerNativeOrderReference,
    ContractCurrency, ContractMultiplier, Deliverable, DeliverableComponent, ExactDecimal,
    ExecutionBrokerId, ExecutionRoute, IntentId, LegRatio, LogicalOrderId, NetOrderPrice,
    OptionComboIntent, OptionContract, OptionExerciseStyle, OptionInstrumentKey, OptionOrderLeg,
    OptionRight, OptionSettlementType, OptionSymbol, OrderSide, PositionEffect, Price,
    ProviderOrderEvidence, ProviderOrderIdentity, ProviderRecordId, ProviderUnavailableReason,
    Quantity, Revision, RoutedOrderIdentity, StrategyInstanceId, Strike, TradingClass, Underlying,
};

fn route(environment: BrokerEnvironment) -> ExecutionRoute {
    ExecutionRoute::new(
        StrategyInstanceId::new("synthetic-exec-strategy").expect("valid synthetic strategy"),
        AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            environment,
            AccountScope::new("synthetic-exec-account").expect("valid synthetic account"),
        ),
    )
}

fn instrument(strike: &str) -> domain::InstrumentKey {
    let underlying = Underlying::new("SPY").expect("valid synthetic underlying");
    let deliverable = Deliverable::new(vec![
        DeliverableComponent::equity(
            underlying.clone(),
            ExactDecimal::parse_json_number("100").expect("valid synthetic deliverable"),
        )
        .expect("valid synthetic equity deliverable"),
    ])
    .expect("valid synthetic deliverable basket");
    let contract = OptionContract::new(
        OptionSymbol::new(
            underlying,
            domain::ExpirationDate::new(2027, 1, 15).expect("valid synthetic expiration"),
            OptionRight::Call,
            Strike::parse_json_number(strike).expect("valid synthetic strike"),
        ),
        ContractMultiplier::new(100).expect("valid synthetic multiplier"),
        ContractCurrency::new("USD").expect("valid synthetic currency"),
        deliverable,
    );
    domain::InstrumentKey::option(OptionInstrumentKey::new(
        contract,
        TradingClass::new("SPY").expect("valid synthetic trading class"),
        OptionExerciseStyle::American,
        OptionSettlementType::Physical,
    ))
}

fn intent(
    intent_id: &str,
    logical_order_id: &str,
    route: ExecutionRoute,
    first: &str,
    second: &str,
) -> OptionComboIntent {
    OptionComboIntent::new(
        IntentId::new(intent_id).expect("valid synthetic intent ID"),
        LogicalOrderId::new(logical_order_id).expect("valid synthetic logical order ID"),
        route,
        Quantity::new(1).expect("valid synthetic quantity"),
        vec![
            OptionOrderLeg::new(
                instrument(first),
                OrderSide::Buy,
                PositionEffect::Open,
                LegRatio::new(1).expect("valid ratio"),
                Quantity::new(1).expect("valid quantity"),
            ),
            OptionOrderLeg::new(
                instrument(second),
                OrderSide::Sell,
                PositionEffect::Open,
                LegRatio::new(1).expect("valid ratio"),
                Quantity::new(1).expect("valid quantity"),
            ),
        ],
        NetOrderPrice::net_debit(Price::parse_json_number("1.25").expect("valid exact price"))
            .expect("positive synthetic debit"),
    )
    .expect("valid synthetic option-combo intent")
}

fn provider_identity(account: AccountNamespace, raw_id: &str) -> ProviderOrderIdentity {
    ProviderOrderIdentity::new(
        account,
        BrokerNativeOrderReference::Alpaca(domain::AlpacaNativeOrderId::new(
            ProviderRecordId::new(raw_id).expect("valid synthetic provider order ID"),
        )),
    )
    .expect("provider identity matches synthetic account")
}

fn current_order(intent: &OptionComboIntent, provider_id: &str) -> RoutedOrderIdentity {
    current_order_with_identity(
        intent,
        provider_identity(intent.route().account_namespace().clone(), provider_id),
    )
}

fn current_order_with_identity(
    intent: &OptionComboIntent,
    provider_order: ProviderOrderIdentity,
) -> RoutedOrderIdentity {
    RoutedOrderIdentity::new(
        intent.intent_id().clone(),
        intent.logical_order_id().clone(),
        intent.route().clone(),
        ProviderOrderEvidence::Known(provider_order),
    )
    .expect("valid synthetic routed order identity")
}

#[test]
fn execution_submit_accepts_exact_core_intent_and_rejects_live_route() {
    let paper = intent(
        "synthetic-intent-submit",
        "synthetic-logical-submit",
        route(BrokerEnvironment::Paper),
        "600",
        "605",
    );
    let request = ExecutionSubmitRequest::new(paper, Revision::new(0))
        .expect("synthetic Paper intent is admissible to the software port");
    assert_eq!(request.expected_revision(), Revision::new(0));
    assert_eq!(
        request.intent().net_price().amount().decimal().to_string(),
        "1.25"
    );

    let live = intent(
        "synthetic-intent-live",
        "synthetic-logical-live",
        route(BrokerEnvironment::Live),
        "600",
        "605",
    );
    assert_eq!(
        ExecutionSubmitRequest::new(live, Revision::new(0)).err(),
        Some(ExecutionRequestError::LiveDisabled),
    );
}

#[test]
fn execution_rejects_duplicate_qualified_legs_and_revision_overflow() {
    let duplicate = OptionComboIntent::new(
        IntentId::new("synthetic-intent-duplicate").unwrap(),
        LogicalOrderId::new("synthetic-logical-duplicate").unwrap(),
        route(BrokerEnvironment::Paper),
        Quantity::new(1).unwrap(),
        vec![
            OptionOrderLeg::new(
                instrument("600"),
                OrderSide::Buy,
                PositionEffect::Open,
                LegRatio::new(1).unwrap(),
                Quantity::new(1).unwrap(),
            ),
            OptionOrderLeg::new(
                instrument("600"),
                OrderSide::Sell,
                PositionEffect::Open,
                LegRatio::new(1).unwrap(),
                Quantity::new(1).unwrap(),
            ),
        ],
        NetOrderPrice::ZeroCost,
    )
    .expect("core permits generic duplicate legs; execution contract must reject them");
    assert_eq!(
        validate_intent(&duplicate),
        Err(ExecutionRequestError::DuplicateInstrument),
    );

    let ordinary = intent(
        "synthetic-intent-overflow",
        "synthetic-logical-overflow",
        route(BrokerEnvironment::Paper),
        "600",
        "605",
    );
    assert_eq!(
        ExecutionSubmitRequest::new(ordinary, Revision::new(u64::MAX)).err(),
        Some(ExecutionRequestError::RevisionExhausted),
    );
}

fn replace_command() -> (ExecutionCommand, ProviderOrderIdentity) {
    let original = intent(
        "synthetic-intent-original",
        "synthetic-logical-replace",
        route(BrokerEnvironment::Paper),
        "600",
        "605",
    );
    let replacement = intent(
        "synthetic-intent-replacement",
        "synthetic-logical-replace",
        route(BrokerEnvironment::Paper),
        "600",
        "610",
    );
    let identity = provider_identity(
        original.route().account_namespace().clone(),
        "synthetic-provider-order-1",
    );
    let current = current_order_with_identity(&original, identity.clone());
    let request = ExecutionReplaceRequest::new(current, Revision::new(4), replacement)
        .expect("same synthetic route and logical order can be replaced");
    (ExecutionCommand::Replace(Box::new(request)), identity)
}

#[test]
fn replace_request_requires_known_identity_and_fixed_route_lineage() {
    let original = intent(
        "synthetic-intent-validate-replace",
        "synthetic-logical-validate-replace",
        route(BrokerEnvironment::Paper),
        "600",
        "605",
    );
    let current = current_order(&original, "synthetic-provider-order-validate");
    let changed_logical = intent(
        "synthetic-intent-validate-replace-new",
        "synthetic-logical-other",
        route(BrokerEnvironment::Paper),
        "600",
        "610",
    );
    assert_eq!(
        ExecutionReplaceRequest::new(current.clone(), Revision::new(0), changed_logical).err(),
        Some(ExecutionRequestError::ReplacementLineageMismatch),
    );

    let changed_route = intent(
        "synthetic-intent-validate-replace-route",
        "synthetic-logical-validate-replace",
        ExecutionRoute::new(
            StrategyInstanceId::new("synthetic-other-strategy").unwrap(),
            current.route().account_namespace().clone(),
        ),
        "600",
        "610",
    );
    assert_eq!(
        ExecutionReplaceRequest::new(current, Revision::new(0), changed_route).err(),
        Some(ExecutionRequestError::ReplacementLineageMismatch),
    );

    let unknown = RoutedOrderIdentity::new(
        original.intent_id().clone(),
        original.logical_order_id().clone(),
        original.route().clone(),
        ProviderOrderEvidence::Unavailable(ProviderUnavailableReason::NotReported),
    )
    .unwrap();
    let replacement = intent(
        "synthetic-intent-validate-replace-unknown",
        "synthetic-logical-validate-replace",
        route(BrokerEnvironment::Paper),
        "600",
        "610",
    );
    assert_eq!(
        ExecutionReplaceRequest::new(unknown, Revision::new(0), replacement).err(),
        Some(ExecutionRequestError::ProviderIdentityUnknown),
    );
}

#[test]
fn replace_same_identity_binds_logical_route_and_next_revision() {
    let (command, identity) = replace_command();
    let current_intent = match &command {
        ExecutionCommand::Replace(request) => request.replacement().intent_id().clone(),
        ExecutionCommand::Submit(_) | ExecutionCommand::Cancel(_) => unreachable!(),
    };
    let accepted = ExecutionOutcome::accepted_for(
        &command,
        ProviderOrderAcknowledgement::Replace(Box::new(
            ProviderOrderReplacementLink::same_identity(identity.clone()),
        )),
        Revision::new(5),
    )
    .expect("same-ID replacement ACK binds the new intent and revision");
    assert_eq!(accepted.category(), ExecutionOutcomeCategory::Accepted);
    assert_eq!(accepted.accepted_revision(), Some(Revision::new(5)));
    assert_eq!(
        accepted.accepted_order().unwrap().intent_id(),
        &current_intent
    );

    let wrong_revision = ExecutionOutcome::accepted_for(
        &command,
        ProviderOrderAcknowledgement::Replace(Box::new(
            ProviderOrderReplacementLink::same_identity(identity),
        )),
        Revision::new(7),
    );
    assert_eq!(
        wrong_revision.err(),
        Some(ExecutionAcceptanceError::RevisionMismatch),
    );
}

#[test]
fn replace_new_identity_requires_exact_parent_and_same_namespace() {
    let (command, identity) = replace_command();
    let changed_identity = provider_identity(
        match &command {
            ExecutionCommand::Replace(request) => {
                request.current().route().account_namespace().clone()
            }
            _ => unreachable!(),
        },
        "synthetic-provider-order-other",
    );
    let valid_new_identity = ExecutionOutcome::accepted_for(
        &command,
        ProviderOrderAcknowledgement::Replace(Box::new(
            ProviderOrderReplacementLink::from_reported_replaces(
                identity.clone(),
                changed_identity.clone(),
                Some(identity.clone()),
            )
            .expect("provider-reported parent exactly links the replacement"),
        )),
        Revision::new(5),
    )
    .expect("a new provider ID is accepted when its exact predecessor is linked");
    assert_eq!(
        valid_new_identity
            .accepted_order()
            .and_then(|order| match order.provider_order() {
                ProviderOrderEvidence::Known(identity) => Some(identity),
                _ => None,
            }),
        Some(&changed_identity),
    );

    assert_eq!(
        ProviderOrderReplacementLink::from_reported_replaces(
            identity.clone(),
            changed_identity.clone(),
            None,
        )
        .err(),
        Some(ExecutionAcceptanceError::ReplaceLinkMissing),
    );
    let wrong_parent = provider_identity(
        match &command {
            ExecutionCommand::Replace(request) => {
                request.current().route().account_namespace().clone()
            }
            _ => unreachable!(),
        },
        "synthetic-provider-order-wrong-parent",
    );
    assert_eq!(
        ProviderOrderReplacementLink::from_reported_replaces(
            identity.clone(),
            changed_identity.clone(),
            Some(wrong_parent),
        )
        .err(),
        Some(ExecutionAcceptanceError::ReplacePredecessorMismatch),
    );
    let other_account = AccountNamespace::new(
        ExecutionBrokerId::Alpaca,
        BrokerEnvironment::Paper,
        AccountScope::new("synthetic-other-exec-account").expect("valid synthetic account"),
    );
    let cross_account_identity =
        provider_identity(other_account, "synthetic-provider-cross-account");
    assert_eq!(
        ProviderOrderReplacementLink::from_reported_replaces(
            identity.clone(),
            cross_account_identity,
            Some(identity.clone()),
        )
        .err(),
        Some(ExecutionAcceptanceError::NamespaceMismatch),
    );

    let wrong_revision = ExecutionOutcome::accepted_for(
        &command,
        ProviderOrderAcknowledgement::Replace(Box::new(
            ProviderOrderReplacementLink::same_identity(identity),
        )),
        Revision::new(7),
    );
    assert_eq!(
        wrong_revision.err(),
        Some(ExecutionAcceptanceError::RevisionMismatch),
    );
}

#[test]
fn cancel_acceptance_cannot_change_provider_identity_and_unknown_stays_unknown() {
    let intent = intent(
        "synthetic-intent-cancel",
        "synthetic-logical-cancel",
        route(BrokerEnvironment::Paper),
        "600",
        "605",
    );
    let current = current_order(&intent, "synthetic-provider-order-cancel");
    let cancel = ExecutionCancelRequest::new(current.clone(), Revision::new(2))
        .expect("known synthetic provider order may be cancelled through the port contract");
    let command = ExecutionCommand::Cancel(Box::new(cancel));
    let changed_identity = provider_identity(
        current.route().account_namespace().clone(),
        "synthetic-provider-order-other",
    );
    assert_eq!(
        ExecutionOutcome::accepted_for(
            &command,
            ProviderOrderAcknowledgement::Cancel(Box::new(changed_identity)),
            Revision::new(3),
        )
        .err(),
        Some(ExecutionAcceptanceError::CancelIdentityMismatch),
    );
    let unknown = ExecutionOutcome::unknown(ExecutionOutcomeReason::DeadlineElapsed);
    assert_eq!(unknown.category(), ExecutionOutcomeCategory::Unknown);
    assert_eq!(
        unknown.reason(),
        Some(ExecutionOutcomeReason::DeadlineElapsed)
    );
    assert!(unknown.accepted_order().is_none());
    assert_eq!(unknown.accepted_revision(), None);
}
