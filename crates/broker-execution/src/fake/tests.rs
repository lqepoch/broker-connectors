use super::{FakeExecutionOutcomePlan, FakeExecutionPort, FakeExecutionPortError};
use crate::{
    ExecutionCancelRequest, ExecutionCommandCategory, ExecutionOutcomeCategory,
    ExecutionOutcomeReason, ExecutionPort, ExecutionReplaceRequest, ExecutionSubmitRequest,
};
use domain::{
    AccountNamespace, AccountScope, AlpacaNativeOrderId, BrokerEnvironment,
    BrokerNativeOrderReference, ContractCurrency, ContractMultiplier, Deliverable,
    DeliverableComponent, ExactDecimal, ExecutionBrokerId, ExecutionRoute, IntentId, LegRatio,
    LogicalOrderId, NetOrderPrice, OptionComboIntent, OptionContract, OptionExerciseStyle,
    OptionInstrumentKey, OptionOrderLeg, OptionRight, OptionSettlementType, OptionSymbol,
    OrderSide, PositionEffect, Price, ProviderOrderEvidence, ProviderOrderIdentity,
    ProviderRecordId, Quantity, Revision, RoutedOrderIdentity, StrategyInstanceId, Strike,
    TradingClass, Underlying,
};
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn block_fake<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("offline fake must complete without an executor"),
    }
}

fn route() -> ExecutionRoute {
    ExecutionRoute::new(
        StrategyInstanceId::new("synthetic-offline-strategy").unwrap(),
        AccountNamespace::new(
            ExecutionBrokerId::Alpaca,
            BrokerEnvironment::Paper,
            AccountScope::new("synthetic-offline-account").unwrap(),
        ),
    )
}

fn instrument(strike: &str) -> domain::InstrumentKey {
    let underlying = Underlying::new("SPY").unwrap();
    let deliverable = Deliverable::new(vec![
        DeliverableComponent::equity(
            underlying.clone(),
            ExactDecimal::parse_json_number("100").unwrap(),
        )
        .unwrap(),
    ])
    .unwrap();
    let contract = OptionContract::new(
        OptionSymbol::new(
            underlying,
            domain::ExpirationDate::new(2027, 1, 15).unwrap(),
            OptionRight::Call,
            Strike::parse_json_number(strike).unwrap(),
        ),
        ContractMultiplier::new(100).unwrap(),
        ContractCurrency::new("USD").unwrap(),
        deliverable,
    );
    domain::InstrumentKey::option(OptionInstrumentKey::new(
        contract,
        TradingClass::new("SPY").unwrap(),
        OptionExerciseStyle::American,
        OptionSettlementType::Physical,
    ))
}

fn intent(intent_id: &str, logical_id: &str, second_strike: &str) -> OptionComboIntent {
    OptionComboIntent::new(
        IntentId::new(intent_id).unwrap(),
        LogicalOrderId::new(logical_id).unwrap(),
        route(),
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
                instrument(second_strike),
                OrderSide::Sell,
                PositionEffect::Open,
                LegRatio::new(1).unwrap(),
                Quantity::new(1).unwrap(),
            ),
        ],
        NetOrderPrice::net_debit(Price::parse_json_number("1.25").unwrap()).unwrap(),
    )
    .unwrap()
}

fn provider_order(raw_id: &str) -> ProviderOrderIdentity {
    provider_order_in(route().account_namespace().clone(), raw_id)
}

fn provider_order_in(account: AccountNamespace, raw_id: &str) -> ProviderOrderIdentity {
    ProviderOrderIdentity::new(
        account,
        BrokerNativeOrderReference::Alpaca(AlpacaNativeOrderId::new(
            ProviderRecordId::new(raw_id).unwrap(),
        )),
    )
    .unwrap()
}

fn current_order(raw_id: &str) -> RoutedOrderIdentity {
    let intent = intent(
        "synthetic-intent-current",
        "synthetic-logical-replace",
        "605",
    );
    RoutedOrderIdentity::new(
        intent.intent_id().clone(),
        intent.logical_order_id().clone(),
        intent.route().clone(),
        ProviderOrderEvidence::Known(provider_order(raw_id)),
    )
    .unwrap()
}

fn replace_request(current: RoutedOrderIdentity, intent_id: &str) -> ExecutionReplaceRequest {
    let replacement = intent(intent_id, "synthetic-logical-replace", "610");
    ExecutionReplaceRequest::new(current, Revision::new(1), replacement).unwrap()
}

fn large_synthetic_intent(sequence: usize) -> OptionComboIntent {
    let strategy = StrategyInstanceId::new("s".repeat(256)).unwrap();
    let account = AccountScope::new("a".repeat(256)).unwrap();
    let route = ExecutionRoute::new(
        strategy,
        AccountNamespace::new(ExecutionBrokerId::Alpaca, BrokerEnvironment::Paper, account),
    );
    let trading_class = TradingClass::new(&"T".repeat(256)).unwrap();
    let exercise = OptionExerciseStyle::Other(domain::ProviderCode::new("E".repeat(256)).unwrap());
    let settlement =
        OptionSettlementType::Other(domain::ProviderCode::new("S".repeat(256)).unwrap());
    let legs = (0..16)
        .map(|leg_index| {
            let underlying = Underlying::new(&format!("U{leg_index:05}")).unwrap();
            let deliverable = Deliverable::new(
                (0..16)
                    .map(|component_index| {
                        DeliverableComponent::equity(
                            Underlying::new(&format!("D{component_index:05}")).unwrap(),
                            ExactDecimal::parse_json_number("1").unwrap(),
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap();
            let contract = OptionContract::new(
                OptionSymbol::new(
                    underlying,
                    domain::ExpirationDate::new(2027, 1, 15).unwrap(),
                    OptionRight::Call,
                    Strike::parse_json_number(&(600 + leg_index).to_string()).unwrap(),
                ),
                ContractMultiplier::new(100).unwrap(),
                ContractCurrency::new("USD").unwrap(),
                deliverable,
            );
            OptionOrderLeg::new(
                domain::InstrumentKey::option(OptionInstrumentKey::new(
                    contract,
                    trading_class.clone(),
                    exercise.clone(),
                    settlement.clone(),
                )),
                if leg_index % 2 == 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                PositionEffect::Open,
                LegRatio::new(1).unwrap(),
                Quantity::new(1).unwrap(),
            )
        })
        .collect();
    OptionComboIntent::new(
        IntentId::new(format!("synthetic-large-intent-{sequence}")).unwrap(),
        LogicalOrderId::new(format!("synthetic-large-logical-{sequence}")).unwrap(),
        route,
        Quantity::new(1).unwrap(),
        legs,
        NetOrderPrice::net_debit(Price::parse_json_number("1.25").unwrap()).unwrap(),
    )
    .unwrap()
}

#[test]
fn offline_fake_runs_a_synthetic_submit_replace_cancel_lifecycle() {
    let fake = FakeExecutionPort::new(3).unwrap();
    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-order-initial"),
    )))
    .unwrap();
    let initial = intent("synthetic-intent-open", "synthetic-logical-lineage", "605");
    let submitted =
        block_fake(fake.submit(ExecutionSubmitRequest::new(initial, Revision::new(0)).unwrap()));
    assert_eq!(submitted.category(), ExecutionOutcomeCategory::Accepted);
    assert_eq!(submitted.accepted_revision(), Some(Revision::new(1)));
    let current = submitted.accepted_order().unwrap().clone();

    fake.script(FakeExecutionOutcomePlan::ReplaceAcknowledged {
        provider_order: Box::new(provider_order("synthetic-order-replacement")),
        replaces: Some(Box::new(provider_order("synthetic-order-initial"))),
    })
    .unwrap();
    let replacement = intent(
        "synthetic-intent-replace",
        "synthetic-logical-lineage",
        "610",
    );
    let replaced = block_fake(
        fake.replace(ExecutionReplaceRequest::new(current, Revision::new(1), replacement).unwrap()),
    );
    assert_eq!(replaced.category(), ExecutionOutcomeCategory::Accepted);
    assert_eq!(replaced.accepted_revision(), Some(Revision::new(2)));
    let expected_replacement = provider_order("synthetic-order-replacement");
    assert_eq!(
        replaced
            .accepted_order()
            .and_then(|order| match order.provider_order() {
                ProviderOrderEvidence::Known(identity) => Some(identity),
                _ => None,
            }),
        Some(&expected_replacement),
    );
    let current = replaced.accepted_order().unwrap().clone();

    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-order-replacement"),
    )))
    .unwrap();
    let cancelled =
        block_fake(fake.cancel(ExecutionCancelRequest::new(current, Revision::new(2)).unwrap()));
    assert_eq!(cancelled.category(), ExecutionOutcomeCategory::Accepted);
    assert_eq!(cancelled.accepted_revision(), Some(Revision::new(3)));

    let commands = fake.take_recorded_commands();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0].category(), ExecutionCommandCategory::Submit);
    assert_eq!(commands[1].category(), ExecutionCommandCategory::Replace);
    assert_eq!(commands[2].category(), ExecutionCommandCategory::Cancel);
}

#[test]
fn offline_fake_accepts_same_identity_replace_explicitly() {
    let fake = FakeExecutionPort::new(2).unwrap();
    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-order-same"),
    )))
    .unwrap();
    let submitted = block_fake(
        fake.submit(
            ExecutionSubmitRequest::new(
                intent("synthetic-intent-same", "synthetic-logical-replace", "605"),
                Revision::new(0),
            )
            .unwrap(),
        ),
    );
    let current = submitted.accepted_order().unwrap().clone();

    fake.script(FakeExecutionOutcomePlan::ReplaceAcknowledged {
        provider_order: Box::new(provider_order("synthetic-order-same")),
        replaces: None,
    })
    .unwrap();
    let replaced =
        block_fake(fake.replace(replace_request(current, "synthetic-intent-same-replace")));
    assert_eq!(replaced.category(), ExecutionOutcomeCategory::Accepted);
    assert_eq!(replaced.accepted_revision(), Some(Revision::new(2)));
}

#[test]
fn offline_fake_fails_closed_for_invalid_new_replace_identity_links() {
    let old = provider_order("synthetic-order-old");
    let other_parent = provider_order("synthetic-order-wrong-parent");
    let other_account = AccountNamespace::new(
        ExecutionBrokerId::Alpaca,
        BrokerEnvironment::Paper,
        AccountScope::new("synthetic-offline-other-account").unwrap(),
    );
    let invalid_plans = [
        FakeExecutionOutcomePlan::ReplaceAcknowledged {
            provider_order: Box::new(provider_order("synthetic-order-new-missing-link")),
            replaces: None,
        },
        FakeExecutionOutcomePlan::ReplaceAcknowledged {
            provider_order: Box::new(provider_order("synthetic-order-new-wrong-parent")),
            replaces: Some(Box::new(other_parent)),
        },
        FakeExecutionOutcomePlan::ReplaceAcknowledged {
            provider_order: Box::new(provider_order_in(
                other_account,
                "synthetic-order-new-cross-account",
            )),
            replaces: Some(Box::new(old)),
        },
    ];

    for plan in invalid_plans {
        let fake = FakeExecutionPort::new(1).unwrap();
        fake.script(plan).unwrap();
        let result = block_fake(fake.replace(replace_request(
            current_order("synthetic-order-old"),
            "synthetic-intent-invalid-replace",
        )));
        assert_eq!(result.category(), ExecutionOutcomeCategory::Unknown);
        assert_eq!(
            result.reason(),
            Some(ExecutionOutcomeReason::ProtocolViolation)
        );
    }
}

#[test]
fn offline_fake_enforces_aggregate_retained_data_budget_without_dropping_script() {
    let fake = FakeExecutionPort::new(crate::MAX_OFFLINE_FAKE_COMMANDS).unwrap();
    let mut hit_byte_limit = false;
    for sequence in 0..crate::MAX_OFFLINE_FAKE_COMMANDS {
        fake.script(FakeExecutionOutcomePlan::Unknown(
            ExecutionOutcomeReason::DeadlineElapsed,
        ))
        .unwrap();
        let request =
            ExecutionSubmitRequest::new(large_synthetic_intent(sequence), Revision::new(0))
                .unwrap();
        let outcome = block_fake(fake.submit(request));
        if outcome.category() == ExecutionOutcomeCategory::DefinitelyNotSent {
            assert_eq!(
                outcome.reason(),
                Some(ExecutionOutcomeReason::CapacityExceeded)
            );
            assert!(fake.recorded_commands() < crate::MAX_OFFLINE_FAKE_COMMANDS);
            assert_eq!(fake.queued_outcomes(), 1);
            hit_byte_limit = true;
            break;
        }
        assert_eq!(outcome.category(), ExecutionOutcomeCategory::Unknown);
    }
    assert!(
        hit_byte_limit,
        "large synthetic commands should reach the byte budget first"
    );
}

#[test]
fn offline_fake_preserves_unknown_and_surfaces_both_capacity_limits() {
    assert_eq!(
        FakeExecutionPort::new(0).err(),
        Some(FakeExecutionPortError::InvalidCapacity)
    );
    let fake = FakeExecutionPort::new(1).unwrap();
    fake.script(FakeExecutionOutcomePlan::Unknown(
        ExecutionOutcomeReason::DeadlineElapsed,
    ))
    .unwrap();
    assert_eq!(
        fake.script(FakeExecutionOutcomePlan::Unknown(
            ExecutionOutcomeReason::DeadlineElapsed,
        )),
        Err(FakeExecutionPortError::ScriptCapacityExceeded),
    );

    let submit = || {
        ExecutionSubmitRequest::new(
            intent(
                "synthetic-intent-bounded",
                "synthetic-logical-bounded",
                "605",
            ),
            Revision::new(0),
        )
        .unwrap()
    };
    let first = block_fake(fake.submit(submit()));
    assert_eq!(first.category(), ExecutionOutcomeCategory::Unknown);
    assert_eq!(
        first.reason(),
        Some(ExecutionOutcomeReason::DeadlineElapsed)
    );
    assert_eq!(fake.recorded_commands(), 1);

    let second = block_fake(fake.submit(submit()));
    assert_eq!(
        second.category(),
        ExecutionOutcomeCategory::DefinitelyNotSent
    );
    assert_eq!(
        second.reason(),
        Some(ExecutionOutcomeReason::CapacityExceeded)
    );
    assert_eq!(fake.recorded_commands(), 1);
    assert_eq!(fake.queued_outcomes(), 0);
    assert_eq!(fake.take_recorded_commands().len(), 1);
}

#[test]
fn fake_debug_never_discloses_scripted_provider_identity() {
    let fake = FakeExecutionPort::new(2).unwrap();
    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-native-order-secret"),
    )))
    .unwrap();
    let debug = format!("{fake:?}");
    assert!(!debug.contains("synthetic-native-order-secret"));
    let plan = FakeExecutionOutcomePlan::Accepted(Box::new(provider_order(
        "synthetic-native-order-secret",
    )));
    assert!(!format!("{plan:?}").contains("synthetic-native-order-secret"));
    let replace_plan = FakeExecutionOutcomePlan::ReplaceAcknowledged {
        provider_order: Box::new(provider_order("synthetic-new-order-secret")),
        replaces: Some(Box::new(provider_order("synthetic-old-order-secret"))),
    };
    let debug = format!("{replace_plan:?}");
    assert!(!debug.contains("synthetic-new-order-secret"));
    assert!(!debug.contains("synthetic-old-order-secret"));
}
