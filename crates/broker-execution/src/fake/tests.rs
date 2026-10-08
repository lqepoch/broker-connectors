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
    OrderSide, PositionEffect, Price, ProviderOrderIdentity, ProviderRecordId, Quantity, Revision,
    StrategyInstanceId, Strike, TradingClass, Underlying,
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
    ProviderOrderIdentity::new(
        route().account_namespace().clone(),
        BrokerNativeOrderReference::Alpaca(AlpacaNativeOrderId::new(
            ProviderRecordId::new(raw_id).unwrap(),
        )),
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

    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-order-initial"),
    )))
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
    let current = replaced.accepted_order().unwrap().clone();

    fake.script(FakeExecutionOutcomePlan::Accepted(Box::new(
        provider_order("synthetic-order-initial"),
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
}
