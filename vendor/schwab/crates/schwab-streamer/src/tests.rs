use super::*;

fn ack_for(command: &StreamerCommand, accepted: bool) -> CommandAcknowledgement {
    CommandAcknowledgement::new(
        command.connection_generation(),
        command.request_id(),
        command.service(),
        command.command(),
        accepted,
    )
}

#[test]
fn service_manifests_are_fixed_and_match_current_contract_evidence() {
    assert_eq!(SERVICE_MANIFESTS.len(), 3);
    assert_eq!(
        StreamerService::AcctActivity.manifest().name(),
        "ACCT_ACTIVITY"
    );
    assert_eq!(StreamerService::AcctActivity.manifest().fields(), "0,1,2,3");
    assert_eq!(
        StreamerService::LevelOneEquities.manifest().name(),
        "LEVELONE_EQUITIES"
    );
    assert_eq!(
        StreamerService::LevelOneEquities.manifest().fields(),
        "0,45,46,51,52"
    );
    assert_eq!(
        StreamerService::LevelOneOptions.manifest().name(),
        "LEVELONE_OPTIONS"
    );
    assert_eq!(
        StreamerService::LevelOneOptions.manifest().fields(),
        "0,2,3,38"
    );
}

#[test]
fn acknowledged_field_manifest_is_scoped_to_its_service_and_cleared_on_reconnect() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity desired state is valid");
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("option desired state is valid");
    assert_eq!(
        manager.desired_fields(StreamerService::AcctActivity),
        Some("0,1,2,3")
    );
    assert_eq!(
        manager.desired_fields(StreamerService::LevelOneEquities),
        Some("0,45,46,51,52")
    );
    assert_eq!(
        manager.desired_fields(StreamerService::LevelOneOptions),
        Some("0,2,3,38")
    );

    let replay = manager.reconnect().expect("initial connection succeeds");
    let equity = replay
        .get(StreamerService::LevelOneEquities)
        .expect("equity replay exists")
        .clone();
    let options = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneEquities),
        None
    );
    assert!(matches!(
        manager.acknowledge(ack_for(&equity, true)),
        AckDisposition::Accepted { .. }
    ));
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneEquities),
        Some("0,45,46,51,52")
    );
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneOptions),
        None
    );
    assert!(matches!(
        manager.acknowledge(ack_for(&options, true)),
        AckDisposition::Accepted { .. }
    ));
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneOptions),
        Some("0,2,3,38")
    );

    manager
        .reconnect()
        .expect("reconnect invalidates field ACKs");
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneEquities),
        None
    );
    assert_eq!(
        manager.acknowledged_fields(StreamerService::LevelOneOptions),
        None
    );
}

#[test]
fn reconnect_replays_only_current_desired_keys_with_fresh_subs_fences() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity desired state is valid");
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION-A"])
        .expect("option desired state is valid");

    let first_replay = manager.reconnect().expect("first connection succeeds");
    assert_eq!(first_replay.login_request_id().value(), 1);
    assert_eq!(first_replay.iter().count(), 3);
    assert_eq!(
        first_replay
            .get(StreamerService::AcctActivity)
            .expect("activity replay is present")
            .request_id()
            .value(),
        2
    );
    for service in SERVICE_MANIFESTS.map(super::manifest::ServiceManifest::service) {
        let command = first_replay
            .get(service)
            .expect("all three services have desired keys");
        assert_eq!(command.command(), SubscriptionCommand::Subs);
        assert_eq!(command.fields(), service.manifest().fields());
        assert_eq!(manager.readiness(service), ServiceReadiness::Pending);
        assert!(matches!(
            manager.acknowledge(ack_for(command, true)),
            AckDisposition::Accepted {
                readiness: ServiceReadiness::Ready
            }
        ));
    }

    let old_generation = manager
        .connection_generation()
        .expect("connected generation");
    let second_replay = manager.reconnect().expect("reconnect succeeds");
    assert!(
        manager
            .connection_generation()
            .expect("replacement generation")
            .value()
            > old_generation.value()
    );
    for service in SERVICE_MANIFESTS.map(super::manifest::ServiceManifest::service) {
        let command = second_replay
            .get(service)
            .expect("desired keys survive reconnect");
        assert_eq!(command.command(), SubscriptionCommand::Subs);
        assert_eq!(
            command.keys().collect::<Vec<_>>(),
            match service {
                StreamerService::AcctActivity => vec!["Account Activity"],
                StreamerService::LevelOneEquities => vec!["SYNTH-EQ"],
                StreamerService::LevelOneOptions => vec!["SYNTH-OPTION-A"],
            }
        );
        assert_eq!(manager.acknowledged_keys(service), None);
        assert_eq!(manager.readiness(service), ServiceReadiness::Pending);
    }
}

#[test]
fn failed_ack_degrades_only_its_service_and_keeps_desired_keys_retryable() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("equity desired state is valid");
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION-A"])
        .expect("option desired state is valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let activity = replay
        .get(StreamerService::AcctActivity)
        .expect("activity replay exists")
        .clone();
    let equity = replay
        .get(StreamerService::LevelOneEquities)
        .expect("equity replay exists")
        .clone();
    let options = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();

    assert!(matches!(
        manager.acknowledge(ack_for(&activity, true)),
        AckDisposition::Accepted { .. }
    ));
    assert!(matches!(
        manager.acknowledge(ack_for(&equity, true)),
        AckDisposition::Accepted { .. }
    ));
    assert_eq!(
        manager.acknowledge(ack_for(&options, false)),
        AckDisposition::Rejected
    );
    assert_eq!(
        manager.readiness(StreamerService::AcctActivity),
        ServiceReadiness::Ready
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneEquities),
        ServiceReadiness::Ready
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Degraded
    );
    assert_eq!(
        manager.desired_keys(StreamerService::LevelOneOptions),
        Some(vec!["SYNTH-OPTION-A".to_owned()])
    );

    let retry = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("retry planning is valid")
        .expect("failed option subscription remains retryable");
    assert_eq!(retry.command(), SubscriptionCommand::Subs);
    assert_eq!(retry.fields(), "0,2,3,38");
    assert_eq!(retry.keys_csv(), "SYNTH-OPTION-A");
    assert!(matches!(
        manager.acknowledge(ack_for(&retry, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
}

#[test]
fn timed_out_add_invalidates_ack_base_and_late_ack_cannot_restore_it() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["A"])
        .expect("initial desired state is valid");
    let first_replay = manager.reconnect().expect("initial connection succeeds");
    let activity = first_replay
        .get(StreamerService::AcctActivity)
        .expect("activity replay exists")
        .clone();
    let first = first_replay
        .get(StreamerService::LevelOneOptions)
        .expect("options replay exists")
        .clone();
    assert!(matches!(
        manager.acknowledge(ack_for(&activity, true)),
        AckDisposition::Accepted { .. }
    ));
    assert!(matches!(
        manager.acknowledge(ack_for(&first, true)),
        AckDisposition::Accepted { .. }
    ));

    manager
        .set_desired(StreamerService::LevelOneOptions, ["A", "B"])
        .expect("added desired key is valid");
    let add = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("delta planning succeeds")
        .expect("one new key plans ADD");
    assert_eq!(add.command(), SubscriptionCommand::Add);
    assert_eq!(add.keys_csv(), "B");
    let generation = add.connection_generation();
    let request_id = add.request_id();

    assert!(manager.timeout_command(StreamerService::LevelOneOptions, generation, request_id,));
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        None,
        "a potentially accepted mutation leaves no trusted remote base"
    );
    assert_eq!(
        manager.acknowledge(ack_for(&add, true)),
        AckDisposition::Ignored(AckIgnoreReason::UnknownRequest),
        "late ACK cannot restore a timed-out mutation"
    );

    let recovery = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("repair planning succeeds")
        .expect("invalidated ACK base forces a full subscription replay");
    assert_eq!(recovery.command(), SubscriptionCommand::Subs);
    assert_eq!(recovery.keys_csv(), "A,B");
    assert_ne!(recovery.request_id(), request_id);
    assert_eq!(
        manager.acknowledge(ack_for(&recovery, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    );
}

#[test]
fn timed_out_unsubscribe_with_empty_desire_stays_degraded_until_reconnect() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneEquities, ["EQ"])
        .expect("equity desired state is valid");
    manager
        .set_desired(StreamerService::LevelOneOptions, ["A", "B"])
        .expect("option desired state is valid");
    let replay = manager.reconnect().expect("initial connection succeeds");
    let activity = replay
        .get(StreamerService::AcctActivity)
        .expect("account activity replay exists")
        .clone();
    let equity = replay
        .get(StreamerService::LevelOneEquities)
        .expect("equity replay exists")
        .clone();
    let options = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();
    for command in [&activity, &equity, &options] {
        assert!(matches!(
            manager.acknowledge(ack_for(command, true)),
            AckDisposition::Accepted {
                readiness: ServiceReadiness::Ready
            }
        ));
    }

    manager
        .set_desired(StreamerService::LevelOneOptions, ["B"])
        .expect("one option key is removed");
    let unsubscribe = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("UNSUBS planning succeeds")
        .expect("removed key requires an unsubscribe");
    assert_eq!(unsubscribe.command(), SubscriptionCommand::Unsubs);
    assert_eq!(unsubscribe.keys_csv(), "A");
    assert!(manager.timeout_command(
        StreamerService::LevelOneOptions,
        unsubscribe.connection_generation(),
        unsubscribe.request_id(),
    ));
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        None
    );

    // A subsequent desired-state change must not erase uncertainty left by
    // the timed-out UNSUBS or invent a successful empty snapshot.
    manager
        .set_desired(StreamerService::LevelOneOptions, std::iter::empty::<&str>())
        .expect("empty desired state is valid");
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Degraded
    );
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        None
    );
    assert!(
        manager
            .next_command(StreamerService::LevelOneOptions)
            .expect("ambiguous empty recovery remains safe")
            .is_none()
    );
    assert_eq!(
        manager.acknowledge(ack_for(&unsubscribe, true)),
        AckDisposition::Ignored(AckIgnoreReason::UnknownRequest),
        "a late acknowledgement cannot restore timed-out remote state"
    );

    // The failure remains scoped to OPTIONS while the shared connection is
    // still usable by activity and equities. A replacement socket gives all
    // desired services a fresh generation and a known empty OPTIONS state.
    assert_eq!(
        manager.readiness(StreamerService::AcctActivity),
        ServiceReadiness::Ready
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneEquities),
        ServiceReadiness::Ready
    );
    let replacement = manager.reconnect().expect("new socket recovery succeeds");
    assert!(replacement.get(StreamerService::LevelOneOptions).is_none());
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        Some(Vec::new())
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Ready
    );
    assert!(replacement.get(StreamerService::AcctActivity).is_some());
    assert!(replacement.get(StreamerService::LevelOneEquities).is_some());
}

#[test]
fn stale_generation_and_mismatched_ack_fields_cannot_change_readiness() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION-A"])
        .expect("option desired state is valid");
    let first = manager.reconnect().expect("initial connection succeeds");
    let old = first
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();
    let generation = old.connection_generation();

    let wrong_service = CommandAcknowledgement::new(
        generation,
        old.request_id(),
        StreamerService::LevelOneEquities,
        old.command(),
        true,
    );
    assert_eq!(
        manager.acknowledge(wrong_service),
        AckDisposition::Ignored(AckIgnoreReason::WrongService)
    );
    let wrong_command = CommandAcknowledgement::new(
        generation,
        old.request_id(),
        old.service(),
        SubscriptionCommand::Add,
        true,
    );
    assert_eq!(
        manager.acknowledge(wrong_command),
        AckDisposition::Ignored(AckIgnoreReason::WrongCommand)
    );
    let unknown_request = CommandAcknowledgement::new(
        generation,
        RequestId::new(old.request_id().value().saturating_add(100)),
        old.service(),
        old.command(),
        true,
    );
    assert_eq!(
        manager.acknowledge(unknown_request),
        AckDisposition::Ignored(AckIgnoreReason::UnknownRequest)
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Pending
    );

    let second = manager
        .reconnect()
        .expect("replacement connection succeeds");
    let current = second
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists on replacement connection")
        .clone();
    assert_eq!(
        manager.acknowledge(ack_for(&old, true)),
        AckDisposition::Ignored(AckIgnoreReason::StaleConnection)
    );
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Pending
    );
    assert!(matches!(
        manager.acknowledge(ack_for(&current, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
}

#[test]
fn pure_key_additions_and_removals_use_deltas_and_mixed_change_uses_subs() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["B", "A", "A"])
        .expect("option desired state is valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let initial = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();
    assert_eq!(initial.command(), SubscriptionCommand::Subs);
    assert_eq!(initial.keys_csv(), "A,B");
    manager.acknowledge(ack_for(&initial, true));

    manager
        .set_desired(StreamerService::LevelOneOptions, ["A", "B", "C"])
        .expect("adding desired key succeeds");
    let add = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("ADD planning succeeds")
        .expect("new key requires ADD");
    assert_eq!(add.command(), SubscriptionCommand::Add);
    assert_eq!(add.keys_csv(), "C");
    manager.acknowledge(ack_for(&add, true));

    manager
        .set_desired(StreamerService::LevelOneOptions, ["A", "C"])
        .expect("removing desired key succeeds");
    let unsubs = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("UNSUBS planning succeeds")
        .expect("removed key requires UNSUBS");
    assert_eq!(unsubs.command(), SubscriptionCommand::Unsubs);
    assert_eq!(unsubs.keys_csv(), "B");
    manager.acknowledge(ack_for(&unsubs, true));

    manager
        .set_desired(StreamerService::LevelOneOptions, ["C", "D"])
        .expect("mixed desired change succeeds");
    let replace = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("mixed SUBS planning succeeds")
        .expect("mixed change requires replacement");
    assert_eq!(replace.command(), SubscriptionCommand::Subs);
    assert_eq!(replace.keys_csv(), "C,D");
    assert!(matches!(
        manager.acknowledge(ack_for(&replace, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
    let replay_after_update = manager
        .reconnect()
        .expect("reconnect after key removal succeeds");
    let replayed_options = replay_after_update
        .get(StreamerService::LevelOneOptions)
        .expect("current option keys replay");
    assert_eq!(replayed_options.command(), SubscriptionCommand::Subs);
    assert_eq!(replayed_options.keys_csv(), "C,D");
}

#[test]
fn desired_changes_during_pending_command_are_synced_after_its_ack() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-A"])
        .expect("initial option key is valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let first = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option replay exists")
        .clone();

    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-B"])
        .expect("new desired state is valid");
    assert!(
        manager
            .next_command(StreamerService::LevelOneOptions)
            .expect("pending command suppresses a second command")
            .is_none()
    );
    assert_eq!(
        manager.acknowledge(ack_for(&first, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::NeedsSync
        }
    );
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        Some(vec!["SYNTH-A".to_owned()])
    );
    let second = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("changed desired state can be planned")
        .expect("new snapshot differs from acknowledged keys");
    assert_eq!(second.command(), SubscriptionCommand::Subs);
    assert_eq!(second.keys_csv(), "SYNTH-B");
    assert!(matches!(
        manager.acknowledge(ack_for(&second, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
}

#[test]
fn key_input_limits_reject_atomically_and_deduplicate_canonically() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, [" SAFE ", "SAFE", ""])
        .expect("trimmed duplicates are canonicalized");
    let initial_revision = manager.desired_revision(StreamerService::LevelOneOptions);
    assert_eq!(
        manager.desired_keys(StreamerService::LevelOneOptions),
        Some(vec!["SAFE".to_owned()])
    );

    let too_many_unique = (0..=MAX_KEYS_PER_SERVICE)
        .map(|index| format!("KEY-{index}"))
        .collect::<Vec<_>>();
    assert_eq!(
        manager.set_desired(StreamerService::LevelOneOptions, too_many_unique),
        Err(ServiceStateError::TooManyKeys {
            limit: MAX_KEYS_PER_SERVICE
        })
    );
    assert_eq!(
        manager.set_desired(
            StreamerService::LevelOneOptions,
            std::iter::repeat_n("DUPLICATE", MAX_KEY_INPUT_ITEMS + 1)
        ),
        Err(ServiceStateError::TooManyInputKeys {
            limit: MAX_KEY_INPUT_ITEMS
        })
    );
    assert_eq!(
        manager.set_desired(StreamerService::LevelOneOptions, ["BAD,KEY"]),
        Err(ServiceStateError::InvalidKey)
    );
    assert_eq!(
        manager.set_desired(
            StreamerService::LevelOneOptions,
            ["x".repeat(MAX_KEY_BYTES + 1)]
        ),
        Err(ServiceStateError::KeyTooLong {
            limit: MAX_KEY_BYTES
        })
    );
    assert_eq!(
        manager.desired_revision(StreamerService::LevelOneOptions),
        initial_revision
    );
    assert_eq!(
        manager.desired_keys(StreamerService::LevelOneOptions),
        Some(vec!["SAFE".to_owned()])
    );
}

#[test]
fn serialized_key_byte_limit_is_enforced_before_state_mutation() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SAFE"])
        .expect("baseline desired state is valid");
    let initial_revision = manager.desired_revision(StreamerService::LevelOneOptions);
    let large_set = (0..MAX_KEYS_PER_SERVICE)
        .map(|index| format!("{}{:04}", "x".repeat(MAX_KEY_BYTES - 4), index))
        .collect::<Vec<_>>();
    assert_eq!(
        manager.set_desired(StreamerService::LevelOneOptions, large_set),
        Err(ServiceStateError::KeySetTooLarge {
            limit: MAX_SERIALIZED_KEY_BYTES
        })
    );
    assert_eq!(
        manager.desired_revision(StreamerService::LevelOneOptions),
        initial_revision
    );
    assert_eq!(
        manager.desired_keys(StreamerService::LevelOneOptions),
        Some(vec!["SAFE".to_owned()])
    );
}

#[test]
fn empty_service_is_not_subscribed_and_nonempty_desire_requires_connection() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, std::iter::empty::<&str>())
        .expect("empty desired set is valid");
    assert!(matches!(
        manager.next_command(StreamerService::LevelOneOptions),
        Ok(None)
    ));
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::Ready
    );
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-OPTION"])
        .expect("nonempty desired set is valid");
    assert_eq!(
        manager.next_command(StreamerService::LevelOneOptions),
        Err(ServiceStateError::NotConnected)
    );
    let replay = manager.reconnect().expect("connection succeeds");
    assert_eq!(
        replay
            .get(StreamerService::LevelOneOptions)
            .expect("nonempty option keys replay")
            .command(),
        SubscriptionCommand::Subs
    );
}

#[test]
fn removing_all_acknowledged_keys_plans_unsubscribe_and_acknowledges_empty_state() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-A", "SYNTH-B"])
        .expect("initial desired keys are valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let initial = replay
        .get(StreamerService::LevelOneOptions)
        .expect("nonempty desired keys replay")
        .clone();
    assert!(matches!(
        manager.acknowledge(ack_for(&initial, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));

    manager
        .set_desired(StreamerService::LevelOneOptions, std::iter::empty::<&str>())
        .expect("clearing desired keys is valid");
    assert_eq!(
        manager.readiness(StreamerService::LevelOneOptions),
        ServiceReadiness::NeedsSync
    );
    let unsubscribe = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("empty desired state syncs successfully")
        .expect("acknowledged keys must be removed");
    assert_eq!(unsubscribe.command(), SubscriptionCommand::Unsubs);
    assert_eq!(unsubscribe.keys_csv(), "SYNTH-A,SYNTH-B");
    assert!(matches!(
        manager.acknowledge(ack_for(&unsubscribe, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
    assert_eq!(
        manager.acknowledged_keys(StreamerService::LevelOneOptions),
        Some(Vec::new())
    );
}

#[test]
fn clearing_desired_keys_while_subs_is_pending_syncs_after_its_ack() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, ["SYNTH-A"])
        .expect("initial desired key is valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let initial = replay
        .get(StreamerService::LevelOneOptions)
        .expect("initial option SUBS exists")
        .clone();

    manager
        .set_desired(StreamerService::LevelOneOptions, std::iter::empty::<&str>())
        .expect("clearing desired keys is valid");
    assert!(
        manager
            .next_command(StreamerService::LevelOneOptions)
            .expect("pending command suppresses a concurrent command")
            .is_none()
    );
    assert_eq!(
        manager.acknowledge(ack_for(&initial, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::NeedsSync
        }
    );
    let unsubscribe = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("follow-up UNSUBS is valid")
        .expect("accepted in-flight SUBS must be removed");
    assert_eq!(unsubscribe.command(), SubscriptionCommand::Unsubs);
    assert_eq!(unsubscribe.keys_csv(), "SYNTH-A");
    assert!(matches!(
        manager.acknowledge(ack_for(&unsubscribe, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
}

#[test]
fn subscription_state_debug_redacts_desired_acknowledged_and_pending_keys() {
    let marker = "SYNTHETIC-OCC-DO-NOT-LOG";
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneOptions, [marker])
        .expect("synthetic key is valid");
    let replay = manager.reconnect().expect("connection succeeds");
    let command = replay
        .get(StreamerService::LevelOneOptions)
        .expect("option command exists")
        .clone();
    assert!(matches!(
        manager.acknowledge(ack_for(&command, true)),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
    manager
        .set_desired(StreamerService::LevelOneOptions, std::iter::empty::<&str>())
        .expect("clearing synthetic option key is valid");
    let pending_unsubscribe = manager
        .next_command(StreamerService::LevelOneOptions)
        .expect("unsubscribe planning succeeds")
        .expect("acknowledged key requires an unsubscribe");
    let debug = format!("{manager:?}{replay:?}{command:?}{pending_unsubscribe:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(marker));
}

#[test]
fn market_row_key_normalization_matches_single_key_set_validation() {
    let mut cases = vec![
        String::new(),
        " \t\n".to_owned(),
        "QQQ".to_owned(),
        "  QQQ  ".to_owned(),
        "QQQ,SPY".to_owned(),
        "QQQ\u{0000}SPY".to_owned(),
        "x".repeat(MAX_KEY_BYTES),
        "x".repeat(MAX_KEY_BYTES + 1),
        " 股票 ".to_owned(),
    ];
    cases.extend((0..512).map(|index| format!("SYNTH-{index:04}   260928P{:08}", 600_000 + index)));
    cases.extend((0..32).map(|index| format!("  SYNTH-{index:02},BAD  ")));

    for input in cases {
        let candidate = crate::state::BoundedKeySet::normalize_key(&input);
        let baseline = crate::state::BoundedKeySet::from_keys([input.as_str()]);
        assert_eq!(candidate.is_ok(), baseline.is_ok(), "input={input:?}");
        if let Ok(candidate) = candidate {
            let baseline = baseline.expect("candidate acceptance matches baseline");
            let baseline = baseline.iter().next().unwrap_or("");
            assert_eq!(candidate, baseline, "input={input:?}");
        }
    }

    let explicit_cases = [
        ("", Some("")),
        (" \t\n", Some("")),
        ("QQQ", Some("QQQ")),
        ("  QQQ  ", Some("QQQ")),
        ("QQQ\tSPY", None),
        ("QQQ,SPY", None),
    ];
    for (input, expected) in explicit_cases {
        match expected {
            Some(expected) => assert_eq!(
                crate::state::BoundedKeySet::normalize_key(input),
                Ok(expected),
                "input={input:?}"
            ),
            None => assert!(
                crate::state::BoundedKeySet::normalize_key(input).is_err(),
                "input={input:?}"
            ),
        }
    }
    let max_length_key = "x".repeat(MAX_KEY_BYTES);
    assert_eq!(
        crate::state::BoundedKeySet::normalize_key(&max_length_key),
        Ok(max_length_key.as_str()),
    );
    let over_length_key = "x".repeat(MAX_KEY_BYTES + 1);
    assert_eq!(
        crate::state::BoundedKeySet::normalize_key(&over_length_key),
        Err(ServiceStateError::KeyTooLong {
            limit: MAX_KEY_BYTES
        })
    );
}

#[test]
#[ignore = "repeatable local-only benchmark for per-row Streamer key validation"]
fn ignored_market_row_key_validation_benchmark() {
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    const SAMPLES: usize = 7;
    const ITERATIONS_PER_SAMPLE: usize = 250_000;

    let keys = (0..1024)
        .map(|index| match index % 4 {
            0 => format!("SYNTH-{index:04}"),
            _ => format!("SYNTH-{index:04}   260928P{:08}", 600_000 + index),
        })
        .collect::<Vec<_>>();

    let run = |candidate: bool| {
        let started = Instant::now();
        let mut accepted = 0usize;
        for index in 0..ITERATIONS_PER_SAMPLE {
            let key = black_box(keys[index % keys.len()].as_str());
            accepted += usize::from(if candidate {
                crate::state::BoundedKeySet::normalize_key(key).is_ok()
            } else {
                crate::state::BoundedKeySet::from_keys([key]).is_ok()
            });
        }
        black_box(accepted);
        started.elapsed().as_nanos()
    };

    let mut baseline = Vec::with_capacity(SAMPLES);
    let mut candidate = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        if sample % 2 == 0 {
            baseline.push(run(false));
            candidate.push(run(true));
        } else {
            candidate.push(run(true));
            baseline.push(run(false));
        }
    }
    baseline.sort_unstable();
    candidate.sort_unstable();
    let baseline_median_ns = baseline[SAMPLES / 2];
    let candidate_median_ns = candidate[SAMPLES / 2];
    let baseline_median_seconds = Duration::from_nanos(
        u64::try_from(baseline_median_ns).expect("local benchmark duration fits u64 nanoseconds"),
    )
    .as_secs_f64();
    let candidate_median_seconds = Duration::from_nanos(
        u64::try_from(candidate_median_ns).expect("local benchmark duration fits u64 nanoseconds"),
    )
    .as_secs_f64();
    println!(
        "MARKET_KEY_BENCH samples={SAMPLES} iterations_per_sample={ITERATIONS_PER_SAMPLE} corpus_keys={} baseline_median_ns={} candidate_median_ns={} speedup_percent={:.2}",
        keys.len(),
        baseline_median_ns,
        candidate_median_ns,
        (baseline_median_seconds / candidate_median_seconds - 1.0) * 100.0,
    );
}
