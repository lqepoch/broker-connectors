//! Offline contract checks for the public Schwab Streamer decoder and state API.
//! 仅验证公开 decoder 与订阅状态契约的离线合成用例。

use schwab_streamer::{
    AckDisposition, CommandAcknowledgement, ServiceReadiness, ServiceSubscriptionManager,
    StreamerService, parse_streamer_frame,
};

#[test]
fn public_surface_decodes_opaque_synthetic_wire_fields() {
    let frame = br#"{"data":[{"service":"LEVELONE_EQUITIES","timestamp":1,"command":"UPDATE","content":[{"key":"SYNTH-EQ","1":123.45,"2":7}]}]}"#;
    let decoded = parse_streamer_frame(frame).expect("bounded synthetic frame decodes");
    let row = &decoded.data.expect("data payload exists")[0].content[0];
    assert_eq!(row.key.as_deref(), Some("SYNTH-EQ"));
    assert_eq!(
        row.fields.get("1").map(ToString::to_string),
        Some("123.45".into())
    );
    assert_eq!(
        row.fields.get("2").map(ToString::to_string),
        Some("7".into())
    );
}

#[test]
fn public_subscription_state_tracks_ack_without_exposing_a_socket_runtime() {
    let mut manager = ServiceSubscriptionManager::new();
    manager
        .set_desired(StreamerService::LevelOneEquities, ["SYNTH-EQ"])
        .expect("synthetic desired key is valid");
    let replay = manager.reconnect().expect("local generation starts");
    let command = replay
        .get(StreamerService::LevelOneEquities)
        .expect("service command is generated");
    assert_eq!(
        manager.readiness(StreamerService::LevelOneEquities),
        ServiceReadiness::Pending
    );
    assert!(matches!(
        manager.acknowledge(CommandAcknowledgement::new(
            command.connection_generation(),
            command.request_id(),
            command.service(),
            command.command(),
            true,
        )),
        AckDisposition::Accepted {
            readiness: ServiceReadiness::Ready
        }
    ));
}
