use crate::messages::encode_protobuf_message;
use crate::testdata::responses::{MANAGED_ACCOUNT, MARKET_RULE};

use super::*;
use std::fs;
use tempfile::TempDir;

fn test_recorder(directory: &TempDir) -> MessageRecorder {
    MessageRecorder::recording_to(directory.path())
}

fn recorded_files(directory: &str) -> Vec<std::path::PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.map(|item| item.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()
        .unwrap()
}

#[test]
fn production_recorder_ignores_legacy_environment_variable() {
    let temp_dir = TempDir::new().unwrap();
    temp_env::with_var("IBAPI_RECORDING_DIR", Some(temp_dir.path().to_str().unwrap()), || {
        let recorder = MessageRecorder::disabled();
        assert!(!recorder.enabled);
        assert_eq!(recorder.recording_dir, "");

        recorder.record_request(b"synthetic request");
        recorder.record_response(&ResponseMessage::from_simple(MANAGED_ACCOUNT));
        assert!(fs::read_dir(temp_dir.path()).unwrap().next().is_none());
    });
}

#[test]
fn explicit_test_recorder_uses_temporary_directory() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_string_lossy();
    let recorder = test_recorder(&temp_dir);

    assert!(recorder.enabled);
    assert!(recorder.recording_dir.starts_with(path.as_ref()));
    assert_eq!(fs::canonicalize(&recorder.recording_dir).unwrap(), temp_dir.path());
}

#[test]
fn records_synthetic_request_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let data = encode_protobuf_message(63, &[0x08, 0xd0, 0x46]);
    let recorder = test_recorder(&temp_dir);

    recorder.record_request(&data);

    let files = recorded_files(&recorder.recording_dir);
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with("-request.msg"));
    assert_eq!(fs::read(&files[0]).unwrap(), data);
}

#[test]
fn records_synthetic_text_response() {
    let temp_dir = TempDir::new().unwrap();
    let message = ResponseMessage::from_simple(MARKET_RULE);
    let recorder = test_recorder(&temp_dir);

    recorder.record_response(&message);

    let files = recorded_files(&recorder.recording_dir);
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with("-response.msg"));
    assert_eq!(fs::read_to_string(&files[0]).unwrap(), message.encode_simple());
}

#[test]
fn records_synthetic_protobuf_response_frame() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![0x08, 0xd0, 0x46];
    let message = ResponseMessage::from_protobuf(crate::messages::IncomingMessages::CurrentTime as i32, payload.clone());
    let recorder = test_recorder(&temp_dir);

    recorder.record_response(&message);

    let file = recorded_files(&recorder.recording_dir).pop().unwrap();
    let content = fs::read(&file).unwrap();
    assert_eq!(
        content,
        encode_protobuf_message(crate::messages::IncomingMessages::CurrentTime as i32, &payload),
        "a recorded response must be the wire frame a replay would read"
    );
    assert!(content.ends_with(&payload), "the synthetic payload must survive the round trip");
}

#[test]
fn records_multiple_synthetic_messages() {
    let temp_dir = TempDir::new().unwrap();
    let request = encode_protobuf_message(1, &[]);
    let response = ResponseMessage::from_simple(MANAGED_ACCOUNT);
    let recorder = test_recorder(&temp_dir);

    recorder.record_request(&request);
    recorder.record_response(&response);

    assert_eq!(recorded_files(&recorder.recording_dir).len(), 2);
}

#[test]
fn records_unrecognized_synthetic_message_id_verbatim() {
    use crate::common::test_utils::helpers::UNKNOWN_MESSAGE_ID;

    let temp_dir = TempDir::new().unwrap();
    let payload = vec![0x08, 0x64];
    let message = ResponseMessage::from_protobuf(UNKNOWN_MESSAGE_ID, payload.clone());
    assert_eq!(message.message_type(), crate::messages::IncomingMessages::NotValid);
    let recorder = test_recorder(&temp_dir);

    recorder.record_response(&message);

    let file = recorded_files(&recorder.recording_dir).pop().unwrap();
    assert_eq!(
        fs::read(file).unwrap(),
        encode_protobuf_message(UNKNOWN_MESSAGE_ID, &payload),
        "the synthetic frame must retain its wire id"
    );
}
