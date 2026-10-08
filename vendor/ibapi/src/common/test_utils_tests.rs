use super::helpers::*;
use crate::messages::{encode_protobuf_message, OutgoingMessages};
use crate::server_versions;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn test_create_test_client() {
    let (client, message_bus) = create_test_client();
    assert_eq!(client.server_version(), server_versions::SIZE_RULES);
    assert!(message_bus.request_messages.read().unwrap().is_empty());
    assert!(message_bus.response_messages.is_empty());
}

#[test]
fn test_create_test_client_with_version() {
    let custom_version = 150;
    let (client, message_bus) = create_test_client_with_version(custom_version);
    assert_eq!(client.server_version(), custom_version);
    assert!(message_bus.request_messages.read().unwrap().is_empty());
    assert!(message_bus.response_messages.is_empty());
}

#[test]
fn test_create_test_client_with_responses() {
    let responses = vec!["1|2|123|".to_string(), "2|2|456|".to_string()];
    let (client, message_bus) = create_test_client_with_responses(responses.clone());
    assert_eq!(client.server_version(), server_versions::SIZE_RULES);
    assert_eq!(message_bus.response_messages, responses);
}

#[test]
fn test_assert_request_msg_id() {
    let (_client, message_bus) = create_test_client();

    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(encode_protobuf_message(OutgoingMessages::RequestAccountSummary as i32, &[]));
    }

    assert_request_msg_id(&message_bus, 0, OutgoingMessages::RequestAccountSummary);
}

#[test]
fn test_request_message_count() {
    let (_client, message_bus) = create_test_client();

    assert_eq!(request_message_count(&message_bus), 0);

    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(encode_protobuf_message(1, &[]));
        request_messages.push(encode_protobuf_message(2, &[]));
    }

    assert_eq!(request_message_count(&message_bus), 2);
}

#[test]
fn test_constants() {
    assert_eq!(TEST_ACCOUNT, "DU1234567");
    assert_eq!(TEST_CONTRACT_ID, 1001);
    assert_eq!(TEST_ORDER_ID, 5001);
    assert_eq!(TEST_TICKER_ID, 100);
}

#[test]
fn assert_request_proto_matches_expected_body() {
    use crate::proto::ManagedAccountsRequest;
    use prost::Message;

    let (_client, message_bus) = create_test_client();

    let expected = ManagedAccountsRequest::default();
    let mut body = Vec::new();
    expected.encode(&mut body).unwrap();

    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(encode_protobuf_message(OutgoingMessages::RequestManagedAccounts as i32, &body));
    }

    assert_request_proto(&message_bus, 0, OutgoingMessages::RequestManagedAccounts, &expected);
}

#[test]
fn assert_request_helper_resolves_msg_id_from_builder() {
    use crate::testdata::builders::{positions::request_positions_multi, RequestEncoder};

    let (_client, message_bus) = create_test_client();

    let builder = request_positions_multi().account("DU9999999").model_code("TARGET2024");

    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(builder.encode_request());
    }

    assert_request(&message_bus, 0, &builder);
}

#[test]
#[should_panic(expected = "request 0 body mismatch")]
fn assert_request_helper_panics_on_body_mismatch() {
    use crate::testdata::builders::{positions::request_positions_multi, RequestEncoder};

    let (_client, message_bus) = create_test_client();

    let on_wire = request_positions_multi().account("DU0000001");
    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(on_wire.encode_request());
    }

    let expected = request_positions_multi().account("DU0000002");
    assert_request(&message_bus, 0, &expected);
}

#[test]
fn test_create_test_client_with_responses_and_version() {
    let responses = vec!["1|2|123|".to_string()];
    let custom_version = 150;
    let (client, message_bus) = create_test_client_with_responses_and_version(responses.clone(), custom_version);
    assert_eq!(client.server_version(), custom_version);
    assert_eq!(message_bus.response_messages, responses);
}

#[test]
#[should_panic(expected = "Expected at least 2 request messages, got 0")]
fn assert_request_msg_id_panics_on_short_buffer() {
    let (_client, message_bus) = create_test_client();
    assert_request_msg_id(&message_bus, 1, OutgoingMessages::RequestAccountSummary);
}

#[test]
fn assert_tws_error_message_matches_code_and_substring() {
    let err = tws_error_notice(10089, "not subscribed to market data");
    assert_tws_error_message(err, 10089, "not subscribed");
}

#[test]
#[should_panic(expected = "expected Error::Notice(code=10089)")]
fn assert_tws_error_message_panics_on_wrong_variant() {
    let err = crate::Error::Simple("nope".to_string());
    assert_tws_error_message(err, 10089, "not subscribed");
}

#[test]
#[should_panic(expected = "wrong error code")]
fn assert_tws_error_message_panics_on_wrong_code() {
    let err = tws_error_notice(1, "not subscribed");
    assert_tws_error_message(err, 10089, "not subscribed");
}

#[test]
#[should_panic(expected = "TWS notice text mismatch")]
fn assert_tws_error_message_panics_on_missing_substring() {
    let err = tws_error_notice(10089, "other text");
    assert_tws_error_message(err, 10089, "not subscribed");
}

#[test]
#[should_panic(expected = "request 0 body mismatch")]
fn assert_request_proto_panics_on_body_mismatch() {
    use crate::proto::AccountSummaryRequest;
    use prost::Message;

    let (_client, message_bus) = create_test_client();

    let on_wire = AccountSummaryRequest {
        req_id: Some(7),
        ..Default::default()
    };
    let mut body = Vec::new();
    on_wire.encode(&mut body).unwrap();

    {
        let mut request_messages = message_bus.request_messages.write().unwrap();
        request_messages.push(encode_protobuf_message(OutgoingMessages::RequestAccountSummary as i32, &body));
    }

    let expected = AccountSummaryRequest {
        req_id: Some(99),
        ..Default::default()
    };
    assert_request_proto(&message_bus, 0, OutgoingMessages::RequestAccountSummary, &expected);
}

#[test]
fn assertion_helper_panics_redact_synthetic_payloads() {
    fn panic_message(result: std::thread::Result<()>) -> String {
        let payload = result.expect_err("the assertion helper must reject the unexpected result");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|message| (*message).to_string()))
            .unwrap_or_else(|| String::from("non-string panic"))
    }

    let marker = "SYNTHETIC_ACCOUNT_SECRET_7842";
    let (_client, message_bus) = create_test_client();
    let on_wire = crate::proto::PositionsMultiRequest {
        req_id: Some(42),
        account: Some(marker.to_string()),
        model_code: Some("synthetic-model".to_string()),
    };
    let mut request_messages = message_bus.request_messages.write().unwrap();
    request_messages.push(crate::messages::encode_protobuf_message(
        OutgoingMessages::RequestPositionsMulti as i32,
        &prost::Message::encode_to_vec(&on_wire),
    ));
    drop(request_messages);
    let mismatched_request = crate::proto::PositionsMultiRequest {
        req_id: Some(42),
        account: Some("different-synthetic-account".to_string()),
        model_code: Some("synthetic-model".to_string()),
    };
    let request_panic = panic_message(catch_unwind(AssertUnwindSafe(|| {
        assert_request_proto(&message_bus, 0, OutgoingMessages::RequestPositionsMulti, &mismatched_request);
    })));

    let panics = [
        request_panic,
        panic_message(catch_unwind(AssertUnwindSafe(|| {
            assert_decimal_parse_error(Ok(marker.to_string()), marker);
        }))),
        panic_message(catch_unwind(AssertUnwindSafe(|| {
            assert_decimal_parse_error(Err::<String, _>(crate::Error::Simple(marker.to_string())), "expected");
        }))),
        panic_message(catch_unwind(AssertUnwindSafe(|| {
            assert_missing_field(Ok(marker.to_string()), marker, marker);
        }))),
        panic_message(catch_unwind(AssertUnwindSafe(|| {
            assert_rejects_text_framing(crate::messages::IncomingMessages::NotValid, &format!("999999|{marker}|"), |_| {
                Ok::<_, crate::Error>(marker.to_string())
            });
        }))),
        panic_message(catch_unwind(AssertUnwindSafe(|| {
            assert_tws_error_message(tws_error_notice(1, marker), 1, "missing marker");
        }))),
    ];

    assert!(
        panics.iter().all(|message| !message.contains(marker)),
        "panic diagnostics must omit synthetic payloads"
    );
    assert!(panics[0].contains("body mismatch"));
    assert!(panics[1].contains("success"));
    assert!(panics[2].contains("error"));
    assert!(panics[3].contains("success"));
    assert!(panics[4].contains("success"));

    let message = crate::messages::ResponseMessage::from_simple(&format!("11|{marker}|"));
    let summary = message.diagnostic_summary();
    let unexpected = crate::Error::unexpected_response(&message).to_string();
    let notice = crate::messages::Notice::synthesized(201, marker.to_string());
    let notice_summary = notice.diagnostic_summary();
    let routed_notice = crate::subscriptions::common::RoutedItem::Notice(notice.clone()).diagnostic_summary();
    let routed_error =
        crate::subscriptions::common::RoutedItem::Error(crate::Error::Parse(0, marker.to_string(), marker.to_string())).diagnostic_summary();
    let account_error = crate::Error::AccountUpdatesInUse {
        active: crate::accounts::types::AccountId::from(marker),
        requested: crate::accounts::types::AccountId::from("SYNTHETIC_OTHER_ACCOUNT"),
    };
    assert!(!summary.contains(marker));
    assert!(!unexpected.contains(marker));
    assert!(!notice_summary.contains(marker));
    assert!(!routed_notice.contains(marker));
    assert!(!routed_error.contains(marker));
    assert_eq!(account_error.diagnostic_class(), "account-updates-in-use");
}
