use super::*;
use serde_json::Value;

fn parse(input: &str) -> Result<StreamerWireFrame, StreamerWireError> {
    parse_streamer_frame(input.as_bytes())
}

#[test]
fn parses_response_data_notify_and_preserves_unknown_fields() {
    let frame = parse(
        r#"{
          "vendorFrame": {"version": 2},
          "response": [{
            "service": "ADMIN",
            "requestid": 42,
            "command": "LOGIN",
            "timestamp": 1800000000000,
            "content": {"code": "26", "msg": "OK", "vendorCode": "kept"},
            "vendorResponse": true
          }],
          "data": [{
            "service": "LEVELONE_OPTIONS",
            "timestamp": 1800000000001,
            "command": "SUBS",
            "content": [
              {"key": "SYNTH-OPTION", "2": 1.25, "vendorRow": {"keep": true}},
              {"3": null}
            ],
            "vendorData": 7
          }],
          "notify": [
            {"heartbeat": "1800000000002", "vendorNotify": [1, 2]},
            {}
          ]
        }"#,
    )
    .expect("synthetic frame is valid");

    assert_eq!(frame.extra_fields["vendorFrame"]["version"], 2);
    let response = &frame.response.as_ref().expect("response array")[0];
    assert_eq!(response.service, "ADMIN");
    assert_eq!(response.request_id, "42");
    assert_eq!(response.content.code, 26);
    assert_eq!(response.content.msg, "OK");
    assert_eq!(response.extra_fields["vendorResponse"], true);

    let data = &frame.data.as_ref().expect("data array")[0];
    assert_eq!(data.service, "LEVELONE_OPTIONS");
    assert_eq!(data.content[0].key.as_deref(), Some("SYNTH-OPTION"));
    assert_eq!(data.content[0].fields["2"], 1.25);
    assert_eq!(data.content[0].fields["vendorRow"]["keep"], true);
    assert_eq!(data.content[1].key, None);
    assert_eq!(data.content[1].fields["3"], Value::Null);
    assert_eq!(data.extra_fields["vendorData"], 7);

    let notifications = frame.notify.as_ref().expect("notify array");
    assert_eq!(notifications[0].heartbeat.as_deref(), Some("1800000000002"));
    assert_eq!(notifications[0].extra_fields["vendorNotify"][1], 2);
    assert_eq!(notifications[1].heartbeat, None);
}

#[test]
fn normalizes_string_and_safe_numeric_wire_forms() {
    let frame = parse(
        r#"{"response":[
          {"service":"ADMIN","requestid":"0012","command":"LOGIN","timestamp":1,"content":{"code":"0","msg":"OK"}},
          {"service":"LEVELONE_OPTIONS","requestid":26.0,"command":"SUBS","timestamp":2,"content":{"code":26.0,"msg":"OK"}},
          {"service":"ADMIN","requestid":9007199254740991,"command":"LOGIN","timestamp":3,"content":{"code":-9223372036854775808.0,"msg":"OK"}}
        ]}"#,
    )
    .expect("numeric and numeric-string forms are accepted");
    let responses = frame.response.expect("response array");
    assert_eq!(responses[0].request_id, "0012");
    assert_eq!(responses[0].content.code, 0);
    assert_eq!(responses[1].request_id, "26");
    assert_eq!(responses[1].content.code, 26);
    assert_eq!(responses[2].request_id, "9007199254740991");
    assert_eq!(responses[2].content.code, i64::MIN);
}

#[test]
fn timestamp_conversion_matches_zod_number_coercion_for_json_values() {
    let frame = parse(
        r#"{
          "response": [
            {"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":"1800000000000","content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"2","command":"LOGIN","timestamp":null,"content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"3","command":"LOGIN","timestamp":true,"content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"4","command":"LOGIN","timestamp":false,"content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"5","command":"LOGIN","timestamp":"","content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"6","command":"LOGIN","timestamp":" ","content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"7","command":"LOGIN","timestamp":"0x10","content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"8","command":"LOGIN","timestamp":[],"content":{"code":0,"msg":"OK"}},
            {"service":"ADMIN","requestid":"9","command":"LOGIN","timestamp":[1],"content":{"code":0,"msg":"OK"}}
          ],
          "data": [
            {"service":"LEVELONE_OPTIONS","timestamp":"1800000000001","command":"SUBS","content":[]},
            {"service":"LEVELONE_OPTIONS","timestamp":null,"command":"SUBS","content":[]}
          ]
        }"#,
    )
    .expect("Zod Number coercions have finite equivalents");
    let timestamps = frame
        .response
        .expect("response array")
        .into_iter()
        .map(|response| response.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(
        timestamps,
        vec![1_800_000_000_000.0, 0.0, 1.0, 0.0, 0.0, 0.0, 16.0, 0.0, 1.0]
    );
    let data_timestamps = frame
        .data
        .expect("data array")
        .into_iter()
        .map(|payload| payload.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(data_timestamps, vec![1_800_000_000_001.0, 0.0]);
}

#[test]
fn timestamp_visitor_preserves_json_coercion_and_rejects_non_finite_results() {
    let cases = [
        ("number", "1.25", Some(1.25)),
        (
            "large integer uses JavaScript number rounding",
            "9007199254740993",
            Some(9_007_199_254_740_992.0),
        ),
        (
            "numeric string",
            "\"1800000000000\"",
            Some(1_800_000_000_000.0),
        ),
        ("hex string", "\"0x10\"", Some(16.0)),
        ("empty string", "\"\"", Some(0.0)),
        ("true", "true", Some(1.0)),
        ("false", "false", Some(0.0)),
        ("null", "null", Some(0.0)),
        ("empty array", "[]", Some(0.0)),
        ("single element array", "[1]", Some(1.0)),
        ("nested empty array", "[[]]", Some(0.0)),
        ("nested number array", "[[1]]", Some(1.0)),
        ("null array element", "[null]", Some(0.0)),
        ("large finite JSON number", "1e100", Some(1e100)),
        (
            "largest finite JSON number",
            "1.7976931348623157e308",
            Some(f64::MAX),
        ),
        ("boolean array", "[true]", None),
        ("multi-element array", "[1,2]", None),
        ("nested object array", "[[{}]]", None),
        ("object", "{}", None),
        ("nested object", "{\"x\":1}", None),
        ("invalid string", "\"not-a-number\"", None),
        ("infinite string", "\"Infinity\"", None),
        ("overflow string", "\"1e9999\"", None),
        ("overflow JSON number", "1e9999", None),
        ("negative overflow JSON number", "-1e9999", None),
    ];

    for (case, value, expected) in cases {
        let input = format!(
            r#"{{"response":[{{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":{value},"content":{{"code":0,"msg":"x"}}}}]}}"#
        );
        match (parse(&input), expected) {
            (Ok(frame), Some(expected)) => {
                let actual = frame.response.expect("response array")[0].timestamp;
                assert_eq!(actual, expected, "{case}");
            }
            (Err(StreamerWireError::InvalidSchema), None) => {}
            (result, expected) => panic!("{case}: got {result:?}, expected {expected:?}"),
        }
    }
}

#[test]
fn command_result_codes_match_current_node_command_mapping() {
    let vectors = [
        ("ADMIN", "LOGIN", 0, true),
        ("ACCT_ACTIVITY", "SUBS", 26, true),
        ("LEVELONE_EQUITIES", "UNSUBS", 27, true),
        ("LEVELONE_OPTIONS", "ADD", 28, true),
        ("LEVELONE_OPTIONS", "VIEW", 29, true),
        ("LEVELONE_OPTIONS", "SUBS", 27, false),
        ("ADMIN", "LOGIN", 26, false),
        ("LEVELONE_EQUITIES", "UNKNOWN", 26, false),
        ("LEVELONE_EQUITIES", "UNKNOWN", 0, true),
    ];
    for (service, command, code, expected) in vectors {
        assert_eq!(
            is_successful_streamer_command(service, command, code),
            expected,
            "service={service} command={command} code={code}"
        );
    }
}

#[test]
fn rejects_missing_fields_null_optionals_and_wrong_field_types() {
    let invalid_frames = [
        ("null collection", r#"{"response":null}"#),
        (
            "missing message",
            r#"{"response":[{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":1,"content":{"code":0}}]}"#,
        ),
        (
            "null request ID",
            r#"{"response":[{"service":"ADMIN","requestid":null,"command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"x"}}]}"#,
        ),
        (
            "fractional numeric request ID",
            r#"{"response":[{"service":"ADMIN","requestid":1.5,"command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"x"}}]}"#,
        ),
        (
            "unsafe numeric request ID",
            r#"{"response":[{"service":"ADMIN","requestid":9007199254740992,"command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"x"}}]}"#,
        ),
        (
            "uncoercible timestamp",
            r#"{"response":[{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":"not-a-number","content":{"code":0,"msg":"x"}}]}"#,
        ),
        (
            "null code",
            r#"{"response":[{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":1,"content":{"code":null,"msg":"x"}}]}"#,
        ),
        (
            "fractional code string",
            r#"{"response":[{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":1,"content":{"code":"26.0","msg":"x"}}]}"#,
        ),
        ("null data collection", r#"{"data":null}"#),
        (
            "null data content",
            r#"{"data":[{"service":"LEVELONE_OPTIONS","timestamp":1,"command":"SUBS","content":null}]}"#,
        ),
        (
            "null row key",
            r#"{"data":[{"service":"LEVELONE_OPTIONS","timestamp":1,"command":"SUBS","content":[{"key":null}]}]}"#,
        ),
        ("null heartbeat", r#"{"notify":[{"heartbeat":null}]}"#),
        ("numeric heartbeat", r#"{"notify":[{"heartbeat":1}]}"#),
    ];

    for (case, input) in invalid_frames {
        assert_eq!(
            parse(input),
            Err(StreamerWireError::InvalidSchema),
            "{case}"
        );
    }
    assert_eq!(parse("[]"), Err(StreamerWireError::InvalidSchema));
}

#[test]
fn array_item_limits_apply_to_frame_and_data_content_arrays() {
    let response = r#"{"service":"ADMIN","requestid":"1","command":"LOGIN","timestamp":1,"content":{"code":0,"msg":"OK"}}"#;
    let response_items = std::iter::repeat_n(response, MAX_WIRE_ARRAY_ITEMS + 1)
        .collect::<Vec<_>>()
        .join(",");
    let response_frame = format!(r#"{{"response":[{response_items}]}}"#);
    assert!(response_frame.len() < MAX_WIRE_FRAME_BYTES);
    assert_eq!(
        parse(&response_frame),
        Err(StreamerWireError::InvalidSchema)
    );

    let rows = std::iter::repeat_n("{}", MAX_WIRE_ARRAY_ITEMS + 1)
        .collect::<Vec<_>>()
        .join(",");
    let data_frame = format!(
        r#"{{"data":[{{"service":"LEVELONE_OPTIONS","timestamp":1,"command":"SUBS","content":[{rows}]}}]}}"#
    );
    assert_eq!(parse(&data_frame), Err(StreamerWireError::InvalidSchema));

    let notify_items = std::iter::repeat_n("{}", MAX_WIRE_ARRAY_ITEMS + 1)
        .collect::<Vec<_>>()
        .join(",");
    let notify_frame = format!(r#"{{"notify":[{notify_items}]}}"#);
    assert_eq!(parse(&notify_frame), Err(StreamerWireError::InvalidSchema));
}

#[test]
fn frame_size_and_decode_errors_do_not_expose_source_payload() {
    assert_eq!(parse(""), Err(StreamerWireError::EmptyFrame));
    assert_eq!(parse("{broken"), Err(StreamerWireError::MalformedJson));
    let secret_marker = "SYNTHETIC_PRIVATE_MARKER";
    let invalid = format!(r#"{{"response":[{{"service":"{secret_marker}"}}]}}"#);
    let error = parse(&invalid).expect_err("required response fields are missing");
    assert_eq!(error, StreamerWireError::InvalidSchema);
    assert!(!error.to_string().contains(secret_marker));
    assert!(!format!("{error:?}").contains(secret_marker));

    let oversize = vec![b' '; MAX_WIRE_FRAME_BYTES + 1];
    assert_eq!(
        parse_streamer_frame(&oversize),
        Err(StreamerWireError::FrameTooLarge {
            limit: MAX_WIRE_FRAME_BYTES
        })
    );
}

#[test]
fn debug_output_redacts_response_data_notification_and_unknown_values() {
    let marker = "SYNTHETIC_OCC_AND_ACTIVITY_SECRET";
    let input = format!(
        r#"{{"vendor-{marker}":"{marker}","response":[{{"service":"{marker}","requestid":"{marker}","command":"{marker}","timestamp":1,"content":{{"code":0,"msg":"{marker}","vendor":"{marker}"}}}}],"data":[{{"service":"{marker}","timestamp":1,"command":"SUBS","content":[{{"key":"{marker}","2":"{marker}"}}]}}],"notify":[{{"heartbeat":"{marker}","vendor":"{marker}"}}]}}"#
    );
    let frame = parse(&input).expect("synthetic values satisfy the codec");
    let response = &frame.response.as_ref().expect("response record")[0];
    let data = &frame.data.as_ref().expect("data record")[0];
    let row = &data.content[0];
    let notify = &frame.notify.as_ref().expect("notify record")[0];
    let debug = format!(
        "{frame:?}{response:?}{:?}{data:?}{row:?}{notify:?}",
        response.content
    );
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(marker));
}
