//! Test-only Node mutation wire contract.
//!
//! This module is included from `lib.rs` only under `cfg(test)`. It has no
//! transport, token, execution, WAL, or public SDK integration. The serialized
//! body is captured Node `JSON.stringify` output, not a Rust order DTO.
//! 提供 mutation contract 的内部辅助实现。

use std::collections::BTreeMap;
use std::fmt;

use crate::client::SCHWAB_API_ROOT;
use crate::routes::{AccountsQuery, BrokerIdentifier, PathIdentifier, ReadRequest};

const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Copy, Eq, PartialEq)]
enum MutationOperation {
    Place,
    Replace,
    Cancel,
}

impl MutationOperation {
    const fn method(self) -> &'static str {
        match self {
            Self::Place => "POST",
            Self::Replace => "PUT",
            Self::Cancel => "DELETE",
        }
    }

    const fn requires_location_id(self) -> bool {
        !matches!(self, Self::Cancel)
    }
}

impl fmt::Debug for MutationOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Place => "Place",
            Self::Replace => "Replace",
            Self::Cancel => "Cancel",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationContractError {
    OperationShape,
    JsonObject,
    Path,
}

impl fmt::Display for MutationContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OperationShape => "REST_MUTATION_CONTRACT_SHAPE_INVALID",
            Self::JsonObject => "REST_MUTATION_CONTRACT_BODY_INVALID",
            Self::Path => "REST_MUTATION_CONTRACT_PATH_INVALID",
        })
    }
}

/// Opaque, exact JSON bytes observed from Node. Its formatter never prints
/// order details, contract symbols, prices, account identifiers, or tokens.
#[derive(Clone, Eq, PartialEq)]
struct JsonObject(Vec<u8>);

impl JsonObject {
    fn from_node_json(bytes: &[u8]) -> Result<Self, MutationContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| MutationContractError::JsonObject)?;
        if !value.is_object() {
            return Err(MutationContractError::JsonObject);
        }
        Ok(Self(bytes.to_vec()))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for JsonObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonObject")
            .field("bytes", &self.0.len())
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Inert wire-shape description, compiled into unit tests only. It cannot
/// dispatch requests or be converted into the production GET-only request.
struct MutationWireRequest {
    operation: MutationOperation,
    path: String,
    body: Option<JsonObject>,
}

impl MutationWireRequest {
    fn new(
        operation: MutationOperation,
        account_hash: &str,
        order_id: Option<&str>,
        body: Option<JsonObject>,
    ) -> Result<Self, MutationContractError> {
        let valid_shape = match operation {
            MutationOperation::Place => order_id.is_none() && body.is_some(),
            MutationOperation::Replace => order_id.is_some() && body.is_some(),
            MutationOperation::Cancel => order_id.is_some(),
        };
        if !valid_shape {
            return Err(MutationContractError::OperationShape);
        }

        let account_hash =
            PathIdentifier::new(account_hash).map_err(|_| MutationContractError::Path)?;
        let path = match (operation, order_id) {
            (MutationOperation::Place, None) => {
                let account_route = ReadRequest::Account {
                    account_hash,
                    query: AccountsQuery::default(),
                }
                .endpoint()
                .map_err(|_| MutationContractError::Path)?;
                format!("{}/orders", account_route.path())
            }
            (MutationOperation::Replace | MutationOperation::Cancel, Some(order_id)) => {
                let order_id =
                    BrokerIdentifier::new(order_id).map_err(|_| MutationContractError::Path)?;
                ReadRequest::Order {
                    account_hash,
                    order_id,
                }
                .endpoint()
                .map_err(|_| MutationContractError::Path)?
                .path()
                .to_owned()
            }
            _ => return Err(MutationContractError::OperationShape),
        };

        Ok(Self {
            operation,
            path,
            body,
        })
    }

    fn method(&self) -> &'static str {
        self.operation.method()
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn body(&self) -> Option<&[u8]> {
        self.body.as_ref().map(JsonObject::as_bytes)
    }

    fn content_type(&self) -> Option<&'static str> {
        self.body.as_ref().map(|_| JSON_CONTENT_TYPE)
    }

    /// Node's mutation override fixes retries to zero regardless of global or
    /// call-site retry settings. This value describes policy only; it does not
    /// execute an HTTP attempt.
    const fn max_physical_attempts() -> u8 {
        1
    }

    const fn automatic_replay_allowed() -> bool {
        false
    }

    fn classify_response(&self, response: &MutationWireResponse) -> MutationClassification {
        let location = response
            .header("location")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let order_id = location.as_deref().and_then(order_id_from_location);

        if response.status >= 500 || response.status == 0 {
            return MutationClassification::Unknown(UnknownOutcome {
                operation: self.operation,
                reason: UnknownReason::ServerErrorResponse,
                status: Some(response.status),
                location,
            });
        }
        if !(200..300).contains(&response.status) {
            // This mirrors the Node SDK's SchwabApiError classification only.
            // It does not prove that the provider applied no side effect.
            return MutationClassification::ApiError {
                status: response.status,
            };
        }
        if self.operation.requires_location_id() && order_id.is_none() {
            return MutationClassification::Unknown(UnknownOutcome {
                operation: self.operation,
                reason: UnknownReason::MissingOrUnusableLocation,
                status: Some(response.status),
                location,
            });
        }

        MutationClassification::Returned {
            status: response.status,
            location,
            order_id,
        }
    }

    fn classify_transport_failure(
        &self,
        kind: TransportFailureKind,
        response: Option<&MutationWireResponse>,
    ) -> MutationClassification {
        let (reason, status, location) = match kind {
            TransportFailureKind::FetchRejected => (UnknownReason::TransportFailure, None, None),
            TransportFailureKind::ResponseBodyRead => (
                UnknownReason::ResponseBodyReadFailure,
                response.map(|value| value.status),
                response.and_then(|value| value.header("location").map(str::to_owned)),
            ),
        };
        MutationClassification::Unknown(UnknownOutcome {
            operation: self.operation,
            reason,
            status,
            location,
        })
    }
}

impl fmt::Debug for MutationWireRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationWireRequest")
            .field("operation", &self.operation)
            .field("method", &self.method())
            .field("path", &"[REDACTED]")
            .field("body", &self.body)
            .field("max_physical_attempts", &Self::max_physical_attempts())
            .finish()
    }
}

/// Synthetic response bytes from the Node fake-fetch contract. The response
/// body and header values are retained for assertions and always redacted in
/// diagnostics.
struct MutationWireResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Option<Vec<u8>>,
}

impl MutationWireResponse {
    fn new(
        status: u16,
        headers: impl IntoIterator<Item = (String, String)>,
        body: Option<Vec<u8>>,
    ) -> Self {
        Self {
            status,
            headers: headers
                .into_iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
                .collect(),
            body,
        }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn status(&self) -> u16 {
        self.status
    }

    fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }
}

impl fmt::Debug for MutationWireResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationWireResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.as_ref().map_or(0, Vec::len))
            .field("headers", &"[REDACTED]")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportFailureKind {
    /// Node exposes fetch rejection as a network error without send-stage
    /// evidence, so it remains UNKNOWN regardless of DNS/connect/TLS detail.
    FetchRejected,
    ResponseBodyRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnknownReason {
    TransportFailure,
    ResponseBodyReadFailure,
    ServerErrorResponse,
    MissingOrUnusableLocation,
}

struct UnknownOutcome {
    operation: MutationOperation,
    reason: UnknownReason,
    status: Option<u16>,
    location: Option<String>,
}

impl fmt::Debug for UnknownOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnknownOutcome")
            .field("operation", &self.operation)
            .field("reason", &self.reason)
            .field("status", &self.status)
            .field("location", &self.location.as_ref().map(|_| "[REDACTED]"))
            .field("replay_allowed", &false)
            .finish()
    }
}

enum MutationClassification {
    Returned {
        status: u16,
        location: Option<String>,
        order_id: Option<String>,
    },
    ApiError {
        status: u16,
    },
    Unknown(UnknownOutcome),
}

impl fmt::Debug for MutationClassification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Returned { status, .. } => formatter
                .debug_struct("MutationClassification::Returned")
                .field("status", status)
                .field("location", &"[REDACTED]")
                .field("order_id", &"[REDACTED]")
                .finish(),
            Self::ApiError { status } => formatter
                .debug_struct("MutationClassification::ApiError")
                .field("status", status)
                .field("body", &"[REDACTED]")
                .finish(),
            Self::Unknown(outcome) => formatter
                .debug_tuple("MutationClassification::Unknown")
                .field(outcome)
                .finish(),
        }
    }
}

fn order_id_from_location(location: &str) -> Option<String> {
    let base = reqwest::Url::parse(SCHWAB_API_ROOT).ok()?;
    let url = base.join(location).ok()?;
    let segments = url
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let id = *segments.last()?;
    let parent = segments.get(segments.len().checked_sub(2)?)?;
    (parent.eq_ignore_ascii_case("orders")
        && !id.is_empty()
        && id.bytes().all(|byte| byte.is_ascii_digit()))
    .then(|| id.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const MUTATION_FIXTURE: &str =
        include_str!("../../../test/fixtures/rust-v2/sdk_mutations.json");
    const ERROR_FIXTURE: &str =
        include_str!("../../../test/fixtures/rust-v2/sdk_mutation_errors_172.json");
    const SDK_FIXTURE: &str = include_str!("../../../test/fixtures/rust-v2/sdk.json");

    fn parse_fixture(raw: &str) -> Value {
        serde_json::from_str(raw).expect("checked-in synthetic fixture JSON")
    }

    fn case<'a>(fixture: &'a Value, id: &str) -> &'a Value {
        fixture["cases"]
            .as_array()
            .expect("fixture cases")
            .iter()
            .find(|scenario| scenario["id"] == id)
            .unwrap_or_else(|| panic!("missing synthetic mutation vector {id}"))
    }

    fn request_for_case(fixture: &Value, scenario: &Value) -> MutationWireRequest {
        let operation = match scenario["operation"].as_str().unwrap_or_else(|| {
            match scenario["kind"].as_str() {
                Some(
                    "accepted-post"
                    | "post-503"
                    | "post-network-failure"
                    | "missing-location"
                    | "body-read-failure",
                ) => "place",
                Some("mutation-401") => "cancel",
                _ => panic!("fixture operation missing"),
            }
        }) {
            "place" => MutationOperation::Place,
            "replace" => MutationOperation::Replace,
            "cancel" => MutationOperation::Cancel,
            value => panic!("unexpected mutation operation {value}"),
        };
        let request = &fixture["request"];
        let account_hash = request["accountHash"]
            .as_str()
            .expect("synthetic account hash");
        let order_id = (operation != MutationOperation::Place)
            .then(|| request["orderId"].as_str().expect("synthetic order ID"));
        let body_string = scenario["expected"]["requests"][0]["bodyJson"].as_str();
        let body = body_string
            .filter(|value| !value.is_empty())
            .map(|value| JsonObject::from_node_json(value.as_bytes()).expect("Node JSON object"));

        MutationWireRequest::new(operation, account_hash, order_id, body)
            .expect("fixture mutation shape and identifiers")
    }

    fn assert_request_matches_fixture(fixture: &Value, scenario: &Value) {
        let request = request_for_case(fixture, scenario);
        let expected = &scenario["expected"]["requests"][0];
        assert_eq!(request.method(), expected["method"]);
        assert_eq!(
            request.path(),
            expected["pathname"].as_str().expect("Node request path")
        );
        let expected_body = expected["bodyJson"].as_str();
        assert_eq!(
            request
                .body()
                .map(std::str::from_utf8)
                .transpose()
                .expect("Node JSON is UTF-8"),
            expected_body,
            "the exact Node-captured JSON.stringify bytes must be retained"
        );
        assert_eq!(
            request.content_type(),
            expected_body.map(|_| JSON_CONTENT_TYPE),
            "Node sets JSON content type only when a JSON body is present"
        );
        if let Some(body) = request.body() {
            let actual_body: Value =
                serde_json::from_slice(body).expect("captured Node JSON body is valid");
            let input_order = match request.operation {
                MutationOperation::Place => &fixture["request"]["order"],
                MutationOperation::Replace => fixture["request"]
                    .get("replaceOrder")
                    .filter(|value| !value.is_null())
                    .unwrap_or(&fixture["request"]["order"]),
                MutationOperation::Cancel => {
                    panic!("current Cancel characterization has no request body")
                }
            };
            assert_eq!(
                &actual_body, input_order,
                "captured Node JSON body must represent the operation input for {}",
                scenario["id"]
            );
        }
        assert_eq!(MutationWireRequest::max_physical_attempts(), 1);
        assert!(!MutationWireRequest::automatic_replay_allowed());
        assert!(!request.path().contains("previewOrder"));
    }

    fn response_from_error_vector(scenario: &Value) -> MutationWireResponse {
        let transport = &scenario["transport"];
        let mut headers = BTreeMap::new();
        if let Some(values) = transport["headers"].as_object() {
            for (key, value) in values {
                if let Some(value) = value.as_str() {
                    headers.insert(key.clone(), value.to_owned());
                }
            }
        }
        if let Some(location) = transport["location"].as_str() {
            headers.insert("Location".to_owned(), location.to_owned());
        }
        let body = transport
            .get("body")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::to_vec(value).expect("synthetic JSON response body"));
        MutationWireResponse::new(
            u16::try_from(transport["status"].as_u64().expect("HTTP response status"))
                .expect("HTTP response status fits u16"),
            headers,
            body,
        )
    }

    #[test]
    fn place_replace_cancel_requests_match_all_current_node_mutation_vectors() {
        let fixtures = [
            parse_fixture(MUTATION_FIXTURE),
            parse_fixture(ERROR_FIXTURE),
        ];
        for fixture in &fixtures {
            for scenario in fixture["cases"].as_array().expect("fixture cases") {
                assert_request_matches_fixture(fixture, scenario);
            }
        }
    }

    #[test]
    fn exact_mutation_paths_reuse_the_read_allowlist_encoder_without_exposing_a_writer() {
        let fixture = parse_fixture(ERROR_FIXTURE);
        let place = case(&fixture, "place-400-preserves-explicit-api-error-body");
        let replace = case(
            &fixture,
            "replace-429-preserves-rate-limit-metadata-without-retry",
        );
        let cancel = case(&fixture, "cancel-409-preserves-explicit-api-error-body");
        assert_eq!(
            request_for_case(&fixture, place).path(),
            "/trader/v1/accounts/synthetic%2Fhash%2B/orders"
        );
        assert_eq!(
            request_for_case(&fixture, replace).path(),
            "/trader/v1/accounts/synthetic%2Fhash%2B/orders/4242"
        );
        assert_eq!(
            request_for_case(&fixture, cancel).path(),
            "/trader/v1/accounts/synthetic%2Fhash%2B/orders/4242"
        );

        assert_eq!(
            MutationWireRequest::new(MutationOperation::Place, "synthetic/hash+", None, None,)
                .unwrap_err(),
            MutationContractError::OperationShape
        );
        assert_eq!(
            MutationWireRequest::new(
                MutationOperation::Replace,
                "synthetic/hash+",
                Some("4242"),
                None,
            )
            .unwrap_err(),
            MutationContractError::OperationShape
        );
        assert_eq!(
            MutationWireRequest::new(
                MutationOperation::Cancel,
                "synthetic/hash+",
                Some("9223372036854775808"),
                None,
            )
            .unwrap_err(),
            MutationContractError::Path
        );
    }

    #[test]
    fn fixture_error_responses_keep_status_headers_and_body_and_do_not_prove_rejection() {
        let fixture = parse_fixture(ERROR_FIXTURE);
        for scenario in fixture["cases"].as_array().expect("error vectors") {
            if scenario["expected"]["outcome"]["kind"] == "returned" {
                continue;
            }
            let response = response_from_error_vector(scenario);
            let request = request_for_case(&fixture, scenario);
            let classification = request.classify_response(&response);
            let status = response.status();
            match classification {
                MutationClassification::ApiError { status: actual } => assert_eq!(actual, status),
                MutationClassification::Unknown(ref unknown) => {
                    assert_eq!(unknown.status, Some(status));
                }
                MutationClassification::Returned { .. } => {
                    panic!("error vector unexpectedly returned: {}", scenario["id"])
                }
            }
            if let Some(body) = response.body() {
                let parsed: Value =
                    serde_json::from_slice(body).expect("synthetic JSON error body");
                assert_eq!(parsed, scenario["transport"]["body"]);
            }
        }

        let rate_limited = response_from_error_vector(case(
            &fixture,
            "replace-429-preserves-rate-limit-metadata-without-retry",
        ));
        assert_eq!(rate_limited.header("retry-after"), Some("17"));
        assert_eq!(rate_limited.header("ratelimit-limit"), Some("120"));
        assert_eq!(rate_limited.header("ratelimit-remaining"), Some("0"));
        assert_eq!(rate_limited.header("ratelimit-reset"), Some("1800000000"));
        assert!(
            rate_limited
                .headers()
                .keys()
                .all(|name| name == name.to_ascii_lowercase().as_str())
        );
    }

    #[test]
    fn mutation_success_location_and_cancel_empty_success_follow_node_classification() {
        let fixture = parse_fixture(MUTATION_FIXTURE);
        let replace = case(&fixture, "replace-201-location-preserves-frozen-payload");
        let request = request_for_case(&fixture, replace);
        let location = replace["transport"]["location"]
            .as_str()
            .expect("synthetic Location");
        let response =
            MutationWireResponse::new(201, [("Location".to_owned(), location.to_owned())], None);
        match request.classify_response(&response) {
            MutationClassification::Returned {
                status,
                location: actual_location,
                order_id,
            } => {
                assert_eq!(status, 201);
                assert_eq!(actual_location.as_deref(), Some(location));
                assert_eq!(order_id.as_deref(), Some("4242"));
            }
            _ => panic!("Node's successful Replace vector must be returned"),
        }

        let cancel = case(&fixture, "cancel-204-without-location-is-known-success");
        let request = request_for_case(&fixture, cancel);
        let response = MutationWireResponse::new(204, std::iter::empty::<(String, String)>(), None);
        assert!(matches!(
            request.classify_response(&response),
            MutationClassification::Returned {
                status: 204,
                location: None,
                order_id: None
            }
        ));

        // The older SDK golden records the returned Place order ID, but not
        // the raw Location header. Use the Node source's documented Location
        // path shape with that fixture ID; this is parser coverage, not a
        // claim that the fixture captured this exact header.
        let sdk = parse_fixture(SDK_FIXTURE);
        let accepted = case(&sdk, "accepted-post-returns-location-metadata");
        let errors = parse_fixture(ERROR_FIXTURE);
        let place_case = case(&errors, "place-400-preserves-explicit-api-error-body");
        let place = request_for_case(&errors, place_case);
        let expected_id = accepted["expected"]["orderId"]
            .as_str()
            .expect("synthetic SDK Place order ID");
        let location = format!("/trader/v1/accounts/synthetic%2Fhash%2B/orders/{expected_id}");
        let response = MutationWireResponse::new(201, [("Location".to_owned(), location)], None);
        assert!(matches!(
            place.classify_response(&response),
            MutationClassification::Returned { order_id: Some(id), .. } if id == expected_id
        ));

        let invalid_location = parse_fixture(ERROR_FIXTURE);
        let scenario = case(
            &invalid_location,
            "replace-success-with-unusable-location-is-unknown",
        );
        let response = response_from_error_vector(scenario);
        let request = request_for_case(&invalid_location, scenario);
        assert!(matches!(
            request.classify_response(&response),
            MutationClassification::Unknown(UnknownOutcome {
                reason: UnknownReason::MissingOrUnusableLocation,
                status: Some(200),
                ..
            })
        ));
    }

    #[test]
    fn bug_in_legacy_external_location_is_only_mirrored_by_test_parser() {
        let fixture = parse_fixture(ERROR_FIXTURE);
        let scenario = case(&fixture, "replace-absolute-external-location-is-accepted");
        assert_eq!(scenario["classification"], "BUG_IN_LEGACY");
        let request = request_for_case(&fixture, scenario);
        let response = response_from_error_vector(scenario);

        // BUG_IN_LEGACY: the current Node parser checks only pathname suffixes
        // and accepts an absolute foreign origin. This mirror exists only to
        // prove Node behavior. Any future Rust production parser must validate
        // the trusted Schwab origin and the expected request/account path.
        assert!(matches!(
            request.classify_response(&response),
            MutationClassification::Returned {
                status: 200,
                location: Some(location),
                order_id: Some(order_id),
            } if location.starts_with("https://outside.example.invalid/") && order_id == "4242"
        ));
    }

    #[test]
    fn every_characterized_unknown_transport_or_server_result_is_never_replayable() {
        let fixture = parse_fixture(MUTATION_FIXTURE);
        for (id, expected_reason) in [
            (
                "replace-503-is-unknown-despite-retry-override",
                UnknownReason::ServerErrorResponse,
            ),
            (
                "cancel-503-is-unknown-despite-retry-override",
                UnknownReason::ServerErrorResponse,
            ),
        ] {
            let scenario = case(&fixture, id);
            let request = request_for_case(&fixture, scenario);
            assert!(matches!(
                request.classify_response(&response_from_error_vector(scenario)),
                MutationClassification::Unknown(UnknownOutcome { reason, status: Some(503), .. }) if reason == expected_reason
            ));
            assert_eq!(MutationWireRequest::max_physical_attempts(), 1);
            assert!(!MutationWireRequest::automatic_replay_allowed());
        }

        let errors = parse_fixture(ERROR_FIXTURE);
        let place = request_for_case(
            &errors,
            case(&errors, "place-400-preserves-explicit-api-error-body"),
        );
        let place_503 =
            MutationWireResponse::new(503, std::iter::empty::<(String, String)>(), None);
        assert!(matches!(
            place.classify_response(&place_503),
            MutationClassification::Unknown(UnknownOutcome {
                reason: UnknownReason::ServerErrorResponse,
                status: Some(503),
                ..
            })
        ));
        let place_without_location =
            MutationWireResponse::new(201, std::iter::empty::<(String, String)>(), None);
        assert!(matches!(
            place.classify_response(&place_without_location),
            MutationClassification::Unknown(UnknownOutcome {
                reason: UnknownReason::MissingOrUnusableLocation,
                status: Some(201),
                ..
            })
        ));
        for kind in [
            TransportFailureKind::FetchRejected,
            TransportFailureKind::ResponseBodyRead,
        ] {
            let response = MutationWireResponse::new(
                201,
                [(
                    "Location".to_owned(),
                    "/trader/v1/accounts/synthetic%2Fhash%2B/orders/4242".to_owned(),
                )],
                None,
            );
            let result = place.classify_transport_failure(kind, Some(&response));
            assert!(matches!(result, MutationClassification::Unknown(_)));
            assert!(!MutationWireRequest::automatic_replay_allowed());
        }
        assert!(matches!(
            place.classify_transport_failure(TransportFailureKind::FetchRejected, None),
            MutationClassification::Unknown(UnknownOutcome {
                reason: UnknownReason::TransportFailure,
                status: None,
                ..
            })
        ));
        assert!(matches!(
            place.classify_transport_failure(
                TransportFailureKind::ResponseBodyRead,
                Some(&MutationWireResponse::new(201, [], None))
            ),
            MutationClassification::Unknown(UnknownOutcome {
                reason: UnknownReason::ResponseBodyReadFailure,
                status: Some(201),
                ..
            })
        ));

        let sdk = parse_fixture(SDK_FIXTURE);
        assert_eq!(sdk["cases"].as_array().expect("Node SDK fixture").len(), 8);
        for id in [
            "post-503-is-unknown-and-cannot-enable-retry",
            "post-network-failure-is-unknown-without-retry",
            "post-201-without-location-remains-unknown",
            "post-201-body-read-failure-is-sdk-unknown",
        ] {
            assert_eq!(
                case(&sdk, id)["expected"]["errorCode"],
                "SCHWAB_UNKNOWN_OUTCOME"
            );
        }
    }

    #[test]
    fn debug_redacts_order_payload_account_location_and_response_body() {
        let fixture = parse_fixture(ERROR_FIXTURE);
        let scenario = case(&fixture, "place-400-preserves-explicit-api-error-body");
        let request = request_for_case(&fixture, scenario);
        let request_debug = format!("{request:?}");
        for sensitive in [
            "synthetic/hash",
            "SYNTHETIC-LONG-PUT",
            "SYNTHETIC-SHORT-PUT",
            "0.93",
        ] {
            assert!(!request_debug.contains(sensitive));
        }
        assert!(request_debug.contains("[REDACTED]"));

        let response = response_from_error_vector(scenario);
        let response_debug = format!("{response:?}");
        assert!(!response_debug.contains("synthetic invalid order"));
        assert!(response_debug.contains("[REDACTED]"));

        let unknown = request.classify_response(&response);
        let outcome_debug = format!("{unknown:?}");
        assert!(!outcome_debug.contains("synthetic/hash"));
        assert!(!outcome_debug.contains("SYNTHETIC-LONG-PUT"));
    }

    #[test]
    fn cancel_body_is_optional_but_only_opaque_fixture_json_is_preserved() {
        let payload =
            JsonObject::from_node_json(br#"{"id":4242}"#).expect("CancelOrderRequest JSON");
        let request = MutationWireRequest::new(
            MutationOperation::Cancel,
            "synthetic/hash+",
            Some("4242"),
            Some(payload),
        )
        .expect("typed Node CancelOrderRequest may be present");
        assert_eq!(request.method(), "DELETE");
        assert_eq!(request.content_type(), Some(JSON_CONTENT_TYPE));
        assert_eq!(request.body(), Some(br#"{"id":4242}"#.as_slice()));
        assert_eq!(
            JsonObject::from_node_json(b"[]").unwrap_err(),
            MutationContractError::JsonObject
        );
        assert_eq!(
            MutationWireRequest::new(
                MutationOperation::Cancel,
                "synthetic/hash+",
                Some("4242"),
                None,
            )
            .expect("current cancel vector has no body")
            .content_type(),
            None
        );
        assert_eq!(
            JsonObject::from_node_json(
                br#"{"note":"source-backed optional CancelOrderRequest fields: id and order"}"#
            )
            .expect("source-backed synthetic JSON")
            .as_bytes(),
            br#"{"note":"source-backed optional CancelOrderRequest fields: id and order"}"#
        );
    }
}
