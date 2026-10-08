//! Secret-conscious LOGIN and subscription JSON envelopes.
//! 定义 Streamer 命令与响应协议常量。

use std::fmt::{self, Debug, Formatter};

use serde::Serialize;
use zeroize::Zeroizing;

use crate::command::{RequestId, StreamerCommand, SubscriptionCommand};
use crate::credentials::StreamerSessionCredentials;
use crate::session::PortFailure;

/// Serialized authentication frame with redacted diagnostics and zeroizing
/// application-owned storage.
pub(crate) struct LoginPayload(Zeroizing<String>);

impl LoginPayload {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for LoginPayload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("LoginPayload([REDACTED])")
    }
}

/// Serialized subscription frame with redacted diagnostics and zeroizing
/// application-owned storage.
pub(crate) struct SubscriptionPayload(Zeroizing<String>);

impl SubscriptionPayload {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for SubscriptionPayload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubscriptionPayload([REDACTED])")
    }
}

#[derive(Serialize)]
struct LoginEnvelope<'a> {
    requests: [LoginRequest<'a>; 1],
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    service: &'static str,
    command: &'static str,
    requestid: String,
    #[serde(rename = "SchwabClientCustomerId")]
    customer_id: &'a str,
    #[serde(rename = "SchwabClientCorrelId")]
    correlation_id: &'a str,
    parameters: LoginParameters<'a>,
}

#[derive(Serialize)]
struct LoginParameters<'a> {
    #[serde(rename = "Authorization")]
    authorization: &'a str,
    #[serde(rename = "SchwabClientChannel")]
    channel: &'a str,
    #[serde(rename = "SchwabClientFunctionId")]
    function_id: &'a str,
}

#[derive(Serialize)]
struct SubscriptionEnvelope<'a> {
    requests: [SubscriptionRequest<'a>; 1],
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    service: &'static str,
    command: &'static str,
    requestid: String,
    #[serde(rename = "SchwabClientCustomerId")]
    customer_id: &'a str,
    #[serde(rename = "SchwabClientCorrelId")]
    correlation_id: &'a str,
    parameters: SubscriptionParameters<'a>,
}

#[derive(Serialize)]
struct SubscriptionParameters<'a> {
    keys: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<&'a str>,
}

pub(crate) fn serialize_login(
    credentials: &StreamerSessionCredentials,
    request_id: RequestId,
) -> Result<LoginPayload, PortFailure> {
    let envelope = LoginEnvelope {
        requests: [LoginRequest {
            service: "ADMIN",
            command: "LOGIN",
            requestid: request_id.as_wire_value(),
            customer_id: credentials.customer_id.as_str(),
            correlation_id: credentials.correlation_id.as_str(),
            parameters: LoginParameters {
                authorization: credentials.access_token.expose_to_login_serializer(),
                channel: credentials.channel.as_str(),
                function_id: credentials.function_id.as_str(),
            },
        }],
    };
    serde_json::to_string(&envelope)
        .map(Zeroizing::new)
        .map(LoginPayload)
        .map_err(|_| PortFailure::SendFailed)
}

pub(crate) fn serialize_subscription(
    command: &StreamerCommand,
    customer_id: &str,
    correlation_id: &str,
) -> Result<SubscriptionPayload, PortFailure> {
    let keys_csv = Zeroizing::new(command.keys_csv());
    let parameters = SubscriptionParameters {
        keys: keys_csv.as_str(),
        // Current Node sends keys without fields on UNSUBS. The other
        // supported operations use the manifest field snapshot.
        fields: (command.command() != SubscriptionCommand::Unsubs).then_some(command.fields()),
    };
    let envelope = SubscriptionEnvelope {
        requests: [SubscriptionRequest {
            service: command.service().manifest().name(),
            command: command.command().name(),
            requestid: command.request_id().as_wire_value(),
            customer_id,
            correlation_id,
            parameters,
        }],
    };
    serde_json::to_string(&envelope)
        .map(Zeroizing::new)
        .map(SubscriptionPayload)
        .map_err(|_| PortFailure::SendFailed)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, from_str};
    use zeroize::{ZeroizeOnDrop, Zeroizing};

    use super::*;
    use crate::command::{ConnectionGeneration, RequestId};
    use crate::credentials::{StreamerLoginSecret, StreamerSessionCredentials};
    use crate::manifest::StreamerService;
    use crate::state::BoundedKeySet;

    #[test]
    fn login_wire_shape_matches_node() {
        let credentials = StreamerSessionCredentials::new(
            "wss://streamer.example.invalid/dynamic",
            "synthetic-customer",
            "synthetic-correlation",
            "N9",
            "synthetic-function",
            StreamerLoginSecret::new("synthetic-access-token".to_owned())
                .expect("synthetic bearer is valid"),
        )
        .expect("synthetic context is valid");
        let login =
            serialize_login(&credentials, RequestId::new(7)).expect("LOGIN envelope serializes");
        let json: Value = from_str(login.as_str()).expect("serialized LOGIN is JSON");
        assert_eq!(format!("{login:?}"), "LoginPayload([REDACTED])");
        assert_eq!(json["requests"][0]["service"], "ADMIN");
        assert_eq!(json["requests"][0]["command"], "LOGIN");
        assert_eq!(json["requests"][0]["requestid"], "7");
        assert_eq!(
            json["requests"][0]["SchwabClientCustomerId"],
            "synthetic-customer"
        );
        assert_eq!(
            json["requests"][0]["SchwabClientCorrelId"],
            "synthetic-correlation"
        );
        assert_eq!(
            json["requests"][0]["parameters"]["Authorization"],
            "synthetic-access-token"
        );
        assert_eq!(
            json["requests"][0]["parameters"]["SchwabClientChannel"],
            "N9"
        );
        assert_eq!(
            json["requests"][0]["parameters"]["SchwabClientFunctionId"],
            "synthetic-function"
        );
    }

    #[test]
    fn subscription_wire_shape_keeps_service_correlation_and_node_fields() {
        let command = StreamerCommand::new(
            ConnectionGeneration::new(3),
            RequestId::new(8),
            StreamerService::LevelOneEquities,
            SubscriptionCommand::Add,
            4,
            BoundedKeySet::from_keys(["QQQ"]).expect("test key is valid"),
        );
        let payload = serialize_subscription(
            &command,
            "synthetic-customer-marker",
            "synthetic-correlation-marker",
        )
        .expect("subscription serializes");
        let json: Value = from_str(payload.as_str()).expect("subscription is JSON");
        let debug = format!("{payload:?}");
        assert_eq!(debug, "SubscriptionPayload([REDACTED])");
        assert!(!debug.contains("synthetic-customer-marker"));
        assert!(!debug.contains("synthetic-correlation-marker"));
        assert_eq!(json["requests"][0]["service"], "LEVELONE_EQUITIES");
        assert_eq!(json["requests"][0]["command"], "ADD");
        assert_eq!(json["requests"][0]["requestid"], "8");
        assert_eq!(
            json["requests"][0]["SchwabClientCustomerId"],
            "synthetic-customer-marker"
        );
        assert_eq!(
            json["requests"][0]["SchwabClientCorrelId"],
            "synthetic-correlation-marker"
        );
        assert_eq!(json["requests"][0]["parameters"]["keys"], "QQQ");
        assert_eq!(json["requests"][0]["parameters"]["fields"], "0,45,46,51,52");
        assert_zeroizing_drop_storage(&payload.0);
        drop(payload);
    }

    #[test]
    fn unsubscribe_wire_shape_omits_fields_like_current_node_callsite() {
        let command = StreamerCommand::new(
            ConnectionGeneration::new(3),
            RequestId::new(9),
            StreamerService::LevelOneOptions,
            SubscriptionCommand::Unsubs,
            4,
            BoundedKeySet::from_keys(["QQQ   260101P00100000"])
                .expect("synthetic option key is valid"),
        );
        let payload =
            serialize_subscription(&command, "synthetic-customer", "synthetic-correlation")
                .expect("unsubscribe serializes");
        let json: Value = from_str(payload.as_str()).expect("unsubscribe is JSON");
        assert_eq!(json["requests"][0]["command"], "UNSUBS");
        assert!(json["requests"][0]["parameters"].get("fields").is_none());
    }

    fn assert_zeroizing_drop_storage(value: &Zeroizing<String>) {
        fn assert_zeroize_on_drop<T: ZeroizeOnDrop>(_: &T) {}

        assert_zeroize_on_drop(value);
    }
}
