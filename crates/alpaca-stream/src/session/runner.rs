//! State-machine execution for the authenticated options stream.
//!
//! 已认证期权流的状态机执行逻辑。

#[allow(clippy::wildcard_imports)]
// The runner is a child module that intentionally shares private session types.
use super::*;

use crate::ControlEvent;
use std::collections::BTreeSet;

use broker_ports::{
    RawFrameCapture, RawFrameCaptureAck, RawFrameCaptureError, RawFrameDisposition,
    RawFrameFinalization, RawFrameFinalizationError, RawFramePayload, RawFrameSink,
    RawFrameWireEncoding,
};
use market_contracts::{
    DecimalString, EntitlementState, MarketEventV1, NumericEncodingV1, UtcTimestamp,
};
use std::sync::Arc;

const RAW_FRAME_SINK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
struct RawFrameCorrelation {
    frame_sequence: u64,
    event_count: u32,
    sha256: String,
}

struct PendingRawCapture {
    sink: Arc<dyn RawFrameSink>,
    capture: RawFrameCapture,
    acknowledgement: RawFrameCaptureAck,
    received_at_utc: chrono::DateTime<chrono::Utc>,
}

struct DecodedProviderFrame {
    messages: Vec<ProviderMessage>,
    raw_frame: Option<RawFrameCorrelation>,
}

struct FramedMarketMessage {
    message: ProviderMessage,
    raw_frame: RawFrameCorrelation,
    event_ordinal: u32,
}

#[derive(Clone, Copy)]
enum FrameCaptureMode {
    None,
    SubscriptionHandshake,
    ActiveMarketData,
}

impl<P, C, W> Runner<P, C, W>
where
    P: CredentialProvider,
    C: SocketConnector,
    W: FreshnessClock,
{
    pub(super) async fn run(&mut self) -> Result<SessionExit, StreamError> {
        let retry_policy = self.config.reconnect.clone();
        let mut retries_used = 0u8;
        loop {
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or(StreamError::SequenceExhausted)?;
            let generation = SessionGeneration::new(self.generation);
            if self.phases.phase() == InternalPhase::SessionLost {
                self.transition(generation, InternalPhase::Connecting, None)?;
            } else if let Err(error) =
                publish_phase(&self.publishers, generation, SessionPhase::Connecting, None)
            {
                return Err(map_lane_failure(error));
            }
            self.publishers.begin_generation(generation).await;

            match self.run_attempt(generation).await {
                AttemptEnd::Cancelled => {
                    self.finish_generation(generation, SessionStatusCause::Cancelled)
                        .await;
                    return Ok(SessionExit::Cancelled);
                }
                AttemptEnd::ConsumersClosed => {
                    self.finish_generation(generation, SessionStatusCause::ConsumerClosed)
                        .await;
                    return Err(StreamError::ConsumerClosed);
                }
                #[cfg(feature = "offline-test-support")]
                AttemptEnd::FixtureEnd => return Ok(SessionExit::FixtureEnd),
                AttemptEnd::Failed(failure) => {
                    self.publishers.invalidate_generation(generation).await;
                    self.transition(generation, InternalPhase::SessionLost, Some(failure.cause))?;
                    if !failure.retryable {
                        let _ =
                            self.transition(generation, InternalPhase::Closed, Some(failure.cause));
                        return Err(failure.error);
                    }
                    if retries_used >= retry_policy.max_attempts() {
                        let _ =
                            self.transition(generation, InternalPhase::Closed, Some(failure.cause));
                        return Err(StreamError::RetryLimitReached);
                    }
                    let delay = reconnect_delay(
                        &retry_policy,
                        retries_used,
                        self.retry_seed ^ self.generation,
                    );
                    match cancellable(
                        time::sleep(delay),
                        &mut self.shutdown,
                        &mut self.consumers_closed,
                    )
                    .await
                    {
                        Abort::Completed(()) => retries_used = retries_used.saturating_add(1),
                        Abort::Cancelled => {
                            let _ = self.transition(
                                generation,
                                InternalPhase::Closed,
                                Some(SessionStatusCause::Cancelled),
                            );
                            return Ok(SessionExit::Cancelled);
                        }
                        Abort::ConsumersClosed => {
                            let _ = self.transition(
                                generation,
                                InternalPhase::Closed,
                                Some(SessionStatusCause::ConsumerClosed),
                            );
                            return Err(StreamError::ConsumerClosed);
                        }
                    }
                }
            }
        }
    }

    async fn run_attempt(&mut self, generation: SessionGeneration) -> AttemptEnd {
        let endpoint = self.config.endpoint();
        let future = time::timeout(
            self.config.limits.connect_timeout,
            self.connector.connect(endpoint),
        );
        let connected =
            match cancellable(future, &mut self.shutdown, &mut self.consumers_closed).await {
                Abort::Cancelled => return AttemptEnd::Cancelled,
                Abort::ConsumersClosed => return AttemptEnd::ConsumersClosed,
                Abort::Completed(Err(_)) => {
                    return AttemptEnd::Failed(AttemptFailure {
                        error: StreamError::ConnectionTimeout,
                        cause: SessionStatusCause::ConnectionTimeout,
                        retryable: true,
                    });
                }
                Abort::Completed(Ok(Err(ConnectFailure::AuthenticationRejected))) => {
                    return failed(
                        StreamError::AuthenticationRejected,
                        SessionStatusCause::AuthenticationRejected,
                        false,
                    );
                }
                Abort::Completed(Ok(Err(ConnectFailure::EndpointRejected))) => {
                    return failed(
                        StreamError::EndpointRejected,
                        SessionStatusCause::ProtocolViolation,
                        false,
                    );
                }
                Abort::Completed(Ok(Err(ConnectFailure::Retryable))) => {
                    return failed(
                        StreamError::TransportLost,
                        SessionStatusCause::TransportLost,
                        true,
                    );
                }
                Abort::Completed(Ok(Ok(socket))) => socket,
            };

        let mut socket = connected;
        let result = self.run_connected(generation, &mut socket).await;
        let _ = time::timeout(CLOSE_TIMEOUT, socket.close()).await;
        result
    }

    #[allow(clippy::too_many_lines)] // Keep the authenticated protocol state machine auditable in order.
    async fn run_connected<S: StreamSocket>(
        &mut self,
        generation: SessionGeneration,
        socket: &mut S,
    ) -> AttemptEnd {
        let mut frame_sequence = 0_u64;
        if let Err(error) = self.transition(generation, InternalPhase::AwaitingConnected, None) {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        let authentication_deadline = Instant::now() + self.config.limits.authentication_timeout;
        if let Err(error) = self
            .wait_handshake(
                socket,
                generation,
                HandshakeTarget::Connected,
                authentication_deadline,
                &mut frame_sequence,
            )
            .await
        {
            return error;
        }
        if let Err(error) = self.transition(generation, InternalPhase::Authenticating, None) {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        let credentials = match self.load_credentials(authentication_deadline).await {
            Ok(credentials) => credentials,
            Err(error) => return error,
        };
        let Ok(auth_payload) = encode_auth(&credentials) else {
            return failed(
                StreamError::ProtocolViolation,
                SessionStatusCause::ProtocolViolation,
                false,
            );
        };
        drop(credentials);
        if let Err(error) = self
            .send_until(
                socket,
                auth_payload,
                authentication_deadline,
                StreamError::AuthenticationTimeout,
                SessionStatusCause::AuthenticationTimeout,
            )
            .await
        {
            return error;
        }
        if let Err(error) = self.transition(generation, InternalPhase::AwaitingAuthentication, None)
        {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        if let Err(error) = self
            .wait_handshake(
                socket,
                generation,
                HandshakeTarget::Authenticated,
                authentication_deadline,
                &mut frame_sequence,
            )
            .await
        {
            return error;
        }
        if let Err(error) = self.transition(generation, InternalPhase::Subscribing, None) {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        let Ok(subscription_payload) = encode_subscriptions(&self.config.subscriptions) else {
            return failed(
                StreamError::ProtocolViolation,
                SessionStatusCause::ProtocolViolation,
                false,
            );
        };
        let acknowledgement_deadline = Instant::now() + self.config.limits.acknowledgement_timeout;
        if let Err(error) = self
            .send_until(
                socket,
                Zeroizing::new(subscription_payload),
                acknowledgement_deadline,
                StreamError::AcknowledgementTimeout,
                SessionStatusCause::AcknowledgementTimeout,
            )
            .await
        {
            return error;
        }
        if let Err(error) = self.transition(
            generation,
            InternalPhase::AwaitingSubscriptionAcknowledgement,
            None,
        ) {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        let trailing_data = match self
            .wait_handshake(
                socket,
                generation,
                HandshakeTarget::Subscription,
                acknowledgement_deadline,
                &mut frame_sequence,
            )
            .await
        {
            Ok(messages) => messages,
            Err(error) => return error,
        };
        if let Err(error) = self.transition(generation, InternalPhase::AwaitingFreshData, None) {
            return AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation));
        }
        let mut sequence = 0u64;
        match self
            .process_market_data(generation, &mut sequence, trailing_data)
            .await
        {
            Ok(()) => {}
            Err(error) => return error,
        }

        loop {
            let frame = match self
                .receive_frame(
                    socket,
                    generation,
                    None,
                    StreamError::TransportLost,
                    SessionStatusCause::TransportLost,
                    FrameCaptureMode::ActiveMarketData,
                    &mut frame_sequence,
                )
                .await
            {
                Ok(frame) => frame,
                Err(error) => return error,
            };
            if let Err(error) = self
                .process_active_frame(generation, &mut sequence, frame)
                .await
            {
                return error;
            }
        }
    }

    async fn load_credentials(
        &mut self,
        deadline: Instant,
    ) -> Result<crate::credentials::AlpacaCredentials, AttemptEnd> {
        let future = time::timeout_at(deadline, self.credential_provider.load_credentials());
        match cancellable(future, &mut self.shutdown, &mut self.consumers_closed).await {
            Abort::Cancelled => Err(AttemptEnd::Cancelled),
            Abort::ConsumersClosed => Err(AttemptEnd::ConsumersClosed),
            Abort::Completed(Err(_)) => Err(failed(
                StreamError::AuthenticationTimeout,
                SessionStatusCause::AuthenticationTimeout,
                false,
            )),
            Abort::Completed(Ok(Err(CredentialFailure::Unavailable))) => Err(failed(
                StreamError::CredentialsUnavailable,
                SessionStatusCause::AuthenticationRejected,
                false,
            )),
            Abort::Completed(Ok(Ok(credentials))) => Ok(credentials),
        }
    }

    async fn send_until<S: StreamSocket>(
        &mut self,
        socket: &mut S,
        payload: Zeroizing<Vec<u8>>,
        deadline: Instant,
        timeout_error: StreamError,
        timeout_cause: SessionStatusCause,
    ) -> Result<(), AttemptEnd> {
        let future = time::timeout_at(deadline, socket.send_binary(payload));
        match cancellable(future, &mut self.shutdown, &mut self.consumers_closed).await {
            Abort::Cancelled => Err(AttemptEnd::Cancelled),
            Abort::ConsumersClosed => Err(AttemptEnd::ConsumersClosed),
            Abort::Completed(Err(_)) => Err(failed(timeout_error, timeout_cause, true)),
            Abort::Completed(Ok(Err(_))) => Err(failed(
                StreamError::TransportLost,
                SessionStatusCause::TransportLost,
                true,
            )),
            Abort::Completed(Ok(Ok(()))) => Ok(()),
        }
    }

    #[allow(clippy::too_many_lines)] // ACK validation and raw-event correlation are one protocol transition.
    async fn wait_handshake<S: StreamSocket>(
        &mut self,
        socket: &mut S,
        generation: SessionGeneration,
        target: HandshakeTarget,
        deadline: Instant,
        frame_sequence: &mut u64,
    ) -> Result<Vec<FramedMarketMessage>, AttemptEnd> {
        loop {
            let (timeout_error, timeout_cause) = match target {
                HandshakeTarget::Connected | HandshakeTarget::Authenticated => (
                    StreamError::AuthenticationTimeout,
                    SessionStatusCause::AuthenticationTimeout,
                ),
                HandshakeTarget::Subscription => (
                    StreamError::AcknowledgementTimeout,
                    SessionStatusCause::AcknowledgementTimeout,
                ),
            };
            let frame = self
                .receive_frame(
                    socket,
                    generation,
                    Some(deadline),
                    timeout_error,
                    timeout_cause,
                    if matches!(target, HandshakeTarget::Subscription) {
                        FrameCaptureMode::SubscriptionHandshake
                    } else {
                        FrameCaptureMode::None
                    },
                    frame_sequence,
                )
                .await?;
            let mut acknowledgement_seen = false;
            let mut trailing_data = Vec::new();
            let mut event_ordinal = 0_u32;
            let DecodedProviderFrame {
                messages,
                raw_frame,
            } = frame;
            for message in messages {
                match message {
                    ProviderMessage::Error(error) => {
                        publish_provider_error(&self.publishers, generation, error)
                            .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
                        return Err(provider_failure(error));
                    }
                    ProviderMessage::Unknown(type_tag) => {
                        self.publish_unknown(generation, type_tag)?;
                    }
                    ProviderMessage::Success(SuccessMessage::Other) => {}
                    ProviderMessage::Success(SuccessMessage::Connected)
                        if matches!(target, HandshakeTarget::Connected)
                            && !acknowledgement_seen =>
                    {
                        acknowledgement_seen = true;
                    }
                    ProviderMessage::Success(SuccessMessage::Authenticated)
                        if matches!(target, HandshakeTarget::Authenticated)
                            && !acknowledgement_seen =>
                    {
                        acknowledgement_seen = true;
                    }
                    ProviderMessage::Subscription(acknowledgement)
                        if matches!(target, HandshakeTarget::Subscription)
                            && !acknowledgement_seen =>
                    {
                        if !self.acknowledgement_matches(&acknowledgement) {
                            return Err(failed(
                                StreamError::SubscriptionRejected,
                                SessionStatusCause::SubscriptionRejected,
                                false,
                            ));
                        }
                        publish_subscription(&self.publishers, generation, acknowledgement)
                            .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
                        acknowledgement_seen = true;
                    }
                    quote @ (ProviderMessage::Quote(_) | ProviderMessage::Trade(_))
                        if matches!(target, HandshakeTarget::Subscription)
                            && acknowledgement_seen =>
                    {
                        if !raw_event_is_normalizable(&quote) {
                            return Err(self.protocol_violation(
                                generation,
                                ProtocolViolationReason::UnexpectedMarketMessage,
                            ));
                        }
                        event_ordinal = event_ordinal.checked_add(1).ok_or_else(|| {
                            failed(
                                StreamError::SequenceExhausted,
                                SessionStatusCause::ProtocolViolation,
                                false,
                            )
                        })?;
                        let Some(raw_frame) = raw_frame.clone() else {
                            return Err(self.protocol_violation(
                                generation,
                                ProtocolViolationReason::UnexpectedMarketMessage,
                            ));
                        };
                        if event_ordinal > raw_frame.event_count {
                            return Err(self.protocol_violation(
                                generation,
                                ProtocolViolationReason::UnexpectedMarketMessage,
                            ));
                        }
                        trailing_data.push(FramedMarketMessage {
                            message: quote,
                            raw_frame,
                            event_ordinal,
                        });
                    }
                    ProviderMessage::Success(_)
                    | ProviderMessage::Subscription(_)
                    | ProviderMessage::Quote(_)
                    | ProviderMessage::Trade(_) => {
                        return Err(failed(
                            StreamError::ProtocolViolation,
                            SessionStatusCause::ProtocolViolation,
                            false,
                        ));
                    }
                }
            }
            if acknowledgement_seen {
                return Ok(trailing_data);
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Protocol deadlines and both capture ACK phases remain auditable together.
    async fn receive_frame<S: StreamSocket>(
        &mut self,
        socket: &mut S,
        generation: SessionGeneration,
        deadline: Option<Instant>,
        timeout_error: StreamError,
        timeout_cause: SessionStatusCause,
        capture_mode: FrameCaptureMode,
        frame_sequence: &mut u64,
    ) -> Result<DecodedProviderFrame, AttemptEnd> {
        let receive = async {
            match deadline {
                Some(deadline) => match time::timeout_at(deadline, socket.receive()).await {
                    Ok(Ok(frame)) => Ok(frame),
                    Ok(Err(_)) => Err(ReceiveFailure::Transport),
                    Err(_) => Err(ReceiveFailure::Timeout(timeout_error)),
                },
                None => socket
                    .receive()
                    .await
                    .map_err(|_| ReceiveFailure::Transport),
            }
        };
        match cancellable(receive, &mut self.shutdown, &mut self.consumers_closed).await {
            Abort::Cancelled => Err(AttemptEnd::Cancelled),
            Abort::ConsumersClosed => Err(AttemptEnd::ConsumersClosed),
            Abort::Completed(Err(ReceiveFailure::Timeout(error))) => {
                Err(failed(error, timeout_cause, true))
            }
            Abort::Completed(Err(ReceiveFailure::Transport) | Ok(None)) => Err(failed(
                StreamError::TransportLost,
                SessionStatusCause::TransportLost,
                true,
            )),
            Abort::Completed(Ok(Some(SocketFrame::Text))) => {
                Err(self.protocol_violation(generation, ProtocolViolationReason::TextFrame))
            }
            #[cfg(feature = "offline-test-support")]
            Abort::Completed(Ok(Some(SocketFrame::FixtureEnd)))
                if matches!(capture_mode, FrameCaptureMode::ActiveMarketData) =>
            {
                Err(AttemptEnd::FixtureEnd)
            }
            #[cfg(feature = "offline-test-support")]
            Abort::Completed(Ok(Some(SocketFrame::FixtureEnd))) => Err(self
                .protocol_violation(generation, ProtocolViolationReason::UnexpectedMarketMessage)),
            Abort::Completed(Ok(Some(SocketFrame::Binary(payload)))) => {
                let mut memory_payload = Some(payload);
                let received_at_utc =
                    chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
                let pending_capture = if !matches!(capture_mode, FrameCaptureMode::None)
                    && self.raw_frame_sink.is_some()
                {
                    Some(
                        self.persist_predecode_capture(
                            generation,
                            frame_sequence,
                            memory_payload
                                .take()
                                .expect("captured frame bytes are still owned before decode"),
                            received_at_utc,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let decode_bytes = pending_capture.as_ref().map_or_else(
                    || {
                        memory_payload
                            .as_deref()
                            .expect("memory-only frame bytes remain available before decode")
                    },
                    |pending| pending.capture.payload().as_bytes(),
                );
                match decode_frame_with_diagnostics(decode_bytes) {
                    Ok(messages) => {
                        let acknowledgement_matches = messages
                            .iter()
                            .find_map(|message| match message {
                                ProviderMessage::Subscription(acknowledgement) => {
                                    Some(self.acknowledgement_matches(acknowledgement))
                                }
                                _ => None,
                            })
                            .unwrap_or(false);
                        let analysis =
                            analyze_raw_frame(&messages, capture_mode, acknowledgement_matches)
                                .map_err(|_| {
                                    self.protocol_violation(
                                        generation,
                                        ProtocolViolationReason::UnexpectedMarketMessage,
                                    )
                                })?;
                        let raw_frame = match pending_capture {
                            Some(pending) => {
                                let summary = analysis.unwrap_or_else(control_finalization);
                                Some(self.finalize_and_publish_capture(pending, summary).await?)
                            }
                            None => analysis
                                .map(|summary| {
                                    self.capture_raw_frame(
                                        generation,
                                        frame_sequence,
                                        memory_payload
                                            .take()
                                            .expect("memory-only frame bytes remain available"),
                                        received_at_utc,
                                        &summary,
                                    )
                                })
                                .transpose()?,
                        };
                        Ok(DecodedProviderFrame {
                            messages,
                            raw_frame,
                        })
                    }
                    Err(error) => {
                        if let Some(pending) = pending_capture {
                            self.finalize_and_publish_capture(
                                pending,
                                decode_failure_finalization(),
                            )
                            .await?;
                        } else if !matches!(capture_mode, FrameCaptureMode::None) {
                            self.capture_raw_frame(
                                generation,
                                frame_sequence,
                                memory_payload
                                    .take()
                                    .expect("memory-only frame bytes remain available"),
                                received_at_utc,
                                &decode_failure_finalization(),
                            )?;
                        }
                        Err(self
                            .protocol_violation(generation, ProtocolViolationReason::Decode(error)))
                    }
                }
            }
        }
    }

    fn capture_raw_frame(
        &self,
        generation: SessionGeneration,
        frame_sequence: &mut u64,
        payload: Vec<u8>,
        received_at_utc: chrono::DateTime<chrono::Utc>,
        analysis: &RawFrameFinalization,
    ) -> Result<RawFrameCorrelation, AttemptEnd> {
        *frame_sequence = frame_sequence.checked_add(1).ok_or_else(|| {
            failed(
                StreamError::SequenceExhausted,
                SessionStatusCause::ProtocolViolation,
                false,
            )
        })?;
        let payload = match RawFramePayload::capture(payload) {
            Ok(payload) => payload,
            Err(RawFrameCaptureError::BudgetExceeded) => {
                return Err(AttemptEnd::Failed(lane_failure(
                    LaneFailure::RawFrameOverloaded,
                )));
            }
            Err(RawFrameCaptureError::Oversized) => {
                return Err(
                    self.protocol_violation(generation, ProtocolViolationReason::RawFrameOversized)
                );
            }
        };
        let frame = crate::InboundRawMarketFrame {
            capture_key: None,
            source_generation: generation,
            frame_sequence: *frame_sequence,
            received_at_utc,
            wire_encoding: broker_ports::RawFrameWireEncoding::MessagePack,
            event_count: analysis.event_count(),
            symbols: analysis.symbols().to_vec(),
            numeric_encoding: analysis.numeric_encoding(),
            disposition: analysis.disposition(),
            payload: payload.clone(),
        };
        self.publishers
            .control(ControlEvent::RawMarketFrame(frame))
            .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
        Ok(RawFrameCorrelation {
            frame_sequence: *frame_sequence,
            event_count: analysis.event_count(),
            sha256: payload.sha256().to_owned(),
        })
    }

    async fn persist_predecode_capture(
        &mut self,
        generation: SessionGeneration,
        frame_sequence: &mut u64,
        payload: Vec<u8>,
        received_at_utc: chrono::DateTime<chrono::Utc>,
    ) -> Result<PendingRawCapture, AttemptEnd> {
        *frame_sequence = frame_sequence.checked_add(1).ok_or_else(|| {
            failed(
                StreamError::SequenceExhausted,
                SessionStatusCause::ProtocolViolation,
                false,
            )
        })?;
        let payload = RawFramePayload::capture(payload).map_err(|_| raw_capture_failure())?;
        let feed = self
            .config
            .feed
            .as_str()
            .map_err(|_| raw_capture_failure())?;
        let timestamp = UtcTimestamp::parse(
            &received_at_utc.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .map_err(|_| raw_capture_failure())?;
        let sink = Arc::clone(
            self.raw_frame_sink
                .as_ref()
                .expect("predecode capture requires a configured sink"),
        );
        let capture_instance_id = self
            .raw_capture_instance_id
            .expect("sink identity is fixed at logical subscription creation");
        let capture = RawFrameCapture::new(
            capture_instance_id,
            "alpaca",
            feed,
            EntitlementState::Unknown,
            generation.get(),
            *frame_sequence,
            timestamp,
            RawFrameWireEncoding::MessagePack,
            payload,
        )
        .map_err(|_| raw_capture_failure())?;
        let predecode = time::timeout(RAW_FRAME_SINK_TIMEOUT, sink.persist_before_decode(&capture));
        let acknowledgement =
            match cancellable(predecode, &mut self.shutdown, &mut self.consumers_closed).await {
                Abort::Cancelled => return Err(AttemptEnd::Cancelled),
                Abort::ConsumersClosed => return Err(AttemptEnd::ConsumersClosed),
                Abort::Completed(Err(_) | Ok(Err(_))) => {
                    return Err(raw_capture_failure());
                }
                Abort::Completed(Ok(Ok(acknowledgement))) => acknowledgement,
            };
        if !acknowledgement.matches(&capture) {
            return Err(raw_capture_failure());
        }
        Ok(PendingRawCapture {
            sink,
            capture,
            acknowledgement,
            received_at_utc,
        })
    }

    async fn finalize_and_publish_capture(
        &mut self,
        pending: PendingRawCapture,
        summary: RawFrameFinalization,
    ) -> Result<RawFrameCorrelation, AttemptEnd> {
        let finalization = time::timeout(
            RAW_FRAME_SINK_TIMEOUT,
            pending
                .sink
                .finalize_after_decode(&pending.acknowledgement, &summary),
        );
        let acknowledgement =
            match cancellable(finalization, &mut self.shutdown, &mut self.consumers_closed).await {
                Abort::Cancelled => return Err(AttemptEnd::Cancelled),
                Abort::ConsumersClosed => return Err(AttemptEnd::ConsumersClosed),
                Abort::Completed(Err(_) | Ok(Err(_))) => {
                    return Err(raw_capture_failure());
                }
                Abort::Completed(Ok(Ok(acknowledgement))) => acknowledgement,
            };
        if !acknowledgement.matches(&pending.acknowledgement, &summary) {
            return Err(raw_capture_failure());
        }
        let payload = pending.capture.payload().clone();
        let frame = crate::InboundRawMarketFrame {
            capture_key: Some(pending.capture.capture_key().clone()),
            source_generation: SessionGeneration::new(pending.capture.source_generation()),
            frame_sequence: pending.capture.frame_sequence(),
            received_at_utc: pending.received_at_utc,
            wire_encoding: pending.capture.wire_encoding(),
            event_count: summary.event_count(),
            symbols: summary.symbols().to_vec(),
            numeric_encoding: summary.numeric_encoding(),
            disposition: summary.disposition(),
            payload: payload.clone(),
        };
        self.publishers
            .control(ControlEvent::RawMarketFrame(frame))
            .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
        Ok(RawFrameCorrelation {
            frame_sequence: pending.capture.frame_sequence(),
            event_count: summary.event_count(),
            sha256: payload.sha256().to_owned(),
        })
    }

    async fn process_active_frame(
        &mut self,
        generation: SessionGeneration,
        sequence: &mut u64,
        frame: DecodedProviderFrame,
    ) -> Result<(), AttemptEnd> {
        let mut market = Vec::new();
        let mut event_ordinal = 0_u32;
        for message in frame.messages {
            match message {
                ProviderMessage::Error(error) => {
                    publish_provider_error(&self.publishers, generation, error)
                        .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
                    return Err(provider_failure(error));
                }
                ProviderMessage::Unknown(type_tag) => {
                    self.publish_unknown(generation, type_tag)?;
                    return Err(failed(
                        StreamError::ProtocolViolation,
                        SessionStatusCause::ProtocolViolation,
                        false,
                    ));
                }
                ProviderMessage::Subscription(_) => {
                    return Err(self.protocol_violation(
                        generation,
                        ProtocolViolationReason::UnexpectedSubscription,
                    ));
                }
                ProviderMessage::Success(_) => {
                    return Err(self.protocol_violation(
                        generation,
                        ProtocolViolationReason::UnexpectedSuccess,
                    ));
                }
                quote @ (ProviderMessage::Quote(_) | ProviderMessage::Trade(_)) => {
                    if !raw_event_is_normalizable(&quote) {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::UnexpectedMarketMessage,
                        ));
                    }
                    event_ordinal = event_ordinal.checked_add(1).ok_or_else(|| {
                        failed(
                            StreamError::SequenceExhausted,
                            SessionStatusCause::ProtocolViolation,
                            false,
                        )
                    })?;
                    let Some(raw_frame) = frame.raw_frame.clone() else {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::UnexpectedMarketMessage,
                        ));
                    };
                    if event_ordinal > raw_frame.event_count {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::UnexpectedMarketMessage,
                        ));
                    }
                    market.push(FramedMarketMessage {
                        message: quote,
                        raw_frame,
                        event_ordinal,
                    });
                }
            }
        }
        self.process_market_data(generation, sequence, market).await
    }

    async fn process_market_data(
        &mut self,
        generation: SessionGeneration,
        sequence: &mut u64,
        messages: Vec<FramedMarketMessage>,
    ) -> Result<(), AttemptEnd> {
        for framed in messages {
            let stamp = Self::next_ingest(
                generation,
                sequence,
                &framed.raw_frame,
                framed.event_ordinal,
            )?;
            match framed.message {
                ProviderMessage::Quote(quote) => {
                    if !self.config.subscriptions.wants_quote(&quote.symbol) {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::QuoteSymbolNotSubscribed,
                        ));
                    }
                    if quote.raw_frame_sha256 != framed.raw_frame.sha256 {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::UnexpectedMarketMessage,
                        ));
                    }
                    let freshness = classify_freshness(
                        &quote.timestamp,
                        self.freshness_clock.now(),
                        self.config.limits.ready_max_age,
                        self.config.limits.ready_future_skew,
                    );
                    self.publishers
                        .quote(QuoteUpdate {
                            quote,
                            feed: self.config.feed,
                            ingest: stamp,
                            freshness,
                            coalesced_updates: 0,
                        })
                        .await
                        .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
                    self.maybe_ready(generation, freshness)?;
                }
                ProviderMessage::Trade(trade) => {
                    if !self.config.subscriptions.wants_trade(&trade.symbol) {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::TradeSymbolNotSubscribed,
                        ));
                    }
                    if trade.raw_frame_sha256 != framed.raw_frame.sha256 {
                        return Err(self.protocol_violation(
                            generation,
                            ProtocolViolationReason::UnexpectedMarketMessage,
                        ));
                    }
                    let freshness = classify_freshness(
                        &trade.timestamp,
                        self.freshness_clock.now(),
                        self.config.limits.ready_max_age,
                        self.config.limits.ready_future_skew,
                    );
                    self.publishers
                        .trade(TradeUpdate {
                            trade,
                            feed: self.config.feed,
                            ingest: stamp,
                            freshness,
                        })
                        .await
                        .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))?;
                    self.maybe_ready(generation, freshness)?;
                }
                _ => {
                    return Err(self.protocol_violation(
                        generation,
                        ProtocolViolationReason::UnexpectedMarketMessage,
                    ));
                }
            }
        }
        Ok(())
    }

    fn next_ingest(
        generation: SessionGeneration,
        sequence: &mut u64,
        raw_frame: &RawFrameCorrelation,
        event_ordinal: u32,
    ) -> Result<IngestStamp, AttemptEnd> {
        *sequence = (*sequence).checked_add(1).ok_or_else(|| {
            failed(
                StreamError::SequenceExhausted,
                SessionStatusCause::ProtocolViolation,
                false,
            )
        })?;
        Ok(IngestStamp {
            generation,
            sequence: *sequence,
            raw_frame_sequence: raw_frame.frame_sequence,
            raw_frame_event_ordinal: event_ordinal,
            raw_frame_event_count: raw_frame.event_count,
            received_at: Instant::now(),
            received_at_utc: chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()),
        })
    }

    fn maybe_ready(
        &mut self,
        generation: SessionGeneration,
        freshness: DataFreshness,
    ) -> Result<(), AttemptEnd> {
        if freshness == DataFreshness::Fresh
            && self.phases.phase() == InternalPhase::AwaitingFreshData
        {
            self.transition(generation, InternalPhase::Ready, None)
                .map_err(|error| {
                    AttemptEnd::Failed(terminal(error, SessionStatusCause::ProtocolViolation))
                })?;
        }
        Ok(())
    }

    fn acknowledgement_matches(
        &self,
        acknowledgement: &crate::model::SubscriptionAcknowledgement,
    ) -> bool {
        acknowledgement.quotes == self.config.subscriptions.quotes
            && acknowledgement.trades == self.config.subscriptions.trades
    }

    fn publish_unknown(
        &self,
        generation: SessionGeneration,
        type_tag: String,
    ) -> Result<(), AttemptEnd> {
        publish_unknown(&self.publishers, generation, type_tag)
            .map_err(|failure| AttemptEnd::Failed(lane_failure(failure)))
    }

    fn protocol_violation(
        &self,
        generation: SessionGeneration,
        reason: ProtocolViolationReason,
    ) -> AttemptEnd {
        log_protocol_violation(
            self.config.feed,
            generation,
            self.phases.phase().public(),
            reason,
        );
        failed(
            StreamError::ProtocolViolation,
            SessionStatusCause::ProtocolViolation,
            false,
        )
    }

    fn transition(
        &mut self,
        generation: SessionGeneration,
        next: InternalPhase,
        cause: Option<SessionStatusCause>,
    ) -> Result<(), StreamError> {
        self.phases
            .transition(next)
            .map_err(|_| StreamError::InternalStateViolation)?;
        publish_phase(&self.publishers, generation, next.public(), cause).map_err(map_lane_failure)
    }

    async fn finish_generation(
        &mut self,
        generation: SessionGeneration,
        cause: SessionStatusCause,
    ) {
        self.publishers.invalidate_generation(generation).await;
        if self.phases.phase() != InternalPhase::SessionLost
            && self.phases.phase() != InternalPhase::Closed
            && self.phases.transition(InternalPhase::SessionLost).is_ok()
        {
            let _ = publish_phase(
                &self.publishers,
                generation,
                SessionPhase::SessionLost,
                Some(cause),
            );
        }
        if self.phases.phase() != InternalPhase::Closed
            && self.phases.transition(InternalPhase::Closed).is_ok()
        {
            let _ = publish_phase(
                &self.publishers,
                generation,
                SessionPhase::Closed,
                Some(cause),
            );
        }
    }
}

fn analyze_raw_frame(
    messages: &[ProviderMessage],
    mode: FrameCaptureMode,
    acknowledgement_matches: bool,
) -> Result<Option<RawFrameFinalization>, RawFrameFinalizationError> {
    let mut event_count = 0_u32;
    let mut symbols = BTreeSet::new();
    let mut observed_encoding = None;
    let mut mixed_encoding = false;
    let mut saw_unknown = false;
    let mut saw_provider_error = false;
    let mut acknowledgement_seen = false;
    let mut protocol_shape_invalid = false;

    for message in messages {
        match message {
            ProviderMessage::Quote(quote) => {
                if matches!(mode, FrameCaptureMode::SubscriptionHandshake) && !acknowledgement_seen
                {
                    protocol_shape_invalid = true;
                }
                let bid = numeric_encoding(quote.bid_price);
                let ask = numeric_encoding(quote.ask_price);
                if bid == ask {
                    observe_encoding(&mut observed_encoding, &mut mixed_encoding, bid);
                } else {
                    mixed_encoding = true;
                    protocol_shape_invalid = true;
                }
                if raw_event_is_normalizable(message) && bid == ask {
                    event_count = event_count.saturating_add(1);
                    symbols.insert(quote.symbol.as_str().to_owned());
                } else {
                    protocol_shape_invalid = true;
                }
            }
            ProviderMessage::Trade(trade) => {
                if matches!(mode, FrameCaptureMode::SubscriptionHandshake) && !acknowledgement_seen
                {
                    protocol_shape_invalid = true;
                }
                observe_encoding(
                    &mut observed_encoding,
                    &mut mixed_encoding,
                    numeric_encoding(trade.price),
                );
                if raw_event_is_normalizable(message) {
                    event_count = event_count.saturating_add(1);
                    symbols.insert(trade.symbol.as_str().to_owned());
                } else {
                    protocol_shape_invalid = true;
                }
            }
            ProviderMessage::Unknown(_) => saw_unknown = true,
            ProviderMessage::Error(_) => saw_provider_error = true,
            ProviderMessage::Subscription(_) => {
                if !matches!(mode, FrameCaptureMode::SubscriptionHandshake)
                    || acknowledgement_seen
                    || !acknowledgement_matches
                {
                    protocol_shape_invalid = true;
                }
                acknowledgement_seen = true;
            }
            ProviderMessage::Success(SuccessMessage::Other) => {}
            ProviderMessage::Success(_) => protocol_shape_invalid = true,
        }
    }

    if matches!(mode, FrameCaptureMode::None) {
        return Ok(None);
    }
    if !saw_unknown
        && !saw_provider_error
        && !protocol_shape_invalid
        && (event_count == 0
            || matches!(mode, FrameCaptureMode::SubscriptionHandshake) && !acknowledgement_seen)
    {
        return Ok(None);
    }

    let disposition = if saw_unknown {
        RawFrameDisposition::UnknownMessage
    } else if saw_provider_error {
        RawFrameDisposition::ProviderError
    } else if protocol_shape_invalid {
        RawFrameDisposition::DecodeFailure
    } else {
        RawFrameDisposition::DecodedMarketData
    };
    let (event_count, symbols, encoding) =
        if matches!(disposition, RawFrameDisposition::DecodeFailure) {
            (0, Vec::new(), None)
        } else {
            (
                event_count,
                symbols.into_iter().collect(),
                (event_count > 0 && !mixed_encoding)
                    .then_some(observed_encoding)
                    .flatten(),
            )
        };
    RawFrameFinalization::new(event_count, symbols, encoding, disposition).map(Some)
}

fn control_finalization() -> RawFrameFinalization {
    RawFrameFinalization::new(0, Vec::new(), None, RawFrameDisposition::ControlMessage)
        .expect("fixed control summary is valid")
}

fn decode_failure_finalization() -> RawFrameFinalization {
    RawFrameFinalization::new(0, Vec::new(), None, RawFrameDisposition::DecodeFailure)
        .expect("fixed decode-failure summary is valid")
}

fn raw_capture_failure() -> AttemptEnd {
    AttemptEnd::Failed(terminal(
        StreamError::RawCaptureFailed,
        SessionStatusCause::ProtocolViolation,
    ))
}

fn observe_encoding(
    observed: &mut Option<NumericEncodingV1>,
    mixed: &mut bool,
    next: NumericEncodingV1,
) {
    if let Some(current) = observed {
        if *current != next {
            *mixed = true;
        }
    } else {
        *observed = Some(next);
    }
}

fn numeric_encoding(number: crate::MarketNumber) -> NumericEncodingV1 {
    number.encoding()
}

fn raw_event_is_normalizable(message: &ProviderMessage) -> bool {
    match message {
        ProviderMessage::Quote(quote) => {
            let (Some(bid), Some(ask)) = (
                quote.bid_price.decimal_string(),
                quote.ask_price.decimal_string(),
            ) else {
                return false;
            };
            MarketEventV1::OptionQuote {
                symbol: quote.symbol.as_str().to_owned(),
                bid: Some(bid),
                ask: Some(ask),
                bid_size: DecimalString::new(quote.bid_size.to_string()).ok(),
                ask_size: DecimalString::new(quote.ask_size.to_string()).ok(),
            }
            .validate()
            .is_ok()
                && timestamp_is_representable(&quote.timestamp)
        }
        ProviderMessage::Trade(trade) => {
            let Some(price) = trade.price.decimal_string() else {
                return false;
            };
            MarketEventV1::OptionTrade {
                symbol: trade.symbol.as_str().to_owned(),
                price,
                size: DecimalString::new(trade.size.to_string())
                    .expect("u64 decimal text is a valid decimal token"),
            }
            .validate()
            .is_ok()
                && timestamp_is_representable(&trade.timestamp)
        }
        _ => true,
    }
}

fn timestamp_is_representable(timestamp: &crate::ProviderTimestamp) -> bool {
    chrono::DateTime::<chrono::Utc>::from_timestamp(
        timestamp.unix_seconds(),
        timestamp.nanosecond(),
    )
    .is_some()
}
