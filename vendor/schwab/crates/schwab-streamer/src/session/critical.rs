//! Bounded, lossless delivery for critical Streamer session events.
//!
//! 关键 Streamer 事件使用有界队列；达到上限时显式终止 session。

use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

use super::{SessionEvent, SessionRunError};

/// Unique critical-event consumer paired with a control handle.
/// 与控制句柄配对的唯一关键事件消费者。
pub struct CriticalEventReceiver {
    pub(super) buffer: Arc<CriticalBuffer>,
}

impl Debug for CriticalEventReceiver {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CriticalEventReceiver")
            .field("buffer", &"[REDACTED]")
            .finish()
    }
}

impl CriticalEventReceiver {
    /// Receives the next critical event, or None after runtime shutdown.
    /// 获取下一个关键事件；运行时关闭后返回 `None`。
    pub async fn recv(&mut self) -> Option<SessionEvent> {
        loop {
            let notified = self.buffer.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.buffer.state.lock().await;
                if let Some(event) = state.events.pop_front() {
                    state.queued_bytes = state.queued_bytes.saturating_sub(event.bytes);
                    return Some(event.event);
                }
                if state.closed
                    || self
                        .buffer
                        .close_requested
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    return None;
                }
            }
            notified.await;
        }
    }
}

// INVARIANT: Dropping the sole receiver closes delivery; the producer fails closed.
// 不变量：唯一消费者被释放后交付通道关闭，生产者以 fail-closed 方式停止。
impl Drop for CriticalEventReceiver {
    fn drop(&mut self) {
        let buffer = self.buffer.clone();
        if let Ok(mut state) = buffer.state.try_lock() {
            state.closed = true;
            buffer.notify.notify_waiters();
        } else {
            buffer
                .close_requested
                .store(true, std::sync::atomic::Ordering::Release);
            buffer.notify.notify_waiters();
        }
    }
}

struct CriticalEnvelope {
    event: SessionEvent,
    bytes: usize,
}

struct CriticalState {
    events: VecDeque<CriticalEnvelope>,
    queued_bytes: usize,
    closed: bool,
}

pub(super) struct CriticalBuffer {
    state: Mutex<CriticalState>,
    notify: Notify,
    capacity: usize,
    max_bytes: usize,
    close_requested: std::sync::atomic::AtomicBool,
}

impl CriticalBuffer {
    pub(super) fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(CriticalState {
                events: VecDeque::with_capacity(capacity),
                queued_bytes: 0,
                closed: false,
            }),
            notify: Notify::new(),
            capacity,
            max_bytes,
            close_requested: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) async fn push(&self, event: SessionEvent) -> Result<(), SessionRunError> {
        let bytes = critical_event_size(&event)?;
        let mut state = self.state.lock().await;
        if self
            .close_requested
            .load(std::sync::atomic::Ordering::Acquire)
            || state.closed
        {
            return Err(SessionRunError::CriticalConsumerClosed);
        }
        // INVARIANT: Overflow fails the session explicitly; critical events are never silently dropped.
        // 不变量：队列溢出会显式终止 session；关键事件绝不静默丢弃。
        if state.events.len() >= self.capacity
            || bytes > self.max_bytes.saturating_sub(state.queued_bytes)
        {
            return Err(SessionRunError::CriticalDeliveryOverflow);
        }
        state.queued_bytes = state.queued_bytes.saturating_add(bytes);
        state.events.push_back(CriticalEnvelope { event, bytes });
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    pub(super) async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        drop(state);
        self.notify.notify_waiters();
    }

    // INVARIANT: Owner drop wakes the receiver without taking its lock; queued critical events remain drainable.
    // 不变量：owner 释放时无需持有消费者锁即可唤醒接收端；已排队关键事件仍可继续读取。
    pub(super) fn close_without_waiting(&self) {
        self.close_requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_waiters();
    }
}

fn critical_event_size(event: &SessionEvent) -> Result<usize, SessionRunError> {
    match event {
        SessionEvent::Activity { payload, .. } => serde_json::to_vec(payload)
            .map(|bytes| bytes.len())
            .map_err(|_| SessionRunError::CriticalDeliveryOverflow),
        SessionEvent::Notification(payload) => serde_json::to_vec(payload)
            .map(|bytes| bytes.len())
            .map_err(|_| SessionRunError::CriticalDeliveryOverflow),
        _ => Ok(256),
    }
}
