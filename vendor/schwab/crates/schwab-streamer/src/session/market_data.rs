//! Bounded latest-value delivery and sparse-row coalescing for market data.
//!
//! 市场数据通道以有界的最新值缓冲合并稀疏更新。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
#[cfg(test)]
use tokio::time::timeout;

use super::fingerprint::streamer_delta_fingerprint;
use super::{
    ConnectionGeneration, MARKET_DATA_ORDER_FENCE_ENTRY_OVERHEAD, MARKET_FIELD_PROVENANCE_OVERHEAD,
    MARKET_ROW_PROVENANCE_METADATA_OVERHEAD, MAX_MARKET_DATA_FENCE_BYTES,
    MAX_MARKET_DATA_FENCE_KEY_BYTES, MAX_MARKET_DATA_FENCE_KEYS, MAX_MERGED_MARKET_FIELDS,
    MAX_MERGED_MARKET_PROVENANCE_BYTES, MAX_MERGED_MARKET_ROW_BYTES, SessionRunError,
    StreamerService,
};

/// Source and local receive times for one field in a coalesced quote row.
/// 稀疏行情合并后，单个字段的行情源时间与本地接收时间。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MarketDataFieldProvenance {
    /// Broker timestamp of the accepted delta that last changed this field.
    /// 最近一次改变此字段的有效增量所携带的 broker 时间戳。
    pub source_timestamp: f64,
    /// Monotonic instant when the delta carrying this field reached the buffer.
    /// 携带此字段的增量到达缓冲区时的单调时钟时刻。
    pub received_at: Instant,
    /// Row revision assigned to the delta that most recently supplied this field.
    /// 最近一次提供此字段的增量所分配的行 revision。
    pub update_revision: u64,
}

/// Coalesced latest sparse quote row for one service/key pair.
/// 按服务和键合并后的最新稀疏行情行。
pub struct MarketDataUpdate {
    /// Market-data service which produced this update.
    /// 产生此更新的行情服务。
    pub service: StreamerService,
    /// Socket generation that owns this merged row.
    /// 拥有此合并行的 socket generation。
    pub generation: ConnectionGeneration,
    /// Exact broker key from the service row.
    /// 服务数据行中的原始 broker 键。
    pub key: String,
    /// Broker timestamp of the latest accepted delta.
    /// 最近一次接受的增量所携带的 broker 时间戳。
    pub source_timestamp: f64,
    /// Monotonic per-key revision assigned after sparse merge.
    /// 稀疏合并后为此键分配的单调递增 revision。
    pub revision: u64,
    /// Number of later deltas merged into this queued row.
    /// 合并进此排队数据行的后续增量数量。
    pub coalesced_updates: u64,
    /// Merged field set; absent delta fields preserve previous values.
    /// 合并后的字段集合；增量中缺失的字段保留先前值。
    pub fields: BTreeMap<String, Value>,
    /// Last-changed provenance for each retained field, keyed identically to `fields`.
    /// 每个保留字段最近一次变更的来源信息，键与 `fields` 一致。
    pub field_provenance: BTreeMap<String, MarketDataFieldProvenance>,
    /// Local monotonic receive instant of the latest accepted delta.
    /// 最近一次接受的增量的本地单调时钟接收时刻。
    pub received_at: Instant,
}

impl Debug for MarketDataUpdate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketDataUpdate")
            .field("service", &self.service)
            .field("generation", &self.generation)
            .field("key", &"[REDACTED]")
            .field("source_timestamp", &self.source_timestamp)
            .field("revision", &self.revision)
            .field("coalesced_updates", &self.coalesced_updates)
            .field("fields", &"[REDACTED]")
            .field("field_provenance", &"[REDACTED]")
            .field("received_at", &self.received_at)
            .finish()
    }
}

/// Unique latest-value market-data consumer. Rows are keyed by service/key.
/// 唯一的最新值市场数据消费者；更新按服务和键分组。
pub struct MarketDataReceiver {
    pub(super) buffer: Arc<MarketDataBuffer>,
}

impl Debug for MarketDataReceiver {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketDataReceiver")
            .field("buffer", &"[REDACTED]")
            .finish()
    }
}

impl MarketDataReceiver {
    /// Receives the next coalesced sparse quote update, or None at shutdown.
    /// After runtime cancellation, the first poll discards any queued quotes
    /// and ordering fences before returning None.
    /// 运行时取消后，首次轮询会清空排队报价和顺序栅栏，再返回 `None`。
    pub async fn recv(&mut self) -> Option<MarketDataUpdate> {
        loop {
            let notified = self.buffer.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.buffer.state.lock().await;
                if state.closed
                    || self
                        .buffer
                        .close_requested
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    state.closed = true;
                    state.clear();
                    return None;
                }
                if let Some(update) = state.pop_first() {
                    if self
                        .buffer
                        .close_requested
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        state.closed = true;
                        state.clear();
                        return None;
                    }
                    return Some(update.update);
                }
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn buffer_state_counts_for_test(&self) -> (bool, usize, usize, usize) {
        let state = self.buffer.state.lock().await;
        (
            state.closed,
            state.len(),
            state.fence_len(),
            state.fence_bytes,
        )
    }
}

impl Drop for MarketDataReceiver {
    fn drop(&mut self) {
        let buffer = self.buffer.clone();
        if let Ok(mut state) = buffer.state.try_lock() {
            state.closed = true;
            state.clear();
            buffer.notify.notify_waiters();
        } else {
            buffer
                .close_requested
                .store(true, std::sync::atomic::Ordering::Release);
            buffer.notify.notify_waiters();
        }
    }
}

struct MarketBufferState {
    generation: Option<ConnectionGeneration>,
    // INVARIANT: Service maps preserve (service, key) ordering without cross-service merging.
    // 不变量：服务映射保持（服务、键）顺序，不会跨服务合并数据。
    // Keep one ordered key map per market service so the hot update path can
    // look up the incoming String by &str without allocating a temporary key.
    // Array order matches the prior (service, key) BTreeMap ordering.
    updates: [BTreeMap<String, BufferedMarketDataUpdate>; 2],
    // INVARIANT: Ordering fences survive delivery and reset only at generation change or shutdown.
    // 不变量：数据交付后顺序栅栏仍保留，只在 generation 变化或关闭时重置。
    // Ordering fences outlive queue delivery so duplicates and stale rows stay
    // fenced until generation reset. The two maps together share one hard cap.
    fences: [BTreeMap<String, MarketDataOrderingFence>; 2],
    fence_bytes: usize,
    closed: bool,
}

impl MarketBufferState {
    fn updates_for_mut(
        &mut self,
        service: StreamerService,
    ) -> Option<&mut BTreeMap<String, BufferedMarketDataUpdate>> {
        self.updates.get_mut(market_data_service_index(service)?)
    }

    fn len(&self) -> usize {
        self.updates.iter().map(BTreeMap::len).sum()
    }

    fn fence_len(&self) -> usize {
        self.fences.iter().map(BTreeMap::len).sum()
    }

    fn pop_first(&mut self) -> Option<BufferedMarketDataUpdate> {
        self.updates
            .iter_mut()
            .find_map(|service_updates| service_updates.pop_first().map(|(_, row)| row))
    }

    fn clear(&mut self) {
        for service_updates in &mut self.updates {
            service_updates.clear();
        }
        for service_fences in &mut self.fences {
            service_fences.clear();
        }
        self.fence_bytes = 0;
    }
}

#[derive(Clone, Copy)]
struct MarketDataOrderingFence {
    source_timestamp: f64,
    revision: u64,
    raw_delta_fingerprint: [u8; 32],
}

fn market_data_service_index(service: StreamerService) -> Option<usize> {
    match service {
        StreamerService::LevelOneEquities => Some(0),
        StreamerService::LevelOneOptions => Some(1),
        StreamerService::AcctActivity => None,
    }
}

struct BufferedMarketDataUpdate {
    update: MarketDataUpdate,
    encoded_size: usize,
    provenance_size: usize,
}

pub(super) struct MarketDataBuffer {
    state: Mutex<MarketBufferState>,
    notify: Notify,
    capacity: usize,
    fence_capacity: usize,
    fence_byte_capacity: usize,
    close_requested: std::sync::atomic::AtomicBool,
}

impl MarketDataBuffer {
    pub(super) fn new(capacity: usize) -> Self {
        Self::with_limits(
            capacity,
            MAX_MARKET_DATA_FENCE_KEYS,
            MAX_MARKET_DATA_FENCE_BYTES,
        )
    }

    fn with_limits(capacity: usize, fence_capacity: usize, fence_byte_capacity: usize) -> Self {
        Self {
            state: Mutex::new(MarketBufferState {
                generation: None,
                updates: std::array::from_fn(|_| BTreeMap::new()),
                fences: std::array::from_fn(|_| BTreeMap::new()),
                fence_bytes: 0,
                closed: false,
            }),
            notify: Notify::new(),
            capacity,
            fence_capacity,
            fence_byte_capacity,
            close_requested: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) async fn reset_generation(&self, generation: Option<ConnectionGeneration>) {
        let mut state = self.state.lock().await;
        state.generation = generation;
        state.clear();
        drop(state);
        self.notify.notify_waiters();
    }

    // Keep the two-phase stale check, hashing outside the lock, and atomic
    // merge/fence update adjacent so the bounded buffer invariant is reviewable.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn push_delta(
        &self,
        service: StreamerService,
        generation: ConnectionGeneration,
        key: String,
        source_timestamp: f64,
        fields: BTreeMap<String, Value>,
    ) -> Result<(), SessionRunError> {
        let Some(service_index) = market_data_service_index(service) else {
            return Err(SessionRunError::MarketDataCapacityExceeded);
        };
        if !source_timestamp.is_finite()
            || key.is_empty()
            || key.len() > MAX_MARKET_DATA_FENCE_KEY_BYTES
        {
            return Err(SessionRunError::MarketDataCapacityExceeded);
        }

        // INVARIANT: Reject stale updates before hashing, then recheck under the authoritative lock.
        // 不变量：哈希前拒绝过期更新，并在权威锁内再次检查状态。
        // Reject stale generations and older source timestamps before walking
        // the sparse field map for serialized size or hashing. The second
        // locked check below remains authoritative if state changes while the
        // digest is computed.
        let received_at = Instant::now();
        {
            let state = self.state.lock().await;
            if self
                .close_requested
                .load(std::sync::atomic::Ordering::Acquire)
                || state.closed
            {
                return Err(SessionRunError::MarketDataConsumerClosed);
            }
            if state.generation != Some(generation) {
                return Ok(());
            }
            if let Some(fence) = state.fences[service_index].get(key.as_str()) {
                if source_timestamp < fence.source_timestamp {
                    return Ok(());
                }
            } else if state.fence_len() >= self.fence_capacity {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }
        }

        let encoded_delta_size = encoded_market_fields_size(&fields)?;
        let raw_delta_fingerprint = streamer_delta_fingerprint(&fields)?;
        let fence_entry_size = estimated_market_order_fence_size(&key)?;
        let mut state = self.state.lock().await;
        if self
            .close_requested
            .load(std::sync::atomic::Ordering::Acquire)
            || state.closed
        {
            return Err(SessionRunError::MarketDataConsumerClosed);
        }
        if state.generation != Some(generation) {
            return Ok(());
        }
        let existing_fence = state.fences[service_index].get(key.as_str()).copied();
        if let Some(fence) = existing_fence {
            if source_timestamp < fence.source_timestamp {
                return Ok(());
            }
            if source_timestamp.partial_cmp(&fence.source_timestamp)
                == Some(std::cmp::Ordering::Equal)
                && raw_delta_fingerprint == fence.raw_delta_fingerprint
            {
                return Ok(());
            }
        } else if state.fence_len() >= self.fence_capacity {
            return Err(SessionRunError::MarketDataCapacityExceeded);
        }
        let next_fence_bytes = if existing_fence.is_some() {
            state.fence_bytes
        } else {
            let next_bytes = state
                .fence_bytes
                .checked_add(fence_entry_size)
                .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            if next_bytes > self.fence_byte_capacity {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }
            next_bytes
        };
        let current_revision = state.updates[service_index]
            .get(key.as_str())
            .map(|current| current.update.revision);
        let previous_revision = current_revision
            .or_else(|| existing_fence.map(|fence| fence.revision))
            .unwrap_or(0);
        let next_revision = previous_revision
            .checked_add(1)
            .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
        let next_coalesced_updates = state.updates[service_index]
            .get(key.as_str())
            .map(|current| {
                current
                    .update
                    .coalesced_updates
                    .checked_add(1)
                    .ok_or(SessionRunError::MarketDataCapacityExceeded)
            })
            .transpose()?
            .unwrap_or(0);
        let new_fence_key = if existing_fence.is_none() {
            let mut fence_key = String::with_capacity(key.len());
            fence_key.push_str(&key);
            Some(fence_key)
        } else {
            None
        };

        if let Some(current) = state.updates[service_index].get_mut(key.as_str()) {
            let added_fields = fields
                .keys()
                .filter(|field| !current.update.fields.contains_key(*field))
                .count();
            let next_field_count = current
                .update
                .fields
                .len()
                .checked_add(added_fields)
                .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            if next_field_count > MAX_MERGED_MARKET_FIELDS {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }

            let mut next_encoded_size = current.encoded_size;
            for (field, value) in &fields {
                let encoded_value = serialized_json_size(value)?;
                if let Some(previous) = current.update.fields.get(field) {
                    next_encoded_size = next_encoded_size
                        .checked_sub(serialized_json_size(previous)?)
                        .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
                } else {
                    next_encoded_size = next_encoded_size
                        .checked_add(
                            serialized_json_size(field)?
                                .checked_add(1)
                                .ok_or(SessionRunError::MarketDataCapacityExceeded)?,
                        )
                        .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
                }
                next_encoded_size = next_encoded_size
                    .checked_add(encoded_value)
                    .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            }
            let previous_separators = current.update.fields.len().saturating_sub(1);
            let next_separators = next_field_count.saturating_sub(1);
            next_encoded_size = next_encoded_size
                .checked_add(next_separators - previous_separators)
                .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            if next_encoded_size > MAX_MERGED_MARKET_ROW_BYTES {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }

            let added_provenance_size = fields
                .keys()
                .filter(|field| !current.update.field_provenance.contains_key(*field))
                .map(|field| estimated_market_field_provenance_size(field))
                .try_fold(0usize, |total, size| {
                    total
                        .checked_add(size?)
                        .ok_or(SessionRunError::MarketDataCapacityExceeded)
                })?;
            let next_provenance_size = current
                .provenance_size
                .checked_add(added_provenance_size)
                .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            if next_provenance_size > MAX_MERGED_MARKET_PROVENANCE_BYTES {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }

            for (field, value) in fields {
                current.update.fields.insert(field.clone(), value);
                current.update.field_provenance.insert(
                    field,
                    MarketDataFieldProvenance {
                        source_timestamp,
                        received_at,
                        update_revision: next_revision,
                    },
                );
            }
            current.encoded_size = next_encoded_size;
            current.provenance_size = next_provenance_size;
            current.update.source_timestamp = source_timestamp;
            current.update.revision = next_revision;
            current.update.coalesced_updates = next_coalesced_updates;
            current.update.received_at = received_at;
        } else {
            if state.len() >= self.capacity {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }
            let provenance_size = fields
                .keys()
                .map(|field| estimated_market_field_provenance_size(field))
                .try_fold(MARKET_ROW_PROVENANCE_METADATA_OVERHEAD, |total, size| {
                    total
                        .checked_add(size?)
                        .ok_or(SessionRunError::MarketDataCapacityExceeded)
                })?;
            if provenance_size > MAX_MERGED_MARKET_PROVENANCE_BYTES {
                return Err(SessionRunError::MarketDataCapacityExceeded);
            }
            let field_provenance = fields
                .keys()
                .map(|field| {
                    (
                        field.clone(),
                        MarketDataFieldProvenance {
                            source_timestamp,
                            received_at,
                            update_revision: next_revision,
                        },
                    )
                })
                .collect();
            state.updates[service_index].insert(
                key.clone(),
                BufferedMarketDataUpdate {
                    update: MarketDataUpdate {
                        service,
                        generation,
                        key: key.clone(),
                        source_timestamp,
                        revision: next_revision,
                        coalesced_updates: 0,
                        fields,
                        field_provenance,
                        received_at,
                    },
                    encoded_size: encoded_delta_size,
                    provenance_size,
                },
            );
        }
        let new_fence = MarketDataOrderingFence {
            source_timestamp,
            revision: next_revision,
            raw_delta_fingerprint,
        };
        if let Some(fence) = state.fences[service_index].get_mut(key.as_str()) {
            *fence = new_fence;
        } else if let Some(fence_key) = new_fence_key {
            state.fence_bytes = next_fence_bytes;
            state.fences[service_index].insert(fence_key, new_fence);
        }
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    pub(super) async fn retain_keys(&self, service: StreamerService, keys: BTreeSet<String>) {
        let mut state = self.state.lock().await;
        if let Some(updates) = state.updates_for_mut(service) {
            updates.retain(|key, _| keys.contains(key));
        }
    }

    pub(super) async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.clear();
        drop(state);
        self.notify.notify_waiters();
    }

    pub(super) fn close_without_waiting(&self) {
        // INVARIANT: Deferred close is completed by the receiver before it can return another row.
        // 不变量：延迟关闭会由消费者完成，消费者不会在此后再返回排队数据。
        if let Ok(mut state) = self.state.try_lock() {
            state.closed = true;
            state.clear();
        } else {
            self.close_requested
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.notify.notify_waiters();
    }
}

fn estimated_market_order_fence_size(key: &str) -> Result<usize, SessionRunError> {
    key.len()
        .checked_add(MARKET_DATA_ORDER_FENCE_ENTRY_OVERHEAD)
        .ok_or(SessionRunError::MarketDataCapacityExceeded)
}

fn estimated_market_field_provenance_size(field: &str) -> Result<usize, SessionRunError> {
    serialized_json_size(field).and_then(|encoded_key_size| {
        encoded_key_size
            .checked_add(MARKET_FIELD_PROVENANCE_OVERHEAD)
            .ok_or(SessionRunError::MarketDataCapacityExceeded)
    })
}

fn encoded_market_fields_size(fields: &BTreeMap<String, Value>) -> Result<usize, SessionRunError> {
    if fields.len() > MAX_MERGED_MARKET_FIELDS {
        return Err(SessionRunError::MarketDataCapacityExceeded);
    }

    let mut encoded_fields_size = 0usize;
    for (field, value) in fields {
        let key_and_colon_size = serialized_json_size(field)?
            .checked_add(1)
            .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
        let encoded_one_field_size = key_and_colon_size
            .checked_add(serialized_json_size(value)?)
            .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
        encoded_fields_size = encoded_fields_size
            .checked_add(encoded_one_field_size)
            .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
    }
    let encoded_size = 2usize
        .checked_add(encoded_fields_size)
        .and_then(|size| size.checked_add(fields.len().saturating_sub(1)))
        .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
    if encoded_size > MAX_MERGED_MARKET_ROW_BYTES {
        return Err(SessionRunError::MarketDataCapacityExceeded);
    }
    Ok(encoded_size)
}

fn serialized_json_size<T: serde::Serialize + ?Sized>(value: &T) -> Result<usize, SessionRunError> {
    let mut counter = JsonSizeCounter::default();
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| SessionRunError::MarketDataCapacityExceeded)?;
    Ok(counter.bytes)
}

#[derive(Default)]
struct JsonSizeCounter {
    bytes: usize,
}

impl std::io::Write for JsonSizeCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("serialized JSON size overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod market_data_buffer_tests {
    use super::*;

    #[tokio::test]
    // This benchmark intentionally reports setup, workload, and percentile
    // calculation together so its synthetic work is easy to audit.
    #[allow(clippy::too_many_lines)]
    #[ignore = "deterministic local-only market row performance benchmark"]
    async fn ignored_market_row_sparse_update_benchmark() {
        const KEY_COUNT: usize = 1024;
        const UPDATES_PER_KEY: usize = 200;
        let profiles = [
            (
                "equities-5",
                ["0", "45", "46", "51", "52"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>(),
            ),
            (
                "sparse-64",
                (0..64)
                    .map(|index| format!("stable-{index:02}"))
                    .collect::<Vec<_>>(),
            ),
        ];

        for (profile, field_names) in profiles {
            let buffer = MarketDataBuffer::new(KEY_COUNT);
            let generation = ConnectionGeneration::new(7);
            buffer.reset_generation(Some(generation)).await;
            let keys = (0..KEY_COUNT)
                .map(|index| format!("SYNTH-{index:04}"))
                .collect::<Vec<_>>();

            for (key_index, key) in keys.iter().enumerate() {
                let fields = field_names
                    .iter()
                    .enumerate()
                    .map(|(field_index, name)| {
                        (
                            name.clone(),
                            Value::from(
                                i64::try_from(key_index * 100 + field_index)
                                    .expect("synthetic field value fits i64"),
                            ),
                        )
                    })
                    .collect();
                buffer
                    .push_delta(
                        StreamerService::LevelOneEquities,
                        generation,
                        key.clone(),
                        0.0,
                        fields,
                    )
                    .await
                    .expect("synthetic initial rows fit the configured bounds");
            }

            let mut batch_durations = Vec::with_capacity(UPDATES_PER_KEY);
            let started = std::time::Instant::now();
            for batch in 0..UPDATES_PER_KEY {
                let batch_started = std::time::Instant::now();
                for (key_index, key) in keys.iter().enumerate() {
                    let field_index = (batch + key_index) % field_names.len();
                    let mut fields = BTreeMap::new();
                    fields.insert(
                        field_names[field_index].clone(),
                        Value::from(
                            i64::try_from(batch * KEY_COUNT + key_index)
                                .expect("synthetic update value fits i64"),
                        ),
                    );
                    buffer
                        .push_delta(
                            StreamerService::LevelOneEquities,
                            generation,
                            key.clone(),
                            f64::from(
                                u32::try_from(batch + 1)
                                    .expect("synthetic timestamp fits u32"),
                            ),
                            fields,
                        )
                        .await
                        .expect("synthetic sparse updates fit the configured bounds");
                }
                batch_durations.push(batch_started.elapsed());
            }
            let elapsed = started.elapsed();
            batch_durations.sort_unstable();
            let p95_batch = batch_durations[(batch_durations.len() * 95).div_ceil(100) - 1];
            let updates = KEY_COUNT * UPDATES_PER_KEY;
            let throughput_updates_per_second = f64::from(
                u32::try_from(updates).expect("synthetic benchmark update count fits u32"),
            ) / elapsed.as_secs_f64();
            let amortized_p95_update_nanos = p95_batch.as_secs_f64() * 1_000_000_000.0
                / f64::from(u32::try_from(KEY_COUNT).expect("key count fits u32"));

            println!(
                "MARKET_ROW_BENCH profile={profile} keys={KEY_COUNT} updates_per_key={UPDATES_PER_KEY} fields_per_row={} total_updates={updates} elapsed_ns={} throughput_updates_per_second={throughput_updates_per_second:.3} p95_batch_updates={KEY_COUNT} p95_batch_ns={} p95_batch_amortized_update_ns={amortized_p95_update_nanos:.3}",
                field_names.len(),
                elapsed.as_nanos(),
                p95_batch.as_nanos(),
            );
        }
    }

    async fn active_buffer() -> (MarketDataBuffer, ConnectionGeneration) {
        let buffer = MarketDataBuffer::new(8);
        let generation = ConnectionGeneration::new(11);
        buffer.reset_generation(Some(generation)).await;
        (buffer, generation)
    }

    async fn push_row(
        buffer: &MarketDataBuffer,
        generation: ConnectionGeneration,
        key: &str,
        timestamp: f64,
        fields: BTreeMap<String, Value>,
    ) -> Result<(), SessionRunError> {
        buffer
            .push_delta(
                StreamerService::LevelOneEquities,
                generation,
                key.to_owned(),
                timestamp,
                fields,
            )
            .await
    }

    async fn assert_cached_size_matches_json(
        buffer: &MarketDataBuffer,
        key: &str,
    ) -> BTreeMap<String, Value> {
        let state = buffer.state.lock().await;
        let buffered = state.updates[0]
            .get(key)
            .expect("the synthetic row remains buffered");
        assert_eq!(
            buffered.encoded_size,
            serde_json::to_vec(&buffered.update.fields)
                .expect("a JSON value map serializes")
                .len()
        );
        assert_eq!(
            buffered.update.fields.keys().collect::<Vec<_>>(),
            buffered.update.field_provenance.keys().collect::<Vec<_>>()
        );
        let expected_provenance_size = buffered
            .update
            .field_provenance
            .keys()
            .map(|field| {
                estimated_market_field_provenance_size(field)
                    .expect("a field key has a bounded serialized size")
            })
            .try_fold(MARKET_ROW_PROVENANCE_METADATA_OVERHEAD, |total, size| {
                total.checked_add(size)
            })
            .expect("provenance size estimate fits usize");
        assert_eq!(buffered.provenance_size, expected_provenance_size);
        assert!(buffered.provenance_size <= MAX_MERGED_MARKET_PROVENANCE_BYTES);
        buffered.update.fields.clone()
    }

    async fn snapshot_row(
        buffer: &MarketDataBuffer,
        key: &str,
    ) -> (
        BTreeMap<String, Value>,
        usize,
        usize,
        u64,
        u64,
        f64,
        BTreeMap<String, MarketDataFieldProvenance>,
        Instant,
    ) {
        let state = buffer.state.lock().await;
        let buffered = state.updates[0]
            .get(key)
            .expect("the synthetic row remains buffered");
        (
            buffered.update.fields.clone(),
            buffered.encoded_size,
            buffered.provenance_size,
            buffered.update.revision,
            buffered.update.coalesced_updates,
            buffered.update.source_timestamp,
            buffered.update.field_provenance.clone(),
            buffered.update.received_at,
        )
    }

    #[tokio::test]
    async fn encoded_size_matches_serde_for_escaped_and_mixed_values() {
        let (buffer, generation) = active_buffer().await;
        let mut fields = BTreeMap::new();
        fields.insert(
            "quote\" slash\\ newline\n nul\0".to_owned(),
            Value::String("quote \" slash \\ newline\n tab\t nul\0".to_owned()),
        );
        fields.insert("number".to_owned(), serde_json::json!(-12.375));
        fields.insert("null".to_owned(), Value::Null);
        fields.insert(
            "nested".to_owned(),
            serde_json::json!({
                "inner\"key": [1, null, true, {"deep": "escaped\tvalue"}],
                "unicode": "猫"
            }),
        );

        push_row(&buffer, generation, "SYNTH-MIXED", 1.0, fields)
            .await
            .expect("mixed JSON values fit the row bound");
        assert_cached_size_matches_json(&buffer, "SYNTH-MIXED").await;
    }

    #[tokio::test]
    async fn replacements_and_added_fields_keep_cached_size_exact() {
        let (buffer, generation) = active_buffer().await;
        let mut initial = BTreeMap::new();
        initial.insert("price".to_owned(), Value::String("0.9".to_owned()));
        initial.insert("quantity".to_owned(), Value::from(3));
        push_row(&buffer, generation, "SYNTH-REPLACE", 1.0, initial)
            .await
            .expect("initial row fits");
        assert_cached_size_matches_json(&buffer, "SYNTH-REPLACE").await;

        for (timestamp, name, value) in [
            (2.0, "price", Value::from(0.95)),
            (3.0, "quantity", Value::Null),
            (4.0, "new\"field", serde_json::json!({"deep": ["value", 8]})),
            (
                5.0,
                "price",
                Value::String("a much longer replacement".to_owned()),
            ),
        ] {
            let mut delta = BTreeMap::new();
            delta.insert(name.to_owned(), value);
            push_row(&buffer, generation, "SYNTH-REPLACE", timestamp, delta)
                .await
                .expect("replacement remains inside both caps");
            assert_cached_size_matches_json(&buffer, "SYNTH-REPLACE").await;
        }
    }

    #[tokio::test]
    async fn field_cap_accepts_128_fields_and_rejects_129_transactionally() {
        let (buffer, generation) = active_buffer().await;
        let fields = (0..MAX_MERGED_MARKET_FIELDS)
            .map(|index| {
                (
                    format!("field-{index:03}"),
                    Value::from(i64::try_from(index).expect("bounded field index fits i64")),
                )
            })
            .collect();
        push_row(&buffer, generation, "SYNTH-FIELDS", 1.0, fields)
            .await
            .expect("the exact field-count bound is accepted");
        assert_cached_size_matches_json(&buffer, "SYNTH-FIELDS").await;
        let before = snapshot_row(&buffer, "SYNTH-FIELDS").await;

        let mut delta = BTreeMap::new();
        delta.insert("field-overflow".to_owned(), Value::from(1));
        assert_eq!(
            push_row(&buffer, generation, "SYNTH-FIELDS", 2.0, delta).await,
            Err(SessionRunError::MarketDataCapacityExceeded)
        );
        assert_eq!(snapshot_row(&buffer, "SYNTH-FIELDS").await, before);
    }

    #[tokio::test]
    async fn byte_cap_accepts_exact_size_and_rejects_growth_transactionally() {
        let (buffer, generation) = active_buffer().await;
        let key = "payload";
        let fixed_size = 2 + serde_json::to_vec(key).expect("key serializes").len() + 1 + 2;
        let payload_len = MAX_MERGED_MARKET_ROW_BYTES - fixed_size;
        let mut fields = BTreeMap::new();
        fields.insert(key.to_owned(), Value::String("a".repeat(payload_len)));
        push_row(&buffer, generation, "SYNTH-BYTES", 1.0, fields)
            .await
            .expect("a serialized row exactly at the byte cap is accepted");
        assert_cached_size_matches_json(&buffer, "SYNTH-BYTES").await;
        let before = snapshot_row(&buffer, "SYNTH-BYTES").await;
        assert_eq!(before.1, MAX_MERGED_MARKET_ROW_BYTES);

        let mut delta = BTreeMap::new();
        delta.insert(key.to_owned(), Value::String("a".repeat(payload_len + 1)));
        assert_eq!(
            push_row(&buffer, generation, "SYNTH-BYTES", 2.0, delta).await,
            Err(SessionRunError::MarketDataCapacityExceeded)
        );
        assert_eq!(snapshot_row(&buffer, "SYNTH-BYTES").await, before);
    }

    #[tokio::test]
    async fn cached_size_matches_serde_after_generated_sparse_updates() {
        let (buffer, generation) = active_buffer().await;
        let names = ["0", "45", "escaped\"key", "nested", "nullable"];
        let mut initial = BTreeMap::new();
        for (index, name) in names.iter().enumerate() {
            initial.insert(
                name.to_string(),
                Value::from(i64::try_from(index).expect("bounded field index fits i64")),
            );
        }
        push_row(&buffer, generation, "SYNTH-GENERATED", 0.0, initial)
            .await
            .expect("generated initial row fits");

        for step in 0..512usize {
            let value = match step % 7 {
                0 => Value::from(i64::try_from(step).expect("generated step fits i64") * -17),
                1 => Value::String(format!("quote=\"{step}\" slash=\\ line=\n")),
                2 => Value::Null,
                3 => serde_json::json!({"nested": [step, null, {"enabled": true}]}),
                4 => Value::Bool(step % 2 == 0),
                5 => serde_json::json!(["array", step, {"escape\"key": "value\t"}]),
                _ => serde_json::json!(
                    f64::from(u32::try_from(step).expect("generated step fits u32")) / 13.0
                ),
            };
            let mut delta = BTreeMap::new();
            delta.insert(names[step % names.len()].to_owned(), value);
            push_row(
                &buffer,
                generation,
                "SYNTH-GENERATED",
                f64::from(u32::try_from(step + 1).expect("generated timestamp fits u32")),
                delta,
            )
            .await
            .expect("generated sparse update fits both caps");
            assert_cached_size_matches_json(&buffer, "SYNTH-GENERATED").await;
        }
    }

    #[tokio::test]
    // Keep the sparse merge/provenance timeline together as one executable
    // contract; splitting it would hide the ordering relationships.
    #[allow(clippy::too_many_lines)]
    async fn coalesced_fields_keep_independent_last_changed_provenance() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SYNTH-PROVENANCE",
            1.0,
            BTreeMap::from([("bid".to_owned(), Value::from(31))]),
        )
        .await
        .expect("initial bid fits");
        let first_bid_provenance = {
            let state = buffer.state.lock().await;
            state.updates[0]
                .get("SYNTH-PROVENANCE")
                .expect("the bid row remains queued")
                .update
                .field_provenance["bid"]
        };
        let before_duplicate = {
            let state = buffer.state.lock().await;
            let buffered = state.updates[0]
                .get("SYNTH-PROVENANCE")
                .expect("the bid row remains queued");
            (
                buffered.update.revision,
                buffered.update.coalesced_updates,
                buffered.update.received_at,
                buffered.update.field_provenance["bid"],
            )
        };
        push_row(
            &buffer,
            generation,
            "SYNTH-PROVENANCE",
            1.0,
            BTreeMap::from([("bid".to_owned(), serde_json::json!(31.0))]),
        )
        .await
        .expect("same JavaScript-number delta is ignored as a duplicate");
        {
            let state = buffer.state.lock().await;
            let update = &state.updates[0]
                .get("SYNTH-PROVENANCE")
                .expect("the original row remains queued")
                .update;
            assert_eq!(update.revision, before_duplicate.0);
            assert_eq!(update.coalesced_updates, before_duplicate.1);
            assert_eq!(update.received_at, before_duplicate.2);
            assert_eq!(update.field_provenance["bid"], before_duplicate.3);
        }

        push_row(
            &buffer,
            generation,
            "SYNTH-PROVENANCE",
            1.0,
            BTreeMap::from([("bid".to_owned(), Value::from(32))]),
        )
        .await
        .expect("changed content at the same source time is accepted");
        let changed_bid_provenance = {
            let state = buffer.state.lock().await;
            let update = &state.updates[0]
                .get("SYNTH-PROVENANCE")
                .expect("the changed row remains queued")
                .update;
            assert_eq!(update.revision, before_duplicate.0 + 1);
            assert_eq!(update.coalesced_updates, before_duplicate.1 + 1);
            assert_eq!(update.fields["bid"], Value::from(32));
            assert_eq!(update.field_provenance["bid"].source_timestamp, 1.0);
            assert_eq!(
                update.field_provenance["bid"].update_revision,
                update.revision
            );
            update.field_provenance["bid"]
        };

        push_row(
            &buffer,
            generation,
            "SYNTH-PROVENANCE",
            2.0,
            BTreeMap::from([
                ("greek".to_owned(), Value::from(-12)),
                ("mark".to_owned(), Value::from(700)),
            ]),
        )
        .await
        .expect("Greek and Mark fields merge sparsely");
        {
            let state = buffer.state.lock().await;
            let update = &state.updates[0]
                .get("SYNTH-PROVENANCE")
                .expect("the merged row remains queued")
                .update;
            assert_eq!(update.field_provenance["bid"], changed_bid_provenance);
            assert_eq!(update.field_provenance["bid"].source_timestamp, 1.0);
            assert_eq!(update.field_provenance["greek"].source_timestamp, 2.0);
            assert_eq!(update.field_provenance["mark"].source_timestamp, 2.0);
            assert_eq!(update.source_timestamp, 2.0);
            assert_eq!(
                update.received_at,
                update.field_provenance["mark"].received_at
            );
        }

        push_row(
            &buffer,
            generation,
            "SYNTH-PROVENANCE",
            3.0,
            BTreeMap::from([("bid".to_owned(), Value::from(33))]),
        )
        .await
        .expect("a later bid replaces its own provenance");
        let state = buffer.state.lock().await;
        let update = &state.updates[0]
            .get("SYNTH-PROVENANCE")
            .expect("the row remains queued")
            .update;
        assert_eq!(update.field_provenance["bid"].source_timestamp, 3.0);
        assert_ne!(update.field_provenance["bid"], changed_bid_provenance);
        assert_ne!(update.field_provenance["bid"], first_bid_provenance);
        assert_eq!(update.field_provenance["greek"].source_timestamp, 2.0);
    }

    #[tokio::test]
    async fn generation_reset_discards_all_old_field_provenance() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SYNTH-GENERATION",
            1.0,
            BTreeMap::from([("old-field".to_owned(), Value::from(1))]),
        )
        .await
        .expect("old-generation field fits");
        let next_generation = ConnectionGeneration::new(generation.value() + 1);
        buffer.reset_generation(Some(next_generation)).await;

        push_row(
            &buffer,
            generation,
            "SYNTH-GENERATION",
            2.0,
            BTreeMap::from([("stale-field".to_owned(), Value::from(2))]),
        )
        .await
        .expect("old-generation deltas are ignored");
        push_row(
            &buffer,
            next_generation,
            "SYNTH-GENERATION",
            3.0,
            BTreeMap::from([("new-field".to_owned(), Value::from(3))]),
        )
        .await
        .expect("new-generation field fits");

        let state = buffer.state.lock().await;
        let update = &state.updates[0]
            .get("SYNTH-GENERATION")
            .expect("the new-generation row is queued")
            .update;
        assert_eq!(update.generation, next_generation);
        assert_eq!(update.revision, 1);
        assert_eq!(update.fields.len(), 1);
        assert!(update.fields.contains_key("new-field"));
        assert_eq!(update.field_provenance.len(), 1);
        assert!(update.field_provenance.contains_key("new-field"));
        assert_eq!(update.field_provenance["new-field"].source_timestamp, 3.0);
    }

    #[tokio::test]
    // This scenario covers delivery, duplicate suppression, reconnect, and
    // stale-tail rejection in one deterministic state-machine trace.
    #[allow(clippy::too_many_lines)]
    async fn ordering_fences_survive_delivery_and_reset_rejects_old_generation_tail() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SYNTH-FENCE",
            10.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("initial row fits");
        let initial_notification = buffer.notify.notified();
        tokio::pin!(initial_notification);
        initial_notification.as_mut().enable();
        timeout(Duration::from_secs(1), initial_notification)
            .await
            .expect("initial row wakes a waiting consumer");
        {
            let mut state = buffer.state.lock().await;
            let delivered = state.pop_first().expect("consumer takes the row");
            assert_eq!(delivered.update.revision, 1);
            assert_eq!(state.len(), 0);
            assert_eq!(state.fence_len(), 1);
            assert_eq!(state.fences[0]["SYNTH-FENCE"].source_timestamp, 10.0);
        }

        let duplicate_notification = buffer.notify.notified();
        tokio::pin!(duplicate_notification);
        duplicate_notification.as_mut().enable();
        push_row(
            &buffer,
            generation,
            "SYNTH-FENCE",
            10.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("consumed exact repeat is ignored");
        assert!(
            timeout(Duration::from_millis(10), duplicate_notification)
                .await
                .is_err()
        );
        push_row(
            &buffer,
            generation,
            "SYNTH-FENCE",
            9.0,
            BTreeMap::from([("46".to_owned(), Value::from(1_800_000_000_000_i64))]),
        )
        .await
        .expect("consumed stale row is ignored");
        {
            let state = buffer.state.lock().await;
            assert_eq!(state.len(), 0);
            assert_eq!(state.fence_len(), 1);
            assert_eq!(state.fences[0]["SYNTH-FENCE"].revision, 1);
        }

        push_row(
            &buffer,
            generation,
            "SYNTH-FENCE",
            10.0,
            BTreeMap::from([("45".to_owned(), Value::from(601))]),
        )
        .await
        .expect("changed equal-time row remains admissible after delivery");
        {
            let mut state = buffer.state.lock().await;
            let changed = state.pop_first().expect("changed row is queued");
            assert_eq!(changed.update.revision, 2);
            assert_eq!(changed.update.fields["45"], Value::from(601));
        }

        let next_generation = ConnectionGeneration::new(generation.value() + 1);
        buffer.reset_generation(Some(next_generation)).await;
        {
            let state = buffer.state.lock().await;
            assert_eq!(state.fence_len(), 0);
            assert_eq!(state.fence_bytes, 0);
        }
        push_row(
            &buffer,
            generation,
            "SYNTH-FENCE",
            100.0,
            BTreeMap::from([("stale-generation".to_owned(), Value::from(1))]),
        )
        .await
        .expect("old-generation tail packet is ignored");
        assert_eq!(buffer.state.lock().await.len(), 0);
        push_row(
            &buffer,
            next_generation,
            "SYNTH-FENCE",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(700))]),
        )
        .await
        .expect("new generation accepts its own lower source timestamp");
        let mut state = buffer.state.lock().await;
        let new_generation_row = state.pop_first().expect("new generation row is queued");
        assert_eq!(new_generation_row.update.generation, next_generation);
        assert_eq!(new_generation_row.update.revision, 1);
    }

    #[tokio::test]
    async fn stale_oversized_delta_is_rejected_before_field_accounting() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SYNTH-STALE-OVERSIZED",
            10.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("initial row fits");
        let delivered = buffer
            .state
            .lock()
            .await
            .pop_first()
            .expect("consumer takes the initial row");
        assert_eq!(delivered.update.revision, 1);

        let oversized_fields = (0..=MAX_MERGED_MARKET_FIELDS)
            .map(|index| (format!("field-{index:03}"), Value::from(index)))
            .collect();
        push_row(
            &buffer,
            generation,
            "SYNTH-STALE-OVERSIZED",
            9.0,
            oversized_fields,
        )
        .await
        .expect("older source time is discarded before expensive field checks");

        let state = buffer.state.lock().await;
        assert_eq!(state.len(), 0);
        assert_eq!(state.fence_len(), 1);
        assert_eq!(
            state.fence_bytes,
            estimated_market_order_fence_size("SYNTH-STALE-OVERSIZED").unwrap()
        );
        let fence = state.fences[0]
            .get("SYNTH-STALE-OVERSIZED")
            .expect("consumed key retains its ordering fence");
        assert_eq!(fence.source_timestamp, 10.0);
        assert_eq!(fence.revision, 1);
    }

    #[tokio::test]
    async fn closing_market_data_buffer_clears_rows_and_ordering_fences() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SYNTH-CLOSE",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("row fits before close");
        {
            let state = buffer.state.lock().await;
            assert_eq!(state.len(), 1);
            assert_eq!(state.fence_len(), 1);
        }

        buffer.close().await;

        let state = buffer.state.lock().await;
        assert!(state.closed);
        assert_eq!(state.len(), 0);
        assert_eq!(state.fence_len(), 0);
        assert_eq!(state.fence_bytes, 0);
    }

    #[tokio::test]
    async fn deferred_owner_drop_cleanup_cannot_deliver_a_queued_quote() {
        let generation = ConnectionGeneration::new(31);
        let buffer = Arc::new(MarketDataBuffer::new(2));
        buffer.reset_generation(Some(generation)).await;
        push_row(
            &buffer,
            generation,
            "SYNTH-DEFERRED-CLOSE",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("row fits before close request");
        let mut receiver = MarketDataReceiver {
            buffer: Arc::clone(&buffer),
        };

        let state_guard = buffer.state.lock().await;
        buffer.close_without_waiting();
        assert!(
            buffer
                .close_requested
                .load(std::sync::atomic::Ordering::Acquire)
        );
        drop(state_guard);

        assert!(receiver.recv().await.is_none());
        let state = buffer.state.lock().await;
        assert!(state.closed);
        assert_eq!(state.len(), 0);
        assert_eq!(state.fence_len(), 0);
        assert_eq!(state.fence_bytes, 0);
    }

    #[tokio::test]
    // This scenario checks exact byte/key boundaries and confirms a rejected
    // addition never evicts the existing ordering fence.
    #[allow(clippy::too_many_lines)]
    async fn ordering_fence_key_and_entry_limits_fail_closed_without_eviction() {
        let generation = ConnectionGeneration::new(29);
        let per_entry_limit =
            MAX_MARKET_DATA_FENCE_KEY_BYTES + MARKET_DATA_ORDER_FENCE_ENTRY_OVERHEAD;
        let bytes_limited = MarketDataBuffer::with_limits(2, 2, per_entry_limit);
        bytes_limited.reset_generation(Some(generation)).await;
        let max_key = "K".repeat(MAX_MARKET_DATA_FENCE_KEY_BYTES);
        push_row(
            &bytes_limited,
            generation,
            &max_key,
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("maximum key length fits exactly in the fence byte estimate");
        let delivered = bytes_limited
            .state
            .lock()
            .await
            .pop_first()
            .expect("the row can be consumed while its fence remains");
        assert_eq!(delivered.update.key.len(), MAX_MARKET_DATA_FENCE_KEY_BYTES);

        let oversized_key = "X".repeat(MAX_MARKET_DATA_FENCE_KEY_BYTES + 1);
        let key_error = push_row(
            &bytes_limited,
            generation,
            &oversized_key,
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect_err("oversized fence key is rejected before insertion");
        assert_eq!(key_error, SessionRunError::MarketDataCapacityExceeded);
        assert!(!key_error.to_string().contains(&oversized_key));

        let byte_error = push_row(
            &bytes_limited,
            generation,
            "SECOND",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect_err("fence byte cap is explicit after a delivered row");
        assert_eq!(byte_error, SessionRunError::MarketDataCapacityExceeded);
        {
            let state = bytes_limited.state.lock().await;
            assert_eq!(state.fence_len(), 1);
            assert_eq!(state.fence_bytes, per_entry_limit);
            assert_eq!(state.len(), 0);
        }

        let entry_limited = MarketDataBuffer::with_limits(2, 1, MAX_MARKET_DATA_FENCE_BYTES);
        entry_limited.reset_generation(Some(generation)).await;
        push_row(
            &entry_limited,
            generation,
            "FIRST",
            2.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("first key consumes the only fence entry");
        let delivered = entry_limited
            .state
            .lock()
            .await
            .pop_first()
            .expect("the first row can be consumed");
        assert_eq!(delivered.update.revision, 1);
        let entry_error = push_row(
            &entry_limited,
            generation,
            "SECOND",
            2.0,
            BTreeMap::from([("45".to_owned(), Value::from(601))]),
        )
        .await
        .expect_err("entry cap does not silently evict delivered keys");
        assert_eq!(entry_error, SessionRunError::MarketDataCapacityExceeded);
        let other_service_error = entry_limited
            .push_delta(
                StreamerService::LevelOneOptions,
                generation,
                "FIRST".to_owned(),
                2.0,
                BTreeMap::from([("2".to_owned(), Value::from(0.5))]),
            )
            .await
            .expect_err("fence count is shared across market-data services");
        assert_eq!(
            other_service_error,
            SessionRunError::MarketDataCapacityExceeded
        );
        push_row(
            &entry_limited,
            generation,
            "FIRST",
            1.0,
            BTreeMap::from([("46".to_owned(), Value::from(1))]),
        )
        .await
        .expect("existing fence continues to reject stale timestamps");
        push_row(
            &entry_limited,
            generation,
            "FIRST",
            2.0,
            BTreeMap::from([("45".to_owned(), Value::from(602))]),
        )
        .await
        .expect("existing key can advance without allocating another fence");
        let updated = entry_limited
            .state
            .lock()
            .await
            .pop_first()
            .expect("updated existing key is queued");
        assert_eq!(updated.update.revision, 2);
    }

    #[test]
    fn sparse_row_fingerprint_matches_node_stable_value_equivalence() {
        let first: BTreeMap<String, Value> =
            serde_json::from_str(r#"{"quote":31,"nested":{"z":-0.0,"a":[true,"x"]}}"#)
                .expect("first sparse row parses");
        let equivalent: BTreeMap<String, Value> =
            serde_json::from_str(r#"{"nested":{"a":[true,"x"],"z":0},"quote":31.0}"#)
                .expect("equivalent sparse row parses");
        let changed: BTreeMap<String, Value> =
            serde_json::from_str(r#"{"nested":{"a":[true,"y"],"z":0},"quote":31}"#)
                .expect("changed sparse row parses");
        assert_eq!(
            streamer_delta_fingerprint(&first).expect("fingerprint is valid"),
            streamer_delta_fingerprint(&equivalent).expect("fingerprint is valid")
        );
        assert_ne!(
            streamer_delta_fingerprint(&first).expect("fingerprint is valid"),
            streamer_delta_fingerprint(&changed).expect("fingerprint is valid")
        );
    }

    #[tokio::test]
    async fn debug_redacts_market_data_key_field_names_and_values() {
        let (buffer, generation) = active_buffer().await;
        push_row(
            &buffer,
            generation,
            "SECRET_CONTRACT_KEY",
            1.0,
            BTreeMap::from([(
                "SECRET_FIELD_NAME".to_owned(),
                Value::String("SECRET_ROW_VALUE".to_owned()),
            )]),
        )
        .await
        .expect("synthetic sensitive-looking row fits");
        let state = buffer.state.lock().await;
        let update = &state.updates[0]
            .get("SECRET_CONTRACT_KEY")
            .expect("the row remains queued")
            .update;
        let debug = format!("{update:?}");
        assert!(!debug.contains("SECRET_CONTRACT_KEY"));
        assert!(!debug.contains("SECRET_FIELD_NAME"));
        assert!(!debug.contains("SECRET_ROW_VALUE"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn equal_keys_on_separate_services_coalesce_and_dequeue_independently() {
        let buffer = MarketDataBuffer::new(3);
        let generation = ConnectionGeneration::new(17);
        buffer.reset_generation(Some(generation)).await;

        let mut options = BTreeMap::new();
        options.insert("2".to_owned(), Value::from(0.25));
        buffer
            .push_delta(
                StreamerService::LevelOneOptions,
                generation,
                "SYNTH-SHARED".to_owned(),
                1.0,
                options,
            )
            .await
            .expect("options row arrives first and occupies its own service map");

        let mut equities = BTreeMap::new();
        equities.insert("45".to_owned(), Value::from(600));
        push_row(&buffer, generation, "SYNTH-SHARED", 1.0, equities)
            .await
            .expect("same symbol on equities remains a separate row");

        push_row(
            &buffer,
            generation,
            "SYNTH-AAA",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(601))]),
        )
        .await
        .expect("second equity key is retained in sorted key order");

        let mut equity_delta = BTreeMap::new();
        equity_delta.insert("46".to_owned(), Value::from(1_800_000_000_000_i64));
        push_row(&buffer, generation, "SYNTH-SHARED", 2.0, equity_delta)
            .await
            .expect("equity sparse delta merges only into the equity row");

        let mut state = buffer.state.lock().await;
        let first_equity = state
            .pop_first()
            .expect("equities sort before options even though options arrived first");
        assert_eq!(
            first_equity.update.service,
            StreamerService::LevelOneEquities
        );
        assert_eq!(first_equity.update.key, "SYNTH-AAA");

        let equity = state
            .pop_first()
            .expect("the next equity row follows key order before options");
        assert_eq!(equity.update.service, StreamerService::LevelOneEquities);
        assert_eq!(equity.update.key, "SYNTH-SHARED");
        assert_eq!(equity.update.revision, 2);
        assert_eq!(equity.update.fields.get("45"), Some(&Value::from(600)));
        assert_eq!(
            equity.update.fields.get("46"),
            Some(&Value::from(1_800_000_000_000_i64))
        );

        let options = state
            .pop_first()
            .expect("options row remains independently queued");
        assert_eq!(options.update.service, StreamerService::LevelOneOptions);
        assert_eq!(options.update.revision, 1);
        assert_eq!(options.update.fields.get("2"), Some(&Value::from(0.25)));
        assert!(state.pop_first().is_none());
    }

    #[tokio::test]
    async fn market_data_capacity_remains_shared_between_services() {
        let buffer = MarketDataBuffer::new(1);
        let generation = ConnectionGeneration::new(19);
        buffer.reset_generation(Some(generation)).await;
        push_row(
            &buffer,
            generation,
            "SYNTH-EQ",
            1.0,
            BTreeMap::from([("45".to_owned(), Value::from(600))]),
        )
        .await
        .expect("the first service row fills the single shared slot");

        assert_eq!(
            buffer
                .push_delta(
                    StreamerService::LevelOneOptions,
                    generation,
                    "SYNTH-OPT".to_owned(),
                    1.0,
                    BTreeMap::from([("2".to_owned(), Value::from(0.25))]),
                )
                .await,
            Err(SessionRunError::MarketDataCapacityExceeded)
        );
    }
}
