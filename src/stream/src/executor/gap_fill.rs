// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::Op;
use risingwave_common::gap_fill::{
    FillStrategy, apply_interpolation_step, calculate_interpolation_step,
};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{CheckedAdd, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use tracing::warn;

use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::executor::prelude::*;

pub struct GapFillExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    pub schema: Schema,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: HashMap<usize, FillStrategy>,
    pub gap_interval: NonStrictExpression,
    pub state_table: StateTable<S>,
    pub partition_by_indices: Vec<usize>,
    pub upstream_stream_key: Vec<usize>,
    pub pointer_key_indices: Vec<usize>,
}

/// Serialized key for ordering rows within a partition: (time_col, stream_key_cols...).
/// Uses memcmp-serialized format for correct BTreeMap ordering.
pub type IntraPartitionKey = Vec<u8>;

/// Serialized partition key for identifying which partition a row belongs to.
pub type PartitionKey = Vec<u8>;

const GAPFILL_CACHE_MAX_PARTITIONS: usize = 64;
const GAPFILL_CACHE_PER_PARTITION_CAPACITY: usize = 512;

/// State row layout:
///   [output_cols..., prev_sk_0..N, next_sk_0..N]
///   where prev_sk/next_sk store the intra-partition row identity of the neighbor:
///   (time column, upstream stream key columns excluding partition/time).
///
/// Only original (anchor) rows are persisted. Filled rows are computed on the fly.
pub struct ManagedGapFillState<S: StateStore> {
    state_table: StateTable<S>,
    /// Number of columns from the upstream output schema.
    output_col_count: usize,
    partition_by_indices: Vec<usize>,
    pointer_key_indices: Vec<usize>,
    /// Serde for partition prefix key.
    partition_key_serde: OrderedRowSerde,
    /// Serde for intra-partition ordering key: (time_col, stream_key_cols...).
    /// Uses time-first ordering for correct gap fill range scans.
    intra_key_serde: OrderedRowSerde,
}

impl<S: StateStore> ManagedGapFillState<S> {
    pub fn new(
        state_table: StateTable<S>,
        schema: &Schema,
        partition_by_indices: Vec<usize>,
        pointer_key_indices: Vec<usize>,
    ) -> Self {
        use risingwave_common::util::sort_util::OrderType;

        let output_col_count = schema.len();

        // Build serde for partition prefix
        let partition_types: Vec<_> = partition_by_indices
            .iter()
            .map(|&i| schema[i].data_type())
            .collect();
        let partition_orders = vec![OrderType::ascending(); partition_types.len()];
        let partition_key_serde = OrderedRowSerde::new(partition_types, partition_orders);

        // Build serde for intra-partition ordering key.
        // This must match the PK columns after the partition prefix.
        let intra_types: Vec<_> = pointer_key_indices
            .iter()
            .map(|&i| schema[i].data_type())
            .collect();
        let intra_orders = vec![OrderType::ascending(); intra_types.len()];
        let intra_key_serde = OrderedRowSerde::new(intra_types, intra_orders);

        Self {
            state_table,
            output_col_count,
            partition_by_indices,
            pointer_key_indices,
            partition_key_serde,
            intra_key_serde,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    pub fn insert(&mut self, value: impl Row) {
        self.state_table.insert(value);
    }

    pub fn delete(&mut self, value: impl Row) {
        self.state_table.delete(value);
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.state_table.commit(epoch).await
    }

    /// Index of prev pointer start in a state row.
    fn prev_pointer_idx(&self) -> usize {
        self.output_col_count
    }

    /// Index of next pointer start in a state row.
    fn next_pointer_idx(&self) -> usize {
        self.output_col_count + self.pointer_key_indices.len()
    }

    /// Extract partition key from an upstream row.
    fn extract_partition_key(&self, row: &OwnedRow) -> PartitionKey {
        row.project(&self.partition_by_indices)
            .memcmp_serialize(&self.partition_key_serde)
    }

    /// Extract partition key as OwnedRow from an upstream row.
    fn extract_partition_row(&self, row: &OwnedRow) -> OwnedRow {
        let data: Vec<Datum> = self
            .partition_by_indices
            .iter()
            .map(|&i| row.datum_at(i).to_owned_datum())
            .collect();
        OwnedRow::new(data)
    }

    /// Extract the intra-partition ordering key from an upstream row.
    /// Matches the PK columns after partition prefix: (time, remaining_sk with dedup).
    fn extract_intra_key(&self, row: &OwnedRow) -> IntraPartitionKey {
        row.project(&self.pointer_key_indices)
            .memcmp_serialize(&self.intra_key_serde)
    }

    /// Build the state row from an upstream row with prev/next pointer datums.
    /// State row = [output_cols..., prev_sk_0..N, next_sk_0..N]
    fn build_state_row(
        &self,
        row: &OwnedRow,
        prev_pointer: &[Datum],
        next_pointer: &[Datum],
    ) -> OwnedRow {
        let mut data = row.as_inner().to_vec();
        data.extend_from_slice(prev_pointer);
        data.extend_from_slice(next_pointer);
        OwnedRow::new(data)
    }

    /// Extract only the output columns from a state row.
    fn state_row_to_output_row(&self, state_row: &OwnedRow) -> OwnedRow {
        let data: Vec<Datum> = (0..self.output_col_count)
            .map(|i| state_row.datum_at(i).to_owned_datum())
            .collect();
        OwnedRow::new(data)
    }

    /// Build a null pointer (all None datums) for prev or next.
    fn null_pointer(&self) -> Vec<Datum> {
        vec![None; self.pointer_key_indices.len()]
    }

    /// Build pointer datums from an upstream row — the intra-partition row identity.
    fn row_to_pointer(&self, row: &OwnedRow) -> Vec<Datum> {
        self.pointer_key_indices
            .iter()
            .map(|&sk| row.datum_at(sk).to_owned_datum())
            .collect()
    }

    /// Check if a pointer is null (first datum is None = no neighbor).
    fn is_null_pointer(pointer: &[Datum]) -> bool {
        pointer.first().is_none_or(|d| d.is_none())
    }

    /// Reconstruct a PK row from a partition prefix and pointer datums.
    /// The pointer contains the full intra-partition row identity:
    /// (time column, upstream stream key columns excluding partition/time).
    /// Returns None if the pointer is null.
    fn pointer_to_pk_row(&self, partition_row: &OwnedRow, pointer: &[Datum]) -> Option<OwnedRow> {
        if Self::is_null_pointer(pointer) {
            return None;
        }
        let mut pk_data = partition_row.as_inner().to_vec();
        pk_data.extend_from_slice(pointer);
        Some(OwnedRow::new(pk_data))
    }

    /// Point-get a row from state table by its full PK prefix (partition + time + sk).
    async fn get_row(&self, pk_row: &OwnedRow) -> StreamExecutorResult<Option<OwnedRow>> {
        let result = self.state_table.get_row(pk_row).await?;
        Ok(result)
    }

    /// Find the previous neighbor within the same partition by scanning backward.
    /// Uses the partition as prefix and scans rows with intra-key < target intra-key.
    async fn find_prev_in_partition(
        &self,
        partition_row: &OwnedRow,
        target_intra_key: &IntraPartitionKey,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        let target_intra_row = self.intra_key_serde.deserialize(target_intra_key)?;
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
            &(Bound::Unbounded, Bound::Excluded(target_intra_row));

        let iter = self
            .state_table
            .rev_iter_with_prefix(partition_row, sub_range, PrefetchOptions::default())
            .await?;
        pin_mut!(iter);

        if let Some(item) = iter.next().await {
            let state_row = item?.into_owned_row();
            Ok(Some(state_row))
        } else {
            Ok(None)
        }
    }

    /// Find the next neighbor within the same partition by scanning forward.
    async fn find_next_in_partition(
        &self,
        partition_row: &OwnedRow,
        target_intra_key: &IntraPartitionKey,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        let target_intra_row = self.intra_key_serde.deserialize(target_intra_key)?;
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
            &(Bound::Excluded(target_intra_row), Bound::Unbounded);

        let iter = self
            .state_table
            .iter_with_prefix(partition_row, sub_range, PrefetchOptions::default())
            .await?;
        pin_mut!(iter);

        if let Some(item) = iter.next().await {
            let state_row = item?.into_owned_row();
            Ok(Some(state_row))
        } else {
            Ok(None)
        }
    }

    /// Update a row's prev pointer in the state table.
    /// Deletes the old state row and inserts the updated one.
    fn update_prev_pointer(
        &mut self,
        old_state_row: &OwnedRow,
        new_prev_pointer: &[Datum],
    ) -> OwnedRow {
        self.state_table.delete(old_state_row);
        let mut new_data = old_state_row.as_inner().to_vec();
        let start = self.prev_pointer_idx();
        let count = self.pointer_key_indices.len();
        new_data[start..start + count].clone_from_slice(new_prev_pointer);
        let new_state_row = OwnedRow::new(new_data);
        self.state_table.insert(&new_state_row);
        new_state_row
    }

    /// Update a row's next pointer in the state table.
    fn update_next_pointer(
        &mut self,
        old_state_row: &OwnedRow,
        new_next_pointer: &[Datum],
    ) -> OwnedRow {
        self.state_table.delete(old_state_row);
        let mut new_data = old_state_row.as_inner().to_vec();
        let start = self.next_pointer_idx();
        let count = self.pointer_key_indices.len();
        new_data[start..start + count].clone_from_slice(new_next_pointer);
        let new_state_row = OwnedRow::new(new_data);
        self.state_table.insert(&new_state_row);
        new_state_row
    }
}

/// Per-partition cache: BTreeMap ordered by intra-partition key → output OwnedRow.
/// Only stores original anchor rows (no filled rows).
struct PartitionCache {
    rows: BTreeMap<IntraPartitionKey, OwnedRow>,
}

impl PartitionCache {
    fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
        }
    }

    fn insert(&mut self, key: IntraPartitionKey, row: OwnedRow) {
        self.rows.insert(key, row);
    }

    fn remove(&mut self, key: &IntraPartitionKey) {
        self.rows.remove(key);
    }

    fn find_prev(&self, key: &IntraPartitionKey) -> Option<(&IntraPartitionKey, &OwnedRow)> {
        self.rows.range::<IntraPartitionKey, _>(..key).next_back()
    }

    fn find_next(&self, key: &IntraPartitionKey) -> Option<(&IntraPartitionKey, &OwnedRow)> {
        self.rows
            .range::<IntraPartitionKey, _>((Bound::Excluded(key), Bound::Unbounded))
            .next()
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Manages per-partition caches with LRU eviction at partition granularity.
pub struct GapFillCacheManager {
    caches: HashMap<PartitionKey, PartitionCache>,
    lru_order: VecDeque<PartitionKey>,
    max_partitions: usize,
    per_partition_capacity: usize,
}

impl GapFillCacheManager {
    pub fn new(max_partitions: usize, per_partition_capacity: usize) -> Self {
        Self {
            caches: HashMap::new(),
            lru_order: VecDeque::new(),
            max_partitions,
            per_partition_capacity,
        }
    }

    /// Touch a partition to mark it as recently used.
    fn touch_partition(&mut self, partition_key: &PartitionKey) {
        if let Some(pos) = self.lru_order.iter().position(|k| k == partition_key) {
            self.lru_order.remove(pos);
        }
        self.lru_order.push_back(partition_key.clone());
    }

    /// Evict least-recently-used partitions if over capacity.
    fn evict_if_needed(&mut self) {
        while self.caches.len() > self.max_partitions {
            if let Some(oldest) = self.lru_order.pop_front() {
                self.caches.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// Get or create the cache for a partition.
    fn get_or_create_partition(&mut self, partition_key: &PartitionKey) -> &mut PartitionCache {
        self.touch_partition(partition_key);
        if !self.caches.contains_key(partition_key) {
            self.caches
                .insert(partition_key.clone(), PartitionCache::new());
            self.evict_if_needed();
        }
        self.caches.get_mut(partition_key).unwrap()
    }

    /// Insert a row into the partition cache.
    pub fn insert(
        &mut self,
        partition_key: &PartitionKey,
        intra_key: IntraPartitionKey,
        row: OwnedRow,
    ) {
        let cap = self.per_partition_capacity;
        let cache = self.get_or_create_partition(partition_key);
        cache.insert(intra_key, row);
        while cache.len() > cap {
            cache.rows.pop_first();
        }
    }

    /// Remove a row from the partition cache.
    pub fn remove(&mut self, partition_key: &PartitionKey, intra_key: &IntraPartitionKey) {
        if let Some(cache) = self.caches.get_mut(partition_key) {
            cache.remove(intra_key);
        }
    }

    /// Find previous row in the same partition from cache.
    pub fn find_prev(
        &mut self,
        partition_key: &PartitionKey,
        intra_key: &IntraPartitionKey,
    ) -> Option<OwnedRow> {
        self.touch_partition(partition_key);
        self.caches
            .get(partition_key)
            .and_then(|cache| cache.find_prev(intra_key))
            .map(|(_, row)| row.clone())
    }

    /// Find next row in the same partition from cache.
    pub fn find_next(
        &mut self,
        partition_key: &PartitionKey,
        intra_key: &IntraPartitionKey,
    ) -> Option<OwnedRow> {
        self.touch_partition(partition_key);
        self.caches
            .get(partition_key)
            .and_then(|cache| cache.find_next(intra_key))
            .map(|(_, row)| row.clone())
    }
}

pub struct GapFillExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: HashMap<usize, FillStrategy>,
    gap_interval: NonStrictExpression,

    // State management
    managed_state: ManagedGapFillState<S>,
    cache_manager: GapFillCacheManager,

    // Metrics
    metrics: GapFillMetrics,
}

pub struct GapFillMetrics {
    pub gap_fill_generated_rows_count: LabelGuardedIntCounter,
}

impl<S: StateStore> GapFillExecutor<S> {
    pub fn new(args: GapFillExecutorArgs<S>) -> Self {
        let managed_state = ManagedGapFillState::new(
            args.state_table,
            &args.schema,
            args.partition_by_indices,
            args.pointer_key_indices,
        );
        let cache_manager = GapFillCacheManager::new(
            GAPFILL_CACHE_MAX_PARTITIONS,
            GAPFILL_CACHE_PER_PARTITION_CAPACITY,
        );

        let metrics = args.ctx.streaming_metrics.clone();
        let actor_id = args.ctx.id.to_string();
        let fragment_id = args.ctx.fragment_id.to_string();
        let gap_fill_metrics = GapFillMetrics {
            gap_fill_generated_rows_count: metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        };

        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            time_column_index: args.time_column_index,
            fill_columns: args.fill_columns,
            gap_interval: args.gap_interval,
            managed_state,
            cache_manager,
            metrics: gap_fill_metrics,
        }
    }

    /// Generates interpolated rows between two time points (`prev_row` and `curr_row`) using a static interval.
    ///
    /// # Parameters
    /// - `prev_row`: Reference to the previous row (start of the gap).
    /// - `curr_row`: Reference to the current row (end of the gap).
    /// - `interval`: The interval to use for generating each filled row (typically a time interval).
    /// - `time_column_index`: The index of the time column in the row, used to increment time values.
    /// - `fill_columns`: A `HashMap` mapping column indices to their respective `FillStrategy`.
    /// - `metrics`: Metrics for tracking the number of generated rows.
    ///
    /// # Fill Strategy Application
    /// For each filled row, the function applies the specified `FillStrategy` for each column:
    /// - `FillStrategy::Previous`: Uses the value from the previous row.
    /// - `FillStrategy::Linear`: Interpolates linearly between the previous and current row values.
    /// - Other strategies may be supported as defined in `FillStrategy`.
    ///
    /// Returns a vector of `OwnedRow` representing the filled rows between `prev_row` and `curr_row`.
    fn generate_filled_rows_between_static(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        interval: &risingwave_common::types::Interval,
        time_column_index: usize,
        partition_by_indices: &[usize],
        fill_columns: &HashMap<usize, FillStrategy>,
        metrics: &GapFillMetrics,
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        let mut filled_rows = Vec::new();

        let (Some(prev_time_scalar), Some(curr_time_scalar)) = (
            prev_row.datum_at(time_column_index),
            curr_row.datum_at(time_column_index),
        ) else {
            return Ok(filled_rows);
        };

        let prev_time = match prev_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", prev_time_scalar);
                return Ok(filled_rows);
            }
        };

        let curr_time = match curr_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", curr_time_scalar);
                return Ok(filled_rows);
            }
        };

        if prev_time >= curr_time {
            return Ok(filled_rows);
        }

        // Calculate the number of rows to be generated and validate
        let mut fill_time = match prev_time.checked_add(*interval) {
            Some(t) => t,
            None => {
                // If the interval is so large that adding it to prev_time causes overflow,
                // it means we shouldn't do gap fill at all.
                warn!(
                    "Gap fill interval is too large, causing timestamp overflow. \
                     No gap filling will be performed between {:?} and {:?}.",
                    prev_time, curr_time
                );
                return Ok(filled_rows);
            }
        };

        // Check if fill_time is already >= curr_time, which means no gap to fill
        if fill_time >= curr_time {
            return Ok(filled_rows);
        }

        // Count the number of rows to generate
        let mut row_count = 0;
        let mut temp_time = fill_time;
        while temp_time < curr_time {
            row_count += 1;
            temp_time = match temp_time.checked_add(*interval) {
                Some(t) => t,
                None => break,
            };
        }

        // Pre-compute interpolation steps for each column that requires interpolation
        let mut interpolation_steps: Vec<Option<ScalarImpl>> = Vec::new();
        let mut interpolation_states: Vec<Datum> = Vec::new();

        for i in 0..prev_row.len() {
            if let Some(strategy) = fill_columns.get(&i) {
                if matches!(strategy, FillStrategy::Interpolate) {
                    let step = calculate_interpolation_step(
                        prev_row.datum_at(i),
                        curr_row.datum_at(i),
                        row_count + 1,
                    );
                    interpolation_steps.push(step.clone());
                    interpolation_states.push(prev_row.datum_at(i).to_owned_datum());
                } else {
                    interpolation_steps.push(None);
                    interpolation_states.push(None);
                }
            } else {
                interpolation_steps.push(None);
                interpolation_states.push(None);
            }
        }

        // Generate filled rows, applying the appropriate strategy for each column
        while fill_time < curr_time {
            let mut new_row_data = Vec::with_capacity(prev_row.len());

            for col_idx in 0..prev_row.len() {
                let datum = if col_idx == time_column_index {
                    // Time column: use the incremented timestamp
                    let fill_time_scalar = match prev_time_scalar {
                        ScalarRefImpl::Timestamp(_) => ScalarImpl::Timestamp(fill_time),
                        ScalarRefImpl::Timestamptz(_) => {
                            let micros = fill_time.0.and_utc().timestamp_micros();
                            ScalarImpl::Timestamptz(
                                risingwave_common::types::Timestamptz::from_micros(micros),
                            )
                        }
                        _ => unreachable!("Time column should be Timestamp or Timestamptz"),
                    };
                    Some(fill_time_scalar)
                } else if partition_by_indices.contains(&col_idx) {
                    // Gap-filled rows must stay in the same partition as the surrounding anchors.
                    prev_row.datum_at(col_idx).to_owned_datum()
                } else if let Some(strategy) = fill_columns.get(&col_idx) {
                    // Apply the fill strategy for this column
                    match strategy {
                        FillStrategy::Locf => prev_row.datum_at(col_idx).to_owned_datum(),
                        FillStrategy::Null => None,
                        FillStrategy::Interpolate => {
                            // Apply interpolation step and update cumulative value
                            if let Some(step) = &interpolation_steps[col_idx] {
                                apply_interpolation_step(&mut interpolation_states[col_idx], step);
                                interpolation_states[col_idx].clone()
                            } else {
                                // If interpolation step is None, fill with NULL
                                None
                            }
                        }
                    }
                } else {
                    // No strategy specified, default to NULL
                    None
                };
                new_row_data.push(datum);
            }

            filled_rows.push(OwnedRow::new(new_row_data));

            fill_time = match fill_time.checked_add(*interval) {
                Some(t) => t,
                None => {
                    // Time overflow during iteration, stop filling
                    warn!(
                        "Gap fill stopped due to timestamp overflow after generating {} rows.",
                        filled_rows.len()
                    );
                    break;
                }
            };
        }

        // Update metrics with the number of generated rows
        metrics
            .gap_fill_generated_rows_count
            .inc_by(filled_rows.len() as u64);

        Ok(filled_rows)
    }
}

impl<S: StateStore> Execute for GapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> GapFillExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            mut managed_state,
            mut cache_manager,
            schema,
            chunk_size,
            time_column_index,
            fill_columns,
            gap_interval,
            ctx,
            input,
            metrics,
        } = *self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        managed_state.init_epoch(first_epoch).await?;

        // Calculate and validate gap interval once at initialization
        let dummy_row = OwnedRow::new(vec![]);
        let interval_datum = gap_interval.eval_row_infallible(&dummy_row).await;
        let interval = interval_datum
            .ok_or_else(|| anyhow::anyhow!("Gap interval expression returned null"))?
            .into_interval();

        if interval.months() == 0 && interval.days() == 0 && interval.usecs() == 0 {
            Err(anyhow::anyhow!("Gap interval cannot be zero"))?;
        }

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact_vis();
                    let mut chunk_builder =
                        StreamChunkBuilder::new(chunk_size, schema.data_types());

                    for (op, row_ref) in chunk.rows() {
                        let row = row_ref.to_owned_row();
                        let partition_key = managed_state.extract_partition_key(&row);
                        let partition_row = managed_state.extract_partition_row(&row);
                        let intra_key = managed_state.extract_intra_key(&row);

                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                // 1. Find prev and next neighbors in the same partition.
                                //    Check cache first (has uncommitted inserts), then state table.
                                let prev_output = cache_manager
                                    .find_prev(&partition_key, &intra_key)
                                    .or_else(|| None); // Cache lookup
                                let prev_output = if prev_output.is_some() {
                                    prev_output
                                } else {
                                    // Cache miss, scan state table.
                                    managed_state
                                        .find_prev_in_partition(&partition_row, &intra_key)
                                        .await?
                                        .map(|sr| managed_state.state_row_to_output_row(&sr))
                                };

                                let next_output =
                                    cache_manager.find_next(&partition_key, &intra_key);
                                let next_output = if next_output.is_some() {
                                    next_output
                                } else {
                                    managed_state
                                        .find_next_in_partition(&partition_row, &intra_key)
                                        .await?
                                        .map(|sr| managed_state.state_row_to_output_row(&sr))
                                };

                                // 2. If both neighbors existed, delete old filled rows between them.
                                if let (Some(prev_out_ref), Some(next_out_ref)) =
                                    (&prev_output, &next_output)
                                {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        prev_out_ref,
                                        next_out_ref,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 3. Build pointer datums for the new row.
                                let new_row_pointer = managed_state.row_to_pointer(&row);
                                let prev_pointer = prev_output
                                    .as_ref()
                                    .map(|r| managed_state.row_to_pointer(r));
                                let next_pointer = next_output
                                    .as_ref()
                                    .map(|r| managed_state.row_to_pointer(r));

                                // 4. Update prev's next pointer to point to new row.
                                //    We need to get the state row from the state table for pointer update.
                                if let Some(prev_ptr) = &prev_pointer {
                                    let pk =
                                        managed_state.pointer_to_pk_row(&partition_row, prev_ptr);
                                    if let Some(pk) = pk {
                                        if let Some(prev_sr) = managed_state.get_row(&pk).await? {
                                            managed_state
                                                .update_next_pointer(&prev_sr, &new_row_pointer);
                                        }
                                    }
                                }

                                // 5. Update next's prev pointer to point to new row.
                                if let Some(next_ptr) = &next_pointer {
                                    let pk =
                                        managed_state.pointer_to_pk_row(&partition_row, next_ptr);
                                    if let Some(pk) = pk {
                                        if let Some(next_sr) = managed_state.get_row(&pk).await? {
                                            managed_state
                                                .update_prev_pointer(&next_sr, &new_row_pointer);
                                        }
                                    }
                                }

                                // 6. Insert the new row into state with prev/next pointers.
                                let null_ptr = managed_state.null_pointer();
                                let state_row = managed_state.build_state_row(
                                    &row,
                                    prev_pointer.as_deref().unwrap_or(&null_ptr),
                                    next_pointer.as_deref().unwrap_or(&null_ptr),
                                );
                                managed_state.insert(&state_row);

                                // 7. Update cache.
                                cache_manager.insert(
                                    &partition_key,
                                    intra_key.clone(),
                                    row.clone(),
                                );

                                // 8. Emit the inserted row.
                                if let Some(chunk) = chunk_builder.append_row(op, &row) {
                                    yield Message::Chunk(chunk);
                                }

                                // 9. Emit new filled rows between prev and new row.
                                if let Some(prev_out) = &prev_output {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        &row,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 10. Emit new filled rows between new row and next.
                                if let Some(next_out) = &next_output {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        &row,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }
                            }
                            Op::Delete | Op::UpdateDelete => {
                                // 1. Find prev/next neighbors using cache first, then state table.
                                let prev_output =
                                    cache_manager.find_prev(&partition_key, &intra_key);
                                let prev_output = if prev_output.is_some() {
                                    prev_output
                                } else {
                                    managed_state
                                        .find_prev_in_partition(&partition_row, &intra_key)
                                        .await?
                                        .map(|sr| managed_state.state_row_to_output_row(&sr))
                                };

                                let next_output =
                                    cache_manager.find_next(&partition_key, &intra_key);
                                let next_output = if next_output.is_some() {
                                    next_output
                                } else {
                                    managed_state
                                        .find_next_in_partition(&partition_row, &intra_key)
                                        .await?
                                        .map(|sr| managed_state.state_row_to_output_row(&sr))
                                };

                                // 2. Delete old filled rows on both sides.
                                if let Some(prev_out) = &prev_output {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        &row,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                if let Some(next_out) = &next_output {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        &row,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 3. Find the actual state row to delete.
                                //    Build the full PK and point-get from state table.
                                let row_pointer = managed_state.row_to_pointer(&row);
                                let pk_row =
                                    managed_state.pointer_to_pk_row(&partition_row, &row_pointer);
                                let cur_state_row = if let Some(ref pk) = pk_row {
                                    managed_state.get_row(pk).await?
                                } else {
                                    None
                                };

                                if let Some(cur_sr) = &cur_state_row {
                                    // 4. Delete the row from state.
                                    managed_state.delete(cur_sr);
                                }
                                cache_manager.remove(&partition_key, &intra_key);

                                // 5. Emit the delete for the original row.
                                if let Some(chunk) = chunk_builder.append_row(op, &row) {
                                    yield Message::Chunk(chunk);
                                }

                                // 6. Update prev's next pointer to point to next.
                                let null_ptr = managed_state.null_pointer();
                                let prev_pointer = prev_output
                                    .as_ref()
                                    .map(|r| managed_state.row_to_pointer(r));
                                let next_pointer = next_output
                                    .as_ref()
                                    .map(|r| managed_state.row_to_pointer(r));

                                if let Some(prev_ptr) = &prev_pointer {
                                    let pk =
                                        managed_state.pointer_to_pk_row(&partition_row, prev_ptr);
                                    if let Some(pk) = pk {
                                        if let Some(prev_sr) = managed_state.get_row(&pk).await? {
                                            managed_state.update_next_pointer(
                                                &prev_sr,
                                                next_pointer.as_deref().unwrap_or(&null_ptr),
                                            );
                                        }
                                    }
                                }

                                // 7. Update next's prev pointer to point to prev.
                                if let Some(next_ptr) = &next_pointer {
                                    let pk =
                                        managed_state.pointer_to_pk_row(&partition_row, next_ptr);
                                    if let Some(pk) = pk {
                                        if let Some(next_sr) = managed_state.get_row(&pk).await? {
                                            managed_state.update_prev_pointer(
                                                &next_sr,
                                                prev_pointer.as_deref().unwrap_or(&null_ptr),
                                            );
                                        }
                                    }
                                }

                                // 8. If both neighbors exist, emit new fills between them.
                                if let (Some(prev_out), Some(next_out)) =
                                    (&prev_output, &next_output)
                                {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
                Message::Barrier(barrier) => {
                    let post_commit = managed_state.flush(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);
                    post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Interval, ScalarImpl, Timestamp};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::LiteralExpression;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MessageSender, MockSource};

    async fn create_executor(
        store: MemoryStateStore,
        fill_columns: HashMap<usize, FillStrategy>,
        schema: Schema,
        gap_interval: Interval,
    ) -> (MessageSender, BoxedMessageStream) {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), vec![0]);

        let time_column_index = 0;
        let partition_by_indices: Vec<usize> = vec![];
        // Stream key = [0] (time column). Since time is the only sk col,
        // prev/next pointers each have 1 datum (the time value).
        let upstream_stream_key: Vec<usize> = vec![0];
        let pointer_key_indices = upstream_stream_key.clone();

        let mut table_columns: Vec<ColumnDesc> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc::unnamed(ColumnId::new(i as i32), f.data_type.clone()))
            .collect();

        // Add prev/next pointer columns — each stores the full stream key.
        // For this test: sk=[0], so each pointer has 1 datum (time value).
        let time_dt = schema[time_column_index].data_type();
        let mut col_id = table_columns.len() as i32;
        // prev pointer (sk[0])
        table_columns.push(ColumnDesc::unnamed(ColumnId::new(col_id), time_dt.clone()));
        col_id += 1;
        // next pointer (sk[0])
        table_columns.push(ColumnDesc::unnamed(ColumnId::new(col_id), time_dt));

        // PK: (partition_cols, time_col, stream_key) with dedup = [0]
        // (no partition, time=0, sk=[0] already covered)
        let table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(0),
                table_columns,
                vec![OrderType::ascending()],
                vec![0],
                0,
                vec![],
            ),
            store,
            None,
        )
        .await;

        let executor = GapFillExecutor::new(GapFillExecutorArgs {
            ctx: ActorContext::for_test(123),
            input: source,
            schema: schema.clone(),
            chunk_size: 1024,
            time_column_index,
            fill_columns,
            gap_interval: NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            state_table: table,
            partition_by_indices,
            upstream_stream_key,
            pointer_key_indices,
        });

        (tx, executor.boxed().execute())
    }

    fn test_gap_fill_metrics() -> GapFillMetrics {
        let ctx = ActorContext::for_test(123);
        let actor_id = ctx.id.to_string();
        let fragment_id = ctx.fragment_id.to_string();

        GapFillMetrics {
            gap_fill_generated_rows_count: ctx
                .streaming_metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        }
    }

    #[test]
    fn test_generate_filled_rows_preserve_partition_columns() {
        let prev_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(7)),
            Some(ScalarImpl::Timestamp(
                "2023-04-01T10:00:00".parse::<Timestamp>().unwrap(),
            )),
            Some(ScalarImpl::Int32(10)),
            Some(ScalarImpl::Int32(99)),
        ]);
        let curr_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(7)),
            Some(ScalarImpl::Timestamp(
                "2023-04-01T10:03:00".parse::<Timestamp>().unwrap(),
            )),
            Some(ScalarImpl::Int32(40)),
            Some(ScalarImpl::Int32(88)),
        ]);

        let filled_rows = GapFillExecutor::<MemoryStateStore>::generate_filled_rows_between_static(
            &prev_row,
            &curr_row,
            &Interval::from_minutes(1),
            1,
            &[0],
            &HashMap::from([(2, FillStrategy::Locf)]),
            &test_gap_fill_metrics(),
        )
        .unwrap();

        assert_eq!(
            filled_rows,
            vec![
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(7)),
                    Some(ScalarImpl::Timestamp(
                        "2023-04-01T10:01:00".parse::<Timestamp>().unwrap(),
                    )),
                    Some(ScalarImpl::Int32(10)),
                    None,
                ]),
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(7)),
                    Some(ScalarImpl::Timestamp(
                        "2023-04-01T10:02:00".parse::<Timestamp>().unwrap(),
                    )),
                    Some(ScalarImpl::Int32(10)),
                    None,
                ]),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_locf() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Locf), (2, FillStrategy::Locf)]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        let expected = StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0
            + 2022-01-01T00:01:00 1   1.0
            + 2022-01-01T00:02:00 1   1.0",
        );

        // Simple comparison since the test utility assumes Int64 keys.
        assert_eq!(chunk.ops(), expected.ops());
        assert_eq!(chunk.visibility(), expected.visibility());

        // Compare each row individually.
        let chunk_rows: Vec<_> = chunk.rows().collect();
        let expected_rows: Vec<_> = expected.rows().collect();
        assert_eq!(chunk_rows.len(), expected_rows.len());

        for (i, ((op1, row1), (op2, row2))) in
            chunk_rows.iter().zip_eq(expected_rows.iter()).enumerate()
        {
            assert_eq!(op1, op2, "Row {} operation mismatch", i);
            assert_eq!(
                row1.to_owned_row(),
                row2.to_owned_row(),
                "Row {} data mismatch",
                i
            );
        }

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        // This tests if the executor can correctly retract old filled rows and create new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        // Expect a chunk that retracts the old fills, inserts the new row, and adds the new fills.
        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();

        let expected2 = StreamChunk::from_pretty(
            " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 1   1.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 2   2.0",
        );

        assert_eq!(chunk2.sort_rows(), expected2.sort_rows());

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and generation of new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        // This should also trigger retraction and re-generation of fills.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        // The filled rows' values don't change as they depend on the first row,
        // but they are still retracted and re-inserted due to the general path logic.
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 1   1.0
                U- 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0
                U+ 2022-01-01T00:03:00 5   5.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_null() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Null), (2, FillStrategy::Null)]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 2   2.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   ."
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 .   .
                U- 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   .
                U+ 2022-01-01T00:03:00 5   5.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_interpolate() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
        ]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:01:00 5   5.5
                + 2022-01-01T00:02:00 10  10.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and re-calculation of interpolated values.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 5   5.5
                - 2022-01-01T00:02:00 10  10.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        // This will cause the interpolated values to be re-calculated.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 10  10.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                U- 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 4   4.0
                + 2022-01-01T00:02:00 7   7.0
                U+ 2022-01-01T00:03:00 10  10.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_recovery() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Locf), (2, FillStrategy::Interpolate)]);

        // --- First run ---
        let (mut tx, mut executor) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // Send a chunk and commit.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        // Consume the initial filled chunk.
        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 1   2.0
                + 2022-01-01T00:02:00 1   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap(); // Barrier to commit.

        // --- Second run (after recovery) ---
        let (mut tx2, mut executor2) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier, which triggers recovery.
        tx2.push_barrier(test_epoch(2), false);
        executor2.next().await.unwrap().unwrap(); // Barrier

        // After recovery, the executor should not output anything for the loaded state.

        // Send a new chunk, which should fill the gap between old and new data.
        tx2.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:05:00 6   10.0",
        ));

        let chunk2 = executor2
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:04:00 4   7.0
                + 2022-01-01T00:05:00 6   10.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_mixed_strategy() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);

        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ]);
        let gap_interval = Interval::from_days(1);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, gap_interval).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Send an initial chunk with a gap to test mixed filling strategies.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-02T10:00:00 17 100 .    130.0
                + 2023-04-03T10:00:00 25 150 3.0 160.0
                + 2023-04-04T10:00:00 37 150 .    180.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            - 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));
        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 17 100 .    130.0
                - 2023-04-03T10:00:00 25 150 3.0 160.0
                - 2023-04-04T10:00:00 37 150 .    180.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            U- 2023-04-05T10:00:00 50 200 5.0 200.0
            U+ 2023-04-05T10:00:00 50 200 5.0 300.0",
        ));
        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                U- 2023-04-05T10:00:00 50 200 5.0 200.0
                + 2023-04-02T10:00:00 20 100 .    150.0
                + 2023-04-03T10:00:00 30 100 .    200.0
                + 2023-04-04T10:00:00 40 100 .    250.0
                U+ 2023-04-05T10:00:00 50 200 5.0 300.0"
            )
            .sort_rows()
        );
    }
}
