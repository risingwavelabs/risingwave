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

use std::collections::BTreeMap;
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::Op;
use risingwave_common::gap_fill_types::FillStrategy;
use risingwave_common::row::{self, CompactedRow, OwnedRow, Row};
use risingwave_common::types::{
    CheckedAdd, Datum, Decimal, ScalarImpl, ScalarRefImpl, ToOwnedDatum,
};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common_estimate_size::EstimateSize;
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
    pub fill_columns: Vec<(usize, FillStrategy)>,
    pub gap_interval: NonStrictExpression,
    pub state_table: StateTable<S>,
}

/// Tracks if a row is original data or a filled value.
#[derive(Debug, Clone, PartialEq)]
pub enum RowType {
    Original,
    Filled,
}

/// Cache key for Gap Fill, derived from the time column.
pub type GapFillCacheKey = Vec<u8>;

/// Cache for storing Gap Fill rows.
pub type GapFillCache = BTreeMap<GapFillCacheKey, (CompactedRow, RowType)>;

const GAPFILL_CACHE_DEFAULT_CAPACITY: usize = 1024;

pub struct ManagedGapFillState<S: StateStore> {
    state_table: StateTable<S>,
    time_key_serde: OrderedRowSerde,
    time_column_index: usize,
}

#[derive(Clone, PartialEq, Debug)]
pub struct GapFillStateRow {
    pub cache_key: GapFillCacheKey,
    pub row: OwnedRow,
    pub row_type: RowType,
}

impl GapFillStateRow {
    pub fn new(cache_key: GapFillCacheKey, row: OwnedRow, row_type: RowType) -> Self {
        Self {
            cache_key,
            row,
            row_type,
        }
    }
}

impl<S: StateStore> ManagedGapFillState<S> {
    pub fn new(state_table: StateTable<S>, time_column_index: usize, schema: &Schema) -> Self {
        // Create serializer for time column only.
        let time_column_type = schema[time_column_index].data_type();
        let time_key_serde = OrderedRowSerde::new(
            vec![time_column_type],
            vec![risingwave_common::util::sort_util::OrderType::ascending()],
        );

        Self {
            state_table,
            time_key_serde,
            time_column_index,
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

    /// Batch delete multiple rows from state table.
    pub fn batch_delete(&mut self, rows: Vec<impl Row>) {
        for row in rows {
            self.state_table.delete(row);
        }
    }

    /// Scans `StateStore` for filled rows between two time points (exclusive).
    pub async fn scan_filled_rows_between(
        &self,
        start_time: &GapFillCacheKey,
        end_time: &GapFillCacheKey,
    ) -> StreamExecutorResult<Vec<(OwnedRow, OwnedRow)>> {
        let mut filled_rows_to_delete = Vec::new();

        let start_time_row = self.time_key_serde.deserialize(start_time)?;
        let end_time_row = self.time_key_serde.deserialize(end_time)?;

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(
            Bound::Excluded(start_time_row),
            Bound::Excluded(end_time_row),
        );

        let state_table_iter = self
            .state_table
            .iter_with_prefix(
                &None::<row::Empty>,
                sub_range,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;

        pin_mut!(state_table_iter);
        while let Some(row_result) = state_table_iter.next().await {
            let row = row_result?.into_owned_row();

            if let Some(is_filled_datum) = row.datum_at(row.len() - 1)
                && let Some(ScalarImpl::Bool(is_filled)) = is_filled_datum.to_owned_datum()
                && is_filled
            {
                // This is a filled row, add to deletion list.
                // For state table, we use the time column as the key.
                let time_datum = row.datum_at(self.time_column_index);
                let state_key = OwnedRow::new(vec![time_datum.to_owned_datum()]);
                filled_rows_to_delete.push((state_key, row));
            }
        }

        Ok(filled_rows_to_delete)
    }

    /// Serialize time value to cache key.
    pub fn serialize_time_to_cache_key(&self, row: impl Row) -> GapFillCacheKey {
        let time_datum = row.datum_at(self.time_column_index);
        let time_row = OwnedRow::new(vec![time_datum.to_owned_datum()]);
        time_row.memcmp_serialize(&self.time_key_serde)
    }

    /// Scans a range of data from `StateStore` to build a window of rows before `end_time`.
    pub async fn scan_range_before(
        &self,
        end_time: &GapFillCacheKey,
        limit: usize,
    ) -> StreamExecutorResult<Vec<(GapFillCacheKey, CompactedRow, RowType)>> {
        let end_time_row = self.time_key_serde.deserialize(end_time)?;

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
            &(Bound::Unbounded, Bound::Excluded(end_time_row));

        let state_table_iter = self
            .state_table
            .iter_with_prefix(
                &None::<row::Empty>,
                sub_range,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;
        pin_mut!(state_table_iter);

        let mut results = Vec::new();

        // Collect all data and then take the last `limit` rows.
        let mut temp_results = Vec::new();
        while let Some(item) = state_table_iter.next().await {
            let state_row = item?.into_owned_row();
            let gapfill_row = self.get_gapfill_row(state_row);
            temp_results.push((
                gapfill_row.cache_key,
                (&gapfill_row.row).into(),
                gapfill_row.row_type,
            ));
        }

        let start_idx = if temp_results.len() > limit {
            temp_results.len() - limit
        } else {
            0
        };
        results.extend_from_slice(&temp_results[start_idx..]);

        Ok(results)
    }

    /// Scans a range of data from `StateStore` to build a window of rows after `start_time`.
    pub async fn scan_range_after(
        &self,
        start_time: &GapFillCacheKey,
        limit: usize,
    ) -> StreamExecutorResult<Vec<(GapFillCacheKey, CompactedRow, RowType)>> {
        let start_time_row = self.time_key_serde.deserialize(start_time)?;

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
            &(Bound::Excluded(start_time_row), Bound::Unbounded);

        let state_table_iter = self
            .state_table
            .iter_with_prefix(
                &None::<row::Empty>,
                sub_range,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;
        pin_mut!(state_table_iter);

        let mut results = Vec::new();
        let mut count = 0;

        while let Some(item) = state_table_iter.next().await
            && count < limit
        {
            let state_row = item?.into_owned_row();
            let gapfill_row = self.get_gapfill_row(state_row);
            results.push((
                gapfill_row.cache_key,
                (&gapfill_row.row).into(),
                gapfill_row.row_type,
            ));
            count += 1;
        }

        Ok(results)
    }

    /// Converts a state table row to a `GapFillStateRow`.
    fn get_gapfill_row(&self, state_row: OwnedRow) -> GapFillStateRow {
        let is_filled = Self::extract_is_filled_flag(&state_row);
        let output_row = Self::state_row_to_output_row(&state_row);
        let cache_key = self.serialize_time_to_cache_key(&output_row);

        GapFillStateRow::new(
            cache_key,
            output_row,
            if is_filled {
                RowType::Filled
            } else {
                RowType::Original
            },
        )
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.state_table.commit(epoch).await
    }

    /// Convert a row to state table format by appending the `is_filled` flag.
    fn row_to_state_row(row: &OwnedRow, is_filled: bool) -> OwnedRow {
        let mut state_row_data = row.as_inner().to_vec();
        state_row_data.push(Some(ScalarImpl::Bool(is_filled)));
        OwnedRow::new(state_row_data)
    }

    /// Convert a state table row back to output format by removing the `is_filled` flag.
    fn state_row_to_output_row(state_row: &OwnedRow) -> OwnedRow {
        let mut output_data = state_row.as_inner().to_vec();
        output_data.pop();
        OwnedRow::new(output_data)
    }

    /// Extract the `is_filled` flag from a state table row.
    fn extract_is_filled_flag(state_row: &OwnedRow) -> bool {
        let last_idx = state_row.len() - 1;
        if let Some(ScalarImpl::Bool(is_filled)) = state_row
            .datum_at(last_idx)
            .and_then(|d| d.to_owned_datum())
        {
            is_filled
        } else {
            false
        }
    }
}

/// A cache for `GapFillExecutor` that stores a continuous time window of data.
pub struct GapFillCacheManager {
    cache: GapFillCache,
    capacity: usize,

    /// The time bounds of the cached window. `None` if the cache is empty.
    window_bounds: Option<(GapFillCacheKey, GapFillCacheKey)>,
}

impl EstimateSize for GapFillCacheManager {
    fn estimated_heap_size(&self) -> usize {
        // Sum the estimated heap size of all entries in the cache.
        self.cache
            .iter()
            .map(|(key, (row, _row_type))| {
                key.estimated_heap_size()
                    + row.estimated_heap_size()
                    + std::mem::size_of::<RowType>()
            })
            .sum()
    }
}

impl GapFillCacheManager {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: GapFillCache::new(),
            capacity,
            window_bounds: None,
        }
    }

    /// Check if the given time is within the current cache window bounds.
    pub fn contains_time(&self, cache_key: &GapFillCacheKey) -> bool {
        if let Some((earliest, latest)) = &self.window_bounds {
            cache_key >= earliest && cache_key <= latest
        } else {
            false
        }
    }

    /// Load a continuous window of data into the cache, replacing all existing data.
    pub fn load_window(&mut self, window_data: Vec<(GapFillCacheKey, CompactedRow, RowType)>) {
        self.cache.clear();
        self.window_bounds = None;

        if window_data.is_empty() {
            return;
        }

        let mut earliest: Option<GapFillCacheKey> = None;
        let mut latest: Option<GapFillCacheKey> = None;

        for (cache_key, row, row_type) in window_data {
            self.cache.insert(cache_key.clone(), (row, row_type));

            if earliest.is_none() || &cache_key < earliest.as_ref().unwrap() {
                earliest = Some(cache_key.clone());
            }
            if latest.is_none() || &cache_key > latest.as_ref().unwrap() {
                latest = Some(cache_key.clone());
            }
        }

        if let (Some(earliest), Some(latest)) = (earliest, latest) {
            self.window_bounds = Some((earliest, latest));
        }
    }

    /// Finds the closest previous original row, scanning through the `StateStore` if not in cache.
    pub async fn find_robust_prev_original<S: StateStore>(
        &mut self,
        target_time: &GapFillCacheKey,
        managed_state: &ManagedGapFillState<S>,
    ) -> StreamExecutorResult<Option<(GapFillCacheKey, CompactedRow)>> {
        // Check current cache for a quick hit.
        if self.contains_time(target_time)
            && let Some(result) = self.find_prev_original_in_cache(target_time)
        {
            return Ok(Some(result));
        }

        // Cache miss or uncertain, start iterative window scanning.
        let mut current_search_end_time = target_time.clone();

        loop {
            // Load a window of data before the current search end time.
            let window_rows = managed_state
                .scan_range_before(&current_search_end_time, self.capacity)
                .await?;

            if window_rows.is_empty() {
                return Ok(None);
            }

            self.load_window(window_rows.clone());

            // Search for previous original row in this new continuous window.
            if let Some(result) = self.find_prev_original_in_cache(target_time) {
                // Found, and since we're scanning backwards, this is guaranteed to be the closest.
                return Ok(Some(result));
            } else {
                // No original row found in this window, continue searching backwards.
                if let Some(earliest_key) = window_rows.first().map(|(key, _, _)| key) {
                    current_search_end_time = earliest_key.clone();
                } else {
                    // This shouldn't happen since we checked `window_rows` is not empty.
                    return Ok(None);
                }
            }
        }
    }

    /// Finds the closest next original row, scanning through the `StateStore` if not in cache.
    pub async fn find_robust_next_original<S: StateStore>(
        &mut self,
        target_time: &GapFillCacheKey,
        managed_state: &ManagedGapFillState<S>,
    ) -> StreamExecutorResult<Option<(GapFillCacheKey, CompactedRow)>> {
        // Check current cache for a quick hit.
        if self.contains_time(target_time)
            && let Some(result) = self.find_next_original_in_cache(target_time)
        {
            return Ok(Some(result));
        }

        // Cache miss or uncertain, start iterative window scanning.
        let mut current_search_start_time = target_time.clone();

        loop {
            // Load a window of data after current_search_start_time.
            let window_rows = managed_state
                .scan_range_after(&current_search_start_time, self.capacity)
                .await?;

            if window_rows.is_empty() {
                return Ok(None);
            }

            self.load_window(window_rows.clone());

            // Search for next original row in this new continuous window.
            if let Some(result) = self.find_next_original_in_cache(target_time) {
                // Found, and since we're scanning forwards, this is guaranteed to be the closest.
                return Ok(Some(result));
            } else {
                // No original row found in this window, continue searching forwards.
                if let Some(latest_key) = window_rows.last().map(|(key, _, _)| key) {
                    current_search_start_time = latest_key.clone();
                } else {
                    // This shouldn't happen since we checked `window_rows` is not empty.
                    return Ok(None);
                }
            }
        }
    }

    /// Inserts a row into the cache, evicting the oldest row if capacity is exceeded.
    pub fn insert(&mut self, cache_key: GapFillCacheKey, row: CompactedRow, row_type: RowType) {
        self.cache.insert(cache_key.clone(), (row, row_type));

        // Update window bounds to include the new key
        self.update_window_bounds_for_insert(&cache_key);

        while self.cache.len() > self.capacity {
            if let Some((_key, _value)) = self.cache.pop_first() {
                // Update bounds after eviction
                self.update_window_bounds_after_removal();
            } else {
                break;
            }
        }
    }

    pub fn remove(&mut self, cache_key: &GapFillCacheKey) -> Option<(CompactedRow, RowType)> {
        let result = self.cache.remove(cache_key);
        if result.is_some() {
            self.update_window_bounds_after_removal();
        }
        result
    }

    /// Removes entries from the cache that are marked for deletion in the state table.
    pub fn sync_clean_cache_entries(
        &mut self,
        state_rows_to_delete: &[(OwnedRow, OwnedRow)],
        managed_state: &ManagedGapFillState<impl StateStore>,
    ) {
        let mut removed_any = false;
        for (_state_key, data_row) in state_rows_to_delete {
            let cache_key = managed_state.serialize_time_to_cache_key(data_row);
            if self.cache.remove(&cache_key).is_some() {
                removed_any = true;
            }
        }
        if removed_any {
            self.update_window_bounds_after_removal();
        }
    }

    /// Finds the closest previous original row in the cache.
    fn find_prev_original_in_cache(
        &self,
        cache_key: &GapFillCacheKey,
    ) -> Option<(GapFillCacheKey, CompactedRow)> {
        // Use window bounds to optimize: if the cache_key is before our window,
        // we definitely won't find anything
        if let Some((earliest, _)) = &self.window_bounds
            && cache_key <= earliest
        {
            return None;
        }

        self.cache
            .range::<GapFillCacheKey, _>(..cache_key)
            .rev()
            .find(|(_, (_, row_type))| *row_type == RowType::Original)
            .map(|(key, (row, _))| (key.clone(), row.clone()))
    }

    /// Finds the closest next original row in the cache.
    fn find_next_original_in_cache(
        &self,
        cache_key: &GapFillCacheKey,
    ) -> Option<(GapFillCacheKey, CompactedRow)> {
        // Use window bounds to optimize: if the cache_key is after our window,
        // we definitely won't find anything
        if let Some((_, latest)) = &self.window_bounds
            && cache_key >= latest
        {
            return None;
        }

        self.cache
            .range::<GapFillCacheKey, _>((Bound::Excluded(cache_key), Bound::Unbounded))
            .find(|(_, (_, row_type))| *row_type == RowType::Original)
            .map(|(key, (row, _))| (key.clone(), row.clone()))
    }

    /// Updates window bounds when a new key is inserted.
    fn update_window_bounds_for_insert(&mut self, new_key: &GapFillCacheKey) {
        match &mut self.window_bounds {
            None => {
                // Cache was empty, this is the first entry
                self.window_bounds = Some((new_key.clone(), new_key.clone()));
            }
            Some((earliest, latest)) => {
                // Update bounds if necessary
                if new_key < earliest {
                    *earliest = new_key.clone();
                }
                if new_key > latest {
                    *latest = new_key.clone();
                }
            }
        }
    }

    /// Updates window bounds after removing entries from the cache.
    fn update_window_bounds_after_removal(&mut self) {
        if self.cache.is_empty() {
            self.window_bounds = None;
        } else {
            // Recalculate bounds from the current cache contents
            let earliest = self.cache.keys().next().cloned();
            let latest = self.cache.keys().next_back().cloned();
            if let (Some(earliest), Some(latest)) = (earliest, latest) {
                self.window_bounds = Some((earliest, latest));
            }
        }
    }

    /// Scans cache for filled rows between two time points (exclusive).
    pub fn scan_filled_rows_between_in_cache<S: StateStore>(
        &self,
        start_time: &GapFillCacheKey,
        end_time: &GapFillCacheKey,
        managed_state: &ManagedGapFillState<S>,
    ) -> StreamExecutorResult<(Vec<(OwnedRow, OwnedRow)>, bool)> {
        let mut filled_rows_in_cache = Vec::new();

        // Check if both start_time and end_time are within cache bounds
        let range_fully_in_cache = self.contains_time(start_time) && self.contains_time(end_time);

        if range_fully_in_cache {
            for (_cache_key, (compacted_row, row_type)) in self.cache.range::<GapFillCacheKey, _>((
                std::ops::Bound::Excluded(start_time),
                std::ops::Bound::Excluded(end_time),
            )) {
                if *row_type == RowType::Filled {
                    // Convert compacted row back to owned row
                    let data_types = managed_state.state_table.get_data_types();
                    let mut row_data_types = data_types.to_vec();
                    // Remove the is_filled column type since cache stores original schema
                    row_data_types.pop();

                    let row = compacted_row.deserialize(&row_data_types)?;
                    let time_datum = row.datum_at(managed_state.time_column_index);
                    let state_key = OwnedRow::new(vec![time_datum.to_owned_datum()]);
                    // Convert to state row format (with is_filled flag)
                    let state_row = ManagedGapFillState::<S>::row_to_state_row(&row, true);
                    filled_rows_in_cache.push((state_key, state_row));
                }
            }
        }

        Ok((filled_rows_in_cache, range_fully_in_cache))
    }

    pub async fn scan_filled_rows_between<S: StateStore>(
        &self,
        start_time: &GapFillCacheKey,
        end_time: &GapFillCacheKey,
        managed_state: &ManagedGapFillState<S>,
    ) -> StreamExecutorResult<Vec<(OwnedRow, OwnedRow)>> {
        // Try to scan filled rows from cache first
        let (filled_rows_cache, range_fully_in_cache) =
            self.scan_filled_rows_between_in_cache(start_time, end_time, managed_state)?;

        if range_fully_in_cache {
            // Cache covers the entire range, use cache results
            Ok(filled_rows_cache)
        } else {
            // Cache doesn't cover the range, fall back to state table scan
            managed_state
                .scan_filled_rows_between(start_time, end_time)
                .await
        }
    }
}

pub struct GapFillExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: Vec<(usize, FillStrategy)>,
    gap_interval: NonStrictExpression,

    // State management
    managed_state: ManagedGapFillState<S>,
    cache_manager: GapFillCacheManager,
}

impl<S: StateStore> GapFillExecutor<S> {
    pub fn new(args: GapFillExecutorArgs<S>) -> Self {
        let managed_state =
            ManagedGapFillState::new(args.state_table, args.time_column_index, &args.schema);
        let cache_manager = GapFillCacheManager::new(GAPFILL_CACHE_DEFAULT_CAPACITY);

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
        }
    }

    /// Calculates the step size for interpolation between two values.
    ///
    /// # Parameters
    /// - `d1`: The starting value as a `Datum`.
    /// - `d2`: The ending value as a `Datum`.
    /// - `steps`: The number of steps to interpolate between `d1` and `d2`.
    ///
    /// # Returns
    /// Returns a `Datum` representing the step size for each interpolation step,
    /// or `None` if the input values are not compatible or `steps` is zero.
    ///
    /// # Calculation
    /// For supported types, computes `(d2 - d1) / steps` and returns the result as a `Datum`.
    fn calculate_step(d1: Datum, d2: Datum, steps: usize) -> Datum {
        let (Some(s1), Some(s2)) = (d1, d2) else {
            return None;
        };
        if steps == 0 {
            debug_assert!(
                steps != 0,
                "GapFillExecutor::calculate_step called with steps == 0, which may indicate a logic error."
            );
            return None;
        }
        match (ScalarRefImpl::from(&s1), ScalarRefImpl::from(&s2)) {
            (ScalarRefImpl::Int16(v1), ScalarRefImpl::Int16(v2)) => {
                Some(ScalarImpl::Int16((v2 - v1) / steps as i16))
            }
            (ScalarRefImpl::Int32(v1), ScalarRefImpl::Int32(v2)) => {
                Some(ScalarImpl::Int32((v2 - v1) / steps as i32))
            }
            (ScalarRefImpl::Int64(v1), ScalarRefImpl::Int64(v2)) => {
                Some(ScalarImpl::Int64((v2 - v1) / steps as i64))
            }
            (ScalarRefImpl::Float32(v1), ScalarRefImpl::Float32(v2)) => {
                Some(ScalarImpl::Float32((v2 - v1) / steps as f32))
            }
            (ScalarRefImpl::Float64(v1), ScalarRefImpl::Float64(v2)) => {
                Some(ScalarImpl::Float64((v2 - v1) / steps as f64))
            }
            (ScalarRefImpl::Decimal(v1), ScalarRefImpl::Decimal(v2)) => {
                Some(ScalarImpl::Decimal((v2 - v1) / Decimal::from(steps)))
            }
            _ => None,
        }
    }

    /// Mutates the `current` datum by adding the value of `step` to it.
    /// This function is used during the interpolation process in gap filling,
    /// where it incrementally updates the datum to generate intermediate values
    /// between known data points.
    fn apply_step(current: &mut Datum, step: &ScalarImpl) {
        if let Some(curr) = current.as_mut() {
            match (curr, step) {
                (ScalarImpl::Int16(v1), &ScalarImpl::Int16(v2)) => *v1 += v2,
                (ScalarImpl::Int32(v1), &ScalarImpl::Int32(v2)) => *v1 += v2,
                (ScalarImpl::Int64(v1), &ScalarImpl::Int64(v2)) => *v1 += v2,
                (ScalarImpl::Float32(v1), &ScalarImpl::Float32(v2)) => *v1 += v2,
                (ScalarImpl::Float64(v1), &ScalarImpl::Float64(v2)) => *v1 += v2,
                (ScalarImpl::Decimal(v1), &ScalarImpl::Decimal(v2)) => *v1 = *v1 + v2,
                _ => (),
            }
        }
    }

    /// Generates interpolated rows between two time points (`prev_row` and `curr_row`) using a static interval.
    ///
    /// # Parameters
    /// - `prev_row`: Reference to the previous row (start of the gap).
    /// - `curr_row`: Reference to the current row (end of the gap).
    /// - `interval`: The interval to use for generating each filled row (typically a time interval).
    /// - `time_column_index`: The index of the time column in the row, used to increment time values.
    /// - `fill_columns`: A slice of tuples, each containing a column index and a `FillStrategy` specifying how to fill missing values in that column.
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
        fill_columns: &[(usize, FillStrategy)],
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

        // Generate template row with fill strategies
        let mut fill_values: Vec<Datum> = Vec::with_capacity(prev_row.len());
        for i in 0..prev_row.len() {
            if i == time_column_index {
                fill_values.push(None); // Will be set later
            } else if let Some((_, strategy)) = fill_columns.iter().find(|(col, _)| *col == i) {
                match strategy {
                    FillStrategy::Locf | FillStrategy::Interpolate => {
                        fill_values.push(prev_row.datum_at(i).to_owned_datum())
                    }
                    FillStrategy::Null => fill_values.push(None),
                }
            } else {
                fill_values.push(prev_row.datum_at(i).to_owned_datum());
            }
        }

        // Generate filled timestamps
        let mut fill_time = match prev_time.checked_add(*interval) {
            Some(t) => t,
            None => return Ok(filled_rows),
        };

        let mut data = Vec::new();
        while fill_time < curr_time {
            let mut new_row_data = fill_values.clone();
            let fill_time_scalar = match prev_time_scalar {
                ScalarRefImpl::Timestamp(_) => ScalarImpl::Timestamp(fill_time),
                ScalarRefImpl::Timestamptz(_) => {
                    let micros = fill_time.0.and_utc().timestamp_micros();
                    ScalarImpl::Timestamptz(risingwave_common::types::Timestamptz::from_micros(
                        micros,
                    ))
                }
                _ => unreachable!("Time column should be Timestamp or Timestamptz"),
            };
            new_row_data[time_column_index] = Some(fill_time_scalar);
            data.push(new_row_data);

            fill_time = match fill_time.checked_add(*interval) {
                Some(t) => t,
                None => break,
            };
        }

        // --- NEW LOGIC FOR INTERPOLATION ---
        for (col_idx, strategy) in fill_columns {
            if matches!(strategy, FillStrategy::Interpolate) {
                let steps = data.len();
                let step = Self::calculate_step(
                    prev_row.datum_at(*col_idx).to_owned_datum(),
                    curr_row.datum_at(*col_idx).to_owned_datum(),
                    steps + 1,
                );
                if let Some(step) = step {
                    let mut cumulative_value = prev_row.datum_at(*col_idx).to_owned_datum();
                    for row in &mut data {
                        Self::apply_step(&mut cumulative_value, &step);
                        row[*col_idx] = cumulative_value.clone();
                    }
                }
            }
        }

        for row_data in data {
            filled_rows.push(OwnedRow::new(row_data));
        }

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
        } = *self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        managed_state.init_epoch(first_epoch).await?;

        // Start with an empty cache - use lazy loading strategy.
        // Data will be loaded on-demand when gap filling operations require it.

        // Calculate and validate gap interval once at initialization
        let dummy_row = OwnedRow::new(vec![]);
        let interval_datum = gap_interval.eval_row_infallible(&dummy_row).await;
        let interval = interval_datum
            .ok_or_else(|| anyhow::anyhow!("Gap interval expression returned null"))?
            .into_interval();

        // Validate that gap interval is not zero
        if interval.months() == 0 && interval.days() == 0 && interval.usecs() == 0 {
            Err(anyhow::anyhow!("Gap interval cannot be zero"))?;
        }

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact();
                    let mut output_rows = Vec::new();

                    for (op, row_ref) in chunk.rows() {
                        let row = row_ref.into_owned_row();

                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                let cache_key = managed_state.serialize_time_to_cache_key(&row);

                                // Find previous and next original row neighbors.
                                let prev_original = cache_manager
                                    .find_robust_prev_original(&cache_key, &managed_state)
                                    .await?;
                                let next_original = cache_manager
                                    .find_robust_next_original(&cache_key, &managed_state)
                                    .await?;

                                // If both neighbors exist, delete fill rows between them.
                                if let (Some((prev_key, _)), Some((next_key, _))) =
                                    (&prev_original, &next_original)
                                {
                                    let filled_rows_to_delete = cache_manager
                                        .scan_filled_rows_between(
                                            prev_key,
                                            next_key,
                                            &managed_state,
                                        )
                                        .await?;

                                    cache_manager.sync_clean_cache_entries(
                                        &filled_rows_to_delete,
                                        &managed_state,
                                    );

                                    let mut state_rows_to_delete = Vec::new();
                                    for (_state_key, state_row) in filled_rows_to_delete {
                                        state_rows_to_delete.push(state_row.clone());

                                        let output_row =
                                            ManagedGapFillState::<S>::state_row_to_output_row(
                                                &state_row,
                                            );
                                        output_rows.push((Op::Delete, output_row));
                                    }

                                    managed_state.batch_delete(state_rows_to_delete);
                                }

                                // Insert new original row.
                                let state_row =
                                    ManagedGapFillState::<S>::row_to_state_row(&row, false);
                                managed_state.insert(&state_row);
                                cache_manager.insert(
                                    cache_key.clone(),
                                    (&row).into(),
                                    RowType::Original,
                                );

                                output_rows.push((op, row.clone()));

                                // Refill gaps adjacent to the new row.
                                if let Some((_prev_key, prev_row_data)) = prev_original {
                                    let prev_row =
                                        prev_row_data.deserialize(&schema.data_types())?;
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        &prev_row,
                                        &row,
                                        &interval,
                                        time_column_index,
                                        &fill_columns,
                                    )?;

                                    for filled_row in filled_rows {
                                        let fill_cache_key =
                                            managed_state.serialize_time_to_cache_key(&filled_row);
                                        let state_row = ManagedGapFillState::<S>::row_to_state_row(
                                            &filled_row,
                                            true,
                                        );
                                        managed_state.insert(&state_row);
                                        cache_manager.insert(
                                            fill_cache_key,
                                            (&filled_row).into(),
                                            RowType::Filled,
                                        );
                                        output_rows.push((Op::Insert, filled_row));
                                    }
                                }

                                if let Some((_next_key, next_row_data)) = next_original {
                                    let next_row =
                                        next_row_data.deserialize(&schema.data_types())?;
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        &row,
                                        &next_row,
                                        &interval,
                                        time_column_index,
                                        &fill_columns,
                                    )?;

                                    for filled_row in filled_rows {
                                        let fill_cache_key =
                                            managed_state.serialize_time_to_cache_key(&filled_row);
                                        let state_row = ManagedGapFillState::<S>::row_to_state_row(
                                            &filled_row,
                                            true,
                                        );
                                        managed_state.insert(&state_row);
                                        cache_manager.insert(
                                            fill_cache_key,
                                            (&filled_row).into(),
                                            RowType::Filled,
                                        );
                                        output_rows.push((Op::Insert, filled_row));
                                    }
                                }
                            }
                            Op::Delete | Op::UpdateDelete => {
                                let cache_key = managed_state.serialize_time_to_cache_key(&row);

                                // Find previous and next original row neighbors before deletion.
                                let prev_original = cache_manager
                                    .find_robust_prev_original(&cache_key, &managed_state)
                                    .await?;
                                let next_original = cache_manager
                                    .find_robust_next_original(&cache_key, &managed_state)
                                    .await?;

                                // Delete fill rows on both sides of the row to be deleted.
                                let mut filled_rows_to_delete = Vec::new();

                                if let Some((prev_key, _)) = &prev_original {
                                    let fills_left = cache_manager
                                        .scan_filled_rows_between(
                                            prev_key,
                                            &cache_key,
                                            &managed_state,
                                        )
                                        .await?;
                                    filled_rows_to_delete.extend(fills_left);
                                }

                                if let Some((next_key, _)) = &next_original {
                                    let fills_right = cache_manager
                                        .scan_filled_rows_between(
                                            &cache_key,
                                            next_key,
                                            &managed_state,
                                        )
                                        .await?;
                                    filled_rows_to_delete.extend(fills_right);
                                }

                                cache_manager.sync_clean_cache_entries(
                                    &filled_rows_to_delete,
                                    &managed_state,
                                );

                                let mut state_rows_to_delete = Vec::new();
                                for (_state_key, state_row) in filled_rows_to_delete {
                                    state_rows_to_delete.push(state_row.clone());

                                    let output_row =
                                        ManagedGapFillState::<S>::state_row_to_output_row(
                                            &state_row,
                                        );
                                    output_rows.push((Op::Delete, output_row));
                                }
                                managed_state.batch_delete(state_rows_to_delete);

                                // Delete the original row.
                                let state_row =
                                    ManagedGapFillState::<S>::row_to_state_row(&row, false);
                                managed_state.delete(&state_row);
                                cache_manager.remove(&cache_key);
                                output_rows.push((op, row.clone()));

                                // If both neighbors exist, refill the gap between them.
                                if let (Some((_, prev_row_data)), Some((_, next_row_data))) =
                                    (prev_original, next_original)
                                {
                                    let prev_row =
                                        prev_row_data.deserialize(&schema.data_types())?;
                                    let next_row =
                                        next_row_data.deserialize(&schema.data_types())?;
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        &prev_row,
                                        &next_row,
                                        &interval,
                                        time_column_index,
                                        &fill_columns,
                                    )?;

                                    for filled_row in filled_rows {
                                        let fill_cache_key =
                                            managed_state.serialize_time_to_cache_key(&filled_row);
                                        let state_row = ManagedGapFillState::<S>::row_to_state_row(
                                            &filled_row,
                                            true,
                                        );
                                        managed_state.insert(&state_row);
                                        cache_manager.insert(
                                            fill_cache_key,
                                            (&filled_row).into(),
                                            RowType::Filled,
                                        );
                                        output_rows.push((Op::Insert, filled_row));
                                    }
                                }
                            }
                        }
                    }

                    if !output_rows.is_empty() {
                        let mut chunk_builder =
                            StreamChunkBuilder::new(chunk_size, schema.data_types());
                        for (op, row) in output_rows {
                            if let Some(chunk) = chunk_builder.append_row(op, &row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                        if let Some(chunk) = chunk_builder.take() {
                            yield Message::Chunk(chunk);
                        }
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
    use risingwave_common::types::{DataType, Interval};
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
        fill_columns: Vec<(usize, FillStrategy)>,
        schema: Schema,
        gap_interval: Interval,
    ) -> (MessageSender, BoxedMessageStream) {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), vec![0]);

        let mut table_columns: Vec<ColumnDesc> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc::unnamed(ColumnId::new(i as i32), f.data_type.clone()))
            .collect();

        // Add the is_filled flag column at the end.
        let is_filled_column_id = table_columns.len() as i32;
        table_columns.push(ColumnDesc::unnamed(
            ColumnId::new(is_filled_column_id),
            DataType::Boolean,
        ));

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

        let time_column_index = 0;

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
        });

        (tx, executor.boxed().execute())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_locf() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = vec![(1, FillStrategy::Locf), (2, FillStrategy::Locf)];
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
        let fill_columns = vec![(1, FillStrategy::Null), (2, FillStrategy::Null)];
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
        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
        ];
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
        let fill_columns = vec![(1, FillStrategy::Locf), (2, FillStrategy::Interpolate)];

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

        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ];
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
