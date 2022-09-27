// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Aggregators with state store support

use std::collections::BTreeMap;
use std::sync::Arc;

pub use extreme::*;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;
pub use value::*;

use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::aggregation::array_agg::ManagedArrayAggState;
use crate::executor::managed_state::aggregation::string_agg::ManagedStringAggState;
use crate::executor::PkIndices;

mod array_agg;
mod extreme;
mod string_agg;
mod value;

/// Verify if the data going through the state is valid by checking if `ops.len() ==
/// visibility.len() == data[x].len()`.
pub fn verify_batch(
    ops: risingwave_common::array::stream_chunk::Ops<'_>,
    visibility: Option<&risingwave_common::buffer::Bitmap>,
    data: &[&risingwave_common::array::ArrayImpl],
) -> bool {
    let mut all_lengths = vec![ops.len()];
    if let Some(visibility) = visibility {
        all_lengths.push(visibility.len());
    }
    all_lengths.extend(data.iter().map(|x| x.len()));
    all_lengths.iter().min() == all_lengths.iter().max()
}

/// Common cache structure for managed table states (non-append-only `min`/`max`, `string_agg`).
pub struct Cache<K: Ord, V> {
    /// The capacity of the cache.
    capacity: usize,
    /// Ordered cache entries.
    entries: BTreeMap<K, V>,
}

impl<K: Ord, V> Cache<K, V> {
    /// Create a new cache with specified capacity and order requirements.
    /// To create a cache with unlimited capacity, use `usize::MAX` for `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Default::default(),
        }
    }

    /// Get the capacity of the cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear the cache.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Insert an entry into the cache.
    /// Key: `OrderedRow` composed of order by fields.
    /// Value: The value fields that are to be aggregated.
    pub fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key, value);
        // evict if capacity is reached
        while self.entries.len() > self.capacity {
            self.entries.pop_last();
        }
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: K) {
        self.entries.remove(&key);
    }

    /// Get the last (largest) key in the cache
    pub fn last_key(&self) -> Option<&K> {
        self.entries.last_key_value().map(|(k, _)| k)
    }

    /// Get the first (smallest) value in the cache.
    pub fn first_value(&self) -> Option<&V> {
        self.entries.first_key_value().map(|(_, v)| v)
    }

    /// Iterate over the values in the cache.
    pub fn iter_values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }
}

/// All managed state for aggregation. The managed state will manage the cache and integrate
/// the state with the underlying state store. Managed states can only be evicted from outer cache
/// when they are not dirty.
pub enum ManagedStateImpl<S: StateStore> {
    /// States as single scalar value e.g. `COUNT`, `SUM`
    Value(ManagedValueState),

    /// States as table structure e.g. `MAX`, `STRING_AGG`
    Table(Box<dyn ManagedTableState<S>>),
}

impl<S: StateStore> ManagedStateImpl<S> {
    pub async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => state.apply_chunk(ops, visibility, columns),
            Self::Table(state) => {
                state
                    .apply_chunk(ops, visibility, columns, state_table)
                    .await
            }
        }
    }

    /// Get the output of the state. Must flush before getting output.
    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        match self {
            Self::Value(state) => Ok(state.get_output()),
            Self::Table(state) => state.get_output(state_table).await,
        }
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        match self {
            Self::Value(state) => state.is_dirty(),
            Self::Table(state) => state.is_dirty(),
        }
    }

    /// Flush the internal state to a write batch.
    pub fn flush(&mut self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => state.flush(state_table),
            Self::Table(state) => state.flush(state_table),
        }
    }

    /// Create a managed state from `agg_call`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_managed_state(
        agg_call: AggCall,
        row_count: Option<usize>,
        pk_indices: PkIndices,
        is_row_count: bool,
        group_key: Option<&Row>,
        extreme_cache_size: usize,
        state_table: &StateTable<S>,
        state_table_col_mapping: Arc<StateTableColumnMapping>,
    ) -> StreamExecutorResult<Self> {
        assert!(
            is_row_count || row_count.is_some(),
            "should set row_count for value states other than row count agg call"
        );
        match agg_call.kind {
            AggKind::Avg | AggKind::Count | AggKind::Sum | AggKind::ApproxCountDistinct => {
                Ok(Self::Value(
                    ManagedValueState::new(agg_call, row_count, group_key, state_table).await?,
                ))
            }
            // optimization: use single-value state for append-only min/max
            AggKind::Max | AggKind::Min if agg_call.append_only => Ok(Self::Value(
                ManagedValueState::new(agg_call, row_count, group_key, state_table).await?,
            )),
            AggKind::Max | AggKind::Min => Ok(Self::Table(Box::new(GenericExtremeState::new(
                agg_call,
                group_key,
                pk_indices,
                state_table_col_mapping,
                row_count.unwrap(),
                extreme_cache_size,
            )))),
            AggKind::StringAgg => Ok(Self::Table(Box::new(ManagedStringAggState::new(
                agg_call,
                group_key,
                pk_indices,
                state_table_col_mapping,
                row_count.unwrap(),
            )))),
            AggKind::ArrayAgg => Ok(Self::Table(Box::new(ManagedArrayAggState::new(
                agg_call,
                group_key,
                pk_indices,
                state_table_col_mapping,
                row_count.unwrap(),
            )))),
        }
    }
}
