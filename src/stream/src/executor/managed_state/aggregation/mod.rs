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

use std::collections::BTreeSet;
use std::sync::Arc;

pub use extreme::*;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_common::util::sort_util::{DescOrderedRow, OrderPair};
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;
pub use value::*;

use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::aggregation::string_agg::ManagedStringAggState;
use crate::executor::PkIndices;

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

#[derive(Debug)]
pub struct Cache {
    synced: bool,            // `false` means not synced with state table (cold start)
    capacity: Option<usize>, // `None` means unlimited capacity
    order_pairs: Arc<Vec<OrderPair>>, // order requirements used to sort cached rows
    rows: BTreeSet<DescOrderedRow>, // in reverse order of `order_pairs`
}

impl Cache {
    pub fn new(capacity: Option<usize>, order_pairs: Vec<OrderPair>) -> Self {
        Self {
            synced: false,
            capacity,
            order_pairs: Arc::new(order_pairs),
            rows: BTreeSet::new(),
        }
    }

    pub fn is_cold_start(&self) -> bool {
        !self.synced
    }

    pub fn set_synced(&mut self) {
        self.synced = true;
    }

    pub fn insert(&mut self, row: Row) {
        if self.synced {
            let ordered_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.insert(ordered_row);
            // evict if capacity is reached
            if let Some(capacity) = self.capacity {
                while self.rows.len() > capacity {
                    self.rows.pop_first();
                }
            }
        }
    }

    pub fn remove(&mut self, row: Row) {
        if self.synced {
            let ordered_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.remove(&ordered_row);
        }
    }

    pub fn first(&self) -> Option<&Row> {
        if self.synced {
            // get the last because the rows are sorted reversely
            self.rows.last().map(|row| &row.row)
        } else {
            None
        }
    }

    pub fn iter_rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter().rev().map(|row| &row.row)
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
    pub async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        epoch: u64,
        state_table: &mut RowBasedStateTable<S>,
    ) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => state.apply_batch(ops, visibility, data),
            Self::Table(state) => {
                state
                    .apply_batch(ops, visibility, data, epoch, state_table)
                    .await
            }
        }
    }

    /// Get the output of the state. Must flush before getting output.
    pub async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::Value(state) => state.get_output(),
            Self::Table(state) => state.get_output(epoch, state_table).await,
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
    pub fn flush(&mut self, state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => state.flush(state_table),
            Self::Table(state) => state.flush(state_table),
        }
    }

    /// Create a managed state from `agg_call`.
    pub async fn create_managed_state(
        agg_call: AggCall,
        row_count: Option<usize>,
        pk_indices: PkIndices,
        is_row_count: bool,
        pk: Option<&Row>,
        state_table: &RowBasedStateTable<S>,
        state_table_col_mapping: Arc<StateTableColumnMapping>,
    ) -> StreamExecutorResult<Self> {
        match agg_call.kind {
            AggKind::Max | AggKind::Min => {
                assert!(
                    row_count.is_some(),
                    "should set row_count for value states other than AggKind::RowCount"
                );

                if agg_call.append_only {
                    // optimization: use single-value state for append-only min/max
                    Ok(Self::Value(
                        ManagedValueState::new(agg_call, row_count, pk, state_table).await?,
                    ))
                } else {
                    Ok(Self::Table(Box::new(GenericExtremeState::new(
                        agg_call,
                        pk,
                        pk_indices,
                        state_table_col_mapping,
                        row_count.unwrap(),
                        // TODO: estimate a good cache size instead of hard-coding
                        Some(1024),
                    ))))
                }
            }
            AggKind::StringAgg => Ok(Self::Table(Box::new(ManagedStringAggState::new(
                agg_call,
                pk,
                pk_indices,
                state_table_col_mapping,
            )))),
            // TODO: for append-only lists, we can create `ManagedValueState` instead of
            // `ManagedExtremeState`.
            AggKind::Avg | AggKind::Count | AggKind::Sum | AggKind::ApproxCountDistinct => {
                assert!(
                    is_row_count || row_count.is_some(),
                    "should set row_count for value states other than AggKind::RowCount"
                );
                Ok(Self::Value(
                    ManagedValueState::new(agg_call, row_count, pk, state_table).await?,
                ))
            }
            AggKind::SingleValue => Ok(Self::Value(
                ManagedValueState::new(agg_call, row_count, pk, state_table).await?,
            )),
        }
    }
}
