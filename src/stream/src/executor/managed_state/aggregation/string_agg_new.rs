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

use std::collections::BTreeSet;
use std::marker::PhantomData;

use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::Op::{Delete, Insert, UpdateDelete, UpdateInsert};
use risingwave_common::array::{Array, ArrayImpl, Row, Utf8Array};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{Datum, ScalarImpl, ScalarRef};
use risingwave_common::util::sort_util::OrderableRow;
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;

use super::ManagedTableState;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::{PkDataTypes, PkIndices};

pub struct ManagedStringAggState<S: StateStore> {
    _phantom_data: PhantomData<S>,

    upstream_pk_indices: PkIndices,

    // Primary key to look up in relational table. For value state, there is only one row.
    /// If None, the pk is empty vector (simple agg). If not None, the pk is group key (hash agg).
    group_key: Option<Row>, // TODO(rc): this seems useless after we have `state_table_col_indices`

    agg_col_idx: usize,
    state_table_col_indices: Vec<usize>,
    state_table_agg_col_idx: usize,

    dirty: bool,
    cache: BTreeSet<OrderableRow>,
}

impl<S: StateStore> ManagedStringAggState<S> {
    pub fn new(
        agg_call: AggCall,
        pk_indices: PkIndices,
        pk_data_types: PkDataTypes,
        group_key: Option<&Row>,
        state_table_col_indices: Vec<usize>,
    ) -> StreamExecutorResult<Self> {
        println!(
            "[rc] new, pk_indices: {:?}, pk_data_types: {:?}, group_key: {:?}",
            pk_indices, pk_data_types, group_key
        );
        let agg_col_idx = agg_call.args.val_indices()[0];
        let state_table_agg_col_idx = state_table_col_indices
            .iter()
            .find_position(|idx| **idx == agg_col_idx)
            .map(|(pos, _)| pos)
            .expect("the column to be aggregate must appear in the state table");
        Ok(Self {
            _phantom_data: PhantomData,
            upstream_pk_indices: pk_indices,
            group_key: group_key.cloned(),
            agg_col_idx,
            state_table_col_indices,
            state_table_agg_col_idx,
            dirty: false,
            cache: BTreeSet::new(),
        })
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedStringAggState<S> {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        chunk_cols: &[&ArrayImpl],
        epoch: u64,
        state_table: &mut RowBasedStateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, chunk_cols));
        println!(
            "[rc] ManagedStringAggState::apply_chunk, ops: {:?}, visibility: {:?}, chunk_cols: {:?}",
            ops, visibility, chunk_cols
        );

        for (i, op) in ops.iter().enumerate() {
            let visible = visibility.map(|x| x.is_set(i).unwrap()).unwrap_or(true);
            if !visible {
                continue;
            }
            self.dirty = true;

            let state_row = Row::new(
                self.state_table_col_indices
                    .iter()
                    .map(|col_idx| chunk_cols[*col_idx].datum_at(i))
                    .collect(),
            );
            println!("[rc] state_row: {:?}", state_row);
            println!(
                "[rc] state table schema: {:?}",
                state_table.storage_table().schema()
            );

            match op {
                Insert | UpdateInsert => {
                    state_table.insert(state_row)?;
                }
                Delete | UpdateDelete => {
                    state_table.delete(state_row)?;
                }
            }
        }

        Ok(())
    }

    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        let all_data_iter = if let Some(group_key) = self.group_key.as_ref() {
            state_table.iter_with_pk_prefix(group_key, epoch).await?
        } else {
            state_table.iter(epoch).await?
        };
        pin_mut!(all_data_iter);

        let mut agg_result = String::new();

        #[for_await]
        for state_row in all_data_iter {
            let state_row = state_row?;
            let value = state_row[self.state_table_agg_col_idx]
                .clone()
                .map(ScalarImpl::into_utf8);
            println!("[rc] state_row: {:?}, value: {:?}", state_row, value);
            if let Some(s) = value {
                agg_result.push_str(&s);
            }
        }

        Ok(Some(agg_result.into()))
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn flush(&mut self, state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()> {
        println!("[rc] ManagedStringAggState::flush");
        if self.dirty {
            // TODO(rc): do more things later
        }
        Ok(())
    }
}
