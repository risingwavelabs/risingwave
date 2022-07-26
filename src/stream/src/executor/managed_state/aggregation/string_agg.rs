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

use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::for_await;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::Op::{Delete, Insert, UpdateDelete, UpdateInsert};
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::sort_util::{OrderPair, OrderType, OrderableRow};
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;

use super::ManagedTableState;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

#[derive(Debug)]
struct Cache {
    dirty: Option<bool>, // None means not synced from state table (cold start)
    order_pairs: Arc<Vec<OrderPair>>,
    rows: BTreeSet<OrderableRow>,
}

impl Cache {
    fn new(order_pairs: Vec<OrderPair>) -> Cache {
        Cache {
            dirty: None,
            order_pairs: Arc::new(order_pairs),
            rows: BTreeSet::new(),
        }
    }

    fn is_cold_start(&self) -> bool {
        self.dirty.is_none()
    }

    fn is_dirty(&self) -> bool {
        self.dirty.unwrap_or(false)
    }

    fn set_dirty(&mut self, dirty: bool) {
        self.dirty = Some(dirty);
    }

    fn insert(&mut self, row: Row) {
        if !self.is_cold_start() {
            let orderable_row = OrderableRow::new(row, None, self.order_pairs.clone());
            self.rows.insert(orderable_row);
            self.set_dirty(true);
        }
    }

    fn remove(&mut self, row: Row) {
        if !self.is_cold_start() {
            let orderable_row = OrderableRow::new(row, None, self.order_pairs.clone());
            self.rows.remove(&orderable_row);
            self.set_dirty(true);
        }
    }
}

pub struct ManagedStringAggState<S: StateStore> {
    _phantom_data: PhantomData<S>,

    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Contains the column indices in upstream schema that are selected into state table.
    state_table_col_indices: Vec<usize>,

    /// The column to aggregate in state table.
    state_table_agg_col_idx: usize,

    /// In-memory fully synced cache.
    cache: Cache,
}

impl<S: StateStore> ManagedStringAggState<S> {
    pub fn new(
        agg_call: AggCall,
        group_key: Option<&Row>,
        pk_indices: PkIndices,
        state_table_col_indices: Vec<usize>,
    ) -> StreamExecutorResult<Self> {
        // construct a hashmap from the selected columns in the state table
        let col_mapping: HashMap<usize, usize> = state_table_col_indices
            .iter()
            .enumerate()
            .map(|(i, col_idx)| (*col_idx, i))
            .collect();
        println!("[rc] col_mapping: {:?}", col_mapping);
        // map agg column to state table column index
        let state_table_agg_col_idx = col_mapping
            .get(&agg_call.args.val_indices()[0])
            .expect("the column to be aggregate must appear in the state table")
            .clone();
        // map order by columns to state table column indices
        let order_pair = agg_call
            .order_pairs
            .iter()
            .map(|o| {
                OrderPair::new(
                    col_mapping
                        .get(&o.column_idx)
                        .expect("the column to be order by must appear in the state table")
                        .clone(),
                    o.order_type,
                )
            })
            .chain(pk_indices.iter().map(|idx| {
                OrderPair::new(
                    col_mapping
                        .get(idx)
                        .expect("the pk columns must appear in the state table")
                        .clone(),
                    OrderType::Ascending,
                )
            }))
            .collect();
        println!("[rc] order_pair: {:?}", order_pair);
        Ok(Self {
            _phantom_data: PhantomData,
            group_key: group_key.cloned(),
            state_table_col_indices,
            state_table_agg_col_idx,
            cache: Cache::new(order_pair),
        })
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedStringAggState<S> {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        chunk_cols: &[&ArrayImpl], // contains all upstream columns
        _epoch: u64,
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

            let state_row = Row::new(
                self.state_table_col_indices
                    .iter()
                    .map(|col_idx| chunk_cols[*col_idx].datum_at(i))
                    .collect(),
            );
            println!("[rc] apply state_row: {:?}", state_row);
            println!(
                "[rc] state table schema: {:?}",
                state_table.storage_table().schema()
            );

            match op {
                Insert | UpdateInsert => {
                    self.cache.insert(state_row.clone());
                    state_table.insert(state_row)?;
                }
                Delete | UpdateDelete => {
                    self.cache.remove(state_row.clone());
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
        let mut agg_result = String::new();

        if self.cache.is_cold_start() {
            println!("[rc] cold start");
            let all_data_iter = if let Some(group_key) = self.group_key.as_ref() {
                state_table.iter_with_pk_prefix(group_key, epoch).await?
            } else {
                state_table.iter(epoch).await?
            };
            pin_mut!(all_data_iter);

            #[for_await]
            for state_row in all_data_iter {
                let state_row = state_row?;
                self.cache.insert(state_row.as_ref().to_owned());
                let value = state_row[self.state_table_agg_col_idx]
                    .clone()
                    .map(ScalarImpl::into_utf8);
                println!("[rc] state_row: {:?}, value: {:?}", state_row, value);
                if let Some(s) = value {
                    agg_result.push_str(&s);
                }
            }

            self.cache.set_dirty(false); // now the cache is fully synced
        } else {
            println!("[rc] warm start");
            for orderable_row in self.cache.rows.iter().rev() {
                let value = orderable_row.row[self.state_table_agg_col_idx]
                    .clone()
                    .map(ScalarImpl::into_utf8);
                println!(
                    "[rc] orderable_row: {:?}, value: {:?}",
                    orderable_row, value
                );
                if let Some(s) = value {
                    agg_result.push_str(&s);
                }
            }
        }

        Ok(Some(agg_result.into()))
    }

    fn is_dirty(&self) -> bool {
        self.cache.is_dirty()
    }

    fn flush(&mut self, _state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()> {
        Ok(())
    }
}
