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
use risingwave_common::util::sort_util::{DescOrderedRow, OrderPair, OrderType};
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;

use super::ManagedTableState;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

#[derive(Debug)]
struct Cache {
    synced: bool, // `false` means not synced from state table (cold start)
    order_pairs: Arc<Vec<OrderPair>>, // order requirements used to sort cached rows
    rows: BTreeSet<DescOrderedRow>,
}

impl Cache {
    fn new(order_pairs: Vec<OrderPair>) -> Cache {
        Cache {
            synced: false,
            order_pairs: Arc::new(order_pairs),
            rows: BTreeSet::new(),
        }
    }

    fn is_cold_start(&self) -> bool {
        !self.synced
    }

    fn set_synced(&mut self) {
        self.synced = true;
    }

    fn insert(&mut self, row: Row) {
        if self.synced {
            let orderable_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.insert(orderable_row);
        }
    }

    fn remove(&mut self, row: Row) {
        if self.synced {
            let orderable_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.remove(&orderable_row);
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
        // map agg column to state table column index
        let state_table_agg_col_idx = *col_mapping
            .get(&agg_call.args.val_indices()[0])
            .expect("the column to be aggregate must appear in the state table");
        // map order by columns to state table column indices
        let order_pair = agg_call
            .order_pairs
            .iter()
            .map(|o| {
                OrderPair::new(
                    *col_mapping
                        .get(&o.column_idx)
                        .expect("the column to be order by must appear in the state table"),
                    o.order_type,
                )
            })
            .chain(pk_indices.iter().map(|idx| {
                OrderPair::new(
                    *col_mapping
                        .get(idx)
                        .expect("the pk columns must appear in the state table"),
                    OrderType::Ascending,
                )
            }))
            .collect();
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
            let all_data_iter = if let Some(group_key) = self.group_key.as_ref() {
                state_table.iter_with_pk_prefix(group_key, epoch).await?
            } else {
                state_table.iter(epoch).await?
            };
            pin_mut!(all_data_iter);

            self.cache.set_synced(); // after the following loop the cache should be fully synced

            #[for_await]
            for state_row in all_data_iter {
                let state_row = state_row?;
                self.cache.insert(state_row.as_ref().to_owned());
                let value = state_row[self.state_table_agg_col_idx]
                    .clone()
                    .map(ScalarImpl::into_utf8);
                if let Some(s) = value {
                    agg_result.push_str(&s);
                }
            }
        } else {
            // rev() is required because cache.rows is in reverse order
            for orderable_row in self.cache.rows.iter().rev() {
                let value = orderable_row.row[self.state_table_agg_col_idx]
                    .clone()
                    .map(ScalarImpl::into_utf8);
                if let Some(s) = value {
                    agg_result.push_str(&s);
                }
            }
        }

        Ok(Some(agg_result.into()))
    }

    fn is_dirty(&self) -> bool {
        false
    }

    fn flush(&mut self, _state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Row, StreamChunk, StreamChunkTestExt};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::state_table::RowBasedStateTable;

    use super::ManagedStringAggState;
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::managed_state::aggregation::ManagedTableState;
    use crate::executor::StreamExecutorResult;

    #[tokio::test]
    async fn test_string_agg_state_simple_agg_without_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Unary(DataType::Varchar, 0),
            return_type: DataType::Varchar,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        };

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
        ];
        let state_table_col_indices = vec![3, 0];
        let mut state_table = RowBasedStateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![OrderType::Ascending],
            vec![0], // [_row_id]
        );

        let mut agg_state =
            ManagedStringAggState::new(agg_call, None, input_pk_indices, state_table_col_indices)?;

        let mut epoch = 0;

        let chunk = StreamChunk::from_pretty(
            " T i i I
            + a 1 8 123
            + b 5 2 128
            - b 5 2 128
            + c 1 3 130",
        );
        let (ops, columns, visibility) = chunk.into_inner();
        let chunk_cols: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
        agg_state
            .apply_batch(
                &ops,
                visibility.as_ref(),
                &chunk_cols,
                epoch,
                &mut state_table,
            )
            .await?;

        epoch += 1;
        agg_state.flush(&mut state_table)?;
        state_table.commit(epoch).await.unwrap();

        let res = agg_state.get_output(epoch, &state_table).await?;
        match res {
            Some(ScalarImpl::Utf8(s)) => {
                assert!(s.len() == 2);
                assert!(s.contains('a'));
                assert!(s.contains('c'));
            }
            _ => panic!("unexpected output"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state_simple_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Unary(DataType::Varchar, 0),
            return_type: DataType::Varchar,
            order_pairs: vec![
                OrderPair::new(1, OrderType::Ascending),  // b ASC
                OrderPair::new(0, OrderType::Descending), // a DESC
            ],
            append_only: false,
            filter: None,
        };

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // b
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
        ];
        let state_table_col_indices = vec![1, 0, 3];
        let mut state_table = RowBasedStateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending,  // b ASC
                OrderType::Descending, // a DESC
                OrderType::Ascending,  // _row_id ASC
            ],
            vec![0, 1, 2], // [b, a, _row_id]
        );

        let mut agg_state =
            ManagedStringAggState::new(agg_call, None, input_pk_indices, state_table_col_indices)?;

        let mut epoch = 0;

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let chunk_cols: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_batch(
                    &ops,
                    visibility.as_ref(),
                    &chunk_cols,
                    epoch,
                    &mut state_table,
                )
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch += 1;

            let res = agg_state.get_output(epoch, &state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "ca".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let chunk_cols: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_batch(
                    &ops,
                    visibility.as_ref(),
                    &chunk_cols,
                    epoch,
                    &mut state_table,
                )
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch += 1;

            let res = agg_state.get_output(epoch, &state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "dcae".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state_grouped_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Unary(DataType::Varchar, 0),
            return_type: DataType::Varchar,
            order_pairs: vec![
                OrderPair::new(1, OrderType::Ascending), // b ASC
            ],
            append_only: false,
            filter: None,
        };

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // group by c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32), // order by b
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar), // a
        ];
        let state_table_col_indices = vec![2, 1, 3, 0];
        let mut state_table = RowBasedStateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // c ASC
                OrderType::Ascending, // b ASC
                OrderType::Ascending, // _row_id ASC
            ],
            vec![0, 1, 2], // [c, b, _row_id]
        );

        let mut agg_state = ManagedStringAggState::new(
            agg_call,
            Some(&Row::new(vec![Some(8.into())])),
            input_pk_indices,
            state_table_col_indices,
        )?;

        let mut epoch = 0;

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 1 3 130 D // hide this row",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let chunk_cols: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_batch(
                    &ops,
                    visibility.as_ref(),
                    &chunk_cols,
                    epoch,
                    &mut state_table,
                )
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch += 1;

            let res = agg_state.get_output(epoch, &state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "ab".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 0 2 134 D // hide this row
                + e 2 8 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let chunk_cols: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_batch(
                    &ops,
                    visibility.as_ref(),
                    &chunk_cols,
                    epoch,
                    &mut state_table,
                )
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch += 1;

            let res = agg_state.get_output(epoch, &state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "aeb".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }
}
