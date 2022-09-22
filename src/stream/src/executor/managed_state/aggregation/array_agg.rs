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

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::for_await;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::Op::{Delete, Insert, UpdateDelete, UpdateInsert};
use risingwave_common::array::{ArrayImpl, ListValue, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::OrderedRow;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::{Cache, ManagedTableState};
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::iter_state_table;
use crate::executor::PkIndices;

pub struct ManagedArrayAggState<S: StateStore> {
    _phantom_data: PhantomData<S>,

    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Contains the column mapping between upstream schema and state table.
    state_table_col_mapping: Arc<StateTableColumnMapping>,

    /// The column to aggregate in state table.
    state_table_agg_col_idx: usize,

    /// The columns to order by in state table.
    state_table_order_col_indices: Vec<usize>,

    /// The order types of `state_table_order_col_indices`.
    state_table_order_types: Vec<OrderType>,

    /// In-memory all-or-nothing cache.
    cache: Cache<OrderedRow, Datum>,

    /// Whether the cache is fully synced to state table.
    cache_synced: bool,
}

impl<S: StateStore> ManagedArrayAggState<S> {
    pub fn new(
        agg_call: AggCall,
        group_key: Option<&Row>,
        pk_indices: PkIndices,
        col_mapping: Arc<StateTableColumnMapping>,
        row_count: usize,
    ) -> Self {
        // map agg column to state table column index
        let state_table_agg_col_idx = col_mapping
            .upstream_to_state_table(agg_call.args.val_indices()[0])
            .expect("the column to be aggregate must appear in the state table");
        // map order by columns to state table column indices
        let (state_table_order_col_indices, state_table_order_types) = agg_call
            .order_pairs
            .iter()
            .map(|o| {
                (
                    col_mapping
                        .upstream_to_state_table(o.column_idx)
                        .expect("the column to be order by must appear in the state table"),
                    o.order_type,
                )
            })
            .chain(pk_indices.iter().map(|idx| {
                (
                    col_mapping
                        .upstream_to_state_table(*idx)
                        .expect("the pk columns must appear in the state table"),
                    OrderType::Ascending,
                )
            }))
            .unzip();
        Self {
            _phantom_data: PhantomData,
            group_key: group_key.cloned(),
            state_table_col_mapping: col_mapping,
            state_table_agg_col_idx,
            state_table_order_col_indices,
            state_table_order_types,
            cache: Cache::new(usize::MAX),
            cache_synced: row_count == 0, // if there is no row, the cache is synced initially
        }
    }

    fn state_row_to_cache_entry(&self, state_row: &Row) -> (OrderedRow, Datum) {
        let cache_key = OrderedRow::new(
            state_row.by_indices(&self.state_table_order_col_indices),
            &self.state_table_order_types,
        );
        let cache_data = state_row[self.state_table_agg_col_idx].clone();
        (cache_key, cache_data)
    }

    fn apply_chunk_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, columns));

        // should not skip NULL value like `string_agg` and `min`/`max`
        for (i, op) in ops
            .iter()
            .enumerate()
            .filter(|(i, _)| visibility.map(|x| x.is_set(*i).unwrap()).unwrap_or(true))
        {
            let state_row = Row::new(
                self.state_table_col_mapping
                    .upstream_columns()
                    .iter()
                    .map(|col_idx| columns[*col_idx].datum_at(i))
                    .collect(),
            );
            let (cache_key, cache_data) = self.state_row_to_cache_entry(&state_row);

            match op {
                Insert | UpdateInsert => {
                    if self.cache_synced {
                        self.cache.insert(cache_key, cache_data);
                    }
                    state_table.insert(state_row);
                }
                Delete | UpdateDelete => {
                    if self.cache_synced {
                        self.cache.remove(cache_key);
                    }
                    state_table.delete(state_row);
                }
            }
        }

        Ok(())
    }

    async fn get_output_inner(
        &mut self,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        if !self.cache_synced {
            let all_data_iter = iter_state_table(state_table, self.group_key.as_ref()).await?;
            pin_mut!(all_data_iter);

            self.cache.clear();
            #[for_await]
            for state_row in all_data_iter {
                let state_row = state_row?;
                let (cache_key, cache_data) = self.state_row_to_cache_entry(&state_row);
                self.cache.insert(cache_key, cache_data.clone());
            }
            self.cache_synced = true;
        }

        let mut values = Vec::with_capacity(self.cache.len());
        for cache_data in self.cache.iter_values() {
            values.push(cache_data.clone());
        }
        Ok(Some(ListValue::new(values).into()))
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedArrayAggState<S> {
    async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl], // contains all upstream columns
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.apply_chunk_inner(ops, visibility, columns, state_table)
    }

    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.get_output_inner(state_table).await
    }

    fn is_dirty(&self) -> bool {
        false
    }

    fn flush(&mut self, _state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::array::{Row, StreamChunk, StreamChunkTestExt};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;

    use super::ManagedArrayAggState;
    use crate::common::StateTableColumnMapping;
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::managed_state::aggregation::ManagedTableState;
    use crate::executor::StreamExecutorResult;

    #[tokio::test]
    async fn test_array_agg_state_simple_agg_without_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Varchar, 0), // array_agg(a)
            return_type: DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
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
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![3, 0]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![OrderType::Ascending],
            vec![0], // [_row_id]
        );

        let mut agg_state =
            ManagedArrayAggState::new(agg_call, None, input_pk_indices, state_table_col_mapping, 0);

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        let chunk = StreamChunk::from_pretty(
            " T i i I
            + a 1 8 123
            + b 5 2 128
            - b 5 2 128
            + . 7 6 129
            + c 1 3 130",
        );
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
        agg_state
            .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
            .await?;

        epoch.inc();
        agg_state.flush(&mut state_table)?;
        state_table.commit(epoch).await.unwrap();

        let res = agg_state.get_output(&state_table).await?;
        match res {
            Some(ScalarImpl::List(res)) => {
                let res = res
                    .values()
                    .iter()
                    .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                    .collect_vec();
                assert_eq!(res.len(), 3);
                assert!(res.contains(&Some("a".to_string())));
                assert!(res.contains(&Some("c".to_string())));
                assert!(res.contains(&None));
            }
            _ => panic!("unexpected output"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_state_simple_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Int32, 1), // array_agg(b)
            return_type: DataType::Int32,
            order_pairs: vec![
                OrderPair::new(2, OrderType::Ascending),  // c ASC
                OrderPair::new(0, OrderType::Descending), // a DESC
            ],
            append_only: false,
            filter: None,
        };

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // b
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![2, 0, 3, 1]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending,  // c ASC
                OrderType::Descending, // a DESC
                OrderType::Ascending,  // _row_id ASC
            ],
            vec![0, 1, 2], // [c, a, _row_id]
        );

        let mut agg_state =
            ManagedArrayAggState::new(agg_call, None, input_pk_indices, state_table_col_mapping, 0);

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 2 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch.inc();

            let res = agg_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_int32).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some(2), Some(1)]);
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
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();

            let res = agg_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_int32).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some(2), Some(2), Some(0), Some(1)]);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_state_grouped_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
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
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![2, 1, 3, 0]));
        let mut state_table = StateTable::new_without_distribution(
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

        let mut agg_state = ManagedArrayAggState::new(
            agg_call,
            Some(&Row::new(vec![Some(8.into())])),
            input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 1 3 130 D // hide this row",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();
            epoch.inc();

            let res = agg_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some("a".to_string()), Some("b".to_string())]);
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
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            agg_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            agg_state.flush(&mut state_table)?;
            state_table.commit(epoch).await.unwrap();

            let res = agg_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                        .collect_vec();
                    assert_eq!(
                        res,
                        vec![
                            Some("a".to_string()),
                            Some("e".to_string()),
                            Some("b".to_string())
                        ]
                    );
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }
}
