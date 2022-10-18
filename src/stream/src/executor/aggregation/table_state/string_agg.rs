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

use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::for_await;
use risingwave_common::array::Row;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::{Cache, CacheKey, ManagedTableState};
use crate::common::iter_state_table;
use crate::executor::error::StreamExecutorResult;

#[derive(Clone)]
struct StringAggData {
    delim: String,
    value: String,
}

pub struct ManagedStringAggState<S: StateStore> {
    _phantom_data: PhantomData<S>,

    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// The column to aggregate in state table.
    state_table_agg_col_idx: usize,

    /// The column as delimiter in state table.
    state_table_delim_col_idx: usize,

    /// The columns to order by in state table.
    state_table_order_col_indices: Vec<usize>,

    /// In-memory all-or-nothing cache.
    cache: Cache<StringAggData>,

    /// Whether the cache is fully synced to state table.
    cache_synced: bool,

    /// Serializer for cache key.
    cache_key_serializer: OrderedRowSerde,
}

impl<S: StateStore> ManagedStringAggState<S> {
    pub fn new(
        state_table_arg_col_indices: Vec<usize>,
        state_table_order_col_indices: Vec<usize>,
        group_key: Option<&Row>,
        row_count: usize,
        cache_key_serializer: OrderedRowSerde,
    ) -> Self {
        // map agg column to state table column index
        let state_table_agg_col_idx = state_table_arg_col_indices[0];
        let state_table_delim_col_idx = state_table_arg_col_indices[1];
        Self {
            _phantom_data: PhantomData,
            group_key: group_key.cloned(),
            state_table_agg_col_idx,
            state_table_delim_col_idx,
            state_table_order_col_indices,
            cache: Cache::new(usize::MAX),
            cache_synced: row_count == 0, // if there is no row, the cache is synced initially
            cache_key_serializer,
        }
    }

    fn state_row_to_cache_key(&self, state_row: &Row) -> CacheKey {
        let mut cache_key = Vec::new();
        self.cache_key_serializer.serialize_datums(
            self.state_table_order_col_indices
                .iter()
                .map(|col_idx| &(state_row.0)[*col_idx]),
            &mut cache_key,
        );
        cache_key
    }

    fn state_row_to_cache_value(&self, state_row: &Row) -> StringAggData {
        StringAggData {
            delim: state_row[self.state_table_delim_col_idx]
                .clone()
                .map(ScalarImpl::into_utf8)
                .unwrap_or_default(),
            value: state_row[self.state_table_agg_col_idx]
                .clone()
                .map(ScalarImpl::into_utf8)
                .expect("NULL values should be filtered out"),
        }
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
                let cache_key = self.state_row_to_cache_key(state_row.as_ref());
                let cache_value = self.state_row_to_cache_value(state_row.as_ref());
                self.cache.insert(cache_key, cache_value);
            }
            self.cache_synced = true;
        }

        Ok(self.get_output_from_cache().unwrap())
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedStringAggState<S> {
    fn insert(&mut self, state_row: &Row) {
        let cache_key = self.state_row_to_cache_key(state_row);
        let cache_value = self.state_row_to_cache_value(state_row);
        if self.cache_synced {
            self.cache.insert(cache_key, cache_value);
        }
    }

    fn delete(&mut self, state_row: &Row) {
        let cache_key = self.state_row_to_cache_key(state_row);
        if self.cache_synced {
            self.cache.remove(cache_key);
        }
    }

    fn is_synced(&self) -> bool {
        self.cache_synced
    }

    fn get_output_from_cache(&self) -> Option<Datum> {
        if self.cache_synced {
            let mut result = match self.cache.first_value() {
                Some(data) => data.value.clone(),
                None => return Some(None), // return NULL if no rows to aggregate
            };
            for StringAggData { value, delim } in self.cache.iter_values().skip(1) {
                result.push_str(delim);
                result.push_str(value);
            }
            Some(Some(result.into()))
        } else {
            None
        }
    }

    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.get_output_inner(state_table).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderPair;
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::aggregation::AggArgs;

    #[tokio::test]
    async fn test_string_agg_state_simple_agg_without_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![4];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        };

        // see `LogicalAgg::infer_stream_agg_state` for the construction of state table
        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Varchar), // _delim
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![4, 0, 1]);
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![OrderType::Ascending],
            vec![0], // [_row_id]
        );

        let mut managed_state = ManagedStringAggState::new(
            &agg_call,
            None,
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        let chunk = StreamChunk::from_pretty(
            " T T i i I
            + a , 1 8 123
            + b , 5 2 128
            - b , 5 2 128
            + c , 1 3 130",
        );
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
        managed_state
            .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
            .await?;

        epoch.inc();
        state_table.commit_for_test(epoch).await.unwrap();

        let res = managed_state.get_output(&state_table).await?;
        match res {
            Some(ScalarImpl::Utf8(s)) => {
                // should be "a,c" or "c,a"
                assert_eq!(s.len(), 3);
                assert!(s.contains('a'));
                assert!(s.contains('c'));
                assert_eq!(&s[1..2], ",");
            }
            _ => panic!("unexpected output"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state_simple_agg_null_value() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![2];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        };

        // see `LogicalAgg::infer_stream_agg_state` for the construction of state table
        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Varchar), // _delim
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![2, 0, 1]);
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![OrderType::Ascending],
            vec![0], // [_row_id]
        );

        let mut managed_state = ManagedStringAggState::new(
            &agg_call,
            None,
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        let chunk = StreamChunk::from_pretty(
            " T T I
            + a 1 123
            + . 2 128
            + c . 129
            + d 4 130",
        );
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
        managed_state
            .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
            .await?;

        epoch.inc();
        state_table.commit_for_test(epoch).await.unwrap();

        let res = managed_state.get_output(&state_table).await?;
        match res {
            Some(ScalarImpl::Utf8(s)) => {
                // should be something like "ac4d"
                assert_eq!(s.len(), 4);
                assert!(s.contains('a'));
                assert!(s.contains('c'));
                assert!(s.contains('d'));
                assert!(s.contains('4'));
                assert!(!s.contains('2'));
            }
            _ => panic!("unexpected output"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state_simple_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![4];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            order_pairs: vec![
                OrderPair::new(2, OrderType::Ascending),  // b ASC
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
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar), // _delim
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![2, 0, 4, 1]);
        let mut state_table = StateTable::new_without_distribution(
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

        let mut managed_state = ManagedStringAggState::new(
            &agg_call,
            None,
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T T i i I
                + a , 1 8 123
                + b / 5 2 128
                - b / 5 2 128
                + c _ 1 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "c,a".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T T i i I
                + d - 0 8 134
                + e + 2 2 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "d_c,a+e".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state_grouped_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![4];
        let agg_call = AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            order_pairs: vec![
                OrderPair::new(2, OrderType::Ascending), // b ASC
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
            ColumnDesc::unnamed(ColumnId::new(4), DataType::Varchar), // _delim
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![3, 2, 4, 0, 1]);
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

        let mut managed_state = ManagedStringAggState::new(
            &agg_call,
            Some(&Row::new(vec![Some(8.into())])),
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();
        {
            let chunk = StreamChunk::from_pretty(
                " T T i i I
                + a _ 1 8 123
                + b _ 5 8 128
                + c _ 1 3 130 D // hide this row",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "a_b".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T T i i I
                + d , 0 2 134 D // hide this row
                + e , 2 8 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "a,e_b".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }
}
