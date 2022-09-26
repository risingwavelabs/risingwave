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
use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::*;
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::Cache;
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::AggCall;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::iter_state_table;
use crate::executor::PkIndices;

/// Memcomparable row.
type CacheKey = Vec<u8>;

/// Generic managed agg state for min/max.
/// It maintains a top N cache internally, using `HashSet`, and the sort key
/// is composed of (agg input value, upstream pk).
pub struct GenericExtremeState<S: StateStore> {
    _phantom_data: PhantomData<S>,

    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Contains the column mapping between upstream schema and state table.
    state_table_col_mapping: Arc<StateTableColumnMapping>,

    // The column to aggregate in input chunk.
    upstream_agg_col_idx: usize,

    /// The column to aggregate in state table.
    state_table_agg_col_idx: usize,

    /// The columns to order by in state table.
    state_table_order_col_indices: Vec<usize>,

    /// Number of all items in the state store.
    total_count: usize,

    /// Cache for the top N elements in the state. Note that the cache
    /// won't store group_key so the column indices should be offsetted
    /// by group_key.len(), which is handled by `state_row_to_cache_row`.
    cache: Cache<CacheKey, Datum>,

    /// Whether the cache is synced to state table. The cache is synced iff:
    /// - the cache is empty and `total_count` is 0, or
    /// - the cache is not empty and elements in it are the top ones in the state table.
    cache_synced: bool,

    /// Serializer for cache key.
    cache_key_serializer: OrderedRowSerializer,
}

/// A trait over all table-structured states.
///
/// It is true that this interface also fits to value managed state, but we won't implement
/// `ManagedTableState` for them. We want to reduce the overhead of `BoxedFuture`. For
/// `ManagedValueState`, we can directly forward its async functions to `ManagedStateImpl`, instead
/// of adding a layer of indirection caused by async traits.
#[async_trait]
pub trait ManagedTableState<S: StateStore>: Send + Sync + 'static {
    async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum>;

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool;

    /// Flush the internal state to a write batch.
    fn flush(&mut self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()>;
}

impl<S: StateStore> GenericExtremeState<S> {
    /// Create a managed extreme state. If `cache_capacity` is `None`, the cache will be
    /// fully synced, otherwise it will only retain top entries.
    pub fn new(
        agg_call: AggCall,
        group_key: Option<&Row>,
        pk_indices: PkIndices,
        col_mapping: Arc<StateTableColumnMapping>,
        row_count: usize,
        cache_capacity: usize,
    ) -> Self {
        let upstream_agg_col_idx = agg_call.args.val_indices()[0];
        // map agg column to state table column index
        let state_table_agg_col_idx = col_mapping
            .upstream_to_state_table(agg_call.args.val_indices()[0])
            .expect("the column to be aggregate must appear in the state table");
        // map order by columns to state table column indices
        let (state_table_order_col_indices, state_table_order_types) = std::iter::once((
            state_table_agg_col_idx,
            match agg_call.kind {
                AggKind::Min => OrderType::Ascending,
                AggKind::Max => OrderType::Descending,
                _ => unreachable!(),
            },
        ))
        .chain(pk_indices.iter().map(|idx| {
            (
                col_mapping
                    .upstream_to_state_table(*idx)
                    .expect("the pk columns must appear in the state table"),
                OrderType::Ascending,
            )
        }))
        .unzip();
        let cache_key_serializer = OrderedRowSerializer::new(state_table_order_types);

        Self {
            _phantom_data: PhantomData,
            group_key: group_key.cloned(),
            state_table_col_mapping: col_mapping,
            upstream_agg_col_idx,
            state_table_agg_col_idx,
            state_table_order_col_indices,
            total_count: row_count,
            cache: Cache::new(cache_capacity),
            cache_synced: row_count == 0, // if there is no row, the cache is synced initially
            cache_key_serializer,
        }
    }

    fn state_row_to_cache_entry(&self, state_row: &Row) -> StreamExecutorResult<(Vec<u8>, Datum)> {
        let mut cache_key = Vec::new();
        self.cache_key_serializer.serialize_datums(
            self.state_table_order_col_indices
                .iter()
                .map(|col_idx| &(state_row.0)[*col_idx]),
            &mut cache_key,
        );
        let cache_data = state_row[self.state_table_agg_col_idx].clone();
        Ok((cache_key, cache_data))
    }

    /// Apply a chunk of data to the state.
    fn apply_chunk_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, columns));

        for (i, op) in ops
            .iter()
            .enumerate()
            .filter(|(i, _)| visibility.map(|x| x.is_set(*i)).unwrap_or(true))
            .filter(|(i, _)| columns[self.upstream_agg_col_idx].null_bitmap().is_set(*i))
        {
            let state_row = Row::new(
                self.state_table_col_mapping
                    .upstream_columns()
                    .iter()
                    .map(|col_idx| columns[*col_idx].datum_at(i))
                    .collect(),
            );
            let (cache_key, cache_data) = self.state_row_to_cache_entry(&state_row)?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    if self.cache_synced
                        && (self.cache.len() == self.total_count
                            || &cache_key < self.cache.last_key().unwrap())
                    {
                        self.cache.insert(cache_key, cache_data);
                    }
                    state_table.insert(state_row);
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    if self.cache_synced {
                        self.cache.remove(cache_key);
                        if self.total_count > 1 /* still has rows after deletion */ && self.cache.is_empty()
                        {
                            self.cache_synced = false;
                        }
                    }
                    state_table.delete(state_row);
                    self.total_count -= 1;
                }
            }
        }

        Ok(())
    }

    fn get_output_from_cache(&self) -> Option<Datum> {
        if self.cache_synced {
            self.cache.first_value().cloned()
        } else {
            None
        }
    }

    async fn get_output_inner(
        &mut self,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        // try to get the result from cache
        if let Some(datum) = self.get_output_from_cache() {
            Ok(datum)
        } else {
            // read from state table and fill in the cache
            let all_data_iter = iter_state_table(state_table, self.group_key.as_ref()).await?;
            pin_mut!(all_data_iter);

            self.cache.clear();
            #[for_await]
            for state_row in all_data_iter.take(self.cache.capacity()) {
                let state_row = state_row?;
                let (cache_key, cache_data) = self.state_row_to_cache_entry(state_row.as_ref())?;
                self.cache.insert(cache_key, cache_data);
            }
            self.cache_synced = true;

            // try to get the result from cache again
            Ok(self.get_output_from_cache().unwrap_or(None))
        }
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for GenericExtremeState<S> {
    async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.apply_chunk_inner(ops, visibility, columns, state_table)
    }

    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.get_output_inner(state_table).await
    }

    /// Check if this state needs a flush.
    /// TODO: Remove this. #4035
    fn is_dirty(&self) -> bool {
        unreachable!("Should not call this function anymore, check state table for dirty data");
    }

    /// TODO: Remove this. #4035
    fn flush(&mut self, _state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::aggregation::AggArgs;

    fn create_agg_call(kind: AggKind, arg_type: DataType, arg_idx: usize) -> AggCall {
        AggCall {
            kind,
            args: AggArgs::Unary(arg_type.clone(), arg_idx),
            return_type: arg_type,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_extreme_state_basic_min() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let agg_call = create_agg_call(AggKind::Min, DataType::Int32, 2); // min(c)

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(0x2333);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![2, 3]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
            vec![0, 1], // [c, _row_id]
        );

        let mut managed_state = GenericExtremeState::new(
            agg_call.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping.clone(),
            0,
            usize::MAX,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 3);
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
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 2);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let row_count = managed_state.total_count;
            let mut managed_state = GenericExtremeState::new(
                agg_call,
                None,
                input_pk_indices,
                state_table_col_mapping,
                row_count,
                usize::MAX,
            );
            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 2);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_state_basic_max() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let agg_call = create_agg_call(AggKind::Max, DataType::Int32, 2); // max(c)

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(0x2333);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![2, 3]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Descending, // for AggKind::Max
                OrderType::Ascending,
            ],
            vec![0, 1], // [c, _row_id]
        );

        let mut managed_state = GenericExtremeState::new(
            agg_call.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping.clone(),
            0,
            usize::MAX,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 8);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 0 9 134
                + e 2 2 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 9);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let row_count = managed_state.total_count;
            let mut managed_state = GenericExtremeState::new(
                agg_call,
                None,
                input_pk_indices,
                state_table_col_mapping,
                row_count,
                usize::MAX,
            );
            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 9);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_state_with_null_value() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let agg_call_1 = create_agg_call(AggKind::Min, DataType::Varchar, 0); // min(a)
        let agg_call_2 = create_agg_call(AggKind::Max, DataType::Varchar, 1); // max(b)

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(0x6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),   // _row_id
        ];
        let state_table_col_mapping_1 = Arc::new(StateTableColumnMapping::new(vec![0, 3]));
        let mut state_table_1 = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
            vec![0, 1], // [b, _row_id]
        );
        let table_id = TableId::new(0x2333);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // b
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping_2 = Arc::new(StateTableColumnMapping::new(vec![1, 3]));
        let mut state_table_2 = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Descending, // for AggKind::Max
                OrderType::Ascending,
            ],
            vec![0, 1], // [b, _row_id]
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table_1.init_epoch(epoch);
        state_table_2.init_epoch(epoch);
        epoch.inc();

        let mut managed_state_1 = GenericExtremeState::new(
            agg_call_1.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping_1.clone(),
            0,
            usize::MAX,
        );
        let mut managed_state_2 = GenericExtremeState::new(
            agg_call_2.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping_2.clone(),
            0,
            usize::MAX,
        );

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130
                + . 9 4 131
                + . 6 5 132
                + c . 3 133",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state_1
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table_1)
                .await?;
            managed_state_2
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table_2)
                .await?;

            managed_state_1.flush(&mut state_table_1)?;
            managed_state_2.flush(&mut state_table_2)?;
            state_table_1.commit_for_test(epoch).await.unwrap();
            state_table_2.commit_for_test(epoch).await.unwrap();

            match managed_state_1.get_output(&state_table_1).await? {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(&s, "a");
                }
                _ => panic!("unexpected output"),
            }
            match managed_state_2.get_output(&state_table_2).await? {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 9);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_state_grouped() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let agg_call = create_agg_call(AggKind::Max, DataType::Int32, 1); // max(b)

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // group by c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32), // b
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![2, 1, 3]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending,  // c ASC
                OrderType::Descending, // b DESC for AggKind::Max
                OrderType::Ascending,  // _row_id ASC
            ],
            vec![0, 1, 2], // [c, b, _row_id]
        );
        let group_key = Row::new(vec![Some(8.into())]);

        let mut managed_state = GenericExtremeState::new(
            agg_call.clone(),
            Some(&group_key),
            input_pk_indices.clone(),
            state_table_col_mapping.clone(),
            0,
            usize::MAX,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 7 3 130 D // hide this row",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 5);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 9 2 134 D // hide this row
                + e 8 8 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 8);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let row_count = managed_state.total_count;
            let mut managed_state = GenericExtremeState::new(
                agg_call,
                Some(&group_key),
                input_pk_indices,
                state_table_col_mapping,
                row_count,
                usize::MAX,
            );
            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 8);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_state_with_random_values() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let input_pk_indices = vec![1]; // _row_id
        let agg_call = create_agg_call(AggKind::Min, DataType::Int32, 0); // min(a)

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(0x2333);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // a
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![0, 1]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
            vec![0, 1], // [a, _row_id]
        );
        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();
        let mut managed_state = GenericExtremeState::new(
            agg_call.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping.clone(),
            0,
            1024,
        );

        let mut rng = thread_rng();
        let insert_values: Vec<i32> = (0..10000).map(|_| rng.gen()).collect_vec();
        let delete_values: HashSet<_> = insert_values
            .iter()
            .choose_multiple(&mut rng, 1000)
            .into_iter()
            .collect();
        let mut min_value = i32::MAX;

        {
            let mut pretty_lines = vec!["i I".to_string()];
            for (row_id, value) in insert_values
                .iter()
                .enumerate()
                .take(insert_values.len() / 2)
            {
                pretty_lines.push(format!("+ {} {}", value, row_id));
                if delete_values.contains(&value) {
                    pretty_lines.push(format!("- {} {}", value, row_id));
                    continue;
                }
                if *value < min_value {
                    min_value = *value;
                }
            }

            let chunk = StreamChunk::from_pretty(&pretty_lines.join("\n"));
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, min_value);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let mut pretty_lines = vec!["i I".to_string()];
            for (row_id, value) in insert_values
                .iter()
                .enumerate()
                .skip(insert_values.len() / 2)
            {
                pretty_lines.push(format!("+ {} {}", value, row_id));
                if delete_values.contains(&value) {
                    pretty_lines.push(format!("- {} {}", value, row_id));
                    continue;
                }
                if *value < min_value {
                    min_value = *value;
                }
            }

            let chunk = StreamChunk::from_pretty(&pretty_lines.join("\n"));
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, min_value);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_state_cache_maintenance() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let input_pk_indices = vec![1]; // _row_id
        let agg_call = create_agg_call(AggKind::Min, DataType::Int32, 0); // min(a)

        // see `LogicalAgg::infer_internal_table_catalog` for the construction of state table
        let table_id = TableId::new(0x2333);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // a
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64), // _row_id
        ];
        let state_table_col_mapping = Arc::new(StateTableColumnMapping::new(vec![0, 1]));
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
            vec![0, 1], // [a, _row_id]
        );

        let mut managed_state = GenericExtremeState::new(
            agg_call.clone(),
            None,
            input_pk_indices.clone(),
            state_table_col_mapping.clone(),
            0,
            3, // cache capacity = 3 for easy testing
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " i  I
                + 4  123
                + 8  128
                + 12 129",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 4);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " i I
                + 9  130 // this will evict 12
                - 9  130
                + 13 128
                - 4  123
                - 8  128",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 12);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " i  I
                + 1  131
                + 2  132
                + 3  133 // evict all from cache
                - 1  131
                - 2  132
                - 3  133
                + 14 134",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            managed_state.flush(&mut state_table)?;
            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 12);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }
}
