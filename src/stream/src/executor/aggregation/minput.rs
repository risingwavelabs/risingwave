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

use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Op, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Datum, DatumRef, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;
use smallvec::SmallVec;

use super::state_cache::array_agg::ArrayAgg;
use super::state_cache::extreme::ExtremeAgg;
use super::state_cache::string_agg::StringAgg;
use super::state_cache::{CacheKey, GenericStateCache, StateCache};
use super::AggCall;
use crate::common::{iter_state_table, StateTableColumnMapping};
use crate::executor::{PkIndices, StreamExecutorResult};

/// Aggregation state as a materialization of input chunks.
///
/// For example, in `string_agg`, several useful columns are picked from input chunks and
/// stored in the state table when applying chunks, and the aggregation result is calculated
/// when need to get output.
pub struct MaterializedInputState<S: StateStore> {
    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Argument column indices in input chunks.
    arg_col_indices: Vec<usize>,

    /// Argument column indices in state table.
    state_table_arg_col_indices: Vec<usize>,

    /// The columns to order by in input chunks.
    order_col_indices: Vec<usize>,

    /// The columns to order by in state table.
    state_table_order_col_indices: Vec<usize>,

    /// Cache of state table.
    cache: Box<dyn StateCache>,

    /// Serializer for cache key.
    cache_key_serializer: OrderedRowSerde,

    _phantom_data: PhantomData<S>,
}

impl<S: StateStore> MaterializedInputState<S> {
    /// Create an instance from [`AggCall`].
    pub fn new(
        agg_call: &AggCall,
        group_key: Option<&Row>,
        pk_indices: &PkIndices,
        col_mapping: &StateTableColumnMapping,
        row_count: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> Self {
        let arg_col_indices = agg_call.args.val_indices().to_vec();
        let (mut order_col_indices, mut order_types) =
            if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
                // `min`/`max` need not to order by any other columns, but have to
                // order by the agg value implicitly.
                let order_type = if agg_call.kind == AggKind::Min {
                    OrderType::Ascending
                } else {
                    OrderType::Descending
                };
                (vec![arg_col_indices[0]], vec![order_type])
            } else {
                agg_call
                    .order_pairs
                    .iter()
                    .map(|p| (p.column_idx, p.order_type))
                    .unzip()
            };

        let pk_len = pk_indices.len();
        order_col_indices.reserve_exact(pk_len);
        order_col_indices.extend(pk_indices.iter());
        order_types.reserve_exact(pk_len);
        order_types.extend(std::iter::repeat(OrderType::Ascending).take(pk_len));

        // map argument columns to state table column indices
        let state_table_arg_col_indices = arg_col_indices
            .iter()
            .map(|i| {
                col_mapping
                    .upstream_to_state_table(*i)
                    .expect("the argument columns must appear in the state table")
            })
            .collect_vec();

        // map order by columns to state table column indices
        let state_table_order_col_indices = order_col_indices
            .iter()
            .map(|i| {
                col_mapping
                    .upstream_to_state_table(*i)
                    .expect("the order columns must appear in the state table")
            })
            .collect_vec();

        let cache_key_data_types = order_col_indices
            .iter()
            .map(|i| input_schema[*i].data_type())
            .collect_vec();
        let cache_key_serializer = OrderedRowSerde::new(cache_key_data_types, order_types);

        let cache_capacity = if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
            extreme_cache_size
        } else {
            usize::MAX
        };

        let cache: Box<dyn StateCache> =
            match agg_call.kind {
                AggKind::Min | AggKind::Max | AggKind::FirstValue => Box::new(
                    GenericStateCache::new(ExtremeAgg, cache_capacity, row_count),
                ),
                AggKind::StringAgg => {
                    Box::new(GenericStateCache::new(StringAgg, cache_capacity, row_count))
                }
                AggKind::ArrayAgg => {
                    Box::new(GenericStateCache::new(ArrayAgg, cache_capacity, row_count))
                }
                _ => panic!(
                    "Agg kind `{}` is not expected to have materialized input state",
                    agg_call.kind
                ),
            };

        Self {
            group_key: group_key.cloned(),
            arg_col_indices,
            state_table_arg_col_indices,
            order_col_indices,
            state_table_order_col_indices,
            cache,
            cache_key_serializer,
            _phantom_data: PhantomData,
        }
    }

    /// Apply a chunk of data to the state cache.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        self.cache.apply_batch(StateCacheInputBatch::new(
            ops,
            visibility,
            columns,
            &self.cache_key_serializer,
            &self.arg_col_indices,
            &self.order_col_indices,
        ));
        Ok(())
    }

    /// Get the output of the state.
    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        if !self.cache.is_synced() {
            let all_data_iter = iter_state_table(state_table, self.group_key.as_ref()).await?;
            pin_mut!(all_data_iter);

            let mut cache_filler = self.cache.begin_syncing();
            #[for_await]
            for state_row in all_data_iter.take(cache_filler.capacity()) {
                let state_row = state_row?;
                let cache_key = {
                    let mut cache_key = Vec::new();
                    self.cache_key_serializer.serialize_datums(
                        self.state_table_order_col_indices
                            .iter()
                            .map(|col_idx| &(state_row.0)[*col_idx]),
                        &mut cache_key,
                    );
                    cache_key
                };
                let cache_value = self
                    .state_table_arg_col_indices
                    .iter()
                    .map(|i| state_row[*i].as_ref().map(ScalarImpl::as_scalar_ref_impl))
                    .collect();
                cache_filler.insert(cache_key, cache_value);
            }
        }
        debug_assert!(self.cache.is_synced());
        Ok(self.cache.get_output())
    }
}

pub struct StateCacheInputBatch<'a> {
    idx: usize,
    ops: Ops<'a>,
    visibility: Option<&'a Bitmap>,
    columns: &'a [&'a ArrayImpl],
    cache_key_serializer: &'a OrderedRowSerde,
    arg_col_indices: &'a [usize],
    order_col_indices: &'a [usize],
}

impl<'a> StateCacheInputBatch<'a> {
    fn new(
        ops: Ops<'a>,
        visibility: Option<&'a Bitmap>,
        columns: &'a [&'a ArrayImpl],
        cache_key_serializer: &'a OrderedRowSerde,
        arg_col_indices: &'a [usize],
        order_col_indices: &'a [usize],
    ) -> Self {
        let first_idx = visibility.map_or(0, |v| v.next_set_bit(0).unwrap_or(ops.len()));
        Self {
            idx: first_idx,
            ops,
            visibility,
            columns,
            cache_key_serializer,
            arg_col_indices,
            order_col_indices,
        }
    }
}

impl<'a> Iterator for StateCacheInputBatch<'a> {
    type Item = (Op, CacheKey, SmallVec<[DatumRef<'a>; 2]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.ops.len() {
            None
        } else {
            let op = self.ops[self.idx];
            let key = {
                let mut key = Vec::new();
                self.cache_key_serializer.serialize_datum_refs(
                    self.order_col_indices
                        .iter()
                        .map(|col_idx| self.columns[*col_idx].value_at(self.idx)),
                    &mut key,
                );
                key
            };
            let value = self
                .arg_col_indices
                .iter()
                .map(|col_idx| self.columns[*col_idx].value_at(self.idx))
                .collect();
            self.idx = self.visibility.map_or(self.idx + 1, |v| {
                v.next_set_bit(self.idx + 1).unwrap_or(self.ops.len())
            });
            Some((op, key, value))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use rand::seq::IteratorRandom;
    use rand::Rng;
    use risingwave_common::array::{Row, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;
    use risingwave_storage::StateStore;

    use super::MaterializedInputState;
    use crate::common::StateTableColumnMapping;
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::StreamExecutorResult;

    fn create_chunk<S: StateStore>(
        pretty: &str,
        table: &mut StateTable<S>,
        col_mapping: &StateTableColumnMapping,
    ) -> StreamChunk {
        let chunk = StreamChunk::from_pretty(pretty);
        table.write_chunk(StreamChunk::new(
            chunk.ops().to_vec(),
            col_mapping
                .upstream_columns()
                .iter()
                .map(|col_idx| chunk.columns()[*col_idx].clone())
                .collect(),
            chunk.visibility().cloned(),
        ));
        chunk
    }

    fn create_mem_state_table(
        input_schema: &Schema,
        upstream_columns: Vec<usize>,
        order_types: Vec<OrderType>,
    ) -> (StateTable<MemoryStateStore>, StateTableColumnMapping) {
        // see `LogicalAgg::infer_stream_agg_state` for the construction of state table
        let table_id = TableId::new(rand::thread_rng().gen());
        let columns = upstream_columns
            .iter()
            .map(|col_idx| input_schema[*col_idx].data_type())
            .enumerate()
            .map(|(i, data_type)| ColumnDesc::unnamed(ColumnId::new(i as i32), data_type))
            .collect_vec();
        let mapping = StateTableColumnMapping::new(upstream_columns);
        let pk_len = order_types.len();
        let table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            order_types,
            (0..pk_len).collect(),
        );
        (table, mapping)
    }

    fn create_extreme_agg_call(kind: AggKind, arg_type: DataType, arg_idx: usize) -> AggCall {
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
    async fn test_extreme_agg_state_basic_min() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call = create_extreme_agg_call(AggKind::Min, DataType::Int32, 2); // min(c)

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 3],
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
        );

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            usize::MAX,
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        let mut row_count = 0;

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130",
                &mut table,
                &mapping,
            );
            row_count += 2;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 3);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );
            row_count += 2;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 2);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                &agg_call,
                None,
                &input_pk_indices,
                &mapping,
                row_count,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table).await?;
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
    async fn test_extreme_agg_state_basic_max() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);
        let agg_call = create_extreme_agg_call(AggKind::Max, DataType::Int32, 2); // max(c)

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 3],
            vec![
                OrderType::Descending, // for AggKind::Max
                OrderType::Ascending,
            ],
        );

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            usize::MAX,
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        let mut row_count = 0;

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130",
                &mut table,
                &mapping,
            );
            row_count += 2;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 8);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 0 9 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );
            row_count += 2;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 9);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                &agg_call,
                None,
                &input_pk_indices,
                &mapping,
                row_count,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table).await?;
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
    async fn test_extreme_agg_state_with_hidden_input() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3]; // _row_id
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);
        let agg_call_1 = create_extreme_agg_call(AggKind::Min, DataType::Varchar, 0); // min(a)
        let agg_call_2 = create_extreme_agg_call(AggKind::Max, DataType::Varchar, 1); // max(b)

        let (mut table_1, mapping_1) = create_mem_state_table(
            &input_schema,
            vec![0, 3],
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
        );
        let (mut table_2, mapping_2) = create_mem_state_table(
            &input_schema,
            vec![1, 3],
            vec![
                OrderType::Descending, // for AggKind::Max
                OrderType::Ascending,
            ],
        );

        let epoch = EpochPair::new_test_epoch(1);
        table_1.init_epoch(epoch);
        table_2.init_epoch(epoch);
        epoch.inc();

        let mut state_1 = MaterializedInputState::new(
            &agg_call_1,
            None,
            &input_pk_indices,
            &mapping_1,
            0,
            usize::MAX,
            &input_schema,
        );
        let mut state_2 = MaterializedInputState::new(
            &agg_call_2,
            None,
            &input_pk_indices,
            &mapping_2,
            0,
            usize::MAX,
            &input_schema,
        );

        {
            let chunk_1 = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130
                + . 9 4 131 D
                + . 6 5 132 D
                + c . 3 133",
                &mut table_1,
                &mapping_1,
            );
            let chunk_2 = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 1 3 130
                + . 9 4 131
                + . 6 5 132
                + c . 3 133 D",
                &mut table_2,
                &mapping_2,
            );

            [chunk_1, chunk_2]
                .into_iter()
                .zip_eq([&mut state_1, &mut state_2])
                .try_for_each(|(chunk, state)| {
                    let (ops, columns, visibility) = chunk.into_inner();
                    let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
                    state.apply_chunk(&ops, visibility.as_ref(), &columns)
                })?;

            table_1.commit_for_test(epoch).await.unwrap();
            table_2.commit_for_test(epoch).await.unwrap();

            match state_1.get_output(&table_1).await? {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(&s, "a");
                }
                _ => panic!("unexpected output"),
            }
            match state_2.get_output(&table_2).await? {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 9);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_grouped() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);
        let agg_call = create_extreme_agg_call(AggKind::Max, DataType::Int32, 1); // max(b)

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 1, 3],
            vec![
                OrderType::Ascending,  // c ASC
                OrderType::Descending, // b DESC for AggKind::Max
                OrderType::Ascending,  // _row_id ASC
            ],
        );
        let group_key = Row::new(vec![Some(8.into())]);

        let mut state = MaterializedInputState::new(
            &agg_call,
            Some(&group_key),
            &input_pk_indices,
            &mapping,
            0,
            usize::MAX,
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        let mut row_count = 0;

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 7 3 130 D // hide this row",
                &mut table,
                &mapping,
            );
            row_count += 2;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 5);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 9 2 134 D // hide this row
                + e 8 8 137",
                &mut table,
                &mapping,
            );
            row_count += 1;

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 8);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                &agg_call,
                Some(&group_key),
                &input_pk_indices,
                &mapping,
                row_count,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table).await?;
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
    async fn test_extreme_agg_state_with_random_values() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let input_pk_indices = vec![1]; // _row_id
        let field1 = Field::unnamed(DataType::Int32);
        let field2 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2]);
        let agg_call = create_extreme_agg_call(AggKind::Min, DataType::Int32, 0); // min(a)

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![0, 1],
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            1024,
            &input_schema,
        );

        let mut rng = rand::thread_rng();
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

            let chunk = create_chunk(&pretty_lines.join("\n"), &mut table, &mapping);
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
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

            let chunk = create_chunk(&pretty_lines.join("\n"), &mut table, &mapping);
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
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
    async fn test_extreme_agg_state_cache_maintenance() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let input_pk_indices = vec![1]; // _row_id
        let field1 = Field::unnamed(DataType::Int32);
        let field2 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2]);
        let agg_call = create_extreme_agg_call(AggKind::Min, DataType::Int32, 0); // min(a)

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![0, 1],
            vec![
                OrderType::Ascending, // for AggKind::Min
                OrderType::Ascending,
            ],
        );

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            3, // cache capacity = 3 for easy testing
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = create_chunk(
                " i  I
                + 4  123
                + 8  128
                + 12 129",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 4);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " i I
                + 9  130 // this will evict 12
                - 9  130
                + 13 128
                - 4  123
                - 8  128",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 12);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " i  I
                + 1  131
                + 2  132
                + 3  133 // evict all from cache
                - 1  131
                - 2  132
                - 3  133
                + 14 134",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Int32(s)) => {
                    assert_eq!(s, 12);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![4];
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Varchar);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int32);
        let field5 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4, field5]);

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

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 0, 4, 1],
            vec![
                OrderType::Ascending,  // _row_id ASC
                OrderType::Descending, // a DESC
                OrderType::Ascending,  // b ASC
            ],
        );

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            usize::MAX,
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = create_chunk(
                " T T i i I
                + a , 1 8 123
                + b / 5 2 128
                - b / 5 2 128
                + c _ 1 3 130",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s, "c,a".to_string());
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = create_chunk(
                " T T i i I
                + d - 0 8 134
                + e + 2 2 137",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
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
    async fn test_array_agg_state() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

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

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 0, 3, 1],
            vec![
                OrderType::Ascending,  // c ASC
                OrderType::Descending, // a DESC
                OrderType::Ascending,  // _row_id ASC
            ],
        );

        let mut state = MaterializedInputState::new(
            &agg_call,
            None,
            &input_pk_indices,
            &mapping,
            0,
            usize::MAX,
            &input_schema,
        );

        let epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 2 3 130",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = state.get_output(&table).await?;
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
            let chunk = create_chunk(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            table.commit_for_test(epoch).await.unwrap();

            let res = state.get_output(&table).await?;
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
}
