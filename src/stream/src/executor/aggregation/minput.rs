// Copyright 2023 RisingWave Labs
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

use std::marker::PhantomData;

use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_proc_macro::EstimateSize;
use risingwave_expr::agg::{AggCall, AggKind};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::agg_state_cache::{AggStateCache, GenericAggStateCache, StateCacheInputBatch};
use super::minput_agg_impl::array_agg::ArrayAgg;
use super::minput_agg_impl::extreme::ExtremeAgg;
use super::minput_agg_impl::string_agg::StringAgg;
use crate::common::cache::{OrderedStateCache, TopNStateCache};
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;
use crate::executor::{PkIndices, StreamExecutorResult};

/// Aggregation state as a materialization of input chunks.
///
/// For example, in `string_agg`, several useful columns are picked from input chunks and
/// stored in the state table when applying chunks, and the aggregation result is calculated
/// when need to get output.
#[derive(EstimateSize)]
pub struct MaterializedInputState<S: StateStore> {
    /// Argument column indices in input chunks.
    arg_col_indices: Vec<usize>,

    /// Argument column indices in state table, group key skipped.
    state_table_arg_col_indices: Vec<usize>,

    /// The columns to order by in input chunks.
    order_col_indices: Vec<usize>,

    /// The columns to order by in state table, group key skipped.
    state_table_order_col_indices: Vec<usize>,

    /// Cache of state table.
    cache: Box<dyn AggStateCache + Send + Sync>,

    /// Serializer for cache key.
    #[estimate_size(ignore)]
    cache_key_serializer: OrderedRowSerde,

    _phantom_data: PhantomData<S>,
}

impl<S: StateStore> MaterializedInputState<S> {
    /// Create an instance from [`AggCall`].
    pub fn new(
        agg_call: &AggCall,
        pk_indices: &PkIndices,
        col_mapping: &StateTableColumnMapping,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> Self {
        let arg_col_indices = agg_call.args.val_indices().to_vec();
        let (mut order_col_indices, mut order_types) =
            if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
                // `min`/`max` need not to order by any other columns, but have to
                // order by the agg value implicitly.
                let order_type = if agg_call.kind == AggKind::Min {
                    OrderType::ascending()
                } else {
                    OrderType::descending()
                };
                (vec![arg_col_indices[0]], vec![order_type])
            } else {
                agg_call
                    .column_orders
                    .iter()
                    .map(|p| (p.column_index, p.order_type))
                    .unzip()
            };

        let pk_len = pk_indices.len();
        order_col_indices.extend(pk_indices.iter());
        order_types.extend(itertools::repeat_n(OrderType::ascending(), pk_len));

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

        let cache: Box<dyn AggStateCache + Send + Sync> = match agg_call.kind {
            AggKind::Min | AggKind::Max | AggKind::FirstValue => Box::new(
                GenericAggStateCache::new(TopNStateCache::new(extreme_cache_size), ExtremeAgg),
            ),
            AggKind::StringAgg => Box::new(GenericAggStateCache::new(
                OrderedStateCache::new(),
                StringAgg,
            )),
            AggKind::ArrayAgg => Box::new(GenericAggStateCache::new(
                OrderedStateCache::new(),
                ArrayAgg,
            )),
            _ => panic!(
                "Agg kind `{}` is not expected to have materialized input state",
                agg_call.kind
            ),
        };

        Self {
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
    pub async fn get_output(
        &mut self,
        state_table: &StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<Datum> {
        if !self.cache.is_synced() {
            let mut cache_filler = self.cache.begin_syncing();

            let all_data_iter = state_table
                .iter_with_pk_prefix(
                    &group_key,
                    PrefetchOptions {
                        exhaust_iter: cache_filler.capacity().is_none(),
                    },
                )
                .await?;
            pin_mut!(all_data_iter);

            #[for_await]
            for state_row in all_data_iter.take(cache_filler.capacity().unwrap_or(usize::MAX)) {
                let state_row: OwnedRow = state_row?;
                let cache_key = {
                    let mut cache_key = Vec::new();
                    self.cache_key_serializer.serialize(
                        state_row
                            .as_ref()
                            .project(&self.state_table_order_col_indices),
                        &mut cache_key,
                    );
                    cache_key
                };
                let cache_value = self
                    .state_table_arg_col_indices
                    .iter()
                    .map(|i| state_row[*i].as_ref().map(ScalarImpl::as_scalar_ref_impl))
                    .collect();
                cache_filler.append(cache_key, cache_value);
            }
            cache_filler.finish();
        }
        assert!(self.cache.is_synced());
        Ok(self.cache.get_output())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use rand::seq::IteratorRandom;
    use rand::Rng;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::iter_util::ZipEqFast;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_expr::agg::{AggArgs, AggCall, AggKind};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use super::MaterializedInputState;
    use crate::common::table::state_table::StateTable;
    use crate::common::StateTableColumnMapping;
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

    async fn create_mem_state_table(
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
        let mapping = StateTableColumnMapping::new(upstream_columns, None);
        let pk_len = order_types.len();
        let table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            order_types,
            (0..pk_len).collect(),
        )
        .await;
        (table, mapping)
    }

    fn create_extreme_agg_call(kind: AggKind, arg_type: DataType, arg_idx: usize) -> AggCall {
        AggCall {
            kind,
            args: AggArgs::Unary(arg_type.clone(), arg_idx),
            return_type: arg_type,
            column_orders: vec![],
            filter: None,
            distinct: false,
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
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 3],
            vec![
                OrderType::ascending(), // for AggKind::Min
                OrderType::ascending(),
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            usize::MAX,
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

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

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
                &input_pk_indices,
                &mapping,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table, group_key.as_ref()).await?;
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
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 3],
            vec![
                OrderType::descending(), // for AggKind::Max
                OrderType::ascending(),
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            usize::MAX,
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

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

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
                &input_pk_indices,
                &mapping,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table, group_key.as_ref()).await?;
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
        let group_key = None;

        let (mut table_1, mapping_1) = create_mem_state_table(
            &input_schema,
            vec![0, 3],
            vec![
                OrderType::ascending(), // for AggKind::Min
                OrderType::ascending(),
            ],
        )
        .await;
        let (mut table_2, mapping_2) = create_mem_state_table(
            &input_schema,
            vec![1, 3],
            vec![
                OrderType::descending(), // for AggKind::Max
                OrderType::ascending(),
            ],
        )
        .await;

        let mut epoch = EpochPair::new_test_epoch(1);
        table_1.init_epoch(epoch);
        table_2.init_epoch(epoch);

        let mut state_1 = MaterializedInputState::new(
            &agg_call_1,
            &input_pk_indices,
            &mapping_1,
            usize::MAX,
            &input_schema,
        );
        let mut state_2 = MaterializedInputState::new(
            &agg_call_2,
            &input_pk_indices,
            &mapping_2,
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
                .zip_eq_fast([&mut state_1, &mut state_2])
                .try_for_each(|(chunk, state)| {
                    let (ops, columns, visibility) = chunk.into_inner();
                    let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
                    state.apply_chunk(&ops, visibility.as_ref(), &columns)
                })?;

            epoch.inc();
            table_1.commit(epoch).await.unwrap();
            table_2.commit(epoch).await.unwrap();

            match state_1.get_output(&table_1, group_key.as_ref()).await? {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s.as_ref(), "a");
                }
                _ => panic!("unexpected output"),
            }
            match state_2.get_output(&table_2, group_key.as_ref()).await? {
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
        let group_key = Some(OwnedRow::new(vec![Some(8.into())]));

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 1, 3],
            vec![
                OrderType::ascending(),  // c ASC
                OrderType::descending(), // b DESC for AggKind::Max
                OrderType::ascending(),  // _row_id ASC
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            usize::MAX,
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 7 3 130 D // hide this row",
                &mut table,
                &mapping,
            );

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            let (ops, columns, visibility) = chunk.into_inner();
            let columns: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            state.apply_chunk(&ops, visibility.as_ref(), &columns)?;

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
                &input_pk_indices,
                &mapping,
                usize::MAX,
                &input_schema,
            );
            let res = state.get_output(&table, group_key.as_ref()).await?;
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
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![0, 1],
            vec![
                OrderType::ascending(), // for AggKind::Min
                OrderType::ascending(),
            ],
        )
        .await;

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![0, 1],
            vec![
                OrderType::ascending(), // for AggKind::Min
                OrderType::ascending(),
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            3, // cache capacity = 3 for easy testing
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
            column_orders: vec![
                ColumnOrder::new(2, OrderType::ascending()),  // b ASC
                ColumnOrder::new(0, OrderType::descending()), // a DESC
            ],
            filter: None,
            distinct: false,
        };
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 0, 4, 1],
            vec![
                OrderType::ascending(),  // b ASC
                OrderType::descending(), // a DESC
                OrderType::ascending(),  // _row_id ASC
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            usize::MAX,
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);

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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s.as_ref(), "c,a".to_string());
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
            match res {
                Some(ScalarImpl::Utf8(s)) => {
                    assert_eq!(s.as_ref(), "d_c,a+e".to_string());
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
            column_orders: vec![
                ColumnOrder::new(2, OrderType::ascending()),  // c ASC
                ColumnOrder::new(0, OrderType::descending()), // a DESC
            ],
            filter: None,
            distinct: false,
        };
        let group_key = None;

        let (mut table, mapping) = create_mem_state_table(
            &input_schema,
            vec![2, 0, 3, 1],
            vec![
                OrderType::ascending(),  // c ASC
                OrderType::descending(), // a DESC
                OrderType::ascending(),  // _row_id ASC
            ],
        )
        .await;

        let mut state = MaterializedInputState::new(
            &agg_call,
            &input_pk_indices,
            &mapping,
            usize::MAX,
            &input_schema,
        );

        let mut epoch = EpochPair::new_test_epoch(1);
        table.init_epoch(epoch);
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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

            epoch.inc();
            table.commit(epoch).await.unwrap();

            let res = state.get_output(&table, group_key.as_ref()).await?;
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
