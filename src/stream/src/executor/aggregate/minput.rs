// Copyright 2025 RisingWave Labs
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

use std::ops::Bound::{self};

use futures::{StreamExt, pin_mut};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::types::Datum;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggType, BoxedAggregateFunction, PbAggKind};
use risingwave_pb::stream_plan::PbAggNodeVersion;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::agg_group::{AggStateCacheStats, GroupKey};
use super::agg_state_cache::{AggStateCache, GenericAggStateCache};
use crate::common::StateTableColumnMapping;
use crate::common::state_cache::{OrderedStateCache, TopNStateCache};
use crate::common::table::state_table::StateTable;
use crate::executor::{PkIndices, StreamExecutorResult};

/// Aggregation state as a materialization of input chunks.
///
/// For example, in `string_agg`, several useful columns are picked from input chunks and
/// stored in the state table when applying chunks, and the aggregation result is calculated
/// when need to get output.
#[derive(EstimateSize)]
pub struct MaterializedInputState {
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

    /// Whether to output the first value from cache.
    output_first_value: bool,

    /// Serializer for cache key.
    #[estimate_size(ignore)]
    cache_key_serializer: OrderedRowSerde,
}

impl MaterializedInputState {
    /// Create an instance from [`AggCall`].
    pub fn new(
        version: PbAggNodeVersion,
        agg_call: &AggCall,
        pk_indices: &PkIndices,
        order_columns: &[ColumnOrder],
        col_mapping: &StateTableColumnMapping,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        if agg_call.distinct && version < PbAggNodeVersion::Issue12140 {
            panic!(
                "RisingWave versions before issue #12140 is resolved has critical bug, you must re-create current MV to ensure correctness."
            );
        }

        let arg_col_indices = agg_call.args.val_indices().to_vec();

        let (order_col_indices, order_types) = if version < PbAggNodeVersion::Issue13465 {
            generate_order_columns_before_version_issue_13465(
                agg_call,
                pk_indices,
                &arg_col_indices,
            )
        } else {
            order_columns
                .iter()
                .map(|o| (o.column_index, o.order_type))
                .unzip()
        };

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

        let cache: Box<dyn AggStateCache + Send + Sync> = match agg_call.agg_type {
            AggType::Builtin(
                PbAggKind::Min | PbAggKind::Max | PbAggKind::FirstValue | PbAggKind::LastValue,
            ) => Box::new(GenericAggStateCache::new(
                TopNStateCache::new(extreme_cache_size),
                agg_call.args.arg_types(),
            )),
            AggType::Builtin(
                PbAggKind::StringAgg
                | PbAggKind::ArrayAgg
                | PbAggKind::JsonbAgg
                | PbAggKind::JsonbObjectAgg,
            )
            | AggType::WrapScalar(_) => Box::new(GenericAggStateCache::new(
                OrderedStateCache::new(),
                agg_call.args.arg_types(),
            )),
            _ => panic!(
                "Agg type `{}` is not expected to have materialized input state",
                agg_call.agg_type
            ),
        };
        let output_first_value = matches!(
            agg_call.agg_type,
            AggType::Builtin(
                PbAggKind::Min | PbAggKind::Max | PbAggKind::FirstValue | PbAggKind::LastValue
            )
        );

        Ok(Self {
            arg_col_indices,
            state_table_arg_col_indices,
            order_col_indices,
            state_table_order_col_indices,
            cache,
            output_first_value,
            cache_key_serializer,
        })
    }

    /// Apply a chunk of data to the state cache.
    /// This method should never involve any state table operations.
    pub fn apply_chunk(&mut self, chunk: &StreamChunk) -> StreamExecutorResult<()> {
        self.cache.apply_batch(
            chunk,
            &self.cache_key_serializer,
            &self.arg_col_indices,
            &self.order_col_indices,
        );
        Ok(())
    }

    /// Get the output of the state.
    /// We may need to read from the state table into the cache to get the output.
    pub async fn get_output(
        &mut self,
        state_table: &StateTable<impl StateStore>,
        group_key: Option<&GroupKey>,
        func: &BoxedAggregateFunction,
    ) -> StreamExecutorResult<(Datum, AggStateCacheStats)> {
        let mut stats = AggStateCacheStats::default();
        stats.agg_state_cache_lookup_count += 1;

        if !self.cache.is_synced() {
            stats.agg_state_cache_miss_count += 1;

            let mut cache_filler = self.cache.begin_syncing();
            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let all_data_iter = state_table
                .iter_with_prefix(
                    group_key.map(GroupKey::table_pk),
                    sub_range,
                    PrefetchOptions {
                        prefetch: cache_filler.capacity().is_none(),
                        for_large_query: false,
                    },
                )
                .await?;
            pin_mut!(all_data_iter);

            #[for_await]
            for keyed_row in all_data_iter.take(cache_filler.capacity().unwrap_or(usize::MAX)) {
                let state_row = keyed_row?;
                let cache_key = {
                    let mut cache_key = Vec::new();
                    self.cache_key_serializer.serialize(
                        state_row
                            .as_ref()
                            .project(&self.state_table_order_col_indices),
                        &mut cache_key,
                    );
                    cache_key.into()
                };
                let cache_value = self
                    .state_table_arg_col_indices
                    .iter()
                    .map(|i| state_row[*i].clone())
                    .collect();
                cache_filler.append(cache_key, cache_value);
            }
            cache_filler.finish();
        }
        assert!(self.cache.is_synced());

        if self.output_first_value {
            // special case for `min`, `max`, `first_value` and `last_value`
            // take the first value from the cache
            Ok((self.cache.output_first(), stats))
        } else {
            const CHUNK_SIZE: usize = 1024;
            let chunks = self.cache.output_batches(CHUNK_SIZE).collect_vec();
            let mut state = func.create_state()?;
            for chunk in chunks {
                func.update(&mut state, &chunk).await?;
            }
            Ok((func.get_result(&state).await?, stats))
        }
    }

    #[cfg(test)]
    async fn get_output_no_stats(
        &mut self,
        state_table: &StateTable<impl StateStore>,
        group_key: Option<&GroupKey>,
        func: &BoxedAggregateFunction,
    ) -> StreamExecutorResult<Datum> {
        let (res, _stats) = self.get_output(state_table, group_key, func).await?;
        Ok(res)
    }
}

/// Copied from old code before <https://github.com/risingwavelabs/risingwave/commit/0020507edbc4010b20aeeb560c7bea9159315602>.
fn generate_order_columns_before_version_issue_13465(
    agg_call: &AggCall,
    pk_indices: &PkIndices,
    arg_col_indices: &[usize],
) -> (Vec<usize>, Vec<OrderType>) {
    let (mut order_col_indices, mut order_types) = if matches!(
        agg_call.agg_type,
        AggType::Builtin(PbAggKind::Min | PbAggKind::Max)
    ) {
        // `min`/`max` need not to order by any other columns, but have to
        // order by the agg value implicitly.
        let order_type = if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Min)) {
            OrderType::ascending()
        } else {
            OrderType::descending()
        };
        (vec![arg_col_indices[0]], vec![order_type])
    } else {
        agg_call
            .column_orders
            .iter()
            .map(|p| {
                (
                    p.column_index,
                    if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::LastValue)) {
                        p.order_type.reverse()
                    } else {
                        p.order_type
                    },
                )
            })
            .unzip()
    };

    if agg_call.distinct {
        // If distinct, we need to materialize input with the distinct keys
        // As we only support single-column distinct for now, we use the
        // `agg_call.args.val_indices()[0]` as the distinct key.
        if !order_col_indices.contains(&agg_call.args.val_indices()[0]) {
            order_col_indices.push(agg_call.args.val_indices()[0]);
            order_types.push(OrderType::ascending());
        }
    } else {
        // If not distinct, we need to materialize input with the primary keys
        let pk_len = pk_indices.len();
        order_col_indices.extend(pk_indices.iter());
        order_types.extend(itertools::repeat_n(OrderType::ascending(), pk_len));
    }

    (order_col_indices, order_types)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use rand::Rng;
    use rand::seq::IteratorRandom;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ListValue};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_expr::aggregate::{AggCall, build_append_only};
    use risingwave_pb::stream_plan::PbAggNodeVersion;
    use risingwave_storage::StateStore;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::StateTableColumnMapping;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::{PkIndices, StreamExecutorResult};

    fn create_chunk<S: StateStore>(
        pretty: &str,
        table: &mut StateTable<S>,
        col_mapping: &StateTableColumnMapping,
    ) -> StreamChunk {
        let chunk = StreamChunk::from_pretty(pretty);
        table.write_chunk(chunk.project(col_mapping.upstream_columns()));
        chunk
    }

    async fn create_mem_state_table(
        input_schema: &Schema,
        upstream_columns: Vec<usize>,
        order_types: Vec<OrderType>,
    ) -> (StateTable<MemoryStateStore>, StateTableColumnMapping) {
        // see `LogicalAgg::infer_stream_agg_state` for the construction of state table
        let table_id = TableId::new(rand::rng().random());
        let columns = upstream_columns
            .iter()
            .map(|col_idx| input_schema[*col_idx].data_type())
            .enumerate()
            .map(|(i, data_type)| ColumnDesc::unnamed(ColumnId::new(i as i32), data_type))
            .collect_vec();
        let mapping = StateTableColumnMapping::new(upstream_columns, None);
        let pk_len = order_types.len();
        let table = StateTable::from_table_catalog(
            &gen_pbtable(table_id, columns, order_types, (0..pk_len).collect(), 0),
            MemoryStateStore::new(),
            None,
        )
        .await;
        (table, mapping)
    }

    #[tokio::test]
    async fn test_extreme_agg_state_basic_min() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call = AggCall::from_pretty("(min:int4 $2:int4)"); // min(c)
        let agg = build_append_only(&agg_call).unwrap();
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

        let order_columns = vec![
            ColumnOrder::new(2, OrderType::ascending()), // c ASC for AggKind::Min
            ColumnOrder::new(3, OrderType::ascending()), // _row_id
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

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

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(3i32.into()));
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(2i32.into()));
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                PbAggNodeVersion::LATEST,
                &agg_call,
                &PkIndices::new(), // unused
                &order_columns,
                &mapping,
                usize::MAX,
                &input_schema,
            )
            .unwrap();
            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(2i32.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_basic_max() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call = AggCall::from_pretty("(max:int4 $2:int4)"); // max(c)
        let agg = build_append_only(&agg_call).unwrap();
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

        let order_columns = vec![
            ColumnOrder::new(2, OrderType::descending()), // c DESC for AggKind::Max
            ColumnOrder::new(3, OrderType::ascending()),  // _row_id
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

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

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(8i32.into()));
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 0 9 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(9i32.into()));
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                PbAggNodeVersion::LATEST,
                &agg_call,
                &PkIndices::new(), // unused
                &order_columns,
                &mapping,
                usize::MAX,
                &input_schema,
            )
            .unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(9i32.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_with_hidden_input() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call_1 = AggCall::from_pretty("(min:varchar $0:varchar)"); // min(a)
        let agg_call_2 = AggCall::from_pretty("(max:int4 $1:int4)"); // max(b)
        let agg1 = build_append_only(&agg_call_1).unwrap();
        let agg2 = build_append_only(&agg_call_2).unwrap();
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

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table_1.init_epoch(epoch).await.unwrap();
        table_2.init_epoch(epoch).await.unwrap();

        let order_columns_1 = vec![
            ColumnOrder::new(0, OrderType::ascending()), // a ASC for AggKind::Min
            ColumnOrder::new(3, OrderType::ascending()), // _row_id
        ];
        let mut state_1 = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call_1,
            &PkIndices::new(), // unused
            &order_columns_1,
            &mapping_1,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let order_columns_2 = vec![
            ColumnOrder::new(1, OrderType::descending()), // b DESC for AggKind::Max
            ColumnOrder::new(3, OrderType::ascending()),  // _row_id
        ];
        let mut state_2 = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call_2,
            &PkIndices::new(), // unused
            &order_columns_2,
            &mapping_2,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

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

            state_1.apply_chunk(&chunk_1)?;
            state_2.apply_chunk(&chunk_2)?;

            epoch.inc_for_test();
            table_1.commit_for_test(epoch).await.unwrap();
            table_2.commit_for_test(epoch).await.unwrap();

            let out1 = state_1
                .get_output_no_stats(&table_1, group_key.as_ref(), &agg1)
                .await?;
            assert_eq!(out1, Some("a".into()));

            let out2 = state_2
                .get_output_no_stats(&table_2, group_key.as_ref(), &agg2)
                .await?;
            assert_eq!(out2, Some(9i32.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_grouped() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call = AggCall::from_pretty("(max:int4 $1:int4)"); // max(b)
        let agg = build_append_only(&agg_call).unwrap();
        let group_key = Some(GroupKey::new(OwnedRow::new(vec![Some(8.into())]), None));

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

        let order_columns = vec![
            ColumnOrder::new(1, OrderType::descending()), // b DESC for AggKind::Max
            ColumnOrder::new(3, OrderType::ascending()),  // _row_id
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

        {
            let chunk = create_chunk(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 7 3 130 D // hide this row",
                &mut table,
                &mapping,
            );

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(5i32.into()));
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 9 2 134 D // hide this row
                + e 8 8 137",
                &mut table,
                &mapping,
            );

            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(8i32.into()));
        }

        {
            // test recovery (cold start)
            let mut state = MaterializedInputState::new(
                PbAggNodeVersion::LATEST,
                &agg_call,
                &PkIndices::new(), // unused
                &order_columns,
                &mapping,
                usize::MAX,
                &input_schema,
            )
            .unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(8i32.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_with_random_values() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Int32);
        let field2 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2]);

        let agg_call = AggCall::from_pretty("(min:int4 $0:int4)"); // min(a)
        let agg = build_append_only(&agg_call).unwrap();
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

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

        let order_columns = vec![
            ColumnOrder::new(0, OrderType::ascending()), // a ASC for AggKind::Min
            ColumnOrder::new(1, OrderType::ascending()), // _row_id
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            1024,
            &input_schema,
        )
        .unwrap();

        let mut rng = rand::rng();
        let insert_values: Vec<i32> = (0..10000).map(|_| rng.random()).collect_vec();
        let delete_values: HashSet<_> = insert_values
            .iter()
            .choose_multiple(&mut rng, 1000)
            .into_iter()
            .collect();
        let mut min_value = i32::MAX;

        {
            let mut pretty_lines = vec!["i I".to_owned()];
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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(min_value.into()));
        }

        {
            let mut pretty_lines = vec!["i I".to_owned()];
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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(min_value.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_extreme_agg_state_cache_maintenance() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: int32, _row_id: int64)

        let field1 = Field::unnamed(DataType::Int32);
        let field2 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2]);

        let agg_call = AggCall::from_pretty("(min:int4 $0:int4)"); // min(a)
        let agg = build_append_only(&agg_call).unwrap();
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

        let order_columns = vec![
            ColumnOrder::new(0, OrderType::ascending()), // a ASC for AggKind::Min
            ColumnOrder::new(1, OrderType::ascending()), // _row_id
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            3, // cache capacity = 3 for easy testing
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

        {
            let chunk = create_chunk(
                " i  I
                + 4  123
                + 8  128
                + 12 129",
                &mut table,
                &mapping,
            );
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(4i32.into()));
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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(12i32.into()));
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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some(12i32.into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_state() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, _delim: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
        ]);

        let agg_call = AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar orderby $2:asc $0:desc)",
        );
        let agg = build_append_only(&agg_call).unwrap();
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

        let order_columns = vec![
            ColumnOrder::new(2, OrderType::ascending()),  // b ASC
            ColumnOrder::new(0, OrderType::descending()), // a DESC
            ColumnOrder::new(4, OrderType::ascending()),  // _row_id ASC
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some("c,a".into()));
        }

        {
            let chunk = create_chunk(
                " T T i i I
                + d - 0 8 134
                + e + 2 2 137",
                &mut table,
                &mapping,
            );
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res, Some("d_c,a+e".into()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_state() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let field1 = Field::unnamed(DataType::Varchar);
        let field2 = Field::unnamed(DataType::Int32);
        let field3 = Field::unnamed(DataType::Int32);
        let field4 = Field::unnamed(DataType::Int64);
        let input_schema = Schema::new(vec![field1, field2, field3, field4]);

        let agg_call = AggCall::from_pretty("(array_agg:int4[] $1:int4 orderby $2:asc $0:desc)");
        let agg = build_append_only(&agg_call).unwrap();
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

        let order_columns = vec![
            ColumnOrder::new(2, OrderType::ascending()),  // c ASC
            ColumnOrder::new(0, OrderType::descending()), // a DESC
            ColumnOrder::new(3, OrderType::ascending()),  // _row_id ASC
        ];
        let mut state = MaterializedInputState::new(
            PbAggNodeVersion::LATEST,
            &agg_call,
            &PkIndices::new(), // unused
            &order_columns,
            &mapping,
            usize::MAX,
            &input_schema,
        )
        .unwrap();

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();
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
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res.unwrap().as_list(), &ListValue::from_iter([2, 1]));
        }

        {
            let chunk = create_chunk(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
                &mut table,
                &mapping,
            );
            state.apply_chunk(&chunk)?;

            epoch.inc_for_test();
            table.commit_for_test(epoch).await.unwrap();

            let res = state
                .get_output_no_stats(&table, group_key.as_ref(), &agg)
                .await?;
            assert_eq!(res.unwrap().as_list(), &ListValue::from_iter([2, 2, 0, 1]));
        }

        Ok(())
    }
}
