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

use std::marker::PhantomData;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::FilterByBitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, NullBitmap, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::{cmp_datum_iter, OrderType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::expr::BoxedExpression;

use crate::error::BatchError;
use crate::executor::join::chunked_data::ChunkedData;
use crate::executor::{
    utils, BoxedDataChunkListStream, BoxedExecutor, BufferChunkExecutor, EquiJoinParams,
    HashJoinExecutor, JoinHashMap, JoinType, LookupExecutorBuilder, RowId,
};
use crate::task::ShutdownToken;

/// Lookup Join Base.
/// Used by `LocalLookupJoinExecutor` and `DistributedLookupJoinExecutor`.
pub struct LookupJoinBase<K> {
    pub join_type: JoinType,
    pub condition: Option<BoxedExpression>,
    pub outer_side_input: BoxedExecutor,
    pub outer_side_data_types: Vec<DataType>, // Data types of all columns of outer side table
    pub outer_side_key_idxs: Vec<usize>,
    pub inner_side_builder: Box<dyn LookupExecutorBuilder>,
    pub inner_side_key_types: Vec<DataType>, // Data types only of key columns of inner side table
    pub inner_side_key_idxs: Vec<usize>,
    pub null_safe: Vec<bool>,
    pub lookup_prefix_len: usize,
    pub chunk_builder: DataChunkBuilder,
    pub schema: Schema,
    pub output_indices: Vec<usize>,
    pub chunk_size: usize,
    pub identity: String,
    pub shutdown_rx: ShutdownToken,
    pub mem_ctx: MemoryContext,
    pub _phantom: PhantomData<K>,
}

const AT_LEAST_OUTER_SIDE_ROWS: usize = 512;

impl<K: HashKey> LookupJoinBase<K> {
    /// High level Execution flow:
    /// Repeat 1-3:
    ///   1. Read N rows from outer side input and send keys to inner side builder after
    ///      deduplication.
    ///   2. Inner side input lookups inner side table with keys and builds hash map.
    ///   3. Outer side rows join each inner side rows by probing the hash map.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_execute(mut self: Box<Self>) {
        let outer_side_schema = self.outer_side_input.schema().clone();

        let null_matched = K::Bitmap::from_bool_vec(self.null_safe);

        let mut outer_side_batch_read_stream: BoxedDataChunkListStream =
            utils::batch_read(self.outer_side_input.execute(), AT_LEAST_OUTER_SIDE_ROWS);

        while let Some(chunk_list) = outer_side_batch_read_stream.next().await {
            let chunk_list = chunk_list?;

            // Group rows with the same key datums together
            let groups = chunk_list
                .iter()
                .flat_map(|chunk| {
                    chunk.rows().map(|row| {
                        self.outer_side_key_idxs
                            .iter()
                            .take(self.lookup_prefix_len)
                            .map(|&idx| row.datum_at(idx).to_owned_datum())
                            .collect_vec()
                    })
                })
                .sorted_by(|a, b| cmp_datum_iter(a, b, std::iter::repeat(OrderType::default())))
                .dedup()
                .collect_vec();

            self.inner_side_builder.reset();
            for row_key in groups {
                self.inner_side_builder.add_scan_range(row_key).await?;
            }
            let inner_side_input = self.inner_side_builder.build_executor().await?;

            // Lookup join outer side will become the probe side of hash join,
            // while its inner side will become the build side of hash join.
            let hash_join_probe_side_input = Box::new(BufferChunkExecutor::new(
                outer_side_schema.clone(),
                chunk_list,
            ));
            let hash_join_build_side_input = inner_side_input;
            let hash_join_probe_data_types = self.outer_side_data_types.clone();
            let hash_join_build_data_types = hash_join_build_side_input.schema().data_types();
            let hash_join_probe_side_key_idxs = self.outer_side_key_idxs.clone();
            let hash_join_build_side_key_idxs = self.inner_side_key_idxs.clone();

            let full_data_types = [
                hash_join_probe_data_types.clone(),
                hash_join_build_data_types.clone(),
            ]
            .concat();

            // We need to temporary variable to record heap size, since in each loop we
            // will free build side hash map, and the subtraction is not executed automatically.
            let mut tmp_heap_size = 0i64;

            let mut build_side = Vec::new_in(self.mem_ctx.global_allocator());
            let mut build_row_count = 0;
            #[for_await]
            for build_chunk in hash_join_build_side_input.execute() {
                let build_chunk = build_chunk?;
                if build_chunk.cardinality() > 0 {
                    build_row_count += build_chunk.cardinality();
                    let chunk_estimated_heap_size = build_chunk.estimated_heap_size() as i64;
                    self.mem_ctx.add(chunk_estimated_heap_size);
                    tmp_heap_size += chunk_estimated_heap_size;
                    build_side.push(build_chunk);
                }
            }
            let mut hash_map = JoinHashMap::with_capacity_and_hasher_in(
                build_row_count,
                PrecomputedBuildHasher,
                self.mem_ctx.global_allocator(),
            );
            let mut next_build_row_with_same_key =
                ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

            // Build hash map
            for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
                let build_keys = K::build_many(&hash_join_build_side_key_idxs, build_chunk);

                for (build_row_id, build_key) in build_keys
                    .into_iter()
                    .enumerate()
                    .filter_by_bitmap(build_chunk.visibility())
                {
                    // Only insert key to hash map if it is consistent with the null safe
                    // restriction.
                    if build_key.null_bitmap().is_subset(&null_matched) {
                        let row_id = RowId::new(build_chunk_id, build_row_id);
                        let build_key_estimated_heap_size = build_key.estimated_heap_size() as i64;
                        self.mem_ctx.add(build_key_estimated_heap_size);
                        tmp_heap_size += build_key_estimated_heap_size;
                        next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
                    }
                }
            }

            let params = EquiJoinParams::new(
                hash_join_probe_side_input,
                hash_join_probe_data_types,
                hash_join_probe_side_key_idxs,
                build_side,
                hash_join_build_data_types,
                full_data_types,
                hash_map,
                next_build_row_with_same_key,
                self.chunk_size,
                self.shutdown_rx.clone(),
                None,
            );

            if let Some(cond) = self.condition.as_ref() {
                let stream = match self.join_type {
                    JoinType::Inner => {
                        HashJoinExecutor::do_inner_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftOuter => {
                        HashJoinExecutor::do_left_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftSemi => {
                        HashJoinExecutor::do_left_semi_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftAnti => {
                        HashJoinExecutor::do_left_anti_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::RightOuter
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::FullOuter
                    | JoinType::AsOfInner
                    | JoinType::AsOfLeftOuter => unimplemented!(),
                };
                // For non-equi join, we need an output chunk builder to align the output chunks.
                let mut output_chunk_builder =
                    DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
                #[for_await]
                for chunk in stream {
                    for output_chunk in
                        output_chunk_builder.append_chunk(chunk?.project(&self.output_indices))
                    {
                        yield output_chunk
                    }
                }
                if let Some(output_chunk) = output_chunk_builder.consume_all() {
                    yield output_chunk
                }
            } else {
                let stream = match self.join_type {
                    JoinType::Inner => HashJoinExecutor::do_inner_join(params),
                    JoinType::LeftOuter => HashJoinExecutor::do_left_outer_join(params),
                    JoinType::LeftSemi => HashJoinExecutor::do_left_semi_anti_join::<false>(params),
                    JoinType::LeftAnti => HashJoinExecutor::do_left_semi_anti_join::<true>(params),
                    JoinType::RightOuter
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::FullOuter
                    | JoinType::AsOfInner
                    | JoinType::AsOfLeftOuter => unimplemented!(),
                };
                #[for_await]
                for chunk in stream {
                    yield chunk?.project(&self.output_indices)
                }
            }

            self.mem_ctx.add(-tmp_heap_size);
        }
    }
}
