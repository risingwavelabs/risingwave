// Copyright 2024 RisingWave Labs
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
use prometheus::core::Atomic;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::FilterByBitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, NullBitmap, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::{OrderType, cmp_datum_iter};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::expr::BoxedExpression;

use super::AsOfDesc;
use crate::error::BatchError;
use crate::executor::join::chunked_data::ChunkedData;
use crate::executor::{
    BoxedDataChunkListStream, BoxedExecutor, BufferChunkExecutor, EquiJoinParams, HashJoinExecutor,
    JoinHashMap, JoinType, LookupExecutorBuilder, RowId, utils,
};
use crate::task::ShutdownToken;

/// Lookup Join Base.
/// Used by `LocalLookupJoinExecutor` and `DistributedLookupJoinExecutor`.
pub struct LookupJoinBase<K, B: LookupExecutorBuilder> {
    pub join_type: JoinType,
    pub condition: Option<BoxedExpression>,
    pub outer_side_input: BoxedExecutor,
    pub outer_side_data_types: Vec<DataType>, // Data types of all columns of outer side table
    pub outer_side_key_idxs: Vec<usize>,
    pub inner_side_builder: B,
    pub inner_side_key_types: Vec<DataType>, // Data types only of key columns of inner side table
    pub inner_side_key_idxs: Vec<usize>,
    pub null_safe: Vec<bool>,
    pub lookup_prefix_len: usize,
    pub chunk_builder: DataChunkBuilder,
    pub schema: Schema,
    pub output_indices: Vec<usize>,
    pub chunk_size: usize,
    pub asof_desc: Option<AsOfDesc>,
    pub identity: String,
    pub shutdown_rx: ShutdownToken,
    pub mem_ctx: MemoryContext,
    pub _phantom: PhantomData<K>,
}

const AT_LEAST_OUTER_SIDE_ROWS: usize = 512;

impl<K: HashKey, B: LookupExecutorBuilder> LookupJoinBase<K, B> {
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

            // This round's private context releases remaining charges on completion, errors, or
            // cancellation, before the next round starts.
            let mem_ctx = MemoryContext::new(Some(self.mem_ctx.clone()), TrAdderAtomic::new(0));

            let mut build_side = Vec::new_in(mem_ctx.global_allocator());
            let mut build_row_count = 0;
            #[for_await]
            for build_chunk in hash_join_build_side_input.execute() {
                let build_chunk = build_chunk?;
                if build_chunk.cardinality() > 0 {
                    build_row_count += build_chunk.cardinality();
                    let chunk_estimated_heap_size = build_chunk.estimated_heap_size() as i64;
                    build_side.push(build_chunk);
                    mem_ctx.add_unchecked(chunk_estimated_heap_size);
                }
            }
            let mut hash_map = JoinHashMap::with_capacity_and_hasher_in(
                build_row_count,
                PrecomputedBuildHasher,
                mem_ctx.global_allocator(),
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
                        next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
                        mem_ctx.add_unchecked(build_key_estimated_heap_size);
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
                self.asof_desc.clone(),
            );

            if let Some(cond) = self.condition.as_ref()
                && !params.is_asof_join()
            {
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
                    JoinType::Inner | JoinType::AsOfInner => {
                        HashJoinExecutor::do_inner_join(params)
                    }
                    JoinType::LeftOuter | JoinType::AsOfLeftOuter => {
                        HashJoinExecutor::do_left_outer_join(params)
                    }
                    JoinType::LeftSemi => HashJoinExecutor::do_left_semi_anti_join::<false>(params),
                    JoinType::LeftAnti => HashJoinExecutor::do_left_semi_anti_join::<true>(params),
                    JoinType::RightOuter
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::FullOuter => unimplemented!(),
                };
                #[for_await]
                for chunk in stream {
                    yield chunk?.project(&self.output_indices)
                }
            }
        }
    }
}

#[cfg(test)]
mod memory_budget {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::KeySerialized;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::types::Datum;

    use super::*;
    use crate::error::Result;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::test_utils::memory_cleanup::{self, Exit};

    /// Use an in-memory build-side fixture while exercising the real lookup/hash-join execution.
    struct AccountingLookupBuilder {
        schema: Schema,
        chunk: DataChunk,
        child: MemoryContext,
        parent: MemoryContext,
        rounds: Arc<AtomicUsize>,
        exit: Option<Exit>,
    }

    impl LookupExecutorBuilder for AccountingLookupBuilder {
        fn reset(&mut self) {
            // The shared context stays alive across rounds and executions.
            assert_eq!(self.child.get_bytes_used(), 0);
            assert_eq!(self.parent.get_bytes_used(), 16);
            self.rounds.fetch_add(1, Ordering::Relaxed);
        }

        async fn add_scan_range(&mut self, _key_datums: Vec<Datum>) -> Result<()> {
            Ok(())
        }

        async fn build_executor(&mut self) -> Result<BoxedExecutor> {
            let input: BoxedExecutor = Box::new(MockExecutor::with_chunk(
                self.chunk.clone(),
                self.schema.clone(),
            ));
            Ok(match self.exit {
                Some(exit) => memory_cleanup::input(input, exit, self.child.clone()),
                None => input,
            })
        }
    }

    /// Verifies that a lookup round must record its retained build memory even above budget, then
    /// release those charges before the next round starts. A build-input error or dropping the
    /// stream while waiting for build input or after an output also releases the round's charges.
    ///
    /// Equivalent query: `SELECT o.k, i.k FROM outer_rows o JOIN inner_rows i ON o.k = i.k`.
    /// Input: two outer chunks, each with 512 rows `(K)`, and one inner row `(K)`.
    /// `K` is a string of 128 `x` characters.
    /// Expected output: 512 rows `(K, K)` per lookup round, or 1,024 rows across both rounds.
    /// Interrupted cases fail or wait at the first build EOF, or drop after the first 512-row output.
    #[tokio::test]
    async fn test_over_budget_lookup_join_accounting_memory_across_lookup_rounds() {
        let parent = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 64);
        assert!(parent.add(16));
        let child = MemoryContext::new_with_mem_limit(
            Some(parent.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
            0,
        );
        let schema = Schema::new(vec![Field::unnamed(DataType::Varchar)]);
        let key = "x".repeat(128);
        let inner = DataChunk::from_pretty(&format!("T\n{key}"));
        let keys = <KeySerialized as HashKey>::build_many(&[0], &inner);
        let key_size = keys[0].estimated_heap_size() as i64;
        assert!(key_size > 0);
        let manual_charge = inner.estimated_heap_size() as i64 + key_size;
        let outer = DataChunk::from_pretty(&format!(
            "T\n{}",
            format!("{key}\n").repeat(AT_LEAST_OUTER_SIDE_ROWS)
        ));
        for exit in [
            None,
            Some(Exit::InputError),
            Some(Exit::PendingInput),
            Some(Exit::Output),
        ] {
            let mut input = MockExecutor::new(schema.clone());
            // Each chunk fills batch_read's threshold, forcing two independent build/release rounds.
            input.add(outer.clone());
            input.add(outer.clone());
            let rounds = Arc::new(AtomicUsize::new(0));
            let output_schema = Schema::new(vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Varchar),
            ]);
            let exec = LookupJoinBase::<KeySerialized, _> {
                join_type: JoinType::Inner,
                condition: None,
                outer_side_input: Box::new(input),
                outer_side_data_types: schema.data_types(),
                outer_side_key_idxs: vec![0],
                inner_side_builder: AccountingLookupBuilder {
                    schema: schema.clone(),
                    chunk: inner.clone(),
                    child: child.clone(),
                    parent: parent.clone(),
                    rounds: rounds.clone(),
                    exit,
                },
                inner_side_key_types: schema.data_types(),
                inner_side_key_idxs: vec![0],
                null_safe: vec![false],
                lookup_prefix_len: 1,
                chunk_builder: DataChunkBuilder::new(
                    output_schema.data_types(),
                    AT_LEAST_OUTER_SIDE_ROWS,
                ),
                schema: output_schema,
                output_indices: vec![0, 1],
                chunk_size: AT_LEAST_OUTER_SIDE_ROWS,
                asof_desc: None,
                identity: "accounting-test".into(),
                shutdown_rx: ShutdownToken::empty(),
                mem_ctx: child.clone(),
                _phantom: PhantomData,
            };
            let expected = matches!(exit, None | Some(Exit::Output)).then(|| {
                DataChunk::from_pretty(&format!(
                    "T T\n{}",
                    format!("{key} {key}\n").repeat(AT_LEAST_OUTER_SIDE_ROWS)
                ))
            });
            let mut output = Box::new(exec).do_execute();
            if let Some(exit) = exit {
                memory_cleanup::assert_released(output, exit, expected.as_ref(), &child, &parent)
                    .await;
                assert_eq!(rounds.load(Ordering::Relaxed), 1);
                continue;
            }
            let expected = expected.unwrap();
            for round in 1..=2 {
                assert_eq!(output.next().await.unwrap().unwrap(), expected);
                assert_eq!(rounds.load(Ordering::Relaxed), round);
                // Alongside the manual charge, the monitored Vec/hash map have backing allocations.
                assert!(child.get_bytes_used() > manual_charge);
                assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
                assert!(!child.check_memory_usage());
            }
            assert!(output.next().await.is_none());
            assert_eq!(child.get_bytes_used(), 0);
            assert_eq!(parent.get_bytes_used(), 16);
        }
        drop(child);
        assert_eq!(parent.get_bytes_used(), 16);
        assert!(parent.add(-16));
    }
}
