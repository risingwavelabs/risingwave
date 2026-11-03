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

use std::cmp::Ordering;
use std::iter::empty;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_async_stream::try_stream;
use itertools::{Either, Itertools};
use prometheus::core::Atomic;
use risingwave_common::array::{Array, DataChunk, RowRef};
use risingwave_common::bitmap::{Bitmap, FilterByBitmap};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DefaultOrd};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{AsOfDesc, AsOfInequalityType, ChunkedData, JoinType, RowId};
use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::monitor::BatchSpillMetrics;
use crate::risingwave_common::hash::NullBitmap;
use crate::spill::partition::{Partitioning, SpillContext};
use crate::spill::spill_op::SpillBackend;
use crate::spill::spill_op::SpillBackend::Disk;
use crate::task::ShutdownToken;

/// Hash Join Executor
///
/// High-level idea:
/// 1. Iterate over the build side (i.e. right table) and build a hash map.
/// 2. Iterate over the probe side (i.e. left table) and compute the hash value of each row.
///    Then find the matched build side row for each probe side row in the hash map.
/// 3. Concatenate the matched pair of probe side row and build side row into a single row and push
///    it into the data chunk builder.
/// 4. Yield chunks from the builder.
pub struct HashJoinExecutor<K> {
    /// Join type e.g. inner, left outer, ...
    join_type: JoinType,
    /// Output schema without applying `output_indices`
    #[expect(dead_code)]
    original_schema: Schema,
    /// Output schema after applying `output_indices`
    schema: Schema,
    /// `output_indices` are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Left child executor
    probe_side_source: BoxedExecutor,
    /// Right child executor
    build_side_source: BoxedExecutor,
    /// Column indices of left keys in equi join
    probe_key_idxs: Vec<usize>,
    /// Column indices of right keys in equi join
    build_key_idxs: Vec<usize>,
    /// Non-equi join condition (optional)
    cond: Option<Arc<BoxedExpression>>,
    /// Whether or not to enable 'IS NOT DISTINCT FROM' semantics for a specific probe/build key
    /// column
    null_matched: Vec<bool>,
    identity: String,
    chunk_size: usize,
    /// Whether the join is an as-of join
    asof_desc: Option<AsOfDesc>,

    spill: SpillContext,

    shutdown_rx: ShutdownToken,

    mem_ctx: MemoryContext,
    _phantom: PhantomData<K>,
}

impl<K: HashKey> Executor for HashJoinExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

/// In `JoinHashMap`, we only save the row id of the first build row that has the hash key.
/// In fact, in the build side there may be multiple rows with the same hash key. To handle this
/// case, we use `ChunkedData` to link them together. For example:
///
/// | id | key | row |
/// | --- | --- | --- |
/// | 0 | 1 | (1, 2, 3) |
/// | 1 | 4 | (4, 5, 6) |
/// | 2 | 1 | (1, 3, 7) |
/// | 3 | 1 | (1, 3, 2) |
/// | 4 | 3 | (3, 2, 1) |
///
/// The corresponding join hash map is:
///
/// | key | value |
/// | --- | --- |
/// | 1 | 0 |
/// | 4 | 1 |
/// | 3 | 4 |
///
/// And we save build rows with the same key like this:
///
/// | id | value |
/// | --- | --- |
/// | 0 | 2 |
/// | 1 | None |
/// | 2 | 3 |
/// | 3 | None |
/// | 4 | None |
///
/// This can be seen as an implicit linked list. For convenience, we use `RowIdIter` to iterate all
/// build side row ids with the given key.
pub type JoinHashMap<K> =
    hashbrown::HashMap<K, RowId, PrecomputedBuildHasher, MonitoredGlobalAlloc>;

struct RowIdIter<'a> {
    current_row_id: Option<RowId>,
    next_row_id: &'a ChunkedData<Option<RowId>>,
}

impl ChunkedData<Option<RowId>> {
    fn row_id_iter(&self, begin: Option<RowId>) -> RowIdIter<'_> {
        RowIdIter {
            current_row_id: begin,
            next_row_id: self,
        }
    }
}

impl Iterator for RowIdIter<'_> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_row_id.inspect(|row_id| {
            self.current_row_id = self.next_row_id[*row_id];
        })
    }
}

pub struct EquiJoinParams<K> {
    probe_side: BoxedExecutor,
    probe_data_types: Vec<DataType>,
    probe_key_idxs: Vec<usize>,
    build_side: Vec<DataChunk, MonitoredGlobalAlloc>,
    build_data_types: Vec<DataType>,
    full_data_types: Vec<DataType>,
    hash_map: JoinHashMap<K>,
    next_build_row_with_same_key: ChunkedData<Option<RowId>>,
    chunk_size: usize,
    shutdown_rx: ShutdownToken,
    asof_desc: Option<AsOfDesc>,
}

impl<K> EquiJoinParams<K> {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        probe_side: BoxedExecutor,
        probe_data_types: Vec<DataType>,
        probe_key_idxs: Vec<usize>,
        build_side: Vec<DataChunk, MonitoredGlobalAlloc>,
        build_data_types: Vec<DataType>,
        full_data_types: Vec<DataType>,
        hash_map: JoinHashMap<K>,
        next_build_row_with_same_key: ChunkedData<Option<RowId>>,
        chunk_size: usize,
        shutdown_rx: ShutdownToken,
        asof_desc: Option<AsOfDesc>,
    ) -> Self {
        Self {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            asof_desc,
        }
    }

    pub(crate) fn is_asof_join(&self) -> bool {
        self.asof_desc.is_some()
    }
}

/// What a join type outputs for probe or build rows, besides matched pairs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RowOutput {
    /// Nothing.
    Omit,
    /// Each matched row, once (semi join).
    Matched,
    /// Each unmatched row (anti join).
    Unmatched,
    /// Each unmatched row, padded with NULLs for the other side (outer join).
    UnmatchedWithNulls,
}

impl RowOutput {
    fn emits(self, matched: bool) -> bool {
        match self {
            RowOutput::Omit => false,
            RowOutput::Matched => matched,
            RowOutput::Unmatched | RowOutput::UnmatchedWithNulls => !matched,
        }
    }
}

/// What a join type outputs. See [`HashJoinExecutor::do_probe`].
#[derive(Clone, Copy)]
struct JoinOutput {
    /// Whether to output each matched pair of a probe row and a build row.
    pairs: bool,
    probe_rows: RowOutput,
    build_rows: RowOutput,
}

impl JoinOutput {
    fn new(join_type: JoinType) -> Self {
        use RowOutput::*;
        let (pairs, probe_rows, build_rows) = match join_type {
            JoinType::Inner | JoinType::AsOfInner => (true, Omit, Omit),
            JoinType::LeftOuter | JoinType::AsOfLeftOuter => (true, UnmatchedWithNulls, Omit),
            JoinType::LeftSemi => (false, Matched, Omit),
            JoinType::LeftAnti => (false, Unmatched, Omit),
            JoinType::RightOuter => (true, Omit, UnmatchedWithNulls),
            JoinType::RightSemi => (false, Omit, Matched),
            JoinType::RightAnti => (false, Omit, Unmatched),
            JoinType::FullOuter => (true, UnmatchedWithNulls, UnmatchedWithNulls),
        };
        Self {
            pairs,
            probe_rows,
            build_rows,
        }
    }

    /// Whether all candidates of a probe row are needed. If not, only whether the probe row
    /// matched matters, and its remaining candidates are skipped once it has.
    fn needs_every_candidate(self) -> bool {
        self.pairs || self.build_rows != RowOutput::Omit
    }

    /// Data types of the rows output for probe or build rows.
    fn row_data_types(
        self,
        probe_data_types: &[DataType],
        build_data_types: &[DataType],
        full_data_types: &[DataType],
    ) -> Vec<DataType> {
        use RowOutput::*;
        match (self.probe_rows, self.build_rows) {
            (Matched | Unmatched, _) => probe_data_types.to_vec(),
            (_, Matched | Unmatched) => build_data_types.to_vec(),
            _ => full_data_types.to_vec(),
        }
    }
}

/// Whether each probe row of the current probe chunk and each build row has matched. Each side
/// is only tracked if the join outputs rows of that side.
struct MatchState {
    probe: Option<Vec<bool>>,
    build: Option<ChunkedData<bool>>,
}

impl MatchState {
    fn mark(&mut self, probe_row_id: usize, build_row_id: RowId) {
        self.mark_probe(probe_row_id);
        if let Some(build) = &mut self.build {
            build[build_row_id] = true;
        }
    }

    fn mark_probe(&mut self, probe_row_id: usize) {
        if let Some(probe) = &mut self.probe {
            probe[probe_row_id] = true;
        }
    }

    fn probe_matched(&self, probe_row_id: usize) -> bool {
        self.probe.as_ref().is_some_and(|probe| probe[probe_row_id])
    }
}

/// The probe and build row ids of the candidate pairs buffered for evaluating the non-equi
/// condition, in the order of the buffered rows. Only the ids of the sides whose match state is
/// tracked are kept.
#[derive(Default)]
struct PairRowIds {
    probe: Vec<usize>,
    build: Vec<RowId>,
}

impl PairRowIds {
    fn push(&mut self, matched: &MatchState, probe_row_id: usize, build_row_id: RowId) {
        if matched.probe.is_some() {
            self.probe.push(probe_row_id);
        }
        if matched.build.is_some() {
            self.build.push(build_row_id);
        }
    }

    /// Evaluates `cond` on `pairs`, the buffered candidate pairs, and marks the rows of each pair
    /// that satisfies it as matched. Returns the matched pairs if the join outputs them.
    async fn evaluate(
        &mut self,
        mut pairs: DataChunk,
        cond: &BoxedExpression,
        matched: &mut MatchState,
        output_pairs: bool,
    ) -> Result<Option<DataChunk>> {
        let satisfied: Bitmap = cond.eval(&pairs).await?.as_bool().iter().collect();
        for pair in satisfied.iter_ones() {
            if let Some(probe) = &mut matched.probe {
                probe[self.probe[pair]] = true;
            }
            if let Some(build) = &mut matched.build {
                build[self.build[pair]] = true;
            }
        }
        self.probe.clear();
        self.build.clear();
        Ok(output_pairs.then(|| {
            pairs.set_visibility(satisfied);
            pairs
        }))
    }
}

impl<K: HashKey> HashJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // This invocation owns its accounting. Its private context releases remaining charges
        // on completion, errors, or cancellation, even if the shared parent stays alive.
        let mem_ctx = MemoryContext::new(Some(self.mem_ctx.clone()), TrAdderAtomic::new(0));
        let mut need_to_spill = false;

        let probe_schema = self.probe_side_source.schema().clone();
        let build_schema = self.build_side_source.schema().clone();
        let probe_data_types = self.probe_side_source.schema().data_types();
        let build_data_types = self.build_side_source.schema().data_types();
        let full_data_types = [probe_data_types.clone(), build_data_types.clone()].concat();

        let mut build_side = Vec::new_in(mem_ctx.global_allocator());
        let mut build_row_count = 0;
        let mut build_side_stream = self.build_side_source.execute();
        #[for_await]
        for build_chunk in &mut build_side_stream {
            // Unmatched build rows are emitted by walking all physical rows, so drop invisible rows
            // here to keep them out of right outer/anti and full outer join results.
            let build_chunk = build_chunk?.compact_vis();
            if build_chunk.cardinality() > 0 {
                build_row_count += build_chunk.cardinality();
                let chunk_estimated_heap_size = build_chunk.estimated_heap_size();
                // push build_chunk to build_side before checking memory limit, otherwise we will lose that chunk when spilling.
                build_side.push(build_chunk);
                mem_ctx.add_unchecked(chunk_estimated_heap_size as i64);
                if self.spill.should_spill(&mem_ctx)? {
                    need_to_spill = true;
                    break;
                }
            }
        }
        let mut hash_map = JoinHashMap::with_capacity_and_hasher_in(
            build_row_count,
            PrecomputedBuildHasher,
            mem_ctx.global_allocator(),
        );
        let mut next_build_row_with_same_key =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        let null_matched = K::Bitmap::from_bool_vec(self.null_matched.clone());

        let mut mem_added_by_hash_table = 0;
        if !need_to_spill {
            // Build hash map
            'build: for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
                let build_keys = K::build_many(&self.build_key_idxs, build_chunk);

                for (build_row_id, build_key) in build_keys
                    .into_iter()
                    .enumerate()
                    .filter_by_bitmap(build_chunk.visibility())
                {
                    self.shutdown_rx.check()?;
                    // Only insert key to hash map if it is consistent with the null safe restriction.
                    if build_key.null_bitmap().is_subset(&null_matched) {
                        let row_id = RowId::new(build_chunk_id, build_row_id);
                        let build_key_size = build_key.estimated_heap_size() as i64;
                        next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
                        // Retain and charge the key before deciding whether to spill the table.
                        mem_added_by_hash_table += build_key_size;
                        mem_ctx.add_unchecked(build_key_size);
                        if self.spill.should_spill(&mem_ctx)? {
                            need_to_spill = true;
                            break 'build;
                        }
                    }
                }
            }
        }

        if need_to_spill {
            // A spilling version of hash join based on the RFC: Spill Hash Join https://github.com/risingwavelabs/rfcs/pull/91
            // Both sides are partitioned by join key, so matching rows meet in the same partition,
            // which is joined by a sub `HashJoinExecutor` that may spill again.
            const BUILD: usize = 0;
            const PROBE: usize = 1;
            let mut spill = (self.spill)
                .start(
                    &self.identity,
                    self.chunk_size,
                    [
                        (
                            build_data_types.clone(),
                            Partitioning::Hash(self.build_key_idxs.clone()),
                        ),
                        (
                            probe_data_types.clone(),
                            Partitioning::Hash(self.probe_key_idxs.clone()),
                        ),
                    ],
                )
                .await?;

            // Release memory occupied by the hash map
            drop(hash_map);
            drop(next_build_row_with_same_key);
            mem_ctx.add_unchecked(-mem_added_by_hash_table);

            for chunk in build_side {
                let chunk_size = chunk.estimated_heap_size() as i64;
                spill.write(BUILD, chunk).await?;
                mem_ctx.add_unchecked(-chunk_size);
            }
            #[for_await]
            for chunk in build_side_stream {
                spill.write(BUILD, chunk?).await?;
            }
            #[for_await]
            for chunk in self.probe_side_source.execute() {
                spill.write(PROBE, chunk?).await?;
            }
            let partitions = spill.finish().await?;

            for i in 0..partitions.partition_num() {
                let ([build_side_stream, probe_side_stream], spill) = partitions.read(i).await?;
                let sub_hash_join_executor: HashJoinExecutor<K> = HashJoinExecutor::new_inner(
                    self.join_type,
                    self.output_indices.clone(),
                    Box::new(WrapStreamExecutor::new(
                        probe_schema.clone(),
                        probe_side_stream,
                    )),
                    Box::new(WrapStreamExecutor::new(
                        build_schema.clone(),
                        build_side_stream,
                    )),
                    self.probe_key_idxs.clone(),
                    self.build_key_idxs.clone(),
                    self.null_matched.clone(),
                    self.cond.clone(),
                    format!("{}-sub{}", self.identity.clone(), i),
                    self.chunk_size,
                    self.asof_desc.clone(),
                    spill,
                    self.shutdown_rx.clone(),
                    self.mem_ctx.clone(),
                );

                #[for_await]
                for chunk in Box::new(sub_hash_join_executor).execute() {
                    yield chunk?;
                }

                partitions.clear(i).await?;
            }
        } else {
            let params = EquiJoinParams::new(
                self.probe_side_source,
                probe_data_types,
                self.probe_key_idxs,
                build_side,
                build_data_types,
                full_data_types,
                hash_map,
                next_build_row_with_same_key,
                self.chunk_size,
                self.shutdown_rx.clone(),
                self.asof_desc,
            );

            #[for_await]
            for chunk in Self::probe_and_project(
                params,
                self.join_type,
                self.cond.as_deref(),
                &self.output_indices,
                self.schema.data_types(),
            ) {
                yield chunk?
            }
            // Build data can be reused throughout the join. The private context releases its
            // remaining charges when this invocation exits.
        }
    }

    /// Probes the hash table, projects the output to `output_indices`, and yields chunks of
    /// `params.chunk_size` rows.
    ///
    /// AS OF joins apply their condition through `asof_desc`, so `cond` is ignored for them.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub(super) async fn probe_and_project<'a>(
        params: EquiJoinParams<K>,
        join_type: JoinType,
        cond: Option<&'a BoxedExpression>,
        output_indices: &'a [usize],
        output_data_types: Vec<DataType>,
    ) {
        let cond = cond.filter(|_| !params.is_asof_join());
        let chunk_size = params.chunk_size;
        let stream = Self::do_probe(params, join_type, cond);
        if cond.is_some() {
            // Evaluating the condition hides rows and leaves partial chunks behind, so realign
            // the output chunks.
            let mut output_chunk_builder = DataChunkBuilder::new(output_data_types, chunk_size);
            #[for_await]
            for chunk in stream {
                for output_chunk in
                    output_chunk_builder.append_chunk(chunk?.project(output_indices))
                {
                    yield output_chunk
                }
            }
            if let Some(output_chunk) = output_chunk_builder.consume_all() {
                yield output_chunk
            }
        } else {
            #[for_await]
            for chunk in stream {
                yield chunk?.project(output_indices)
            }
        }
    }

    /// Probes the hash table with each probe row and produces the join output.
    ///
    /// The candidates of a probe row are the build rows with the same key, or for an AS OF join,
    /// the closest of them. A candidate pair matches if it satisfies the non-equi condition
    /// `cond`, or always if there is none. A probe or build row is matched if it is part of a
    /// matched pair. [`JoinOutput`] gives what each join type outputs:
    ///
    /// | join type         | matched pairs | probe rows            | build rows            |
    /// |-------------------|---------------|-----------------------|-----------------------|
    /// | inner             | yes           |                       |                       |
    /// | left outer        | yes           | unmatched, with NULLs |                       |
    /// | left semi / anti  |               | matched / unmatched   |                       |
    /// | right outer       | yes           |                       | unmatched, with NULLs |
    /// | right semi / anti |               |                       | matched / unmatched   |
    /// | full outer        | yes           | unmatched, with NULLs | unmatched, with NULLs |
    ///
    /// With a condition, candidate pairs are buffered and the condition is evaluated on a whole
    /// chunk of them at a time. Probe rows are output per probe chunk: if the join outputs them,
    /// all candidates of a probe chunk are evaluated before moving on to the next one, so that
    /// they have their final match state. Build rows are output after the last probe chunk.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_probe(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            asof_desc,
        }: EquiJoinParams<K>,
        join_type: JoinType,
        cond: Option<&BoxedExpression>,
    ) {
        let output = JoinOutput::new(join_type);
        let mut pair_builder = DataChunkBuilder::new(full_data_types.clone(), chunk_size);
        let mut pair_row_ids = PairRowIds::default();
        let mut row_builder = DataChunkBuilder::new(
            output.row_data_types(&probe_data_types, &build_data_types, &full_data_types),
            chunk_size,
        );
        // One row of NULLs for each side, to pad the unmatched rows of the other side.
        let null_probe_row = Self::null_row(&probe_data_types);
        let null_build_row = Self::null_row(&build_data_types);
        let mut matched = MatchState {
            probe: (output.probe_rows != RowOutput::Omit).then(Vec::new),
            build: (output.build_rows != RowOutput::Omit)
                .then(|| ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity())))
                .transpose()?,
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            shutdown_rx.check()?;
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            if let Some(probe_matched) = &mut matched.probe {
                probe_matched.clear();
                probe_matched.resize(probe_chunk.capacity(), false);
            }

            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                let first_same_key_row = hash_map.get(probe_key).copied();
                if cond.is_none() && !output.pairs {
                    // Semi and anti joins without a condition: every candidate matches, and only
                    // which rows matched is needed.
                    if first_same_key_row.is_some() {
                        matched.mark_probe(probe_row_id);
                    }
                    if let Some(build_matched) = &mut matched.build {
                        for build_row_id in
                            next_build_row_with_same_key.row_id_iter(first_same_key_row)
                        {
                            shutdown_rx.check()?;
                            build_matched[build_row_id] = true;
                        }
                    }
                    continue;
                }
                let same_key_rows = next_build_row_with_same_key.row_id_iter(first_same_key_row);
                let candidates = match &asof_desc {
                    Some(asof_desc) => Either::Right(
                        Self::find_asof_matched_rows(
                            probe_chunk.row_at_unchecked_vis(probe_row_id),
                            &build_side,
                            same_key_rows,
                            asof_desc,
                        )
                        .into_iter(),
                    ),
                    None => Either::Left(same_key_rows),
                };
                for build_row_id in candidates {
                    shutdown_rx.check()?;
                    if !output.needs_every_candidate() && matched.probe_matched(probe_row_id) {
                        break;
                    }
                    if let Some(cond) = cond {
                        pair_row_ids.push(&matched, probe_row_id, build_row_id);
                        if let Some(pairs) = Self::append_one_row(
                            &mut pair_builder,
                            &probe_chunk,
                            probe_row_id,
                            &build_side[build_row_id.chunk_id()],
                            build_row_id.row_id(),
                        ) && let Some(chunk) = pair_row_ids
                            .evaluate(pairs, cond, &mut matched, output.pairs)
                            .await?
                        {
                            yield chunk
                        }
                    } else {
                        matched.mark(probe_row_id, build_row_id);
                        if output.pairs
                            && let Some(chunk) = Self::append_one_row(
                                &mut pair_builder,
                                &probe_chunk,
                                probe_row_id,
                                &build_side[build_row_id.chunk_id()],
                                build_row_id.row_id(),
                            )
                        {
                            yield chunk
                        }
                    }
                }
            }

            // Evaluate the remaining candidates of this probe chunk, so that its probe rows have
            // their final match state before they are output.
            if let Some(cond) = cond
                && output.probe_rows != RowOutput::Omit
                && let Some(pairs) = pair_builder.consume_all()
                && let Some(chunk) = pair_row_ids
                    .evaluate(pairs, cond, &mut matched, output.pairs)
                    .await?
            {
                yield chunk
            }
            if let Some(probe_matched) = &matched.probe {
                for probe_row_id in probe_chunk.visibility().iter_ones() {
                    if output.probe_rows.emits(probe_matched[probe_row_id])
                        && let Some(chunk) = Self::append_probe_row_output(
                            &mut row_builder,
                            output.probe_rows,
                            &probe_chunk,
                            probe_row_id,
                            &null_build_row,
                        )
                    {
                        yield chunk
                    }
                }
            }
        }

        if let Some(pairs) = pair_builder.consume_all() {
            if let Some(cond) = cond {
                if let Some(chunk) = pair_row_ids
                    .evaluate(pairs, cond, &mut matched, output.pairs)
                    .await?
                {
                    yield chunk
                }
            } else {
                yield pairs
            }
        }
        if let Some(build_matched) = &matched.build {
            for build_row_id in build_matched.all_row_ids() {
                if output.build_rows.emits(build_matched[build_row_id])
                    && let Some(chunk) = Self::append_build_row_output(
                        &mut row_builder,
                        output.build_rows,
                        &build_side[build_row_id.chunk_id()],
                        build_row_id.row_id(),
                        &null_probe_row,
                    )
                {
                    yield chunk
                }
            }
        }
        if let Some(chunk) = row_builder.consume_all() {
            yield chunk
        }
    }

    fn null_row(data_types: &[DataType]) -> DataChunk {
        let columns = data_types
            .iter()
            .map(|data_type| {
                let mut builder = data_type.create_array_builder(1);
                builder.append_null();
                builder.finish().into()
            })
            .collect();
        DataChunk::new(columns, 1)
    }

    fn append_probe_row_output(
        chunk_builder: &mut DataChunkBuilder,
        output: RowOutput,
        probe_chunk: &DataChunk,
        probe_row_id: usize,
        null_build_row: &DataChunk,
    ) -> Option<DataChunk> {
        if output == RowOutput::UnmatchedWithNulls {
            Self::append_one_row(chunk_builder, probe_chunk, probe_row_id, null_build_row, 0)
        } else {
            Self::append_one_probe_row(chunk_builder, probe_chunk, probe_row_id)
        }
    }

    fn append_build_row_output(
        chunk_builder: &mut DataChunkBuilder,
        output: RowOutput,
        build_chunk: &DataChunk,
        build_row_id: usize,
        null_probe_row: &DataChunk,
    ) -> Option<DataChunk> {
        if output == RowOutput::UnmatchedWithNulls {
            Self::append_one_row(chunk_builder, null_probe_row, 0, build_chunk, build_row_id)
        } else {
            Self::append_one_build_row(chunk_builder, build_chunk, build_row_id)
        }
    }

    fn append_one_row(
        chunk_builder: &mut DataChunkBuilder,
        probe_chunk: &DataChunk,
        probe_row_id: usize,
        build_chunk: &DataChunk,
        build_row_id: usize,
    ) -> Option<DataChunk> {
        chunk_builder.append_one_row_from_array_elements(
            probe_chunk.columns().iter().map(|c| c.as_ref()),
            probe_row_id,
            build_chunk.columns().iter().map(|c| c.as_ref()),
            build_row_id,
        )
    }

    fn append_one_probe_row(
        chunk_builder: &mut DataChunkBuilder,
        probe_chunk: &DataChunk,
        probe_row_id: usize,
    ) -> Option<DataChunk> {
        chunk_builder.append_one_row_from_array_elements(
            probe_chunk.columns().iter().map(|c| c.as_ref()),
            probe_row_id,
            empty(),
            0,
        )
    }

    fn append_one_build_row(
        chunk_builder: &mut DataChunkBuilder,
        build_chunk: &DataChunk,
        build_row_id: usize,
    ) -> Option<DataChunk> {
        chunk_builder.append_one_row_from_array_elements(
            empty(),
            0,
            build_chunk.columns().iter().map(|c| c.as_ref()),
            build_row_id,
        )
    }

    fn find_asof_matched_rows(
        probe_row_ref: RowRef<'_>,
        build_side: &[DataChunk],
        build_side_row_iter: RowIdIter<'_>,
        asof_join_condition: &AsOfDesc,
    ) -> Option<RowId> {
        let probe_inequality_value = probe_row_ref.datum_at(asof_join_condition.left_idx);
        if let Some(probe_inequality_scalar) = probe_inequality_value {
            let mut result_row_id: Option<RowId> = None;
            let mut build_row_ref;

            for build_row_id in build_side_row_iter {
                build_row_ref =
                    build_side[build_row_id.chunk_id()].row_at_unchecked_vis(build_row_id.row_id());
                let build_inequality_value = build_row_ref.datum_at(asof_join_condition.right_idx);
                if let Some(build_inequality_scalar) = build_inequality_value {
                    let mut pick_result = |compare: fn(Ordering) -> bool| {
                        if let Some(result_row_id_inner) = result_row_id {
                            let result_row_ref = build_side[result_row_id_inner.chunk_id()]
                                .row_at_unchecked_vis(result_row_id_inner.row_id());
                            let result_inequality_scalar = result_row_ref
                                .datum_at(asof_join_condition.right_idx)
                                .unwrap();
                            if compare(
                                probe_inequality_scalar.default_cmp(&build_inequality_scalar),
                            ) && compare(
                                build_inequality_scalar.default_cmp(&result_inequality_scalar),
                            ) {
                                result_row_id = Some(build_row_id);
                            }
                        } else if compare(
                            probe_inequality_scalar.default_cmp(&build_inequality_scalar),
                        ) {
                            result_row_id = Some(build_row_id);
                        }
                    };
                    match asof_join_condition.inequality_type {
                        AsOfInequalityType::Lt => {
                            pick_result(Ordering::is_lt);
                        }
                        AsOfInequalityType::Le => {
                            pick_result(Ordering::is_le);
                        }
                        AsOfInequalityType::Gt => {
                            pick_result(Ordering::is_gt);
                        }
                        AsOfInequalityType::Ge => {
                            pick_result(Ordering::is_ge);
                        }
                    }
                }
            }
            result_row_id
        } else {
            None
        }
    }
}

impl BoxedExecutorBuilder for HashJoinExecutor<()> {
    async fn new_boxed_executor(
        context: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [left_child, right_child]: [_; 2] = inputs.try_into().unwrap();

        let hash_join_node = try_match_expand!(
            context.plan_node().get_node_body().unwrap(),
            NodeBody::HashJoin
        )?;

        let join_type = JoinType::from_prost(hash_join_node.get_join_type()?);

        let cond = match hash_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let left_key_idxs = hash_join_node
            .get_left_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect_vec();
        let right_key_idxs = hash_join_node
            .get_right_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect_vec();

        ensure!(left_key_idxs.len() == right_key_idxs.len());

        let right_data_types = right_child.schema().data_types();
        let right_key_types = right_key_idxs
            .iter()
            .map(|&idx| right_data_types[idx].clone())
            .collect_vec();

        let output_indices: Vec<usize> = hash_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let identity = context.plan_node().get_identity().clone();

        let asof_desc = hash_join_node
            .asof_desc
            .map(|desc| AsOfDesc::from_protobuf(&desc))
            .transpose()?;

        Ok(HashJoinExecutorArgs {
            join_type,
            output_indices,
            probe_side_source: left_child,
            build_side_source: right_child,
            probe_key_idxs: left_key_idxs,
            build_key_idxs: right_key_idxs,
            null_matched: hash_join_node.get_null_safe().clone(),
            cond,
            identity: identity.clone(),
            right_key_types,
            chunk_size: context.context().get_config().developer.chunk_size,
            asof_desc,
            spill_backend: if context.context().get_config().enable_spill {
                Some(Disk)
            } else {
                None
            },
            spill_metrics: context.context().spill_metrics(),
            shutdown_rx: context.shutdown_rx().clone(),
            mem_ctx: context.context().create_executor_mem_context(&identity),
        }
        .dispatch())
    }
}

struct HashJoinExecutorArgs {
    join_type: JoinType,
    output_indices: Vec<usize>,
    probe_side_source: BoxedExecutor,
    build_side_source: BoxedExecutor,
    probe_key_idxs: Vec<usize>,
    build_key_idxs: Vec<usize>,
    null_matched: Vec<bool>,
    cond: Option<BoxedExpression>,
    identity: String,
    right_key_types: Vec<DataType>,
    chunk_size: usize,
    asof_desc: Option<AsOfDesc>,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    shutdown_rx: ShutdownToken,
    mem_ctx: MemoryContext,
}

impl HashKeyDispatcher for HashJoinExecutorArgs {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashJoinExecutor::<K>::new(
            self.join_type,
            self.output_indices,
            self.probe_side_source,
            self.build_side_source,
            self.probe_key_idxs,
            self.build_key_idxs,
            self.null_matched,
            self.cond.map(Arc::new),
            self.identity,
            self.chunk_size,
            self.asof_desc,
            self.spill_backend,
            self.spill_metrics,
            self.shutdown_rx,
            self.mem_ctx,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.right_key_types
    }
}

impl<K> HashJoinExecutor<K> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        join_type: JoinType,
        output_indices: Vec<usize>,
        probe_side_source: BoxedExecutor,
        build_side_source: BoxedExecutor,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        null_matched: Vec<bool>,
        cond: Option<Arc<BoxedExpression>>,
        identity: String,
        chunk_size: usize,
        asof_desc: Option<AsOfDesc>,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        shutdown_rx: ShutdownToken,
        mem_ctx: MemoryContext,
    ) -> Self {
        Self::new_inner(
            join_type,
            output_indices,
            probe_side_source,
            build_side_source,
            probe_key_idxs,
            build_key_idxs,
            null_matched,
            cond,
            identity,
            chunk_size,
            asof_desc,
            SpillContext::new(spill_backend, spill_metrics),
            shutdown_rx,
            mem_ctx,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn new_inner(
        join_type: JoinType,
        output_indices: Vec<usize>,
        probe_side_source: BoxedExecutor,
        build_side_source: BoxedExecutor,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        null_matched: Vec<bool>,
        cond: Option<Arc<BoxedExpression>>,
        identity: String,
        chunk_size: usize,
        asof_desc: Option<AsOfDesc>,
        spill: SpillContext,
        shutdown_rx: ShutdownToken,
        mem_ctx: MemoryContext,
    ) -> Self {
        assert_eq!(probe_key_idxs.len(), build_key_idxs.len());
        assert_eq!(probe_key_idxs.len(), null_matched.len());
        let original_schema = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => probe_side_source.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => build_side_source.schema().clone(),
            _ => Schema::from_iter(
                probe_side_source
                    .schema()
                    .fields()
                    .iter()
                    .chain(build_side_source.schema().fields().iter())
                    .cloned(),
            ),
        };
        let schema = Schema::from_iter(
            output_indices
                .iter()
                .map(|&idx| original_schema[idx].clone()),
        );
        Self {
            join_type,
            original_schema,
            schema,
            output_indices,
            probe_side_source,
            build_side_source,
            probe_key_idxs,
            build_key_idxs,
            null_matched,
            cond,
            identity,
            chunk_size,
            asof_desc,
            shutdown_rx,
            spill,
            mem_ctx,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use futures_async_stream::for_await;
    use itertools::Itertools;
    use risingwave_common::array::{Array, ArrayBuilderImpl, DataChunk, I32Array, I64Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::Key32;
    use risingwave_common::memory::MemoryContext;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::memcmp_encoding::encode_chunk;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_expr::expr::{BoxedExpression, build_from_pretty};

    use super::{AsOfDesc, AsOfInequalityType, ChunkedData, HashJoinExecutor, JoinType, RowId};
    use crate::error::{BatchError, Result};
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{BoxedExecutor, Executor};
    use crate::monitor::BatchSpillMetrics;
    use crate::spill::spill_op::SpillBackend;
    use crate::task::ShutdownToken;

    const CHUNK_SIZE: usize = 1024;

    struct DataChunkMerger {
        array_builders: Vec<ArrayBuilderImpl>,
        array_len: usize,
    }

    impl DataChunkMerger {
        fn new(data_types: Vec<DataType>) -> Result<Self> {
            let array_builders = data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(CHUNK_SIZE))
                .collect();

            Ok(Self {
                array_builders,
                array_len: 0,
            })
        }

        fn append(&mut self, data_chunk: &DataChunk) -> Result<()> {
            ensure!(self.array_builders.len() == data_chunk.dimension());
            for idx in 0..self.array_builders.len() {
                self.array_builders[idx].append_array(data_chunk.column_at(idx));
            }
            self.array_len += data_chunk.capacity();

            Ok(())
        }

        fn finish(self) -> Result<DataChunk> {
            let columns = self
                .array_builders
                .into_iter()
                .map(|b| b.finish().into())
                .collect();

            Ok(DataChunk::new(columns, self.array_len))
        }
    }

    /// Sort each row in the data chunk and compare with the rows in the data chunk.
    fn compare_data_chunk_with_rowsort(left: &DataChunk, right: &DataChunk) -> bool {
        assert!(left.is_vis_compacted());
        assert!(right.is_vis_compacted());

        if left.cardinality() != right.cardinality() {
            return false;
        }

        // Sort and compare
        let column_orders = (0..left.columns().len())
            .map(|i| ColumnOrder::new(i, OrderType::ascending()))
            .collect_vec();
        let left_encoded_chunk = encode_chunk(left, &column_orders).unwrap();
        let mut sorted_left = left_encoded_chunk
            .into_iter()
            .enumerate()
            .map(|(row_id, row)| (left.row_at_unchecked_vis(row_id), row))
            .collect_vec();
        sorted_left.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        let right_encoded_chunk = encode_chunk(right, &column_orders).unwrap();
        let mut sorted_right = right_encoded_chunk
            .into_iter()
            .enumerate()
            .map(|(row_id, row)| (right.row_at_unchecked_vis(row_id), row))
            .collect_vec();
        sorted_right.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        sorted_left
            .into_iter()
            .map(|(row, _)| row)
            .zip_eq_debug(sorted_right.into_iter().map(|(row, _)| row))
            .all(|(row1, row2)| row1 == row2)
    }

    struct TestFixture {
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, null), (null, 8.4::FLOAT), (3, 3.9::FLOAT), (null, null),
    /// (4, 6.6::FLOAT), (3, null), (null, 0.7::FLOAT), (5, null), (null, 5.5::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (8, 6.1::REAL), (2, null), (null, 8.9::REAL), (3, null), (null, 3.5::REAL),
    /// (6, null), (4, 7.5::REAL), (6, null), (null, 8::REAL), (7, null),
    /// (null, 9.1::REAL), (9, null), (3, 3.7::REAL), (9, null), (null, 9.6::REAL),
    /// (100, null), (null, 8.18::REAL), (200, null);
    /// ```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![DataType::Int32, DataType::Float32],
                right_types: vec![DataType::Int32, DataType::Float64],
                join_type,
            }
        }

        fn create_left_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float32),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i f
                 1 6.1
                 2 .
                 . 8.4
                 3 3.9
                 . .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i f
                 4 6.6
                 3 .
                 . 0.7
                 5 .
                 . 5.5",
            ));

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float64),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i F
                 8 6.1
                 2 .
                 . 8.9
                 3 .
                 . 3.5
                 6 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i F
                 4 7.5
                 6 .
                 . 8
                 7 .
                 . 9.1
                 9 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "  i F
                   3 3.7
                   9 .
                   . 9.6
                 100 .
                   . 8.18
                 200 .   ",
            ));

            Box::new(executor)
        }

        fn output_data_types(&self) -> Vec<DataType> {
            let join_type = self.join_type;
            if join_type.keep_all() {
                [self.left_types.clone(), self.right_types.clone()].concat()
            } else if join_type.keep_left() {
                self.left_types.clone()
            } else if join_type.keep_right() {
                self.right_types.clone()
            } else {
                unreachable!()
            }
        }

        fn create_cond() -> BoxedExpression {
            build_from_pretty("(less_than:boolean $1:float4 $3:float8)")
        }

        fn create_join_executor_with_chunk_size_and_executors(
            &self,
            has_non_equi_cond: bool,
            null_safe: bool,
            chunk_size: usize,
            left_child: BoxedExecutor,
            right_child: BoxedExecutor,
            shutdown_rx: ShutdownToken,
            parent_mem_ctx: Option<MemoryContext>,
            test_spill: bool,
        ) -> BoxedExecutor {
            let join_type = self.join_type;

            let output_indices = (0..match join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => left_child.schema().fields().len(),
                JoinType::RightSemi | JoinType::RightAnti => right_child.schema().fields().len(),
                _ => left_child.schema().fields().len() + right_child.schema().fields().len(),
            })
                .collect();

            let cond = if has_non_equi_cond {
                Some(Self::create_cond().into())
            } else {
                None
            };

            let mem_ctx = if test_spill {
                MemoryContext::new_with_mem_limit(
                    parent_mem_ctx,
                    LabelGuardedIntGauge::test_int_gauge::<4>(),
                    0,
                )
            } else {
                MemoryContext::new(parent_mem_ctx, LabelGuardedIntGauge::test_int_gauge::<4>())
            };
            Box::new(HashJoinExecutor::<Key32>::new(
                join_type,
                output_indices,
                left_child,
                right_child,
                vec![0],
                vec![0],
                vec![null_safe],
                cond,
                "HashJoinExecutor".to_owned(),
                chunk_size,
                None,
                if test_spill {
                    Some(SpillBackend::Memory)
                } else {
                    None
                },
                BatchSpillMetrics::for_test(),
                shutdown_rx,
                mem_ctx,
            ))
        }

        async fn do_test(&self, expected: DataChunk, has_non_equi_cond: bool, null_safe: bool) {
            let left_executor = self.create_left_executor();
            let right_executor = self.create_right_executor();
            self.do_test_with_chunk_size_and_executors(
                expected.clone(),
                has_non_equi_cond,
                null_safe,
                self::CHUNK_SIZE,
                left_executor,
                right_executor,
                false,
            )
            .await;

            // Test spill
            let left_executor = self.create_left_executor();
            let right_executor = self.create_right_executor();
            self.do_test_with_chunk_size_and_executors(
                expected,
                has_non_equi_cond,
                null_safe,
                self::CHUNK_SIZE,
                left_executor,
                right_executor,
                true,
            )
            .await;
        }

        async fn do_test_with_chunk_size_and_executors(
            &self,
            expected: DataChunk,
            has_non_equi_cond: bool,
            null_safe: bool,
            chunk_size: usize,
            left_executor: BoxedExecutor,
            right_executor: BoxedExecutor,
            test_spill: bool,
        ) {
            let parent_mem_context =
                MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);

            {
                let join_executor = self.create_join_executor_with_chunk_size_and_executors(
                    has_non_equi_cond,
                    null_safe,
                    chunk_size,
                    left_executor,
                    right_executor,
                    ShutdownToken::empty(),
                    Some(parent_mem_context.clone()),
                    test_spill,
                );

                let mut data_chunk_merger = DataChunkMerger::new(self.output_data_types()).unwrap();

                let fields = &join_executor.schema().fields;

                if self.join_type.keep_all() {
                    assert_eq!(fields[1].data_type, DataType::Float32);
                    assert_eq!(fields[3].data_type, DataType::Float64);
                } else if self.join_type.keep_left() {
                    assert_eq!(fields[1].data_type, DataType::Float32);
                } else if self.join_type.keep_right() {
                    assert_eq!(fields[1].data_type, DataType::Float64)
                } else {
                    unreachable!()
                }

                let mut stream = join_executor.execute();

                while let Some(data_chunk) = stream.next().await {
                    let data_chunk = data_chunk.unwrap();
                    let data_chunk = data_chunk.compact_vis();
                    data_chunk_merger.append(&data_chunk).unwrap();
                }

                let result_chunk = data_chunk_merger.finish().unwrap();
                println!("expected: {:?}", expected);
                println!("result: {:?}", result_chunk);

                // TODO: Replace this with unsorted comparison
                // assert_eq!(expected, result_chunk);
                assert!(compare_data_chunk_with_rowsort(&expected, &result_chunk));
            }

            assert_eq!(0, parent_mem_context.get_bytes_used());
        }

        async fn do_test_shutdown(&self, has_non_equi_cond: bool) {
            // Test `ShutdownMsg::Cancel`
            let left_executor = self.create_left_executor();
            let right_executor = self.create_right_executor();
            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            let join_executor = self.create_join_executor_with_chunk_size_and_executors(
                has_non_equi_cond,
                false,
                self::CHUNK_SIZE,
                left_executor,
                right_executor,
                shutdown_rx,
                None,
                false,
            );
            shutdown_tx.cancel();
            #[for_await]
            for chunk in join_executor.execute() {
                assert!(chunk.is_err());
                break;
            }

            // Test `ShutdownMsg::Abort`
            let left_executor = self.create_left_executor();
            let right_executor = self.create_right_executor();
            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            let join_executor = self.create_join_executor_with_chunk_size_and_executors(
                has_non_equi_cond,
                false,
                self::CHUNK_SIZE,
                left_executor,
                right_executor,
                shutdown_rx,
                None,
                false,
            );
            shutdown_tx.abort("test");
            #[for_await]
            for chunk in join_executor.execute() {
                assert!(chunk.is_err());
                break;
            }
        }
    }

    /// Sql:
    /// ```sql
    /// select * from t1 join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   2   .
             3   3.9 3   3.7
             3   3.9 3   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 join t2 on t1.v1 is not distinct from t2.v1;
    /// ```
    #[tokio::test]
    async fn test_null_safe_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2    .  2     .
             .  8.4  .  8.18
             .  8.4  .  9.6
             .  8.4  .  9.1
             .  8.4  .  8
             .  8.4  .  3.5
             .  8.4  .  8.9
             3  3.9  3  3.7
             3  3.9  3     .
             .    .  .  8.18
             .    .  .  9.6
             .    .  .  9.1
             .    .  .  8
             .    .  .  3.5
             .    .  .  8.9
             4  6.6  4  7.5
             3    .  3  3.7
             3    .  3     .
             .  0.7  .  8.18
             .  0.7  .  9.6
             .  0.7  .  9.1
             .  0.7  .  8
             .  0.7  .  3.5
             .  0.7  .  8.9
             .  5.5  .  8.18
             .  5.5  .  9.6
             .  5.5  .  9.1
             .  5.5  .  8
             .  5.5  .  3.5
             .  5.5  .  8.9",
        );

        test_fixture.do_test(expected_chunk, false, true).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_inner_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   6.6 4   7.5",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   6.1 .   .
             2   .   2   .
             .   8.4 .   .
             3   3.9 3   3.7
             3   3.9 3   .
             .   .   .   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   .   .
             3   3.9 .   .
             4   6.6 4   7.5
             3   .   .   .
             1   6.1 .   .
             .   8.4 .   .
             .   .   .   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 right outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   2   .
             3   3.9 3   3.7
             3   3.9 3   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   .   8   6.1
             .   .   .   8.9
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   6.6 4   7.5
             .   .   8   6.1
             .   .   2   .
             .   .   .   8.9
             .   .   3   .
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   3   3.7
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    /// ```sql
    /// select * from t1 full outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   6.1 .   .
             2   .   2   .
             .   8.4 .   .
             3   3.9 3   3.7
             3   3.9 3   .
             .   .   .   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .
             .   .   8   6.1
             .   .   .   8.9
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    /// ```sql
    /// select * from t1 full outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   .   .
             3   3.9 .   .
             4   6.6 4   7.5
             3   .   .   .
             1   6.1 .   .
             .   8.4 .   .
             .   .   .   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .
             .   .   8   6.1
             .   .   2   .
             .   .   .   8.9
             .   .   3   .
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   3   3.7
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             1   6.1
             .   8.4
             .   .
             .   0.7
             5   .
             .   5.5",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    #[tokio::test]
    async fn test_left_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             2   .
             3   3.9
             3   .
             1   6.1
             .   8.4
             .   .
             .   0.7
             5   .
             .   5.5",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             2   .
             3   3.9
             4   6.6
             3   .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    #[tokio::test]
    async fn test_left_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             4   6.6",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    /// Tests handling of edge case:
    /// Match is found for a `probe_row`,
    /// but there are still candidate rows in the iterator for that `probe_row`.
    /// These should not be buffered or we will have duplicate rows in output.
    #[tokio::test]
    async fn test_left_semi_join_with_non_equi_condition_duplicates() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Float32),
            ],
        };

        // Build side
        let mut left_executor = MockExecutor::new(schema);
        left_executor.add(DataChunk::from_pretty(
            "i f
                 1 1.0
                 1 1.0
                 1 1.0
                 1 1.0
                 2 1.0",
        ));

        // Probe side
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut right_executor = MockExecutor::new(schema);
        right_executor.add(DataChunk::from_pretty(
            "i F
                 1 2.0
                 1 2.0
                 1 2.0
                 1 2.0
                 2 2.0",
        ));

        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);
        let expected_chunk = DataChunk::from_pretty(
            "i f
            1 1.0
            1 1.0
            1 1.0
            1 1.0
            2 1.0",
        );

        test_fixture
            .do_test_with_chunk_size_and_executors(
                expected_chunk,
                true,
                false,
                3,
                Box::new(left_executor),
                Box::new(right_executor),
                false,
            )
            .await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             8   6.1
             .   8.9
             .   3.5
             6   .
             6   .
             .   8.0
             7   .
             .   9.1
             9   .
             9   .
             .   9.6
             100 .
             .   8.18
             200 .",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    #[tokio::test]
    async fn test_right_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             8   6.1
             2   .
             .   8.9
             3   .
             .   3.5
             6   .
             6   .
             .   8
             7   .
             .   9.1
             9   .
             3   3.7
             9   .
             .   9.6
             100 .
             .   8.18
             200 .",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             2   .
             3   .
             4   7.5
             3   3.7",
        );

        test_fixture.do_test(expected_chunk, false, false).await;
    }

    #[tokio::test]
    async fn test_right_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             4   7.5",
        );

        test_fixture.do_test(expected_chunk, true, false).await;
    }

    /// Build rows hidden by visibility must not be emitted as unmatched build rows.
    #[tokio::test]
    async fn test_invisible_build_rows_are_not_emitted() {
        let cases = [
            (
                JoinType::RightOuter,
                "i f   i F
                 1 1.0 1 2.0
                 . .   3 4.0",
            ),
            (
                JoinType::FullOuter,
                "i f   i F
                 1 1.0 1 2.0
                 . .   3 4.0",
            ),
            (
                JoinType::RightAnti,
                "i F
                 3 4.0",
            ),
        ];
        for (join_type, expected) in cases {
            for has_non_equi_cond in [false, true] {
                for test_spill in [false, true] {
                    let mut left = MockExecutor::new(Schema::new(vec![
                        Field::unnamed(DataType::Int32),
                        Field::unnamed(DataType::Float32),
                    ]));
                    left.add(DataChunk::from_pretty(
                        "i f
                         1 1.0",
                    ));
                    let mut right = MockExecutor::new(Schema::new(vec![
                        Field::unnamed(DataType::Int32),
                        Field::unnamed(DataType::Float64),
                    ]));
                    right.add(DataChunk::from_pretty(
                        "i F
                         1 2.0
                         2 3.0 D
                         3 4.0",
                    ));
                    TestFixture::with_join_type(join_type)
                        .do_test_with_chunk_size_and_executors(
                            DataChunk::from_pretty(expected),
                            has_non_equi_cond,
                            false,
                            CHUNK_SIZE,
                            Box::new(left),
                            Box::new(right),
                            test_spill,
                        )
                        .await;
                }
            }
        }
    }

    #[test]
    fn test_find_asof_matched_rows_prefers_closest_le_match() {
        // Le: left <= right, so probe <= build.
        // probe=5, builds=[10, 20]. Both satisfy 5<=10 and 5<=20.
        // Closest (minimum build) = 10 at row 0.
        let probe_chunk = DataChunk::from_pretty(
            "i
             5",
        );
        let build_chunk = DataChunk::from_pretty(
            "i
             10
             20",
        );
        let build_side = vec![build_chunk];

        let mut next_row_id = ChunkedData::with_chunk_sizes([2]).unwrap();
        next_row_id[RowId::new(0, 0)] = Some(RowId::new(0, 1));
        next_row_id[RowId::new(0, 1)] = None;

        let asof_desc = AsOfDesc {
            left_idx: 0,
            right_idx: 0,
            inequality_type: AsOfInequalityType::Le,
        };

        let matched = HashJoinExecutor::<Key32>::find_asof_matched_rows(
            probe_chunk.row_at_unchecked_vis(0),
            &build_side,
            next_row_id.row_id_iter(Some(RowId::new(0, 0))),
            &asof_desc,
        );

        assert_eq!(matched, Some(RowId::new(0, 0)));
    }

    #[test]
    fn test_find_asof_matched_rows_prefers_closest_ge_match() {
        // Ge: left >= right, so probe >= build.
        // probe=12, builds=[5, 10]. Both satisfy 12>=5 and 12>=10.
        // Closest (maximum build) = 10 at row 1.
        let probe_chunk = DataChunk::from_pretty(
            "i
             12",
        );
        let build_chunk = DataChunk::from_pretty(
            "i
             5
             10",
        );
        let build_side = vec![build_chunk];

        let mut next_row_id = ChunkedData::with_chunk_sizes([2]).unwrap();
        next_row_id[RowId::new(0, 0)] = Some(RowId::new(0, 1));
        next_row_id[RowId::new(0, 1)] = None;

        let asof_desc = AsOfDesc {
            left_idx: 0,
            right_idx: 0,
            inequality_type: AsOfInequalityType::Ge,
        };

        let matched = HashJoinExecutor::<Key32>::find_asof_matched_rows(
            probe_chunk.row_at_unchecked_vis(0),
            &build_side,
            next_row_id.row_id_iter(Some(RowId::new(0, 0))),
            &asof_desc,
        );

        assert_eq!(matched, Some(RowId::new(0, 1)));
    }

    #[tokio::test]
    async fn test_batch_hash_join_asof_ge_returns_closest_match() {
        let left_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let right_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };

        let mut left_executor = MockExecutor::new(left_schema);
        left_executor.add(DataChunk::from_pretty(
            "i i
             3 12",
        ));

        let mut right_executor = MockExecutor::new(right_schema);
        right_executor.add(DataChunk::from_pretty(
            "i i
             3 5
             3 10",
        ));

        let join_executor = Box::new(HashJoinExecutor::<Key32>::new(
            JoinType::Inner,
            vec![0, 1, 2, 3],
            Box::new(left_executor),
            Box::new(right_executor),
            vec![0],
            vec![0],
            vec![false],
            None,
            "HashJoinExecutor".to_owned(),
            CHUNK_SIZE,
            Some(AsOfDesc {
                left_idx: 1,
                right_idx: 1,
                inequality_type: AsOfInequalityType::Ge,
            }),
            None,
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
            MemoryContext::new(None, LabelGuardedIntGauge::test_int_gauge::<4>()),
        ));

        let mut stream = join_executor.execute();
        let chunk = stream.next().await.unwrap().unwrap().compact_vis();
        let expected = DataChunk::from_pretty(
            "i i i i
             3 12 3 10",
        );

        assert!(compare_data_chunk_with_rowsort(&expected, &chunk));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_shutdown() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;

        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);
        test_fixture.do_test_shutdown(false).await;
        test_fixture.do_test_shutdown(true).await;
    }

    /// All build rows share one key, so re-partitioning never shrinks the spilled partition. The
    /// join gives up after a bounded number of spills instead of re-spilling until the stack
    /// overflows.
    #[tokio::test]
    async fn test_spill_skewed_key_fails_with_out_of_memory() {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
        ]);
        // Large enough that every spilled partition exceeds the size checked against the limit.
        let rows = 1 << 17;
        let mut build_side = MockExecutor::new(schema.clone());
        build_side.add(DataChunk::new(
            vec![
                I32Array::from_iter(std::iter::repeat_n(Some(1), rows)).into_ref(),
                I64Array::from_iter((0..rows as i64).map(Some)).into_ref(),
            ],
            rows,
        ));
        let mut probe_side = MockExecutor::new(schema);
        probe_side.add(DataChunk::from_pretty(
            "i I
             1 1",
        ));
        let join = Box::new(HashJoinExecutor::<Key32>::new(
            JoinType::Inner,
            (0..4).collect(),
            Box::new(probe_side),
            Box::new(build_side),
            vec![0],
            vec![0],
            vec![false],
            None,
            "HashJoinExecutor".to_owned(),
            CHUNK_SIZE,
            None,
            Some(SpillBackend::Memory),
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
            MemoryContext::for_spill_test(),
        ));
        let mut stream = join.execute();
        let error = loop {
            match stream.next().await {
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => panic!("the skewed join should run out of memory"),
            }
        };
        assert!(matches!(error, BatchError::OutOfMemory(_)), "{error:?}");
    }
}
