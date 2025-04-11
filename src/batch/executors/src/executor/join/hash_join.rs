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

use std::cmp::Ordering;
use std::iter;
use std::iter::empty;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, DataChunk, RowRef};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder, FilterByBitmap};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::row::{Row, RowExt, repeat_n};
use risingwave_common::types::{DataType, Datum, DefaultOrd};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::expr::{BoxedExpression, Expression, build_from_prost};
use risingwave_pb::Message;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::data::DataChunk as PbDataChunk;

use super::{AsOfDesc, AsOfInequalityType, ChunkedData, JoinType, RowId};
use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::monitor::BatchSpillMetrics;
use crate::risingwave_common::hash::NullBitmap;
use crate::spill::spill_op::SpillBackend::Disk;
use crate::spill::spill_op::{
    DEFAULT_SPILL_PARTITION_NUM, SPILL_AT_LEAST_MEMORY, SpillBackend, SpillBuildHasher, SpillOp,
};
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

    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    /// The upper bound of memory usage for this executor.
    memory_upper_bound: Option<u64>,

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
    #[allow(clippy::too_many_arguments)]
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

/// State variables used in left outer/semi/anti join and full outer join.
#[derive(Default)]
struct LeftNonEquiJoinState {
    /// The number of columns in probe side.
    probe_column_count: usize,
    /// The offset of the first output row in **current** chunk for each probe side row that has
    /// been processed.
    first_output_row_id: Vec<usize>,
    /// Whether the probe row being processed currently has output rows in **next** output chunk.
    has_more_output_rows: bool,
    /// Whether the probe row being processed currently has matched non-NULL build rows in **last**
    /// output chunk.
    found_matched: bool,
}

/// State variables used in right outer/semi/anti join and full outer join.
#[derive(Default)]
struct RightNonEquiJoinState {
    /// Corresponding build row id for each row in **current** output chunk.
    build_row_ids: Vec<RowId>,
    /// Whether a build row has been matched.
    build_row_matched: ChunkedData<bool>,
}

pub struct JoinSpillManager {
    op: SpillOp,
    partition_num: usize,
    probe_side_writers: Vec<opendal::Writer>,
    build_side_writers: Vec<opendal::Writer>,
    probe_side_chunk_builders: Vec<DataChunkBuilder>,
    build_side_chunk_builders: Vec<DataChunkBuilder>,
    spill_build_hasher: SpillBuildHasher,
    probe_side_data_types: Vec<DataType>,
    build_side_data_types: Vec<DataType>,
    spill_chunk_size: usize,
    spill_metrics: Arc<BatchSpillMetrics>,
}

/// `JoinSpillManager` is used to manage how to write spill data file and read them back.
/// The spill data first need to be partitioned. Each partition contains 2 files: `join_probe_side_file` and `join_build_side_file`.
/// The spill file consume a data chunk and serialize the chunk into a protobuf bytes.
/// Finally, spill file content will look like the below.
/// The file write pattern is append-only and the read pattern is sequential scan.
/// This can maximize the disk IO performance.
///
/// ```text
/// [proto_len]
/// [proto_bytes]
/// ...
/// [proto_len]
/// [proto_bytes]
/// ```
impl JoinSpillManager {
    pub fn new(
        spill_backend: SpillBackend,
        join_identity: &String,
        partition_num: usize,
        probe_side_data_types: Vec<DataType>,
        build_side_data_types: Vec<DataType>,
        spill_chunk_size: usize,
        spill_metrics: Arc<BatchSpillMetrics>,
    ) -> Result<Self> {
        let suffix_uuid = uuid::Uuid::new_v4();
        let dir = format!("/{}-{}/", join_identity, suffix_uuid);
        let op = SpillOp::create(dir, spill_backend)?;
        let probe_side_writers = Vec::with_capacity(partition_num);
        let build_side_writers = Vec::with_capacity(partition_num);
        let probe_side_chunk_builders = Vec::with_capacity(partition_num);
        let build_side_chunk_builders = Vec::with_capacity(partition_num);
        let spill_build_hasher = SpillBuildHasher(suffix_uuid.as_u64_pair().1);
        Ok(Self {
            op,
            partition_num,
            probe_side_writers,
            build_side_writers,
            probe_side_chunk_builders,
            build_side_chunk_builders,
            spill_build_hasher,
            probe_side_data_types,
            build_side_data_types,
            spill_chunk_size,
            spill_metrics,
        })
    }

    pub async fn init_writers(&mut self) -> Result<()> {
        for i in 0..self.partition_num {
            let join_probe_side_partition_file_name = format!("join-probe-side-p{}", i);
            let w = self
                .op
                .writer_with(&join_probe_side_partition_file_name)
                .await?;
            self.probe_side_writers.push(w);

            let join_build_side_partition_file_name = format!("join-build-side-p{}", i);
            let w = self
                .op
                .writer_with(&join_build_side_partition_file_name)
                .await?;
            self.build_side_writers.push(w);
            self.probe_side_chunk_builders.push(DataChunkBuilder::new(
                self.probe_side_data_types.clone(),
                self.spill_chunk_size,
            ));
            self.build_side_chunk_builders.push(DataChunkBuilder::new(
                self.build_side_data_types.clone(),
                self.spill_chunk_size,
            ));
        }
        Ok(())
    }

    pub async fn write_probe_side_chunk(
        &mut self,
        chunk: DataChunk,
        hash_codes: Vec<u64>,
    ) -> Result<()> {
        let (columns, vis) = chunk.into_parts_v2();
        for partition in 0..self.partition_num {
            let new_vis = vis.clone()
                & Bitmap::from_iter(
                    hash_codes
                        .iter()
                        .map(|hash_code| (*hash_code as usize % self.partition_num) == partition),
                );
            let new_chunk = DataChunk::from_parts(columns.clone(), new_vis);
            for output_chunk in self.probe_side_chunk_builders[partition].append_chunk(new_chunk) {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.probe_side_writers[partition].write(len_bytes).await?;
                self.probe_side_writers[partition].write(buf).await?;
            }
        }
        Ok(())
    }

    pub async fn write_build_side_chunk(
        &mut self,
        chunk: DataChunk,
        hash_codes: Vec<u64>,
    ) -> Result<()> {
        let (columns, vis) = chunk.into_parts_v2();
        for partition in 0..self.partition_num {
            let new_vis = vis.clone()
                & Bitmap::from_iter(
                    hash_codes
                        .iter()
                        .map(|hash_code| (*hash_code as usize % self.partition_num) == partition),
                );
            let new_chunk = DataChunk::from_parts(columns.clone(), new_vis);
            for output_chunk in self.build_side_chunk_builders[partition].append_chunk(new_chunk) {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.build_side_writers[partition].write(len_bytes).await?;
                self.build_side_writers[partition].write(buf).await?;
            }
        }
        Ok(())
    }

    pub async fn close_writers(&mut self) -> Result<()> {
        for partition in 0..self.partition_num {
            if let Some(output_chunk) = self.probe_side_chunk_builders[partition].consume_all() {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.probe_side_writers[partition].write(len_bytes).await?;
                self.probe_side_writers[partition].write(buf).await?;
            }

            if let Some(output_chunk) = self.build_side_chunk_builders[partition].consume_all() {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.build_side_writers[partition].write(len_bytes).await?;
                self.build_side_writers[partition].write(buf).await?;
            }
        }

        for mut w in self.probe_side_writers.drain(..) {
            w.close().await?;
        }
        for mut w in self.build_side_writers.drain(..) {
            w.close().await?;
        }
        Ok(())
    }

    async fn read_probe_side_partition(
        &mut self,
        partition: usize,
    ) -> Result<BoxedDataChunkStream> {
        let join_probe_side_partition_file_name = format!("join-probe-side-p{}", partition);
        let r = self
            .op
            .reader_with(&join_probe_side_partition_file_name)
            .await?;
        Ok(SpillOp::read_stream(r, self.spill_metrics.clone()))
    }

    async fn read_build_side_partition(
        &mut self,
        partition: usize,
    ) -> Result<BoxedDataChunkStream> {
        let join_build_side_partition_file_name = format!("join-build-side-p{}", partition);
        let r = self
            .op
            .reader_with(&join_build_side_partition_file_name)
            .await?;
        Ok(SpillOp::read_stream(r, self.spill_metrics.clone()))
    }

    pub async fn estimate_partition_size(&self, partition: usize) -> Result<u64> {
        let join_probe_side_partition_file_name = format!("join-probe-side-p{}", partition);
        let probe_size = self
            .op
            .stat(&join_probe_side_partition_file_name)
            .await?
            .content_length();
        let join_build_side_partition_file_name = format!("join-build-side-p{}", partition);
        let build_size = self
            .op
            .stat(&join_build_side_partition_file_name)
            .await?
            .content_length();
        Ok(probe_size + build_size)
    }

    async fn clear_partition(&mut self, partition: usize) -> Result<()> {
        let join_probe_side_partition_file_name = format!("join-probe-side-p{}", partition);
        self.op.delete(&join_probe_side_partition_file_name).await?;
        let join_build_side_partition_file_name = format!("join-build-side-p{}", partition);
        self.op.delete(&join_build_side_partition_file_name).await?;
        Ok(())
    }
}

impl<K: HashKey> HashJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let mut need_to_spill = false;
        // If the memory upper bound is less than 1MB, we don't need to check memory usage.
        let check_memory = match self.memory_upper_bound {
            Some(upper_bound) => upper_bound > SPILL_AT_LEAST_MEMORY,
            None => true,
        };

        let probe_schema = self.probe_side_source.schema().clone();
        let build_schema = self.build_side_source.schema().clone();
        let probe_data_types = self.probe_side_source.schema().data_types();
        let build_data_types = self.build_side_source.schema().data_types();
        let full_data_types = [probe_data_types.clone(), build_data_types.clone()].concat();

        let mut build_side = Vec::new_in(self.mem_ctx.global_allocator());
        let mut build_row_count = 0;
        let mut build_side_stream = self.build_side_source.execute();
        #[for_await]
        for build_chunk in &mut build_side_stream {
            let build_chunk = build_chunk?;
            if build_chunk.cardinality() > 0 {
                build_row_count += build_chunk.cardinality();
                let chunk_estimated_heap_size = build_chunk.estimated_heap_size();
                // push build_chunk to build_side before checking memory limit, otherwise we will lose that chunk when spilling.
                build_side.push(build_chunk);
                if !self.mem_ctx.add(chunk_estimated_heap_size as i64) && check_memory {
                    if self.spill_backend.is_some() {
                        need_to_spill = true;
                        break;
                    } else {
                        Err(BatchError::OutOfMemory(self.mem_ctx.mem_limit()))?;
                    }
                }
            }
        }
        let mut hash_map = JoinHashMap::with_capacity_and_hasher_in(
            build_row_count,
            PrecomputedBuildHasher,
            self.mem_ctx.global_allocator(),
        );
        let mut next_build_row_with_same_key =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        let null_matched = K::Bitmap::from_bool_vec(self.null_matched.clone());

        let mut mem_added_by_hash_table = 0;
        if !need_to_spill {
            // Build hash map
            for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
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
                        mem_added_by_hash_table += build_key_size;
                        if !self.mem_ctx.add(build_key_size) && check_memory {
                            if self.spill_backend.is_some() {
                                need_to_spill = true;
                                break;
                            } else {
                                Err(BatchError::OutOfMemory(self.mem_ctx.mem_limit()))?;
                            }
                        }
                        next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
                    }
                }
            }
        }

        if need_to_spill {
            // A spilling version of hash join based on the RFC: Spill Hash Join https://github.com/risingwavelabs/rfcs/pull/91
            // When HashJoinExecutor told memory is insufficient, JoinSpillManager will start to partition the hash table and spill to disk.
            // After spilling the hash table, JoinSpillManager will consume all chunks from its build side input executor and probe side input executor.
            // Finally, we would get e.g. 20 partitions. Each partition should contain a portion of the original build side input and probr side input data.
            // A sub HashJoinExecutor would be used to consume each partition one by one.
            // If memory is still not enough in the sub HashJoinExecutor, it will spill its inputs recursively.
            info!(
                "batch hash join executor {} starts to spill out",
                &self.identity
            );
            let mut join_spill_manager = JoinSpillManager::new(
                self.spill_backend.clone().unwrap(),
                &self.identity,
                DEFAULT_SPILL_PARTITION_NUM,
                probe_data_types.clone(),
                build_data_types.clone(),
                self.chunk_size,
                self.spill_metrics.clone(),
            )?;
            join_spill_manager.init_writers().await?;

            // Release memory occupied by the hash map
            self.mem_ctx.add(-mem_added_by_hash_table);
            drop(hash_map);
            drop(next_build_row_with_same_key);

            // Spill buffered build side chunks
            for chunk in build_side {
                // Release the memory occupied by the buffered chunks
                self.mem_ctx.add(-(chunk.estimated_heap_size() as i64));
                let hash_codes = chunk.get_hash_values(
                    self.build_key_idxs.as_slice(),
                    join_spill_manager.spill_build_hasher,
                );
                join_spill_manager
                    .write_build_side_chunk(
                        chunk,
                        hash_codes
                            .into_iter()
                            .map(|hash_code| hash_code.value())
                            .collect(),
                    )
                    .await?;
            }

            // Spill build side chunks
            #[for_await]
            for chunk in build_side_stream {
                let chunk = chunk?;
                let hash_codes = chunk.get_hash_values(
                    self.build_key_idxs.as_slice(),
                    join_spill_manager.spill_build_hasher,
                );
                join_spill_manager
                    .write_build_side_chunk(
                        chunk,
                        hash_codes
                            .into_iter()
                            .map(|hash_code| hash_code.value())
                            .collect(),
                    )
                    .await?;
            }

            // Spill probe side chunks
            #[for_await]
            for chunk in self.probe_side_source.execute() {
                let chunk = chunk?;
                let hash_codes = chunk.get_hash_values(
                    self.probe_key_idxs.as_slice(),
                    join_spill_manager.spill_build_hasher,
                );
                join_spill_manager
                    .write_probe_side_chunk(
                        chunk,
                        hash_codes
                            .into_iter()
                            .map(|hash_code| hash_code.value())
                            .collect(),
                    )
                    .await?;
            }

            join_spill_manager.close_writers().await?;

            // Process each partition one by one.
            for i in 0..join_spill_manager.partition_num {
                let partition_size = join_spill_manager.estimate_partition_size(i).await?;
                let probe_side_stream = join_spill_manager.read_probe_side_partition(i).await?;
                let build_side_stream = join_spill_manager.read_build_side_partition(i).await?;

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
                    self.spill_backend.clone(),
                    self.spill_metrics.clone(),
                    Some(partition_size),
                    self.shutdown_rx.clone(),
                    self.mem_ctx.clone(),
                );

                debug!(
                    "create sub_hash_join {} for hash_join {} to spill",
                    sub_hash_join_executor.identity, self.identity
                );

                let sub_hash_join_executor = Box::new(sub_hash_join_executor).execute();

                #[for_await]
                for chunk in sub_hash_join_executor {
                    let chunk = chunk?;
                    yield chunk;
                }

                // Clear files of the current partition.
                join_spill_manager.clear_partition(i).await?;
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

            if let Some(cond) = self.cond.as_ref()
                && !params.is_asof_join()
            {
                let stream = match self.join_type {
                    JoinType::Inner => Self::do_inner_join_with_non_equi_condition(params, cond),
                    JoinType::LeftOuter => {
                        Self::do_left_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftSemi => {
                        Self::do_left_semi_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftAnti => {
                        Self::do_left_anti_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::RightOuter => {
                        Self::do_right_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::RightSemi => {
                        Self::do_right_semi_anti_join_with_non_equi_condition::<false>(params, cond)
                    }
                    JoinType::RightAnti => {
                        Self::do_right_semi_anti_join_with_non_equi_condition::<true>(params, cond)
                    }
                    JoinType::FullOuter => {
                        Self::do_full_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::AsOfInner | JoinType::AsOfLeftOuter => {
                        unreachable!("AsOf join should not reach here")
                    }
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
                    JoinType::Inner | JoinType::AsOfInner => Self::do_inner_join(params),
                    JoinType::LeftOuter | JoinType::AsOfLeftOuter => {
                        Self::do_left_outer_join(params)
                    }
                    JoinType::LeftSemi => Self::do_left_semi_anti_join::<false>(params),
                    JoinType::LeftAnti => Self::do_left_semi_anti_join::<true>(params),
                    JoinType::RightOuter => Self::do_right_outer_join(params),
                    JoinType::RightSemi => Self::do_right_semi_anti_join::<false>(params),
                    JoinType::RightAnti => Self::do_right_semi_anti_join::<true>(params),
                    JoinType::FullOuter => Self::do_full_outer_join(params),
                };
                #[for_await]
                for chunk in stream {
                    yield chunk?.project(&self.output_indices)
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_inner_join(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            asof_desc,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                let build_side_row_iter =
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied());
                if let Some(asof_desc) = &asof_desc {
                    if let Some(build_row_id) = Self::find_asof_matched_rows(
                        probe_chunk.row_at_unchecked_vis(probe_row_id),
                        &build_side,
                        build_side_row_iter,
                        asof_desc,
                    ) {
                        shutdown_rx.check()?;
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            &build_side[build_row_id.chunk_id()],
                            build_row_id.row_id(),
                        ) {
                            yield spilled
                        }
                    }
                } else {
                    for build_row_id in build_side_row_iter {
                        shutdown_rx.check()?;
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            yield spilled
                        }
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_inner_join_with_non_equi_condition(
        params: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        #[for_await]
        for chunk in Self::do_inner_join(params) {
            let mut chunk = chunk?;
            chunk.set_visibility(cond.eval(&chunk).await?.as_bool().iter().collect());
            yield chunk
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_left_outer_join(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            asof_desc,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    let build_side_row_iter =
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id));
                    if let Some(asof_desc) = &asof_desc {
                        if let Some(build_row_id) = Self::find_asof_matched_rows(
                            probe_chunk.row_at_unchecked_vis(probe_row_id),
                            &build_side,
                            build_side_row_iter,
                            asof_desc,
                        ) {
                            shutdown_rx.check()?;
                            if let Some(spilled) = Self::append_one_row(
                                &mut chunk_builder,
                                &probe_chunk,
                                probe_row_id,
                                &build_side[build_row_id.chunk_id()],
                                build_row_id.row_id(),
                            ) {
                                yield spilled
                            }
                        } else {
                            shutdown_rx.check()?;
                            let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                            if let Some(spilled) = Self::append_one_row_with_null_build_side(
                                &mut chunk_builder,
                                probe_row,
                                build_data_types.len(),
                            ) {
                                yield spilled
                            }
                        }
                    } else {
                        for build_row_id in build_side_row_iter {
                            shutdown_rx.check()?;
                            let build_chunk = &build_side[build_row_id.chunk_id()];
                            if let Some(spilled) = Self::append_one_row(
                                &mut chunk_builder,
                                &probe_chunk,
                                probe_row_id,
                                build_chunk,
                                build_row_id.row_id(),
                            ) {
                                yield spilled
                            }
                        }
                    }
                } else {
                    shutdown_rx.check()?;
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    ) {
                        yield spilled
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_left_outer_join_with_non_equi_condition(
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
            ..
        }: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types.clone(), chunk_size);
        let mut remaining_chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut non_equi_state = LeftNonEquiJoinState {
            probe_column_count: probe_data_types.len(),
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());

                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        shutdown_rx.check()?;
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_left_outer_join_non_equi_condition(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )
                            .await?
                        }
                    }
                } else {
                    shutdown_rx.check()?;
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut remaining_chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    ) {
                        yield spilled
                    }
                }
            }
        }
        non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all() {
            yield Self::process_left_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )
            .await?
        }

        if let Some(spilled) = remaining_chunk_builder.consume_all() {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_left_semi_anti_join<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            hash_map,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(probe_data_types, chunk_size);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                shutdown_rx.check()?;
                if !ANTI_JOIN {
                    if hash_map.contains_key(probe_key) {
                        if let Some(spilled) = Self::append_one_probe_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                        ) {
                            yield spilled
                        }
                    }
                } else if hash_map.get(probe_key).is_none() {
                    if let Some(spilled) =
                        Self::append_one_probe_row(&mut chunk_builder, &probe_chunk, probe_row_id)
                    {
                        yield spilled
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }

    /// High-level idea:
    /// 1. For each probe_row, append candidate rows to buffer.
    ///    Candidate rows: Those satisfying equi_predicate (==).
    /// 2. If buffer becomes full, process it.
    ///    Apply non_equi_join predicates e.g. `>=`, `<=` to filter rows.
    ///    Track if probe_row is matched to avoid duplicates.
    /// 3. If we matched probe_row in spilled chunk,
    ///    stop appending its candidate rows,
    ///    to avoid matching it again in next spilled chunk.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_left_semi_join_with_non_equi_condition<'a>(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
        cond: &'a BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut non_equi_state = LeftNonEquiJoinState::default();

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                non_equi_state.found_matched = false;
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());

                    for build_row_id in
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id))
                    {
                        shutdown_rx.check()?;
                        if non_equi_state.found_matched {
                            break;
                        }
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            yield Self::process_left_semi_anti_join_non_equi_condition::<false>(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )
                            .await?
                        }
                    }
                }
            }
        }

        // Process remaining rows in buffer
        if let Some(spilled) = chunk_builder.consume_all() {
            yield Self::process_left_semi_anti_join_non_equi_condition::<false>(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )
            .await?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_left_anti_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut remaining_chunk_builder = DataChunkBuilder::new(probe_data_types, chunk_size);
        let mut non_equi_state = LeftNonEquiJoinState::default();

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());
                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        shutdown_rx.check()?;
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_left_semi_anti_join_non_equi_condition::<true>(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )
                            .await?
                        }
                    }
                } else if let Some(spilled) = Self::append_one_probe_row(
                    &mut remaining_chunk_builder,
                    &probe_chunk,
                    probe_row_id,
                ) {
                    yield spilled
                }
            }
        }
        non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all() {
            yield Self::process_left_semi_anti_join_non_equi_condition::<true>(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )
            .await?
        }
        if let Some(spilled) = remaining_chunk_builder.consume_all() {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_right_outer_join(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    shutdown_rx.check()?;
                    build_row_matched[build_row_id] = true;
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    ) {
                        yield spilled
                    }
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_right_outer_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    shutdown_rx.check()?;
                    non_equi_state.build_row_ids.push(build_row_id);
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    ) {
                        yield Self::process_right_outer_join_non_equi_condition(
                            spilled,
                            cond.as_ref(),
                            &mut non_equi_state,
                        )
                        .await?
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield Self::process_right_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )
            .await?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &non_equi_state.build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_right_semi_anti_join<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(build_data_types, chunk_size);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for probe_key in probe_keys.iter().filter_by_bitmap(probe_chunk.visibility()) {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    shutdown_rx.check()?;
                    build_row_matched[build_row_id] = true;
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_semi_anti_join::<ANTI_JOIN>(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_right_semi_anti_join_with_non_equi_condition<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            chunk_size,
            shutdown_rx,
            ..
        }: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut remaining_chunk_builder = DataChunkBuilder::new(build_data_types, chunk_size);
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    shutdown_rx.check()?;
                    non_equi_state.build_row_ids.push(build_row_id);
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    ) {
                        Self::process_right_semi_anti_join_non_equi_condition(
                            spilled,
                            cond.as_ref(),
                            &mut non_equi_state,
                        )
                        .await?
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            Self::process_right_semi_anti_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )
            .await?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_semi_anti_join::<ANTI_JOIN>(
            &mut remaining_chunk_builder,
            &build_side,
            &non_equi_state.build_row_matched,
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_full_outer_join(
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
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    for build_row_id in
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id))
                    {
                        shutdown_rx.check()?;
                        build_row_matched[build_row_id] = true;
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            yield spilled
                        }
                    }
                } else {
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    ) {
                        yield spilled
                    }
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn do_full_outer_join_with_non_equi_condition(
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
            ..
        }: EquiJoinParams<K>,
        cond: &BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::new(full_data_types.clone(), chunk_size);
        let mut remaining_chunk_builder = DataChunkBuilder::new(full_data_types, chunk_size);
        let mut left_non_equi_state = LeftNonEquiJoinState {
            probe_column_count: probe_data_types.len(),
            ..Default::default()
        };
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut right_non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build_many(&probe_key_idxs, &probe_chunk);
            for (probe_row_id, probe_key) in probe_keys
                .iter()
                .enumerate()
                .filter_by_bitmap(probe_chunk.visibility())
            {
                left_non_equi_state.found_matched = false;
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    left_non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());
                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        shutdown_rx.check()?;
                        right_non_equi_state.build_row_ids.push(build_row_id);
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        ) {
                            left_non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_full_outer_join_non_equi_condition(
                                spilled,
                                cond.as_ref(),
                                &mut left_non_equi_state,
                                &mut right_non_equi_state,
                            )
                            .await?
                        }
                    }
                } else {
                    shutdown_rx.check()?;
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut remaining_chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    ) {
                        yield spilled
                    }
                }
            }
        }
        left_non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all() {
            yield Self::process_full_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut left_non_equi_state,
                &mut right_non_equi_state,
            )
            .await?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut remaining_chunk_builder,
            &build_side,
            &right_non_equi_state.build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    /// Process output chunk for left outer join when non-equi condition is presented.
    ///
    /// # Arguments
    /// * `chunk` - Output chunk from `do_left_outer_join_with_non_equi_condition`, containing:
    ///     - Concatenation of probe row and its corresponding build row according to the hash map.
    ///     - Concatenation of probe row and `NULL` build row, if there is no matched build row
    ///       found for the probe row.
    /// * `cond` - Non-equi join condition.
    /// * `probe_column_count` - The number of columns in the probe side.
    /// * `first_output_row_id` - The offset of the first output row in `chunk` for each probe side
    ///   row that has been processed.
    /// * `has_more_output_rows` - Whether the probe row being processed currently has output rows
    ///   in next output chunk.
    /// * `found_matched` - Whether the probe row being processed currently has matched non-NULL
    ///   build rows in last output chunk.
    ///
    /// # Examples
    /// Assume we have two tables `t1` and `t2` as probe side and build side, respectively.
    /// ```sql
    /// CREATE TABLE t1 (v1 int, v2 int);
    /// CREATE TABLE t2 (v3 int);
    /// ```
    ///
    /// Now we de left outer join on `t1` and `t2`, as the following query shows:
    /// ```sql
    /// SELECT * FROM t1 LEFT JOIN t2 ON t1.v1 = t2.v3 AND t1.v2 <> t2.v3;
    /// ```
    ///
    /// Assume the chunk builder in `do_left_outer_join_with_non_equi_condition` has buffer size 5,
    /// and we have the following chunk as the first output ('-' represents NULL).
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | 1 |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | 3 |
    /// | 4 | 3 | 3 | 3 |
    ///
    /// We have the following precondition:
    /// ```ignore
    /// assert_eq!(probe_column_count, 2);
    /// assert_eq!(first_out_row_id, vec![0, 1, 2, 3]);
    /// assert_eq!(has_more_output_rows);
    /// assert_eq!(!found_matched);
    /// ```
    ///
    /// In `process_left_outer_join_non_equi_condition`, we transform the chunk in following steps.
    ///
    /// 1. Evaluate the non-equi condition on the chunk. Here the condition is `t1.v2 <> t2.v3`.
    ///
    /// We get the result array:
    ///
    /// | offset | value |
    /// | --- | --- |
    /// | 0 | true |
    /// | 1 | false |
    /// | 2 | false |
    /// | 3 | false |
    /// | 4 | false |
    ///
    /// 2. Set the build side columns to NULL if the corresponding result value is false.
    ///
    /// The chunk is changed to:
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | - |
    /// | 4 | 3 | 3 | - |
    ///
    /// 3. Remove duplicate rows with NULL build side. This is done by setting the visibility bitmap
    ///    of the chunk.
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | ~~3~~ | ~~3~~ | ~~-~~ |
    /// | 4 | ~~3~~ | ~~3~~ | ~~-~~ |
    ///
    /// For the probe row being processed currently (`(3, 3)` here), we don't have output rows with
    /// non-NULL build side, so we set `found_matched` to false.
    ///
    /// In `do_left_outer_join_with_non_equi_condition`, we have next output chunk as follows:
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 3 | 3 | 3 |
    /// | 1 | 3 | 3 | 3 |
    /// | 2 | 5 | 5 | - |
    /// | 3 | 5 | 3 | - |
    /// | 4 | 5 | 3 | - |
    ///
    /// This time We have the following precondition:
    /// ```ignore
    /// assert_eq!(probe_column_count, 2);
    /// assert_eq!(first_out_row_id, vec![2, 3]);
    /// assert_eq!(!has_more_output_rows);
    /// assert_eq!(!found_matched);
    /// ```
    ///
    /// The transformed chunk is as follows after the same steps.
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | ~~3~~ | ~~3~~ | ~~3~~ |
    /// | 1 | 3 | 3 | - |
    /// | 2 | 5 | 5 | - |
    /// | 3 | 5 | 3 | - |
    /// | 4 | ~~5~~ | ~~3~~ | ~~-~~ |
    ///
    /// After we add these chunks to output chunk builder in `do_execute`, we get the final output:
    ///
    /// Chunk 1
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | - |
    /// | 4 | 5 | 5 | - |
    ///
    /// Chunk 2
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 5 | 3 | - |
    ///
    ///
    /// For more information about how `process_*_join_non_equi_condition` work, see their unit
    /// tests.
    async fn process_left_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        LeftNonEquiJoinState {
            probe_column_count,
            first_output_row_id,
            has_more_output_rows,
            found_matched,
        }: &mut LeftNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk).await?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .nullify_build_side_for_non_equi_condition(&filter, *probe_column_count)
            .remove_duplicate_rows_for_left_outer_join(
                &filter,
                first_output_row_id,
                *has_more_output_rows,
                found_matched,
            )
            .take())
    }

    /// Filters for candidate rows which satisfy `non_equi` predicate.
    /// Removes duplicate rows.
    async fn process_left_semi_anti_join_non_equi_condition<const ANTI_JOIN: bool>(
        chunk: DataChunk,
        cond: &dyn Expression,
        LeftNonEquiJoinState {
            first_output_row_id,
            found_matched,
            has_more_output_rows,
            ..
        }: &mut LeftNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk).await?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .remove_duplicate_rows_for_left_semi_anti_join::<ANTI_JOIN>(
                &filter,
                first_output_row_id,
                *has_more_output_rows,
                found_matched,
            )
            .take())
    }

    async fn process_right_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk).await?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .remove_duplicate_rows_for_right_outer_join(&filter, build_row_ids, build_row_matched)
            .take())
    }

    async fn process_right_semi_anti_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Result<()> {
        let filter = cond.eval(&chunk).await?.as_bool().iter().collect();
        DataChunkMutator(chunk).remove_duplicate_rows_for_right_semi_anti_join(
            &filter,
            build_row_ids,
            build_row_matched,
        );
        Ok(())
    }

    async fn process_full_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        left_non_equi_state: &mut LeftNonEquiJoinState,
        right_non_equi_state: &mut RightNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk).await?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .nullify_build_side_for_non_equi_condition(
                &filter,
                left_non_equi_state.probe_column_count,
            )
            .remove_duplicate_rows_for_full_outer_join(
                &filter,
                left_non_equi_state,
                right_non_equi_state,
            )
            .take())
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn handle_remaining_build_rows_for_right_outer_join<'a>(
        chunk_builder: &'a mut DataChunkBuilder,
        build_side: &'a [DataChunk],
        build_row_matched: &'a ChunkedData<bool>,
        probe_column_count: usize,
    ) {
        for build_row_id in build_row_matched
            .all_row_ids()
            .filter(|build_row_id| !build_row_matched[*build_row_id])
        {
            let build_row =
                build_side[build_row_id.chunk_id()].row_at_unchecked_vis(build_row_id.row_id());
            if let Some(spilled) = Self::append_one_row_with_null_probe_side(
                chunk_builder,
                build_row,
                probe_column_count,
            ) {
                yield spilled
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn handle_remaining_build_rows_for_right_semi_anti_join<'a, const ANTI_JOIN: bool>(
        chunk_builder: &'a mut DataChunkBuilder,
        build_side: &'a [DataChunk],
        build_row_matched: &'a ChunkedData<bool>,
    ) {
        for build_row_id in build_row_matched.all_row_ids().filter(|build_row_id| {
            if !ANTI_JOIN {
                build_row_matched[*build_row_id]
            } else {
                !build_row_matched[*build_row_id]
            }
        }) {
            if let Some(spilled) = Self::append_one_build_row(
                chunk_builder,
                &build_side[build_row_id.chunk_id()],
                build_row_id.row_id(),
            ) {
                yield spilled
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
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

    fn append_one_row_with_null_build_side(
        chunk_builder: &mut DataChunkBuilder,
        probe_row_ref: RowRef<'_>,
        build_column_count: usize,
    ) -> Option<DataChunk> {
        chunk_builder.append_one_row(probe_row_ref.chain(repeat_n(Datum::None, build_column_count)))
    }

    fn append_one_row_with_null_probe_side(
        chunk_builder: &mut DataChunkBuilder,
        build_row_ref: RowRef<'_>,
        probe_column_count: usize,
    ) -> Option<DataChunk> {
        chunk_builder.append_one_row(repeat_n(Datum::None, probe_column_count).chain(build_row_ref))
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
                                probe_inequality_scalar.default_cmp(&result_inequality_scalar),
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

/// `DataChunkMutator` transforms the given data chunk for non-equi join.
#[repr(transparent)]
struct DataChunkMutator(DataChunk);

impl DataChunkMutator {
    fn nullify_build_side_for_non_equi_condition(
        self,
        filter: &Bitmap,
        probe_column_count: usize,
    ) -> Self {
        let (mut columns, vis) = self.0.into_parts();

        for build_column in columns.split_off(probe_column_count) {
            // Is it really safe to use Arc::try_unwrap here?
            let mut array = Arc::try_unwrap(build_column).unwrap();
            array.set_bitmap(array.null_bitmap() & filter);
            columns.push(array.into());
        }

        Self(DataChunk::new(columns, vis))
    }

    fn remove_duplicate_rows_for_left_outer_join(
        mut self,
        filter: &Bitmap,
        first_output_row_ids: &mut Vec<usize>,
        has_more_output_rows: bool,
        found_non_null: &mut bool,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in iter::once(&0)
            .chain(first_output_row_ids.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id) {
                    *found_non_null = true;
                    new_visibility.set(row_id, true);
                }
            }
            if !*found_non_null {
                new_visibility.set(start_row_id, true);
            }
            *found_non_null = false;
        }

        let start_row_id = first_output_row_ids.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id) {
                *found_non_null = true;
                new_visibility.set(row_id, true);
            }
        }
        if !has_more_output_rows {
            if !*found_non_null {
                new_visibility.set(start_row_id, true);
            }
            *found_non_null = false;
        }

        first_output_row_ids.clear();

        self.0
            .set_visibility(new_visibility.finish() & self.0.visibility());
        self
    }

    /// Removes duplicate rows using `filter`
    /// and only returns the first match for each window.
    /// Windows are indicated by `first_output_row_ids`.
    fn remove_duplicate_rows_for_left_semi_anti_join<const ANTI_JOIN: bool>(
        mut self,
        filter: &Bitmap,
        first_output_row_ids: &mut Vec<usize>,
        has_more_output_rows: bool,
        found_matched: &mut bool,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in iter::once(&0)
            .chain(first_output_row_ids.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id) {
                    if !ANTI_JOIN && !*found_matched {
                        new_visibility.set(row_id, true);
                    }
                    *found_matched = true;
                    break;
                }
            }
            if ANTI_JOIN && !*found_matched {
                new_visibility.set(start_row_id, true);
            }
            *found_matched = false;
        }

        let start_row_id = first_output_row_ids.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id) {
                if !ANTI_JOIN && !*found_matched {
                    new_visibility.set(row_id, true);
                }
                *found_matched = true;
                break;
            }
        }
        if !has_more_output_rows && ANTI_JOIN {
            if !*found_matched {
                new_visibility.set(start_row_id, true);
            }
            *found_matched = false;
        }

        first_output_row_ids.clear();

        self.0
            .set_visibility(new_visibility.finish() & self.0.visibility());
        self
    }

    fn remove_duplicate_rows_for_right_outer_join(
        mut self,
        filter: &Bitmap,
        build_row_ids: &mut Vec<RowId>,
        build_row_matched: &mut ChunkedData<bool>,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());
        for (output_row_id, (output_row_non_null, &build_row_id)) in
            filter.iter().zip_eq_fast(build_row_ids.iter()).enumerate()
        {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
                new_visibility.set(output_row_id, true);
            }
        }

        build_row_ids.clear();

        self.0
            .set_visibility(new_visibility.finish() & self.0.visibility());
        self
    }

    fn remove_duplicate_rows_for_right_semi_anti_join(
        self,
        filter: &Bitmap,
        build_row_ids: &mut Vec<RowId>,
        build_row_matched: &mut ChunkedData<bool>,
    ) {
        for (output_row_non_null, &build_row_id) in filter.iter().zip_eq_fast(build_row_ids.iter())
        {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
            }
        }

        build_row_ids.clear();
    }

    fn remove_duplicate_rows_for_full_outer_join(
        mut self,
        filter: &Bitmap,
        LeftNonEquiJoinState {
            first_output_row_id,
            has_more_output_rows,
            found_matched,
            ..
        }: &mut LeftNonEquiJoinState,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in iter::once(&0)
            .chain(first_output_row_id.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id) {
                    *found_matched = true;
                    new_visibility.set(row_id, true);
                }
            }
            if !*found_matched {
                new_visibility.set(start_row_id, true);
            }
            *found_matched = false;
        }

        let start_row_id = first_output_row_id.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id) {
                *found_matched = true;
                new_visibility.set(row_id, true);
            }
        }
        if !*has_more_output_rows && !*found_matched {
            new_visibility.set(start_row_id, true);
        }

        first_output_row_id.clear();

        for (output_row_id, (output_row_non_null, &build_row_id)) in
            filter.iter().zip_eq_fast(build_row_ids.iter()).enumerate()
        {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
                new_visibility.set(output_row_id, true);
            }
        }

        build_row_ids.clear();

        self.0
            .set_visibility(new_visibility.finish() & self.0.visibility());
        self
    }

    fn take(self) -> DataChunk {
        self.0
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
    #[allow(clippy::too_many_arguments)]
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
            spill_backend,
            spill_metrics,
            None,
            shutdown_rx,
            mem_ctx,
        )
    }

    #[allow(clippy::too_many_arguments)]
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
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        memory_upper_bound: Option<u64>,
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
            spill_backend,
            spill_metrics,
            memory_upper_bound,
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
    use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
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

    use super::{
        ChunkedData, HashJoinExecutor, JoinType, LeftNonEquiJoinState, RightNonEquiJoinState, RowId,
    };
    use crate::error::Result;
    use crate::executor::BoxedExecutor;
    use crate::executor::test_utils::MockExecutor;
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
        assert!(left.is_compacted());
        assert!(right.is_compacted());

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
                    LabelGuardedIntGauge::test_int_gauge(),
                    0,
                )
            } else {
                MemoryContext::new(parent_mem_ctx, LabelGuardedIntGauge::test_int_gauge())
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
                MemoryContext::root(LabelGuardedIntGauge::test_int_gauge(), u64::MAX);

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
                    let data_chunk = data_chunk.compact();
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
    /// Match is found for a probe_row,
    /// but there are still candidate rows in the iterator for that probe_row.
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

    #[tokio::test]
    async fn test_process_left_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   9.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             2   4.0 .   .
             3   5.0 .   .
             3   5.0 .   .
             4   1.0 4   9.0",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            has_more_output_rows: true,
            found_matched: false,
        };
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0
             5   4.0 .   .
             6   7.0 .   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 .   .
             5   4.0 .   .
             6   7.0 6   8.0",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);
    }

    #[tokio::test]
    async fn test_process_left_semi_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            found_matched: false,
            ..Default::default()
        };
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0",
        );
        state.first_output_row_id = vec![2, 3];
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             6   7.0 6   8.0",
        );
        state.first_output_row_id = vec![2, 3];
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
    }

    #[tokio::test]
    async fn test_process_left_anti_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   4.0",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            has_more_output_rows: true,
            found_matched: false,
        };
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             5   4.0 5   .
             6   7.0 6   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             5   4.0 5   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
    }

    #[tokio::test]
    async fn test_process_right_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5",
        );
        let cond = TestFixture::create_cond();
        // For simplicity, all rows are in one chunk.
        // Build side table
        // 0  - (1, 5.5)
        // 1  - (1, 2.5)
        // 2  - ?
        // 3  - (3, 4.0)
        // 4  - (3, 3.0)
        // 5  - (4, 0)
        // 6  - ?
        // 7  - (4, 0.5)
        // 8  - (4, 0.6)
        // 9  - (4, 2.0)
        // 10 - (5, .)
        // 11 - ?
        // 12 - (6, .)
        // 13 - (6, 5.0)
        // Rows with '?' are never matched here.
        let build_row_matched = ChunkedData::with_chunk_sizes([14].into_iter()).unwrap();
        let mut state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched,
        };
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_right_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0",
        );
        state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_right_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v
            }])
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_process_right_semi_anti_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let cond = TestFixture::create_cond();
        let build_row_matched = ChunkedData::with_chunk_sizes([14].into_iter()).unwrap();
        let mut state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched,
        };

        assert!(
            HashJoinExecutor::<Key32>::process_right_semi_anti_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .is_ok()
        );
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(
            HashJoinExecutor::<Key32>::process_right_semi_anti_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .await
            .is_ok()
        );
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v
            }])
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_process_full_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             3   5.0 .   .
             3   5.0 .   .",
        );
        let cond = TestFixture::create_cond();
        let mut left_state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 4, 6],
            has_more_output_rows: true,
            found_matched: false,
        };
        let mut right_state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched: ChunkedData::with_chunk_sizes([14].into_iter()).unwrap(),
        };
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_full_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut left_state,
                &mut right_state,
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(left_state.first_output_row_id, Vec::<usize>::new());
        assert!(!left_state.found_matched);
        assert_eq!(right_state.build_row_ids, Vec::new());
        assert_eq!(
            right_state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0
             5   4.0 .   .
             6   7.0 6   8.0",
        );
        left_state.first_output_row_id = vec![2, 3];
        left_state.has_more_output_rows = false;
        right_state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(compare_data_chunk_with_rowsort(
            &HashJoinExecutor::<Key32>::process_full_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut left_state,
                &mut right_state,
            )
            .await
            .unwrap()
            .compact(),
            &expect
        ));
        assert_eq!(left_state.first_output_row_id, Vec::<usize>::new());
        assert!(left_state.found_matched);
        assert_eq!(right_state.build_row_ids, Vec::new());
        assert_eq!(
            right_state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v[13] = true;
                v
            }])
            .unwrap()
        );
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
}
