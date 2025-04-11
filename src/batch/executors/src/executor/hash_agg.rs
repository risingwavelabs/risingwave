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

use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use futures_async_stream::try_stream;
use hashbrown::hash_map::Entry;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::bitmap::{Bitmap, FilterByBitmap};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::Message;
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::data::DataChunk as PbDataChunk;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::monitor::BatchSpillMetrics;
use crate::spill::spill_op::SpillBackend::Disk;
use crate::spill::spill_op::{
    DEFAULT_SPILL_PARTITION_NUM, SPILL_AT_LEAST_MEMORY, SpillBackend, SpillBuildHasher, SpillOp,
};
use crate::task::{ShutdownToken, TaskId};

type AggHashMap<K, A> = hashbrown::HashMap<K, Vec<AggregateState>, PrecomputedBuildHasher, A>;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(
            Arc::new(self.aggs),
            self.group_key_columns,
            self.group_key_types,
            self.schema,
            self.child,
            self.identity,
            self.chunk_size,
            self.mem_context,
            self.spill_backend,
            self.spill_metrics,
            self.shutdown_rx,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder {
    aggs: Vec<BoxedAggregateFunction>,
    group_key_columns: Vec<usize>,
    group_key_types: Vec<DataType>,
    child: BoxedExecutor,
    schema: Schema,
    #[expect(dead_code)]
    task_id: TaskId,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    shutdown_rx: ShutdownToken,
}

impl HashAggExecutorBuilder {
    fn deserialize(
        hash_agg_node: &HashAggNode,
        child: BoxedExecutor,
        task_id: TaskId,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        shutdown_rx: ShutdownToken,
    ) -> Result<BoxedExecutor> {
        let aggs: Vec<_> = hash_agg_node
            .get_agg_calls()
            .iter()
            .map(|agg| AggCall::from_protobuf(agg).and_then(|agg| build_agg(&agg)))
            .try_collect()?;

        let group_key_columns = hash_agg_node
            .get_group_key()
            .iter()
            .map(|x| *x as usize)
            .collect_vec();

        let child_schema = child.schema();

        let group_key_types = group_key_columns
            .iter()
            .map(|i| child_schema.fields[*i].data_type.clone())
            .collect_vec();

        let fields = group_key_types
            .iter()
            .cloned()
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let builder = HashAggExecutorBuilder {
            aggs,
            group_key_columns,
            group_key_types,
            child,
            schema: Schema { fields },
            task_id,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            shutdown_rx,
        };

        Ok(builder.dispatch())
    }
}

impl BoxedExecutorBuilder for HashAggExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hash_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HashAgg
        )?;

        let identity = source.plan_node().get_identity();

        let spill_metrics = source.context().spill_metrics();

        Self::deserialize(
            hash_agg_node,
            child,
            source.task_id.clone(),
            identity.clone(),
            source.context().get_config().developer.chunk_size,
            source.context().create_executor_mem_context(identity),
            if source.context().get_config().enable_spill {
                Some(Disk)
            } else {
                None
            },
            spill_metrics,
            source.shutdown_rx().clone(),
        )
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub struct HashAggExecutor<K> {
    /// Aggregate functions.
    aggs: Arc<Vec<BoxedAggregateFunction>>,
    /// Column indexes that specify a group
    group_key_columns: Vec<usize>,
    /// Data types of group key columns
    group_key_types: Vec<DataType>,
    /// Output schema
    schema: Schema,
    child: BoxedExecutor,
    /// Used to initialize the state of the aggregation from the spilled files.
    init_agg_state_executor: Option<BoxedExecutor>,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    /// The upper bound of memory usage for this executor.
    memory_upper_bound: Option<u64>,
    shutdown_rx: ShutdownToken,
    _phantom: PhantomData<K>,
}

impl<K> HashAggExecutor<K> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggs: Arc<Vec<BoxedAggregateFunction>>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        Self::new_inner(
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            None,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            None,
            shutdown_rx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        aggs: Arc<Vec<BoxedAggregateFunction>>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        init_agg_state_executor: Option<BoxedExecutor>,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        memory_upper_bound: Option<u64>,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        HashAggExecutor {
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            init_agg_state_executor,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            memory_upper_bound,
            shutdown_rx,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey + Send + Sync> Executor for HashAggExecutor<K> {
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

/// `AggSpillManager` is used to manage how to write spill data file and read them back.
/// The spill data first need to be partitioned. Each partition contains 2 files: `agg_state_file` and `input_chunks_file`.
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
pub struct AggSpillManager {
    op: SpillOp,
    partition_num: usize,
    agg_state_writers: Vec<opendal::Writer>,
    agg_state_chunk_builder: Vec<DataChunkBuilder>,
    input_writers: Vec<opendal::Writer>,
    input_chunk_builders: Vec<DataChunkBuilder>,
    spill_build_hasher: SpillBuildHasher,
    group_key_types: Vec<DataType>,
    child_data_types: Vec<DataType>,
    agg_data_types: Vec<DataType>,
    spill_chunk_size: usize,
    spill_metrics: Arc<BatchSpillMetrics>,
}

impl AggSpillManager {
    fn new(
        spill_backend: SpillBackend,
        agg_identity: &String,
        partition_num: usize,
        group_key_types: Vec<DataType>,
        agg_data_types: Vec<DataType>,
        child_data_types: Vec<DataType>,
        spill_chunk_size: usize,
        spill_metrics: Arc<BatchSpillMetrics>,
    ) -> Result<Self> {
        let suffix_uuid = uuid::Uuid::new_v4();
        let dir = format!("/{}-{}/", agg_identity, suffix_uuid);
        let op = SpillOp::create(dir, spill_backend)?;
        let agg_state_writers = Vec::with_capacity(partition_num);
        let agg_state_chunk_builder = Vec::with_capacity(partition_num);
        let input_writers = Vec::with_capacity(partition_num);
        let input_chunk_builders = Vec::with_capacity(partition_num);
        // Use uuid to generate an unique hasher so that when recursive spilling happens they would use a different hasher to avoid data skew.
        let spill_build_hasher = SpillBuildHasher(suffix_uuid.as_u64_pair().1);
        Ok(Self {
            op,
            partition_num,
            agg_state_writers,
            agg_state_chunk_builder,
            input_writers,
            input_chunk_builders,
            spill_build_hasher,
            group_key_types,
            child_data_types,
            agg_data_types,
            spill_chunk_size,
            spill_metrics,
        })
    }

    async fn init_writers(&mut self) -> Result<()> {
        for i in 0..self.partition_num {
            let agg_state_partition_file_name = format!("agg-state-p{}", i);
            let w = self.op.writer_with(&agg_state_partition_file_name).await?;
            self.agg_state_writers.push(w);

            let partition_file_name = format!("input-chunks-p{}", i);
            let w = self.op.writer_with(&partition_file_name).await?;
            self.input_writers.push(w);
            self.input_chunk_builders.push(DataChunkBuilder::new(
                self.child_data_types.clone(),
                self.spill_chunk_size,
            ));
            self.agg_state_chunk_builder.push(DataChunkBuilder::new(
                self.group_key_types
                    .iter()
                    .cloned()
                    .chain(self.agg_data_types.iter().cloned())
                    .collect(),
                self.spill_chunk_size,
            ));
        }
        Ok(())
    }

    async fn write_agg_state_row(&mut self, row: impl Row, hash_code: u64) -> Result<()> {
        let partition = hash_code as usize % self.partition_num;
        if let Some(output_chunk) = self.agg_state_chunk_builder[partition].append_one_row(row) {
            let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
            let buf = Message::encode_to_vec(&chunk_pb);
            let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
            self.spill_metrics
                .batch_spill_write_bytes
                .inc_by((buf.len() + len_bytes.len()) as u64);
            self.agg_state_writers[partition].write(len_bytes).await?;
            self.agg_state_writers[partition].write(buf).await?;
        }
        Ok(())
    }

    async fn write_input_chunk(&mut self, chunk: DataChunk, hash_codes: Vec<u64>) -> Result<()> {
        let (columns, vis) = chunk.into_parts_v2();
        for partition in 0..self.partition_num {
            let new_vis = vis.clone()
                & Bitmap::from_iter(
                    hash_codes
                        .iter()
                        .map(|hash_code| (*hash_code as usize % self.partition_num) == partition),
                );
            let new_chunk = DataChunk::from_parts(columns.clone(), new_vis);
            for output_chunk in self.input_chunk_builders[partition].append_chunk(new_chunk) {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.input_writers[partition].write(len_bytes).await?;
                self.input_writers[partition].write(buf).await?;
            }
        }
        Ok(())
    }

    async fn close_writers(&mut self) -> Result<()> {
        for partition in 0..self.partition_num {
            if let Some(output_chunk) = self.agg_state_chunk_builder[partition].consume_all() {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.agg_state_writers[partition].write(len_bytes).await?;
                self.agg_state_writers[partition].write(buf).await?;
            }

            if let Some(output_chunk) = self.input_chunk_builders[partition].consume_all() {
                let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                self.spill_metrics
                    .batch_spill_write_bytes
                    .inc_by((buf.len() + len_bytes.len()) as u64);
                self.input_writers[partition].write(len_bytes).await?;
                self.input_writers[partition].write(buf).await?;
            }
        }

        for mut w in self.agg_state_writers.drain(..) {
            w.close().await?;
        }
        for mut w in self.input_writers.drain(..) {
            w.close().await?;
        }
        Ok(())
    }

    async fn read_agg_state_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let agg_state_partition_file_name = format!("agg-state-p{}", partition);
        let r = self.op.reader_with(&agg_state_partition_file_name).await?;
        Ok(SpillOp::read_stream(r, self.spill_metrics.clone()))
    }

    async fn read_input_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        let r = self.op.reader_with(&input_partition_file_name).await?;
        Ok(SpillOp::read_stream(r, self.spill_metrics.clone()))
    }

    async fn estimate_partition_size(&self, partition: usize) -> Result<u64> {
        let agg_state_partition_file_name = format!("agg-state-p{}", partition);
        let agg_state_size = self
            .op
            .stat(&agg_state_partition_file_name)
            .await?
            .content_length();
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        let input_size = self
            .op
            .stat(&input_partition_file_name)
            .await?
            .content_length();
        Ok(agg_state_size + input_size)
    }

    async fn clear_partition(&mut self, partition: usize) -> Result<()> {
        let agg_state_partition_file_name = format!("agg-state-p{}", partition);
        self.op.delete(&agg_state_partition_file_name).await?;
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        self.op.delete(&input_partition_file_name).await?;
        Ok(())
    }
}

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let child_schema = self.child.schema().clone();
        let mut need_to_spill = false;
        // If the memory upper bound is less than 1MB, we don't need to check memory usage.
        let check_memory = match self.memory_upper_bound {
            Some(upper_bound) => upper_bound > SPILL_AT_LEAST_MEMORY,
            None => true,
        };

        // hash map for each agg groups
        let mut groups = AggHashMap::<K, _>::with_hasher_in(
            PrecomputedBuildHasher,
            self.mem_context.global_allocator(),
        );

        if let Some(init_agg_state_executor) = self.init_agg_state_executor {
            // `init_agg_state_executor` exists which means this is a sub `HashAggExecutor` used to consume spilling data.
            // The spilled agg states by its parent executor need to be recovered first.
            let mut init_agg_state_stream = init_agg_state_executor.execute();
            #[for_await]
            for chunk in &mut init_agg_state_stream {
                let chunk = chunk?;
                let group_key_indices = (0..self.group_key_columns.len()).collect_vec();
                let keys = K::build_many(&group_key_indices, &chunk);
                let mut memory_usage_diff = 0;
                for (row_id, key) in keys.into_iter().enumerate() {
                    let mut agg_states = vec![];
                    for i in 0..self.aggs.len() {
                        let agg = &self.aggs[i];
                        let datum = chunk
                            .row_at(row_id)
                            .0
                            .datum_at(self.group_key_columns.len() + i)
                            .to_owned_datum();
                        let agg_state = agg.decode_state(datum)?;
                        memory_usage_diff += agg_state.estimated_size() as i64;
                        agg_states.push(agg_state);
                    }
                    groups.try_insert(key, agg_states).unwrap();
                }

                if !self.mem_context.add(memory_usage_diff) && check_memory {
                    warn!(
                        "not enough memory to load one partition agg state after spill which is not a normal case, so keep going"
                    );
                }
            }
        }

        let mut input_stream = self.child.execute();
        // consume all chunks to compute the agg result
        #[for_await]
        for chunk in &mut input_stream {
            let chunk = StreamChunk::from(chunk?);
            let keys = K::build_many(self.group_key_columns.as_slice(), &chunk);
            let mut memory_usage_diff = 0;
            for (row_id, key) in keys
                .into_iter()
                .enumerate()
                .filter_by_bitmap(chunk.visibility())
            {
                let mut new_group = false;
                let states = match groups.entry(key) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        new_group = true;
                        let states = self
                            .aggs
                            .iter()
                            .map(|agg| agg.create_state())
                            .try_collect()?;
                        entry.insert(states)
                    }
                };

                // TODO: currently not a vectorized implementation
                for (agg, state) in self.aggs.iter().zip_eq_fast(states) {
                    if !new_group {
                        memory_usage_diff -= state.estimated_size() as i64;
                    }
                    agg.update_range(state, &chunk, row_id..row_id + 1).await?;
                    memory_usage_diff += state.estimated_size() as i64;
                }
            }
            // update memory usage
            if !self.mem_context.add(memory_usage_diff) && check_memory {
                if self.spill_backend.is_some() {
                    need_to_spill = true;
                    break;
                } else {
                    Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                }
            }
        }

        if need_to_spill {
            // A spilling version of aggregation based on the RFC: Spill Hash Aggregation https://github.com/risingwavelabs/rfcs/pull/89
            // When HashAggExecutor told memory is insufficient, AggSpillManager will start to partition the hash table and spill to disk.
            // After spilling the hash table, AggSpillManager will consume all chunks from the input executor,
            // partition and spill to disk with the same hash function as the hash table spilling.
            // Finally, we would get e.g. 20 partitions. Each partition should contain a portion of the original hash table and input data.
            // A sub HashAggExecutor would be used to consume each partition one by one.
            // If memory is still not enough in the sub HashAggExecutor, it will spill its hash table and input recursively.
            info!(
                "batch hash agg executor {} starts to spill out",
                &self.identity
            );
            let mut agg_spill_manager = AggSpillManager::new(
                self.spill_backend.clone().unwrap(),
                &self.identity,
                DEFAULT_SPILL_PARTITION_NUM,
                self.group_key_types.clone(),
                self.aggs.iter().map(|agg| agg.return_type()).collect(),
                child_schema.data_types(),
                self.chunk_size,
                self.spill_metrics.clone(),
            )?;
            agg_spill_manager.init_writers().await?;

            let mut memory_usage_diff = 0;
            // Spill agg states.
            for (key, states) in groups {
                let key_row = key.deserialize(&self.group_key_types)?;
                let mut agg_datums = vec![];
                for (agg, state) in self.aggs.iter().zip_eq_fast(states) {
                    let encode_state = agg.encode_state(&state)?;
                    memory_usage_diff -= state.estimated_size() as i64;
                    agg_datums.push(encode_state);
                }
                let agg_state_row = OwnedRow::from_iter(agg_datums.into_iter());
                let hash_code = agg_spill_manager.spill_build_hasher.hash_one(key);
                agg_spill_manager
                    .write_agg_state_row(key_row.chain(agg_state_row), hash_code)
                    .await?;
            }

            // Release memory occupied by agg hash map.
            self.mem_context.add(memory_usage_diff);

            // Spill input chunks.
            #[for_await]
            for chunk in input_stream {
                let chunk: DataChunk = chunk?;
                let hash_codes = chunk.get_hash_values(
                    self.group_key_columns.as_slice(),
                    agg_spill_manager.spill_build_hasher,
                );
                agg_spill_manager
                    .write_input_chunk(
                        chunk,
                        hash_codes
                            .into_iter()
                            .map(|hash_code| hash_code.value())
                            .collect(),
                    )
                    .await?;
            }

            agg_spill_manager.close_writers().await?;

            // Process each partition one by one.
            for i in 0..agg_spill_manager.partition_num {
                let partition_size = agg_spill_manager.estimate_partition_size(i).await?;

                let agg_state_stream = agg_spill_manager.read_agg_state_partition(i).await?;
                let input_stream = agg_spill_manager.read_input_partition(i).await?;

                let sub_hash_agg_executor: HashAggExecutor<K> = HashAggExecutor::new_inner(
                    self.aggs.clone(),
                    self.group_key_columns.clone(),
                    self.group_key_types.clone(),
                    self.schema.clone(),
                    Box::new(WrapStreamExecutor::new(child_schema.clone(), input_stream)),
                    Some(Box::new(WrapStreamExecutor::new(
                        self.schema.clone(),
                        agg_state_stream,
                    ))),
                    format!("{}-sub{}", self.identity.clone(), i),
                    self.chunk_size,
                    self.mem_context.clone(),
                    self.spill_backend.clone(),
                    self.spill_metrics.clone(),
                    Some(partition_size),
                    self.shutdown_rx.clone(),
                );

                debug!(
                    "create sub_hash_agg {} for hash_agg {} to spill",
                    sub_hash_agg_executor.identity, self.identity
                );

                let sub_hash_agg_stream = Box::new(sub_hash_agg_executor).execute();

                #[for_await]
                for chunk in sub_hash_agg_stream {
                    let chunk = chunk?;
                    yield chunk;
                }

                // Clear files of the current partition.
                agg_spill_manager.clear_partition(i).await?;
            }
        } else {
            // Don't use `into_iter` here, it may cause memory leak.
            let mut result = groups.iter_mut();
            let cardinality = self.chunk_size;
            loop {
                let mut group_builders: Vec<_> = self
                    .group_key_types
                    .iter()
                    .map(|datatype| datatype.create_array_builder(cardinality))
                    .collect();

                let mut agg_builders: Vec<_> = self
                    .aggs
                    .iter()
                    .map(|agg| agg.return_type().create_array_builder(cardinality))
                    .collect();

                let mut has_next = false;
                let mut array_len = 0;
                for (key, states) in result.by_ref().take(cardinality) {
                    self.shutdown_rx.check()?;
                    has_next = true;
                    array_len += 1;
                    key.deserialize_to_builders(&mut group_builders[..], &self.group_key_types)?;
                    for ((agg, state), builder) in (self.aggs.iter())
                        .zip_eq_fast(states)
                        .zip_eq_fast(&mut agg_builders)
                    {
                        let result = agg.get_result(state).await?;
                        builder.append(result);
                    }
                }
                if !has_next {
                    break; // exit loop
                }

                let columns = group_builders
                    .into_iter()
                    .chain(agg_builders)
                    .map(|b| b.finish().into())
                    .collect::<Vec<_>>();

                let output = DataChunk::new(columns, array_len);
                yield output;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{AllocError, Allocator, Global, Layout};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_async_stream::for_await;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
    use risingwave_pb::expr::{AggCall, InputRef};

    use super::*;
    use crate::executor::SortExecutor;
    use crate::executor::test_utils::{MockExecutor, diff_executor_output};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn execute_int32_grouped() {
        let parent_mem = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge(), u64::MAX);
        {
            let src_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i i
                 0 1 1
                 1 1 1
                 0 0 1
                 1 1 2
                 1 0 1
                 0 0 2
                 1 1 3
                 0 1 2",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));

            let agg_call = AggCall {
                kind: PbAggKind::Sum as i32,
                args: vec![InputRef {
                    index: 2,
                    r#type: Some(PbDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                }],
                return_type: Some(PbDataType {
                    type_name: TypeName::Int64 as i32,
                    ..Default::default()
                }),
                distinct: false,
                order_by: vec![],
                filter: None,
                direct_args: vec![],
                udf: None,
                scalar: None,
            };

            let agg_prost = HashAggNode {
                group_key: vec![0, 1],
                agg_calls: vec![agg_call],
            };

            let mem_context = MemoryContext::new(
                Some(parent_mem.clone()),
                LabelGuardedIntGauge::test_int_gauge(),
            );
            let actual_exec = HashAggExecutorBuilder::deserialize(
                &agg_prost,
                src_exec,
                TaskId::default(),
                "HashAggExecutor".to_owned(),
                CHUNK_SIZE,
                mem_context.clone(),
                None,
                BatchSpillMetrics::for_test(),
                ShutdownToken::empty(),
            )
            .unwrap();

            // TODO: currently the order is fixed unless the hasher is changed
            let expect_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i I
                 1 0 1
                 0 0 3
                 0 1 3
                 1 1 6",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));
            diff_executor_output(actual_exec, expect_exec).await;

            // check estimated memory usage = 4 groups x state size
            assert_eq!(mem_context.get_bytes_used() as usize, 4 * 24);
        }

        // Ensure that agg memory counter has been dropped.
        assert_eq!(0, parent_mem.get_bytes_used());
    }

    #[tokio::test]
    async fn execute_count_star() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i
                 0
                 1
                 0
                 1
                 1
                 0
                 1
                 0",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32)]),
        );

        let agg_call = AggCall {
            kind: PbAggKind::Count as i32,
            args: vec![],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
        )
        .unwrap();

        let expect_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "I
                 8",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int64)]),
        );
        diff_executor_output(actual_exec, Box::new(expect_exec)).await;
    }

    /// A test to verify that `HashMap` may leak memory counter when using `into_iter`.
    #[test]
    #[should_panic] // TODO(MrCroxx): This bug is fixed and the test should panic. Remove the test and fix the related code later.
    fn test_hashmap_into_iter_bug() {
        let dropped: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        {
            struct MyAllocInner {
                drop_flag: Arc<AtomicBool>,
            }

            #[derive(Clone)]
            struct MyAlloc {
                #[expect(dead_code)]
                inner: Arc<MyAllocInner>,
            }

            impl Drop for MyAllocInner {
                fn drop(&mut self) {
                    println!("MyAlloc freed.");
                    self.drop_flag.store(true, Ordering::SeqCst);
                }
            }

            unsafe impl Allocator for MyAlloc {
                fn allocate(
                    &self,
                    layout: Layout,
                ) -> std::result::Result<NonNull<[u8]>, AllocError> {
                    let g = Global;
                    g.allocate(layout)
                }

                unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                    unsafe {
                        let g = Global;
                        g.deallocate(ptr, layout)
                    }
                }
            }

            let mut map = hashbrown::HashMap::with_capacity_in(
                10,
                MyAlloc {
                    inner: Arc::new(MyAllocInner {
                        drop_flag: dropped.clone(),
                    }),
                },
            );
            for i in 0..10 {
                map.entry(i).or_insert_with(|| "i".to_owned());
            }

            for (k, v) in map {
                println!("{}, {}", k, v);
            }
        }

        assert!(!dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i i
                 0 1 1",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32); 3]),
        );

        let agg_call = AggCall {
            kind: PbAggKind::Sum as i32,
            args: vec![InputRef {
                index: 2,
                r#type: Some(PbDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
            shutdown_rx,
        )
        .unwrap();

        shutdown_tx.cancel();

        #[for_await]
        for data in actual_exec.execute() {
            assert!(data.is_err());
            break;
        }
    }

    fn create_order_by_executor(child: BoxedExecutor) -> BoxedExecutor {
        let column_orders = child
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, _)| ColumnOrder {
                column_index: i,
                order_type: OrderType::ascending(),
            })
            .collect_vec();

        Box::new(SortExecutor::new(
            child,
            Arc::new(column_orders),
            "SortExecutor".into(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ))
    }

    #[tokio::test]
    async fn test_spill_hash_agg() {
        let src_exec = Box::new(MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i i
                 0 1 1
                 1 1 1
                 0 0 1
                 1 1 2
                 1 0 1
                 0 0 2
                 1 1 3
                 0 1 2",
            ),
            Schema::new(vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int64),
            ]),
        ));

        let agg_call = AggCall {
            kind: PbAggKind::Sum as i32,
            args: vec![InputRef {
                index: 2,
                r#type: Some(PbDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let mem_context =
            MemoryContext::new_with_mem_limit(None, LabelGuardedIntGauge::test_int_gauge(), 0);
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            src_exec,
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            mem_context.clone(),
            Some(SpillBackend::Memory),
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
        )
        .unwrap();

        let actual_exec = create_order_by_executor(actual_exec);

        let expect_exec = Box::new(MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i I
                 1 0 1
                 0 0 3
                 0 1 3
                 1 1 6",
            ),
            Schema::new(vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int64),
            ]),
        ));

        let expect_exec = create_order_by_executor(expect_exec);
        diff_executor_output(actual_exec, expect_exec).await;
    }
}
