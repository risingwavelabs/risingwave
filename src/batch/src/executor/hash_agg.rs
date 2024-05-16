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

use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures_async_stream::try_stream;
use futures_util::AsyncReadExt;
use itertools::Itertools;
use prost::Message;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::data::DataChunk as PbDataChunk;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use twox_hash::XxHash64;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::spill::spill_op::{SpillOp, DEFAULT_SPILL_PARTITION_NUM};
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

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
            None,
            self.identity,
            self.chunk_size,
            self.mem_context,
            self.enable_spill,
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
    task_id: TaskId,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    enable_spill: bool,
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
        enable_spill: bool,
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
            enable_spill,
            shutdown_rx,
        };

        Ok(builder.dispatch())
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HashAggExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hash_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HashAgg
        )?;

        let identity = source.plan_node().get_identity();

        Self::deserialize(
            hash_agg_node,
            child,
            source.task_id.clone(),
            identity.clone(),
            source.context.get_config().developer.chunk_size,
            source.context.create_executor_mem_context(identity),
            source.context.get_config().enable_spill,
            source.shutdown_rx.clone(),
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
    enable_spill: bool,
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
        init_agg_state_executor: Option<BoxedExecutor>,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        enable_spill: bool,
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
            enable_spill,
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

#[derive(Default, Clone, Copy)]
pub struct SpillBuildHasher(u64);

impl BuildHasher for SpillBuildHasher {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.0)
    }
}

const DEFAULT_SPILL_CHUNK_SIZE: usize = 1024;

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
    agg_state_chunk_builders: Vec<DataChunkBuilder>,
    input_chunk_builders: Vec<DataChunkBuilder>,
    agg_state_writer_txs: Vec<Sender<DataChunk>>,
    input_writer_txs: Vec<Sender<DataChunk>>,
    join_handles: Vec<JoinHandle<opendal::Result<()>>>,
    agg_state_readers: Vec<opendal::Reader>,
    input_readers: Vec<opendal::Reader>,
    spill_build_hasher: SpillBuildHasher,
    group_key_types: Vec<DataType>,
    child_data_types: Vec<DataType>,
    agg_data_types: Vec<DataType>,
    spill_chunk_size: usize,
}

impl AggSpillManager {
    pub fn new(
        agg_identity: &String,
        partition_num: usize,
        group_key_types: Vec<DataType>,
        agg_data_types: Vec<DataType>,
        child_data_types: Vec<DataType>,
        spill_chunk_size: usize,
    ) -> Result<Self> {
        let suffix_uuid = uuid::Uuid::new_v4();
        let dir = format!("/{}-{}/", agg_identity, suffix_uuid);
        let op = SpillOp::create(dir)?;
        let agg_state_writer_txs = Vec::with_capacity(partition_num);
        let agg_state_chunk_builders = Vec::with_capacity(partition_num);
        let input_chunk_builders = Vec::with_capacity(partition_num);
        let join_handles = Vec::with_capacity(partition_num);
        let input_writer_txs = Vec::with_capacity(partition_num);
        let agg_state_readers = Vec::with_capacity(partition_num);
        let input_readers = Vec::with_capacity(partition_num);
        // Use uuid to generate an unique hasher so that when recursive spilling happens they would use a different hasher to avoid data skew.
        let spill_build_hasher = SpillBuildHasher(suffix_uuid.as_u64_pair().1);
        Ok(Self {
            op,
            partition_num,
            agg_state_chunk_builders,
            input_chunk_builders,
            agg_state_writer_txs,
            input_writer_txs,
            join_handles,
            agg_state_readers,
            input_readers,
            spill_build_hasher,
            group_key_types,
            child_data_types,
            agg_data_types,
            spill_chunk_size,
        })
    }

    pub async fn init_writers(&mut self) -> Result<()> {
        let writer_task = |mut writer: opendal::Writer, mut rx: Receiver<DataChunk>| async move {
            while let Some(chunk) = rx.recv().await {
                let chunk_pb: PbDataChunk = chunk.to_protobuf();
                let buf = Message::encode_to_vec(&chunk_pb);
                let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                writer.write(len_bytes).await?;
                writer.write(buf).await?;
            }
            writer.close().await?;
            Ok(())
        };

        for i in 0..self.partition_num {
            let agg_state_partition_file_name = format!("agg-state-p{}", i);
            let agg_state_writer = self.op.writer_with(&agg_state_partition_file_name).await?;
            let (tx,rx) = channel(1);
            self.agg_state_writer_txs.push(tx);
            self.agg_state_chunk_builders.push(DataChunkBuilder::new(
                self.group_key_types
                    .iter()
                    .cloned()
                    .chain(self.agg_data_types.iter().cloned())
                    .collect(),
                self.spill_chunk_size,
            ));
            let join_handle = tokio::task::spawn(writer_task(agg_state_writer, rx));
            self.join_handles.push(join_handle);

            let partition_file_name = format!("input-chunks-p{}", i);
            let input_writer = self.op.writer_with(&partition_file_name).await?;
            let (tx, rx) = channel(1);
            self.input_writer_txs.push(tx);
            self.input_chunk_builders.push(DataChunkBuilder::new(
                self.child_data_types.clone(),
                self.spill_chunk_size,
            ));
            let join_handle = tokio::task::spawn(writer_task(input_writer, rx));
            self.join_handles.push(join_handle);
        }
        Ok(())
    }

    pub async fn write_agg_state_row(&mut self, row: impl Row, hash_code: u64) -> Result<()> {
        let partition = hash_code as usize % self.partition_num;
        if let Some(output_chunk) = self.agg_state_chunk_builders[partition].append_one_row(row) {
            self.agg_state_writer_txs[partition]
                .send(output_chunk)
                .await
                .map_err(|e| anyhow!(e).context("failed to write agg state chunk"))?;
        }
        Ok(())
    }

    pub async fn write_input_chunk(
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
            for output_chunk in self.input_chunk_builders[partition].append_chunk(new_chunk) {
                self.input_writer_txs[partition]
                    .send(output_chunk)
                    .await
                    .map_err(|e| anyhow!(e).context("failed to write input chunk"))?;
            }
        }
        Ok(())
    }

    pub async fn close_writers(&mut self) -> Result<()> {
        for partition in 0..self.partition_num {
            if let Some(output_chunk) = self.agg_state_chunk_builders[partition].consume_all() {
                self.agg_state_writer_txs[partition]
                    .send(output_chunk)
                    .await
                    .map_err(|e| anyhow!(e).context("failed to write agg state chunk"))?;
            }
            if let Some(output_chunk) = self.input_chunk_builders[partition].consume_all() {
                self.input_writer_txs[partition]
                    .send(output_chunk)
                    .await
                    .map_err(|e| anyhow!(e).context("failed to write input chunk"))?;
            }
        }

        // drop txs
        self.agg_state_writer_txs = vec![];
        self.input_writer_txs = vec![];

        // wait join handle to close writer
        for join_handle in self.join_handles.drain(..) {
            join_handle
                .await
                .map_err(|err| anyhow!(err).context("Failed to join shutdown"))??;
        }
        Ok(())
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn read_stream(mut reader: opendal::Reader) {
        let mut buf = [0u8; 4];
        loop {
            if let Err(err) = reader.read_exact(&mut buf).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(anyhow!(err).into());
                }
            }
            let len = u32::from_le_bytes(buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await.map_err(|e| anyhow!(e))?;
            let chunk_pb: PbDataChunk = Message::decode(buf.as_slice()).map_err(|e| anyhow!(e))?;
            let chunk = DataChunk::from_protobuf(&chunk_pb)?;
            yield chunk;
        }
    }

    async fn read_agg_state_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let agg_state_partition_file_name = format!("agg-state-p{}", partition);
        let r = self.op.reader_with(&agg_state_partition_file_name).await?;
        Ok(Self::read_stream(r))
    }

    async fn read_input_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let input_partition_file_name = format!("input-chunks-p{}", partition);
        let r = self.op.reader_with(&input_partition_file_name).await?;
        Ok(Self::read_stream(r))
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

                if !self.mem_context.add(memory_usage_diff) {
                    warn!("not enough memory to load one partition agg state after spill which is not a normal case, so keep going");
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
            for (row_id, (key, visible)) in keys
                .into_iter()
                .zip_eq_fast(chunk.visibility().iter())
                .enumerate()
            {
                if !visible {
                    continue;
                }
                let mut new_group = false;
                let states = groups.entry(key).or_insert_with(|| {
                    new_group = true;
                    self.aggs.iter().map(|agg| agg.create_state()).collect()
                });

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
            if !self.mem_context.add(memory_usage_diff) {
                if self.enable_spill {
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
            // If memory is still not enough in the sub HashAggExecutor, it will partition its hash table and input recursively.
            let mut agg_spill_manager = AggSpillManager::new(
                &self.identity,
                DEFAULT_SPILL_PARTITION_NUM,
                self.group_key_types.clone(),
                self.aggs.iter().map(|agg| agg.return_type()).collect(),
                child_schema.data_types(),
                DEFAULT_SPILL_CHUNK_SIZE,
            )?;
            agg_spill_manager.init_writers().await?;

            let start_time = Instant::now();

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

            debug!("spill write time = {}ms", start_time.elapsed().as_millis());

            // Process each partition one by one.
            for i in 0..agg_spill_manager.partition_num {
                let agg_state_stream = agg_spill_manager.read_agg_state_partition(i).await?;
                let input_stream = agg_spill_manager.read_input_partition(i).await?;

                let sub_hash_agg_executor: HashAggExecutor<K> = HashAggExecutor::new(
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
                    self.enable_spill,
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
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::expr::agg_call::Type;
    use risingwave_pb::expr::{AggCall, InputRef};

    use super::*;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn execute_int32_grouped() {
        let parent_mem = MemoryContext::root(LabelGuardedIntGauge::<4>::test_int_gauge(), u64::MAX);
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
                r#type: Type::Sum as i32,
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
            };

            let agg_prost = HashAggNode {
                group_key: vec![0, 1],
                agg_calls: vec![agg_call],
            };

            let mem_context = MemoryContext::new(
                Some(parent_mem.clone()),
                LabelGuardedIntGauge::<4>::test_int_gauge(),
            );
            let actual_exec = HashAggExecutorBuilder::deserialize(
                &agg_prost,
                src_exec,
                TaskId::default(),
                "HashAggExecutor".to_string(),
                CHUNK_SIZE,
                mem_context.clone(),
                false,
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
            r#type: Type::Count as i32,
            args: vec![],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
        };

        let agg_prost = HashAggNode {
            group_key: vec![],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
            false,
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
                    let g = Global;
                    g.deallocate(ptr, layout)
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
                map.entry(i).or_insert_with(|| "i".to_string());
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
            r#type: Type::Sum as i32,
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
            "HashAggExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
            false,
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
}
