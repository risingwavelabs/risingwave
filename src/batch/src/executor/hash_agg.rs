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

use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use anyhow::anyhow;
use bytes::Bytes;

use futures_async_stream::try_stream;
use futures_util::AsyncReadExt;
use itertools::Itertools;
use opendal::layers::RetryLayer;
use opendal::Operator;
use opendal::services::Fs;
use prost::Message;
use twox_hash::XxHash64;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_pb::data::DataChunk as PbDataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_common::row::RowExt;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

type AggHashMap<K, A> = hashbrown::HashMap<K, Vec<AggregateState>, PrecomputedBuildHasher, A>;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(
            self.aggs,
            self.group_key_columns,
            self.group_key_types,
            self.schema,
            self.child,
            self.identity,
            self.chunk_size,
            self.mem_context,
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
            source.shutdown_rx.clone(),
        )
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub struct HashAggExecutor<K> {
    /// Aggregate functions.
    aggs: Vec<BoxedAggregateFunction>,
    /// Column indexes that specify a group
    group_key_columns: Vec<usize>,
    /// Data types of group key columns
    group_key_types: Vec<DataType>,
    /// Output schema
    schema: Schema,
    child: BoxedExecutor,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    shutdown_rx: ShutdownToken,
    _phantom: PhantomData<K>,
}

impl<K> HashAggExecutor<K> {
    pub fn new(
        aggs: Vec<BoxedAggregateFunction>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        HashAggExecutor {
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            identity,
            chunk_size,
            mem_context,
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

pub fn new_spill_op(root: String) -> Result<SpillOp> {
    assert!(root.ends_with('/'));
    let mut builder = Fs::default();
    builder.root(&root);

    let op: Operator = Operator::new(builder)?
        .layer(RetryLayer::default())
        .finish();
    Ok(SpillOp {
        op,
    })
}

pub struct SpillOp {
    op: Operator,
}

impl SpillOp {
    pub async fn writer_with(&self, name: &str) -> Result<opendal::Writer> {
        Ok(self.op.writer_with(name).append(true).buffer(64 * 1024).await?)
    }

    pub async fn reader_with(&self, name: &str) -> Result<opendal::Reader> {
        Ok(self.op.reader_with(name).buffer(64 * 1024).await?)
    }
}

impl Drop for SpillOp {
    fn drop(&mut self) {
        let op = self.op.clone();
        tokio::task::spawn(async move {
            let result = op.remove_all("/").await;
            if let Err(e) = result {
                error!("Failed to remove spill directory: {}", e);
            }
        });
    }
}

impl DerefMut for SpillOp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl Deref for SpillOp {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.op
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

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // hash map for each agg groups
        let mut groups = AggHashMap::<K, _>::with_hasher_in(
            PrecomputedBuildHasher,
            self.mem_context.global_allocator(),
        );

        let child_schema = self.child.schema().clone();

        let enable_spill = true;
        let mut need_to_spill = false;

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
                if enable_spill {
                    need_to_spill = true;
                    break;
                } else {
                    Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                }
            }
        }

        if need_to_spill {
            let suffix_uuid = uuid::Uuid::new_v4();
            let dir = format!("/tmp/rw_batch_spill-{}-{}/", &self.identity, suffix_uuid.to_string());
            let op = new_spill_op(dir.into())?;
            let partition_num = 4;
            let mut agg_state_writers = Vec::with_capacity(partition_num);
            let mut agg_state_chunk_builder = Vec::with_capacity(partition_num);
            let mut writers = Vec::with_capacity(partition_num);
            let mut chunk_builders = Vec::with_capacity(partition_num);
            let spill_build_hasher = SpillBuildHasher(suffix_uuid.as_u64_pair().1);

            for i in 0..partition_num {
                let agg_state_partition_file_name = format!("agg-state-p{}", i);
                let w = op.writer_with(&agg_state_partition_file_name).await?;
                agg_state_writers.push(w);

                let partition_file_name = format!("input-chunks-p{}", i);
                let w = op.writer_with(&partition_file_name).await?;
                writers.push(w);
                chunk_builders.push(DataChunkBuilder::new(child_schema.data_types(), 1024));
                agg_state_chunk_builder.push(DataChunkBuilder::new(self
                                                              .group_key_types
                                                              .iter().cloned().chain(self
                    .aggs
                    .iter().map(|agg| agg.return_type())).collect(), 1024));
            }

            // Spill the agg state
            let mut memory_usage_diff = 0;
            for (key, states) in groups {
                let key_row = key.deserialize(&self.group_key_types)?;
                let mut agg_datums = vec![];
                for (agg, state) in self.aggs.iter().zip_eq_fast(states) {
                    let encode_state = agg.encode_state(&state)?;
                    memory_usage_diff -= state.estimated_size() as i64;
                    agg_datums.push(encode_state);
                }
                let agg_state_row = OwnedRow::from_iter(agg_datums.into_iter());
                let mut hasher = spill_build_hasher.build_hasher();
                key.hash(&mut hasher);
                let hash_code = hasher.finish();
                let partition = hash_code as usize % partition_num;
                if let Some(output_chunk) = agg_state_chunk_builder[partition].append_one_row(key_row.chain(agg_state_row)) {
                    let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                    let buf = Message::encode_to_vec(&chunk_pb);
                    let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                    agg_state_writers[partition].write(len_bytes).await?;
                    agg_state_writers[partition].write(buf).await?;
                }
            }

            for i in 0..partition_num {
                if let Some(output_chunk) = agg_state_chunk_builder[i].consume_all() {
                    let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                    let buf = Message::encode_to_vec(&chunk_pb);
                    let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                    agg_state_writers[i].write(len_bytes).await?;
                    agg_state_writers[i].write(buf).await?;
                }
            }

            for mut w in agg_state_writers {
                w.close().await?;
            }

            self.mem_context.add(memory_usage_diff);

            groups = AggHashMap::<K, _>::with_hasher_in(
                PrecomputedBuildHasher,
                self.mem_context.global_allocator(),
            );


            #[for_await]
            for chunk in input_stream {
                let chunk: DataChunk = chunk?;
                let hash_codes = chunk.get_hash_values(self.group_key_columns.as_slice(), spill_build_hasher);
                let (columns, vis) = chunk.into_parts_v2();
                for i in 0..partition_num {
                    let new_vis = vis.clone() & Bitmap::from_iter(hash_codes.iter().map(|hash_code| (hash_code.value() as usize % partition_num) == i));
                    let new_chunk = DataChunk::from_parts(columns.clone(), new_vis);
                    for output_chunk in chunk_builders[i].append_chunk(new_chunk) {
                        let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                        let buf = Message::encode_to_vec(&chunk_pb);
                        let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                        writers[i].write(len_bytes).await?;
                        writers[i].write(buf).await?;
                    }
                }
            }

            for i in 0..partition_num {
                if let Some(output_chunk) = chunk_builders[i].consume_all() {
                    let chunk_pb: PbDataChunk = output_chunk.to_protobuf();
                    let buf = Message::encode_to_vec(&chunk_pb);
                    let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
                    writers[i].write(len_bytes).await?;
                    writers[i].write(buf).await?;
                }
            }

            for mut w in writers {
                w.close().await?;
            }

            let mut agg_state_readers = Vec::with_capacity(partition_num);
            for i in 0..partition_num {
                let agg_state_partition_file_name = format!("agg-state-p{}", i);
                let r = op.reader_with(&agg_state_partition_file_name).await?;
                agg_state_readers.push(r);
            }

            let mut readers = Vec::with_capacity(partition_num);
            for i in 0..partition_num {
                let partition_file_name = format!("input-chunks-p{}", i);
                let r = op.reader_with(&partition_file_name).await?;
                readers.push(r);
            }

            for mut agg_state_reader in agg_state_readers {
                let mut buf = [0u8; 4];
                loop {
                    if let Err(err) = agg_state_reader.read_exact(&mut buf).await {
                        if err.kind() == std::io::ErrorKind::UnexpectedEof {
                            break
                        } else {
                            return Err(anyhow!(err).into());
                        }
                    }
                    let len = u32::from_le_bytes(buf) as usize;
                    let mut buf = vec![0u8; len];
                    agg_state_reader.read_exact(&mut buf).await.map_err(|e| anyhow!(e))?;
                    let chunk_pb: PbDataChunk = Message::decode(buf.as_slice()).map_err(|e| anyhow!(e))?;
                    let chunk = DataChunk::from_protobuf(&chunk_pb)?;
                    // println!("agg state chunk = {}", chunk.to_pretty());

                    let group_key_indices = (0..self.group_key_columns.len()).collect_vec();
                    let keys = K::build_many(&group_key_indices, &chunk);
                    let mut memory_usage_diff = 0;
                    for (row_id, key) in keys.into_iter().enumerate() {
                        let mut agg_states = vec![];
                        for i in 0..self.aggs.len() {
                            let agg = &self.aggs[i];
                            let datum = chunk.row_at(row_id).0.datum_at(self.group_key_columns.len() + i).to_owned_datum();
                            let agg_state = agg.decode_state(datum)?;
                            memory_usage_diff += agg_state.estimated_size() as i64;
                            agg_states.push(agg_state);
                        }
                        groups.try_insert(key, agg_states).unwrap();
                    }

                    // if !self.mem_context.add(memory_usage_diff) {
                    //     Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                    // }

                }
            }

            for mut reader in readers {
                let mut buf = [0u8; 4];
                loop {
                    if let Err(err) = reader.read_exact(&mut buf).await {
                        if err.kind() == std::io::ErrorKind::UnexpectedEof {
                            break
                        } else {
                            return Err(anyhow!(err).into());
                        }
                    }
                    let len = u32::from_le_bytes(buf) as usize;
                    let mut buf = vec![0u8; len];
                    reader.read_exact(&mut buf).await.map_err(|e| anyhow!(e))?;
                    let chunk_pb: PbDataChunk = Message::decode(buf.as_slice()).map_err(|e| anyhow!(e))?;
                    let chunk = DataChunk::from_protobuf(&chunk_pb)?;
                    // println!("chunk = {}", chunk.to_pretty());

                    // Process chunk
                    let chunk = StreamChunk::from(chunk);
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
                    // if !self.mem_context.add(memory_usage_diff) {
                    //     Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                    // }

                }
            }
        }

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

#[cfg(test)]
mod tests {
    use std::alloc::{AllocError, Allocator, Global, Layout};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use futures_async_stream::for_await;
    use risingwave_common::catalog::{Field, Schema};
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
