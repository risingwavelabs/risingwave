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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::agg::{build as build_agg, AggCall, BoxedAggState};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashAggNode;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::{BatchTaskContext, TaskId};

type AggHashMap<K, A> = hashbrown::HashMap<K, Vec<BoxedAggState>, PrecomputedBuildHasher, A>;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(
            self.agg_init_states,
            self.group_key_columns,
            self.group_key_types,
            self.schema,
            self.child,
            self.identity,
            self.chunk_size,
            self.mem_context,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder {
    agg_init_states: Vec<BoxedAggState>,
    group_key_columns: Vec<usize>,
    group_key_types: Vec<DataType>,
    child: BoxedExecutor,
    schema: Schema,
    task_id: TaskId,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
}

impl HashAggExecutorBuilder {
    fn deserialize(
        hash_agg_node: &HashAggNode,
        child: BoxedExecutor,
        task_id: TaskId,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
    ) -> Result<BoxedExecutor> {
        let agg_init_states: Vec<_> = hash_agg_node
            .get_agg_calls()
            .iter()
            .map(|agg_call| AggCall::from_protobuf(agg_call).and_then(build_agg))
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
            .chain(agg_init_states.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let builder = HashAggExecutorBuilder {
            agg_init_states,
            group_key_columns,
            group_key_types,
            child,
            schema: Schema { fields },
            task_id,
            identity,
            chunk_size,
            mem_context,
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
        )
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub struct HashAggExecutor<K> {
    /// Factories to construct aggregator for each groups
    agg_init_states: Vec<BoxedAggState>,
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
    _phantom: PhantomData<K>,
}

impl<K> HashAggExecutor<K> {
    pub fn new(
        agg_init_states: Vec<BoxedAggState>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
    ) -> Self {
        HashAggExecutor {
            agg_init_states,
            group_key_columns,
            group_key_types,
            schema,
            child,
            identity,
            chunk_size,
            mem_context,
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

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        // hash map for each agg groups
        let mut groups = AggHashMap::<K, _>::with_hasher_in(
            PrecomputedBuildHasher,
            self.mem_context.global_allocator(),
        );

        // consume all chunks to compute the agg result
        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?.compact();
            let keys = K::build(self.group_key_columns.as_slice(), &chunk)?;
            let mut memory_usage_diff = 0;
            for (row_id, key) in keys.into_iter().enumerate() {
                let mut new_group = false;
                let states = groups.entry(key).or_insert_with(|| {
                    new_group = true;
                    self.agg_init_states.clone()
                });

                // TODO: currently not a vectorized implementation
                for state in states {
                    if !new_group {
                        memory_usage_diff -= state.estimated_size() as i64;
                    }
                    state.update_single(&chunk, row_id).await?;
                    memory_usage_diff += state.estimated_size() as i64;
                }
            }
            // update memory usage
            self.mem_context.add(memory_usage_diff);
        }

        // generate output data chunks
        let mut result = groups.into_iter();
        let cardinality = self.chunk_size;
        loop {
            let mut group_builders: Vec<_> = self
                .group_key_types
                .iter()
                .map(|datatype| datatype.create_array_builder(cardinality))
                .collect();

            let mut agg_builders: Vec<_> = self
                .agg_init_states
                .iter()
                .map(|agg| agg.return_type().create_array_builder(cardinality))
                .collect();

            let mut has_next = false;
            let mut array_len = 0;
            for (key, states) in result.by_ref().take(cardinality) {
                has_next = true;
                array_len += 1;
                key.deserialize_to_builders(&mut group_builders[..], &self.group_key_types)?;
                states
                    .into_iter()
                    .zip_eq_fast(&mut agg_builders)
                    .try_for_each(|(mut aggregator, builder)| aggregator.output(builder))?;
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
    use prometheus::IntGauge;
    use risingwave_common::catalog::{Field, Schema};
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

        let mem_context = MemoryContext::root(IntGauge::new("memory_usage", " ").unwrap());
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            src_exec,
            TaskId::default(),
            "HashAggExecutor".to_string(),
            CHUNK_SIZE,
            mem_context.clone(),
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
        assert_eq!(mem_context.get_bytes_used() as usize, 4 * 72);
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
}
