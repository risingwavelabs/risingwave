// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::{mem, vec};

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::hash::{
    calc_hash_key_kind, HashKey, HashKeyDispatcher, PrecomputedBuildHasher,
};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::vector_op::agg::{AggStateFactory, BoxedAggState};
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::HashAggNode;

use super::{BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::executor::BoxedExecutor;
use crate::task::TaskId;

type AggHashMap<K> = HashMap<K, Vec<BoxedAggState>, PrecomputedBuildHasher>;

struct HashAggExecutorBuilderDispatcher;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilderDispatcher {
    type Input = HashAggExecutorBuilder;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: HashAggExecutorBuilder) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(input).fuse())
    }
}

pub(super) struct HashAggExecutorBuilder {
    agg_factories: Vec<AggStateFactory>,
    group_key_columns: Vec<usize>,
    child: BoxedExecutor,
    group_key_types: Vec<DataType>,
    schema: Schema,
    task_id: TaskId,
    identity: String,
}

impl HashAggExecutorBuilder {
    fn deserialize(
        hash_agg_node: &HashAggNode,
        child: BoxedExecutor,
        task_id: TaskId,
        identity: String,
    ) -> Result<BoxedExecutor> {
        let group_key_columns = hash_agg_node
            .get_group_keys()
            .iter()
            .map(|x| *x as usize)
            .collect_vec();

        let agg_factories = hash_agg_node
            .get_agg_calls()
            .iter()
            .map(AggStateFactory::new)
            .collect::<Result<Vec<AggStateFactory>>>()?;

        let child_schema = child.schema();

        let group_key_types = group_key_columns
            .iter()
            .map(|i| child_schema.fields[*i].data_type.clone())
            .collect_vec();

        let fields = group_key_types
            .iter()
            .cloned()
            .chain(agg_factories.iter().map(|e| e.get_return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let hash_key_kind = calc_hash_key_kind(&group_key_types);

        let builder = HashAggExecutorBuilder {
            agg_factories,
            group_key_columns,
            child,
            group_key_types,
            schema: Schema { fields },
            task_id,
            identity,
        };

        Ok(HashAggExecutorBuilderDispatcher::dispatch_by_kind(
            hash_key_kind,
            builder,
        ))
    }
}

impl BoxedExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);

        let proto_child = &source.plan_node().get_children()[0];
        let child = source.clone_for_plan(proto_child).build()?;

        let hash_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HashAgg
        )?;

        let identity = source.plan_node().get_identity().clone();
        Self::deserialize(hash_agg_node, child, source.task_id.clone(), identity)
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub(super) struct HashAggExecutor<K> {
    /// factories to construct aggregator for each groups
    agg_factories: Vec<AggStateFactory>,
    /// Column indexes of keys that specify a group
    group_key_columns: Vec<usize>,
    /// child executor
    child: BoxedExecutor,
    /// hash map for each agg groups
    groups: AggHashMap<K>,
    /// the aggregated result set
    result: Option<<AggHashMap<K> as IntoIterator>::IntoIter>,
    /// the data types of key columns
    group_key_types: Vec<DataType>,
    schema: Schema,
    identity: String,
}

impl<K> HashAggExecutor<K> {
    fn new(builder: HashAggExecutorBuilder) -> Self {
        HashAggExecutor {
            agg_factories: builder.agg_factories,
            group_key_columns: builder.group_key_columns,
            child: builder.child,
            groups: AggHashMap::<K>::default(),
            group_key_types: builder.group_key_types,
            result: None,
            schema: builder.schema,
            identity: builder.identity,
        }
    }
}

#[async_trait::async_trait]
impl<K: HashKey + Send + Sync> Executor for HashAggExecutor<K> {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;

        while let Some(chunk) = self.child.next().await? {
            let chunk = chunk.compact()?;
            let keys = K::build(self.group_key_columns.as_slice(), &chunk)?;
            for (row_id, key) in keys.into_iter().enumerate() {
                let mut err_flag = Ok(());
                let states: &mut Vec<BoxedAggState> = self.groups.entry(key).or_insert_with(|| {
                    self.agg_factories
                        .iter()
                        .map(AggStateFactory::create_agg_state)
                        .collect::<Result<Vec<_>>>()
                        .unwrap_or_else(|x| {
                            err_flag = Err(x);
                            vec![]
                        })
                });
                err_flag?;

                // TODO: currently not a vectorized implementation
                states
                    .iter_mut()
                    .for_each(|state| state.update_with_row(&chunk, row_id).unwrap());
            }
        }

        assert!(self.result.is_none());
        self.result = Some(mem::take(&mut self.groups).into_iter());

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if let Some(res) = self.result.as_mut() {
            let cardinality = DEFAULT_CHUNK_BUFFER_SIZE;
            let mut group_builders = self
                .group_key_types
                .iter()
                .map(|datatype| datatype.create_array_builder(cardinality))
                .collect::<Result<Vec<_>>>()?;

            let mut agg_builders = self
                .agg_factories
                .iter()
                .map(|agg_factory| {
                    agg_factory
                        .get_return_type()
                        .create_array_builder(cardinality)
                })
                .collect::<Result<Vec<_>>>()?;

            let mut has_next = false;
            for (key, states) in res.take(cardinality) {
                has_next = true;
                key.deserialize_to_builders(&mut group_builders[..])?;
                states
                    .into_iter()
                    .zip_eq(&mut agg_builders)
                    .try_for_each(|(aggregator, builder)| aggregator.output(builder))?;
            }
            if !has_next {
                self.result = None;
                return Ok(None);
            }

            let columns = group_builders
                .into_iter()
                .chain(agg_builders)
                .map(|b| Ok(Column::new(Arc::new(b.finish()?))))
                .collect::<Result<Vec<_>>>()?;

            let ret = DataChunk::builder().columns(columns).build();

            return Ok(Some(ret));
        }

        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I32Array, I64Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::agg_call::{Arg, Type};
    use risingwave_pb::expr::{AggCall, InputRefExpr};

    use super::*;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};

    #[tokio::test]
    async fn execute_int32_grouped() {
        let key1_col = Arc::new(array_nonnull! { I32Array, [0,1,0,1,1,0,1,0] }.into());
        let key2_col = Arc::new(array_nonnull! { I32Array, [1,1,0,1,0,0,1,1] }.into());
        let sum_col = Arc::new(array_nonnull! { I32Array,  [1,1,1,2,1,2,3,2] }.into());

        let t32 = DataType::Int32;
        let t64 = DataType::Int64;

        let src_exec = MockExecutor::with_chunk(
            DataChunk::builder()
                .columns(vec![
                    Column::new(key1_col),
                    Column::new(key2_col),
                    Column::new(sum_col),
                ])
                .build(),
            Schema {
                fields: vec![
                    Field::unnamed(t32.clone()),
                    Field::unnamed(t32.clone()),
                    Field::unnamed(t32.clone()),
                ],
            },
        );

        let agg_call = AggCall {
            r#type: Type::Sum as i32,
            args: vec![Arg {
                input: Some(InputRefExpr { column_idx: 2 }),
                r#type: Some(ProstDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(ProstDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
        };

        let agg_prost = HashAggNode {
            group_keys: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_string(),
        )
        .unwrap();

        let schema = Schema {
            fields: vec![
                Field::unnamed(t32.clone()),
                Field::unnamed(t32),
                Field::unnamed(t64),
            ],
        };

        // TODO: currently the order is fixed
        let group1_col = Arc::new(array_nonnull! { I32Array, [0,1,0,1] }.into());
        let group2_col = Arc::new(array_nonnull! { I32Array, [0,1,1,0] }.into());
        let anssum_col = Arc::new(array_nonnull! { I64Array, [3,6,3,1] }.into());

        let expect_exec = MockExecutor::with_chunk(
            DataChunk::builder()
                .columns(vec![
                    Column::new(group1_col),
                    Column::new(group2_col),
                    Column::new(anssum_col),
                ])
                .build(),
            schema,
        );
        diff_executor_output(actual_exec, Box::new(expect_exec)).await;
    }

    #[tokio::test]
    async fn execute_count_star() {
        let col = Arc::new(array_nonnull! { I32Array, [0,1,0,1,1,0,1,0] }.into());
        let t32 = DataType::Int32;
        let src_exec = MockExecutor::with_chunk(
            DataChunk::builder().columns(vec![Column::new(col)]).build(),
            Schema {
                fields: vec![Field::unnamed(t32.clone())],
            },
        );

        let agg_call = AggCall {
            r#type: Type::Count as i32,
            args: vec![],
            return_type: Some(ProstDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
        };

        let agg_prost = HashAggNode {
            group_keys: vec![],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_string(),
        )
        .unwrap();
        let schema = Schema {
            fields: vec![Field::unnamed(t32)],
        };
        let res = Arc::new(array_nonnull! { I64Array, [8] }.into());

        let expect_exec = MockExecutor::with_chunk(
            DataChunk::builder().columns(vec![Column::new(res)]).build(),
            schema,
        );
        diff_executor_output(actual_exec, Box::new(expect_exec)).await;
    }
}
