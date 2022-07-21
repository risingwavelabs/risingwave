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

use std::collections::{HashMap, LinkedList};
use std::marker::PhantomData;

use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{
    calc_hash_key_kind, HashKey, HashKeyDispatcher, PrecomputedBuildHasher,
};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{ChunkedData, JoinType, RowId};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct HashJoinExecutor<K> {
    join_type: JoinType,
    original_schema: Schema,
    schema: Schema,
    output_indices: Vec<usize>,
    probe_side_source: BoxedExecutor,
    build_side_source: BoxedExecutor,
    probe_key_idxs: Vec<usize>,
    build_key_idxs: Vec<usize>,
    cond: Option<BoxedExpression>,
    identity: String,
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

type JoinHashMap<K> = HashMap<K, RowId, PrecomputedBuildHasher>;

impl<K: HashKey> HashJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut build_side = Vec::new();
        #[for_await]
        for build_chunk in self.build_side_source.execute() {
            let compacted = build_chunk?.compact()?;
            if compacted.capacity() > 0 {
                build_side.push(compacted)
            }
        }
        let build_row_count = build_side.iter().map(|c| c.capacity()).sum();
        let mut hash_map =
            JoinHashMap::with_capacity_and_hasher(build_row_count, PrecomputedBuildHasher);
        let mut next_row_with_same_key =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        // Build hash map
        for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
            let build_keys = K::build(&self.build_key_idxs, build_chunk)?;
            for (build_row_id, build_key) in build_keys.into_iter().enumerate() {
                let row_id = RowId::new(build_chunk_id, build_row_id);
                next_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
            }
        }

        let mut chunk_builder =
            DataChunkBuilder::with_default_size(self.original_schema.data_types());

        let stream = match self.join_type {
            JoinType::Inner => Self::do_inner_join,
            _ => todo!(),
        };

        #[for_await]
        for chunk in stream(
            &mut chunk_builder,
            self.probe_side_source,
            self.probe_key_idxs,
            build_side,
            hash_map,
            next_row_with_same_key,
        ) {
            yield chunk?.reorder_columns(&self.output_indices)
        }

        if let Some(chunk) = chunk_builder.consume_all()? {
            yield chunk.reorder_columns(&self.output_indices)
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_inner_join(
        chunk_builder: &mut DataChunkBuilder,
        probe_side: BoxedExecutor,
        probe_key_idxs: Vec<usize>,
        build_side: Vec<DataChunk>,
        hash_map: JoinHashMap<K>,
        next_row_with_same_key: ChunkedData<Option<RowId>>,
    ) {
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row, probe_key) in probe_chunk.rows().zip_eq(probe_keys) {
                let mut matched_build_row_id = hash_map.get(&probe_key);
                while let Some(&build_row_id) = matched_build_row_id {
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    // Since build chunk is compacted, all rows are visible.
                    let build_row = build_chunk.row_at_unchecked_vis(build_row_id.row_id());
                    let datum_refs = probe_row.values().chain(build_row.values());
                    if let Some(spilled) =
                        chunk_builder.append_one_row_from_datum_refs(datum_refs)?
                    {
                        yield spilled
                    }
                    matched_build_row_id = next_row_with_same_key[build_row_id].as_ref();
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HashJoinExecutor<()> {
    async fn new_boxed_executor<C: BatchTaskContext>(
        context: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 2,
            "HashJoinExecutor should have 2 children!"
        );

        let left_child = inputs.remove(0);
        let right_child = inputs.remove(0);

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

        let hash_key_kind = calc_hash_key_kind(&right_key_types);

        let output_indices: Vec<usize> = hash_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        Ok(HashJoinExecutor::dispatch_by_kind(
            hash_key_kind,
            HashJoinExecutor::new(
                join_type,
                output_indices,
                left_child,
                right_child,
                left_key_idxs,
                right_key_idxs,
                cond,
                context.plan_node().get_identity().clone(),
            ),
        ))
    }
}

impl HashKeyDispatcher for HashJoinExecutor<()> {
    type Input = Self;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: Self::Input) -> Self::Output {
        Box::new(HashJoinExecutor::<K>::new(
            input.join_type,
            input.output_indices,
            input.probe_side_source,
            input.build_side_source,
            input.probe_key_idxs,
            input.build_key_idxs,
            input.cond,
            input.identity,
        ))
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
        cond: Option<BoxedExpression>,
        identity: String,
    ) -> Self {
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
            cond,
            identity,
            _phantom: PhantomData,
        }
    }
}