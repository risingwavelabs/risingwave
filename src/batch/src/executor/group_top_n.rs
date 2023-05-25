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
use std::sync::Arc;
use std::vec::Vec;

use futures_async_stream::try_stream;
use hashbrown::HashMap;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::encode_chunk;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::top_n::{HeapElem, TopNHeap};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Group Top-N Executor
///
/// For each group, use a N-heap to store the smallest N rows.
pub struct GroupTopNExecutor<K: HashKey> {
    child: BoxedExecutor,
    column_orders: Vec<ColumnOrder>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    with_ties: bool,
    schema: Schema,
    identity: String,
    chunk_size: usize,
    mem_ctx: MemoryContext,
    _phantom: PhantomData<K>,
}

pub struct GroupTopNExecutorBuilder {
    child: BoxedExecutor,
    column_orders: Vec<ColumnOrder>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    group_key_types: Vec<DataType>,
    with_ties: bool,
    identity: String,
    chunk_size: usize,
    mem_ctx: MemoryContext,
}

impl HashKeyDispatcher for GroupTopNExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(GroupTopNExecutor::<K>::new(
            self.child,
            self.column_orders,
            self.offset,
            self.limit,
            self.with_ties,
            self.group_key,
            self.identity,
            self.chunk_size,
            self.mem_ctx,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for GroupTopNExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let top_n_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GroupTopN
        )?;

        let column_orders = top_n_node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let group_key = top_n_node
            .group_key
            .iter()
            .map(|x| *x as usize)
            .collect_vec();
        let child_schema = child.schema();
        let group_key_types = group_key
            .iter()
            .map(|x| child_schema.fields[*x].data_type())
            .collect();

        let identity = source.plan_node().get_identity().clone();

        let builder = Self {
            child,
            column_orders,
            offset: top_n_node.get_offset() as usize,
            limit: top_n_node.get_limit() as usize,
            group_key,
            group_key_types,
            with_ties: top_n_node.get_with_ties(),
            identity: identity.clone(),
            chunk_size: source.context.get_config().developer.chunk_size,
            mem_ctx: source.context().create_executor_mem_context(&identity),
        };

        Ok(builder.dispatch())
    }
}

impl<K: HashKey> GroupTopNExecutor<K> {
    pub fn new(
        child: BoxedExecutor,
        column_orders: Vec<ColumnOrder>,
        offset: usize,
        limit: usize,
        with_ties: bool,
        group_key: Vec<usize>,
        identity: String,
        chunk_size: usize,
        mem_ctx: MemoryContext,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            column_orders,
            offset,
            limit,
            with_ties,
            group_key,
            schema,
            identity,
            chunk_size,
            mem_ctx,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey> Executor for GroupTopNExecutor<K> {
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

impl<K: HashKey> GroupTopNExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        let mut groups =
            HashMap::<K, TopNHeap, PrecomputedBuildHasher, MonitoredGlobalAlloc>::with_hasher_in(
                PrecomputedBuildHasher::default(),
                self.mem_ctx.global_allocator(),
            );

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = Arc::new(chunk?.compact());
            let keys = K::build(self.group_key.as_slice(), &chunk)?;

            for (row_id, (encoded_row, key)) in encode_chunk(&chunk, &self.column_orders)?
                .into_iter()
                .zip_eq_fast(keys.into_iter())
                .enumerate()
            {
                let heap = groups.entry(key).or_insert_with(|| {
                    TopNHeap::new(
                        self.limit,
                        self.offset,
                        self.with_ties,
                        self.mem_ctx.clone(),
                    )
                });
                heap.push(HeapElem::new(encoded_row, chunk.row_at(row_id).0));
            }
        }

        let mut chunk_builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        for (_, heap) in groups {
            for ele in heap.dump() {
                if let Some(spilled) = chunk_builder.append_one_row(ele.row()) {
                    yield spilled
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::array::DataChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_group_top_n_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i i
             1 5 1
             2 4 1
             3 3 1
             4 2 1
             5 1 1
             1 6 2
             2 5 2
             3 4 2
             4 3 2
             5 2 2
             ",
        ));
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let top_n_executor = (GroupTopNExecutorBuilder {
            child: Box::new(mock_executor),
            column_orders,
            offset: 1,
            limit: 3,
            with_ties: false,
            group_key: vec![2],
            group_key_types: vec![DataType::Int32],
            identity: "GroupTopNExecutor".to_string(),
            chunk_size: CHUNK_SIZE,
            mem_ctx: MemoryContext::none(),
        })
        .dispatch();

        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            assert!(
                res == DataChunk::from_pretty(
                    "
                    i i i
                    4 2 1
                    3 3 1
                    2 4 1
                    4 3 2
                    3 4 2
                    2 5 2
                    "
                ) || res
                    == DataChunk::from_pretty(
                        "
                    i i i
                    4 3 2
                    3 4 2
                    2 5 2
                    4 2 1
                    3 3 1
                    2 4 1
                    "
                    )
            );
        }

        let res = stream.next().await;
        assert!(matches!(res, None));
    }
}
