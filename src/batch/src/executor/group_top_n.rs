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
use std::marker::PhantomData;
use std::sync::Arc;
use std::vec::Vec;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::encoding_for_comparison::encode_chunk;
use risingwave_common::util::sort_util::OrderPair;
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
    order_pairs: Vec<OrderPair>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    schema: Schema,
    identity: String,
    _phantom: PhantomData<K>,
}

pub struct GroupTopNExecutorBuilder {
    child: BoxedExecutor,
    order_pairs: Vec<OrderPair>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    group_key_types: Vec<DataType>,
    identity: String,
}

impl HashKeyDispatcher for GroupTopNExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(GroupTopNExecutor::<K>::new(
            self.child,
            self.order_pairs,
            self.offset,
            self.limit,
            self.group_key,
            self.identity,
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

        let order_pairs = top_n_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
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

        let builder = Self {
            child,
            order_pairs,
            offset: top_n_node.get_offset() as usize,
            limit: top_n_node.get_limit() as usize,
            group_key,
            group_key_types,
            identity: source.plan_node().get_identity().clone(),
        };

        Ok(builder.dispatch())
    }
}

impl<K: HashKey> GroupTopNExecutor<K> {
    pub fn new(
        child: BoxedExecutor,
        order_pairs: Vec<OrderPair>,
        offset: usize,
        limit: usize,
        group_key: Vec<usize>,
        identity: String,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            order_pairs,
            offset,
            limit,
            group_key,
            schema,
            identity,
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
        let mut groups: HashMap<K, TopNHeap> = HashMap::new();

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = Arc::new(chunk?.compact());
            let keys = K::build(self.group_key.as_slice(), &chunk)?;

            for (row_id, (encoded_row, key)) in encode_chunk(&chunk, &self.order_pairs)
                .into_iter()
                .zip_eq(keys.into_iter())
                .enumerate()
            {
                let heap = groups
                    .entry(key)
                    .or_insert_with(|| TopNHeap::new(self.limit, self.offset));
                heap.push(HeapElem {
                    encoded_row,
                    chunk: chunk.clone(),
                    row_id,
                });
            }
        }

        let mut chunk_builder = DataChunkBuilder::with_default_size(self.schema.data_types());
        for (_, heap) in groups {
            for HeapElem { chunk, row_id, .. } in heap.dump() {
                if let Some(spilled) =
                    chunk_builder.append_one_row_ref(chunk.row_at_unchecked_vis(row_id))
                {
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
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let top_n_executor = (GroupTopNExecutorBuilder {
            child: Box::new(mock_executor),
            order_pairs,
            offset: 1,
            limit: 3,
            group_key: vec![2],
            group_key_types: vec![DataType::Int32],
            identity: "GroupTopNExecutor".to_string(),
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
