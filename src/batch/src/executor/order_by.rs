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

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::encoding_for_comparison::encode_chunk;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// Order By Executor
///
/// High-level idea:
/// 1. Load data chunks from child executor
/// 2. Serialize each row into memcomparable format
/// 3. Sort the serialized rows by quicksort
/// 4. Build and yield data chunks according to the row order
pub struct OrderByExecutor {
    child: BoxedExecutor,
    order_pairs: Vec<OrderPair>,
    identity: String,
    schema: Schema,
}

impl Executor for OrderByExecutor {
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

#[async_trait::async_trait]
impl BoxedExecutorBuilder for OrderByExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 1,
            "OrderByExecutor should have only 1 child!"
        );

        let order_by_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::OrderBy
        )?;

        let order_pairs = order_by_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        Ok(Box::new(OrderByExecutor::new(
            inputs.remove(0),
            order_pairs,
            source.plan_node().get_identity().clone(),
        )))
    }
}

impl OrderByExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(self.schema.data_types());
        let mut chunks = Vec::new();
        let mut encoded_rows = Vec::new();

        #[for_await]
        for chunk in self.child.execute() {
            chunks.push(chunk?.compact()?);
        }

        for chunk in &chunks {
            let encoded_chunk = encode_chunk(chunk, &self.order_pairs);
            encoded_rows.extend(
                encoded_chunk
                    .into_iter()
                    .enumerate()
                    .map(|(row_id, row)| (chunk.row_at_unchecked_vis(row_id), row)),
            );
        }

        encoded_rows.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        for (row, _) in encoded_rows {
            if let Some(spilled) = chunk_builder.append_one_row_ref(row)? {
                yield spilled
            }
        }

        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }
}

impl OrderByExecutor {
    pub fn new(child: BoxedExecutor, order_pairs: Vec<OrderPair>, identity: String) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            order_pairs,
            identity,
            schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{Array, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn test_simple_order_by_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 3
             2 2
             3 1",
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

        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            "OrderByExecutor2".to_string(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
            assert_eq!(col0.array().as_int32().value_at(2), Some(1));
        }
    }

    #[tokio::test]
    async fn test_encoding_for_float() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Float32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            " f    F
             -2.2  3.3
             -1.1  2.2
              1.1  1.1
              2.2 -1.1
              3.3 -2.2",
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
        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            "OrderByExecutor2".to_string(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Float32);
        assert_eq!(fields[1].data_type, DataType::Float64);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_float32().value_at(0), Some(3.3.into()));
            assert_eq!(col0.array().as_float32().value_at(1), Some(2.2.into()));
            assert_eq!(col0.array().as_float32().value_at(2), Some(1.1.into()));
            assert_eq!(col0.array().as_float32().value_at(3), Some((-1.1).into()));
            assert_eq!(col0.array().as_float32().value_at(4), Some((-2.2).into()));
        }
    }

    #[tokio::test]
    async fn test_bsc_for_string() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "T   T
             1.1 3.3
             2.2 2.2
             3.3 1.1",
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
        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            "OrderByExecutor2".to_string(),
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Varchar);
        assert_eq!(fields[1].data_type, DataType::Varchar);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_utf8().value_at(0), Some("3.3"));
            assert_eq!(col0.array().as_utf8().value_at(1), Some("2.2"));
            assert_eq!(col0.array().as_utf8().value_at(2), Some("1.1"));
        }
    }
}
