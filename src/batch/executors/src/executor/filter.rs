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

use futures_async_stream::try_stream;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::expr::{BoxedExpression, build_batch_expr_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

pub struct FilterExecutor {
    expr: BoxedExpression,
    child: BoxedExecutor,
    identity: String,
    chunk_size: usize,
}

impl Executor for FilterExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl FilterExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.child.schema().data_types(), self.chunk_size);

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?.compact();
            let vis_array = self.expr.eval(&data_chunk).await?;

            if let Bool(vis) = vis_array.as_ref() {
                // TODO: should we yield masked data chunk directly?
                for data_chunk in
                    data_chunk_builder.append_chunk(data_chunk.with_visibility(vis.to_bitmap()))
                {
                    yield data_chunk;
                }
            } else {
                bail!("Filter can only receive bool array");
            }
        }

        if let Some(chunk) = data_chunk_builder.consume_all() {
            yield chunk;
        }
    }
}

impl BoxedExecutorBuilder for FilterExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [input]: [_; 1] = inputs.try_into().unwrap();

        let filter_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Filter
        )?;

        let expr_node = filter_node.get_search_condition()?;
        let expr = build_batch_expr_from_prost(expr_node)?;
        Ok(Box::new(Self::new(
            expr,
            input,
            source.plan_node().get_identity().clone(),
            source.context().get_config().developer.chunk_size,
        )))
    }
}

impl FilterExecutor {
    pub fn new(
        expr: BoxedExpression,
        input: BoxedExecutor,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            expr,
            child: input,
            identity,
            chunk_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::stream::StreamExt;
    use risingwave_common::array::{Array, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_pretty;

    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{Executor, FilterExecutor};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_list_filter_executor() {
        use risingwave_common::array::{ArrayBuilder, ListArrayBuilder, ListValue};
        use risingwave_common::types::Scalar;

        let mut builder = ListArrayBuilder::with_type(4, DataType::List(Box::new(DataType::Int32)));

        // Add 4 ListValues to ArrayBuilder
        for i in 1..=4 {
            builder.append(Some(ListValue::from_iter([i]).as_scalar_ref()));
        }

        // Use builder to obtain a single (List) column DataChunk
        let chunk = DataChunk::new(vec![builder.finish().into_ref()], 4);

        // Initialize mock executor
        let mut mock_executor = MockExecutor::new(Schema {
            fields: vec![Field::unnamed(DataType::List(Box::new(DataType::Int32)))],
        });
        mock_executor.add(chunk);

        // Initialize filter executor
        let filter_executor = Box::new(FilterExecutor {
            expr: build_from_pretty("(greater_than:boolean $0:int4[] {2}:int4[])"),
            child: Box::new(mock_executor),
            identity: "FilterExecutor".to_owned(),
            chunk_size: CHUNK_SIZE,
        });

        let fields = &filter_executor.schema().fields;

        assert!(
            fields
                .iter()
                .all(|f| f.data_type == DataType::List(Box::new(DataType::Int32)))
        );

        let mut stream = filter_executor.execute();

        let res = stream.next().await.unwrap();

        assert_matches!(res, Ok(_));
        if let Ok(res) = res {
            let col1 = res.column_at(0);
            let array = col1;
            let col1 = array.as_list();
            assert_eq!(col1.len(), 2);
            // Assert that values 3 and 4 are bigger than 2
            assert_eq!(
                col1.value_at(0),
                Some(ListValue::from_iter([3]).as_scalar_ref())
            );
            assert_eq!(
                col1.value_at(1),
                Some(ListValue::from_iter([4]).as_scalar_ref())
            );
        }
        let res = stream.next().await;
        assert_matches!(res, None);
    }

    #[tokio::test]
    async fn test_filter_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             2 1
             2 2
             4 1
             3 3",
        ));
        let filter_executor = Box::new(FilterExecutor {
            expr: build_from_pretty("(equal:boolean $0:int4 $1:int4)"),
            child: Box::new(mock_executor),
            identity: "FilterExecutor".to_owned(),
            chunk_size: CHUNK_SIZE,
        });
        let fields = &filter_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        let mut stream = filter_executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));
        if let Ok(res) = res {
            let col1 = res.column_at(0);
            let array = col1;
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 2);
            assert_eq!(col1.value_at(0), Some(2));
            assert_eq!(col1.value_at(1), Some(3));
        }
        let res = stream.next().await;
        assert_matches!(res, None);
    }
}
