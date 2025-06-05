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

use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_batch::exchange_source::ExchangeData;
use risingwave_batch::task::task_stats::TaskStatsRef;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::memory::MemoryContext;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::codegen::BoxStream;
use risingwave_pb::batch_plan::PbExchangeSource;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, CreateSource, DefaultCreateSource,
    Executor, ExecutorBuilder, MergeSortExecutor, WrapStreamExecutor,
};
use crate::task::{BatchTaskContext, TaskId};

pub type MergeSortExchangeExecutor = MergeSortExchangeExecutorImpl<DefaultCreateSource>;

/// `MergeSortExchangeExecutor2` takes inputs from multiple sources and
/// The outputs of all the sources have been sorted in the same way.
pub struct MergeSortExchangeExecutorImpl<CS> {
    context: Arc<dyn BatchTaskContext>,
    column_orders: Arc<Vec<ColumnOrder>>,
    proto_sources: Vec<PbExchangeSource>,
    /// Mock-able `CreateSource`.
    source_creators: Vec<CS>,
    schema: Schema,
    task_id: TaskId,
    identity: String,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
    mem_ctx: MemoryContext,
}

impl<CS: 'static + Send + CreateSource> MergeSortExchangeExecutorImpl<CS> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        context: Arc<dyn BatchTaskContext>,
        column_orders: Arc<Vec<ColumnOrder>>,
        proto_sources: Vec<PbExchangeSource>,
        source_creators: Vec<CS>,
        schema: Schema,
        task_id: TaskId,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        let mem_ctx = context.create_executor_mem_context(&identity);

        Self {
            context,
            column_orders,
            proto_sources,
            source_creators,
            schema,
            task_id,
            identity,
            chunk_size,
            mem_ctx,
        }
    }
}

impl<CS: 'static + Send + CreateSource> Executor for MergeSortExchangeExecutorImpl<CS> {
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

#[try_stream(boxed, ok = DataChunk, error = BatchError)]
async fn to_data_chunk_stream(
    exchange_data_stream: BoxStream<'static, Result<ExchangeData>>,
    task_stats: Option<TaskStatsRef>,
) {
    #[for_await]
    for data in exchange_data_stream {
        let data = data?;
        match data {
            ExchangeData::DataChunk(data_chunk) => yield data_chunk,
            ExchangeData::TaskStats(t) => {
                // Accumulate TaskStats of child stage in order to collect QueryStats for local mode.
                if let Some(ref task_stats) = task_stats {
                    task_stats.add(&t);
                }
            }
        }
    }
}

/// Everytime `execute` is called, it tries to produce a chunk of size
/// `self.chunk_size`. It is possible that the chunk's size is smaller than the
/// `self.chunk_size` as the executor runs out of input from `sources`.
impl<CS: 'static + Send + CreateSource> MergeSortExchangeExecutorImpl<CS> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let mut sources: Vec<BoxedExecutor> = vec![];
        for source_idx in 0..self.proto_sources.len() {
            let new_source = self.source_creators[source_idx]
                .create_source(&*self.context, &self.proto_sources[source_idx])
                .await?;
            let data_chunk_stream = to_data_chunk_stream(
                new_source.take_data_stream(),
                self.context.task_stats().clone(),
            );
            sources.push(Box::new(WrapStreamExecutor::new(
                self.schema.clone(),
                data_chunk_stream,
            )));
        }

        let merge_sort_executor = Box::new(MergeSortExecutor::new(
            sources,
            self.column_orders.clone(),
            self.schema,
            format!("MergeSortExecutor{}", &self.task_id.task_id),
            self.chunk_size,
            self.mem_ctx,
        ));

        #[for_await]
        for chunk in merge_sort_executor.execute() {
            yield chunk?;
        }
    }
}

pub struct MergeSortExchangeExecutorBuilder {}

impl BoxedExecutorBuilder for MergeSortExchangeExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "MergeSortExchangeExecutor should not have child!"
        );
        let sort_merge_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::MergeSortExchange
        )?;

        let column_orders = sort_merge_node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();
        let column_orders = Arc::new(column_orders);

        let exchange_node = sort_merge_node.get_exchange()?;
        let proto_sources: Vec<PbExchangeSource> = exchange_node.get_sources().to_vec();
        let source_creators =
            vec![DefaultCreateSource::new(source.context().client_pool()); proto_sources.len()];
        ensure!(!exchange_node.get_sources().is_empty());
        let fields = exchange_node
            .get_input_schema()
            .iter()
            .map(Field::from)
            .collect::<Vec<Field>>();

        Ok(Box::new(MergeSortExchangeExecutor::new(
            source.context().clone(),
            column_orders,
            proto_sources,
            source_creators,
            Schema { fields },
            source.task_id.clone(),
            source.plan_node().get_identity().clone(),
            source.context().get_config().developer.chunk_size,
        )))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::Array;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::{FakeCreateSource, FakeExchangeSource};
    use crate::task::ComputeNodeContext;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_exchange_multiple_sources() {
        let chunk = DataChunk::from_pretty(
            "i
                     1
                     2
                     3",
        );
        let fake_exchange_source = FakeExchangeSource::new(vec![Some(chunk)]);
        let fake_create_source = FakeCreateSource::new(fake_exchange_source);

        let mut proto_sources: Vec<PbExchangeSource> = vec![];
        let mut source_creators = vec![];
        let num_sources = 2;
        for _ in 0..num_sources {
            proto_sources.push(PbExchangeSource::default());
            source_creators.push(fake_create_source.clone());
        }
        let column_orders = Arc::new(vec![ColumnOrder {
            column_index: 0,
            order_type: OrderType::ascending(),
        }]);

        let executor = Box::new(MergeSortExchangeExecutorImpl::<FakeCreateSource>::new(
            ComputeNodeContext::for_test(),
            column_orders,
            proto_sources,
            source_creators,
            Schema {
                fields: vec![Field::unnamed(DataType::Int32)],
            },
            TaskId::default(),
            "MergeSortExchangeExecutor2".to_owned(),
            CHUNK_SIZE,
        ));

        let mut stream = executor.execute();
        let res = stream.next().await;
        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            assert_eq!(res.capacity(), 3 * num_sources);
            let col0 = res.column_at(0);
            assert_eq!(col0.as_int32().value_at(0), Some(1));
            assert_eq!(col0.as_int32().value_at(1), Some(1));
            assert_eq!(col0.as_int32().value_at(2), Some(2));
            assert_eq!(col0.as_int32().value_at(3), Some(2));
            assert_eq!(col0.as_int32().value_at(4), Some(3));
            assert_eq!(col0.as_int32().value_at(5), Some(3));
        }
        let res = stream.next().await;
        assert!(res.is_none());
    }
}
