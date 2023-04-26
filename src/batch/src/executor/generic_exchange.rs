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

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::select_all;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::PbExchangeSource;
use risingwave_pb::plan_common::Field as NodeField;
use risingwave_rpc_client::ComputeClientPoolRef;

use crate::exchange_source::ExchangeSourceImpl;
use crate::execution::grpc_exchange::GrpcExchangeSource;
use crate::execution::local_exchange::LocalExchangeSource;
use crate::executor::ExecutorBuilder;
use crate::task::{BatchTaskContext, TaskId};

pub type ExchangeExecutor<C> = GenericExchangeExecutor<DefaultCreateSource, C>;
use std::sync::Arc;

use crate::executor::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor};
use crate::monitor::BatchMetricsWithTaskLabels;
use crate::task::StopFlag;

pub struct GenericExchangeExecutor<CS, C> {
    proto_sources: Vec<PbExchangeSource>,
    /// Mock-able CreateSource.
    source_creators: Vec<CS>,
    context: C,

    schema: Schema,
    task_id: TaskId,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchMetricsWithTaskLabels>,

    stop_flag: Arc<StopFlag>,
}

/// `CreateSource` determines the right type of `ExchangeSource` to create.
#[async_trait::async_trait]
pub trait CreateSource: Send {
    async fn create_source(
        &self,
        context: impl BatchTaskContext,
        prost_source: &PbExchangeSource,
    ) -> Result<ExchangeSourceImpl>;
}

#[derive(Clone)]
pub struct DefaultCreateSource {
    client_pool: ComputeClientPoolRef,
}

impl DefaultCreateSource {
    pub fn new(client_pool: ComputeClientPoolRef) -> Self {
        Self { client_pool }
    }
}

#[async_trait::async_trait]
impl CreateSource for DefaultCreateSource {
    async fn create_source(
        &self,
        context: impl BatchTaskContext,
        prost_source: &PbExchangeSource,
    ) -> Result<ExchangeSourceImpl> {
        let peer_addr = prost_source.get_host()?.into();
        let task_output_id = prost_source.get_task_output_id()?;
        let task_id = TaskId::from(task_output_id.get_task_id()?);

        if context.is_local_addr(&peer_addr) && prost_source.local_execute_plan.is_none() {
            trace!("Exchange locally [{:?}]", task_output_id);

            Ok(ExchangeSourceImpl::Local(LocalExchangeSource::create(
                task_output_id.try_into()?,
                context,
                task_id,
            )?))
        } else {
            trace!(
                "Exchange remotely from {} [{:?}]",
                &peer_addr,
                task_output_id,
            );

            Ok(ExchangeSourceImpl::Grpc(
                GrpcExchangeSource::create(
                    self.client_pool.get_by_addr(peer_addr).await?,
                    task_output_id.clone(),
                    prost_source.local_execute_plan.clone(),
                )
                .await?,
            ))
        }
    }
}

pub struct GenericExchangeExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for GenericExchangeExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Exchange executor should not have children!"
        );
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Exchange
        )?;

        ensure!(!node.get_sources().is_empty());
        let proto_sources: Vec<PbExchangeSource> = node.get_sources().to_vec();
        let source_creators =
            vec![DefaultCreateSource::new(source.context().client_pool()); proto_sources.len()];

        let input_schema: Vec<NodeField> = node.get_input_schema().to_vec();
        let fields = input_schema.iter().map(Field::from).collect::<Vec<Field>>();
        Ok(Box::new(ExchangeExecutor::<C> {
            proto_sources,
            source_creators,
            context: source.context().clone(),
            schema: Schema { fields },
            task_id: source.task_id.clone(),
            identity: source.plan_node().get_identity().clone(),
            metrics: source.context().batch_metrics(),
            stop_flag: source.context().get_stop_flag(),
        }))
    }
}

impl<CS: 'static + Send + CreateSource, C: BatchTaskContext> Executor
    for GenericExchangeExecutor<CS, C>
{
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

impl<CS: 'static + Send + CreateSource, C: BatchTaskContext> GenericExchangeExecutor<CS, C> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut stream = select_all(
            self.proto_sources
                .into_iter()
                .zip_eq_fast(self.source_creators)
                .map(|(prost_source, source_creator)| {
                    Self::data_chunk_stream(
                        prost_source,
                        source_creator,
                        self.context.clone(),
                        self.metrics.clone(),
                        self.identity.clone(),
                    )
                })
                .collect_vec(),
        )
        .boxed();

        while let Some(data_chunk) = stream.next().await {
            if self.stop_flag.check_stop() {
                return Ok(());
            }
            let data_chunk = data_chunk?;
            yield data_chunk
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn data_chunk_stream(
        prost_source: PbExchangeSource,
        source_creator: CS,
        context: C,
        metrics: Option<BatchMetricsWithTaskLabels>,
        identity: String,
    ) {
        let mut source = source_creator
            .create_source(context.clone(), &prost_source)
            .await?;
        // create the collector
        let source_id = source.get_task_id();
        let counter = metrics.as_ref().map(|metrics| {
            metrics.create_collector_for_exchange_recv_row_number(vec![
                identity,
                source_id.query_id,
                source_id.stage_id.to_string(),
                source_id.task_id.to_string(),
            ])
        });

        loop {
            if let Some(res) = source.take_data().await? {
                if res.cardinality() == 0 {
                    debug!("Exchange source {:?} output empty chunk.", source);
                }

                if let Some(ref counter) = counter {
                    counter.inc_by(res.cardinality().try_into().unwrap());
                }

                yield res;
                continue;
            }
            break;
        }
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use rand::Rng;
    use risingwave_common::array::{DataChunk, I32Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::{FakeCreateSource, FakeExchangeSource};
    use crate::task::ComputeNodeContext;
    #[tokio::test]
    async fn test_exchange_multiple_sources() {
        let context = ComputeNodeContext::for_test();
        let mut proto_sources = vec![];
        let mut source_creators = vec![];
        for _ in 0..2 {
            let mut rng = rand::thread_rng();
            let i = rng.gen_range(1..=100000);
            let chunk = DataChunk::new(vec![array_nonnull! { I32Array, [i] }.into()], 1);
            let chunks = vec![Some(chunk); 100];
            let fake_exchange_source = FakeExchangeSource::new(chunks);
            let fake_create_source = FakeCreateSource::new(fake_exchange_source);
            proto_sources.push(PbExchangeSource::default());
            source_creators.push(fake_create_source);
        }

        let executor = Box::new(
            GenericExchangeExecutor::<FakeCreateSource, ComputeNodeContext> {
                metrics: None,
                proto_sources,
                source_creators,
                context,
                schema: Schema {
                    fields: vec![Field::unnamed(DataType::Int32)],
                },
                task_id: TaskId::default(),
                identity: "GenericExchangeExecutor2".to_string(),
                stop_flag: Arc::new(StopFlag::default()),
            },
        );

        let mut stream = executor.execute();
        let mut chunks: Vec<DataChunk> = vec![];
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            chunks.push(chunk);
            if chunks.len() == 100 {
                chunks.dedup();
                assert_ne!(chunks.len(), 1);
                chunks.clear();
            }
        }
    }
}
