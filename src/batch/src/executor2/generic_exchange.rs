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

use std::marker::PhantomData;

use futures::stream::select_all;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::ExchangeSource as ProstExchangeSource;
use risingwave_pb::plan_common::Field as NodeField;
use risingwave_rpc_client::{ExchangeSource, GrpcExchangeSource};

use crate::execution::local_exchange::LocalExchangeSource;
use crate::executor::ExecutorBuilder;
use crate::task::{BatchEnvironment, TaskId};

pub type ExchangeExecutor2 = GenericExchangeExecutor2<DefaultCreateSource>;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2};

pub struct GenericExchangeExecutor2<C> {
    sources: Vec<ProstExchangeSource>,
    server_addr: HostAddr,
    env: BatchEnvironment,

    source_idx: usize,
    current_source: Option<Box<dyn ExchangeSource>>,

    // Mock-able CreateSource.
    source_creator: PhantomData<C>,
    schema: Schema,
    task_id: TaskId,
    identity: String,
}

/// `CreateSource` determines the right type of `ExchangeSource` to create.
#[async_trait::async_trait]
pub trait CreateSource: Send {
    async fn create_source(
        env: BatchEnvironment,
        prost_source: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>>;
}

pub struct DefaultCreateSource {}

#[async_trait::async_trait]
impl CreateSource for DefaultCreateSource {
    async fn create_source(
        env: BatchEnvironment,
        prost_source: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>> {
        let peer_addr = prost_source.get_host()?.into();

        if is_local_address(env.server_address(), &peer_addr) {
            trace!("Exchange locally [{:?}]", prost_source.get_task_output_id());

            Ok(Box::new(LocalExchangeSource::create(
                prost_source.get_task_output_id()?.try_into()?,
                env,
                task_id,
            )?))
        } else {
            trace!(
                "Exchange remotely from {} [{:?}]",
                &peer_addr,
                prost_source.get_task_output_id()
            );

            Ok(Box::new(
                GrpcExchangeSource::create(peer_addr, prost_source.get_task_output_id()?.clone())
                    .await?,
            ))
        }
    }
}

impl<CS: 'static + CreateSource> BoxedExecutor2Builder for GenericExchangeExecutor2<CS> {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Exchange
        )?;

        let server_addr = source.global_batch_env().server_address().clone();

        ensure!(!node.get_sources().is_empty());
        let sources: Vec<ProstExchangeSource> = node.get_sources().to_vec();
        let input_schema: Vec<NodeField> = node.get_input_schema().to_vec();
        let fields = input_schema.iter().map(Field::from).collect::<Vec<Field>>();
        Ok(Box::new(Self {
            sources,
            server_addr,
            env: source.global_batch_env().clone(),
            source_creator: PhantomData,
            source_idx: 0,
            current_source: None,
            schema: Schema { fields },
            task_id: source.task_id.clone(),
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

impl<CS: 'static + CreateSource> Executor2 for GenericExchangeExecutor2<CS> {
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

impl<CS: 'static + CreateSource> GenericExchangeExecutor2<CS> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut sources: Vec<Box<dyn ExchangeSource>> = vec![];

        for prost_source in &self.sources {
            let source =
                CS::create_source(self.env.clone(), prost_source, self.task_id.clone()).await?;
            sources.push(source);
        }

        let mut stream =
            select_all(sources.into_iter().map(data_chunk_stream).collect_vec()).boxed();

        while let Some(data_chunk) = stream.next().await {
            let data_chunk = data_chunk?;
            yield data_chunk
        }
    }
}
#[try_stream(boxed, ok = DataChunk, error = RwError)]
async fn data_chunk_stream(mut source: Box<dyn ExchangeSource>) {
    loop {
        if let Some(res) = source.take_data().await? {
            if res.cardinality() == 0 {
                debug!("Exchange source {:?} output empty chunk.", source);
            }
            yield res;
            continue;
        }
        break;
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use rand::Rng;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{DataChunk, I32Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::types::DataType;

    use super::*;
    #[tokio::test]
    async fn test_exchange_multiple_sources() {
        #[derive(Debug, Clone)]
        struct FakeExchangeSource {
            chunks: Vec<Option<DataChunk>>,
        }

        #[async_trait::async_trait]
        impl ExchangeSource for FakeExchangeSource {
            async fn take_data(&mut self) -> Result<Option<DataChunk>> {
                if let Some(chunk) = self.chunks.pop() {
                    Ok(chunk)
                } else {
                    Ok(None)
                }
            }
        }

        struct FakeCreateSource {}

        #[async_trait::async_trait]
        impl CreateSource for FakeCreateSource {
            async fn create_source(
                _: BatchEnvironment,
                _: &ProstExchangeSource,
                _: TaskId,
            ) -> Result<Box<dyn ExchangeSource>> {
                let mut rng = rand::thread_rng();
                let i = rng.gen_range(1..=100000);
                let chunk = DataChunk::builder()
                    .columns(vec![Column::new(Arc::new(
                        array_nonnull! { I32Array, [i] }.into(),
                    ))])
                    .build();
                let chunks = vec![Some(chunk); 100];

                Ok(Box::new(FakeExchangeSource { chunks }))
            }
        }

        let mut sources: Vec<ProstExchangeSource> = vec![];
        for _ in 0..2 {
            sources.push(ProstExchangeSource::default());
        }

        let executor = Box::new(GenericExchangeExecutor2::<FakeCreateSource> {
            sources,
            server_addr: "127.0.0.1:5688".parse().unwrap(),
            source_idx: 0,
            current_source: None,
            source_creator: PhantomData,
            env: BatchEnvironment::for_test(),
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int32)],
            },
            task_id: TaskId::default(),
            identity: "GenericExchangeExecutor2".to_string(),
        });

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
