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

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{ExchangeSource as ProstExchangeSource, Field as NodeField};
use risingwave_rpc_client::{ExchangeSource, GrpcExchangeSource};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::execution::local_exchange::LocalExchangeSource;
use crate::executor::{Executor, ExecutorBuilder};
use crate::task::{BatchEnvironment, TaskId};

pub(super) type ExchangeExecutor = GenericExchangeExecutor<DefaultCreateSource>;

pub struct GenericExchangeExecutor<C> {
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
        value: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>>;
}

pub struct DefaultCreateSource {}

#[async_trait::async_trait]
impl CreateSource for DefaultCreateSource {
    async fn create_source(
        env: BatchEnvironment,
        value: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>> {
        let peer_addr = value.get_host()?.into();
        if is_local_address(env.server_address(), &peer_addr) {
            trace!("Exchange locally [{:?}]", value.get_task_output_id());
            return Ok(Box::new(LocalExchangeSource::create(
                value.get_task_output_id()?.try_into()?,
                env,
                task_id,
            )?));
        }
        trace!(
            "Exchange remotely from {} [{:?}]",
            &peer_addr,
            value.get_task_output_id()
        );
        Ok(Box::new(
            GrpcExchangeSource::create(peer_addr, value.get_task_output_id()?.clone()).await?,
        ))
    }
}

impl<CS: 'static + CreateSource> BoxedExecutorBuilder for GenericExchangeExecutor<CS> {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Exchange
        )?;

        let server_addr = source.env.server_address().clone();

        ensure!(!node.get_sources().is_empty());
        let sources: Vec<ProstExchangeSource> = node.get_sources().to_vec();
        let input_schema: Vec<NodeField> = node.get_input_schema().to_vec();
        let fields = input_schema.iter().map(Field::from).collect::<Vec<Field>>();
        Ok(Box::new(
            Self {
                sources,
                server_addr,
                env: source.env.clone(),
                source_creator: PhantomData,
                source_idx: 0,
                current_source: None,
                schema: Schema { fields },
                task_id: source.task_id.clone(),
                identity: source.plan_node().get_identity().clone(),
            }
            .fuse(),
        ))
    }
}

#[async_trait::async_trait]
impl<CS: CreateSource> Executor for GenericExchangeExecutor<CS> {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        loop {
            if self.source_idx >= self.sources.len() {
                break;
            }
            if self.current_source.is_none() {
                let proto_source = &self.sources[self.source_idx];
                let source =
                    CS::create_source(self.env.clone(), proto_source, self.task_id.clone()).await?;
                self.current_source = Some(source);
            }
            let mut source = self.current_source.take().unwrap();
            match source.take_data().await? {
                None => {
                    self.current_source = None;
                    self.source_idx += 1;
                }
                Some(res) => {
                    if res.cardinality() == 0 {
                        debug!("Exchange source {:?} output empty chunk.", source);
                        assert_ne!(res.cardinality(), 0);
                    }
                    self.current_source = Some(source);
                    return Ok(Some(res));
                }
            }
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
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{DataChunk, I32Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::types::DataType;

    use super::*;

    #[tokio::test]
    async fn test_exchange_multiple_sources() {
        #[derive(Debug)]
        struct FakeExchangeSource {
            chunk: Option<DataChunk>,
        }

        #[async_trait::async_trait]
        impl ExchangeSource for FakeExchangeSource {
            async fn take_data(&mut self) -> Result<Option<DataChunk>> {
                let chunk = self.chunk.take();
                Ok(chunk)
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
                let chunk = DataChunk::builder()
                    .columns(vec![Column::new(Arc::new(
                        array_nonnull! { I32Array, [3, 4, 4] }.into(),
                    ))])
                    .build();
                Ok(Box::new(FakeExchangeSource { chunk: Some(chunk) }))
            }
        }

        let mut sources: Vec<ProstExchangeSource> = vec![];
        for _ in 0..3 {
            sources.push(ProstExchangeSource::default());
        }

        let mut executor = GenericExchangeExecutor::<FakeCreateSource> {
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
            identity: "GenericExchangeExecutor".to_string(),
        };

        let mut chunks: usize = 0;
        loop {
            let res = executor.next().await.unwrap();
            match res {
                Some(_) => chunks += 1,
                None => break,
            }
        }
        assert_eq!(chunks, 3);
    }
}
