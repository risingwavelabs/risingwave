use std::marker::PhantomData;
use std::net::SocketAddr;

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, to_socket_addr};
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{ExchangeSource as ProstExchangeSource, Field as NodeField};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::execution::exchange_source::{ExchangeSource, GrpcExchangeSource, LocalExchangeSource};
use crate::executor::{Executor, ExecutorBuilder};
use crate::task::{BatchTaskEnv, TaskId};

pub(super) type ExchangeExecutor = GenericExchangeExecutor<DefaultCreateSource>;

pub struct GenericExchangeExecutor<C> {
    sources: Vec<ProstExchangeSource>,
    server_addr: SocketAddr,
    env: BatchTaskEnv,

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
        env: BatchTaskEnv,
        value: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>>;
}

pub struct DefaultCreateSource {}

#[async_trait::async_trait]
impl CreateSource for DefaultCreateSource {
    async fn create_source(
        env: BatchTaskEnv,
        value: &ProstExchangeSource,
        task_id: TaskId,
    ) -> Result<Box<dyn ExchangeSource>> {
        let peer_addr = to_socket_addr(value.get_host()?)?;
        if is_local_address(env.server_address(), &peer_addr) {
            debug!("Exchange locally [{:?}]", value.get_sink_id());
            return Ok(Box::new(LocalExchangeSource::create(
                value.get_sink_id()?.try_into()?,
                env,
                task_id,
            )?));
        }
        debug!(
            "Exchange remotely from {} [{:?}]",
            &peer_addr,
            value.get_sink_id()
        );
        Ok(Box::new(
            GrpcExchangeSource::create(peer_addr, task_id, value.get_sink_id()?.try_into()?)
                .await?,
        ))
    }
}

impl<CS: 'static + CreateSource> BoxedExecutorBuilder for GenericExchangeExecutor<CS> {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Exchange
        )?;

        let server_addr = *source.env.server_address();

        ensure!(!node.get_sources().is_empty());
        let sources: Vec<ProstExchangeSource> = node.get_sources().to_vec();
        let input_schema: Vec<NodeField> = node.get_input_schema().to_vec();
        let fields = input_schema.iter().map(Field::from).collect::<Vec<Field>>();
        Ok(Box::new(Self {
            sources,
            server_addr,
            env: source.env.clone(),
            source_creator: PhantomData,
            source_idx: 0,
            current_source: None,
            schema: Schema { fields },
            task_id: source.task_id.clone(),
            identity: format!("GenericExchangeExecutor{:?}", source.task_id),
        }))
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
                    assert_ne!(res.cardinality(), 0);
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
    use risingwave_common::types::DataTypeKind;

    use super::*;

    #[tokio::test]
    async fn test_exchange_multiple_sources() {
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
                _: BatchTaskEnv,
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
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_idx: 0,
            current_source: None,
            source_creator: PhantomData,
            env: BatchTaskEnv::for_test(),
            schema: Schema {
                fields: vec![Field::unnamed(DataTypeKind::Int32)],
            },
            task_id: TaskId::default(),
            identity: format!("GenericExchangeExecutor{:?}", TaskId::default()),
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
