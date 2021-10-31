use std::marker::PhantomData;
use std::net::SocketAddr;

use protobuf::Message;

use risingwave_pb::ToProst;
use risingwave_proto::plan::PlanNode_PlanNodeType;
use risingwave_proto::task_service::{ExchangeNode, ExchangeSource as ProtoExchangeSource};

use crate::execution::exchange_source::{ExchangeSource, GrpcExchangeSource, LocalExchangeSource};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::task::GlobalTaskEnv;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::ProtobufError;
use risingwave_common::error::Result;
use risingwave_common::util::addr::{get_host_port, is_local_address};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) type ExchangeExecutor = GenericExchangeExecutor<DefaultCreateSource>;

pub struct GenericExchangeExecutor<C> {
    sources: Vec<ProtoExchangeSource>,
    server_addr: SocketAddr,
    env: GlobalTaskEnv,

    source_idx: usize,
    current_source: Option<Box<dyn ExchangeSource>>,

    // Mock-able CreateSource.
    source_creator: PhantomData<C>,
}

/// `CreateSource` determines the right type of `ExchangeSource` to create.
#[async_trait::async_trait]
pub trait CreateSource: Send {
    async fn create_source(
        env: GlobalTaskEnv,
        value: &ProtoExchangeSource,
    ) -> Result<Box<dyn ExchangeSource>>;
}

pub struct DefaultCreateSource {}

#[async_trait::async_trait]
impl CreateSource for DefaultCreateSource {
    async fn create_source(
        env: GlobalTaskEnv,
        value: &ProtoExchangeSource,
    ) -> Result<Box<dyn ExchangeSource>> {
        let peer_addr = get_host_port(
            format!(
                "{}:{}",
                value.get_host().get_host(),
                value.get_host().get_port()
            )
            .as_str(),
        )?;
        if is_local_address(env.server_address(), &peer_addr) {
            debug!("Exchange locally [{:?}]", value.get_sink_id());
            return Ok(Box::new(LocalExchangeSource::create(
                value.get_sink_id().clone().to_prost(),
                env,
            )?));
        }
        debug!(
            "Exchange remotely from {} [{:?}]",
            &peer_addr,
            value.get_sink_id()
        );
        Ok(Box::new(
            GrpcExchangeSource::create(peer_addr, value.get_sink_id().clone().to_prost()).await?,
        ))
    }
}

impl<CS: 'static + CreateSource> BoxedExecutorBuilder for GenericExchangeExecutor<CS> {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::EXCHANGE);
        let node = ExchangeNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let server_addr = *source.env.server_address();

        ensure!(!node.get_sources().is_empty());
        let sources: Vec<ProtoExchangeSource> = node.get_sources().to_vec();
        Ok(Box::new(Self {
            sources,
            server_addr,
            env: source.env.clone(),
            source_creator: PhantomData,
            source_idx: 0,
            current_source: None,
        }))
    }
}

#[async_trait::async_trait]
impl<CS: CreateSource> Executor for GenericExchangeExecutor<CS> {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        loop {
            if self.source_idx >= self.sources.len() {
                break;
            }
            if self.current_source.is_none() {
                let proto_source = &self.sources[self.source_idx];
                let source = CS::create_source(self.env.clone(), proto_source).await?;
                self.current_source = Some(source);
            }
            let mut source = self.current_source.take().unwrap();
            match source.take_data().await? {
                None => {
                    self.current_source = None;
                    self.source_idx += 1;
                }
                Some(res) => {
                    self.current_source = Some(source);
                    return Ok(ExecutorResult::Batch(res));
                }
            }
        }
        Ok(ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{DataChunk, I32Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::types::Int32Type;

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
                _: GlobalTaskEnv,
                _: &ProtoExchangeSource,
            ) -> Result<Box<dyn ExchangeSource>> {
                let chunk = DataChunk::builder()
                    .columns(vec![Column::new(
                        Arc::new(array_nonnull! { I32Array, [3, 4, 4] }.into()),
                        Int32Type::create(false),
                    )])
                    .build();
                Ok(Box::new(FakeExchangeSource { chunk: Some(chunk) }))
            }
        }

        let mut sources: Vec<ProtoExchangeSource> = vec![];
        for _ in 0..3 {
            sources.push(ProtoExchangeSource::default());
        }

        let mut executor = GenericExchangeExecutor::<FakeCreateSource> {
            sources,
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_idx: 0,
            current_source: None,
            source_creator: PhantomData,
            env: GlobalTaskEnv::for_test(),
        };

        let mut chunks: usize = 0;
        loop {
            let res = executor.execute().await.unwrap();
            match res {
                ExecutorResult::Batch(_) => chunks += 1,
                ExecutorResult::Done => break,
            }
        }
        assert_eq!(chunks, 3);
    }
}
