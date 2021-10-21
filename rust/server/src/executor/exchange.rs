use crate::error::ErrorCode::ProtobufError;
use crate::error::Result;
use crate::execution::exchange_source::{ExchangeSource, GrpcExchangeSource, LocalExchangeSource};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::task::GlobalTaskEnv;
use crate::util::addr::get_host_port;
use protobuf::Message;
use risingwave_proto::plan::PlanNode_PlanNodeType;
use risingwave_proto::task_service::{ExchangeNode, ExchangeSource as ProtoExchangeSource};
use std::net::SocketAddr;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct ExchangeExecutor {
    sources: Vec<Box<dyn ExchangeSource>>,
    server_addr: SocketAddr,
    source_idx: usize,
}

fn is_local_address(server_addr: &SocketAddr, peer_addr: &SocketAddr) -> bool {
    let peer_ip = peer_addr.ip();
    if peer_ip.is_loopback() || peer_ip.is_unspecified() || (peer_addr.ip() == server_addr.ip()) {
        return peer_addr.port() == server_addr.port();
    }
    false
}

impl ExchangeExecutor {
    fn create_source(
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
                value.get_sink_id().clone(),
                env,
            )?));
        }
        debug!(
            "Exchange remotely from {} [{:?}]",
            &peer_addr,
            value.get_sink_id()
        );
        Ok(Box::new(GrpcExchangeSource::create(
            peer_addr,
            value.get_sink_id().clone(),
        )?))
    }
}

impl BoxedExecutorBuilder for ExchangeExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::EXCHANGE);
        let node = ExchangeNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let server_addr = *source.env.server_address();

        let mut sources: Vec<Box<dyn ExchangeSource>> = vec![];
        for proto_source in node.get_sources() {
            sources.push(ExchangeExecutor::create_source(
                source.env.clone(),
                proto_source,
            )?);
        }
        Ok(Box::new(Self {
            sources,
            server_addr,
            source_idx: 0,
        }))
    }
}

impl Executor for ExchangeExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        loop {
            if self.source_idx >= self.sources.len() {
                break;
            }
            let source = self.sources.get_mut(self.source_idx).unwrap();
            let res = async_std::task::block_on(source.take_data())?;
            match res {
                None => self.source_idx += 1,
                Some(res) => return Ok(ExecutorResult::Batch(res)),
            }
        }
        Ok(ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::column::Column;
    use crate::array::{DataChunk, I32Array};
    use crate::array_nonnull;
    use crate::types::Int32Type;
    use std::sync::Arc;

    #[test]
    fn test_is_local_address() {
        let check_local = |a: &str, b: &str| {
            assert!(is_local_address(
                &get_host_port(a).unwrap(),
                &get_host_port(b).unwrap()
            ));
        };
        check_local("127.0.0.1:3456", "0.0.0.0:3456");
        check_local("10.11.12.13:3456", "10.11.12.13:3456");
        check_local("10.11.12.13:3456", "0.0.0.0:3456");
        check_local("10.11.12.13:3456", "127.0.0.1:3456");
    }

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

    #[test]
    fn test_exchange_multiple_sources() {
        let mut sources: Vec<Box<dyn ExchangeSource>> = vec![];
        for _ in 0..3 {
            let chunk = DataChunk::builder()
                .columns(vec![Column::new(
                    Arc::new(array_nonnull! { I32Array, [3, 4, 4] }.into()),
                    Int32Type::create(false),
                )])
                .build();
            sources.push(Box::new(FakeExchangeSource { chunk: Some(chunk) }));
        }

        let mut executor = ExchangeExecutor {
            sources,
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_idx: 0,
        };

        let mut chunks: usize = 0;
        loop {
            let res = executor.execute().unwrap();
            match res {
                ExecutorResult::Batch(_) => chunks += 1,
                ExecutorResult::Done => break,
            }
        }
        assert_eq!(chunks, 3);
    }
}
