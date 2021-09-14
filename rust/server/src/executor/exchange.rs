use crate::error::ErrorCode::ProtobufError;
use crate::error::{Result, RwError};
use crate::execution::exchange_source::{ExchangeSource, GrpcExchangeSource, LocalExchangeSource};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::task::GlobalTaskEnv;
use crate::util::addr::get_host_port;
use protobuf::Message;
use risingwave_proto::plan::PlanNode_PlanNodeType;
use risingwave_proto::task_service::{ExchangeNode, ExchangeSource as ProtoExchangeSource};
use std::convert::TryFrom;
use std::net::SocketAddr;

pub(super) struct ExchangeExecutor {
    sources: Vec<Box<dyn ExchangeSource>>,
    server_addr: SocketAddr,
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

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for ExchangeExecutor {
    type Error = RwError;
    fn try_from(builder: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(builder.plan_node().get_node_type() == PlanNode_PlanNodeType::EXCHANGE);
        let node = ExchangeNode::parse_from_bytes(builder.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let server_addr = *builder.env.server_address();

        let mut sources: Vec<Box<dyn ExchangeSource>> = vec![];
        for proto_source in node.get_sources() {
            sources.push(ExchangeExecutor::create_source(
                builder.env.clone(),
                proto_source,
            )?);
        }
        Ok(Self {
            sources,
            server_addr,
        })
    }
}

impl Executor for ExchangeExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        ensure!(self.sources.len() == 1); // Parallel execution is unsupported yet.

        let source = self.sources.get_mut(0).unwrap();
        let res = async_std::task::block_on(async move { source.take_data().await })?;
        match res {
            None => Ok(ExecutorResult::Done),
            Some(res) => Ok(ExecutorResult::Batch(res)),
        }
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
