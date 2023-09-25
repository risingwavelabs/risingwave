use risingwave_pb::stream_plan::StreamFsFetchNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::BoxedExecutor;
use crate::from_proto::ExecutorBuilder;
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct FsFetchExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for FsFetchExecutorBuilder {
    type Node = StreamFsFetchNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        todo!()
    }
}
