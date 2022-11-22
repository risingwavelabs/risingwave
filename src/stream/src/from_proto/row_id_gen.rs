use risingwave_pb::stream_plan::RowIdGenNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::row_id_gen::RowIdGenExecutor;
use crate::executor::BoxedExecutor;
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct RowIdGenExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for RowIdGenExecutorBuilder {
    type Node = RowIdGenNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();
        let vnodes = params
            .vnode_bitmap
            .expect("vnodes not set for row id gen executor");
        let executor = RowIdGenExecutor::new(
            upstream,
            params.schema,
            params.pk_indices,
            params.executor_id,
            node.row_id_index as _,
            vnodes,
        );
        Ok(Box::new(executor))
    }
}
