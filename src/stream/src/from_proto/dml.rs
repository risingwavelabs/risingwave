use itertools::Itertools;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::dml::DmlExecutor;
use crate::executor::BoxedExecutor;
use crate::task::{ExecutorParams, LocalStreamManagerCore};
pub struct DmlExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for DmlExecutorBuilder {
    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        _store: impl StateStore,
        _stream_manager: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Dml)?;
        let [upstream]: [_; 1] = params.input.try_into().unwrap();
        let table_id = TableId::new(node.table_id);
        let column_descs = node.column_descs.iter().map(Into::into).collect_vec();
        let fields = column_descs.iter().map(Into::into).collect_vec();
        let schema = Schema::new(fields);
        Ok(Box::new(DmlExecutor::new(
            upstream,
            schema,
            params.pk_indices,
            params.executor_id,
            params.env.dml_manager_ref(),
            table_id,
            column_descs,
        )))
    }
}
