use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::TableManagerRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{DropTableNode, PlanNode_PlanNodeType};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct DropTableExecutor {
    table_id: TableId,
    table_manager: TableManagerRef,
}

impl BoxedExecutorBuilder for DropTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::DROP_TABLE);

        let node = DropTableNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_task_env().table_manager_ref(),
        }))
    }
}

impl Executor for DropTableExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        self.table_manager
            .drop_table(&self.table_id)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("drop table executor cleaned!");
        Ok(())
    }
}
