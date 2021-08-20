use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::error::RwError;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::StorageManagerRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{DropTableNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;

pub(super) struct DropTableExecutor {
    table_id: TableId,
    storage_manager: StorageManagerRef,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for DropTableExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::DROP_TABLE);

        let node = DropTableNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        Ok(Self {
            table_id,
            storage_manager: source.global_task_env().storage_manager_ref(),
        })
    }
}

impl Executor for DropTableExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        self.storage_manager
            .drop_table(&self.table_id)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("drop table executor cleaned!");
        Ok(())
    }
}
