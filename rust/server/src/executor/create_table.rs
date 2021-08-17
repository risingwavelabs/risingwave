use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::error::RwError;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::StorageManagerRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{CreateTableNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;

pub(super) struct CreateTableExecutor {
    table_id: TableId,
    column_count: usize,
    storage_manager: StorageManagerRef,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for CreateTableExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::CREATE_TABLE);

        let node = CreateTableNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let column_count = node.get_column_descs().len();

        Ok(Self {
            table_id,
            column_count,
            storage_manager: source.global_task_env().storage_manager_ref(),
        })
    }
}

impl Executor for CreateTableExecutor {
    fn init(&mut self) -> Result<()> {
        info!("create table executor initing!");
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        self.storage_manager
            .create_table(&self.table_id, self.column_count)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }
}
