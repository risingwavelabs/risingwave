use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::TableManagerRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{CreateTableNode, PlanNode_PlanNodeType};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct CreateTableExecutor {
    table_id: TableId,
    column_count: usize,
    table_manager: TableManagerRef,
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::CREATE_TABLE);

        let node = CreateTableNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let column_count = node.get_column_descs().len();

        Ok(Box::new(Self {
            table_id,
            column_count,
            table_manager: source.global_task_env().table_manager_ref(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    fn init(&mut self) -> Result<()> {
        info!("create table executor initing!");
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        self.table_manager
            .create_table(&self.table_id, self.column_count)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }
}
