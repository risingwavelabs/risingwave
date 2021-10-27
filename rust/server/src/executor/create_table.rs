use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult, Schema};
use crate::storage::TableManagerRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{ColumnDesc, CreateTableNode, PlanNode_PlanNodeType};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct CreateTableExecutor {
    table_id: TableId,
    columns: Vec<ColumnDesc>,
    table_manager: TableManagerRef,
    schema: Schema,
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::CREATE_TABLE);

        let node = CreateTableNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let columns = node.get_column_descs().to_vec();

        Ok(Box::new(Self {
            table_id,
            columns,
            table_manager: source.global_task_env().table_manager_ref(),
            schema: Schema { fields: vec![] },
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
            .create_table(&self.table_id, &self.columns)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
