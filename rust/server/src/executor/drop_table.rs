use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::DropTableNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::TableManagerRef;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::Result;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct DropTableExecutor {
    table_id: TableId,
    table_manager: TableManagerRef,
    schema: Schema,
}

impl BoxedExecutorBuilder for DropTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::DropTable);

        let node = DropTableNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            node.to_proto::<risingwave_proto::plan::DropTableNode>()
                .get_table_ref_id(),
        )
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_task_env().table_manager_ref(),
            schema: Schema { fields: vec![] },
        }))
    }
}

#[async_trait::async_trait]
impl Executor for DropTableExecutor {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        self.table_manager
            .drop_table(&self.table_id)
            .map(|_| ExecutorResult::Done)
    }

    async fn clean(&mut self) -> Result<()> {
        info!("drop table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
