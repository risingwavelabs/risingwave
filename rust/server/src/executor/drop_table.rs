use prost::Message;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::DropTableNode;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};
use crate::stream::TableManagerRef;

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

        let table_id = TableId::from(&node.table_ref_id);

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_task_env().table_manager_ref(),
            schema: Schema { fields: vec![] },
        }))
    }
}

#[async_trait::async_trait]
impl Executor for DropTableExecutor {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        self.table_manager
            .drop_table(&self.table_id)
            .await
            .map(|_| None)
    }

    async fn close(&mut self) -> Result<()> {
        info!("drop table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
