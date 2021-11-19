use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_common::array::DataChunk;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::{ColumnDesc, CreateTableNode};
use risingwave_pb::ToProto;
use risingwave_proto::plan::ColumnDesc as ProtoColumnDesc;

use crate::executor::{Executor, ExecutorBuilder};
use crate::storage::TableManagerRef;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::Result;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct CreateTableExecutor {
    table_id: TableId,
    columns: Vec<ColumnDesc>,
    table_manager: TableManagerRef,
    schema: Schema,
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::CreateTable);

        let node = CreateTableNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            node.to_proto::<risingwave_proto::plan::CreateTableNode>()
                .get_table_ref_id(),
        )
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
    async fn init(&mut self) -> Result<()> {
        info!("create table executor initing!");
        Ok(())
    }

    async fn execute(&mut self) -> Result<Option<DataChunk>> {
        self.table_manager
            .create_table(
                &self.table_id,
                &self
                    .columns
                    .iter()
                    .map(|c| c.to_proto::<ProtoColumnDesc>())
                    .collect::<Vec<ProtoColumnDesc>>()[..],
            )
            .await
            .map(|_| None)
    }

    async fn clean(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
