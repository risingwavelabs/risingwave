use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::DropStreamNode;
use risingwave_pb::ToProto;

use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::source::SourceManagerRef;

pub(super) struct DropStreamExecutor {
    table_id: TableId,
    source_manager: SourceManagerRef,
    schema: Schema,
}

#[async_trait::async_trait]
impl Executor for DropStreamExecutor {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        self.source_manager
            .drop_source(&self.table_id)
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

impl BoxedExecutorBuilder for DropStreamExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::DropStream);

        let node = DropStreamNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            node.to_proto::<risingwave_proto::plan::DropStreamNode>()
                .get_table_ref_id(),
        )
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        Ok(Box::new(Self {
            table_id,
            source_manager: source.global_task_env().source_manager_ref(),
            schema: Schema { fields: vec![] },
        }))
    }
}
