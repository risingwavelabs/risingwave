use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::DropStreamNode;
use risingwave_pb::ToProto;

use crate::executor::{
    BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult,
};
use crate::source::SourceManagerRef;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::Result;

pub(super) struct DropStreamExecutor {
    table_id: TableId,
    source_manager: SourceManagerRef,
    schema: Schema,
}

#[async_trait::async_trait]
impl Executor for DropStreamExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        self.source_manager
            .drop_source(&self.table_id)
            .map(|_| ExecutorResult::Done)
    }

    fn clean(&mut self) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_pb::plan::{plan_node::PlanNodeType, PlanNode};

    use crate::executor::drop_stream::DropStreamExecutor;
    use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};
    use crate::source::{
        FileSourceConfig, MemSourceManager, SourceConfig, SourceFormat, SourceManager,
    };
    use crate::task::{GlobalTaskEnv, TaskId};
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::catalog::Schema;

    #[test]
    fn test_drop_stream() {
        let mut plan_node = PlanNode {
            ..Default::default()
        };
        plan_node.set_node_type(PlanNodeType::Filter);
        let task_id = &TaskId {
            task_id: 0,
            stage_id: 0,
            query_id: "".to_string(),
        };
        let builder = ExecutorBuilder::new(&plan_node, task_id, GlobalTaskEnv::for_test());
        assert!(DropStreamExecutor::new_boxed_executor(&builder).is_err())
    }

    #[tokio::test]
    async fn test_drop_stream_execute() {
        let source_manager = Arc::new(MemSourceManager::new());
        let table_id = mock_table_id();
        let mut executor = DropStreamExecutor {
            table_id: table_id.clone(),
            source_manager: source_manager.clone(),
            schema: Schema { fields: vec![] },
        };
        let create_result = source_manager.create_source(
            &table_id,
            SourceFormat::Json,
            &SourceConfig::File(FileSourceConfig {
                filename: "/tmp/data.txt".to_string(),
            }),
            vec![],
        );
        assert!(create_result.is_ok());

        assert!(source_manager.get_source(&table_id).is_ok());
        assert!(executor.init().is_ok());
        assert!(executor.execute().await.is_ok());
        assert!(source_manager.get_source(&table_id).is_err());
        assert!(executor.clean().is_ok())
    }
}
