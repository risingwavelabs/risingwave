use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

pub(super) struct DropStreamExecutor {
    table_id: TableId,
    source_manager: SourceManagerRef,
    schema: Schema,
    identity: String,
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

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for DropStreamExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(source.plan_node().get_node_body(), NodeBody::DropSource)?;

        let table_id = TableId::from(&node.table_ref_id);

        Ok(Box::new(Self {
            table_id,
            source_manager: source.global_task_env().source_manager_ref(),
            schema: Schema { fields: vec![] },
            identity: format!("DropStreamExecutor{:?}", source.task_id),
        }))
    }
}
