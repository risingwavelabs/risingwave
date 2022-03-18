use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

// TODO: All DDLs should be RPC requests from the meta service. Remove this.
pub(super) struct DropTableExecutor {
    table_id: TableId,
    schema: Schema,
    identity: String,
}

impl BoxedExecutorBuilder for DropTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::DropTable
        )?;

        let table_id = TableId::from(&node.table_ref_id);

        Ok(Box::new(Self {
            table_id,
            schema: Schema { fields: vec![] },
            identity: "DropTableExecutor".to_string(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for DropTableExecutor {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        // TODO: ddl may not need to be executed on compute node
        Ok(None)
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
