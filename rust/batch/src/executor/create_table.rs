use prost::Message;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::Result;
use risingwave_common::types::build_from_prost;
use risingwave_common::util::downcast_arc;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::CreateTableNode;
use risingwave_source::SourceManagerRef;
use risingwave_storage::table::TableManagerRef;
use risingwave_storage::TableColumnDesc;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub(super) struct CreateTableExecutor {
    table_id: TableId,
    table_manager: TableManagerRef,
    source_manager: SourceManagerRef,
    table_columns: Vec<TableColumnDesc>,

    v2: bool,
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::CreateTable);

        let node = CreateTableNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from(&node.table_ref_id);

        let table_columns = node
            .get_column_descs()
            .iter()
            .map(|col| {
                Ok(TableColumnDesc {
                    data_type: build_from_prost(col.get_column_type())?,
                    column_id: col.get_column_id(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_task_env().table_manager_ref(),
            source_manager: source.global_task_env().source_manager_ref(),
            table_columns,
            v2: false,
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    async fn open(&mut self) -> Result<()> {
        info!("create table executor initing!");
        let table_columns = std::mem::take(&mut self.table_columns);

        match self.v2 {
            true => {
                let table = self
                    .table_manager
                    .create_table_v2(&self.table_id, table_columns)
                    .await?;
                self.source_manager
                    .create_table_source_v2(&self.table_id, table)?;
            }
            false => {
                let table = self
                    .table_manager
                    .create_table(&self.table_id, table_columns)
                    .await?;
                self.source_manager
                    .create_table_source(&self.table_id, downcast_arc(table.into_any())?)?;
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        info!("create table executor cleaned!");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        panic!("create table executor does not have schema!");
    }
}
