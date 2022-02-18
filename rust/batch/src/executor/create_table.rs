use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::fetch_orders;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{ColumnDesc, ColumnOrder};
use risingwave_source::SourceManagerRef;
use risingwave_storage::table::TableManagerRef;
use risingwave_storage::TableColumnDesc;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub struct CreateTableExecutor {
    table_id: TableId,
    table_manager: TableManagerRef,
    source_manager: SourceManagerRef,
    table_columns: Vec<ColumnDesc>,
    identity: String,
    /// Below for materialized views.
    is_materialized_view: bool,
    associated_table_id: Option<TableId>,
    pk_indices: Vec<usize>,
    column_orders: Vec<ColumnOrder>,
}

impl CreateTableExecutor {
    pub fn new(
        table_id: TableId,
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        table_columns: Vec<ColumnDesc>,
        identity: String,
    ) -> Self {
        Self {
            table_id,
            table_manager,
            source_manager,
            table_columns,
            identity,
            is_materialized_view: false,
            associated_table_id: None,
            pk_indices: vec![],
            column_orders: vec![],
        }
    }
}

impl BoxedExecutorBuilder for CreateTableExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::CreateTable
        )?;

        let table_id = TableId::from(&node.table_ref_id);

        let associated_table_id = node
            .associated_table_ref_id
            .as_ref()
            .map(|_| TableId::from(&node.associated_table_ref_id));

        let pks = node
            .pk_indices
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_task_env().table_manager_ref(),
            source_manager: source.global_task_env().source_manager_ref(),
            table_columns: node.column_descs.clone(),
            identity: "CreateTableExecutor".to_string(),
            is_materialized_view: node.is_materialized_view,
            associated_table_id,
            pk_indices: pks,
            column_orders: node.column_orders.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    async fn open(&mut self) -> Result<()> {
        tracing::info!(
            "create table: table_id={:?}, associated_table_id={:?}, is_materialized_view={}",
            self.table_id,
            self.associated_table_id,
            self.is_materialized_view
        );

        let table_columns = self
            .table_columns
            .to_owned()
            .iter()
            .map(|col| {
                Ok(TableColumnDesc {
                    data_type: DataType::from(col.get_column_type()?),
                    column_id: col.get_column_id(),
                    name: col.get_name().to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if self.is_materialized_view {
            if self.associated_table_id.is_some() {
                // Create associated materialized view for table_v2.
                self.table_manager.register_associated_materialized_view(
                    self.associated_table_id.as_ref().unwrap(),
                    &self.table_id,
                )?;
                self.source_manager.register_associated_materialized_view(
                    self.associated_table_id.as_ref().unwrap(),
                    &self.table_id,
                )?;
            } else {
                // Create normal MV.
                let order_pairs = fetch_orders(&self.column_orders).unwrap();
                let orderings = order_pairs
                    .iter()
                    .map(|order| order.order_type)
                    .collect::<Vec<_>>();

                self.table_manager.create_materialized_view(
                    &self.table_id,
                    &self.table_columns,
                    self.pk_indices.clone(),
                    orderings,
                )?;
            }
        } else {
            // Create table_v2.
            info!("Create table id:{}", &self.table_id.table_id());

            let table = self
                .table_manager
                .create_table_v2(&self.table_id, table_columns)
                .await?;
            self.source_manager
                .create_table_source_v2(&self.table_id, table)?;
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

    fn identity(&self) -> &str {
        &self.identity
    }
}
