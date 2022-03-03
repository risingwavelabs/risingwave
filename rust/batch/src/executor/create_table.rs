use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::plan::create_table_node::Info;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::ColumnDesc;
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

    /// Other info for creating table.
    info: Info,
}

impl CreateTableExecutor {
    pub fn new(
        table_id: TableId,
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        table_columns: Vec<ColumnDesc>,
        identity: String,
        info: Info,
    ) -> Self {
        Self {
            table_id,
            table_manager,
            source_manager,
            table_columns,
            identity,
            info,
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

        Ok(Box::new(Self {
            table_id,
            table_manager: source.global_batch_env().table_manager_ref(),
            source_manager: source.global_batch_env().source_manager_ref(),
            table_columns: node.column_descs.clone(),
            identity: "CreateTableExecutor".to_string(),
            info: node.info.clone().unwrap(),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    async fn open(&mut self) -> Result<()> {
        let table_columns = self
            .table_columns
            .to_owned()
            .iter()
            .map(|col| {
                Ok(TableColumnDesc {
                    data_type: DataType::from(col.get_column_type()?),
                    column_id: ColumnId::from(col.get_column_id()),
                    name: col.get_name().to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        match &self.info {
            Info::TableSource(_) => {
                // Create table_v2.
                info!("Create table id:{}", &self.table_id.table_id());
                // let _table = self
                //     .table_manager
                //     .create_table_v2(&self.table_id, table_columns.clone())
                //     .await?;
                self.source_manager
                    .create_table_source_v2(&self.table_id, table_columns)?;
            }
            Info::MaterializedView(info) => {
                if info.associated_table_ref_id.is_some() {
                    // Create associated materialized view for table_v2.
                    let associated_table_id = TableId::from(&info.associated_table_ref_id);
                    info!(
                        "create associated materialized view: id={:?}, associated={:?}",
                        self.table_id, associated_table_id
                    );

                    // self.table_manager.register_associated_materialized_view(
                    //     &associated_table_id,
                    //     &self.table_id,
                    // )?;
                    self.source_manager.register_associated_materialized_view(
                        &associated_table_id,
                        &self.table_id,
                    )?;
                } else {
                    // Create normal MV.
                    info!("create materialized view: id={:?}", self.table_id);

                    let order_pairs: Vec<_> = info
                        .column_orders
                        .iter()
                        .map(OrderPair::from_prost)
                        .collect();

                    let orderings = order_pairs
                        .iter()
                        .map(|order| order.order_type)
                        .collect::<Vec<_>>();

                    let pk_indices = info.pk_indices.iter().map(|key| *key as usize).collect();

                    self.table_manager.create_materialized_view(
                        &self.table_id,
                        &self.table_columns,
                        pk_indices,
                        orderings,
                    )?;
                }
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

    fn identity(&self) -> &str {
        &self.identity
    }
}
