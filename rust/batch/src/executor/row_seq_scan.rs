use std::sync::Arc;

use itertools::Itertools;
use prost::Message;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataTypeRef;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::RowSeqScanNode;
use risingwave_storage::table::TableIterRef;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};
/// Executor that scans data from row table
pub struct RowSeqScanExecutor {
    /// An iterator to scan MemoryStateStore.
    iter: TableIterRef,
    data_types: Vec<DataTypeRef>,
    column_ids: Vec<usize>,
    schema: Schema,
}

impl RowSeqScanExecutor {
    pub fn new(
        iter: TableIterRef,
        data_types: Vec<DataTypeRef>,
        column_ids: Vec<usize>,
        schema: Schema,
    ) -> Self {
        Self {
            iter,
            data_types,
            column_ids,
            schema,
        }
    }
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::RowSeqScan);

        let seq_scan_node = RowSeqScanNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(|e| RwError::from(ProstError(e)))?;

        let table_id = TableId::from(&seq_scan_node.table_ref_id);

        let table = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?;

        let schema = table.schema();
        let data_types = schema
            .fields
            .iter()
            .map(|f| f.data_type.clone())
            .collect_vec();
        let schema = schema.clone();

        Ok(Box::new(Self {
            data_types,
            column_ids: seq_scan_node
                .get_column_ids()
                .iter()
                .map(|i| *i as usize)
                .collect_vec(),
            iter: table.iter()?,
            schema,
        }))
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn open(&mut self) -> Result<()> {
        self.iter.open().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        match self.iter.next().await? {
            Some(value_row) => {
                // Scan through value pairs.
                // Make rust analyzer happy.
                let row = value_row as Row;
                let row = row.0;
                let columns = self
                    .column_ids
                    .iter()
                    .map(|column_id| {
                        if let (Some(data_type), Some(datum)) =
                            (self.data_types.get(*column_id), row.get(*column_id))
                        {
                            // We can scan row by row here currently.
                            let mut builder = data_type.create_array_builder(1)?;
                            builder.append_datum(datum)?;
                            let array = builder.finish()?;
                            Ok(Column::new(Arc::new(array), data_type.clone()))
                        } else {
                            Err(RwError::from(InternalError("No column found".to_string())))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let data_chunk = DataChunk::builder().columns(columns).build();
                Ok(Some(data_chunk))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
