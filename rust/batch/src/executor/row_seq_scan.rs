use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_storage::table::{ScannableTable, TableIterRef};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};
/// Executor that scans data from row table
pub struct RowSeqScanExecutor {
    table: Arc<dyn ScannableTable>,
    /// An iterator to scan StateStore.
    iter: Option<TableIterRef>,
    data_types: Vec<DataType>,
    column_ids: Vec<usize>,
    schema: Schema,
    identity: String,
}

impl RowSeqScanExecutor {
    pub fn new(
        table: Arc<dyn ScannableTable>,
        data_types: Vec<DataType>,
        column_ids: Vec<usize>,
        schema: Schema,
    ) -> Self {
        Self {
            table,
            iter: None,
            data_types,
            column_ids,
            schema,
            identity: "RowSeqScanExecutor".to_string(),
        }
    }
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_id = TableId::from(&seq_scan_node.table_ref_id);

        let table = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?;

        let schema = table.schema();
        let data_types = schema.fields.iter().map(|f| f.data_type).collect_vec();
        let schema = schema.into_owned();

        let column_ids = seq_scan_node
            .get_column_ids()
            .iter()
            .map(|i| *i as usize)
            .collect_vec();

        Ok(Box::new(Self::new(table, data_types, column_ids, schema)))
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn open(&mut self) -> Result<()> {
        self.iter = Some(self.table.iter().await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let iter = self.iter.as_mut().expect("executor not open");

        match iter.next().await? {
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
                            Ok(Column::new(Arc::new(array)))
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

    fn identity(&self) -> &str {
        &self.identity
    }
}
