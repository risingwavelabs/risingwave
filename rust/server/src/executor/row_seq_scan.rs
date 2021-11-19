use std::sync::Arc;

use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::RowSeqScanNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder};
use crate::storage::{MemRowTable, MemTableRowIter, TableTypes};
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::types::DataTypeRef;

use super::{BoxedExecutor, BoxedExecutorBuilder};

fn make_row_iter(table_ref: Arc<MemRowTable>) -> Result<MemTableRowIter> {
    table_ref.iter()
}

/// Executor that scans data from row table
pub(super) struct RowSeqScanExecutor {
    /// An iterator to scan MemRowTable.
    iter: MemTableRowIter,
    data_types: Vec<DataTypeRef>,
    column_ids: Vec<usize>,
    /// If empty, then the row will be iterated in a different manner.
    has_pk: bool,
    schema: Schema,
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::RowSeqScan);

        let seq_scan_node = RowSeqScanNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(|e| RwError::from(ProstError(e)))?;

        let table_id = TableId::from_protobuf(
            seq_scan_node
                .to_proto::<risingwave_proto::plan::RowSeqScanNode>()
                .get_table_ref_id(),
        )
        .to_rw_result_with("Failed to parse table id")?;

        let table_ref = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?;
        if let TableTypes::Row(table_ref) = table_ref {
            let schema = table_ref.schema();
            let data_types = schema
                .fields
                .iter()
                .map(|f| f.data_type.clone())
                .collect::<Vec<_>>();
            let pks = table_ref.get_pk();
            let schema = schema.clone();

            Ok(Box::new(Self {
                data_types,
                column_ids: seq_scan_node
                    .get_column_ids()
                    .iter()
                    .map(|i| *i as usize)
                    .collect::<Vec<_>>(),
                iter: make_row_iter(table_ref)?,
                has_pk: !pks.is_empty(),
                schema,
            }))
        } else {
            Err(RwError::from(InternalError(
                "RowSeqScan requires a row table".to_string(),
            )))
        }
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<Option<DataChunk>> {
        match self.iter.next() {
            Some((key_row, value_row)) => {
                if !self.has_pk {
                    // If no pk, then return keys with multiple times specified by value.
                    let value_arr = value_row.0;
                    let value_datum = value_arr.get(0).unwrap().clone();
                    let occurrences = value_datum.unwrap();
                    let occ_value = occurrences.as_int32();

                    let row = key_row as Row;
                    let row = row.0;

                    let columns = self
                        .column_ids
                        .iter()
                        .map(|column_id| {
                            if let (Some(data_type), Some(datum)) =
                                (self.data_types.get(*column_id), row.get(*column_id))
                            {
                                // We can scan row by row here currently.
                                let mut builder = data_type.clone().create_array_builder(1)?;
                                let mut i = 0;
                                while i < *occ_value {
                                    builder.append_datum(datum)?;
                                    i += 1;
                                }
                                let array = builder.finish()?;
                                Ok(Column::new(Arc::new(array), data_type.clone()))
                            } else {
                                Err(RwError::from(InternalError("No column found".to_string())))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let data_chunk = DataChunk::builder().columns(columns).build();
                    Ok(Some(data_chunk))
                } else {
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
                                let mut builder = data_type.clone().create_array_builder(1)?;
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
            }
            None => Ok(None),
        }
    }

    async fn clean(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
