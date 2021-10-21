use crate::array::column::Column;
use crate::array::DataChunk;
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};

use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::{MemRowTable, Row, TableRef};
use crate::types::build_from_proto;
use crate::types::DataTypeRef;
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{PlanNode_PlanNodeType, RowSeqScanNode};
use std::sync::Arc;

use super::{BoxedExecutor, BoxedExecutorBuilder};

type RowIter = impl Iterator<Item = (Row, Row)>;

fn make_row_iter(table_ref: Arc<MemRowTable>) -> Result<RowIter> {
    table_ref.iter()
}

/// Executor that scans data from row table
pub(super) struct RowSeqScanExecutor {
    /// An iterator to scan MemRowTable.
    iter: RowIter,
    data_types: Vec<DataTypeRef>,
    column_ids: Vec<usize>,
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::ROW_SEQ_SCAN);

        let seq_scan_node =
            RowSeqScanNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let table_id = TableId::from_protobuf(seq_scan_node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let table_ref = source
            .global_task_env()
            .storage_manager()
            .get_table(&table_id)?;
        if let TableRef::Row(table_ref) = table_ref {
            let data_types = table_ref
                .schema()
                .into_iter()
                .map(|f| build_from_proto(f.get_column_type()))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Self {
                data_types,
                column_ids: seq_scan_node
                    .get_column_ids()
                    .iter()
                    .map(|i| *i as usize)
                    .collect::<Vec<_>>(),
                iter: make_row_iter(table_ref)?,
            }))
        } else {
            Err(RwError::from(InternalError(
                "RowSeqScan requires a row table".to_string(),
            )))
        }
    }
}

impl Executor for RowSeqScanExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        match self.iter.next() {
            Some((_, row)) => {
                // Make rust analyzer happy.
                let row = row as Row;
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
                            builder.append_datum(datum.clone())?;
                            let array = builder.finish()?;
                            Ok(Column::new(Arc::new(array), data_type.clone()))
                        } else {
                            Err(RwError::from(InternalError("No column found".to_string())))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let data_chunk = DataChunk::builder().columns(columns).build();
                Ok(ExecutorResult::Batch(data_chunk))
            }
            None => Ok(ExecutorResult::Done),
        }
    }

    fn clean(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }
}
