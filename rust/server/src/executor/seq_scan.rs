use crate::array::column::Column;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::{MemColumnarTable, TableRef};
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{PlanNode_PlanNodeType, SeqScanNode};
use std::sync::Arc;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct SeqScanExecutor {
    table: Arc<MemColumnarTable>,
    column_indices: Vec<usize>,
    data: Vec<DataChunkRef>,
    chunk_idx: usize,
}

impl BoxedExecutorBuilder for SeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::SEQ_SCAN);

        let seq_scan_node =
            SeqScanNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let table_id = TableId::from_protobuf(seq_scan_node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let table_ref = source
            .global_task_env()
            .storage_manager()
            .get_table(&table_id)?;
        if let TableRef::Columnar(table_ref) = table_ref {
            let column_indices = seq_scan_node
                .get_column_ids()
                .iter()
                .map(|c| table_ref.index_of_column_id(*c))
                .collect::<Result<Vec<usize>>>()?;

            Ok(Box::new(Self {
                table: table_ref,
                column_indices,
                chunk_idx: 0,
                data: Vec::new(),
            }))
        } else {
            Err(RwError::from(InternalError(
                "SeqScan requires a columnar table".to_string(),
            )))
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) -> Result<()> {
        self.data = self.table.get_data()?;
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.chunk_idx >= self.data.len() {
            return Ok(Done);
        }

        let cur_chunk = &self.data[self.chunk_idx];

        let columns = self
            .column_indices
            .iter()
            .map(|idx| cur_chunk.column_at(*idx))
            .collect::<Result<Vec<Column>>>()?;

        // TODO: visibility map here
        let ret = DataChunk::builder().columns(columns).build();

        self.chunk_idx += 1;
        Ok(ExecutorResult::Batch(ret))
    }

    fn clean(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }
}

impl SeqScanExecutor {
    pub(crate) fn new(
        table: Arc<MemColumnarTable>,
        column_indices: Vec<usize>,
        data: Vec<DataChunkRef>,
        chunk_idx: usize,
    ) -> Self {
        Self {
            table,
            column_indices,
            data,
            chunk_idx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, I64Array};
    use crate::catalog::test_utils::mock_table_id;
    use crate::types::Int64Type;
    use crate::*;

    #[test]
    fn test_seq_scan_executor() -> Result<()> {
        let table_id = mock_table_id();
        let table = MemColumnarTable::new(&table_id, 5);

        let col1 = column_nonnull! { I64Array, Int64Type, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, Int64Type, [2, 4, 6, 8, 10] };
        let data_chunk1 = DataChunk::builder().columns(vec![col1]).build();
        let data_chunk2 = DataChunk::builder().columns(vec![col2]).build();
        table.append(data_chunk1)?;
        table.append(data_chunk2)?;

        let mut seq_scan_executor = SeqScanExecutor {
            table: Arc::new(table),
            column_indices: vec![0],
            data: vec![],
            chunk_idx: 0,
        };
        assert!(seq_scan_executor.init().is_ok());

        let result_chunk1 = seq_scan_executor.execute()?.batch_or()?;
        assert_eq!(result_chunk1.dimension(), 1);
        assert_eq!(
            result_chunk1
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
        );

        let result_chunk2 = seq_scan_executor.execute()?.batch_or()?;
        assert_eq!(result_chunk2.dimension(), 1);
        assert_eq!(
            result_chunk2
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
        );
        assert!(seq_scan_executor.execute().is_ok());
        assert!(seq_scan_executor.clean().is_ok());

        Ok(())
    }
}
