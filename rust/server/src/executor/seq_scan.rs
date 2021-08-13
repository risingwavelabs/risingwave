use crate::array::ArrayRef;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::TableRef;
use crate::util::ProtobufConvert;
use protobuf::Message;
use risingwave_proto::plan::{PlanNode_PlanNodeType, SeqScanNode};
use std::convert::TryFrom;
use std::sync::Arc;

pub(super) struct SeqScanExecutor {
    table: TableRef,
    column_idxes: Vec<usize>,
    data: Vec<DataChunkRef>,
    chunk_idx: usize,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for SeqScanExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::SEQ_SCAN);

        let seq_scan_node =
            SeqScanNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let table_id = TableId::from_pb(seq_scan_node.get_table_ref_id().clone())
            .map_err(|e| InternalError(format!("failed to deserialize table id: {:?}", e)))?;

        let table_ref = source
            .task_context()
            .storage_manager()
            .get_table(&table_id)?;
        let column_idxes = seq_scan_node
            .get_column_ids()
            .iter()
            .map(|c| table_ref.index_of_column_id(*c))
            .collect::<Result<Vec<usize>>>()?;

        Ok(Self {
            table: table_ref,
            column_idxes,
            chunk_idx: 0,
            data: Vec::new(),
        })
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

        let arrays = self
            .column_idxes
            .iter()
            .map(|idx| cur_chunk.array_at(*idx))
            .collect::<Result<Vec<ArrayRef>>>()?;

        // TODO: visibility map here
        let ret = DataChunk::builder()
            .cardinality(cur_chunk.cardinality())
            .arrays(arrays)
            .build();

        self.chunk_idx += 1;
        Ok(ExecutorResult::Batch(Arc::new(ret)))
    }

    fn clean(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }
}
