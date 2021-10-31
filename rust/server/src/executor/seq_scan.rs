use std::sync::Arc;

use protobuf::Message;

use pb_convert::FromProtobuf;
use risingwave_proto::plan::{PlanNode_PlanNodeType, SeqScanNode};

use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::storage::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::TableId;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::{InternalError, ProtobufError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::build_from_proto;

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct SeqScanExecutor {
    table: Arc<SimpleMemTable>,
    column_indices: Vec<usize>,
    data: Vec<DataChunkRef>,
    chunk_idx: usize,
    schema: Schema,
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
            .table_manager()
            .get_table(&table_id)?;
        if let SimpleTableRef::Columnar(table_ref) = table_ref {
            let column_indices = seq_scan_node
                .get_column_ids()
                .iter()
                .map(|c| table_ref.index_of_column_id(*c))
                .collect::<Result<Vec<usize>>>()?;

            let fields = table_ref
                .columns()
                .iter()
                .map(|c| Field {
                    data_type: build_from_proto(c.get_column_type()).unwrap(),
                })
                .collect::<Vec<Field>>();

            Ok(Box::new(Self {
                table: table_ref,
                column_indices,
                chunk_idx: 0,
                data: Vec::new(),
                schema: Schema { fields },
            }))
        } else {
            Err(RwError::from(InternalError(
                "SeqScan requires a columnar table".to_string(),
            )))
        }
    }
}

#[async_trait::async_trait]
impl Executor for SeqScanExecutor {
    fn init(&mut self) -> Result<()> {
        self.data = self.table.get_data()?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
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

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SeqScanExecutor {
    pub(crate) fn new(
        table: Arc<SimpleMemTable>,
        column_indices: Vec<usize>,
        data: Vec<DataChunkRef>,
        chunk_idx: usize,
        schema: Schema,
    ) -> Self {
        Self {
            table,
            column_indices,
            data,
            chunk_idx,
            schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use pb_construct::make_proto;
    use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
    use risingwave_proto::plan::{ColumnDesc, ColumnDesc_ColumnEncodingType};

    use crate::*;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::types::{DataTypeKind, Int64Type};

    use super::*;

    #[tokio::test]
    async fn test_seq_scan_executor() -> Result<()> {
        let table_id = mock_table_id();
        let column = make_proto!(ColumnDesc, {
          column_type: make_proto!(DataTypeProto, {
            type_name: DataType_TypeName::INT64
          }),
          encoding: ColumnDesc_ColumnEncodingType::RAW,
          is_primary: false,
          name: "test_col".to_string()
        });
        let columns = vec![column];
        let table = SimpleMemTable::new(&table_id, &columns);

        let fields = table
            .columns()
            .iter()
            .map(|c| Field {
                data_type: build_from_proto(c.get_column_type()).unwrap(),
            })
            .collect::<Vec<Field>>();
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
            schema: Schema { fields },
        };
        assert!(seq_scan_executor.init().is_ok());

        let fields = &seq_scan_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int64);

        let result_chunk1 = seq_scan_executor.execute().await?.batch_or()?;
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

        let result_chunk2 = seq_scan_executor.execute().await?.batch_or()?;
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
        assert!(seq_scan_executor.execute().await.is_ok());
        assert!(seq_scan_executor.clean().is_ok());

        Ok(())
    }
}
