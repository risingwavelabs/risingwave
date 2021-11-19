use std::sync::Arc;

use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::SeqScanNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder};
use crate::storage::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::TableId;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{Result, RwError};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct SeqScanExecutor {
    first_execution: bool,
    table: Arc<BummockTable>,
    column_ids: Vec<i32>,
    column_indices: Vec<usize>,
    data: Vec<DataChunkRef>,
    chunk_idx: usize,
    schema: Schema,
}

impl BoxedExecutorBuilder for SeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::SeqScan);

        let seq_scan_node = SeqScanNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(|e| RwError::from(ProstError(e)))?;

        let table_id = TableId::from_protobuf(
            seq_scan_node
                .to_proto::<risingwave_proto::plan::SeqScanNode>()
                .get_table_ref_id(),
        )
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let table_ref = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?;

        if let TableTypes::BummockTable(table_ref) = table_ref {
            let column_ids = seq_scan_node.get_column_ids();

            let schema = Schema::new(
                seq_scan_node
                    .get_column_type()
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<Field>>>()?,
            );

            Ok(Box::new(Self {
                first_execution: true,
                table: table_ref,
                column_ids: column_ids.to_vec(),
                column_indices: vec![],
                chunk_idx: 0,
                data: Vec::new(),
                schema,
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
    async fn init(&mut self) -> Result<()> {
        info!("SeqScanExecutor initing!");
        Ok(())
    }

    async fn execute(&mut self) -> Result<Option<DataChunk>> {
        if self.first_execution {
            self.first_execution = false;
            if let BummockResult::Data(data) = self.table.get_data().await? {
                self.data = data;
            } else {
                return Ok(None);
            }

            self.column_indices = self
                .column_ids
                .iter()
                .map(|c| self.table.index_of_column_id(*c))
                .into_iter()
                .map(|res| res.unwrap())
                .collect();
        }

        if self.chunk_idx >= self.data.len() {
            return Ok(None);
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
        Ok(Some(ret))
    }

    async fn clean(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SeqScanExecutor {
    pub(crate) fn new(
        first_execution: bool,
        table: Arc<BummockTable>,
        column_ids: Vec<i32>,
        column_indices: Vec<usize>,
        data: Vec<DataChunkRef>,
        chunk_idx: usize,
        schema: Schema,
    ) -> Self {
        Self {
            first_execution,
            table,
            column_ids,
            column_indices,
            data,
            chunk_idx,
            schema,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::*;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataTypeKind, DecimalType, Int64Type};

    use super::*;

    #[tokio::test]
    async fn test_seq_scan_executor() -> Result<()> {
        let table_id = mock_table_id();
        let schema = Schema {
            fields: vec![Field {
                data_type: Arc::new(DecimalType::new(false, 10, 5)?),
            }],
        };
        let table = BummockTable::new(&table_id, &schema);

        let fields = table.schema().fields.to_owned();
        let col1 = column_nonnull! { I64Array, Int64Type, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, Int64Type, [2, 4, 6, 8, 10] };
        let data_chunk1 = DataChunk::builder().columns(vec![col1]).build();
        let data_chunk2 = DataChunk::builder().columns(vec![col2]).build();
        table.append(data_chunk1).await?;
        table.append(data_chunk2).await?;

        let mut seq_scan_executor = SeqScanExecutor {
            first_execution: true,
            table: Arc::new(table),
            column_ids: vec![0],
            column_indices: vec![],
            data: vec![],
            chunk_idx: 0,
            schema: Schema { fields },
        };
        seq_scan_executor.init().await.unwrap();

        let fields = &seq_scan_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Decimal);

        seq_scan_executor.init().await.unwrap();

        let result_chunk1 = seq_scan_executor.execute().await?.unwrap();
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

        let result_chunk2 = seq_scan_executor.execute().await?.unwrap();
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
        seq_scan_executor.execute().await.unwrap();
        seq_scan_executor.clean().await.unwrap();

        Ok(())
    }
}
