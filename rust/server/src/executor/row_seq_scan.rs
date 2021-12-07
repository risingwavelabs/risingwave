use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use prost::Message;

use crate::executor::{Executor, ExecutorBuilder};
use crate::stream_op::{MViewTable, MemoryStateStore};

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::RowSeqScanNode;
use risingwave_pb::ToProto;

use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::types::DataTypeRef;

use super::{BoxedExecutor, BoxedExecutorBuilder};

type MemoryStateStoreIter = <BTreeMap<Row, Row> as IntoIterator>::IntoIter;

/// Executor that scans data from row table
pub(super) struct RowSeqScanExecutor {
    /// An iterator to scan MemoryStateStore.
    iter: Option<MemoryStateStoreIter>,
    // TODO(MrCroxx): Remove me after hummock table iter is impled.
    table: Arc<MViewTable<MemoryStateStore>>,
    data_types: Vec<DataTypeRef>,
    column_ids: Vec<usize>,
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

        let table = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?
            .as_memory();

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
            iter: None,
            table,
            schema,
        }))
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn open(&mut self) -> Result<()> {
        self.iter = Some(self.table.iter().await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        match self.iter.as_mut().unwrap().next() {
            Some((_key_row, value_row)) => {
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

#[cfg(test)]
mod tests {
    use risingwave_common::array::Array;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{Int32Type, Scalar};

    use crate::stream_op::{MViewTable, ManagedMViewState, MemoryStateStore};

    use super::*;

    #[tokio::test]
    async fn test_row_seq_scan() -> Result<()> {
        // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
        let state_store = MemoryStateStore::new();
        let prefix = b"mview-test-42".to_vec();
        let schema = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let pk_columns = vec![0];
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            state_store.clone(),
        );

        let table = Arc::new(MViewTable::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            state_store.clone(),
        ));

        let mut executor = RowSeqScanExecutor {
            iter: None,
            table,
            data_types: schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect(),
            column_ids: vec![0, 1],
            schema,
        };

        state.put(
            Row(vec![Some(1_i32.to_scalar_value())]),
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(4_i32.to_scalar_value()),
            ]),
        );
        state.put(
            Row(vec![Some(2_i32.to_scalar_value())]),
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(5_i32.to_scalar_value()),
            ]),
        );
        state.flush().await.unwrap();

        executor.open().await.unwrap();

        let res_chunk = executor.next().await?.unwrap();
        assert_eq!(res_chunk.dimension(), 2);
        assert_eq!(
            res_chunk
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1)]
        );
        assert_eq!(
            res_chunk
                .column_at(1)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(4)]
        );

        let res_chunk2 = executor.next().await?.unwrap();
        assert_eq!(res_chunk2.dimension(), 2);
        assert_eq!(
            res_chunk2
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            res_chunk2
                .column_at(1)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        executor.close().await.unwrap();
        Ok(())
    }
}
