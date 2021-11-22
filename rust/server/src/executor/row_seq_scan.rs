use std::sync::Arc;

use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::RowSeqScanNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder};
use crate::storage::{MemRowTable, MemTableRowIter, RowTable, TableTypes};
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
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
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
                                let mut builder = data_type.clone().create_array_builder(1)?;
                                let mut i = 0;
                                // Put duplicate tuples in the same chunk.
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
    use risingwave_common::types::Scalar;
    use risingwave_pb::data::{data_type::TypeName, DataType as DataTypeProst};
    use risingwave_pb::plan::{column_desc::ColumnEncodingType, ColumnDesc};
    use risingwave_pb::ToProst;

    use super::*;

    fn mock_first_row() -> Row {
        Row(vec![
            Some((1_i64).to_scalar_value()),
            Some((4_i64).to_scalar_value()),
        ])
    }

    fn mock_second_row() -> Row {
        Row(vec![
            Some((2_i64).to_scalar_value()),
            Some((5_i64).to_scalar_value()),
        ])
    }

    fn mock_first_key_row() -> Row {
        Row(vec![Some((1_i64).to_scalar_value())])
    }

    fn mock_second_key_row() -> Row {
        Row(vec![Some((2_i64).to_scalar_value())])
    }

    #[tokio::test]
    async fn test_row_seq_scan_one_row() -> Result<()> {
        // In this test we test if the memtable can be correctly scanned for one_row insertions.
        let column1 = ColumnDesc {
            column_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            is_primary: false,
            name: "test_col1".to_string(),
        };
        let column2 = ColumnDesc {
            column_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            is_primary: false,
            name: "test_col2".to_string(),
        };
        let columns = vec![column1, column2];

        let columns_revise = &columns
            .iter()
            .map(|c| c.to_proto::<risingwave_proto::plan::ColumnDesc>())
            .collect::<Vec<risingwave_proto::plan::ColumnDesc>>()[..];

        let fields = columns_revise
            .iter()
            .map(|c| {
                Field::try_from(
                    &c.get_column_type()
                        .to_prost::<risingwave_pb::data::DataType>(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let schema = Schema::new(fields);

        let data_types = schema
            .fields
            .iter()
            .map(|f| f.data_type.clone())
            .collect::<Vec<_>>();

        let mem_table = MemRowTable::new(schema.clone(), vec![]);
        let row1 = mock_first_row();
        let row2 = mock_first_row();
        let row3 = mock_second_row();

        mem_table.insert_one_row(row1).unwrap();
        mem_table.insert_one_row(row2).unwrap();
        mem_table.insert_one_row(row3).unwrap();

        let mut row_scan_executor = RowSeqScanExecutor {
            data_types,
            column_ids: vec![0, 1],
            iter: mem_table.iter()?,
            has_pk: false,
            schema,
        };

        row_scan_executor.open().await.unwrap();

        let res_chunk = row_scan_executor.next().await?.unwrap();
        assert_eq!(res_chunk.dimension(), 2);
        assert_eq!(
            res_chunk
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(1)]
        );
        assert_eq!(
            res_chunk
                .column_at(1)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(4), Some(4)]
        );

        let res_chunk2 = row_scan_executor.next().await?.unwrap();
        assert_eq!(res_chunk2.dimension(), 2);
        assert_eq!(
            res_chunk2
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            res_chunk2
                .column_at(1)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        row_scan_executor.close().await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_row_seq_scan() -> Result<()> {
        // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
        let column1 = ColumnDesc {
            column_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            is_primary: true,
            name: "test_col1".to_string(),
        };
        let column2 = ColumnDesc {
            column_type: Some(DataTypeProst {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            is_primary: false,
            name: "test_col2".to_string(),
        };
        let columns = vec![column1, column2];

        let columns_revise = &columns
            .iter()
            .map(|c| c.to_proto::<risingwave_proto::plan::ColumnDesc>())
            .collect::<Vec<risingwave_proto::plan::ColumnDesc>>()[..];

        let fields = columns_revise
            .iter()
            .map(|c| {
                Field::try_from(
                    &c.get_column_type()
                        .to_prost::<risingwave_pb::data::DataType>(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let schema = Schema::new(fields);

        let data_types = schema
            .fields
            .iter()
            .map(|f| f.data_type.clone())
            .collect::<Vec<_>>();

        let mem_table = MemRowTable::new(schema.clone(), vec![]);
        let row1 = mock_first_row();
        let key1 = mock_first_key_row();
        let row2 = mock_second_row();
        let key2 = mock_second_key_row();

        mem_table.insert(key1, row1).unwrap();
        mem_table.insert(key2, row2).unwrap();

        let mut row_scan_executor = RowSeqScanExecutor {
            data_types,
            column_ids: vec![0, 1],
            iter: mem_table.iter()?,
            has_pk: true,
            schema,
        };

        row_scan_executor.open().await.unwrap();

        let res_chunk = row_scan_executor.next().await?.unwrap();
        assert_eq!(res_chunk.dimension(), 2);
        assert_eq!(
            res_chunk
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1)]
        );
        assert_eq!(
            res_chunk
                .column_at(1)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(4)]
        );

        let res_chunk2 = row_scan_executor.next().await?.unwrap();
        assert_eq!(res_chunk2.dimension(), 2);
        assert_eq!(
            res_chunk2
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            res_chunk2
                .column_at(1)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        row_scan_executor.close().await.unwrap();
        Ok(())
    }
}
