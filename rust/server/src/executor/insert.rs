use std::sync::Arc;

use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::InsertNode;
use risingwave_pb::ToProto;

use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::storage::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, DataChunk, I64ArrayBuilder, PrimitiveArrayBuilder,
};
use risingwave_common::catalog::TableId;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{Int32Type, Int64Type};

use super::BoxedExecutor;

/// `InsertExecutor` implements table insertion with values from its child executor.
pub(super) struct InsertExecutor {
    /// target table id
    table_id: TableId,
    table_manager: TableManagerRef,

    child: BoxedExecutor,
    executed: bool,
    schema: Schema,
}

impl InsertExecutor {
    /// Generate next row-id
    fn next_row_id(&self) -> Result<usize> {
        let table_ref = (if let TableTypes::BummockTable(table_ref) =
            self.table_manager.get_table(&self.table_id)?
        {
            Ok(table_ref)
        } else {
            Err(RwError::from(InternalError(
                "Only columnar table support insert".to_string(),
            )))
        })?;

        // TODO: include node_id in the row id to ensure unique in cluster
        Ok(table_ref.next_row_id())
    }
}

#[async_trait::async_trait]
impl Executor for InsertExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        info!("Insert executor");
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }

        let table_ref = (if let TableTypes::BummockTable(table_ref) =
            self.table_manager.get_table(&self.table_id)?
        {
            Ok(table_ref)
        } else {
            Err(RwError::from(InternalError(
                "Only columnar table support insert".to_string(),
            )))
        })?;
        let mut rows_inserted = 0;
        while let Some(child_chunk) = self.child.next().await? {
            let len = child_chunk.capacity();
            assert!(child_chunk.visibility().is_none());

            let mut columns = Vec::with_capacity(child_chunk.columns().len() + 1);

            // add row-id column as first column
            let mut builder = I64ArrayBuilder::new(len).unwrap();
            for _ in 0..len {
                builder.append(Some(self.next_row_id()? as i64)).unwrap();
            }
            let rowid_column = Column::new(
                Arc::new(ArrayImpl::from(builder.finish().unwrap())),
                Int64Type::create(false),
            );
            columns.insert(0, rowid_column);

            for col in child_chunk.columns().iter() {
                columns.push(col.clone());
            }

            let chunk = DataChunk::new(columns, None);
            rows_inserted += table_ref.append(chunk).await?;
        }

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i32>::new(1)?;
            array_builder.append(Some(rows_inserted as i32))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder()
                .columns(vec![Column::new(
                    Arc::new(array.into()),
                    Int32Type::create(false),
                )])
                .build();

            self.executed = true;
            Ok(Some(ret_chunk))
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await?;
        info!("Cleaning insert executor.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl BoxedExecutorBuilder for InsertExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::Insert);
        let insert_node =
            InsertNode::decode(&(source.plan_node()).get_body().value[..]).map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            insert_node
                .to_proto::<risingwave_proto::plan::InsertNode>()
                .get_table_ref_id(),
        )
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let table_manager = source.global_task_env().table_manager_ref();

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build()?;

        Ok(Box::new(Self {
            table_id,
            table_manager,
            child,
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int32Type::create(false),
                }],
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::test_utils::MockExecutor;
    use crate::storage::{SimpleTableManager, TableManager};
    use crate::*;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::{Field, Schema, SchemaId};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::{DataTypeKind, DecimalType, Int64Type};

    use super::*;

    #[tokio::test]
    async fn test_insert_executor() -> Result<()> {
        let table_manager = Arc::new(SimpleTableManager::new());
        // Schema for mock executor.
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of first table
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
            ],
        };

        let table_columns: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| TableColumnDesc {
                data_type: f.data_type.clone(),
                column_id: i as i32, // use column index as column id
            })
            .collect();

        let col1 = column_nonnull! { I64Array, Int64Type, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, Int64Type, [2, 4, 6, 8, 10] };
        let data_chunk: DataChunk = DataChunk::builder().columns(vec![col1, col2]).build();
        mock_executor.add(data_chunk.clone());

        // Create the first table.
        let table_id = TableId::new(SchemaId::default(), 0);
        table_manager
            .create_table(&table_id, table_columns.to_vec())
            .await?;

        let mut insert_executor = InsertExecutor {
            table_id: table_id.clone(),
            table_manager: table_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int32Type::create(false),
                }],
            },
        };
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = insert_executor
            .table_manager
            .get_table(&insert_executor.table_id)?;
        if let TableTypes::BummockTable(table_ref) = table_ref {
            match table_ref.get_data().await? {
                BummockResult::Data(data_ref) => {
                    assert_eq!(
                        data_ref[0]
                            .column_at(0)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(0), Some(1), Some(2), Some(3), Some(4)]
                    );

                    assert_eq!(
                        data_ref[0]
                            .column_at(1)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
                    );
                    assert_eq!(
                        data_ref[0]
                            .column_at(2)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
                    );
                }
                BummockResult::DataEof => {
                    panic!("Empty data returned.")
                }
            }
        } else {
            panic!("invalid table type found.")
        }
        // First insertion test ends.

        // Insert to the same table by a new executor. The result should have growing row ids from
        // the last one seen.
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(data_chunk.clone());
        let mut insert_executor = InsertExecutor {
            table_id: table_id.clone(),
            table_manager: table_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int32Type::create(false),
                }],
            },
        };
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = insert_executor
            .table_manager
            .get_table(&insert_executor.table_id)?;
        if let TableTypes::BummockTable(table_ref) = table_ref {
            match table_ref.get_data().await? {
                BummockResult::Data(data_ref) => {
                    assert_eq!(
                        data_ref[1]
                            .column_at(0)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(5), Some(6), Some(7), Some(8), Some(9)]
                    );
                }
                BummockResult::DataEof => {
                    panic!("Empty data returned.")
                }
            }
        } else {
            panic!("invalid table type found.")
        }
        // Second insertion test ends.

        // Insert into another table. It should have new row ids starting from 0.
        let table_id2 = TableId::new(SchemaId::default(), 1);
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(data_chunk);
        let mut insert_executor = InsertExecutor {
            table_id: table_id2.clone(),
            table_manager: table_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int32Type::create(false),
                }],
            },
        };
        table_manager
            .create_table(&table_id2, table_columns)
            .await?;
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = insert_executor
            .table_manager
            .get_table(&insert_executor.table_id)?;
        if let TableTypes::BummockTable(table_ref) = table_ref {
            match table_ref.get_data().await? {
                BummockResult::Data(data_ref) => {
                    assert_eq!(
                        data_ref[0]
                            .column_at(0)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(0), Some(1), Some(2), Some(3), Some(4)]
                    );

                    assert_eq!(
                        data_ref[0]
                            .column_at(1)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
                    );
                    assert_eq!(
                        data_ref[0]
                            .column_at(2)?
                            .array()
                            .as_int64()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
                    );
                }
                BummockResult::DataEof => {
                    panic!("Empty data returned.")
                }
            }
        } else {
            panic!("invalid table type found.")
        }
        // Third insertion test ends.

        Ok(())
    }
}
