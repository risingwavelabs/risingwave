use std::sync::Arc;

use prost::Message;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, DataChunk, I64ArrayBuilder, Op, PrimitiveArrayBuilder, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::Int64Type;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::InsertNode;
use risingwave_source::{Source, SourceImpl, SourceManagerRef, SourceWriter};

use super::BoxedExecutor;
use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// `InsertExecutor` implements table insertion with values from its child executor.
pub(super) struct InsertExecutor {
    /// target table id
    table_id: TableId,
    source_manager: SourceManagerRef,

    child: BoxedExecutor,
    executed: bool,
    schema: Schema,
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

        let source = self.source_manager.get_source(&self.table_id)?;
        let table_source = if let SourceImpl::Table(ref t) = *source.source {
            t
        } else {
            panic!("InsertExecutor: table source expected");
        };
        let mut writer = table_source.create_writer()?;

        let mut rows_inserted = 0;
        while let Some(child_chunk) = self.child.next().await? {
            let len = child_chunk.capacity();
            assert!(child_chunk.visibility().is_none());

            let mut columns = Vec::with_capacity(child_chunk.columns().len() + 1);

            // add row-id column as first column
            let mut builder = I64ArrayBuilder::new(len).unwrap();
            for _ in 0..len {
                builder
                    .append(Some(table_source.next_row_id() as i64))
                    .unwrap();
            }
            let rowid_column = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
            columns.insert(0, rowid_column);

            for col in child_chunk.columns().iter() {
                columns.push(col.clone());
            }

            let chunk = StreamChunk::new(vec![Op::Insert; len], columns, None);
            writer.write(chunk).await?;

            rows_inserted += len;
        }

        // FIXME: should do flush on checkpoint in the future
        writer.flush().await?;

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1)?;
            array_builder.append(Some(rows_inserted as i64))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder()
                .columns(vec![Column::new(Arc::new(array.into()))])
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

        let table_id = TableId::from(&insert_node.table_ref_id);

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build()?;

        Ok(Box::new(Self {
            table_id,
            source_manager: source.global_task_env().source_manager_ref(),
            child,
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int64Type::create(false),
                }],
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::{Field, Schema, SchemaId};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::{DataTypeKind, DecimalType, Int64Type};
    use risingwave_common::util::downcast_arc;
    use risingwave_source::{MemSourceManager, SourceManager};
    use risingwave_storage::bummock::{BummockResult, BummockTable};
    use risingwave_storage::table::{ScannableTable, SimpleTableManager, TableManager};
    use risingwave_storage::*;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_insert_executor() -> Result<()> {
        let table_manager = Arc::new(SimpleTableManager::new());
        let source_manager = Arc::new(MemSourceManager::new());

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

        let col1 = column_nonnull! { I64Array, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, [2, 4, 6, 8, 10] };
        let data_chunk: DataChunk = DataChunk::builder().columns(vec![col1, col2]).build();
        mock_executor.add(data_chunk.clone());

        // Create the first table.
        let table_id = TableId::new(SchemaId::default(), 0);
        let table = downcast_arc::<BummockTable>(
            table_manager
                .create_table(&table_id, table_columns.to_vec())
                .await?
                .into_any(),
        )?;
        source_manager.create_table_source(&table_id, table)?;

        let mut insert_executor = InsertExecutor {
            table_id: table_id.clone(),
            source_manager: source_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int64Type::create(false),
                }],
            },
        };
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int64);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = downcast_arc::<BummockTable>(
            table_manager
                .get_table(&insert_executor.table_id)?
                .into_any(),
        )?;
        match table_ref
            .get_data_by_columns(&table_ref.get_column_ids())
            .await?
        {
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

        // First insertion test ends.

        // Insert to the same table by a new executor. The result should have growing row ids from
        // the last one seen.
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(data_chunk.clone());
        let mut insert_executor = InsertExecutor {
            table_id: table_id.clone(),
            source_manager: source_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int64Type::create(false),
                }],
            },
        };
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int64);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = downcast_arc::<BummockTable>(
            table_manager
                .get_table(&insert_executor.table_id)?
                .into_any(),
        )?;
        match table_ref
            .get_data_by_columns(&table_ref.get_column_ids())
            .await?
        {
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
        // Second insertion test ends.

        // Insert into another table. It should have new row ids starting from 0.
        let table_id2 = TableId::new(SchemaId::default(), 1);
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(data_chunk);
        let mut insert_executor = InsertExecutor {
            table_id: table_id2.clone(),
            source_manager: source_manager.clone(),
            child: Box::new(mock_executor),
            executed: false,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int64Type::create(false),
                }],
            },
        };
        let table2 = downcast_arc::<BummockTable>(
            table_manager
                .create_table(&table_id2, table_columns)
                .await?
                .into_any(),
        )?;
        source_manager.create_table_source(&table_id2, table2)?;

        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int64);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)]
        );
        let table_ref = downcast_arc::<BummockTable>(
            table_manager
                .get_table(&insert_executor.table_id)?
                .into_any(),
        )?;
        match table_ref
            .get_data_by_columns(&table_ref.get_column_ids())
            .await?
        {
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
        // Third insertion test ends.

        Ok(())
    }
}
