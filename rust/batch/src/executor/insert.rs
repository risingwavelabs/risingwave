use std::iter::once;
use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, DataChunk, I64ArrayBuilder, Op, PrimitiveArrayBuilder, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use super::BoxedExecutor;
use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// `InsertExecutor` implements table insertion with values from its child executor.
pub struct InsertExecutor {
    /// target table id
    table_id: TableId,
    source_manager: SourceManagerRef,
    worker_id: u32,

    child: BoxedExecutor,
    executed: bool,
    schema: Schema,
    identity: String,
}

impl InsertExecutor {
    pub fn new(
        table_id: TableId,
        source_manager: SourceManagerRef,
        child: BoxedExecutor,
        worker_id: u32,
    ) -> Self {
        Self {
            table_id,
            source_manager,
            worker_id,
            child,
            executed: false,
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            identity: "InsertExecutor".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Executor for InsertExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        info!("Insert executor");
        Ok(())
    }

    // TODO: refactor this function since we only have `TableV2` now.
    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }

        let source_desc = self.source_manager.get_source(&self.table_id)?;
        let source = source_desc.source.as_table_v2();

        let mut rows_inserted = 0;
        while let Some(child_chunk) = self.child.next().await? {
            let len = child_chunk.capacity();
            assert!(child_chunk.visibility().is_none());

            // add row-id column as first column
            let mut builder = I64ArrayBuilder::new(len).unwrap();
            for _ in 0..len {
                builder
                    .append(Some(source.next_row_id(self.worker_id)))
                    .unwrap();
            }

            let rowid_column = once(Column::new(Arc::new(ArrayImpl::from(
                builder.finish().unwrap(),
            ))));
            let child_columns = child_chunk.columns().iter().map(|c| c.to_owned());

            // put row id column to the last to match the behavior of mview
            let columns = child_columns.chain(rowid_column).collect();
            let chunk = StreamChunk::new(vec![Op::Insert; len], columns, None);

            source.write_chunk(chunk).await?;
            rows_inserted += len;
        }

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

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for InsertExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let insert_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Insert
        )?;

        let table_id = TableId::from(&insert_node.table_ref_id);

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build()?;

        Ok(Box::new(Self::new(
            table_id,
            source.global_batch_env().source_manager_ref(),
            child,
            source.global_batch_env().worker_id(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::{ColumnId, Field, Schema, SchemaId};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_source::{
        MemSourceManager, Source, SourceManager, StreamSourceReader, TableV2ReaderContext,
    };
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::{SimpleTableManager, TableManager};
    use risingwave_storage::*;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_insert_executor_for_table_v2() -> Result<()> {
        let table_manager = Arc::new(SimpleTableManager::with_in_memory_store());
        let source_manager = Arc::new(MemSourceManager::new());
        let store = MemoryStateStore::new();

        // Schema for mock executor.
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of first table
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Decimal),
                Field::unnamed(DataType::Decimal),
                Field::unnamed(DataType::Decimal),
            ],
        };

        let table_columns: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| TableColumnDesc {
                data_type: f.data_type.clone(),
                column_id: ColumnId::from(i as i32), // use column index as column id
                name: f.name.clone(),
            })
            .collect();

        let col1 = column_nonnull! { I64Array, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, [2, 4, 6, 8, 10] };
        let data_chunk: DataChunk = DataChunk::builder().columns(vec![col1, col2]).build();
        mock_executor.add(data_chunk.clone());

        // Create the first table.
        let table_id = TableId::new(SchemaId::default(), 0);
        let table = table_manager
            .create_table_v2(&table_id, table_columns.to_vec())
            .await?;
        source_manager.create_table_source_v2(&table_id, table)?;

        // Create reader
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.source.as_table_v2();
        let mut reader = source.stream_reader(
            TableV2ReaderContext,
            vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)],
        )?;

        let mut insert_executor = InsertExecutor::new(
            table_id.clone(),
            source_manager.clone(),
            Box::new(mock_executor),
            0,
        );
        insert_executor.open().await.unwrap();
        let fields = &insert_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int64);
        let result = insert_executor.next().await?.unwrap();
        insert_executor.close().await.unwrap();
        assert_eq!(
            result
                .column_at(0)
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5)] // inserted rows
        );

        reader.open().await?;
        let chunk = reader.next().await?;

        assert_eq!(
            chunk.columns()[0]
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
        );

        assert_eq!(
            chunk.columns()[1]
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
        );

        assert_eq!(
            chunk.columns()[2]
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2), Some(3), Some(4)]
        );

        // There's nothing in store since `TableSourceV2` has no side effect.
        // Data will be materialized in associated streaming task.
        let epoch = u64::MAX;
        let full_range = (Bound::<Vec<u8>>::Unbounded, Bound::<Vec<u8>>::Unbounded);
        let store_content = store.scan(full_range, None, epoch).await?;
        assert!(store_content.is_empty());

        Ok(())
    }
}
