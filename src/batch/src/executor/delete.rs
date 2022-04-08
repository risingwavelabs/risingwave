// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use super::BoxedExecutor;
use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// [`DeleteExecutor`] implements table deletion with values from its child executor.
// TODO: concurrent `DELETE` may cause problems. A scheduler might be required.
pub struct DeleteExecutor {
    /// Target table id.
    table_id: TableId,
    source_manager: SourceManagerRef,

    child: BoxedExecutor,
    executed: bool,
    schema: Schema,
    identity: String,
}

impl DeleteExecutor {
    pub fn new(table_id: TableId, source_manager: SourceManagerRef, child: BoxedExecutor) -> Self {
        Self {
            table_id,
            source_manager,
            child,
            executed: false,
            // TODO: support `RETURNING`
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            identity: "DeleteExecutor".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Executor for DeleteExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        info!("Delete executor");
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }

        let source_desc = self.source_manager.get_source(&self.table_id)?;
        let source = source_desc.source.as_table_v2().expect("not table source");

        let mut notifiers = Vec::new();

        while let Some(child_chunk) = self.child.next().await? {
            let len = child_chunk.cardinality();
            assert!(child_chunk.visibility().is_none());

            let chunk = StreamChunk::from_parts(vec![Op::Delete; len], child_chunk);

            let notifier = source.write_chunk(chunk)?;
            notifiers.push(notifier);
        }

        // Wait for all chunks to be taken / written.
        let rows_deleted = try_join_all(notifiers)
            .await
            .map_err(|_| {
                RwError::from(ErrorCode::InternalError(
                    "failed to wait chunks to be written".to_owned(),
                ))
            })?
            .into_iter()
            .sum::<usize>();

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1)?;
            array_builder.append(Some(rows_deleted as i64))?;

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
        info!("Cleaning delete executor.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for DeleteExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let delete_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Delete
        )?;

        let table_id = TableId::from(&delete_node.table_source_ref_id);

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build()?;

        Ok(Box::new(
            Self::new(
                table_id,
                source.global_batch_env().source_manager_ref(),
                child,
            )
            .fuse(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::{schema_test_utils, ColumnDesc, ColumnId};
    use risingwave_common::column_nonnull;
    use risingwave_source::{
        MemSourceManager, Source, SourceManager, StreamSourceReader, TableV2ReaderContext,
    };

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_delete_executor() -> Result<()> {
        let source_manager = Arc::new(MemSourceManager::new());

        // Schema for mock executor.
        let schema = schema_test_utils::ii();
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of the table
        let schema = schema_test_utils::ii();

        let table_columns: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc {
                data_type: f.data_type.clone(),
                column_id: ColumnId::from(i as i32), // use column index as column id
                name: f.name.clone(),
                field_descs: vec![],
                type_name: "".to_string(),
            })
            .collect();

        let col1 = column_nonnull! { I64Array, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, [2, 4, 6, 8, 10] };
        let data_chunk: DataChunk = DataChunk::builder().columns(vec![col1, col2]).build();
        mock_executor.add(data_chunk.clone());

        // Create the table.
        let table_id = TableId::new(0);
        source_manager.create_table_source_v2(&table_id, table_columns.to_vec())?;

        // Create reader
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.source.as_table_v2().unwrap();
        let mut reader = source.stream_reader(TableV2ReaderContext, vec![0.into(), 1.into()])?;

        // Delete
        let mut delete_executor =
            DeleteExecutor::new(table_id, source_manager.clone(), Box::new(mock_executor));
        let handle = tokio::spawn(async move {
            delete_executor.open().await.unwrap();
            let result = delete_executor.next().await.unwrap().unwrap();
            delete_executor.close().await.unwrap();
            assert_eq!(
                result
                    .column_at(0)
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(5)] // deleted rows
            );
        });

        // Read
        reader.open().await?;
        let chunk = reader.next().await?;

        assert_eq!(chunk.ops().to_vec(), vec![Op::Delete; 5]);

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

        handle.await.unwrap();

        Ok(())
    }
}
