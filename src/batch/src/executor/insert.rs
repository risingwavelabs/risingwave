// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::repeat;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use futures::future::try_join_all;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{
    ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, SerialArray, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema, TableId, TableVersionId};
use risingwave_common::error::{Result, RwError};
use risingwave_common::transaction::transaction_message::{generate_txn_id, TxnMsg};
use risingwave_common::transaction::TxnId;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::IndexAndExpr;
use risingwave_source::dml_manager::DmlManagerRef;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// [`InsertExecutor`] implements table insertion with values from its child executor.
pub struct InsertExecutor {
    /// Target table id.
    table_id: TableId,
    table_version_id: TableVersionId,
    dml_manager: DmlManagerRef,

    child: BoxedExecutor,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    column_indices: Vec<usize>,
    sorted_default_columns: Vec<(usize, BoxedExpression)>,

    row_id_index: Option<usize>,
    returning: bool,
    txn_id: TxnId,
}

impl InsertExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        table_version_id: TableVersionId,
        dml_manager: DmlManagerRef,
        child: BoxedExecutor,
        chunk_size: usize,
        identity: String,
        column_indices: Vec<usize>,
        sorted_default_columns: Vec<(usize, BoxedExpression)>,
        row_id_index: Option<usize>,
        returning: bool,
    ) -> Self {
        let table_schema = child.schema().clone();
        Self {
            table_id,
            table_version_id,
            dml_manager,
            child,
            chunk_size,
            schema: if returning {
                table_schema
            } else {
                Schema {
                    fields: vec![Field::unnamed(DataType::Serial)],
                }
            },
            identity,
            column_indices,
            sorted_default_columns,
            row_id_index,
            returning,
            txn_id: generate_txn_id(),
        }
    }
}

impl Executor for InsertExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl InsertExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let data_types = self.child.schema().data_types();
        let mut builder = DataChunkBuilder::new(data_types, 1024);

        let mut notifiers = Vec::new();

        // Transform the data chunk to a stream chunk, then write to the source.
        let write_txn_data = |chunk: DataChunk| async {
            let cap = chunk.capacity();
            let (mut columns, vis) = chunk.into_parts();

            let dummy_chunk = DataChunk::new_dummy(cap);

            let mut ordered_columns = self
                .column_indices
                .iter()
                .enumerate()
                .map(|(i, idx)| (*idx, columns[i].clone()))
                .collect_vec();
            ordered_columns.reserve(ordered_columns.len() + self.sorted_default_columns.len());

            for (idx, expr) in &self.sorted_default_columns {
                let column = expr.eval(&dummy_chunk).await?;
                ordered_columns.push((*idx, column));
            }

            ordered_columns.sort_unstable_by_key(|(idx, _)| *idx);
            columns = ordered_columns
                .into_iter()
                .map(|(_, column)| column)
                .collect_vec();

            // If the user does not specify the primary key, then we need to add a column as the
            // primary key.
            if let Some(row_id_index) = self.row_id_index {
                let row_id_col = SerialArray::from_iter(repeat(None).take(cap));
                columns.insert(row_id_index, Arc::new(row_id_col.into()))
            }

            let stream_chunk =
                StreamChunk::new(vec![Op::Insert; cap], columns, vis.into_visibility());

            self.dml_manager
                .write_txn_msg(
                    self.table_id,
                    self.table_version_id,
                    TxnMsg::Data(self.txn_id, stream_chunk),
                )
                .await
        };

        notifiers.push(
            self.dml_manager
                .write_txn_msg(
                    self.table_id,
                    self.table_version_id,
                    TxnMsg::Begin(self.txn_id),
                )
                .await?,
        );

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?;
            if self.returning {
                yield data_chunk.clone();
            }
            for chunk in builder.append_chunk(data_chunk) {
                notifiers.push(write_txn_data(chunk).await?);
            }
        }

        if let Some(chunk) = builder.consume_all() {
            notifiers.push(write_txn_data(chunk).await?);
        }

        notifiers.push(
            self.dml_manager
                .write_txn_msg(
                    self.table_id,
                    self.table_version_id,
                    TxnMsg::End(self.txn_id),
                )
                .await?,
        );

        // Wait for all chunks to be taken / written.
        let rows_inserted = try_join_all(notifiers)
            .await
            .context("failed to wait chunks to be written")?
            .into_iter()
            .sum::<usize>();

        // create ret value
        if !self.returning {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1);
            array_builder.append(Some(rows_inserted as i64));

            let array = array_builder.finish();
            let ret_chunk = DataChunk::new(vec![Arc::new(array.into())], 1);

            yield ret_chunk
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for InsertExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let insert_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Insert
        )?;

        let table_id = TableId::new(insert_node.table_id);
        let column_indices = insert_node
            .column_indices
            .iter()
            .map(|&i| i as usize)
            .collect();
        let sorted_default_columns = if let Some(default_columns) = &insert_node.default_columns {
            let mut default_columns = default_columns
                .get_default_columns()
                .iter()
                .cloned()
                .map(|IndexAndExpr { index: i, expr: e }| {
                    Ok((
                        i as usize,
                        build_from_prost(&e.ok_or_else(|| anyhow!("expression is None"))?)
                            .map_err(|e| anyhow!("failed to build expression: {}", e))?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            default_columns.sort_unstable_by_key(|(i, _)| *i);
            default_columns
        } else {
            vec![]
        };

        Ok(Box::new(Self::new(
            table_id,
            insert_node.table_version_id,
            source.context().dml_manager(),
            child,
            source.context.get_config().developer.chunk_size,
            source.plan_node().get_identity().clone(),
            column_indices,
            sorted_default_columns,
            insert_node.row_id_index.as_ref().map(|index| *index as _),
            insert_node.returning,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Array, ArrayImpl, I32Array, StructArray};
    use risingwave_common::cache::CachePriority;
    use risingwave_common::catalog::{
        schema_test_utils, ColumnDesc, ColumnId, INITIAL_TABLE_VERSION_ID,
    };
    use risingwave_common::types::DataType;
    use risingwave_source::dml_manager::DmlManager;
    use risingwave_storage::hummock::CachePolicy;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::store::{ReadOptions, StateStoreReadExt};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_insert_executor() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::default());
        let store = MemoryStateStore::new();

        // Make struct field
        let struct_field = Field::unnamed(DataType::new_struct(
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
            vec![],
        ));

        // Schema for mock executor.
        let mut schema = schema_test_utils::ii();
        schema.fields.push(struct_field.clone());
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of the table
        let mut schema = schema_test_utils::ii();
        schema.fields.push(struct_field);
        schema.fields.push(Field::unnamed(DataType::Serial)); // row_id column

        let row_id_index = Some(3);

        let col1 = Arc::new(I32Array::from_iter([1, 3, 5, 7, 9]).into());
        let col2 = Arc::new(I32Array::from_iter([2, 4, 6, 8, 10]).into());
        let array = StructArray::from_slices(
            &[true, false, false, false, false],
            vec![
                I32Array::from_iter([Some(1), None, None, None, None]).into(),
                I32Array::from_iter([Some(2), None, None, None, None]).into(),
                I32Array::from_iter([Some(3), None, None, None, None]).into(),
            ],
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        );
        let col3 = Arc::new(array.into());
        let data_chunk: DataChunk = DataChunk::new(vec![col1, col2, col3], 5);
        mock_executor.add(data_chunk.clone());

        // Create the table.
        let table_id = TableId::new(0);

        // Create reader
        let column_descs = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| ColumnDesc::unnamed(ColumnId::new(i as _), field.data_type.clone()))
            .collect_vec();
        // We must create a variable to hold this `Arc<TableDmlHandle>` here, or it will be dropped
        // due to the `Weak` reference in `DmlManager`.
        let reader = dml_manager
            .register_reader(table_id, INITIAL_TABLE_VERSION_ID, &column_descs)
            .unwrap();
        let mut reader = reader.stream_reader().into_stream();

        // Insert
        let insert_executor = Box::new(InsertExecutor::new(
            table_id,
            INITIAL_TABLE_VERSION_ID,
            dml_manager,
            Box::new(mock_executor),
            1024,
            "InsertExecutor".to_string(),
            vec![0, 1, 2], // Ignoring insertion order
            vec![],
            row_id_index,
            false,
        ));
        let handle = tokio::spawn(async move {
            let mut stream = insert_executor.execute();
            let result = stream.next().await.unwrap().unwrap();

            assert_eq!(
                result.column_at(0).as_int64().iter().collect::<Vec<_>>(),
                vec![Some(5)] // inserted rows
            );
        });

        // Read
        let chunk = reader.next().await.unwrap()?;

        assert_eq!(
            chunk.columns()[0].as_int32().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
        );

        assert_eq!(
            chunk.columns()[1].as_int32().iter().collect::<Vec<_>>(),
            vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
        );

        let array: ArrayImpl = StructArray::from_slices(
            &[true, false, false, false, false],
            vec![
                I32Array::from_iter([Some(1), None, None, None, None]).into(),
                I32Array::from_iter([Some(2), None, None, None, None]).into(),
                I32Array::from_iter([Some(3), None, None, None, None]).into(),
            ],
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .into();
        assert_eq!(*chunk.columns()[2], array);

        let epoch = u64::MAX;
        let full_range = (Bound::Unbounded, Bound::Unbounded);
        let store_content = store
            .scan(
                full_range,
                epoch,
                None,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: false,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await?;
        assert!(store_content.is_empty());

        // Note: We need to keep calling `next()`, so that the executor can collect the `End`
        // notification.
        tokio::spawn(async move {
            reader.next().await.unwrap().unwrap();
        });

        handle.await.unwrap();

        Ok(())
    }
}
