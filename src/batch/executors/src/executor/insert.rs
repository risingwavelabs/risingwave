// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use futures::future::{BoxFuture, FutureExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{
    ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, SerialArray, StreamChunk,
};
use risingwave_common::catalog::{Schema, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_dml::{TableDmlHandleRef, WriteHandle};
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::IndexAndExpr;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    PushContext, PushSink, PushStatus,
};

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
    session_id: u32,
    wait_for_persistence: bool,
}

impl InsertExecutor {
    #[expect(clippy::too_many_arguments)]
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
        session_id: u32,
        wait_for_persistence: bool,
    ) -> Self {
        let table_schema = child.schema().clone();
        let txn_id = dml_manager.gen_txn_id();
        Self {
            table_id,
            table_version_id,
            dml_manager,
            child,
            chunk_size,
            schema: table_schema,
            identity,
            column_indices,
            sorted_default_columns,
            row_id_index,
            returning,
            txn_id,
            session_id,
            wait_for_persistence,
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

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let InsertExecutor {
                table_id,
                table_version_id,
                dml_manager,
                child,
                chunk_size,
                schema: _,
                identity: _,
                column_indices,
                sorted_default_columns,
                row_id_index,
                returning,
                txn_id,
                session_id,
                wait_for_persistence,
            } = *self;
            let data_types = child.schema().data_types();
            let table_dml_handle = dml_manager.table_dml_handle(table_id, table_version_id)?;
            let mut write_handle = table_dml_handle.write_handle(session_id, txn_id)?;
            write_handle.begin()?;

            let mut state = InsertPushState {
                table_dml_handle,
                write_handle: Some(write_handle),
                builder: DataChunkBuilder::new(data_types, chunk_size),
                column_indices,
                sorted_default_columns,
                row_id_index,
                returning,
                wait_for_persistence,
                rows_inserted: 0,
                downstream_finished: false,
            };

            {
                let mut child_sink = InsertChildSink {
                    state: &mut state,
                    sink,
                };
                child.execute_push(context, &mut child_sink).await?;
            }

            state.finish(sink).await
        }
        .boxed()
    }
}

struct InsertPushState {
    table_dml_handle: TableDmlHandleRef,
    write_handle: Option<WriteHandle>,
    builder: DataChunkBuilder,
    column_indices: Vec<usize>,
    sorted_default_columns: Vec<(usize, BoxedExpression)>,
    row_id_index: Option<usize>,
    returning: bool,
    wait_for_persistence: bool,
    rows_inserted: usize,
    downstream_finished: bool,
}

impl InsertPushState {
    async fn write_txn_data(&mut self, chunk: DataChunk) -> Result<DataChunk> {
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

        let returning_chunk = DataChunk::new(columns.clone(), vis.clone());

        if let Some(row_id_index) = self.row_id_index {
            let row_id_col = SerialArray::from_iter(std::iter::repeat_n(None, cap));
            columns.insert(row_id_index, Arc::new(row_id_col.into()))
        }

        let stream_chunk = StreamChunk::with_visibility(vec![Op::Insert; cap], columns, vis);

        #[cfg(debug_assertions)]
        self.table_dml_handle.check_chunk_schema(&stream_chunk);

        self.write_handle
            .as_mut()
            .unwrap()
            .write_chunk(stream_chunk)
            .await?;

        Ok(returning_chunk)
    }

    async fn consume_input_chunk(
        &mut self,
        data_chunk: DataChunk,
        sink: &mut dyn PushSink,
    ) -> Result<()> {
        let chunks = self.builder.append_chunk(data_chunk).collect_vec();
        for chunk in chunks {
            let chunk = self.write_txn_data(chunk).await?;
            self.rows_inserted += chunk.cardinality();
            if self.returning && !self.downstream_finished {
                self.downstream_finished |= sink.push(chunk).await?.is_finished();
            }
        }
        Ok(())
    }

    async fn finish(mut self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        if let Some(chunk) = self.builder.consume_all() {
            let chunk = self.write_txn_data(chunk).await?;
            self.rows_inserted += chunk.cardinality();
            if self.returning && !self.downstream_finished {
                self.downstream_finished |= sink.push(chunk).await?.is_finished();
            }
        }

        let write_handle = self.write_handle.take().unwrap();
        if self.wait_for_persistence {
            write_handle.end_wait_persistence()?.await?;
        } else {
            write_handle.end().await?;
        }

        if !self.returning && !self.downstream_finished {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1);
            array_builder.append(Some(self.rows_inserted as i64));

            let array = array_builder.finish();
            let ret_chunk = DataChunk::new(vec![Arc::new(array.into())], 1);
            self.downstream_finished |= sink.push(ret_chunk).await?.is_finished();
        }

        sink.finish().await
    }
}

struct InsertChildSink<'a, 's> {
    state: &'a mut InsertPushState,
    sink: &'s mut dyn PushSink,
}

impl PushSink for InsertChildSink<'_, '_> {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            self.state.consume_input_chunk(chunk, self.sink).await?;
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }
}

impl InsertExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let data_types = self.child.schema().data_types();
        let mut builder = DataChunkBuilder::new(data_types, self.chunk_size);

        let table_dml_handle = self
            .dml_manager
            .table_dml_handle(self.table_id, self.table_version_id)?;
        let mut write_handle = table_dml_handle.write_handle(self.session_id, self.txn_id)?;

        write_handle.begin()?;

        // Transform the data chunk to a stream chunk, then write to the source.
        // Return the returning chunk.
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

            // Construct the returning chunk, without the `row_id` column.
            let returning_chunk = DataChunk::new(columns.clone(), vis.clone());

            // If the user does not specify the primary key, then we need to add a column as the
            // primary key.
            if let Some(row_id_index) = self.row_id_index {
                let row_id_col = SerialArray::from_iter(std::iter::repeat_n(None, cap));
                columns.insert(row_id_index, Arc::new(row_id_col.into()))
            }

            let stream_chunk = StreamChunk::with_visibility(vec![Op::Insert; cap], columns, vis);

            #[cfg(debug_assertions)]
            table_dml_handle.check_chunk_schema(&stream_chunk);

            write_handle.write_chunk(stream_chunk).await?;

            Result::Ok(returning_chunk)
        };

        let mut rows_inserted = 0;

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?;
            for chunk in builder.append_chunk(data_chunk) {
                let chunk = write_txn_data(chunk).await?;
                rows_inserted += chunk.cardinality();
                if self.returning {
                    yield chunk;
                }
            }
        }

        if let Some(chunk) = builder.consume_all() {
            let chunk = write_txn_data(chunk).await?;
            rows_inserted += chunk.cardinality();
            if self.returning {
                yield chunk;
            }
        }

        if self.wait_for_persistence {
            write_handle.end_wait_persistence()?.await?;
        } else {
            write_handle.end().await?;
        }

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

impl BoxedExecutorBuilder for InsertExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let insert_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Insert
        )?;

        let table_id = insert_node.table_id;
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
                        build_from_prost(&e.context("expression is None")?)
                            .context("failed to build expression")?,
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
            source.context().get_config().developer.chunk_size,
            source.plan_node().get_identity().clone(),
            column_indices,
            sorted_default_columns,
            insert_node.row_id_index.as_ref().map(|index| *index as _),
            insert_node.returning,
            insert_node.session_id,
            insert_node.wait_for_persistence,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use assert_matches::assert_matches;
    use foyer::Hint;
    use futures::StreamExt;
    use risingwave_common::array::{Array, ArrayImpl, I32Array, StructArray};
    use risingwave_common::catalog::{
        ColumnDesc, ColumnId, Field, INITIAL_TABLE_VERSION_ID, schema_test_utils,
    };
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::types::{DataType, StructType};
    use risingwave_dml::dml_manager::DmlManager;
    use risingwave_storage::hummock::CachePolicy;
    use risingwave_storage::hummock::test_utils::*;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_insert_executor() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::for_test());
        let store = MemoryStateStore::new();

        // Make struct field
        let struct_field = Field::unnamed(
            StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]).into(),
        );

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
        let array = StructArray::new(
            StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]),
            vec![
                I32Array::from_iter([Some(1), None, None, None, None]).into_ref(),
                I32Array::from_iter([Some(2), None, None, None, None]).into_ref(),
                I32Array::from_iter([Some(3), None, None, None, None]).into_ref(),
            ],
            [true, false, false, false, false].into_iter().collect(),
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
            "InsertExecutor".to_owned(),
            vec![0, 1, 2], // Ignoring insertion order
            vec![],
            row_id_index,
            false,
            0,
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
        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Begin(_));

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Data(_, chunk) => {
            assert_eq!(
                chunk.columns()[0].as_int32().iter().collect::<Vec<_>>(),
                vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
            );

            assert_eq!(
                chunk.columns()[1].as_int32().iter().collect::<Vec<_>>(),
                vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
            );

            let array: ArrayImpl = StructArray::new(
                StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]),
                vec![
                    I32Array::from_iter([Some(1), None, None, None, None]).into_ref(),
                    I32Array::from_iter([Some(2), None, None, None, None]).into_ref(),
                    I32Array::from_iter([Some(3), None, None, None, None]).into_ref(),
                ],
                [true, false, false, false, false].into_iter().collect(),
            )
            .into();
            assert_eq!(*chunk.columns()[2], array);
        });

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::End(_, None));
        let epoch = u64::MAX;
        let full_range = (Bound::Unbounded, Bound::Unbounded);
        let store_content = store
            .scan(
                full_range,
                epoch,
                None,
                ReadOptions {
                    cache_policy: CachePolicy::Fill(Hint::Normal),
                    ..Default::default()
                },
            )
            .await?;
        assert!(store_content.is_empty());

        handle.await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_executor_wait_for_persistence() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::for_test());

        let schema = schema_test_utils::ii();
        let mut mock_executor = MockExecutor::new(schema.clone());
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 2",
        ));

        let table_id = TableId::new(0);
        let column_descs = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| ColumnDesc::unnamed(ColumnId::new(i as _), field.data_type.clone()))
            .collect_vec();
        let reader = dml_manager
            .register_reader(table_id, INITIAL_TABLE_VERSION_ID, &column_descs)
            .unwrap();
        let mut reader = reader.stream_reader().into_stream();

        let insert_executor = Box::new(InsertExecutor::new(
            table_id,
            INITIAL_TABLE_VERSION_ID,
            dml_manager,
            Box::new(mock_executor),
            1024,
            "InsertExecutor".to_owned(),
            vec![0, 1],
            vec![],
            None,
            false,
            0,
            true,
        ));
        let handle = tokio::spawn(async move {
            let mut stream = insert_executor.execute();
            let result = stream.next().await.unwrap().unwrap();
            assert_eq!(
                result.column_at(0).as_int64().iter().collect::<Vec<_>>(),
                vec![Some(1)]
            );
        });

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Begin(_));
        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Data(_, _));
        assert_matches!(reader.next().await.unwrap()?, TxnMsg::End(_, Some(persistence_notifier)) => {
            assert!(!handle.is_finished());
            persistence_notifier.send(()).unwrap();
        });

        handle.await.unwrap();

        Ok(())
    }
}
