// Copyright 2025 RisingWave Labs
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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`UpdateExecutor`] implements table update with values from its child executor and given
/// expressions.
// Note: multiple `UPDATE`s in a single epoch, or concurrent `UPDATE`s may lead to conflicting
// records. This is validated and filtered on the first `Materialize`.
pub struct UpdateExecutor {
    /// Target table id.
    table_id: TableId,
    table_version_id: TableVersionId,
    dml_manager: DmlManagerRef,
    child: BoxedExecutor,
    old_exprs: Vec<BoxedExpression>,
    new_exprs: Vec<BoxedExpression>,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    returning: bool,
    txn_id: TxnId,
    session_id: u32,
}

impl UpdateExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        table_version_id: TableVersionId,
        dml_manager: DmlManagerRef,
        child: BoxedExecutor,
        old_exprs: Vec<BoxedExpression>,
        new_exprs: Vec<BoxedExpression>,
        chunk_size: usize,
        identity: String,
        returning: bool,
        session_id: u32,
    ) -> Self {
        let chunk_size = chunk_size.next_multiple_of(2);
        let table_schema = child.schema().clone();
        let txn_id = dml_manager.gen_txn_id();

        Self {
            table_id,
            table_version_id,
            dml_manager,
            child,
            old_exprs,
            new_exprs,
            chunk_size,
            schema: if returning {
                table_schema
            } else {
                Schema {
                    fields: vec![Field::unnamed(DataType::Int64)],
                }
            },
            identity,
            returning,
            txn_id,
            session_id,
        }
    }
}

impl Executor for UpdateExecutor {
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

impl UpdateExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let table_dml_handle = self
            .dml_manager
            .table_dml_handle(self.table_id, self.table_version_id)?;

        let data_types = table_dml_handle
            .column_descs()
            .iter()
            .map(|c| c.data_type.clone())
            .collect_vec();

        assert_eq!(
            data_types,
            self.new_exprs.iter().map(|e| e.return_type()).collect_vec(),
            "bad update schema"
        );
        assert_eq!(
            data_types,
            self.old_exprs.iter().map(|e| e.return_type()).collect_vec(),
            "bad update schema"
        );

        let mut builder = DataChunkBuilder::new(data_types, self.chunk_size);

        let mut write_handle: risingwave_dml::WriteHandle =
            table_dml_handle.write_handle(self.session_id, self.txn_id)?;
        write_handle.begin()?;

        // Transform the data chunk to a stream chunk, then write to the source.
        let write_txn_data = |chunk: DataChunk| async {
            // TODO: if the primary key is updated, we should use plain `+,-` instead of `U+,U-`.
            let ops = [Op::UpdateDelete, Op::UpdateInsert]
                .into_iter()
                .cycle()
                .take(chunk.capacity())
                .collect_vec();
            let stream_chunk = StreamChunk::from_parts(ops, chunk);

            #[cfg(debug_assertions)]
            table_dml_handle.check_chunk_schema(&stream_chunk);

            write_handle.write_chunk(stream_chunk).await
        };

        let mut rows_updated = 0;

        #[for_await]
        for input in self.child.execute() {
            let input = input?;

            let old_data_chunk = {
                let mut columns = Vec::with_capacity(self.old_exprs.len());
                for expr in &self.old_exprs {
                    let column = expr.eval(&input).await?;
                    columns.push(column);
                }

                DataChunk::new(columns, input.visibility().clone())
            };

            let updated_data_chunk = {
                let mut columns = Vec::with_capacity(self.new_exprs.len());
                for expr in &self.new_exprs {
                    let column = expr.eval(&input).await?;
                    columns.push(column);
                }

                DataChunk::new(columns, input.visibility().clone())
            };

            if self.returning {
                yield updated_data_chunk.clone();
            }

            for (row_delete, row_insert) in
                (old_data_chunk.rows()).zip_eq_debug(updated_data_chunk.rows())
            {
                rows_updated += 1;
                // If row_delete == row_insert, we don't need to do a actual update
                if row_delete != row_insert {
                    let None = builder.append_one_row(row_delete) else {
                        unreachable!(
                            "no chunk should be yielded when appending the deleted row as the chunk size is always even"
                        );
                    };
                    if let Some(chunk) = builder.append_one_row(row_insert) {
                        write_txn_data(chunk).await?;
                    }
                }
            }
        }

        if let Some(chunk) = builder.consume_all() {
            write_txn_data(chunk).await?;
        }
        write_handle.end().await?;

        // Create ret value
        if !self.returning {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1);
            array_builder.append(Some(rows_updated as i64));

            let array = array_builder.finish();
            let ret_chunk = DataChunk::new(vec![array.into_ref()], 1);

            yield ret_chunk
        }
    }
}

impl BoxedExecutorBuilder for UpdateExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let update_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Update
        )?;

        let table_id = TableId::new(update_node.table_id);

        let old_exprs: Vec<_> = update_node
            .get_old_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;

        let new_exprs: Vec<_> = update_node
            .get_new_exprs()
            .iter()
            .map(build_from_prost)
            .try_collect()?;

        Ok(Box::new(Self::new(
            table_id,
            update_node.table_version_id,
            source.context().dml_manager(),
            child,
            old_exprs,
            new_exprs,
            source.context().get_config().developer.chunk_size,
            source.plan_node().get_identity().clone(),
            update_node.returning,
            update_node.session_id,
        )))
    }
}

#[cfg(test)]
#[cfg(any())]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::catalog::{
        ColumnDesc, ColumnId, INITIAL_TABLE_VERSION_ID, schema_test_utils,
    };
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_dml::dml_manager::DmlManager;
    use risingwave_expr::expr::InputRefExpression;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_update_executor() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::for_test());

        // Schema for mock executor.
        let schema = schema_test_utils::ii();
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of the table
        let schema = schema_test_utils::ii();

        mock_executor.add(DataChunk::from_pretty(
            "i  i
             1  2
             3  4
             5  6
             7  8
             9 10",
        ));

        // Update expressions, will swap two columns.
        let exprs = vec![
            Box::new(InputRefExpression::new(DataType::Int32, 1)) as BoxedExpression,
            Box::new(InputRefExpression::new(DataType::Int32, 0)),
        ];

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

        // Update
        let update_executor = Box::new(UpdateExecutor::new(
            table_id,
            INITIAL_TABLE_VERSION_ID,
            dml_manager,
            Box::new(mock_executor),
            exprs,
            5,
            "UpdateExecutor".to_string(),
            false,
            vec![0, 1],
            0,
        ));

        let handle = tokio::spawn(async move {
            let fields = &update_executor.schema().fields;
            assert_eq!(fields[0].data_type, DataType::Int64);

            let mut stream = update_executor.execute();
            let result = stream.next().await.unwrap().unwrap();

            assert_eq!(
                result.column_at(0).as_int64().iter().collect::<Vec<_>>(),
                vec![Some(5)] // updated rows
            );
        });

        reader.next().await.unwrap()?.into_begin().unwrap();

        // Read
        // As we set the chunk size to 5, we'll get 2 chunks. Note that the update records for one
        // row cannot be cut into two chunks, so the first chunk will actually have 6 rows.
        for updated_rows in [1..=3, 4..=5] {
            let txn_msg = reader.next().await.unwrap()?;
            let chunk = txn_msg.as_stream_chunk().unwrap();
            assert_eq!(
                chunk.ops().chunks(2).collect_vec(),
                vec![&[Op::UpdateDelete, Op::UpdateInsert]; updated_rows.clone().count()]
            );

            assert_eq!(
                chunk.columns()[0].as_int32().iter().collect::<Vec<_>>(),
                updated_rows
                    .clone()
                    .flat_map(|i| [i * 2 - 1, i * 2]) // -1, +2, -3, +4, ...
                    .map(Some)
                    .collect_vec()
            );

            assert_eq!(
                chunk.columns()[1].as_int32().iter().collect::<Vec<_>>(),
                updated_rows
                    .clone()
                    .flat_map(|i| [i * 2, i * 2 - 1]) // -2, +1, -4, +3, ...
                    .map(Some)
                    .collect_vec()
            );
        }

        reader.next().await.unwrap()?.into_end().unwrap();

        handle.await.unwrap();

        Ok(())
    }
}
