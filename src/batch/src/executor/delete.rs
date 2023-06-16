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

use anyhow::anyhow;
use futures::future::try_join_all;
use futures_async_stream::try_stream;
use risingwave_common::array::{
    Array, ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema, TableId, TableVersionId};
use risingwave_common::error::{Result, RwError};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_source::dml_manager::DmlManagerRef;

use crate::error::BatchError;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// [`DeleteExecutor`] implements table deletion with values from its child executor.
// Note: multiple `DELETE`s in a single epoch, or concurrent `DELETE`s may lead to conflicting
// records. This is validated and filtered on the first `Materialize`.
pub struct DeleteExecutor {
    /// Target table id.
    table_id: TableId,
    table_version_id: TableVersionId,
    dml_manager: DmlManagerRef,
    child: BoxedExecutor,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    returning: bool,
    txn_id: TxnId,
}

impl DeleteExecutor {
    pub fn new(
        table_id: TableId,
        table_version_id: TableVersionId,
        dml_manager: DmlManagerRef,
        child: BoxedExecutor,
        chunk_size: usize,
        identity: String,
        returning: bool,
    ) -> Self {
        let table_schema = child.schema().clone();
        let txn_id = dml_manager.gen_txn_id();
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
                    fields: vec![Field::unnamed(DataType::Int64)],
                }
            },
            identity,
            returning,
            txn_id,
        }
    }
}

impl Executor for DeleteExecutor {
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

impl DeleteExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let data_types = self.child.schema().data_types();
        let mut builder = DataChunkBuilder::new(data_types, 1024);

        let table_dml_handle = self
            .dml_manager
            .table_dml_handle(self.table_id, self.table_version_id)?;
        let mut write_handle = table_dml_handle.write_handle(self.txn_id)?;

        let mut notifiers = Vec::new();

        notifiers.push(write_handle.begin()?);

        // Transform the data chunk to a stream chunk, then write to the source.
        let write_txn_data = |chunk: DataChunk| {
            let cap = chunk.capacity();
            let stream_chunk = StreamChunk::from_parts(vec![Op::Delete; cap], chunk);

            #[cfg(debug_assertions)]
            table_dml_handle.check_chunk_schema(&stream_chunk);

            write_handle.write_chunk(stream_chunk)
        };

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = match data_chunk {
                Ok(data_chunk) => data_chunk,
                Err(err) => {
                    write_handle.rollback()?;
                    return Err(err);
                }
            };
            if self.returning {
                yield data_chunk.clone();
            }
            for chunk in builder.append_chunk(data_chunk) {
                notifiers.push(write_txn_data(chunk)?);
            }
        }

        if let Some(chunk) = builder.consume_all() {
            notifiers.push(write_txn_data(chunk)?);
        }

        notifiers.push(write_handle.end()?);

        // Wait for all chunks to be taken / written.
        let rows_deleted = try_join_all(notifiers)
            .await
            .map_err(|_| BatchError::Internal(anyhow!("failed to wait chunks to be written")))?
            .into_iter()
            .sum::<usize>();

        // create ret value
        if !self.returning {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1);
            array_builder.append(Some(rows_deleted as i64));

            let array = array_builder.finish();
            let ret_chunk = DataChunk::new(vec![array.into_ref()], 1);

            yield ret_chunk
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for DeleteExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();
        let delete_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Delete
        )?;

        let table_id = TableId::new(delete_node.table_id);

        Ok(Box::new(Self::new(
            table_id,
            delete_node.table_version_id,
            source.context().dml_manager(),
            child,
            source.context.get_config().developer.chunk_size,
            source.plan_node().get_identity().clone(),
            delete_node.returning,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::Array;
    use risingwave_common::catalog::{
        schema_test_utils, ColumnDesc, ColumnId, INITIAL_TABLE_VERSION_ID,
    };
    use risingwave_common::hash::ActorId;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::util::worker_util::WorkerNodeId;
    use risingwave_source::dml_manager::DmlManager;

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_delete_executor() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::new(WorkerNodeId::default()));

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

        // Create reader
        let table_id = TableId::new(0);
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
        const ACTOR_ID1: ActorId = 1;
        let mut reader = reader.stream_reader(ACTOR_ID1).into_stream();

        // Delete
        let delete_executor = Box::new(DeleteExecutor::new(
            table_id,
            INITIAL_TABLE_VERSION_ID,
            dml_manager,
            Box::new(mock_executor),
            1024,
            "DeleteExecutor".to_string(),
            false,
        ));

        let handle = tokio::spawn(async move {
            let fields = &delete_executor.schema().fields;
            assert_eq!(fields[0].data_type, DataType::Int64);

            let mut stream = delete_executor.execute();
            let result = stream.next().await.unwrap().unwrap();

            assert_eq!(
                result.column_at(0).as_int64().iter().collect::<Vec<_>>(),
                vec![Some(5)] // deleted rows
            );
        });

        // Read
        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Begin(_));

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Data(_, chunk) => {
            assert_eq!(chunk.ops().to_vec(), vec![Op::Delete; 5]);

            assert_eq!(
                chunk.columns()[0].as_int32().iter().collect::<Vec<_>>(),
                vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
            );

            assert_eq!(
                chunk.columns()[1].as_int32().iter().collect::<Vec<_>>(),
                vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
            );
        });

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::End(_));

        handle.await.unwrap();

        Ok(())
    }
}
