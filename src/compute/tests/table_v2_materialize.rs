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

use futures::stream::StreamExt;
use itertools::Itertools;
use risingwave_batch::executor::{CreateTableExecutor, Executor as BatchExecutor};
use risingwave_batch::executor2::executor_wrapper::ExecutorWrapper;
use risingwave_batch::executor2::monitor::BatchMetrics;
use risingwave_batch::executor2::{
    DeleteExecutor2, Executor2, InsertExecutor2, RowSeqScanExecutor2,
};
use risingwave_common::array::{Array, DataChunk, F64Array, I64Array};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::column_nonnull;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::IntoOrdered;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::batch_plan::create_table_node::Info;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;
use risingwave_source::{MemSourceManager, SourceManager};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::table::cell_based_table::CellBasedTable;
// use risingwave_storage::table::mview::MViewTable;
use risingwave_storage::{Keyspace, StateStore, StateStoreImpl};
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{
    Barrier, Executor, MaterializeExecutor, Message, PkIndices, SourceExecutor,
};
use tokio::sync::mpsc::unbounded_channel;

struct SingleChunkExecutor {
    chunk: Option<DataChunk>,
    schema: Schema,
    identity: String,
}

impl SingleChunkExecutor {
    pub fn new(chunk: DataChunk, schema: Schema) -> Self {
        Self {
            chunk: Some(chunk),
            schema,
            identity: "SingleChunkExecutor".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl BatchExecutor for SingleChunkExecutor {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        Ok(self.chunk.take())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

/// This test checks whether batch task and streaming task work together for `TableV2` creation,
/// insertion, deletion, and materialization.
#[tokio::test]
async fn test_table_v2_materialize() -> Result<()> {
    let memory_state_store = MemoryStateStore::new();
    let _state_store_impl = StateStoreImpl::MemoryStateStore(
        memory_state_store
            .clone()
            .monitored(Arc::new(StateStoreMetrics::unused())),
    );
    let source_manager = Arc::new(MemSourceManager::default());
    let source_table_id = TableId::default();
    let table_columns = vec![
        // data
        ProstColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Double as i32,
                ..Default::default()
            }),
            column_id: 0,
            ..Default::default()
        },
        // row id
        ProstColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            column_id: 1,
            ..Default::default()
        },
    ];

    // Create table v2 using `CreateTableExecutor`
    let mut create_table = CreateTableExecutor::new(
        source_table_id,
        source_manager.clone(),
        table_columns,
        "CreateTableExecutor".to_string(),
        Info::TableSource(Default::default()),
    );
    // Execute
    create_table.open().await?;
    create_table.next().await?;
    create_table.close().await?;

    // Ensure the source exists
    let source_desc = source_manager.get_source(&source_table_id)?;
    let get_schema = |column_ids: &[ColumnId]| {
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::unnamed(column_desc.data_type.clone()));
        }
        Schema::new(fields)
    };

    // Create a `SourceExecutor` to read the changes
    let all_column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
    let all_schema = get_schema(&all_column_ids);
    let (barrier_tx, barrier_rx) = unbounded_channel();
    let keyspace = Keyspace::executor_root(MemoryStateStore::new(), 0x2333);
    let stream_source = SourceExecutor::new(
        source_table_id,
        source_desc.clone(),
        keyspace,
        all_column_ids.clone(),
        all_schema.clone(),
        PkIndices::from([1]),
        barrier_rx,
        1,
        1,
        "SourceExecutor".to_string(),
        Arc::new(StreamingMetrics::unused()),
        vec![],
    )?;

    // Create a `Materialize` to write the changes to storage
    let keyspace = Keyspace::table_root(memory_state_store.clone(), &source_table_id);
    let mut materialize = MaterializeExecutor::new(
        Box::new(stream_source),
        keyspace.clone(),
        vec![OrderPair::new(1, OrderType::Ascending)],
        all_column_ids.clone(),
        2,
    )
    .boxed()
    .execute();

    // 1.
    // Test insertion
    //

    // Add some data using `InsertExecutor`, assuming we are inserting into the "mv"
    let columns = vec![column_nonnull! { F64Array, [1.14, 5.14] }];
    let chunk = DataChunk::builder().columns(columns.clone()).build();
    let insert_inner: Box<dyn BatchExecutor> =
        Box::new(SingleChunkExecutor::new(chunk, all_schema.clone()));
    let insert = Box::new(InsertExecutor2::new(
        source_table_id,
        source_manager.clone(),
        Box::new(ExecutorWrapper::from(insert_inner)),
        false,
    ));

    tokio::spawn(async move {
        let mut stream = insert.execute();
        let _ = stream.next().await.unwrap()?;
        Ok::<_, RwError>(())
    });

    let column_descs = all_column_ids
        .into_iter()
        .zip_eq(all_schema.fields.iter().cloned())
        .map(|(column_id, field)| ColumnDesc {
            data_type: field.data_type,
            column_id,
            name: field.name,
            field_descs: vec![],
            type_name: "".to_string(),
        })
        .collect_vec();

    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let keyspace = Keyspace::table_root(memory_state_store, &source_table_id);
    let table = CellBasedTable::new_adhoc(
        keyspace,
        column_descs,
        Arc::new(StateStoreMetrics::unused()),
    );

    let scan = Box::new(RowSeqScanExecutor2::new(
        table.clone(),
        1024,
        true,
        "RowSeqExecutor2".to_string(),
        u64::MAX,
        Arc::new(BatchMetrics::unused()),
    ));
    let mut stream = scan.execute();
    let result = stream.next().await;

    assert!(result.is_none());
    // Send a barrier to start materialized view
    let curr_epoch = 1919;
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch))
        .unwrap();

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    // Poll `Materialize`, should output the same insertion stream chunk
    let message = materialize.next().await.unwrap()?;
    let mut col_row_ids = vec![];
    match message {
        Message::Chunk(c) => {
            let col_data = c.columns()[0].array_ref().as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
            assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

            let col_row_id = c.columns()[1].array_ref().as_int64();
            col_row_ids.push(col_row_id.value_at(0).unwrap());
            col_row_ids.push(col_row_id.value_at(1).unwrap());
        }
        Message::Barrier(_) => panic!(),
    }

    // Send a barrier and poll again, should write changes to storage
    let curr_epoch = 1919;
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch))
        .unwrap();

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    // Scan the table again, we are able to get the data now!
    let scan = Box::new(RowSeqScanExecutor2::new(
        table.clone(),
        1024,
        true,
        "RowSeqScanExecutor2".to_string(),
        u64::MAX,
        Arc::new(BatchMetrics::unused()),
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();

    let col_data = result.columns()[0].array_ref().as_float64();
    assert_eq!(col_data.len(), 2);
    assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
    assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

    // 2.
    // Test deletion
    //

    // Delete some data using `DeleteExecutor`, assuming we are inserting into the "mv"
    let columns = vec![
        column_nonnull! { F64Array, [1.14] },
        column_nonnull! { I64Array, [ col_row_ids[0]] }, // row id column
    ];
    let chunk = DataChunk::builder().columns(columns.clone()).build();
    let delete_inner: Box<dyn BatchExecutor> =
        Box::new(SingleChunkExecutor::new(chunk, all_schema.clone()));
    let delete = Box::new(DeleteExecutor2::new(
        source_table_id,
        source_manager.clone(),
        Box::new(ExecutorWrapper::from(delete_inner)),
    ));

    tokio::spawn(async move {
        let mut stream = delete.execute();
        let _ = stream.next().await.unwrap()?;
        Ok::<_, RwError>(())
    });

    // Poll `Materialize`, should output the same deletion stream chunk
    let message = materialize.next().await.unwrap()?;
    match message {
        Message::Chunk(c) => {
            let col_data = c.columns()[0].array_ref().as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());

            let col_row_id = c.columns()[1].array_ref().as_int64();
            assert_eq!(col_row_id.value_at(0).unwrap(), col_row_ids[0]);
        }
        Message::Barrier(_) => panic!(),
    }

    // Send a barrier and poll again, should write changes to storage
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch + 1))
        .unwrap();

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch + 1
    ));

    // Scan the table again, we are able to see the deletion now!
    let scan = Box::new(RowSeqScanExecutor2::new(
        table.clone(),
        1024,
        true,
        "RowSeqScanExecutor2".to_string(),
        u64::MAX,
        Arc::new(BatchMetrics::unused()),
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();
    let col_data = result.columns()[0].array_ref().as_float64();
    assert_eq!(col_data.len(), 1);
    assert_eq!(col_data.value_at(0).unwrap(), 5.14.into_ordered());

    Ok(())
}
