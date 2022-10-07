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

#![feature(generators)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]

use std::sync::Arc;

use bytes::Bytes;
use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch::executor::{
    BoxedDataChunkStream, BoxedExecutor, DeleteExecutor, Executor as BatchExecutor, InsertExecutor,
    RowSeqScanExecutor, ScanType,
};
use risingwave_common::array::{Array, DataChunk, F64Array, I64Array, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::column_nonnull;
use risingwave_common::error::{Result, RwError};
use risingwave_common::test_prelude::DataChunkTestExt;
use risingwave_common::types::{DataType, IntoOrdered};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_source::{MemSourceManager, SourceDescBuilder, SourceManagerRef};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::state_table_handler::SourceStateTableHandler;
use risingwave_stream::executor::{
    ActorContext, Barrier, Executor, MaterializeExecutor, Message, PkIndices, SourceExecutor,
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

impl BatchExecutor for SingleChunkExecutor {
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

impl SingleChunkExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        yield self.chunk.unwrap()
    }
}

/// This test checks whether batch task and streaming task work together for `Table` creation,
/// insertion, deletion, and materialization.
#[tokio::test]
async fn test_table_materialize() -> StreamResult<()> {
    use risingwave_common::types::DataType;
    use risingwave_source::table_test_utils::create_table_info;
    use risingwave_storage::error::StorageError;
    use risingwave_stream::executor::state_table_handler::default_source_internal_table;

    let memory_state_store = MemoryStateStore::new();
    let source_manager: SourceManagerRef = Arc::new(MemSourceManager::default());
    let source_table_id = TableId::default();
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float64),
        ],
    };
    let info = create_table_info(&schema, Some(0), vec![0]);
    let source_builder = SourceDescBuilder::new(source_table_id, &info, &source_manager);

    // Ensure the source exists
    let source_desc = source_builder.build().await.unwrap();
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
    let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));
    let state_table = SourceStateTableHandler::from_table_catalog(
        &default_source_internal_table(0x2333),
        MemoryStateStore::new(),
    );
    let stream_source = SourceExecutor::new(
        ActorContext::create(0x3f3f3f),
        source_builder,
        source_table_id,
        vnodes,
        state_table,
        all_column_ids.clone(),
        all_schema.clone(),
        PkIndices::from([0]),
        barrier_rx,
        1,
        1,
        "SourceExecutor".to_string(),
        Arc::new(StreamingMetrics::unused()),
        u64::MAX,
    )?;

    // Create a `Materialize` to write the changes to storage

    let mut materialize = MaterializeExecutor::for_test(
        Box::new(stream_source),
        memory_state_store.clone(),
        source_table_id,
        vec![OrderPair::new(0, OrderType::Ascending)],
        all_column_ids.clone(),
        2,
    )
    .boxed()
    .execute();

    // 1.
    // Test insertion
    //

    // Add some data using `InsertExecutor`, assuming we are inserting into the "mv"
    let chunk = DataChunk::from_pretty(
        "F
         1.14
         5.14",
    );
    let insert_inner: BoxedExecutor = Box::new(SingleChunkExecutor::new(chunk, all_schema.clone()));
    let insert = Box::new(InsertExecutor::new(
        source_table_id,
        source_manager.clone(),
        insert_inner,
        "InsertExecutor".to_string(),
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
    let table = StorageTable::for_test(
        memory_state_store.clone(),
        source_table_id,
        column_descs.clone(),
        vec![OrderType::Ascending],
        vec![0],
    );

    let scan = Box::new(RowSeqScanExecutor::new(
        table.schema().clone(),
        vec![ScanType::BatchScan(
            table
                .batch_iter(HummockReadEpoch::Committed(u64::MAX))
                .await
                .map_err(Into::<StorageError>::into)?,
        )],
        1024,
        "RowSeqExecutor2".to_string(),
        None,
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
            let col_row_id = c.columns()[0].array_ref().as_int64();
            col_row_ids.push(col_row_id.value_at(0).unwrap());
            col_row_ids.push(col_row_id.value_at(1).unwrap());

            let col_data = c.columns()[1].array_ref().as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
            assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());
        }
        Message::Barrier(_) => panic!(),
    }

    // Send a barrier and poll again, should write changes to storage
    let curr_epoch = 1920;
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
    let scan = Box::new(RowSeqScanExecutor::new(
        table.schema().clone(),
        vec![ScanType::BatchScan(
            table
                .batch_iter(HummockReadEpoch::Committed(u64::MAX))
                .await
                .map_err(Into::<StorageError>::into)?,
        )],
        1024,
        "RowSeqScanExecutor2".to_string(),
        None,
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();

    let col_data = result.columns()[1].array_ref().as_float64();
    assert_eq!(col_data.len(), 2);
    assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
    assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

    // 2.
    // Test deletion
    //

    // Delete some data using `DeleteExecutor`, assuming we are inserting into the "mv"
    let columns = vec![
        column_nonnull! { I64Array, [ col_row_ids[0]] }, // row id column
        column_nonnull! { F64Array, [1.14] },
    ];
    let chunk = DataChunk::new(columns.clone(), 1);
    let delete_inner: BoxedExecutor = Box::new(SingleChunkExecutor::new(chunk, all_schema.clone()));
    let delete = Box::new(DeleteExecutor::new(
        source_table_id,
        source_manager.clone(),
        delete_inner,
        "DeleteExecutor".to_string(),
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
            let col_row_id = c.columns()[0].array_ref().as_int64();
            assert_eq!(col_row_id.value_at(0).unwrap(), col_row_ids[0]);

            let col_data = c.columns()[1].array_ref().as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
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
    let scan = Box::new(RowSeqScanExecutor::new(
        table.schema().clone(),
        vec![ScanType::BatchScan(
            table
                .batch_iter(HummockReadEpoch::Committed(u64::MAX))
                .await
                .map_err(Into::<StorageError>::into)?,
        )],
        1024,
        "RowSeqScanExecutor2".to_string(),
        None,
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();
    let col_data = result.columns()[1].array_ref().as_float64();
    assert_eq!(col_data.len(), 1);
    assert_eq!(col_data.value_at(0).unwrap(), 5.14.into_ordered());

    Ok(())
}

#[tokio::test]
async fn test_row_seq_scan() -> Result<()> {
    // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
    let memory_state_store = MemoryStateStore::new();

    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int32), // pk
        Field::unnamed(DataType::Int32),
        Field::unnamed(DataType::Int64),
    ]);
    let _column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), schema[2].data_type.clone()),
    ];

    let mut state = StateTable::new_without_distribution(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::Ascending],
        vec![0_usize],
    );
    let table = StorageTable::for_test(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::Ascending],
        vec![0],
    );

    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);
    epoch.inc();
    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(4_i32.into()),
        Some(7_i64.into()),
    ]));
    state.insert(Row(vec![
        Some(2_i32.into()),
        Some(5_i32.into()),
        Some(8_i64.into()),
    ]));
    state.commit_for_test(epoch.inc()).await.unwrap();

    let executor = Box::new(RowSeqScanExecutor::new(
        table.schema().clone(),
        vec![ScanType::BatchScan(
            table
                .batch_iter(HummockReadEpoch::Committed(u64::MAX))
                .await
                .unwrap(),
        )],
        1,
        "RowSeqScanExecutor2".to_string(),
        None,
    ));

    assert_eq!(executor.schema().fields().len(), 3);

    let mut stream = executor.execute();
    let res_chunk = stream.next().await.unwrap().unwrap();

    assert_eq!(res_chunk.dimension(), 3);
    assert_eq!(
        res_chunk
            .column_at(0)
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1)]
    );
    assert_eq!(
        res_chunk
            .column_at(1)
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4)]
    );

    let res_chunk2 = stream.next().await.unwrap().unwrap();
    assert_eq!(res_chunk2.dimension(), 3);
    assert_eq!(
        res_chunk2
            .column_at(0)
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2)]
    );
    assert_eq!(
        res_chunk2
            .column_at(1)
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(5)]
    );
    Ok(())
}
