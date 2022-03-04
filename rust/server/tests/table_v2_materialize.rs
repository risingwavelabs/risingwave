use std::sync::Arc;

use risingwave_batch::executor::{
    CreateTableExecutor, Executor as BatchExecutor, InsertExecutor, RowSeqScanExecutor,
};
use risingwave_common::array::{Array, DataChunk, F64Array};
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::column_nonnull;
use risingwave_common::error::Result;
use risingwave_common::types::IntoOrdered;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan::create_table_node::Info;
use risingwave_pb::plan::ColumnDesc;
use risingwave_source::{MemSourceManager, SourceManager};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;
use risingwave_storage::table::SimpleTableManager;
use risingwave_storage::{Keyspace, StateStore, StateStoreImpl};
use risingwave_stream::executor::{
    new_adhoc_mview_table, Barrier, Epoch, Executor as StreamExecutor, MaterializeExecutor,
    Message, PkIndices, SourceExecutor,
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

/// This test checks whether batch task and streaming task work together for `TableV2` creation
/// and materialization.
#[tokio::test]
async fn test_table_v2_materialize() -> Result<()> {
    let memory_state_store = MemoryStateStore::new();
    let store = StateStoreImpl::MemoryStateStore(
        memory_state_store
            .clone()
            .monitored(DEFAULT_STATE_STORE_STATS.clone()),
    );
    let source_manager = Arc::new(MemSourceManager::new());
    let table_manager = Arc::new(SimpleTableManager::new(store.clone()));
    let source_table_id = TableId::default();
    let table_columns = vec![
        // data
        ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Double as i32,
                ..Default::default()
            }),
            column_id: 0,
            ..Default::default()
        },
        // row id
        ColumnDesc {
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
        table_manager.clone(),
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

    // Register associated materialized view
    let mview_id = TableId::new(1);
    // table_manager.register_associated_materialized_view(&source_table_id, &mview_id)?;
    source_manager.register_associated_materialized_view(&source_table_id, &mview_id)?;

    // Create a `SourceExecutor` to read the changes
    let all_column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
    let all_schema = get_schema(&all_column_ids);
    let (barrier_tx, barrier_rx) = unbounded_channel();
    let stream_source = SourceExecutor::new(
        source_table_id,
        source_desc.clone(),
        all_column_ids.clone(),
        all_schema.clone(),
        PkIndices::from([1]),
        barrier_rx,
        1,
        1,
        "SourceExecutor".to_string(),
    )?;

    // Create a `Materialize` to write the changes to storage
    let keyspace = Keyspace::table_root(memory_state_store, &source_table_id);
    let mut materialize = MaterializeExecutor::new(
        Box::new(stream_source),
        keyspace.clone(),
        vec![OrderPair::new(1, OrderType::Ascending)],
        all_column_ids.clone(),
        2,
        "MaterializeExecutor".to_string(),
    );

    // Add some data using `InsertExecutor`, assuming we are inserting into the "mv"
    let columns = vec![column_nonnull! { F64Array, [1.14, 5.14] }];
    let chunk = DataChunk::builder().columns(columns.clone()).build();
    let insert_inner = SingleChunkExecutor::new(chunk, all_schema.clone());
    let mut insert =
        InsertExecutor::new(mview_id, source_manager.clone(), Box::new(insert_inner), 0);

    insert.open().await?;
    insert.next().await?;
    insert.close().await?;

    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = new_adhoc_mview_table(
        store.clone(),
        &source_table_id,
        &all_column_ids,
        all_schema.fields(),
    );

    let mut scan = RowSeqScanExecutor::new(
        table.clone(),
        1024,
        true,
        "RowSeqExecutor".to_string(),
        u64::MAX,
    );
    scan.open().await?;
    assert!(scan.next().await?.is_none());

    // Poll `Materialize`, should output the same stream chunk
    let message = materialize.next().await?;
    match message {
        Message::Chunk(c) => {
            let col_data = c.columns()[0].array_ref().as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
            assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

            let col_row_id = c.columns()[1].array_ref().as_int64();
            assert_eq!(col_row_id.value_at(0).unwrap(), 0);
            assert_eq!(col_row_id.value_at(1).unwrap(), 1);
        }
        Message::Barrier(_) => panic!(),
    }

    // Send a barrier and poll again, should write changes to storage
    barrier_tx
        .send(Message::Barrier(Barrier::new_test_barrier(1919)))
        .unwrap();

    assert!(matches!(
        materialize.next().await?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch == Epoch::new_test_epoch(1919)
    ));

    // Scan the table again, we are able to get the data now!
    let mut scan = RowSeqScanExecutor::new(
        table.clone(),
        1024,
        true,
        "RowSeqScanExecutor".to_string(),
        u64::MAX,
    );
    scan.open().await?;
    let c = scan.next().await?.unwrap();
    let col_data = c.columns()[0].array_ref().as_float64();
    assert_eq!(col_data.len(), 2);
    assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
    assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

    Ok(())
}
