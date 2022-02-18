use std::sync::Arc;

use risingwave_batch::executor::{
    CreateTableExecutor, Executor as BatchExecutor, InsertExecutor, RowSeqScanExecutor,
};
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, DataChunk, F64Array};
use risingwave_common::array_nonnull;
use risingwave_common::catalog::{Field, Schema, SchemaId, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::IntoOrdered;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan::ColumnDesc;
use risingwave_source::{MemSourceManager, SourceManager};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::{SimpleTableManager, TableManager};
use risingwave_storage::{Keyspace, StateStoreImpl};
use risingwave_stream::executor::{
    Barrier, Executor as StreamExecutor, MaterializeExecutor, Message, PkIndices,
    StreamSourceExecutor,
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

/// This test checks whether batch task and streaming task work together for `TableV2` creation and
/// materialization.
#[tokio::test]
async fn test_table_v2_materialize() -> Result<()> {
    let memory_state_store = MemoryStateStore::new();
    let store = StateStoreImpl::MemoryStateStore(memory_state_store.clone());
    let source_manager = Arc::new(MemSourceManager::new());
    let table_manager = Arc::new(SimpleTableManager::new(store));
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
        source_table_id.clone(),
        table_manager.clone(),
        source_manager.clone(),
        table_columns,
        "CreateTableExecutor".to_string(),
    );
    // Execute
    create_table.open().await?;
    create_table.next().await?;
    create_table.close().await?;

    // Ensure the source exists
    let source_desc = source_manager.get_source(&source_table_id)?;
    let get_schema = |column_ids: &[i32]| {
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::unnamed(column_desc.data_type));
        }
        Schema::new(fields)
    };

    // Register associated materialized view
    let mview_id = TableId::new(SchemaId::default(), 1);
    table_manager.register_associated_materialized_view(&source_table_id, &mview_id)?;
    source_manager.register_associated_materialized_view(&source_table_id, &mview_id)?;

    // Create a `StreamSourceExecutor` to read the changes
    let all_column_ids = vec![0, 1];
    let all_schema = get_schema(&all_column_ids);
    let (barrier_tx, barrier_rx) = unbounded_channel();
    let stream_source = StreamSourceExecutor::new(
        source_table_id.clone(),
        source_desc.clone(),
        all_column_ids.clone(),
        all_schema.clone(),
        PkIndices::from([1]),
        barrier_rx,
        1,
        1,
        "StreamSourceExecutor".to_string(),
    )?;

    // Create a `Materialize` to write the changes to storage
    let keyspace = Keyspace::table_root(memory_state_store, &source_table_id);
    let mut materialize = MaterializeExecutor::new(
        Box::new(stream_source),
        keyspace.clone(),
        all_schema.clone(),
        vec![1],
        vec![OrderType::Ascending],
        2,
        "MaterializeExecutor".to_string(),
    );

    // Add some data using `InsertExecutor`, assuming we are inserting into the "mv"
    let columns = vec![Column::new(Arc::new(
        array_nonnull! { F64Array, [1.14, 5.14] }.into(),
    ))];
    let chunk = DataChunk::builder().columns(columns.clone()).build();
    let insert_inner = SingleChunkExecutor::new(chunk, all_schema);
    let mut insert = InsertExecutor::new(
        mview_id.clone(),
        source_manager.clone(),
        Box::new(insert_inner),
        0,
    );

    insert.open().await?;
    insert.next().await?;
    insert.close().await?;

    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = table_manager.get_table(&mview_id)?;
    let data_column_ids = vec![0];

    let mut scan = RowSeqScanExecutor::new(
        table.clone(),
        data_column_ids.clone(),
        1024,
        true,
        "RowSeqExecutor".to_string(),
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
        .send(Message::Barrier(Barrier::new(1919)))
        .unwrap();

    assert!(matches!(
        materialize.next().await?,
        Message::Barrier(Barrier { epoch: 1919, .. })
    ));

    // Scan the table again, we are able to get the data now!
    let mut scan = RowSeqScanExecutor::new(
        table.clone(),
        data_column_ids.clone(),
        1024,
        true,
        "RowSeqScanExecutor".to_string(),
    );
    scan.open().await?;
    let c = scan.next().await?.unwrap();
    let col_data = c.columns()[0].array_ref().as_float64();
    assert_eq!(col_data.len(), 2);
    assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
    assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

    Ok(())
}
