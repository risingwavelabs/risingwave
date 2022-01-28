use std::sync::Arc;

extern crate risingwave;
extern crate risingwave_batch;

use risingwave_batch::executor::{Executor, RowSeqScanExecutor};
use risingwave_common::array::{Array, Row};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataTypeKind;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;
use risingwave_stream::executor::{MViewTable, ManagedMViewState};

#[tokio::test]
async fn test_row_seq_scan() -> Result<()> {
    // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
    let state_store = MemoryStateStore::new();
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let schema = Schema::new(vec![
        Field::unnamed(DataTypeKind::Int32),
        Field::unnamed(DataTypeKind::Int32),
    ]);
    let pk_columns = vec![0];
    let orderings = vec![OrderType::Ascending];
    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );

    let table = Arc::new(MViewTable::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings,
    ));

    let mut executor = RowSeqScanExecutor::new(
        table,
        schema.fields.iter().map(|field| field.data_type).collect(),
        vec![0, 1],
        schema,
    );

    let epoch: u64 = 0;
    state.put(
        Row(vec![Some(1_i32.into())]),
        Row(vec![Some(1_i32.into()), Some(4_i32.into())]),
    );
    state.put(
        Row(vec![Some(2_i32.into())]),
        Row(vec![Some(2_i32.into()), Some(5_i32.into())]),
    );
    state.flush(epoch).await.unwrap();

    executor.open().await.unwrap();

    let res_chunk = executor.next().await?.unwrap();
    assert_eq!(res_chunk.dimension(), 2);
    assert_eq!(
        res_chunk
            .column_at(0)?
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1)]
    );
    assert_eq!(
        res_chunk
            .column_at(1)?
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4)]
    );

    let res_chunk2 = executor.next().await?.unwrap();
    assert_eq!(res_chunk2.dimension(), 2);
    assert_eq!(
        res_chunk2
            .column_at(0)?
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2)]
    );
    assert_eq!(
        res_chunk2
            .column_at(1)?
            .array()
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(5)]
    );
    executor.close().await.unwrap();
    Ok(())
}
