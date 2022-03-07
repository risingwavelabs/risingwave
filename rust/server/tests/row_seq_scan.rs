use std::sync::Arc;

use itertools::Itertools;
use risingwave_batch::executor::{Executor, RowSeqScanExecutor};
use risingwave_common::array::{Array, Row};
use risingwave_common::catalog::{ColumnId, Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
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
        Field::unnamed(DataType::Int32), // pk
        Field::unnamed(DataType::Int32),
        Field::unnamed(DataType::Int64),
    ]);
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];

    let mut state =
        ManagedMViewState::new(keyspace.clone(), column_ids, vec![OrderType::Ascending]);

    let table = Arc::new(MViewTable::new_adhoc(
        keyspace.clone(),
        &[ColumnId::from(0), ColumnId::from(1)],
        &schema.fields().iter().take(2).cloned().collect_vec(),
    ));

    let mut executor =
        RowSeqScanExecutor::new(table, 1, true, "RowSeqScanExecutor".to_string(), u64::MAX);

    let epoch: u64 = 0;
    state.put(
        Row(vec![Some(1_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(4_i32.into()),
            Some(7_i64.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(5_i32.into()),
            Some(8_i64.into()),
        ]),
    );
    state.flush(epoch).await.unwrap();

    executor.open().await.unwrap();
    assert_eq!(executor.schema().fields().len(), 2);

    let res_chunk = executor.next().await?.unwrap();
    assert_eq!(res_chunk.dimension(), 2);
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

    let res_chunk2 = executor.next().await?.unwrap();
    assert_eq!(res_chunk2.dimension(), 2);
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
    executor.close().await.unwrap();
    Ok(())
}
