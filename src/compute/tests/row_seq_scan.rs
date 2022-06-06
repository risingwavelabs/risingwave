use std::sync::Arc;

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
use futures::StreamExt;
use risingwave_batch::executor::monitor::BatchMetrics;
use risingwave_batch::executor::{Executor, RowSeqScanExecutor};
use risingwave_common::array::{Array, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::Keyspace;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_row_seq_scan() -> Result<()> {
    // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
    let memory_state_store = MemoryStateStore::new();
    let keyspace = Keyspace::executor_root(memory_state_store.clone(), 0x42);

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

    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        vec![OrderType::Ascending],
        None,
        vec![0_usize],
    );
    let table = CellBasedTable::new_adhoc(
        keyspace,
        column_descs,
        Arc::new(StateStoreMetrics::unused()),
    );

    let epoch: u64 = 0;

    state
        .insert(
            &Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(4_i32.into()),
                Some(7_i64.into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            &Row(vec![Some(2_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(5_i32.into()),
                Some(8_i64.into()),
            ]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let executor = Box::new(RowSeqScanExecutor::new(
        table.schema().clone(),
        table.iter(u64::MAX).await.unwrap(),
        1,
        true,
        "RowSeqScanExecutor2".to_string(),
        Arc::new(BatchMetrics::unused()),
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
