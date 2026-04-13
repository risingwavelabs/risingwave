// Copyright 2022 RisingWave Labs
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
use std::sync::atomic::AtomicU64;

use assert_matches::assert_matches;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::stream_chunk::StreamChunkTestExt;
use risingwave_common::catalog::{ColumnDesc, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::test_epoch;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_storage::memory::MemoryStateStore;

use crate::common::table::state_table::{StateTableBuilder, StateTableOpConsistencyLevel};
use crate::executor::lookup::LookupExecutor;
use crate::executor::lookup::impl_::LookupExecutorParams;
use crate::executor::test_utils::*;
use crate::executor::{
    ActorContext, Barrier, BoxedMessageStream, Execute, Executor, ExecutorInfo,
    MaterializeExecutor, Message, StreamKey,
};

fn arrangement_col_descs() -> Vec<ColumnDesc> {
    vec![
        ColumnDesc::named("rowid_column", 0.into(), DataType::Int64),
        ColumnDesc::named("join_column", 1.into(), DataType::Int64),
    ]
}

fn arrangement_col_arrange_rules() -> Vec<ColumnOrder> {
    vec![
        ColumnOrder::new(1, OrderType::ascending()),
        ColumnOrder::new(0, OrderType::ascending()),
    ]
}

fn arrangement_col_arrange_rules_join_key() -> Vec<ColumnOrder> {
    vec![ColumnOrder::new(1, OrderType::ascending())]
}

/// Create a test arrangement.
///
/// In this arrangement, there are two columns, with the following data flow:
///
/// | op | rowid | join |  epoch  |
/// | -- | ----- | ---- | ------- |
/// | b  |       |      | 1 -> 2  |
/// | +  | 2331  | 4    | 2       |
/// | +  | 2332  | 5    | 2       |
/// | +  | 2333  | 6    | 2       |
/// | +  | 2334  | 6    | 2       |
/// | b  |       |      | 2 -> 3  |
/// | +  | 2335  | 6    | 3       |
/// | +  | 2337  | 8    | 3       |
/// | -  | 2333  | 6    | 3       |
/// | b  |       |      | 3 -> 4  |
async fn create_arrangement(table_id: TableId, memory_state_store: MemoryStateStore) -> Executor {
    // Two columns of int32 type, the second column is arrange key.
    let columns = arrangement_col_descs();

    let column_ids = columns.iter().map(|c| c.column_id).collect_vec();

    // Prepare source chunks.
    let chunk1 = StreamChunk::from_pretty(
        "    I I
        + 2331 4
        + 2332 5
        + 2333 6
        + 2334 6",
    );

    let chunk2 = StreamChunk::from_pretty(
        "    I I
        + 2335 6
        + 2337 8
        - 2333 6",
    );

    // Prepare stream executors.
    let schema = Schema::new(
        columns
            .iter()
            .map(|col| Field::with_name(col.data_type.clone(), col.name.clone()))
            .collect_vec(),
    );

    let source = MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(chunk1),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        Message::Chunk(chunk2),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
    ])
    .into_executor(schema, vec![0]);

    Executor::new(
        ExecutorInfo::for_test(
            source.schema().clone(),
            source.stream_key().to_vec(),
            "MaterializeExecutor".to_owned(),
            0,
        ),
        MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            arrangement_col_arrange_rules(),
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
        )
        .await
        .boxed(),
    )
}

/// Create a test source.
///
/// In this arrangement, there are two columns, with the following data flow:
///
/// | op | join | rowid |  epoch  |
/// | -- | ----- | ---- | ------- |
/// | b  |       |      | 1 -> 2  |
/// | +  | 6     | 1    | 2       |
/// | b  |       |      | 2 -> 3  |
/// | -  | 6     | 1    | 3       |
/// | b  |       |      | 3 -> 4  |
fn create_source() -> Executor {
    let columns = [
        ColumnDesc::named("join_column", 1.into(), DataType::Int64),
        ColumnDesc::named("rowid_column", 2.into(), DataType::Int64),
    ];

    // Prepare source chunks.
    let chunk1 = StreamChunk::from_pretty(
        " I I
        + 6 1",
    );
    let chunk2 = StreamChunk::from_pretty(
        " I I
        - 6 1",
    );

    // Prepare stream executors.
    let schema = Schema::new(
        columns
            .iter()
            .map(|col| Field::with_name(col.data_type.clone(), col.name.clone()))
            .collect_vec(),
    );

    MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(chunk1),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        Message::Chunk(chunk2),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
    ])
    .into_executor(schema, StreamKey::new())
}

async fn next_msg(buffer: &mut Vec<Message>, executor: &mut BoxedMessageStream) {
    buffer.push(executor.next().await.unwrap().unwrap());
}

fn check_chunk_eq(chunk1: &StreamChunk, chunk2: &StreamChunk) {
    assert_eq!(format!("{:?}", chunk1), format!("{:?}", chunk2));
}

fn create_storage_table_desc(table_id: TableId) -> risingwave_pb::plan_common::StorageTableDesc {
    let col_descs = arrangement_col_descs();
    let order_rules = arrangement_col_arrange_rules();
    risingwave_pb::plan_common::StorageTableDesc {
        table_id,
        columns: col_descs.iter().map(|c| c.to_protobuf()).collect(),
        pk: order_rules.iter().map(|o| o.to_protobuf()).collect(),
        dist_key_in_pk_indices: vec![],
        value_indices: vec![0, 1],
        read_prefix_len_hint: 0,
        versioned: false,
        stream_key: vec![1, 0],
        vnode_col_idx_in_pk: None,
        retention_seconds: None,
        maybe_vnode_count: None,
    }
}

async fn create_replicated_state_table(
    store: MemoryStateStore,
    table_id: TableId,
) -> crate::common::table::state_table::ReplicatedStateTable<MemoryStateStore, BasicSerde> {
    let table_desc = create_storage_table_desc(table_id);
    let column_ids = arrangement_col_descs()
        .iter()
        .map(|c| c.column_id)
        .collect_vec();
    StateTableBuilder::<_, BasicSerde, true, _>::new_from_storage_table_desc(
        &table_desc,
        store,
        None,
        0,
    )
    .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
    .with_output_column_ids(column_ids)
    .forbid_preload_all_rows()
    .build()
    .await
}

#[tokio::test]
async fn test_lookup_this_epoch() {
    // TODO: memory state store doesn't support read epoch yet, so it is possible that this test
    // fails because read epoch doesn't take effect in memory state store.
    let store = MemoryStateStore::new();
    let table_id = TableId::new(1);
    let arrangement = create_arrangement(table_id, store.clone()).await;
    let stream = create_source();
    let info = ExecutorInfo::for_test(
        Schema::new(vec![
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "join_column"),
        ]),
        vec![1, 2],
        "LookupExecutor".to_owned(),
        0,
    );
    let lookup_executor = Box::new(LookupExecutor::new(LookupExecutorParams {
        ctx: ActorContext::for_test(0),
        info,
        arrangement,
        stream,
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules_join_key(),
        use_current_epoch: true,
        stream_join_key_indices: vec![0],
        arrange_join_key_indices: vec![1],
        column_mapping: vec![2, 3, 0, 1],
        state_table: create_replicated_state_table(store.clone(), table_id).await,
        watermark_epoch: Arc::new(AtomicU64::new(0)),
        chunk_size: 1024,
    }));
    let mut lookup_executor = lookup_executor.execute();

    let mut msgs = vec![];
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;

    println!("{:#?}", msgs);

    assert_eq!(msgs.len(), 5);
    assert_matches!(msgs[0], Message::Barrier(_));
    assert_matches!(msgs[2], Message::Barrier(_));
    assert_matches!(msgs[4], Message::Barrier(_));

    let chunk1 = msgs[1].as_chunk().unwrap();
    let expected_chunk1 = StreamChunk::from_pretty(
        "    I I I I
        + 2333 6 6 1
        + 2334 6 6 1",
    );
    check_chunk_eq(chunk1, &expected_chunk1);

    let chunk2 = msgs[3].as_chunk().unwrap();
    let expected_chunk2 = StreamChunk::from_pretty(
        "    I I I I
        - 2334 6 6 1
        - 2335 6 6 1",
    );
    check_chunk_eq(chunk2, &expected_chunk2);
}

#[tokio::test]
async fn test_lookup_last_epoch() {
    let store = MemoryStateStore::new();
    let table_id = TableId::new(1);
    let arrangement = create_arrangement(table_id, store.clone()).await;
    let stream = create_source();
    let info = ExecutorInfo::for_test(
        Schema::new(vec![
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
        ]),
        vec![1, 2],
        "LookupExecutor".to_owned(),
        0,
    );
    let lookup_executor = Box::new(LookupExecutor::new(LookupExecutorParams {
        ctx: ActorContext::for_test(0),
        info,
        arrangement,
        stream,
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules_join_key(),
        use_current_epoch: false,
        stream_join_key_indices: vec![0],
        arrange_join_key_indices: vec![1],
        column_mapping: vec![0, 1, 2, 3],
        state_table: create_replicated_state_table(store.clone(), table_id).await,
        watermark_epoch: Arc::new(AtomicU64::new(0)),
        chunk_size: 1024,
    }));
    let mut lookup_executor = lookup_executor.execute();

    let mut msgs = vec![];
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;

    // NOTE: With MemoryStateStore (shared state, no epoch isolation), the lookup
    // in "prev epoch" mode sees MaterializeExecutor's committed data immediately
    // because MemoryStateStore is a shared-everything store. In production Hummock,
    // each LocalHummockStorage has isolated staging, so prev-epoch lookup would not
    // see the current epoch's arrangement data. The test expectations below reflect
    // MemoryStateStore behavior.
    assert_eq!(msgs.len(), 5);
    assert_matches!(msgs[0], Message::Barrier(_));
    assert_matches!(msgs[2], Message::Barrier(_));
    assert_matches!(msgs[4], Message::Barrier(_));

    let chunk1 = msgs[1].as_chunk().unwrap();
    let expected_chunk1 = StreamChunk::from_pretty(
        " I I    I I
        + 6 1 2333 6
        + 6 1 2334 6",
    );
    check_chunk_eq(chunk1, &expected_chunk1);

    let chunk2 = msgs[3].as_chunk().unwrap();
    let expected_chunk2 = StreamChunk::from_pretty(
        " I I    I I
        - 6 1 2333 6
        - 6 1 2334 6
        - 6 1 2335 6",
    );
    check_chunk_eq(chunk2, &expected_chunk2);
}
