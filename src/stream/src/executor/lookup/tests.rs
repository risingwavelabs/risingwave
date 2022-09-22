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

use assert_matches::assert_matches;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::StreamChunkTestExt;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::lookup::impl_::LookupExecutorParams;
use crate::executor::lookup::LookupExecutor;
use crate::executor::test_utils::*;
use crate::executor::{
    Barrier, BoxedMessageStream, Executor, MaterializeExecutor, Message, PkIndices,
};

fn arrangement_col_descs() -> Vec<ColumnDesc> {
    vec![
        ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(0),
            name: "rowid_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
        ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(1),
            name: "join_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
    ]
}

fn arrangement_col_arrange_rules() -> Vec<OrderPair> {
    vec![
        OrderPair::new(1, OrderType::Ascending),
        OrderPair::new(0, OrderType::Ascending),
    ]
}

fn arrangement_col_arrange_rules_join_key() -> Vec<OrderPair> {
    vec![OrderPair::new(1, OrderType::Ascending)]
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
fn create_arrangement(
    table_id: TableId,
    memory_state_store: MemoryStateStore,
) -> Box<dyn Executor + Send> {
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

    let source = MockSource::with_messages(
        schema,
        vec![0],
        vec![
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(3)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(4)),
        ],
    );

    Box::new(MaterializeExecutor::for_test(
        Box::new(source),
        memory_state_store,
        table_id,
        arrangement_col_arrange_rules(),
        column_ids,
        1,
    ))
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
fn create_source() -> Box<dyn Executor + Send> {
    let columns = vec![
        ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(1),
            name: "join_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
        ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(2),
            name: "rowid_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
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

    let source = MockSource::with_messages(
        schema,
        PkIndices::new(),
        vec![
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(3)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(4)),
        ],
    );

    Box::new(source)
}

async fn next_msg(buffer: &mut Vec<Message>, executor: &mut BoxedMessageStream) {
    buffer.push(executor.next().await.unwrap().unwrap());
}

fn check_chunk_eq(chunk1: &StreamChunk, chunk2: &StreamChunk) {
    assert_eq!(format!("{:?}", chunk1), format!("{:?}", chunk2));
}

fn build_state_table_helper<S: StateStore>(
    s: S,
    table_id: TableId,
    columns: Vec<ColumnDesc>,
    order_types: Vec<OrderPair>,
    pk_indices: Vec<usize>,
) -> StateTable<S> {
    StateTable::new_without_distribution(
        s,
        table_id,
        columns,
        order_types.iter().map(|pair| pair.order_type).collect_vec(),
        pk_indices,
    )
}
#[tokio::test]
async fn test_lookup_this_epoch() {
    // TODO: memory state store doesn't support read epoch yet, so it is possible that this test
    // fails because read epoch doesn't take effect in memory state store.
    let store = MemoryStateStore::new();
    let table_id = TableId::new(1);
    let arrangement = create_arrangement(table_id, store.clone());
    let stream = create_source();
    let lookup_executor = Box::new(LookupExecutor::new(LookupExecutorParams {
        arrangement,
        stream,
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules_join_key(),
        pk_indices: vec![1, 2],
        use_current_epoch: true,
        stream_join_key_indices: vec![0],
        arrange_join_key_indices: vec![1],
        column_mapping: vec![2, 3, 0, 1],
        schema: Schema::new(vec![
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "join_column"),
        ]),
        state_table: build_state_table_helper(
            store.clone(),
            table_id,
            arrangement_col_descs(),
            arrangement_col_arrange_rules(),
            vec![1, 0],
        ),
        cache_size: 1 << 16,
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
    let arrangement = create_arrangement(table_id, store.clone());
    let stream = create_source();
    let lookup_executor = Box::new(LookupExecutor::new(LookupExecutorParams {
        arrangement,
        stream,
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules_join_key(),
        pk_indices: vec![1, 2],
        use_current_epoch: false,
        stream_join_key_indices: vec![0],
        arrange_join_key_indices: vec![1],
        column_mapping: vec![0, 1, 2, 3],
        schema: Schema::new(vec![
            Field::with_name(DataType::Int64, "rowid_column"),
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "join_column"),
            Field::with_name(DataType::Int64, "rowid_column"),
        ]),
        state_table: build_state_table_helper(
            store.clone(),
            table_id,
            arrangement_col_descs(),
            arrangement_col_arrange_rules(),
            vec![1, 0],
        ),
        cache_size: 1 << 16,
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
    // the arrangement of epoch 0 is not ready yet, should be empty.
    assert_eq!(chunk1.cardinality(), 0);

    let chunk2 = msgs[3].as_chunk().unwrap();
    let expected_chunk2 = StreamChunk::from_pretty(
        " I I    I I
        - 6 1 2333 6
        - 6 1 2334 6",
    );
    check_chunk_eq(chunk2, &expected_chunk2);
}
