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
//
use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::array::{I32Array, Op};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::column_nonnull;
use risingwave_common::types::deserialize_datum_from;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;

use crate::executor::lookup::impl_::LookupExecutorParams;
use crate::executor::test_utils::*;
use crate::executor::*;

fn arrangement_col_descs() -> Vec<ColumnDesc> {
    vec![
        ColumnDesc {
            data_type: DataType::Int32,
            column_id: ColumnId::new(1),
            name: "rowid_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
        ColumnDesc {
            data_type: DataType::Int32,
            column_id: ColumnId::new(2),
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

/// Create a test arrangement.
///
/// In this arrangement, there are two columns, with the following data flow:
///
/// | op | rowid | join |  epoch  |
/// | -- | ----- | ---- | ------- |
/// | b  |       |      | 0 -> 1  |
/// | +  | 2331  | 4    | 1       |
/// | +  | 2332  | 5    | 1       |
/// | +  | 2333  | 6    | 1       |
/// | +  | 2334  | 6    | 1       |
/// | b  |       |      | 1 -> 2  |
/// | +  | 2335  | 6    | 2       |
/// | +  | 2337  | 8    | 2       |
/// | -  | 2333  | 6    | 2       |
/// | b  |       |      | 2 -> 3  |
async fn create_arrangement(
    table_id: TableId,
    memory_state_store: MemoryStateStore,
) -> Box<dyn Executor + Send> {
    // Two columns of int32 type, the second column is arrange key.
    let columns = arrangement_col_descs();

    let column_ids = columns.iter().map(|c| c.column_id).collect_vec();

    // Prepare source chunks.
    let chunk1 = StreamChunk::new(
        vec![Op::Insert, Op::Insert, Op::Insert, Op::Insert],
        vec![
            column_nonnull! { I32Array, [2331, 2332, 2333, 2334] },
            column_nonnull! { I32Array, [4, 5, 6, 6] },
        ],
        None,
    );

    let chunk2 = StreamChunk::new(
        vec![Op::Insert, Op::Insert, Op::Delete],
        vec![
            column_nonnull! { I32Array, [2335, 2337, 2333] },
            column_nonnull! { I32Array, [6, 8, 6] },
        ],
        None,
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
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ],
    );

    let keyspace = Keyspace::table_root(memory_state_store, &table_id);

    Box::new(MaterializeExecutor::new(
        Box::new(source),
        keyspace,
        arrangement_col_arrange_rules(),
        column_ids,
        1,
        "ArrangeExecutor".to_string(),
    ))
}

/// Create a test source.
///
/// In this arrangement, there are two columns, with the following data flow:
///
/// | op | join | rowid |  epoch  |
/// | -- | ----- | ---- | ------- |
/// | b  |       |      | 0 -> 1  |
/// | +  | 6     | 1    | 1       |
/// | b  |       |      | 1 -> 2  |
/// | -  | 6     | 1    | 2       |
/// | b  |       |      | 2 -> 3  |
async fn create_source() -> Box<dyn Executor + Send> {
    let columns = vec![
        ColumnDesc {
            data_type: DataType::Int32,
            column_id: ColumnId::new(1),
            name: "join_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
        ColumnDesc {
            data_type: DataType::Int32,
            column_id: ColumnId::new(2),
            name: "rowid_column".to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        },
    ];

    // Prepare source chunks.
    let chunk1 = StreamChunk::new(
        vec![Op::Insert],
        vec![
            column_nonnull! { I32Array, [6] },
            column_nonnull! { I32Array, [1] },
        ],
        None,
    );

    let chunk2 = StreamChunk::new(
        vec![Op::Delete],
        vec![
            column_nonnull! { I32Array, [6] },
            column_nonnull! { I32Array, [1] },
        ],
        None,
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
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ],
    );

    Box::new(source)
}

async fn next_msg(buffer: &mut Vec<Message>, executor: &mut dyn Executor) {
    buffer.push(executor.next().await.unwrap());
}

fn check_chunk_eq(chunk1: &StreamChunk, chunk2: &StreamChunk) {
    assert_eq!(format!("{:?}", chunk1), format!("{:?}", chunk2));
}

#[tokio::test]
async fn test_lookup_this_epoch() {
    // TODO: memory state store doesn't support read epoch yet, so it is possible that this test
    // fails because read epoch doesn't take effect in memory state store.
    let store = MemoryStateStore::new();
    let table_id = TableId::new(1);
    let arrangement = create_arrangement(table_id, store.clone()).await;
    let stream = create_source().await;
    let mut lookup_executor = LookupExecutor::new(LookupExecutorParams {
        arrangement,
        stream,
        arrangement_keyspace: Keyspace::table_root(store.clone(), &table_id),
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules(),
        pk_indices: vec![1, 2],
        use_current_epoch: true,
        stream_join_key_indices: vec![1],
        arrange_join_key_indices: vec![0],
    });

    let mut msgs = vec![];
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;
    next_msg(&mut msgs, &mut lookup_executor).await;

    for (k, v) in store.scan::<_, Vec<u8>>(.., None, u64::MAX).await.unwrap() {
        let mut deserializer = memcomparable::Deserializer::new(&v[..]);
        println!(
            "{:?} => {:?}",
            k,
            deserialize_datum_from(&DataType::Int32, &mut deserializer).unwrap()
        );
    }

    println!("{:#?}", msgs);

    assert_eq!(msgs.len(), 5);
    assert_matches!(msgs[0], Message::Barrier(_));
    assert_matches!(msgs[2], Message::Barrier(_));
    assert_matches!(msgs[4], Message::Barrier(_));

    let chunk1 = msgs[1].as_chunk().unwrap();
    let expected_chunk1 = StreamChunk::new(
        vec![Op::Insert, Op::Insert],
        vec![
            column_nonnull! { I32Array, [6, 6] },
            column_nonnull! { I32Array, [1, 1] },
            column_nonnull! { I32Array, [2333, 2334] },
            column_nonnull! { I32Array, [6, 6] },
        ],
        None,
    );

    check_chunk_eq(chunk1, &expected_chunk1);

    let chunk2 = msgs[3].as_chunk().unwrap();
    let expected_chunk2 = StreamChunk::new(
        vec![Op::Delete, Op::Delete],
        vec![
            column_nonnull! { I32Array, [6, 6] },
            column_nonnull! { I32Array, [1, 1] },
            column_nonnull! { I32Array, [2334, 2335] },
            column_nonnull! { I32Array, [6, 6] },
        ],
        None,
    );
    check_chunk_eq(chunk2, &expected_chunk2);
}

#[tokio::test]
async fn test_lookup_last_epoch() {
    // TODO: memory state store doesn't support read epoch yet, so this test won't pass for now.
    // Will fix later.
    let store = MemoryStateStore::new();
    let table_id = TableId::new(1);
    let arrangement = create_arrangement(table_id, store.clone()).await;
    let stream = create_source().await;
    let mut lookup_executor = LookupExecutor::new(LookupExecutorParams {
        arrangement,
        stream,
        arrangement_keyspace: Keyspace::table_root(store.clone(), &table_id),
        arrangement_col_descs: arrangement_col_descs(),
        arrangement_order_rules: arrangement_col_arrange_rules(),
        pk_indices: vec![1, 2],
        use_current_epoch: false,
        stream_join_key_indices: vec![1],
        arrange_join_key_indices: vec![0],
    });

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
    let expected_chunk2 = StreamChunk::new(
        vec![Op::Delete, Op::Delete],
        vec![
            column_nonnull! { I32Array, [6, 6] },
            column_nonnull! { I32Array, [1, 1] },
            column_nonnull! { I32Array, [2333, 2334] },
            column_nonnull! { I32Array, [6, 6] },
        ],
        None,
    );
    check_chunk_eq(chunk2, &expected_chunk2);
}
