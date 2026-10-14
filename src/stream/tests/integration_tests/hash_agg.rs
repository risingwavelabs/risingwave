// Copyright 2023 RisingWave Labs
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

use risingwave_common::util::epoch::test_epoch;
use risingwave_expr::aggregate::AggCall;
use risingwave_stream::executor::test_utils::agg_executor::{
    new_boxed_hash_agg_executor, new_boxed_hash_agg_executor_with_max_dirty_groups_heap_size,
};

use crate::prelude::*;

#[tokio::test]
async fn test_hash_agg_count_sum() {
    let store = MemoryStateStore::new();

    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ],
    };

    // This is local hash aggregation, so we add another sum state
    let key_indices = vec![0];
    let agg_calls = vec![
        AggCall::from_pretty("(count:int8)"),
        AggCall::from_pretty("(sum:int8 $1:int8)"),
        // This is local hash aggregation, so we add another sum state
        AggCall::from_pretty("(sum:int8 $2:int8)"),
    ];

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(schema, StreamKey::new());
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        source,
        false,
        agg_calls,
        0,
        key_indices,
        vec![],
        1 << 10,
        false,
        1,
    )
    .await;
    let mut hash_agg = hash_agg.execute();

    tx.push_barrier(test_epoch(1), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I I I
        + 1 1 1
        + 2 2 2
        + 2 2 2",
    ));
    tx.push_barrier(test_epoch(2), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I I I
        - 1 1 1
        - 2 2 2 D
        - 2 2 2
        + 3 3 3",
    ));
    tx.push_barrier(test_epoch(3), false);

    check_until_pending(
        &mut hash_agg,
        expect![[r#"
            - !barrier 1
            - !chunk |-
              +---+---+---+---+---+
              | + | 1 | 1 | 1 | 1 |
              | + | 2 | 2 | 4 | 4 |
              +---+---+---+---+---+
            - !barrier 2
            - !chunk |-
              +----+---+---+---+---+
              |  + | 3 | 1 | 3 | 3 |
              |  - | 1 | 1 | 1 | 1 |
              | U- | 2 | 2 | 4 | 4 |
              | U+ | 2 | 1 | 2 | 2 |
              +----+---+---+---+---+
            - !barrier 3
        "#]],
        SnapshotOptions::default().sort_chunk(true),
    );
}

#[tokio::test]
async fn test_hash_agg_min() {
    let store = MemoryStateStore::new();

    let schema = Schema {
        fields: vec![
            // group key column
            Field::unnamed(DataType::Int64),
            // data column to get minimum
            Field::unnamed(DataType::Int64),
            // primary key column
            Field::unnamed(DataType::Int64),
        ],
    };

    // This is local hash aggregation, so we add another row count state
    let keys = vec![0];
    let agg_calls = vec![
        AggCall::from_pretty("(count:int8)"),
        AggCall::from_pretty("(min:int8 $1:int8)"),
    ];

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(schema, vec![2]);
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        source,
        false,
        agg_calls,
        0,
        keys,
        vec![2],
        1 << 10,
        false,
        1,
    )
    .await;
    let mut hash_agg = hash_agg.execute();

    tx.push_barrier(test_epoch(1), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I     I    I
        + 1   233 1001
        + 1 23333 1002
        + 2  2333 1003",
    ));
    tx.push_barrier(test_epoch(2), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I     I    I
        - 1   233 1001
        - 1 23333 1002 D
        - 2  2333 1003",
    ));
    tx.push_barrier(test_epoch(3), false);

    check_until_pending(
        &mut hash_agg,
        expect![[r#"
            - !barrier 1
            - !chunk |-
              +---+---+---+------+
              | + | 1 | 2 | 233  |
              | + | 2 | 1 | 2333 |
              +---+---+---+------+
            - !barrier 2
            - !chunk |-
              +----+---+---+-------+
              |  - | 2 | 1 | 2333  |
              | U- | 1 | 2 | 233   |
              | U+ | 1 | 1 | 23333 |
              +----+---+---+-------+
            - !barrier 3
        "#]],
        SnapshotOptions::default().sort_chunk(true),
    );
}

#[tokio::test]
async fn test_hash_agg_min_append_only() {
    let store = MemoryStateStore::new();

    let schema = Schema {
        fields: vec![
            // group key column
            Field::unnamed(DataType::Int64),
            // data column to get minimum
            Field::unnamed(DataType::Int64),
            // primary key column
            Field::unnamed(DataType::Int64),
        ],
    };

    let keys = vec![0];
    let agg_calls = vec![
        AggCall::from_pretty("(count:int8)"),
        AggCall::from_pretty("(min:int8 $1:int8)"),
    ];

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(schema, vec![2]);
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        source,
        true, // is append only
        agg_calls,
        0,
        keys,
        vec![2],
        1 << 10,
        false,
        1,
    )
    .await;
    let mut hash_agg = hash_agg.execute();

    tx.push_barrier(test_epoch(1), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I  I  I
            + 2 5  1000
            + 1 15 1001
            + 1 8  1002
            + 2 5  1003
            + 2 10 1004
            ",
    ));
    tx.push_barrier(test_epoch(2), false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I  I  I
            + 1 20 1005
            + 1 1  1006
            + 2 10 1007
            + 2 20 1008
            ",
    ));
    tx.push_barrier(test_epoch(3), false);

    check_until_pending(
        &mut hash_agg,
        expect![[r#"
            - !barrier 1
            - !chunk |-
              +---+---+---+---+
              | + | 1 | 2 | 8 |
              | + | 2 | 3 | 5 |
              +---+---+---+---+
            - !barrier 2
            - !chunk |-
              +----+---+---+---+
              | U- | 1 | 2 | 8 |
              | U- | 2 | 3 | 5 |
              | U+ | 1 | 4 | 1 |
              | U+ | 2 | 5 | 5 |
              +----+---+---+---+
            - !barrier 3
        "#]],
        SnapshotOptions::default().sort_chunk(true),
    );
}

#[tokio::test]
async fn test_hash_agg_emit_on_window_close() {
    let store = MemoryStateStore::new();

    let input_schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Varchar), // to ensure correct group key column mapping
            Field::unnamed(DataType::Int64),   // window group key column
        ],
    };
    let input_window_col = 1;
    let group_key_indices = vec![input_window_col];
    let agg_calls = vec![AggCall::from_pretty("(count:int8)")];

    let create_executor = || async {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema.clone(), StreamKey::new());
        let hash_agg = new_boxed_hash_agg_executor(
            store.clone(),
            source,
            false,
            agg_calls.clone(),
            0,
            group_key_indices.clone(),
            vec![],
            1 << 10,
            true, // enable emit-on-window-close
            1,
        )
        .await;
        (tx, hash_agg.execute())
    };

    check_with_script(
        create_executor,
        &format!(
            r###"
            - !barrier 1
            - !chunk |2
                T I
                + _ 1
                + _ 2
                + _ 3
            - !barrier 2
            - !chunk |2
                T I
                - _ 2
                + _ 4
            - !watermark
                col_idx: {input_window_col}
                val: 3
            - !barrier 3
            - !watermark
                col_idx: {input_window_col}
                val: 4
            - !barrier 4
            - !watermark
                col_idx: {input_window_col}
                val: 10
            - !barrier 5
            - !watermark
                col_idx: {input_window_col}
                val: 20
            - !barrier 6
            "###
        ),
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+---+
                | + | _ | 1 |
                | + | _ | 2 |
                | + | _ | 3 |
                +---+---+---+
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+---+
                | - | _ | 2 |
                | + | _ | 4 |
                +---+---+---+
              output: []
            - input: !watermark
                col_idx: 1
                val: '3'
              output: []
            - input: !barrier 3
              output:
              - !chunk |-
                +---+---+---+
                | + | 1 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: '3'
              - !barrier 3
            - input: !watermark
                col_idx: 1
                val: '4'
              output: []
            - input: !barrier 4
              output:
              - !chunk |-
                +---+---+---+
                | + | 3 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: '4'
              - !barrier 4
            - input: !watermark
                col_idx: 1
                val: '10'
              output: []
            - input: !barrier 5
              output:
              - !chunk |-
                +---+---+---+
                | + | 4 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: '10'
              - !barrier 5
            - input: !watermark
                col_idx: 1
                val: '20'
              output: []
            - input: !barrier 6
              output:
              - !watermark
                col_idx: 0
                val: '20'
              - !barrier 6
        "#]],
        SnapshotOptions::default().sort_chunk(true),
    )
    .await;
}

/// Flushes the dirty groups after every chunk, so windows 1 and 2 are output by watermarks 2 and 3
/// within an epoch, which leaves only window 3 in the cache of the sort buffer. Window 3 is then
/// updated again before watermark 4, which empties the cache.
#[tokio::test]
async fn test_hash_agg_emit_on_window_close_with_mid_epoch_flushes() {
    use futures::StreamExt;
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_stream::executor::Message;

    let input_schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64), // window group key column
        ],
    };
    let input_window_col = 1;
    for barrier_before_last_update in [false, true] {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema.clone(), StreamKey::new());
        let hash_agg = new_boxed_hash_agg_executor_with_max_dirty_groups_heap_size(
            MemoryStateStore::new(),
            source,
            false,
            vec![AggCall::from_pretty("(count:int8)")],
            0,
            vec![input_window_col],
            vec![],
            1 << 10,
            true,
            1,
            0,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        let one_row = |window: i64| StreamChunk::from_pretty(&format!(" T I\n + _ {window}"));
        tx.push_barrier(test_epoch(1), false);
        tx.push_chunk(StreamChunk::from_pretty(" T I\n + _ 1\n + _ 2\n + _ 3"));
        tx.push_int64_watermark(input_window_col, 2);
        tx.push_chunk(one_row(3));
        tx.push_int64_watermark(input_window_col, 3);
        tx.push_chunk(one_row(3));
        let mut last_epoch = 2;
        if barrier_before_last_update {
            tx.push_barrier(test_epoch(2), false);
            last_epoch = 3;
        }
        tx.push_int64_watermark(input_window_col, 4);
        tx.push_chunk(one_row(3));
        tx.push_barrier(test_epoch(last_epoch), false);

        let mut outputs = vec![];
        let mut barriers = 0;
        while barriers < last_epoch {
            match hash_agg.next().await.unwrap().unwrap() {
                Message::Chunk(chunk) => outputs.extend(chunk.rows().map(|(op, row)| {
                    assert_eq!(op, Op::Insert);
                    (
                        row.datum_at(0).unwrap().into_int64(),
                        row.datum_at(1).unwrap().into_int64(),
                    )
                })),
                Message::Barrier(_) => barriers += 1,
                Message::Watermark(_) => {}
            }
        }
        assert_eq!(
            outputs,
            vec![(1, 1), (2, 1), (3, 4)],
            "barrier_before_last_update: {barrier_before_last_update}"
        );
    }
}
