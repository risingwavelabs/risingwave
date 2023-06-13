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

use risingwave_expr::agg::{AggArgs, AggCall, AggKind};
use risingwave_stream::executor::test_utils::agg_executor::new_boxed_hash_agg_executor;

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
        AggCall {
            kind: AggKind::Count, // as row count, index: 0
            args: AggArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(DataType::Int64, 1),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
        // This is local hash aggregation, so we add another sum state
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(DataType::Int64, 2),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
    ];

    let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        Box::new(source),
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

    tx.push_barrier(1, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I I I
        + 1 1 1
        + 2 2 2
        + 2 2 2",
    ));
    tx.push_barrier(2, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I I I
        - 1 1 1
        - 2 2 2 D
        - 2 2 2
        + 3 3 3",
    ));
    tx.push_barrier(3, false);

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
        SnapshotOptions { sort_chunk: true },
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
        AggCall {
            kind: AggKind::Count, // as row count, index: 0
            args: AggArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
        AggCall {
            kind: AggKind::Min,
            args: AggArgs::Unary(DataType::Int64, 1),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
    ];

    let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        Box::new(source),
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

    tx.push_barrier(1, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I     I    I
        + 1   233 1001
        + 1 23333 1002
        + 2  2333 1003",
    ));
    tx.push_barrier(2, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I     I    I
        - 1   233 1001
        - 1 23333 1002 D
        - 2  2333 1003",
    ));
    tx.push_barrier(3, false);

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
        SnapshotOptions { sort_chunk: true },
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
        AggCall {
            kind: AggKind::Count, // as row count, index: 0
            args: AggArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
        AggCall {
            kind: AggKind::Min,
            args: AggArgs::Unary(DataType::Int64, 1),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        },
    ];

    let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        Box::new(source),
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

    tx.push_barrier(1, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I  I  I
            + 2 5  1000
            + 1 15 1001
            + 1 8  1002
            + 2 5  1003
            + 2 10 1004
            ",
    ));
    tx.push_barrier(2, false);
    tx.push_chunk(StreamChunk::from_pretty(
        " I  I  I
            + 1 20 1005
            + 1 1  1006
            + 2 10 1007
            + 2 20 1008
            ",
    ));
    tx.push_barrier(3, false);

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
        SnapshotOptions { sort_chunk: true },
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
    let agg_calls = vec![AggCall {
        kind: AggKind::Count, // as row count, index: 0
        args: AggArgs::None,
        return_type: DataType::Int64,
        column_orders: vec![],
        filter: None,
        distinct: false,
        direct_args: vec![],
    }];

    let create_executor = || async {
        let (tx, source) = MockSource::channel(input_schema.clone(), PkIndices::new());
        let hash_agg = new_boxed_hash_agg_executor(
            store.clone(),
            Box::new(source),
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
        || create_executor(),
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
                val: 3
              output: []
            - input: !barrier 3
              output:
              - !chunk |-
                +---+---+---+
                | + | 1 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: 3
              - !barrier 3
            - input: !watermark
                col_idx: 1
                val: 4
              output: []
            - input: !barrier 4
              output:
              - !chunk |-
                +---+---+---+
                | + | 3 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: 4
              - !barrier 4
            - input: !watermark
                col_idx: 1
                val: 10
              output: []
            - input: !barrier 5
              output:
              - !chunk |-
                +---+---+---+
                | + | 4 | 1 |
                +---+---+---+
              - !watermark
                col_idx: 0
                val: 10
              - !barrier 5
            - input: !watermark
                col_idx: 1
                val: 20
              output: []
            - input: !barrier 6
              output:
              - !watermark
                col_idx: 0
                val: 20
              - !barrier 6
        "#]],
        SnapshotOptions { sort_chunk: true },
    )
    .await;
}
