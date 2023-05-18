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

use risingwave_expr::agg::{AggArgs, AggKind};
use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncCall, WindowFuncKind};
use risingwave_stream::executor::{EowcOverWindowExecutor, EowcOverWindowExecutorArgs};

use crate::prelude::*;

async fn create_executor<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
) -> (MessageSender, BoxedMessageStream) {
    let input_schema = Schema::new(vec![
        Field::unnamed(DataType::Int64),   // order key
        Field::unnamed(DataType::Varchar), // partition key
        Field::unnamed(DataType::Int64),   // pk
        Field::unnamed(DataType::Int32),   // x
    ]);
    let input_pk_indices = vec![2];
    let partition_key_indices = vec![1];
    let order_key_index = 0;

    let table_columns = vec![
        ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // order key
        ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // partition key
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // pk
        ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // x
    ];
    let table_pk_indices = vec![1, 0, 2];
    let table_order_types = vec![
        OrderType::ascending(),
        OrderType::ascending(),
        OrderType::ascending(),
    ];

    let output_pk_indices = vec![2];

    let state_table = StateTable::new_without_distribution_inconsistent_op(
        store,
        TableId::new(1),
        table_columns,
        table_order_types,
        table_pk_indices,
    )
    .await;

    let (tx, source) = MockSource::channel(input_schema, input_pk_indices.clone());
    let executor = EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
        input: source.boxed(),
        actor_ctx: ActorContext::create(123),
        pk_indices: output_pk_indices,
        executor_id: 1,
        calls,
        partition_key_indices,
        order_key_index,
        state_table,
        watermark_epoch: Arc::new(AtomicU64::new(0)),
    });
    (tx, executor.boxed().execute())
}

#[tokio::test]
async fn test_over_window() {
    let store = MemoryStateStore::new();
    let calls = vec![
        WindowFuncCall {
            kind: WindowFuncKind::Lag,
            args: AggArgs::Unary(DataType::Int32, 3),
            return_type: DataType::Int32,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::CurrentRow),
        },
        WindowFuncCall {
            kind: WindowFuncKind::Lead,
            args: AggArgs::Unary(DataType::Int32, 3),
            return_type: DataType::Int32,
            frame: Frame::rows(FrameBound::CurrentRow, FrameBound::Following(1)),
        },
    ];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p1 101 16
    + 4 p2 200 20
- !chunk |2
      I T  I   i
    + 5 p1 102 18
    + 7 p2 201 22
    + 8 p3 300 33
# NOTE: no watermark message here, since watermark(1) was already received
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I  T  I   i
    + 10 p1 103 13
    + 12 p2 202 28
    + 13 p3 301 39
- !barrier 4
"###,
        expect![[r#"
            # This result can be automatically updated. See `TODO:` for more information.
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 16 |
                | + | 4 | p2 | 200 | 20 |
                +---+---+----+-----+----+
              output:
              - !watermark
                col_idx: 0
                val: 1
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 |   | 16 |
                +---+---+----+-----+----+---+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 5 | p1 | 102 | 18 |
                | + | 7 | p2 | 201 | 22 |
                | + | 8 | p3 | 300 | 33 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 101 | 16 | 10 | 18 |
                | + | 4 | p2 | 200 | 20 |    | 22 |
                +---+---+----+-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p1 | 103 | 13 |
                | + | 12 | p2 | 202 | 28 |
                | + | 13 | p3 | 301 | 39 |
                +---+----+----+-----+----+
              output:
              - !watermark
                col_idx: 0
                val: 5
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 5 | p1 | 102 | 18 | 16 | 13 |
                | + | 7 | p2 | 201 | 22 | 20 | 28 |
                | + | 8 | p3 | 300 | 33 |    | 39 |
                +---+---+----+-----+----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
    )
    .await;
}

#[tokio::test]
async fn test_over_window_aggregate() {
    let store = MemoryStateStore::new();
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::Aggregate(AggKind::Sum),
        args: AggArgs::Unary(DataType::Int32, 3),
        return_type: DataType::Int64,
        frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Following(1)),
    }];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p1 101 16
    + 4 p1 102 20
"###,
        expect![[r#"
            # This result can be automatically updated. See `TODO:` for more information.
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 16 |
                | + | 4 | p1 | 102 | 20 |
                +---+---+----+-----+----+
              output:
              - !watermark
                col_idx: 0
                val: 1
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 26 |
                | + | 1 | p1 | 101 | 16 | 46 |
                +---+---+----+-----+----+----+
        "#]],
    )
    .await;
}
