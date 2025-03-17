// Copyright 2025 RisingWave Labs
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

use risingwave_expr::aggregate::{AggArgs, PbAggKind};
use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncCall, WindowFuncKind};
use risingwave_stream::common::table::test_utils::gen_pbtable;
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

    let output_schema = {
        let mut fields = input_schema.fields.clone();
        calls.iter().for_each(|call| {
            fields.push(Field::unnamed(call.return_type.clone()));
        });
        Schema { fields }
    };

    let state_table = StateTable::from_table_catalog_inconsistent_op(
        &gen_pbtable(
            TableId::new(1),
            table_columns,
            table_order_types,
            table_pk_indices,
            0,
        ),
        store,
        None,
    )
    .await;

    let (tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, input_pk_indices.clone());
    let executor = EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
        actor_ctx: ActorContext::for_test(123),

        input: source,

        schema: output_schema,
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
        // lag(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
        },
        // lead(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
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
- !barrier 2
- !chunk |2
      I  T  I   i
    + 10 p1 103 13
    + 12 p2 202 28
    + 13 p3 301 39
- !barrier 3
"###,
        expect![[r#"
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
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p1 | 103 | 13 |
                | + | 12 | p2 | 202 | 28 |
                | + | 13 | p3 | 301 | 39 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 5 | p1 | 102 | 18 | 16 | 13 |
                | + | 7 | p2 | 201 | 22 | 20 | 28 |
                | + | 8 | p3 | 300 | 33 |    | 39 |
                +---+---+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

#[tokio::test]
async fn test_over_window_aggregate() {
    let store = MemoryStateStore::new();
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
        return_type: DataType::Int64,
        args: AggArgs::from_iter([(DataType::Int32, 3)]),
        ignore_nulls: false,
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
        - !chunk |2
              I T  I   i
            + 2 p1 103 30
            + 6 p1 104 11
        "###,
        expect![[r#"
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
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 26 |
                | + | 1 | p1 | 101 | 16 | 46 |
                +---+---+----+-----+----+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 2 | p1 | 103 | 30 |
                | + | 6 | p1 | 104 | 11 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 4 | p1 | 102 | 20 | 66 |
                | + | 2 | p1 | 103 | 30 | 61 |
                +---+---+----+-----+----+----+
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}
