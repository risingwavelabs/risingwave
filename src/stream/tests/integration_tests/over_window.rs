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

use risingwave_common::session_config::OverWindowCachePolicy;
use risingwave_expr::aggregate::{AggArgs, PbAggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameExclusion, WindowFuncCall, WindowFuncKind,
};
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{OverWindowExecutor, OverWindowExecutorArgs};

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
    let order_key_indices = vec![0];
    let order_key_order_types = vec![OrderType::ascending()];

    let mut table_columns = vec![
        ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // order key
        ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // partition key
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // pk
        ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // x
    ];
    for call in &calls {
        table_columns.push(ColumnDesc::unnamed(
            ColumnId::new(table_columns.len() as i32),
            call.return_type.clone(),
        ));
    }
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

    let state_table = StateTable::from_table_catalog(
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
    let executor = OverWindowExecutor::new(OverWindowExecutorArgs {
        actor_ctx: ActorContext::for_test(123),

        input: source,

        schema: output_schema,
        calls,
        partition_key_indices,
        order_key_indices,
        order_key_order_types,
        state_table,
        watermark_epoch: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(StreamingMetrics::unused()),
        chunk_size: 1024,
        cache_policy: OverWindowCachePolicy::Recent,
    });
    (tx, executor.boxed().execute())
}

fn snapshot_options() -> SnapshotOptions {
    SnapshotOptions::default().include_applied_result(true)
}

#[tokio::test]
async fn test_over_window_lag_lead_append_only() {
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
        - !chunk |2
              I T  I   i
            + 5 p1 102 18
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I  T  I   i
            + 10 p1 103 13
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
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 100 | 10 |    | 16 |
                | + | 1 | p1 | 101 | 16 | 10 |    |
                +---+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p1 | 101 | 16 | 10 |    |
                +---+----+-----+----+----+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 5 | p1 | 102 | 18 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                | U- | 1 | p1 | 101 | 16 | 10 |    |
                | U+ | 1 | p1 | 101 | 16 | 10 | 18 |
                |  + | 5 | p1 | 102 | 18 | 16 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p1 | 101 | 16 | 10 | 18 |
                | 5 | p1 | 102 | 18 | 16 |    |
                +---+----+-----+----+----+----+
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
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +----+----+----+-----+----+----+----+
                | U- | 5  | p1 | 102 | 18 | 16 |    |
                | U+ | 5  | p1 | 102 | 18 | 16 | 13 |
                |  + | 10 | p1 | 103 | 13 | 18 |    |
                +----+----+----+-----+----+----+----+
                applied result:
                +----+----+-----+----+----+----+
                | 1  | p1 | 100 | 10 |    | 16 |
                | 1  | p1 | 101 | 16 | 10 | 18 |
                | 5  | p1 | 102 | 18 | 16 | 13 |
                | 10 | p1 | 103 | 13 | 18 |    |
                +----+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
        snapshot_options(),
    )
    .await;
}

#[tokio::test]
async fn test_over_window_lag_lead_with_updates() {
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
            + 1 p2 200 20
            + 2 p1 101 16
            + 3 p1 103 13
            - 3 p1 103 13 // deletes the above row
        - !chunk |2
              I T  I   i
           U- 1 p1 100 10
           U+ 3 p1 100 13 // an order-change update, `x` also changed
            + 5 p1 105 18
            + 6 p2 203 23
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I T  I   i
            - 6 p2 203 23
           U- 2 p1 101 16
           U+ 2 p2 101 16 // a partition-change update
        - !barrier 3
        - recovery
        - !barrier 3
        - !chunk |2
              I  T  I   i
            + 10 p3 300 30
        - !barrier 4
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 2 | p1 | 101 | 16 |
                | + | 3 | p1 | 103 | 13 |
                | - | 3 | p1 | 103 | 13 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 100 | 10 |    | 16 |
                | + | 2 | p1 | 101 | 16 | 10 |    |
                | + | 1 | p2 | 200 | 20 |    |    |
                +---+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p2 | 200 | 20 |    |    |
                | 2 | p1 | 101 | 16 | 10 |    |
                +---+----+-----+----+----+----+
            - input: !chunk |-
                +----+---+----+-----+----+
                | U- | 1 | p1 | 100 | 10 |
                | U+ | 3 | p1 | 100 | 13 |
                |  + | 5 | p1 | 105 | 18 |
                |  + | 6 | p2 | 203 | 23 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                | U- | 2 | p1 | 101 | 16 | 10 |    |
                | U+ | 2 | p1 | 101 | 16 |    | 13 |
                | U- | 1 | p1 | 100 | 10 |    | 16 |
                | U+ | 3 | p1 | 100 | 13 | 16 | 18 |
                |  + | 5 | p1 | 105 | 18 | 13 |    |
                | U- | 1 | p2 | 200 | 20 |    |    |
                | U+ | 1 | p2 | 200 | 20 |    | 23 |
                |  + | 6 | p2 | 203 | 23 | 20 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p2 | 200 | 20 |    | 23 |
                | 2 | p1 | 101 | 16 |    | 13 |
                | 3 | p1 | 100 | 13 | 16 | 18 |
                | 5 | p1 | 105 | 18 | 13 |    |
                | 6 | p2 | 203 | 23 | 20 |    |
                +---+----+-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +----+---+----+-----+----+
                |  - | 6 | p2 | 203 | 23 |
                | U- | 2 | p1 | 101 | 16 |
                | U+ | 2 | p2 | 101 | 16 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                |  - | 2 | p1 | 101 | 16 |    | 13 |
                | U- | 3 | p1 | 100 | 13 | 16 | 18 |
                | U+ | 3 | p1 | 100 | 13 |    | 18 |
                | U- | 1 | p2 | 200 | 20 |    | 23 |
                | U+ | 1 | p2 | 200 | 20 |    | 16 |
                |  + | 2 | p2 | 101 | 16 | 20 |    |
                |  - | 6 | p2 | 203 | 23 | 20 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p2 | 200 | 20 |    | 16 |
                | 2 | p2 | 101 | 16 | 20 |    |
                | 3 | p1 | 100 | 13 |    | 18 |
                | 5 | p1 | 105 | 18 | 13 |    |
                +---+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p3 | 300 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+---+---+
                | + | 10 | p3 | 300 | 30 |   |   |
                +---+----+----+-----+----+---+---+
                applied result:
                +----+----+-----+----+----+----+
                | 1  | p2 | 200 | 20 |    | 16 |
                | 2  | p2 | 101 | 16 | 20 |    |
                | 3  | p1 | 100 | 13 |    | 18 |
                | 5  | p1 | 105 | 18 | 13 |    |
                | 10 | p3 | 300 | 30 |    |    |
                +----+----+-----+----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        snapshot_options(),
    )
    .await;
}

#[tokio::test]
async fn test_over_window_sum() {
    let store = MemoryStateStore::new();
    let calls = vec![
        // sum(x) over (
        //   partition by ..
        //   order by ..
        //   rows between 1 preceding and 2 following exclude current row
        // )
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
            return_type: DataType::Int64,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows_with_exclusion(
                FrameBound::Preceding(1),
                FrameBound::Following(2),
                FrameExclusion::CurrentRow,
            ),
        },
    ];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
              I T  I   i
            + 1 p1 100 10
            + 1 p2 200 20
            + 2 p1 101 16
            + 2 p1 102 17
        - !chunk |2
              I T  I   i
           U- 1 p1 100 10
           U+ 3 p1 100 13 // an order-change update, `x` also changed
            + 5 p1 105 18
            + 6 p2 203 23
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I T  I   i
            - 6 p2 203 23
           U- 2 p1 101 16
           U+ 2 p2 101 16 // a partition-change update
        - !barrier 3
        - recovery
        - !barrier 3
        - !chunk |2
              I  T  I   i
            + 10 p3 300 30
        - !barrier 4
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 2 | p1 | 101 | 16 |
                | + | 2 | p1 | 102 | 17 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 33 |
                | + | 2 | p1 | 101 | 16 | 27 |
                | + | 2 | p1 | 102 | 17 | 16 |
                | + | 1 | p2 | 200 | 20 |    |
                +---+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p1 | 100 | 10 | 33 |
                | 1 | p2 | 200 | 20 |    |
                | 2 | p1 | 101 | 16 | 27 |
                | 2 | p1 | 102 | 17 | 16 |
                +---+----+-----+----+----+
            - input: !chunk |-
                +----+---+----+-----+----+
                | U- | 1 | p1 | 100 | 10 |
                | U+ | 3 | p1 | 100 | 13 |
                |  + | 5 | p1 | 105 | 18 |
                |  + | 6 | p2 | 203 | 23 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+
                | U- | 2 | p1 | 101 | 16 | 27 |
                | U+ | 2 | p1 | 101 | 16 | 30 |
                | U- | 2 | p1 | 102 | 17 | 16 |
                | U+ | 2 | p1 | 102 | 17 | 47 |
                | U- | 1 | p1 | 100 | 10 | 33 |
                | U+ | 3 | p1 | 100 | 13 | 35 |
                |  + | 5 | p1 | 105 | 18 | 13 |
                | U- | 1 | p2 | 200 | 20 |    |
                | U+ | 1 | p2 | 200 | 20 | 23 |
                |  + | 6 | p2 | 203 | 23 | 20 |
                +----+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p2 | 200 | 20 | 23 |
                | 2 | p1 | 101 | 16 | 30 |
                | 2 | p1 | 102 | 17 | 47 |
                | 3 | p1 | 100 | 13 | 35 |
                | 5 | p1 | 105 | 18 | 13 |
                | 6 | p2 | 203 | 23 | 20 |
                +---+----+-----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +----+---+----+-----+----+
                |  - | 6 | p2 | 203 | 23 |
                | U- | 2 | p1 | 101 | 16 |
                | U+ | 2 | p2 | 101 | 16 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+
                |  - | 2 | p1 | 101 | 16 | 30 |
                | U- | 2 | p1 | 102 | 17 | 47 |
                | U+ | 2 | p1 | 102 | 17 | 31 |
                | U- | 1 | p2 | 200 | 20 | 23 |
                | U+ | 1 | p2 | 200 | 20 | 16 |
                |  + | 2 | p2 | 101 | 16 | 20 |
                |  - | 6 | p2 | 203 | 23 | 20 |
                +----+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p2 | 200 | 20 | 16 |
                | 2 | p1 | 102 | 17 | 31 |
                | 2 | p2 | 101 | 16 | 20 |
                | 3 | p1 | 100 | 13 | 35 |
                | 5 | p1 | 105 | 18 | 13 |
                +---+----+-----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p3 | 300 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+---+
                | + | 10 | p3 | 300 | 30 |   |
                +---+----+----+-----+----+---+
                applied result:
                +----+----+-----+----+----+
                | 1  | p2 | 200 | 20 | 16 |
                | 2  | p1 | 102 | 17 | 31 |
                | 2  | p2 | 101 | 16 | 20 |
                | 3  | p1 | 100 | 13 | 35 |
                | 5  | p1 | 105 | 18 | 13 |
                | 10 | p3 | 300 | 30 |    |
                +----+----+-----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        snapshot_options(),
    )
    .await;
}
