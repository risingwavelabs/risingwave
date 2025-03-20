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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::Datum;
use risingwave_expr::expr::{Expression, NonStrictExpression, ValueImpl};
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::executor::project::{MaterializedExprsArgs, MaterializedExprsExecutor};

use crate::prelude::*;

async fn create_executor<S: StateStore>(
    exprs: Vec<NonStrictExpression>,
    store: S,
) -> (MessageSender, BoxedMessageStream) {
    let input_schema = Schema::new(vec![
        Field::unnamed(DataType::Int64), // x
        Field::unnamed(DataType::Int64), // pk
    ]);
    let input_pk_indices = vec![1];
    let table_columns = vec![
        ColumnDesc::named("x", ColumnId::new(0), DataType::Int64),
        ColumnDesc::named("pk", ColumnId::new(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(3), DataType::Int64),
    ];
    let table_pk_indices = vec![1];
    let table_order_types = vec![OrderType::ascending()];

    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::new(1),
            table_columns,
            table_order_types,
            table_pk_indices,
            1,
        ),
        store,
        None,
    )
    .await;

    let (tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, input_pk_indices);
    let executor = MaterializedExprsExecutor::new(MaterializedExprsArgs {
        actor_ctx: ActorContext::for_test(123),
        input: source,
        exprs,
        state_table,
        state_clean_col_idx: None,
        watermark_epoch: Arc::new(AtomicU64::new(0)),
    });
    (tx, executor.boxed().execute())
}

fn snapshot_options() -> SnapshotOptions {
    SnapshotOptions::default().include_applied_result(true)
}

#[tokio::test]
async fn test_materialize_pure_expressions() {
    let store = MemoryStateStore::new();
    let build_exprs = || {
        vec![
            build_from_pretty("(add:int8 42:int2 $1:int8)"),
            build_from_pretty("(subtract:int8 42:int2 $1:int8)"),
        ]
    };

    check_with_script(
        || create_executor(build_exprs(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
               I    I
             + 123  3
             + 124  6
        - !chunk |2
               I    I
             - 124  6
             + 125  9
            U- 125  9
            U+ 125  10
             + 126  12
        - !barrier 2
        - !chunk |2
               I    I
             - 126  12
             + 127  15
             + 126  10
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+-----+---+
                | + | 123 | 3 |
                | + | 124 | 6 |
                +---+-----+---+
              output:
              - !chunk |-
                +---+-----+---+----+----+
                | + | 123 | 3 | 45 | 39 |
                | + | 124 | 6 | 48 | 36 |
                +---+-----+---+----+----+
                applied result:
                +-----+---+----+----+
                | 123 | 3 | 45 | 39 |
                | 124 | 6 | 48 | 36 |
                +-----+---+----+----+
            - input: !chunk |-
                +----+-----+----+
                |  - | 124 | 6  |
                |  + | 125 | 9  |
                | U- | 125 | 9  |
                | U+ | 125 | 10 |
                |  + | 126 | 12 |
                +----+-----+----+
              output:
              - !chunk |-
                +----+-----+----+----+----+
                |  - | 124 | 6  | 48 | 36 |
                |  + | 125 | 9  | 51 | 33 |
                | U- | 125 | 9  | 51 | 33 |
                | U+ | 125 | 10 | 52 | 32 |
                |  + | 126 | 12 | 54 | 30 |
                +----+-----+----+----+----+
                applied result:
                +-----+----+----+----+
                | 123 | 3  | 45 | 39 |
                | 125 | 10 | 52 | 32 |
                | 126 | 12 | 54 | 30 |
                +-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+-----+----+
                | - | 126 | 12 |
                | + | 127 | 15 |
                | + | 126 | 10 |
                +---+-----+----+
              output:
              - !chunk |-
                +---+-----+----+----+----+
                | - | 126 | 12 | 54 | 30 |
                | + | 127 | 15 | 57 | 27 |
                | + | 126 | 10 | 52 | 32 |
                +---+-----+----+----+----+
                applied result:
                +-----+----+----+----+
                | 123 | 3  | 45 | 39 |
                | 125 | 10 | 52 | 32 |
                | 126 | 10 | 52 | 32 |
                | 127 | 15 | 57 | 27 |
                +-----+----+----+----+
        "#]],
        snapshot_options(),
    )
    .await
}

#[derive(Debug)]
struct Count(AtomicU64);

#[async_trait::async_trait]
impl Expression for Count {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    async fn eval_v2(&self, input: &DataChunk) -> risingwave_expr::Result<ValueImpl> {
        let value = self.0.fetch_add(1, Ordering::Relaxed) as i64;
        Ok(ValueImpl::Scalar {
            value: Some(value.into()),
            capacity: input.capacity(),
        })
    }

    async fn eval_row(&self, _input: &OwnedRow) -> risingwave_expr::Result<Datum> {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_materialize_non_pure_expressions() {
    let store = MemoryStateStore::new();
    let build_exprs = || {
        vec![
            NonStrictExpression::for_test(Count(AtomicU64::new(0))),
            NonStrictExpression::for_test(Count(AtomicU64::new(100))),
        ]
    };

    check_with_script(
        || create_executor(build_exprs(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
               I    I
             + 123  3
             + 124  6
        - !chunk |2
               I    I
             - 124  6
             + 125  9
            U- 125  9
            U+ 125  10
             + 126  12
        - !barrier 2
        - !chunk |2
               I    I
             - 126  12
             + 127  15
             + 126  10
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+-----+---+
                | + | 123 | 3 |
                | + | 124 | 6 |
                +---+-----+---+
              output:
              - !chunk |-
                +---+-----+---+---+-----+
                | + | 123 | 3 | 0 | 100 |
                | + | 124 | 6 | 0 | 100 |
                +---+-----+---+---+-----+
                applied result:
                +-----+---+---+-----+
                | 123 | 3 | 0 | 100 |
                | 124 | 6 | 0 | 100 |
                +-----+---+---+-----+
            - input: !chunk |-
                +----+-----+----+
                |  - | 124 | 6  |
                |  + | 125 | 9  |
                | U- | 125 | 9  |
                | U+ | 125 | 10 |
                |  + | 126 | 12 |
                +----+-----+----+
              output:
              - !chunk |-
                +----+-----+----+---+-----+
                |  - | 124 | 6  | 0 | 100 |
                |  + | 125 | 9  | 1 | 101 |
                | U- | 125 | 9  | 1 | 101 |
                | U+ | 125 | 10 | 1 | 101 |
                |  + | 126 | 12 | 1 | 101 |
                +----+-----+----+---+-----+
                applied result:
                +-----+----+---+-----+
                | 123 | 3  | 0 | 100 |
                | 125 | 10 | 1 | 101 |
                | 126 | 12 | 1 | 101 |
                +-----+----+---+-----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+-----+----+
                | - | 126 | 12 |
                | + | 127 | 15 |
                | + | 126 | 10 |
                +---+-----+----+
              output:
              - !chunk |-
                +---+-----+----+---+-----+
                | - | 126 | 12 | 1 | 101 |
                | + | 127 | 15 | 2 | 102 |
                | + | 126 | 10 | 2 | 102 |
                +---+-----+----+---+-----+
                applied result:
                +-----+----+---+-----+
                | 123 | 3  | 0 | 100 |
                | 125 | 10 | 1 | 101 |
                | 126 | 10 | 2 | 102 |
                | 127 | 15 | 2 | 102 |
                +-----+----+---+-----+
        "#]],
        snapshot_options(),
    )
    .await
}
