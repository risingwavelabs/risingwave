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

use risingwave_expr::table_function::repeat;
use risingwave_stream::executor::ProjectSetExecutor;

use crate::prelude::*;

const CHUNK_SIZE: usize = 1024;

fn create_executor() -> (MessageSender, BoxedMessageStream) {
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ],
    };
    let (tx, source) = MockSource::channel(schema, PkIndices::new());

    let test_expr = build_from_pretty("(add:int8 $0:int8 $1:int8)");
    let tf1 = repeat(build_from_pretty("1:int4"), 1);
    let tf2 = repeat(build_from_pretty("2:int4"), 2);

    let project_set = Box::new(ProjectSetExecutor::new(
        Box::new(source),
        vec![],
        vec![test_expr.into(), tf1.into(), tf2.into()],
        1,
        CHUNK_SIZE,
    ));
    (tx, project_set.execute())
}

#[tokio::test]
async fn test_project_set() {
    let (mut tx, mut project_set) = create_executor();

    tx.push_chunk(StreamChunk::from_pretty(
        " I I
        + 1 4
        + 2 5
        + 3 6",
    ));
    tx.push_chunk(StreamChunk::from_pretty(
        " I I
        + 7 8
        - 3 6",
    ));

    check_until_pending(
        &mut project_set,
        expect_test::expect![[r#"
            - !chunk |-
              +---+---+---+---+---+
              | + | 0 | 5 | 1 | 2 |
              | + | 1 | 5 |   | 2 |
              | + | 0 | 7 | 1 | 2 |
              | + | 1 | 7 |   | 2 |
              | + | 0 | 9 | 1 | 2 |
              | + | 1 | 9 |   | 2 |
              +---+---+---+---+---+
            - !chunk |-
              +---+---+----+---+---+
              | + | 0 | 15 | 1 | 2 |
              | + | 1 | 15 |   | 2 |
              | - | 0 | 9  | 1 | 2 |
              | - | 1 | 9  |   | 2 |
              +---+---+----+---+---+
        "#]],
    );
}
