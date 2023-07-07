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

use risingwave_common::cast::str_to_timestamp;
use risingwave_common::types::test_utils::IntervalTestExt;
use risingwave_common::types::Interval;
use risingwave_expr::expr::test_utils::make_hop_window_expression;
use risingwave_stream::executor::{ExecutorInfo, HopWindowExecutor};

use crate::prelude::*;

const TIME_COL_IDX: usize = 2;
const CHUNK_SIZE: usize = 256;

fn create_executor(output_indices: Vec<usize>) -> (MessageSender, BoxedMessageStream) {
    let field1 = Field::unnamed(DataType::Int64);
    let field2 = Field::unnamed(DataType::Int64);
    let field3 = Field::with_name(DataType::Timestamp, "created_at");
    let schema = Schema::new(vec![field1, field2, field3]);
    let pk_indices = vec![0];
    let (tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());

    let window_slide = Interval::from_minutes(15);
    let window_size = Interval::from_minutes(30);
    let offset = Interval::from_minutes(0);
    let (window_start_exprs, window_end_exprs) = make_hop_window_expression(
        DataType::Timestamp,
        TIME_COL_IDX,
        window_size,
        window_slide,
        offset,
    )
    .unwrap();

    (
        tx,
        HopWindowExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            ExecutorInfo {
                schema,
                pk_indices,
                identity: "test".to_string(),
            },
            TIME_COL_IDX,
            window_slide,
            window_size,
            window_start_exprs,
            window_end_exprs,
            output_indices,
            CHUNK_SIZE,
        )
        .boxed()
        .execute(),
    )
}

fn push_watermarks(tx: &mut MessageSender) {
    tx.push_watermark(
        TIME_COL_IDX,
        DataType::Timestamp,
        str_to_timestamp("2023-07-06 18:27:03").unwrap().into(),
    );
    tx.push_watermark(
        TIME_COL_IDX,
        DataType::Timestamp,
        str_to_timestamp("2023-07-06 18:29:59").unwrap().into(),
    );
    tx.push_watermark(
        TIME_COL_IDX,
        DataType::Timestamp,
        str_to_timestamp("2023-07-06 18:30:00").unwrap().into(),
    );
    tx.push_watermark(0, DataType::Int64, 100.into());
    tx.push_watermark(
        TIME_COL_IDX,
        DataType::Timestamp,
        str_to_timestamp("2023-07-06 18:43:40").unwrap().into(),
    );
    tx.push_watermark(
        TIME_COL_IDX,
        DataType::Timestamp,
        str_to_timestamp("2023-07-06 18:50:00").unwrap().into(),
    );
}

#[tokio::test]
async fn test_watermark_full_output() {
    let (mut tx, mut hop) = create_executor((0..5).collect());

    push_watermarks(&mut tx);

    check_until_pending(
        &mut hop,
        expect![[r#"
            - !watermark
              col_idx: 3
              val: 2023-07-06 18:00:00
            - !watermark
              col_idx: 4
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 3
              val: 2023-07-06 18:00:00
            - !watermark
              col_idx: 4
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 3
              val: 2023-07-06 18:15:00
            - !watermark
              col_idx: 4
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 0
              val: '100'
            - !watermark
              col_idx: 3
              val: 2023-07-06 18:15:00
            - !watermark
              col_idx: 4
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 3
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 4
              val: 2023-07-06 19:00:00
        "#]],
        SnapshotOptions::default(),
    );
}

#[tokio::test]
async fn test_watermark_output_indices1() {
    let output_indices = vec![4, 1, 0, 2]; // 4 is `window_end` column
    let (mut tx, mut hop) = create_executor(output_indices);

    push_watermarks(&mut tx);

    check_until_pending(
        &mut hop,
        expect![[r#"
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 2
              val: '100'
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 19:00:00
        "#]],
        SnapshotOptions::default(),
    );
}

#[tokio::test]
async fn test_watermark_output_indices2() {
    let output_indices = vec![3, 4, 1, 0, 2]; // 3 is `window_start` column, 4 is `window_end`
    let (mut tx, mut hop) = create_executor(output_indices);

    push_watermarks(&mut tx);

    check_until_pending(
        &mut hop,
        expect![[r#"
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:00:00
            - !watermark
              col_idx: 1
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:00:00
            - !watermark
              col_idx: 1
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:15:00
            - !watermark
              col_idx: 1
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 3
              val: '100'
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:15:00
            - !watermark
              col_idx: 1
              val: 2023-07-06 18:45:00
            - !watermark
              col_idx: 0
              val: 2023-07-06 18:30:00
            - !watermark
              col_idx: 1
              val: 2023-07-06 19:00:00
        "#]],
        SnapshotOptions::default(),
    );
}
