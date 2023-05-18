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

use futures::{Future, FutureExt, TryStreamExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_stream::executor::test_utils::MessageSender;
use risingwave_stream::executor::{BoxedMessageStream, Message};

/// Drives the executor until it is pending, and then asserts that the output matches
/// `expect`.
///
/// `expect` can be altomatically updated by running the test suite with `UPDATE_EXPECT`
/// environmental variable set.
///
/// TODO: Do we want sth like `check_n_steps` instead, where we do want to wait for a
/// `.await` to complete?
///
/// # Suggested workflow to add a new test
///
/// Just drop this one-liner after creating the executor and sending input messages.
///
/// ```ignore
/// check_until_pending(&mut executor, expect![[""]]).await;
/// ```
///
/// Then just run the tests with env var `UPDATE_EXPECT=1`.
///
/// ```sh
/// UPDATE_EXPECT=1 cargo nextest run -p risingwave_stream
/// # or
/// UPDATE_EXPECT=1 risedev test -p risingwave_stream
/// ```
pub async fn check_until_pending(executor: &mut BoxedMessageStream, expect: expect_test::Expect) {
    let output = run_until_pending(executor).await;
    let output = serde_yaml::to_string(&output).unwrap();
    expect.assert_eq(&output);
}

/// Similar to [`check_until_pending`], but use a DSL test script as input.
///
/// For each input event, it drives the executor until it is pending.
pub async fn check_with_script<F, Fut>(
    create_executor: F,
    test_script: &str,
    expect: expect_test::Expect,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = (MessageSender, BoxedMessageStream)>,
{
    let output = executor_snapshot(create_executor, test_script).await;
    expect.assert_eq(&output);
}

/// This is a DSL for the input and output of executor snapshot tests.
///
/// It immitates [`Message`], but more ser/de friendly.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum SnapshotEvent {
    Barrier(u64),
    Noop,
    Recovery,
    Chunk(String),
    Watermark { col_idx: usize, val: i64 },
}

impl SnapshotEvent {
    #[track_caller]
    fn parse(s: &str) -> Vec<Self> {
        serde_yaml::from_str(s).unwrap()
    }
}

/// One input [event](`SnapshotEvent`) and its corresponding output events.
///
/// A `Vec<Snapshot>` can represent a whole test scenario.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Snapshot {
    input: SnapshotEvent,
    output: Vec<SnapshotEvent>,
}

async fn executor_snapshot<F, Fut>(create_executor: F, inputs: &str) -> String
where
    F: Fn() -> Fut,
    Fut: Future<Output = (MessageSender, BoxedMessageStream)>,
{
    let inputs = SnapshotEvent::parse(inputs);

    let (mut tx, mut executor) = create_executor().await;

    let mut snapshot = Vec::with_capacity(inputs.len());
    for mut event in inputs {
        match &mut event {
            SnapshotEvent::Barrier(epoch) => {
                tx.push_barrier(*epoch, false);
            }
            SnapshotEvent::Noop => unreachable!(),
            SnapshotEvent::Recovery => {
                (tx, executor) = create_executor().await;
            }
            SnapshotEvent::Chunk(chunk_str) => {
                let chunk = StreamChunk::from_pretty(chunk_str);
                *chunk_str = chunk.to_pretty_string();
                tx.push_chunk(chunk);
            }
            SnapshotEvent::Watermark { col_idx, val } => {
                tx.push_watermark(*col_idx, DataType::Int64, (*val).into())
            }
        }

        snapshot.push(Snapshot {
            input: event,
            output: run_until_pending(&mut executor).await,
        });
    }
    // Note: we serialize ourselves, instead of relying on insta::assert_yaml_snapshot,
    // because serde_yaml is prettier. https://github.com/mitsuhiko/insta/issues/372
    let note = "# This result can be automatically updated. See `TODO:` for more information.\n";
    let snapshot = serde_yaml::to_string(&snapshot).unwrap();
    format!("{}{}", note, snapshot)
}

async fn run_until_pending(executor: &mut BoxedMessageStream) -> Vec<SnapshotEvent> {
    let mut output = vec![];

    while let Some(msg) = executor.try_next().now_or_never() {
        let msg = msg.unwrap();
        let msg = match msg {
            Some(msg) => msg,
            None => return output,
        };
        output.push(match msg {
            Message::Chunk(chunk) => SnapshotEvent::Chunk(chunk.to_pretty_string()),
            Message::Barrier(barrier) => SnapshotEvent::Barrier(barrier.epoch.curr),
            Message::Watermark(watermark) => SnapshotEvent::Watermark {
                col_idx: watermark.col_idx,
                val: watermark.val.into_int64(),
            },
        });
    }

    output
}
