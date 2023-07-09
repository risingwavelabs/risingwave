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

use std::collections::{btree_map, BTreeMap};

use futures::{Future, FutureExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::{DataType, DefaultOrdered, ToText};
use risingwave_stream::executor::test_utils::MessageSender;
use risingwave_stream::executor::{BoxedMessageStream, Message};

/// Options to control the building of snapshot output.
#[derive(Debug, Clone, Default)]
pub struct SnapshotOptions {
    /// Whether to sort the output chunk, required if the output chunk has no specified order.
    pub sort_chunk: bool,

    /// Whether to include the result after applying the changes from each output chunk. One can
    /// imagine this as the result of a `SELECT * FROM mv` after each output chunk.
    pub include_applied_result: bool,
}

impl SnapshotOptions {
    pub fn sort_chunk(mut self, sort_chunk: bool) -> Self {
        self.sort_chunk = sort_chunk;
        self
    }

    pub fn include_applied_result(mut self, include_applied_result: bool) -> Self {
        self.include_applied_result = include_applied_result;
        self
    }
}

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
/// Alternatively, you can use `expect_file!` if the inline result doesn't look good.
///
/// Then just run the tests with env var `UPDATE_EXPECT=1`.
///
/// ```sh
/// UPDATE_EXPECT=1 cargo nextest run -p risingwave_stream
/// # or
/// UPDATE_EXPECT=1 risedev test -p risingwave_stream
/// ```
pub fn check_until_pending(
    executor: &mut BoxedMessageStream,
    expect: expect_test::Expect,
    options: SnapshotOptions,
) {
    let output = run_until_pending(executor, &mut Store::default(), options);
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
    options: SnapshotOptions,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = (MessageSender, BoxedMessageStream)>,
{
    let output = executor_snapshot(create_executor, test_script, options).await;
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
    Watermark { col_idx: usize, val: String }, // FIXME(rc): need better `val`
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

async fn executor_snapshot<F, Fut>(
    create_executor: F,
    inputs: &str,
    options: SnapshotOptions,
) -> String
where
    F: Fn() -> Fut,
    Fut: Future<Output = (MessageSender, BoxedMessageStream)>,
{
    let inputs = SnapshotEvent::parse(inputs);

    let (mut tx, mut executor) = create_executor().await;

    let mut store = Store::default();
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
            SnapshotEvent::Watermark { col_idx, val } => tx.push_watermark(
                *col_idx,
                DataType::Int64, // TODO(rc): support timestamp data type
                val.parse::<i64>().unwrap().into(),
            ),
        }

        snapshot.push(Snapshot {
            input: event,
            output: run_until_pending(&mut executor, &mut store, options.clone()),
        });
    }

    serde_yaml::to_string(&snapshot).unwrap()
}

fn run_until_pending(
    executor: &mut BoxedMessageStream,
    store: &mut Store,
    options: SnapshotOptions,
) -> Vec<SnapshotEvent> {
    let mut output = vec![];

    while let Some(msg) = executor.try_next().now_or_never() {
        let msg = msg.unwrap();
        let msg = match msg {
            Some(msg) => msg,
            None => return output,
        };
        output.push(match msg {
            Message::Chunk(mut chunk) => {
                if options.sort_chunk {
                    chunk = chunk.sort_rows();
                }
                let mut output = chunk.to_pretty_string();
                if options.include_applied_result {
                    let applied = store.apply_chunk(&chunk);
                    output += &format!("\napplied result:\n{}", applied.to_pretty_string());
                }
                SnapshotEvent::Chunk(output)
            }
            Message::Barrier(barrier) => SnapshotEvent::Barrier(barrier.epoch.curr),
            Message::Watermark(watermark) => SnapshotEvent::Watermark {
                col_idx: watermark.col_idx,
                val: watermark.val.as_scalar_ref_impl().to_text(),
            },
        });
    }

    output
}

/// A simple multiset-like store to apply the changes from the output chunks.
#[derive(Default)]
struct Store {
    rows: BTreeMap<DefaultOrdered<OwnedRow>, usize>,
}

impl Store {
    /// Apply the changes from the given [`StreamChunk`] to the store and return the applied result
    /// in [`DataChunk`] format.
    fn apply_chunk(&mut self, chunk: &StreamChunk) -> DataChunk {
        let data_types = chunk.data_chunk().data_types();

        for (op, row) in chunk.rows() {
            let row = DefaultOrdered(row.into_owned_row());

            match op {
                Op::Insert | Op::UpdateInsert => {
                    *self.rows.entry(row).or_default() += 1;
                }
                Op::Delete | Op::UpdateDelete => match self.rows.entry(row) {
                    btree_map::Entry::Vacant(v) => panic!("delete non-existing row: {:?}", v.key()),
                    btree_map::Entry::Occupied(mut o) => {
                        *o.get_mut() -= 1;
                        if *o.get() == 0 {
                            o.remove();
                        }
                    }
                },
            }
        }

        let rows = self
            .rows
            .iter()
            .flat_map(|(row, &count)| std::iter::repeat(row).take(count))
            .collect_vec();

        DataChunk::from_rows(&rows, &data_types)
    }
}
