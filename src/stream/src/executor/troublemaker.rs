// Copyright 2024 RisingWave Labs
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

use futures::StreamExt;
use futures_async_stream::try_stream;
use rand::Rng;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::stream_record::{Record, RecordType};
use risingwave_common::array::Op;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

/// TroublemakerExecutor is used to make some trouble in the stream graph. Specifically,
/// it is attached to `StreamScan` and `Source` executors in **insane mode**. It randomly
/// corrupts the stream chunks it receives and sends them downstream, making the stream
/// inconsistent. This should ONLY BE USED IN INSANE MODE FOR TESTING PURPOSES.
pub struct TroublemakerExecutor {
    input: Executor,
    inner: Inner,
}

struct Inner {
    chunk_size: usize,
}

struct Vars {
    chunk_builder: StreamChunkBuilder,
    met_delete_before: bool,
}

impl TroublemakerExecutor {
    pub fn new(input: Executor, chunk_size: usize) -> Self {
        assert!(
            crate::consistency::insane(),
            "we should only make trouble in insane mode"
        );
        Self {
            input,
            inner: Inner { chunk_size },
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self { input, inner: this } = self;

        let data_types = input.schema().data_types();

        let mut vars = Vars {
            chunk_builder: StreamChunkBuilder::new(this.chunk_size, input.schema().data_types()),
            met_delete_before: false,
        };

        #[for_await]
        for msg in input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    for record in chunk.records() {
                        if matches!(
                            record.to_record_type(),
                            RecordType::Delete | RecordType::Update
                        ) {
                            vars.met_delete_before = true;
                        }

                        for (op, row) in
                            make_some_trouble(&data_types, record, vars.met_delete_before)
                        {
                            if let Some(chunk) = vars.chunk_builder.append_row(op, row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }

                    if let Some(chunk) = vars.chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    assert!(vars.chunk_builder.take().is_none(), "we don't merge chunks");
                    yield Message::Barrier(barrier);
                }
                _ => yield msg,
            }
        }
    }
}

impl Execute for TroublemakerExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

fn make_some_trouble(
    data_types: &[DataType],
    record: Record<impl Row>,
    can_change_op: bool,
) -> impl Iterator<Item = (Op, OwnedRow)> + '_ {
    let record = if can_change_op && rand::thread_rng().gen_bool(0.5) {
        // Change the `Op`
        match record {
            Record::Insert { new_row } => Record::Delete {
                old_row: new_row.into_owned_row(),
            },
            Record::Delete { old_row } => Record::Insert {
                new_row: old_row.into_owned_row(),
            },
            Record::Update { old_row, new_row } => Record::Update {
                old_row: new_row.into_owned_row(),
                new_row: old_row.into_owned_row(),
            },
        }
    } else {
        // Just convert the rows to owned rows, without changing the `Op`
        match record {
            Record::Insert { new_row } => Record::Insert {
                new_row: new_row.into_owned_row(),
            },
            Record::Delete { old_row } => Record::Delete {
                old_row: old_row.into_owned_row(),
            },
            Record::Update { old_row, new_row } => Record::Update {
                old_row: old_row.into_owned_row(),
                new_row: new_row.into_owned_row(),
            },
        }
    };

    record
        .into_rows()
        .map(|(op, row)| (op, randomize_row(data_types, row)))
}

fn randomize_row(data_types: &[DataType], row: OwnedRow) -> OwnedRow {
    let mut data = row.into_inner();

    for (datum, data_type) in data.iter_mut().zip_eq_fast(data_types) {
        match rand::thread_rng().gen_range(0..3) {
            0 => {
                // don't change the value
            }
            1 => {
                *datum = None;
            }
            2 => {
                *datum = match data_type {
                    DataType::Boolean => Some(ScalarImpl::Bool(rand::random())),
                    DataType::Int16 => Some(ScalarImpl::Int16(rand::random())),
                    DataType::Int32 => Some(ScalarImpl::Int32(rand::random())),
                    DataType::Int64 => Some(ScalarImpl::Int64(rand::random())),
                    DataType::Float32 => Some(ScalarImpl::Float32(rand::random())),
                    DataType::Float64 => Some(ScalarImpl::Float64(rand::random())),
                    DataType::Decimal => Some(ScalarImpl::Decimal(rand::random::<i64>().into())),
                    DataType::Varchar => Some(ScalarImpl::Utf8(random_string().into_boxed_str())),
                    _ => datum.take(),
                }
            }
            _ => unreachable!(),
        }
    }

    OwnedRow::new(data.into_vec())
}

fn random_string() -> String {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(1..=10);
    let s: String = rng
        .sample_iter(rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    s
}
