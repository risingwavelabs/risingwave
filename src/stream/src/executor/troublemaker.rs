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

use rand::Rng;
use risingwave_common::array::Op;
use risingwave_common::array::stream_record::{Record, RecordType};
use risingwave_common::field_generator::{FieldGeneratorImpl, VarcharProperty};
use risingwave_common::util::iter_util::ZipEqFast;
use smallvec::SmallVec;

use crate::consistency::insane;
use crate::executor::prelude::*;

/// [`TroublemakerExecutor`] is used to make some trouble in the stream graph. Specifically,
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
    field_generators: Box<[Option<FieldGeneratorImpl>]>,
}

impl TroublemakerExecutor {
    pub fn new(input: Executor, chunk_size: usize) -> Self {
        assert!(insane(), "we should only make trouble in insane mode");
        tracing::info!("we got a troublemaker");
        Self {
            input,
            inner: Inner { chunk_size },
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self { input, inner: this } = self;

        let mut field_generators = vec![];
        for data_type in input.schema().data_types() {
            let field_gen = match data_type {
                t @ (DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64) => {
                    FieldGeneratorImpl::with_number_random(t, None, None, rand::random()).ok()
                }
                DataType::Varchar => Some(FieldGeneratorImpl::with_varchar(
                    &VarcharProperty::RandomVariableLength,
                    rand::random(),
                )),
                _ => None,
            };
            field_generators.push(field_gen);
        }

        let mut vars = Vars {
            chunk_builder: StreamChunkBuilder::new(this.chunk_size, input.schema().data_types()),
            met_delete_before: false,
            field_generators: field_generators.into_boxed_slice(),
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

                        for (op, row) in Self::make_some_trouble(&this, &mut vars, record) {
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
                    assert!(
                        vars.chunk_builder.take().is_none(),
                        "we don't merge chunks across barriers"
                    );
                    yield Message::Barrier(barrier);
                }
                _ => yield msg,
            }
        }
    }

    fn make_some_trouble<'a>(
        _this: &'a Inner,
        vars: &'a mut Vars,
        record: Record<impl Row>,
    ) -> SmallVec<[(Op, OwnedRow); 2]> {
        let record = if vars.met_delete_before && rand::rng().random_bool(0.5) {
            // Change the `Op`.
            // Because we don't know the `append_only` property of the stream, we can't
            // generate `Delete` arbitrarily. So we just generate `Delete` after we saw
            // `Delete` or `Update` before.
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
            // Just convert the rows to owned rows, without changing the `Op`.
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
            .map(|(op, row)| {
                let mut data = row.into_inner();

                for (datum, gen) in data
                    .iter_mut()
                    .zip_eq_fast(vars.field_generators.iter_mut())
                {
                    match rand::rng().random_range(0..4) {
                        0 | 1 => {
                            // don't change the value
                        }
                        2 => {
                            *datum = None;
                        }
                        3 => {
                            *datum = gen
                                .as_mut()
                                .and_then(|gen| gen.generate_datum(rand::random()))
                                .or(datum.take());
                        }
                        _ => unreachable!(),
                    }
                }

                (op, OwnedRow::new(data.into_vec()))
            })
            .collect()
    }
}

impl Execute for TroublemakerExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
