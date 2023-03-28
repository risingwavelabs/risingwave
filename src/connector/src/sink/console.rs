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

use std::collections::HashMap;

use async_trait::async_trait;
use itertools::join;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DatumRef, ScalarRefImpl};

use crate::sink::{Result, Sink};

pub const CONSOLE_SINK: &str = "console";

#[derive(Clone, Debug)]
pub struct ConsoleConfig {
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

impl ConsoleConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        Ok(ConsoleConfig {
            prefix: values.get("prefix").cloned(),
            suffix: values.get("suffix").cloned(),
        })
    }
}

#[derive(Debug)]
pub struct ConsoleSink {
    pub epoch: u64,
    pub schema: Schema,
    pub buffer: Vec<String>,
    pub prefix: String,
    pub suffix: String,
}

impl ConsoleSink {
    pub fn new(config: ConsoleConfig, schema: Schema) -> Result<Self> {
        Ok(ConsoleSink {
            epoch: 0,
            schema,
            buffer: vec![],
            prefix: config.prefix.unwrap_or_default(),
            suffix: config.suffix.unwrap_or_default(),
        })
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row_ref) in chunk.rows() {
            let row_repr = join(row_ref.iter().map(parse_datum), ", ");
            let op_repr = match op {
                Op::Insert => "INSERT",
                Op::UpdateDelete => "UPDATE_DELETE",
                Op::UpdateInsert => "UPDATE_INSERT",
                Op::Delete => "DELETE",
            };
            println!("{}{} [{}]{}", self.prefix, op_repr, row_repr, self.suffix);
        }

        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        println!(
            "{}COMMIT [Epoch({})]{}",
            self.prefix, self.epoch, self.suffix
        );
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

fn parse_datum(datum: DatumRef<'_>) -> String {
    match datum {
        None => "NULL".to_string(),
        Some(ScalarRefImpl::Int32(v)) => format!("Int32({})", v),
        Some(ScalarRefImpl::Int64(v)) => format!("Int64({})", v),
        Some(ScalarRefImpl::Float32(v)) => format!("Float32({})", v),
        Some(ScalarRefImpl::Float64(v)) => format!("Float64({})", v),
        Some(ScalarRefImpl::Decimal(v)) => format!("Decimal({})", v),
        Some(ScalarRefImpl::Utf8(v)) => format!("Utf8(\"{}\")", v),
        Some(ScalarRefImpl::Bool(v)) => format!("Bool({})", v),
        Some(ScalarRefImpl::Date(v)) => format!("Date({})", v),
        Some(ScalarRefImpl::Time(v)) => format!("Time({})", v),
        Some(ScalarRefImpl::Interval(v)) => format!("Interval({})", v),
        Some(ScalarRefImpl::Struct(v)) => {
            format!(
                "Struct({})",
                join(v.fields_ref().iter().map(|sub_v| parse_datum(*sub_v)), ", ")
            )
        }
        Some(ScalarRefImpl::List(v)) => {
            format!(
                "List({})",
                join(v.values_ref().iter().map(|sub_v| parse_datum(*sub_v)), ", ")
            )
        }
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{
        ArrayImpl, I32Array, ListArray, Op, StreamChunk, StructArray, Utf8Array,
    };
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use crate::sink::console::{ConsoleConfig, ConsoleSink};
    use crate::sink::Sink;

    #[test]
    fn test_console_sink() {
        futures::executor::block_on(async {
            let schema = Schema::new(vec![
                Field {
                    data_type: DataType::Int32,
                    name: "id".into(),
                    sub_fields: vec![],
                    type_name: "".into(),
                },
                Field {
                    data_type: DataType::Varchar,
                    name: "name".into(),
                    sub_fields: vec![],
                    type_name: "".into(),
                },
                Field {
                    data_type: DataType::new_struct(
                        vec![DataType::List {
                            datatype: Box::new(DataType::Varchar),
                        }],
                        vec!["addresses".into()],
                    ),
                    name: "notes".to_string(),
                    sub_fields: vec![Field {
                        data_type: DataType::List {
                            datatype: Box::new(DataType::Varchar),
                        },
                        name: "addresses".into(),
                        sub_fields: vec![],
                        type_name: "".into(),
                    }],
                    type_name: "".to_string(),
                },
            ]);

            let mut console_sink = ConsoleSink::new(
                ConsoleConfig {
                    prefix: Option::from("[CONSOLE] ".to_string()),
                    suffix: Option::from(";".to_string()),
                },
                schema,
            )
            .unwrap();

            let chunk = StreamChunk::new(
                vec![Op::Insert, Op::Insert, Op::Insert],
                vec![
                    Column::new(Arc::new(ArrayImpl::from(array!(
                        I32Array,
                        [Some(1), Some(2), Some(3)]
                    )))),
                    Column::new(Arc::new(ArrayImpl::from(array!(
                        Utf8Array,
                        [Some("Alice"), Some("Bob"), Some("Clare")]
                    )))),
                    Column::new(Arc::new(ArrayImpl::from(StructArray::from_slices(
                        &[true, true, true],
                        vec![ArrayImpl::from(ListArray::from_iter(
                            vec![
                                Some(ArrayImpl::from(array!(Utf8Array, [Some("1 Poultry Rd")]))),
                                Some(ArrayImpl::from(array!(Utf8Array, [Some("50 Vesey St")]))),
                                Some(ArrayImpl::from(array!(
                                    Utf8Array,
                                    [Some("1 Poultry Rd"), Some("30 Raffles Pl")]
                                ))),
                            ],
                            DataType::Varchar,
                        ))],
                        vec![DataType::List {
                            datatype: Box::new(DataType::Varchar),
                        }],
                    )))),
                ],
                None,
            );

            let chunk_2 = StreamChunk::new(
                vec![Op::Insert, Op::Insert, Op::Insert],
                vec![
                    Column::new(Arc::new(ArrayImpl::from(array!(
                        I32Array,
                        [Some(4), Some(5), Some(6)]
                    )))),
                    Column::new(Arc::new(ArrayImpl::from(array!(
                        Utf8Array,
                        [Some("David"), Some("Eve"), Some("Frank")]
                    )))),
                    Column::new(Arc::new(ArrayImpl::from(StructArray::from_slices(
                        &[true, true, true],
                        vec![ArrayImpl::from(ListArray::from_iter(
                            vec![
                                Some(ArrayImpl::from(array!(
                                    Utf8Array,
                                    [Some("30 Churchill Pl")]
                                ))),
                                Some(ArrayImpl::from(array!(
                                    Utf8Array,
                                    [Some("36 Robinson Rd"), Some("160 Spear St")]
                                ))),
                                Some(ArrayImpl::from(array!(Utf8Array, [Some("160 Spear St")]))),
                            ],
                            DataType::Varchar,
                        ))],
                        vec![DataType::List {
                            datatype: Box::new(DataType::Varchar),
                        }],
                    )))),
                ],
                None,
            );

            console_sink.begin_epoch(0).await.unwrap();
            console_sink
                .write_batch(chunk.clone())
                .await
                .expect("Error");
            console_sink.commit().await.expect("Error");

            console_sink.begin_epoch(1).await.unwrap();
            console_sink
                .write_batch(chunk_2.clone())
                .await
                .expect("Error");
            console_sink.commit().await.expect("Error");
        });
    }
}
