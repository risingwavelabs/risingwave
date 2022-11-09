// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
    pub buffer: Vec<String>,
    pub prefix: String,
    pub suffix: String,
}

impl ConsoleSink {
    pub async fn new(config: ConsoleConfig) -> Result<Self> {
        Ok(ConsoleSink {
            epoch: 0,
            buffer: vec![],
            prefix: config.prefix.unwrap_or("".to_string()),
            suffix: config.suffix.unwrap_or("".to_string()),
        })
    }

    fn parse_datum(datum: DatumRef<'_>) -> String {
        match datum {
            None => "NULL".to_string(),
            Some(ScalarRefImpl::Int32(v)) => v.to_string(),
            Some(ScalarRefImpl::Int64(v)) => v.to_string(),
            Some(ScalarRefImpl::Float32(v)) => v.to_string(),
            Some(ScalarRefImpl::Float64(v)) => v.to_string(),
            Some(ScalarRefImpl::Decimal(v)) => v.to_string(),
            Some(ScalarRefImpl::Utf8(v)) => v.to_string(),
            Some(ScalarRefImpl::Bool(v)) => v.to_string(),
            Some(ScalarRefImpl::NaiveDate(v)) => v.to_string(),
            Some(ScalarRefImpl::NaiveTime(v)) => v.to_string(),
            Some(ScalarRefImpl::Interval(v)) => v.to_string(),
            _ => unimplemented!(),
        }
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn write_batch(&mut self, chunk: StreamChunk, _schema: &Schema) -> Result<()> {
        for (op, row_ref) in chunk.rows() {
            let row_repr = join(row_ref.values().map(Self::parse_datum), ",");
            let op_repr = match op {
                Op::Insert => "Insert",
                Op::UpdateDelete => "UpdateDelete",
                Op::UpdateInsert => "UpdateInsert",
                Op::Delete => "Delete",
            };
            self.buffer.push(format!("{}{}", op_repr, row_repr));
        }

        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        for row in self.buffer.clone() {
            println!("{}{}{}", self.prefix, row, self.suffix)
        }
        self.buffer.clear();
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use crate::sink::console::{ConsoleConfig, ConsoleSink};
    use crate::sink::Sink;

    #[test]
    fn test_console_sink() {
        futures::executor::block_on(async {
            let mut console_sink = ConsoleSink::new(ConsoleConfig {
                prefix: Option::from("[CONSOLE] ".to_string()),
                suffix: Option::from(";".to_string()),
            })
            .await
            .unwrap();

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
            ]);

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
                ],
                None,
            );

            console_sink.begin_epoch(0).await.unwrap();
            console_sink
                .write_batch(chunk.clone(), &schema)
                .await
                .expect("Error");
            console_sink.commit().await.expect("Error");

            console_sink.begin_epoch(1).await.unwrap();
            console_sink
                .write_batch(chunk_2.clone(), &schema)
                .await
                .expect("Error");
            console_sink.commit().await.expect("Error");
        });
    }
}
