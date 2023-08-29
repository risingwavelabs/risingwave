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

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use redis::{Connection, Pipeline};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::Deserialize;
use serde_with::serde_as;

use super::{SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::common::RedisCommon;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const REDIS_SINK: &str = "redis";

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
    #[serde(flatten)]
    pub common: RedisCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

impl RedisConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<RedisConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct RedisSink {
    config: RedisConfig,
    schema: Schema,
    is_append_only: bool,
    pk_indices: Vec<usize>,
}

impl RedisSink {
    pub fn new(
        config: RedisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        if pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Redis Sink Primary Key must be specified."
            )));
        }
        Ok(Self {
            config,
            schema,
            is_append_only,
            pk_indices,
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = RedisSinkWriter;

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        Ok(RedisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )?)
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        let client = self.config.common.build_client()?;
        client.get_connection()?;
        Ok(())
    }
}

pub struct RedisSinkWriter {
    // connection to redis, one per executor
    conn: Option<Connection>,
    // the command pipeline for write-commit
    pipe: Pipeline,
    kv_formatter: Option<(String, String)>,
    epoch: u64,
    schema: Schema,
    is_append_only: bool,
    pk_indices: Vec<usize>,
}
impl RedisSinkWriter {
    pub fn new(
        config: RedisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = config.common.build_client()?;
        let conn = Some(client.get_connection()?);
        let pipe = redis::pipe();
        let kv_formatter = match (config.common.key_format, config.common.value_format) {
            (Some(key_format), Some(value_format)) => Some((key_format, value_format)),
            _ => None,
        };
        Ok(Self {
            schema,
            pk_indices,
            is_append_only,
            conn,
            pipe,
            epoch: 0,
            kv_formatter,
        })
    }

    #[cfg(test)]
    pub fn mock(
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        kv_formatter: Option<(String, String)>,
    ) -> Result<Self> {
        let conn = None;
        let pipe = redis::pipe();
        Ok(Self {
            schema,
            pk_indices,
            is_append_only,
            conn,
            pipe,
            epoch: 0,
            kv_formatter,
        })
    }

    fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            self.get_redis_key_values(row).map(|(key, value)| {
                self.pipe.set(key, value);
            })?;
        }
        Ok(())
    }

    fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => self.get_redis_key_values(row).map(|(key, value)| {
                    self.pipe.set(key, value);
                })?,
                Op::Delete => self.get_redis_key_values(row).map(|(key, _)| {
                    self.pipe.del(key);
                })?,
                Op::UpdateDelete => {}
                Op::UpdateInsert => self.get_redis_key_values(row).map(|(key, value)| {
                    self.pipe.set(key, value);
                })?,
            }
        }
        Ok(())
    }

    fn get_redis_key_values(&mut self, row: RowRef<'_>) -> Result<(String, String)> {
        match &self.kv_formatter {
            Some((key, value)) => {
                let mut key = key.clone();
                let mut value = value.clone();
                for (name, data) in self.schema.names_str().iter().zip_eq_debug(row.iter()) {
                    key = key.replace(&format!("{{{}}}", name), &data.to_text());
                    value = value.replace(&format!("{{{}}}", name), &data.to_text());
                }
                Ok((key, value))
            }
            _ => {
                let key = Self::default_redis_key(row, &self.pk_indices);
                let value = Self::default_redis_value(row);
                Ok((key, value))
            }
        }
    }

    pub fn default_redis_key(row: RowRef<'_>, pk_indices: &[usize]) -> String {
        pk_indices
            .iter()
            .map(|i| row.datum_at(*i).to_text())
            .join(":")
    }

    pub fn default_redis_value(row: RowRef<'_>) -> String {
        format!("[{}]", row.iter().map(|v| v.to_text()).join(","))
    }
}

#[async_trait]
impl SinkWriter for RedisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk)
        } else {
            self.upsert(chunk)
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.pipe.clear();
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            self.pipe.query(&mut self.conn.as_mut().unwrap())?;
            self.pipe.clear();
        }
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        todo!()
    }
}

// #[derive(Clone, Debug)]
// struct FormatString {
//     // fields that appear in the format string
//     fields: Vec<String>,
//     // parts of the format string that are literal text
//     literals: Vec<String>,
// }

// impl FormatString {
//     fn new(s: &str) -> Result<Self> {
//         let mut fields = Vec::new();
//         let mut literals = Vec::new();
//         let mut current_literal = String::new();

//         let mut chars = s.chars();
//         while let Some(c) = chars.next() {
//             if c == '{' {
//                 literals.push(current_literal);
//                 current_literal = String::new();

//                 let mut field_name = String::new();
//                 for c in chars.by_ref() {
//                     if c == '}' {
//                         break;
//                     }
//                     field_name.push(c);
//                 }
//                 if field_name.is_empty() {
//                     return Err(SinkError::Config(anyhow!(
//                         "empty field name in format string".to_string()
//                     )));
//                 }
//                 fields.push(field_name);
//             } else {
//                 current_literal.push(c);
//             }
//         }
//         literals.push(current_literal);

//         Ok(Self { fields, literals })
//     }
// }

// #[derive(Clone, Debug)]
// pub struct RedisKvFormatter {
//     key_format: FormatString,
//     value_format: FormatString,
//     field_index: HashMap<String, usize>,
//     schema: Schema,
// }

// impl RedisKvFormatter {
//     pub fn new(key_format: &str, value_format: &str, schema: Schema) -> Result<Self> {
//         let key_format = FormatString::new(key_format)?;
//         let value_format = FormatString::new(value_format)?;
//         let field_index: HashMap<String, usize> = schema
//             .fields()
//             .iter()
//             .enumerate()
//             .map(|(i, f)| (f.name.clone(), i))
//             .collect();
//         for field in key_format.fields.iter().chain(value_format.fields.iter()) {
//             if !field_index.contains_key(field) {
//                 return Err(SinkError::Config(anyhow!(
//                     "field {} not found in schema",
//                     field
//                 )));
//             }
//         }
//         Ok(Self {
//             key_format,
//             value_format,
//             field_index,
//             schema,
//         })
//     }

//     pub fn format_key(&self, row: RowRef<'_>) -> Result<String> {
//         let mut key = String::new();
//         let mut fields = self.key_format.fields.iter();
//         let mut literals = self.key_format.literals.iter();
//         while let (Some(field), Some(literal)) = (fields.next(), literals.next()) {
//             key.push_str(literal);
//             let field_name = field;
//             let field_index = self
//                 .field_index
//                 .get(field_name)
//                 .ok_or_else(|| SinkError::Config(anyhow!("field {} not found", field_name)))?;
//             let field_value = row.datum_at(*field_index).to_text();
//             key.push_str(&field_value);
//         }
//         if let Some(literal) = literals.next() {
//             key.push_str(literal);
//         }
//         Ok(key)
//     }

//     pub fn format_value(&self, row: RowRef<'_>) -> Result<String> {
//         let mut value = String::new();

//         let mut fields = self.value_format.fields.iter();
//         let mut literals = self.value_format.literals.iter();
//         while let (Some(field), Some(literal)) = (fields.next(), literals.next()) {
//             value.push_str(literal);
//             let field_name = field;
//             let field_index = self
//                 .field_index
//                 .get(field_name)
//                 .ok_or_else(|| SinkError::Config(anyhow!("field {} not found", field_name)))?;
//             let field_value = row.datum_at(*field_index).to_text();
//             value.push_str(&field_value);
//         }
//         if let Some(literal) = literals.next() {
//             value.push_str(literal);
//         }
//         Ok(value)
//     }
// }

#[cfg(test)]
mod test {
    use rdkafka::message::FromBytes;
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;

    #[tokio::test]
    async fn test_write() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_string(),
                sub_fields: vec![],
                type_name: "string".to_string(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_string(),
                sub_fields: vec![],
                type_name: "string".to_string(),
            },
        ]);

        let mut redis_sink_writer = RedisSinkWriter::mock(schema, vec![0], true, None).unwrap();

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
            None,
        );

        redis_sink_writer
            .write_batch(chunk_a)
            .await
            .expect("failed to write batch");
        let expected_a = vec![
            (0, "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$9\r\n[1,Alice]\r\n"),
            (1, "*3\r\n$3\r\nSET\r\n$1\r\n2\r\n$7\r\n[2,Bob]\r\n"),
            (2, "*3\r\n$3\r\nSET\r\n$1\r\n3\r\n$9\r\n[3,Clare]\r\n"),
        ];

        redis_sink_writer
            .pipe
            .cmd_iter()
            .enumerate()
            .zip_eq_debug(expected_a.clone())
            .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                if exp_i == i {
                    assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                }
            });
    }

    #[tokio::test]
    async fn test_format_write() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_string(),
                sub_fields: vec![],
                type_name: "string".to_string(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_string(),
                sub_fields: vec![],
                type_name: "string".to_string(),
            },
        ]);

        let mut redis_sink_writer = RedisSinkWriter::mock(
            schema,
            vec![0],
            true,
            Some((
                "key-{id}".to_string(),
                "values:{id:{id},name:{name}}".to_string(),
            )),
        )
        .unwrap();

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
            None,
        );

        redis_sink_writer
            .write_batch(chunk_a)
            .await
            .expect("failed to write batch");
        let expected_a = vec![
            (
                0,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-1\r\n$24\r\nvalues:{id:1,name:Alice}\r\n",
            ),
            (
                1,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-2\r\n$22\r\nvalues:{id:2,name:Bob}\r\n",
            ),
            (
                2,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-3\r\n$24\r\nvalues:{id:3,name:Clare}\r\n",
            ),
        ];

        redis_sink_writer
            .pipe
            .cmd_iter()
            .enumerate()
            .zip_eq_debug(expected_a.clone())
            .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                if exp_i == i {
                    assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                }
            });
    }
}
