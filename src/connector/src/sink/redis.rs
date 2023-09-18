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

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use async_trait::async_trait;
use redis::{Connection, Pipeline};
use regex::Regex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::Deserialize;
use serde_with::serde_as;

use super::encoder::template::TemplateEncoder;
use super::encoder::{JsonEncoder, TimestampHandlingMode};
use super::formatter::{AppendOnlyFormatter, UpsertFormatter};
use super::{
    FormattedSink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
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
    pub fn new(config: RedisConfig, param: SinkParam) -> Result<Self> {
        if param.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Redis Sink Primary Key must be specified."
            )));
        }
        Ok(Self {
            config,
            schema: param.schema(),
            is_append_only: param.sink_type.is_append_only(),
            pk_indices: param.downstream_pk,
        })
    }
}

fn check_string_format(format: &str, set: &HashSet<String>) -> Result<()> {
    let re = Regex::new(r"\{([^}]*)\}").unwrap();
    if !re.is_match(format) {
        return Err(SinkError::Redis(
            "Can't find {} in key_format or value_format".to_string(),
        ));
    }
    for capture in re.captures_iter(format) {
        if let Some(inner_content) = capture.get(1) && !set.contains(inner_content.as_str()){
            return Err(SinkError::Redis(format!("Can't find field({:?}) in key_format or value_format",inner_content.as_str())))
        }
    }
    Ok(())
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
        let all_set: HashSet<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        let pk_set: HashSet<String> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(k, _)| self.pk_indices.contains(k))
            .map(|(_, v)| v.name.clone())
            .collect();
        match (
            &self.config.common.key_format,
            &self.config.common.value_format,
        ) {
            (Some(key_format), Some(value_format)) => {
                check_string_format(key_format, &pk_set)?;
                check_string_format(value_format, &all_set)?;
            }
            (Some(key_format), None) => {
                check_string_format(key_format, &pk_set)?;
            }
            (None, Some(value_format)) => {
                check_string_format(value_format, &all_set)?;
            }
            (None, None) => {}
        };
        Ok(())
    }
}

impl FormattedSink for Pipeline {
    type K = String;
    type V = String;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        let k = k.unwrap();
        match v {
            Some(v) => self.set(k, v),
            None => self.del(k),
        };
        Ok(())
    }
}
pub struct RedisSinkWriter {
    // connection to redis, one per executor
    conn: Option<Connection>,
    // the command pipeline for write-commit
    pipe: Pipeline,
    kv_format: (Option<String>, Option<String>),
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

        Ok(Self {
            schema,
            pk_indices,
            is_append_only,
            conn,
            pipe,
            epoch: 0,
            kv_format: (config.common.key_format, config.common.value_format),
        })
    }

    #[cfg(test)]
    pub fn mock(
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        kv_format: (Option<String>, Option<String>),
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
            kv_format,
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        match &self.kv_format {
            (Some(k), Some(v)) => {
                let key_encoder = TemplateEncoder::new(&self.schema, Some(&self.pk_indices), k);
                let val_encoder = TemplateEncoder::new(&self.schema, None, v);
                let a = AppendOnlyFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (None, None) => {
                let key_encoder = JsonEncoder::new(
                    &self.schema,
                    Some(&self.pk_indices),
                    TimestampHandlingMode::Milli,
                );
                let val_encoder =
                    JsonEncoder::new(&self.schema, None, TimestampHandlingMode::Milli);
                let a = AppendOnlyFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (None, Some(v)) => {
                let key_encoder = JsonEncoder::new(
                    &self.schema,
                    Some(&self.pk_indices),
                    TimestampHandlingMode::Milli,
                );
                let val_encoder = TemplateEncoder::new(&self.schema, None, v);
                let a = AppendOnlyFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (Some(k), None) => {
                let key_encoder = TemplateEncoder::new(&self.schema, Some(&self.pk_indices), k);
                let val_encoder =
                    JsonEncoder::new(&self.schema, None, TimestampHandlingMode::Milli);
                let a = AppendOnlyFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
        }
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        match &self.kv_format {
            (Some(k), Some(v)) => {
                let key_encoder = TemplateEncoder::new(&self.schema, Some(&self.pk_indices), k);
                let val_encoder = TemplateEncoder::new(&self.schema, None, v);
                let a = UpsertFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (None, None) => {
                let key_encoder = JsonEncoder::new(
                    &self.schema,
                    Some(&self.pk_indices),
                    TimestampHandlingMode::Milli,
                );
                let val_encoder =
                    JsonEncoder::new(&self.schema, None, TimestampHandlingMode::Milli);
                let a = UpsertFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (None, Some(v)) => {
                let key_encoder = JsonEncoder::new(
                    &self.schema,
                    Some(&self.pk_indices),
                    TimestampHandlingMode::Milli,
                );
                let val_encoder = TemplateEncoder::new(&self.schema, None, v);
                let a = UpsertFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
            (Some(k), None) => {
                let key_encoder = TemplateEncoder::new(&self.schema, Some(&self.pk_indices), k);
                let val_encoder =
                    JsonEncoder::new(&self.schema, None, TimestampHandlingMode::Milli);
                let a = UpsertFormatter::new(key_encoder, val_encoder);
                self.pipe.write_chunk(chunk, a).await
            }
        }
    }
}

#[async_trait]
impl SinkWriter for RedisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
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
}

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

        let mut redis_sink_writer =
            RedisSinkWriter::mock(schema, vec![0], true, (None, None)).unwrap();

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
        let expected_a =
            vec![
            (0, "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":1}\r\n$23\r\n{\"id\":1,\"name\":\"Alice\"}\r\n"),
            (1, "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":2}\r\n$21\r\n{\"id\":2,\"name\":\"Bob\"}\r\n"),
            (2, "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":3}\r\n$23\r\n{\"id\":3,\"name\":\"Clare\"}\r\n"),
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
            (
                Some("key-{id}".to_string()),
                Some("values:{id:{id},name:{name}}".to_string()),
            ),
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
