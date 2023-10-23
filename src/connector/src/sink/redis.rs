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
use redis::aio::Connection;
use redis::{Client as RedisClient, Pipeline};
use regex::Regex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;

use super::catalog::SinkFormatDesc;
use super::formatter::SinkFormatterImpl;
use super::writer::FormattedSink;
use super::{SinkError, SinkParam};
use crate::dispatch_sink_formatter_impl;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const REDIS_SINK: &str = "redis";
pub const REDIS_KEY_FORMAT: &str = "redis_key_format";
pub const REDIS_VALUE_FORMAT: &str = "redis_value_format";
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RedisCommon {
    #[serde(rename = "redis.url")]
    pub url: String,
}

impl RedisCommon {
    pub(crate) fn build_client(&self) -> anyhow::Result<RedisClient> {
        let client = RedisClient::open(self.url.clone())?;
        Ok(client)
    }
}
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
    #[serde(flatten)]
    pub common: RedisCommon,
}

impl RedisConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<RedisConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!("{:?}", e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct RedisSink {
    config: RedisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
}

fn check_string_format(format: &str, set: &HashSet<String>) -> Result<()> {
    // We will check if the string inside {} corresponds to a column name in rw.
    // In other words, the content within {} should exclusively consist of column names from rw,
    // which means '{{column_name}}' or '{{column_name1},{column_name2}}' would be incorrect.
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
impl TryFrom<SinkParam> for RedisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        if param.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Redis Sink Primary Key must be specified."
            )));
        }
        let config = RedisConfig::from_hashmap(param.properties.clone())?;
        Ok(Self {
            config,
            schema: param.schema(),
            pk_indices: param.downstream_pk,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
        })
    }
}

impl Sink for RedisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<RedisSinkWriter>;

    const SINK_NAME: &'static str = "redis";

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(RedisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
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
        if matches!(
            self.format_desc.encode,
            super::catalog::SinkEncode::Template
        ) {
            let key_format = self
                .format_desc
                .options
                .get(REDIS_KEY_FORMAT)
                .ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "Cannot find 'redis_key_format',please set it or use JSON"
                    ))
                })?;
            let value_format = self
                .format_desc
                .options
                .get(REDIS_VALUE_FORMAT)
                .ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "Cannot find 'redis_value_format',please set it or use JSON"
                    ))
                })?;
            check_string_format(key_format, &pk_set)?;
            check_string_format(value_format, &all_set)?;
        }
        Ok(())
    }
}

pub struct RedisSinkWriter {
    epoch: u64,
    schema: Schema,
    pk_indices: Vec<usize>,
    formatter: SinkFormatterImpl,
    payload_writer: RedisSinkPayloadWriter,
}

struct RedisSinkPayloadWriter {
    // connection to redis, one per executor
    conn: Option<Connection>,
    // the command pipeline for write-commit
    pipe: Pipeline,
}
impl RedisSinkPayloadWriter {
    pub async fn new(config: RedisConfig) -> Result<Self> {
        let client = config.common.build_client()?;
        let conn = Some(client.get_async_connection().await?);
        let pipe = redis::pipe();

        Ok(Self { conn, pipe })
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        let conn = None;
        let pipe = redis::pipe();
        Self { conn, pipe }
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.pipe.query_async(self.conn.as_mut().unwrap()).await?;
        self.pipe.clear();
        Ok(())
    }
}

impl FormattedSink for RedisSinkPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        let k = k.unwrap();
        match v {
            Some(v) => self.pipe.set(k, v),
            None => self.pipe.del(k),
        };
        Ok(())
    }
}

impl RedisSinkWriter {
    pub async fn new(
        config: RedisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
    ) -> Result<Self> {
        let payload_writer = RedisSinkPayloadWriter::new(config.clone()).await?;
        let formatter =
            SinkFormatterImpl::new_with_redis(schema.clone(), pk_indices.clone(), format_desc)?;

        Ok(Self {
            schema,
            pk_indices,
            epoch: 0,
            formatter,
            payload_writer,
        })
    }

    #[cfg(test)]
    pub fn mock(
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
    ) -> Result<Self> {
        let formatter =
            SinkFormatterImpl::new_with_redis(schema.clone(), pk_indices.clone(), format_desc)?;
        Ok(Self {
            schema,
            pk_indices,
            epoch: 0,
            formatter,
            payload_writer: RedisSinkPayloadWriter::mock(),
        })
    }
}

#[async_trait]
impl SinkWriter for RedisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        dispatch_sink_formatter_impl!(&self.formatter, formatter, {
            self.payload_writer.write_chunk(chunk, formatter).await
        })
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            self.payload_writer.commit().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use rdkafka::message::FromBytes;
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;
    use crate::sink::catalog::{SinkEncode, SinkFormat};

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

        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Json,
            options: BTreeMap::default(),
        };

        let mut redis_sink_writer = RedisSinkWriter::mock(schema, vec![0], &format_desc).unwrap();

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
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
            .payload_writer
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

        let mut btree_map = BTreeMap::default();
        btree_map.insert(REDIS_KEY_FORMAT.to_string(), "key-{id}".to_string());
        btree_map.insert(
            REDIS_VALUE_FORMAT.to_string(),
            "values:{id:{id},name:{name}}".to_string(),
        );
        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Template,
            options: btree_map,
        };

        let mut redis_sink_writer = RedisSinkWriter::mock(schema, vec![0], &format_desc).unwrap();

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
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
            .payload_writer
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
