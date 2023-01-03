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
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use itertools::Itertools;
use redis::{ConnectionLike, RedisResult, Value};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::to_text::ToText;

use crate::sink::{Result, Sink, SinkError};

pub const REDIS_SINK: &str = "redis";

#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub endpoint: String,
    pub key_format: Option<String>,
    pub value_format: Option<String>,
}

impl RedisConfig {
    pub fn from_hashmap(map: HashMap<String, String>) -> Result<Self> {
        let endpoint = map
            .get("endpoint")
            .ok_or_else(|| SinkError::Config("endpoint".to_string()))?
            .to_string();
        let key_format = map.get("key.format").map(|s| s.to_string());
        let value_format = map.get("value.format").map(|s| s.to_string());
        Ok(RedisConfig {
            endpoint,
            key_format,
            value_format,
        })
    }
}

pub struct RedisSink {
    config: RedisConfig,
    schema: Schema,
    // connection to redis, one per executor
    client: Option<redis::Client>,
    conn: RedisConnectionImpl,
    // the command pipeline for write-commit
    pipe: redis::Pipeline,
    kv_formatter: Option<RedisKvFormatter>,
    batch_id: u64,
    epoch: u64,
    update_cache: Vec<String>,
    pk_indices: Vec<usize>,
}

pub enum RedisConnectionImpl {
    Redis(redis::Connection),
    Mock(MockConnection),
}

impl Debug for RedisSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSink")
            .field("config", &self.config)
            .field("schema", &self.schema)
            .field("client", &self.client)
            .field("batch_id", &self.batch_id)
            .field("epoch", &self.epoch)
            .field("update_cache", &self.update_cache)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl RedisSink {
    pub fn new(cfg: RedisConfig, schema: Schema, pk_indices: Vec<usize>) -> Result<Self> {
        let client = redis::Client::open(cfg.endpoint.clone())?;
        let conn = client.get_connection()?;
        let pipe = redis::pipe();
        let config = cfg.clone();
        let kv_formatter = match (cfg.key_format, cfg.value_format) {
            (Some(key_format), Some(value_format)) => Some(RedisKvFormatter::new(
                key_format.as_str(),
                value_format.as_str(),
                schema.clone(),
            )?),
            _ => None,
        };
        Ok(Self {
            config,
            schema,
            client: Some(client),
            conn: RedisConnectionImpl::Redis(conn),
            pipe,
            kv_formatter,
            batch_id: 0,
            epoch: 0,
            update_cache: Vec::with_capacity(1),
            pk_indices,
        })
    }

    #[cfg(test)]
    fn for_test(schema: Schema, pk_indices: Vec<usize>) -> Result<Self> {
        let pipe = redis::pipe();
        Ok(Self {
            config: RedisConfig {
                endpoint: "".parse().unwrap(),
                key_format: None,
                value_format: None,
            },
            schema,
            client: None,
            conn: RedisConnectionImpl::Mock(MockConnection {}),
            pipe,
            kv_formatter: None,
            batch_id: 0,
            epoch: 0,
            update_cache: Vec::new(),
            pk_indices,
        })
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

pub struct MockConnection {}

#[derive(Clone, Debug)]
struct FormatString {
    // fields that appear in the format string
    fields: Vec<String>,
    // parts of the format string that are literal text
    literals: Vec<String>,
}

impl FormatString {
    fn new(s: &str) -> Result<Self> {
        let mut fields = Vec::new();
        let mut literals = Vec::new();
        let mut current_literal = String::new();

        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '{' {
                literals.push(current_literal);
                current_literal = String::new();

                let mut field_name = String::new();
                for c in chars.by_ref() {
                    if c == '}' {
                        break;
                    }
                    field_name.push(c);
                }
                if field_name.is_empty() {
                    return Err(SinkError::Config(
                        "empty field name in format string".to_string(),
                    ));
                }
                fields.push(field_name);
            } else {
                current_literal.push(c);
            }
        }
        literals.push(current_literal);

        Ok(Self { fields, literals })
    }
}

#[derive(Clone, Debug)]
pub struct RedisKvFormatter {
    key_format: FormatString,
    value_format: FormatString,
    field_index: HashMap<String, usize>,
    schema: Schema,
}

impl RedisKvFormatter {
    pub fn new(key_format: &str, value_format: &str, schema: Schema) -> Result<Self> {
        let key_format = FormatString::new(key_format)?;
        let value_format = FormatString::new(value_format)?;
        let field_index: HashMap<String, usize> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        for field in key_format.fields.iter().chain(value_format.fields.iter()) {
            if !field_index.contains_key(field) {
                return Err(SinkError::Config(format!(
                    "field {} not found in schema",
                    field
                )));
            }
        }
        Ok(Self {
            key_format,
            value_format,
            field_index,
            schema,
        })
    }

    pub fn format_key(&self, row: RowRef<'_>) -> Result<String> {
        let mut key = String::new();

        let mut fields = self.key_format.fields.iter().peekable();
        let mut literals = self.key_format.literals.iter().peekable();
        while let (Some(field), Some(literal)) = (fields.next(), literals.next()) {
            key.push_str(literal);
            let field_name = field;
            let field_index = self
                .field_index
                .get(field_name)
                .ok_or_else(|| SinkError::Config(format!("field {} not found", field_name)))?;
            let field_value = row.datum_at(*field_index).to_text();
            key.push_str(&field_value);
        }
        if let Some(literal) = literals.next() {
            key.push_str(literal);
        }
        Ok(key)
    }

    pub fn format_value(&self, row: RowRef<'_>) -> Result<String> {
        let mut value = String::new();

        let mut fields = self.value_format.fields.iter().peekable();
        let mut literals = self.value_format.literals.iter().peekable();
        while let (Some(field), Some(literal)) = (fields.next(), literals.next()) {
            value.push_str(literal);
            let field_name = field;
            let field_index = self
                .field_index
                .get(field_name)
                .ok_or_else(|| SinkError::Config(format!("field {} not found", field_name)))?;
            let field_value = row.datum_at(*field_index).to_text();
            value.push_str(&field_value);
        }
        if let Some(literal) = literals.next() {
            value.push_str(literal);
        }
        Ok(value)
    }
}

impl ConnectionLike for MockConnection {
    fn req_packed_command(&mut self, _cmd: &[u8]) -> RedisResult<Value> {
        Ok(Value::Okay)
    }

    fn req_packed_commands(
        &mut self,
        _cmd: &[u8],
        _offset: usize,
        _count: usize,
    ) -> RedisResult<Vec<Value>> {
        Ok(Vec::new())
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn check_connection(&mut self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        true
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => match &self.kv_formatter {
                    Some(formatter) => {
                        let key = formatter.format_key(row)?;
                        let value = formatter.format_value(row)?;
                        self.pipe.set(key, value);
                    }
                    _ => {
                        let key = Self::default_redis_key(row, &self.pk_indices);
                        let value = Self::default_redis_value(row);
                        self.pipe.set(key, value);
                    }
                },
                Op::Delete => match &self.kv_formatter {
                    Some(formatter) => {
                        let key = formatter.format_key(row)?;
                        self.pipe.del(key);
                    }
                    _ => {
                        let key = Self::default_redis_key(row, &self.pk_indices);
                        self.pipe.del(key);
                    }
                },
                Op::UpdateDelete => {
                    let key = match &self.kv_formatter {
                        Some(formatter) => formatter.format_key(row)?,
                        None => Self::default_redis_key(row, &self.pk_indices),
                    };
                    self.update_cache.push(key);
                }
                Op::UpdateInsert => {
                    let value = match &self.kv_formatter {
                        Some(formatter) => formatter.format_value(row)?,
                        None => Self::default_redis_value(row),
                    };
                    self.pipe.set(
                        self.update_cache
                            .pop()
                            .ok_or_else(|| SinkError::Redis("no update insert".to_string()))?,
                        value,
                    );
                }
            }
        }
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        self.pipe = redis::pipe();
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        match &mut self.conn {
            RedisConnectionImpl::Redis(conn) => {
                self.pipe.query(conn)?;
            }
            RedisConnectionImpl::Mock(conn) => {
                self.pipe.query(conn)?;
            }
        }
        self.pipe.clear();
        self.update_cache.clear();
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.update_cache.clear();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use rdkafka::message::FromBytes;
    use risingwave_common::array;
    use risingwave_common::array::{I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::sink::Sink;

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

        let mut redis_sink =
            RedisSink::for_test(schema, vec![0]).expect("failed to create redis sink");

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                array! {I32Array, [Some(1), Some(2), Some(3)]}.into(),
                array! {Utf8Array, [Some("Alice"), Some("Bob"), Some("Clare")]}.into(),
            ],
            None,
        );

        let chunk_b = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                array! {I32Array, [Some(4), Some(5), Some(6)]}.into(),
                array! {Utf8Array, [Some("David"), Some("Eve"), Some("Frank")]}.into(),
            ],
            None,
        );

        redis_sink
            .write_batch(chunk_a)
            .await
            .expect("failed to write batch");
        let expected_a = vec![
            (0, "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$9\r\n[1,Alice]\r\n"),
            (1, "*3\r\n$3\r\nSET\r\n$1\r\n2\r\n$7\r\n[2,Bob]\r\n"),
            (2, "*3\r\n$3\r\nSET\r\n$1\r\n3\r\n$9\r\n[3,Clare]\r\n"),
        ];

        redis_sink
            .pipe
            .cmd_iter()
            .enumerate()
            .zip_eq(expected_a.clone())
            .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                if exp_i == i {
                    assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                }
            });

        redis_sink.commit().await.expect("commit failed");
        redis_sink
            .write_batch(chunk_b)
            .await
            .expect("failed to write batch");
        let expected_b = vec![
            (0, "*3\r\n$3\r\nSET\r\n$1\r\n4\r\n$9\r\n[4,David]\r\n"),
            (1, "*3\r\n$3\r\nSET\r\n$1\r\n5\r\n$7\r\n[5,Eve]\r\n"),
            (2, "*3\r\n$3\r\nSET\r\n$1\r\n6\r\n$9\r\n[6,Frank]\r\n"),
        ];

        redis_sink
            .pipe
            .cmd_iter()
            .enumerate()
            .zip_eq(expected_b.clone())
            .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                if exp_i == i {
                    assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                }
            });
    }
}
