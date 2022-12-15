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
use risingwave_common::types::to_text::ToText;

use crate::sink::{Result, Sink};

pub const REDIS_SINK: &str = "redis";

#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub endpoint: String,
}

pub struct RedisSink {
    config: RedisConfig,
    schema: Schema,
    // connection to redis, one per executor
    client: Option<redis::Client>,
    conn: RedisConnectionImpl,
    // the command pipeline for write-commit
    pipe: redis::Pipeline,
    batch_id: u64,
    epoch: u64,
    update_cache: HashMap<String, String>,
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
        Ok(Self {
            config: cfg,
            schema,
            client: Some(client),
            conn: RedisConnectionImpl::Redis(conn),
            pipe,
            batch_id: 0,
            epoch: 0,
            update_cache: HashMap::new(),
            pk_indices,
        })
    }

    #[cfg(test)]
    fn for_test(schema: Schema, pk_indices: Vec<usize>) -> Result<Self> {
        let pipe = redis::pipe();
        Ok(Self {
            config: RedisConfig {
                endpoint: "".parse().unwrap(),
            },
            schema,
            client: None,
            conn: RedisConnectionImpl::Mock(MockConnection {}),
            pipe,
            batch_id: 0,
            epoch: 0,
            update_cache: HashMap::new(),
            pk_indices,
        })
    }

    pub fn make_redis_key(row: RowRef<'_>, pk_indices: &[usize]) -> String {
        pk_indices
            .iter()
            .map(|i| row.value_at(*i).to_text())
            .join(":")
    }
}

pub struct MockConnection {}

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
            let key = Self::make_redis_key(row, &self.pk_indices);
            match op {
                Op::Insert => {
                    // current format: key=`pk1[:pk2:pk3:...]` value="[v1,v2,...]"
                    self.pipe.set(
                        key,
                        format!("[{}]", row.values().map(|v| v.to_text()).join(",")),
                    );
                }
                Op::Delete => {
                    self.pipe.del(key);
                }
                Op::UpdateDelete => {
                    self.pipe.del(key);
                }
                Op::UpdateInsert => {
                    self.pipe.set(
                        key,
                        format!("[{}]", row.values().map(|v| v.to_text()).join(",")),
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
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use rdkafka::message::FromBytes;
    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, Op, StreamChunk, Utf8Array};
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

        let chunk_b = StreamChunk::new(
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
