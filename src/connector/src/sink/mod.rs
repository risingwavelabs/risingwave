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

pub mod kafka;
pub mod mysql;
pub mod redis;

use std::collections::HashMap;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, RwError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub use tracing;

use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
pub use crate::sink::mysql::{MySqlConfig, MySqlSink, MYSQL_SINK};
use crate::sink::redis::{RedisConfig, RedisSink};

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()>;

    // the following interface is for transactions, if not supported, return Ok(())
    // start a transaction with epoch number. Note that epoch number should be increasing.
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    // commits the current transaction and marks all messages in the transaction success.
    async fn commit(&mut self) -> Result<()>;

    // aborts the current transaction because some error happens. we should rollback to the last
    // commit point.
    async fn abort(&mut self) -> Result<()>;
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Mysql(MySqlConfig),
    Redis(RedisConfig),
    Kafka(KafkaConfig),
}

#[derive(Clone, Debug, EnumAsInner, Serialize, Deserialize)]
pub enum SinkState {
    Kafka,
    Mysql,
    Redis,
}

impl SinkConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        const SINK_TYPE_KEY: &str = "connector";
        let sink_type = properties
            .get(SINK_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(format!("missing config: {}", SINK_TYPE_KEY)))?;
        match sink_type.to_lowercase().as_str() {
            KAFKA_SINK => Ok(SinkConfig::Kafka(KafkaConfig::from_hashmap(properties)?)),
            MYSQL_SINK => Ok(SinkConfig::Mysql(MySqlConfig::from_hashmap(properties)?)),
            _ => unimplemented!(),
        }
    }

    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkConfig::Mysql(_) => "mysql",
            SinkConfig::Kafka(_) => "kafka",
            SinkConfig::Redis(_) => "redis",
        }
    }
}

#[derive(Debug)]
pub enum SinkImpl {
    MySql(Box<MySqlSink>),
    Redis(Box<RedisSink>),
    Kafka(Box<KafkaSink>),
}

impl SinkImpl {
    pub async fn new(cfg: SinkConfig) -> Result<Self> {
        Ok(match cfg {
            SinkConfig::Mysql(cfg) => SinkImpl::MySql(Box::new(MySqlSink::new(cfg).await?)),
            SinkConfig::Redis(cfg) => SinkImpl::Redis(Box::new(RedisSink::new(cfg)?)),
            SinkConfig::Kafka(cfg) => SinkImpl::Kafka(Box::new(KafkaSink::new(cfg)?)),
        })
    }

    pub fn needs_preparation(&self) -> bool {
        match self {
            SinkImpl::MySql(_) => true,
            SinkImpl::Redis(_) => false,
            SinkImpl::Kafka(_) => false,
        }
    }

    pub async fn prepare(&mut self, schema: &Schema) -> Result<()> {
        match self {
            SinkImpl::MySql(sink) => sink.prepare(schema).await,
            _ => unreachable!(),
        }
    }
}

#[async_trait]
impl Sink for SinkImpl {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        match self {
            SinkImpl::MySql(sink) => sink.write_batch(chunk, schema).await,
            SinkImpl::Redis(sink) => sink.write_batch(chunk, schema).await,
            SinkImpl::Kafka(sink) => sink.write_batch(chunk, schema).await,
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        match self {
            SinkImpl::MySql(sink) => sink.begin_epoch(epoch).await,
            SinkImpl::Redis(sink) => sink.begin_epoch(epoch).await,
            SinkImpl::Kafka(sink) => sink.begin_epoch(epoch).await,
        }
    }

    async fn commit(&mut self) -> Result<()> {
        match self {
            SinkImpl::MySql(sink) => sink.commit().await,
            SinkImpl::Redis(sink) => sink.commit().await,
            SinkImpl::Kafka(sink) => sink.commit().await,
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            SinkImpl::MySql(sink) => sink.abort().await,
            SinkImpl::Redis(sink) => sink.abort().await,
            SinkImpl::Kafka(sink) => sink.abort().await,
        }
    }
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("MySql error: {0}")]
    MySql(String),
    #[error("MySql inner error: {0}")]
    MySQLInner(#[from] mysql_async::Error),
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Json parse error: {0}")]
    JsonParse(String),
    #[error("config error: {0}")]
    Config(String),
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
