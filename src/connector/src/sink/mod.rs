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

pub mod mysql;
pub mod redis;

use std::collections::HashMap;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use risingwave_common::array::{StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result as RwResult, RwError};
use thiserror::Error;

use crate::sink::mysql::{MySQLConfig, MySQLSink};
use crate::sink::redis::{RedisConfig, RedisSink};

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()>;
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Mysql(MySQLConfig),
    Redis(RedisConfig),
}

impl SinkConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> RwResult<Self> {
        const SINK_TYPE_KEY: &str = "sink_type";
        let sink_type = properties.get(SINK_TYPE_KEY).ok_or_else(|| {
            RwError::from(ErrorCode::InvalidConfigValue {
                config_entry: SINK_TYPE_KEY.to_string(),
                config_value: "".to_string(),
            })
        })?;
        match sink_type.to_lowercase().as_str() {
            _ => unimplemented!(),
        }
    }
}

pub enum SinkImpl {
    MySQL(MySQLSink),
    Redis(RedisSink),
}

impl SinkImpl {
    fn new(cfg: SinkConfig) -> Self {
        match cfg {
            SinkConfig::Mysql(cfg) => SinkImpl::MySQL(MySQLSink::new(cfg)),
            SinkConfig::Redis(cfg) => SinkImpl::Redis(RedisSink::new(cfg)),
        }
    }
}

#[async_trait]
impl Sink for SinkImpl {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        match self {
            SinkImpl::MySQL(sink) => sink.write_batch(chunk, schema).await,
            SinkImpl::Redis(sink) => sink.write_batch(chunk, schema).await,
        }
    }
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("MySQL error: {0}")]
    MySQL(#[from] mysql_async::Error),
    #[error("Kafka error: {0}")]
    Kafka(String),
    #[error("Json parse error: {0}")]
    JsonParse(String),
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
