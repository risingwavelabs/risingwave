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

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

use crate::sink::mysql::{MySQLConfig, MySQLSink};
use crate::sink::redis::{RedisConfig, RedisSink};

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()>;
}

pub enum SinkConfig {
    Mysql(MySQLConfig),
    Redis(RedisConfig),
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
    #[error(transparent)]
    MySQL(#[from] mysql_async::Error),
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
