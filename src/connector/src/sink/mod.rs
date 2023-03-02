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

pub mod catalog;
pub mod console;
pub mod kafka;
pub mod redis;
pub mod remote;

use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_rpc_client::error::RpcError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub use tracing;

use self::catalog::SinkType;
use crate::sink::console::{ConsoleConfig, ConsoleSink, CONSOLE_SINK};
use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
use crate::sink::redis::{RedisConfig, RedisSink};
use crate::sink::remote::{RemoteConfig, RemoteSink};
use crate::ConnectorParams;

pub const SINK_FORMAT_OPTION: &str = "format";
pub const SINK_FORMAT_APPEND_ONLY: &str = "append_only";
pub const SINK_FORMAT_DEBEZIUM: &str = "debezium";
pub const SINK_FORMAT_UPSERT: &str = "upsert";
pub const SINK_USER_FORCE_APPEND_ONLY_OPTION: &str = "force_append_only";

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

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
    Redis(RedisConfig),
    Kafka(Box<KafkaConfig>),
    Remote(RemoteConfig),
    Console(ConsoleConfig),
    BlackHole,
}

#[derive(Clone, Debug, EnumAsInner, Serialize, Deserialize)]
pub enum SinkState {
    Kafka,
    Redis,
    Console,
    Remote,
    Blackhole,
}

pub const BLACKHOLE_SINK: &str = "blackhole";

impl SinkConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        const SINK_TYPE_KEY: &str = "connector";
        let sink_type = properties
            .get(SINK_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("missing config: {}", SINK_TYPE_KEY)))?;
        match sink_type.to_lowercase().as_str() {
            KAFKA_SINK => Ok(SinkConfig::Kafka(Box::new(KafkaConfig::from_hashmap(
                properties,
            )?))),
            CONSOLE_SINK => Ok(SinkConfig::Console(ConsoleConfig::from_hashmap(
                properties,
            )?)),
            BLACKHOLE_SINK => Ok(SinkConfig::BlackHole),
            _ => Ok(SinkConfig::Remote(RemoteConfig::from_hashmap(properties)?)),
        }
    }

    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkConfig::Kafka(_) => "kafka",
            SinkConfig::Redis(_) => "redis",
            SinkConfig::Remote(_) => "remote",
            SinkConfig::Console(_) => "console",
            SinkConfig::BlackHole => "blackhole",
        }
    }
}

#[derive(Debug)]
pub enum SinkImpl {
    Redis(Box<RedisSink>),
    Kafka(Box<KafkaSink<true>>),
    UpsertKafka(Box<KafkaSink<false>>),
    Remote(Box<RemoteSink<true>>),
    UpsertRemote(Box<RemoteSink<false>>),
    Console(Box<ConsoleSink>),
    Blackhole,
}

impl SinkImpl {
    pub async fn new(
        cfg: SinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector_params: ConnectorParams,
        sink_type: SinkType,
    ) -> Result<Self> {
        Ok(match cfg {
            SinkConfig::Redis(cfg) => SinkImpl::Redis(Box::new(RedisSink::new(cfg, schema)?)),
            SinkConfig::Kafka(cfg) => {
                if sink_type.is_append_only() {
                    // Append-only Kafka sink
                    SinkImpl::Kafka(Box::new(
                        KafkaSink::<true>::new(*cfg, schema, pk_indices).await?,
                    ))
                } else {
                    // Upsert Kafka sink
                    SinkImpl::UpsertKafka(Box::new(
                        KafkaSink::<false>::new(*cfg, schema, pk_indices).await?,
                    ))
                }
            }
            SinkConfig::Console(cfg) => SinkImpl::Console(Box::new(ConsoleSink::new(cfg, schema)?)),
            SinkConfig::Remote(cfg) => {
                if sink_type.is_append_only() {
                    // Append-only remote sink
                    SinkImpl::Remote(Box::new(
                        RemoteSink::<true>::new(cfg, schema, pk_indices, connector_params).await?,
                    ))
                } else {
                    // Upsert remote sink
                    SinkImpl::UpsertRemote(Box::new(
                        RemoteSink::<false>::new(cfg, schema, pk_indices, connector_params).await?,
                    ))
                }
            }
            SinkConfig::BlackHole => SinkImpl::Blackhole,
        })
    }
}

macro_rules! impl_sink {
    ($($variant_name:ident),*) => {
        #[async_trait]
        impl Sink for SinkImpl {
            async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
                match self {
                    $( SinkImpl::$variant_name(inner) => inner.write_batch(chunk).await, )*
                    SinkImpl::Blackhole => Ok(()),
                }
            }

            async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
                match self {
                    $( SinkImpl::$variant_name(inner) => inner.begin_epoch(epoch).await, )*
                    SinkImpl::Blackhole => Ok(()),
                }
            }

            async fn commit(&mut self) -> Result<()> {
                match self {
                    $( SinkImpl::$variant_name(inner) => inner.commit().await, )*
                    SinkImpl::Blackhole => Ok(()),
                }
            }

            async fn abort(&mut self) -> Result<()> {
                match self {
                    $( SinkImpl::$variant_name(inner) => inner.abort().await, )*
                    SinkImpl::Blackhole => Ok(()),
                }
            }
        }
    }
}

impl_sink! {
    Redis,
    Kafka,
    UpsertKafka,
    Remote,
    UpsertRemote,
    Console
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Remote sink error: {0}")]
    Remote(String),
    #[error("Json parse error: {0}")]
    JsonParse(String),
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
}

impl From<RpcError> for SinkError {
    fn from(value: RpcError) -> Self {
        SinkError::Remote(format!("{:?}", value))
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
