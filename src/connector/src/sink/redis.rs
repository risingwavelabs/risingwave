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


use std::fmt::{format, Debug, Formatter};

use async_trait::async_trait;
use itertools::Itertools;
use redis::{AsyncCommands, Commands, pipe, RedisConnectionInfo, ToRedisArgs};
use redis::cluster::cluster_pipe;
pub use redis::RedisError;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use crate::sink::{Result, Sink, SinkError};

#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<i64>,
    pub service_hosts: Vec<(String, u16)>,
}

#[derive(Debug)]
pub enum RedisSinkState {
    Init,
    Running(u64),
}

pub struct RedisSink {
    pub config: RedisConfig,
    pub client: RedisClientMode,
    connection: ConnectionMode,
    pub last_success_epoch: RedisSinkState,
    pub in_transaction_epoch: Option<u64>,
}

impl Debug for RedisSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub enum RedisClientMode {
    Single(redis::Client),
    Cluster(redis::cluster::ClusterClient),
}

impl RedisClientMode {
    async fn connect(&self) -> Result<ConnectionMode> {
        match self {
            RedisClientMode::Single(c) => Ok(ConnectionMode::Single(c.get_async_connection().await?)),
            RedisClientMode::Cluster(c) => Ok(ConnectionMode::Cluster(c.get_connection()?))
        }
    }
}

enum ConnectionMode {
    Single(redis::aio::Connection),
    Cluster(redis::cluster::ClusterConnection),
}

impl ConnectionMode {
    async fn pipe<K: ToRedisArgs, V: ToRedisArgs>(&mut self, args: Vec<(K, V)>) -> Result<()> {
        match self {
            ConnectionMode::Single(c) => {
                c.set_multiple(args.as_slice()).await?;
            }
            ConnectionMode::Cluster(c) => {
                c.set_multiple(args.as_slice())?;
            }
        }
        Ok(())
    }
}

impl RedisSink {
    pub async fn new(cfg: RedisConfig) -> Result<Self> {
        if cfg.service_hosts.is_empty() {
            return Err(SinkError::Config("".into()));
        }
        let redis_url_builder = |user: Option<String>,
                                 password: Option<String>,
                                 database: Option<i64>,
                                 address: &String,
                                 port: &u16|
         -> Result<String> {
            let auth = if user.is_some() || password.is_some() {
                format!(
                    "{}{}@",
                    user.unwrap_or_else(|| "default".to_string()),
                    match password {
                        Some(p) => ":".to_string() + &p,
                        None => "".to_string(),
                    }
                )
            } else {
                "".to_string()
            };
            Ok(format!(
                "redis://{}{}:{}{}",
                auth,
                address,
                port,
                match database {
                    Some(db) => format!("/{}", db),
                    None => "".into(),
                }
            ))
        };
        let client = match cfg.service_hosts.len() {
            1 => {
                let (address, port) = &cfg.service_hosts[0];
                let redis_url = redis_url_builder(
                    cfg.username.clone(),
                    cfg.password.clone(),
                    cfg.database,
                    address,
                    port,
                )?;
                RedisClientMode::Single(redis::Client::open(redis_url)?)
            }
            _ => {
                let vec_url = cfg
                    .service_hosts
                    .iter()
                    .map(|(address, port)| {
                        redis_url_builder(
                            cfg.username.clone(),
                            cfg.password.clone(),
                            cfg.database,
                            address,
                            port,
                        )
                    })
                    .collect::<Result<Vec<String>>>()?;
                RedisClientMode::Cluster(redis::cluster::ClusterClient::open(vec_url)?)
            }
        };

        let conn = client.connect()?;

        Ok(RedisSink {
            config: cfg.clone(),
            client,
            connection: conn,
            last_success_epoch: RedisSinkState::Init,
            in_transaction_epoch: None,
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, chunk: StreamChunk, _schema: &Schema) -> Result<()> {
        if let (RedisSinkState::Running(epoch), Some(in_txn_epoch)) = (&self.last_success_epoch, &self.in_transaction_epoch) && in_txn_epoch <= epoch {
            return Ok(())
        }

        for (op, row) in chunk.rows() {
        }

        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.in_transaction_epoch = Some(epoch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.last_success_epoch =
            RedisSinkState::Running(self.in_transaction_epoch.take().unwrap());
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.in_transaction_epoch = None;
        Ok(())
    }
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_write() -> Result<()> {
        let config = RedisConfig {
            username: None,
            password: None,
            database: Some(0),
            service_hosts: vec![("127.0.0.1".to_string(), 6379)],
        };
        let mut sink = RedisSink::new(config).await?;
        Ok(())
    }
}
