// Copyright 2025 RisingWave Labs
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

use core::num::NonZero;

use anyhow::anyhow;
use phf::{Set, phf_set};
use risingwave_common::catalog::Field;
use risingwave_pb::connector_service::SinkMetadata;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde_with::serde_as;
use tokio::sync::mpsc::UnboundedSender;
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::jdbc_jni_client::{JdbcJniClient, build_alter_add_column_sql};
use crate::sink::remote::{CoordinatedRemoteSinkWriter, Jdbc, RemoteSink};
use crate::sink::{
    Result, Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError, SinkParam,
    SinkWriterMetrics,
};

pub const REDSHIFT_SINK: &str = "redshift";

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct RedShiftConfig {
    #[serde(rename = "jdbc.url")]
    pub jdbc_url: String,

    #[serde(rename = "user")]
    pub username: Option<String>,

    #[serde(rename = "password")]
    pub password: Option<String>,

    #[serde(rename = "table.name")]
    pub table: String,
}

#[derive(Debug)]
pub struct RedshiftSink {
    remote_sink: RemoteSink<Jdbc>,
    config: RedShiftConfig,
    param: SinkParam,
}
impl EnforceSecret for RedshiftSink {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "user",
        "password",
        "jdbc.url"
    };
}

impl TryFrom<SinkParam> for RedshiftSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = serde_json::from_value::<RedShiftConfig>(
            serde_json::to_value(param.properties.clone()).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        let remote_sink = RemoteSink::try_from(param.clone())?;
        Ok(Self {
            remote_sink,
            config,
            param,
        })
    }
}

impl Sink for RedshiftSink {
    type Coordinator = RedshiftSinkCommitter;
    type LogSinker = CoordinatedLogSinker<CoordinatedRemoteSinkWriter>;

    const SINK_NAME: &'static str = REDSHIFT_SINK;

    async fn validate(&self) -> Result<()> {
        self.remote_sink.validate().await
    }

    fn support_schema_change() -> bool {
        true
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let metrics = SinkWriterMetrics::new(&writer_param);
        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            CoordinatedRemoteSinkWriter::new(self.param.clone(), metrics.clone()).await?,
            NonZero::new(1).unwrap(),
        )
        .await
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _db: DatabaseConnection,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<Self::Coordinator> {
        let coordinator = RedshiftSinkCommitter::new(self.config.clone())?;
        Ok(coordinator)
    }
}

pub struct RedshiftSinkCommitter {
    client: JdbcJniClient,
    table_name: String,
}

impl RedshiftSinkCommitter {
    pub fn new(config: RedShiftConfig) -> Result<Self> {
        let mut jdbc_url = config.jdbc_url;
        if let Some(username) = config.username {
            jdbc_url = format!("{}?user={}", jdbc_url, username);
        }
        if let Some(password) = config.password {
            jdbc_url = format!("{}&password={}", jdbc_url, password);
        }
        let client = JdbcJniClient::new(jdbc_url)?;
        Ok(Self {
            client,
            table_name: config.table,
        })
    }
}
#[async_trait]
impl SinkCommitCoordinator for RedshiftSinkCommitter {
    async fn init(&mut self, _subscriber: SinkCommittedEpochSubscriber) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn commit(
        &mut self,
        _epoch: u64,
        _metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<()> {
        if let Some(add_columns) = add_columns {
            let sql = build_alter_add_column_sql(
                &self.table_name,
                &add_columns
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.to_string()))
                    .collect::<Vec<_>>(),
            );
            self.client.execute_sql_sync(&sql)?;
        }
        Ok(())
    }
}
