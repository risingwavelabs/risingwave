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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::*;
use prost::Message;
use risingwave_common::global_jvm::Jvm;
use risingwave_common::id::SourceId;
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_pb::connector_service::{SourceType, ValidateSourceRequest, ValidateSourceResponse};
use thiserror_ext::AsReport;
use tokio_postgres::types::PgLsn;

use crate::connector_common::{SslMode, create_pg_client};
use crate::error::ConnectorResult;
use crate::source::cdc::external::mysql::build_mysql_connection_pool;
use crate::source::cdc::{
    CdcProperties, CdcSourceTypeTrait, Citus, DebeziumCdcSplit, Mongodb, Mysql, Postgres,
    SqlServer, table_schema_exclude_additional_columns,
};
use crate::source::monitor::metrics::EnumeratorMetrics;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub const DATABASE_SERVERS_KEY: &str = "database.servers";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator<T: CdcSourceTypeTrait> {
    /// The `source_id` in the catalog
    source_id: SourceId,
    worker_node_addrs: Vec<HostAddr>,
    metrics: Arc<EnumeratorMetrics>,
    /// Properties specified in the WITH clause by user for database connection
    properties: Arc<BTreeMap<String, String>>,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitEnumerator for DebeziumSplitEnumerator<T>
where
    Self: ListCdcSplits<CdcSourceType = T> + CdcMonitor,
{
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    async fn new(
        props: CdcProperties<T>,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let server_addrs = props
            .properties
            .get(DATABASE_SERVERS_KEY)
            .map(|s| {
                s.split(',')
                    .map(HostAddr::from_str)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        assert_eq!(
            props.get_source_type_pb(),
            SourceType::from(T::source_type())
        );

        let jvm = Jvm::get_or_init()?;
        let source_id = context.info.source_id;

        // Extract fields before moving props
        let source_type_pb = props.get_source_type_pb();

        // Create Arc once and share it
        let properties_arc = Arc::new(props.properties);
        let properties_arc_for_validation = properties_arc.clone();
        let table_schema_for_validation = props.table_schema;

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            execute_with_jni_env(jvm, |env| {
                let validate_source_request = ValidateSourceRequest {
                    source_id: source_id.as_raw_id() as u64,
                    source_type: source_type_pb as _,
                    properties: (*properties_arc_for_validation).clone(),
                    table_schema: Some(table_schema_exclude_additional_columns(
                        &table_schema_for_validation,
                    )),
                    is_source_job: props.is_cdc_source_job,
                    is_backfill_table: props.is_backfill_table,
                };

                let validate_source_request_bytes =
                    env.byte_array_from_slice(&Message::encode_to_vec(&validate_source_request))?;

                let validate_source_response_bytes = call_static_method!(
                    env,
                    {com.risingwave.connector.source.JniSourceValidateHandler},
                    {byte[] validate(byte[] validateSourceRequestBytes)},
                    &validate_source_request_bytes
                )?;

                let validate_source_response: ValidateSourceResponse = Message::decode(
                    risingwave_jni_core::to_guarded_slice(&validate_source_response_bytes, env)?
                        .deref(),
                )?;

                if let Some(error) = validate_source_response.error {
                    return Err(
                        anyhow!(error.error_message).context("source cannot pass validation")
                    );
                }

                Ok(())
            })
        })
        .await
        .context("failed to validate source")??;

        tracing::debug!("validate cdc source properties success");
        Ok(Self {
            source_id,
            worker_node_addrs: server_addrs,
            metrics: context.metrics.clone(),
            properties: properties_arc,
            _phantom: PhantomData,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<DebeziumCdcSplit<T>>> {
        Ok(self.list_cdc_splits())
    }

    async fn on_tick(&mut self) -> ConnectorResult<()> {
        self.monitor_cdc().await
    }
}

impl<T: CdcSourceTypeTrait> DebeziumSplitEnumerator<T> {
    async fn monitor_postgres_confirmed_flush_lsn(&mut self) -> ConnectorResult<()> {
        // Query confirmed flush LSN and update metrics
        match self.query_confirmed_flush_lsn().await {
            Ok(Some((lsn, slot_name))) => {
                // Update metrics
                self.metrics
                    .pg_cdc_confirmed_flush_lsn
                    .with_guarded_label_values(&[
                        &self.source_id.to_string(),
                        &slot_name.to_owned(),
                    ])
                    .set(lsn as i64);
                tracing::debug!(
                    "Updated confirm_flush_lsn for source {} slot {}: {}",
                    self.source_id,
                    slot_name,
                    lsn
                );
            }
            Ok(None) => {
                tracing::warn!("No confirmed_flush_lsn found for source {}", self.source_id);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to query confirmed_flush_lsn for source {}: {}",
                    self.source_id,
                    e.as_report()
                );
            }
        }
        Ok(())
    }

    /// Query confirmed flush LSN from PostgreSQL, return the slot name and the confirmed flush LSN.
    async fn query_confirmed_flush_lsn(&self) -> ConnectorResult<Option<(u64, &str)>> {
        // Extract connection parameters from CDC properties
        let hostname = self
            .properties
            .get("hostname")
            .ok_or_else(|| anyhow::anyhow!("hostname not found in CDC properties"))?;
        let port = self
            .properties
            .get("port")
            .ok_or_else(|| anyhow::anyhow!("port not found in CDC properties"))?;
        let user = self
            .properties
            .get("username")
            .ok_or_else(|| anyhow::anyhow!("username not found in CDC properties"))?;
        let password = self
            .properties
            .get("password")
            .ok_or_else(|| anyhow::anyhow!("password not found in CDC properties"))?;
        let database = self
            .properties
            .get("database.name")
            .ok_or_else(|| anyhow::anyhow!("database.name not found in CDC properties"))?;

        // Get SSL mode from properties, default to Preferred if not specified
        let ssl_mode = self
            .properties
            .get("ssl.mode")
            .and_then(|s| serde_json::from_value(serde_json::Value::String(s.clone())).ok())
            .unwrap_or(SslMode::Preferred);
        let ssl_root_cert = self.properties.get("database.ssl.root.cert").cloned();

        let slot_name = self
            .properties
            .get("slot.name")
            .ok_or_else(|| anyhow::anyhow!("slot.name not found in CDC properties"))?;

        // Create PostgreSQL client
        let client = create_pg_client(
            user,
            password,
            hostname,
            port,
            database,
            &ssl_mode,
            &ssl_root_cert,
        )
        .await
        .context("Failed to create PostgreSQL client")?;

        let query = "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1";
        let row = client
            .query_opt(query, &[&slot_name])
            .await
            .context("PostgreSQL query confirmed flush lsn error")?;

        match row {
            Some(row) => {
                let confirm_flush_lsn: Option<PgLsn> = row.get(0);
                if let Some(lsn) = confirm_flush_lsn {
                    Ok(Some((lsn.into(), slot_name.as_str())))
                } else {
                    Ok(None)
                }
            }
            None => {
                tracing::warn!("No replication slot found with name: {}", slot_name);
                Ok(None)
            }
        }
    }
}

pub trait ListCdcSplits {
    type CdcSourceType: CdcSourceTypeTrait;
    /// Generates a single split for shared source.
    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>>;
}

/// Trait for CDC-specific monitoring behavior
#[async_trait]
pub trait CdcMonitor {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()>;
}

#[async_trait]
impl<T: CdcSourceTypeTrait> CdcMonitor for DebeziumSplitEnumerator<T> {
    default async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

impl DebeziumSplitEnumerator<Mysql> {
    async fn monitor_mysql_binlog_files(&mut self) -> ConnectorResult<()> {
        // Get hostname and port for metrics labels
        let hostname = self
            .properties
            .get("hostname")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("missing required property 'hostname' for MySQL CDC source")
            })?;
        let port = self
            .properties
            .get("port")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("missing required property 'port' for MySQL CDC source")
            })?;

        // Query binlog files and update metrics
        match self.query_binlog_files().await {
            Ok(binlog_files) => {
                if let Some((oldest_file, oldest_size)) = binlog_files.first() {
                    // Extract sequence number from filename (e.g., "binlog.000001" -> 1)
                    if let Some(seq) = Self::extract_binlog_seq(oldest_file) {
                        self.metrics
                            .mysql_cdc_binlog_file_seq_min
                            .with_guarded_label_values(&[hostname, port])
                            .set(seq as i64);
                        tracing::debug!(
                            "MySQL CDC source {} ({}:{}): oldest binlog = {}, seq = {}, size = {}",
                            self.source_id,
                            hostname,
                            port,
                            oldest_file,
                            seq,
                            oldest_size
                        );
                    }
                }
                if let Some((newest_file, newest_size)) = binlog_files.last() {
                    // Extract sequence number from filename
                    if let Some(seq) = Self::extract_binlog_seq(newest_file) {
                        self.metrics
                            .mysql_cdc_binlog_file_seq_max
                            .with_guarded_label_values(&[hostname, port])
                            .set(seq as i64);
                        tracing::debug!(
                            "MySQL CDC source {} ({}:{}): newest binlog = {}, seq = {}, size = {}",
                            self.source_id,
                            hostname,
                            port,
                            newest_file,
                            seq,
                            newest_size
                        );
                    }
                }
                tracing::debug!(
                    "MySQL CDC source {} ({}:{}): total {} binlog files",
                    self.source_id,
                    hostname,
                    port,
                    binlog_files.len()
                );
            }
            Err(e) => {
                tracing::error!(
                    "Failed to query binlog files for MySQL CDC source {} ({}:{}): {}",
                    self.source_id,
                    hostname,
                    port,
                    e.as_report()
                );
            }
        }
        Ok(())
    }

    /// Extract sequence number from binlog filename
    /// e.g., "binlog.000001" -> Some(1), "mysql-bin.000123" -> Some(123)
    fn extract_binlog_seq(filename: &str) -> Option<u64> {
        filename.rsplit('.').next()?.parse::<u64>().ok()
    }

    /// Query binlog files from MySQL, returns Vec<(filename, size)>
    async fn query_binlog_files(&self) -> ConnectorResult<Vec<(String, u64)>> {
        // Extract connection parameters from CDC properties
        let hostname = self
            .properties
            .get("hostname")
            .ok_or_else(|| anyhow::anyhow!("hostname not found in CDC properties"))?;
        let port = self
            .properties
            .get("port")
            .ok_or_else(|| anyhow::anyhow!("port not found in CDC properties"))?
            .parse::<u16>()
            .context("failed to parse port as u16")?;
        let username = self
            .properties
            .get("username")
            .ok_or_else(|| anyhow::anyhow!("username not found in CDC properties"))?;
        let password = self
            .properties
            .get("password")
            .ok_or_else(|| anyhow::anyhow!("password not found in CDC properties"))?;
        let database = self
            .properties
            .get("database.name")
            .ok_or_else(|| anyhow::anyhow!("database.name not found in CDC properties"))?;

        // Get SSL mode configuration (default to Disabled if not specified)
        let ssl_mode = self
            .properties
            .get("ssl.mode")
            .and_then(|s| serde_json::from_value(serde_json::Value::String(s.clone())).ok())
            .unwrap_or(SslMode::Preferred);

        // Build MySQL connection pool with proper SSL configuration
        let pool =
            build_mysql_connection_pool(hostname, port, username, password, database, ssl_mode);
        let mut conn = pool
            .get_conn()
            .await
            .context("Failed to connect to MySQL")?;

        // Query binlog files using SHOW BINARY LOGS
        // Note: MySQL 8.0+ returns 3 columns: Log_name, File_size, Encrypted
        let query_result: Vec<(String, u64)> = conn
            .query_map(
                "SHOW BINARY LOGS",
                |(log_name, file_size, _encrypted): (String, u64, String)| (log_name, file_size),
            )
            .await
            .context("Failed to execute SHOW BINARY LOGS")?;

        drop(conn);
        pool.disconnect().await.ok();

        Ok(query_result)
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Mysql> {
    type CdcSourceType = Mysql;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

#[async_trait]
impl CdcMonitor for DebeziumSplitEnumerator<Mysql> {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        // For MySQL CDC, query the upstream MySQL binlog files and monitor them.
        self.monitor_mysql_binlog_files().await?;
        Ok(())
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Postgres> {
    type CdcSourceType = Postgres;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

#[async_trait]
impl CdcMonitor for DebeziumSplitEnumerator<Postgres> {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        // For PostgreSQL CDC, query the upstream Postgres confirmed flush lsn and monitor it.
        self.monitor_postgres_confirmed_flush_lsn().await?;
        Ok(())
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Citus> {
    type CdcSourceType = Citus;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        self.worker_node_addrs
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                DebeziumCdcSplit::<Self::CdcSourceType>::new(
                    id as u32,
                    None,
                    Some(addr.to_string()),
                )
            })
            .collect_vec()
    }
}
impl ListCdcSplits for DebeziumSplitEnumerator<Mongodb> {
    type CdcSourceType = Mongodb;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<SqlServer> {
    type CdcSourceType = SqlServer;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}
