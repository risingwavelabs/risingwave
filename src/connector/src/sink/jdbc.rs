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

use anyhow::anyhow;
use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::{Result, SinkError};

pub const JDBC_SINK: &str = "jdbc";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct JdbcConfig {
    /// The JDBC URL for the target database.
    #[serde(rename = "jdbc.url")]
    pub jdbc_url: String,

    /// Database user name.
    #[serde(rename = "user")]
    pub user: Option<String>,

    /// Database password.
    #[serde(rename = "password")]
    pub password: Option<String>,

    /// Target table name.
    #[serde(rename = "table.name")]
    pub table_name: String,

    /// The type of sink. Only "append-only" and "upsert" are supported.
    pub r#type: String,

    /// Database schema name (optional, for databases that support schemas).
    #[serde(rename = "schema.name")]
    pub schema_name: Option<String>,

    /// Database name (optional).
    #[serde(rename = "database.name")]
    pub database_name: Option<String>,

    /// JDBC query timeout in seconds.
    #[serde(rename = "jdbc.query.timeout")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub query_timeout: Option<i32>,

    /// Whether to enable auto-commit mode.
    #[serde(rename = "jdbc.auto.commit")]
    pub auto_commit: Option<bool>,

    /// Number of rows to batch for insert operations (RedShift specific).
    #[serde(rename = "batch.insert.rows")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub batch_insert_rows: Option<i32>,

    // MySQL-specific TCP keepalive configurations
    /// Enable TCP keepalive for MySQL connections.
    /// Default: true
    #[serde(rename = "jdbc.tcp.keep.alive")]
    #[with_option(allow_alter_on_fly)]
    pub tcp_keep_alive: Option<bool>,

    /// Time in milliseconds before sending the first keepalive probe (MySQL specific).
    /// This should be set to a value less than your network's idle timeout.
    /// For AWS RDS, consider setting this to 300000 (5 minutes) or less.
    #[serde(rename = "jdbc.tcp.keep.alive.time.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub tcp_keep_alive_time_ms: Option<i32>,

    /// Interval in milliseconds between keepalive probes (MySQL specific).
    #[serde(rename = "jdbc.tcp.keep.alive.interval.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub tcp_keep_alive_interval_ms: Option<i32>,

    /// Number of unacknowledged keepalive probes before closing the connection (MySQL specific).
    #[serde(rename = "jdbc.tcp.keep.alive.count")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub tcp_keep_alive_count: Option<i32>,
}

impl EnforceSecret for JdbcConfig {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "password"
    };
}

impl JdbcConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<JdbcConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}
