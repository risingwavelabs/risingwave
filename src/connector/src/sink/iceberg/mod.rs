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

mod prometheus;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, MAIN_BRANCH, Operation, SerializedDataFile, Transform, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::sort_position_delete_writer::{
    POSITION_DELETE_SCHEMA, SortPositionDeleteWriterBuilder,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::function_writer::equality_delta_writer::{
    DELETE_OP, EqualityDeltaWriterBuilder, INSERT_OP,
};
use iceberg::writer::function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use prometheus::monitored_general_writer::MonitoredGeneralWriterBuilder;
use prometheus::monitored_position_delete_writer::MonitoredPositionDeleteWriterBuilder;
use regex::Regex;
use risingwave_common::array::arrow::arrow_array_iceberg::{Int32Array, RecordBatch};
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    self, DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
    Schema as ArrowSchema, SchemaRef,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use serde::Deserialize;
use serde_json::from_value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::warn;
use url::Url;
use uuid::Uuid;
use with_options::WithOptions;

use super::decouple_checkpoint_log_sink::iceberg_default_commit_checkpoint_interval;
use super::{
    GLOBAL_SINK_METRICS, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink,
    SinkError, SinkWriterParam,
};
use crate::connector_common::{IcebergCommon, IcebergSinkCompactionUpdate, IcebergTableIdentifier};
use crate::enforce_secret::EnforceSecret;
use crate::sink::catalog::SinkId;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, SinkCommitCoordinator, SinkCommitStrategy, SinkParam};
use crate::{deserialize_bool_from_string, deserialize_optional_string_seq_from_string};

pub const ICEBERG_SINK: &str = "iceberg";
pub const ICEBERG_COW_BRANCH: &str = "ingestion";
pub const ICEBERG_WRITE_MODE_MERGE_ON_READ: &str = "merge-on-read";
pub const ICEBERG_WRITE_MODE_COPY_ON_WRITE: &str = "copy-on-write";

// Configuration constants
pub const ENABLE_COMPACTION: &str = "enable_compaction";
pub const COMPACTION_INTERVAL_SEC: &str = "compaction_interval_sec";
pub const ENABLE_SNAPSHOT_EXPIRATION: &str = "enable_snapshot_expiration";
pub const WRITE_MODE: &str = "write_mode";
pub const SNAPSHOT_EXPIRATION_RETAIN_LAST: &str = "snapshot_expiration_retain_last";
pub const SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS: &str = "snapshot_expiration_max_age_millis";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES: &str = "snapshot_expiration_clear_expired_files";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA: &str =
    "snapshot_expiration_clear_expired_meta_data";
pub const MAX_SNAPSHOTS_NUM: &str = "max_snapshots_num_before_compaction";

fn default_commit_retry_num() -> u32 {
    8
}

fn default_iceberg_write_mode() -> String {
    ICEBERG_WRITE_MODE_MERGE_ON_READ.to_owned()
}

fn default_true() -> bool {
    true
}

fn default_some_true() -> Option<bool> {
    Some(true)
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergConfig {
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(flatten)]
    common: IcebergCommon,

    #[serde(flatten)]
    table: IcebergTableIdentifier,

    #[serde(
        rename = "primary_key",
        default,
        deserialize_with = "deserialize_optional_string_seq_from_string"
    )]
    pub primary_key: Option<Vec<String>>,

    // Props for java catalog props.
    #[serde(skip)]
    pub java_catalog_props: HashMap<String, String>,

    #[serde(default)]
    pub partition_by: Option<String>,

    /// Commit every n(>0) checkpoints, default is 60.
    #[serde(default = "iceberg_default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub create_table_if_not_exists: bool,

    /// Whether it is `exactly_once`, the default is true.
    #[serde(default = "default_some_true")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub is_exactly_once: Option<bool>,
    // Retry commit num when iceberg commit fail. default is 8.
    // # TODO
    // Iceberg table may store the retry commit num in table meta.
    // We should try to find and use that as default commit retry num first.
    #[serde(default = "default_commit_retry_num")]
    pub commit_retry_num: u32,

    /// Whether to enable iceberg compaction.
    #[serde(
        rename = "enable_compaction",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly)]
    pub enable_compaction: bool,

    /// The interval of iceberg compaction
    #[serde(rename = "compaction_interval_sec", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub compaction_interval_sec: Option<u64>,

    /// Whether to enable iceberg expired snapshots.
    #[serde(
        rename = "enable_snapshot_expiration",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly)]
    pub enable_snapshot_expiration: bool,

    /// The iceberg write mode, can be `merge-on-read` or `copy-on-write`.
    #[serde(rename = "write_mode", default = "default_iceberg_write_mode")]
    pub write_mode: String,

    /// The maximum age (in milliseconds) for snapshots before they expire
    /// For example, if set to 3600000, snapshots older than 1 hour will be expired
    #[serde(rename = "snapshot_expiration_max_age_millis", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub snapshot_expiration_max_age_millis: Option<i64>,

    /// The number of snapshots to retain
    #[serde(rename = "snapshot_expiration_retain_last", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub snapshot_expiration_retain_last: Option<i32>,

    #[serde(
        rename = "snapshot_expiration_clear_expired_files",
        default = "default_true",
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly)]
    pub snapshot_expiration_clear_expired_files: bool,

    #[serde(
        rename = "snapshot_expiration_clear_expired_meta_data",
        default = "default_true",
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly)]
    pub snapshot_expiration_clear_expired_meta_data: bool,

    /// The maximum number of snapshots allowed since the last rewrite operation
    /// If set, sink will check snapshot count and wait if exceeded
    #[serde(rename = "max_snapshots_num_before_compaction", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub max_snapshots_num_before_compaction: Option<usize>,
}

impl EnforceSecret for IcebergConfig {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            IcebergCommon::enforce_one(prop)?;
        }
        Ok(())
    }

    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        IcebergCommon::enforce_one(prop)
    }
}

impl IcebergConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let mut config =
            serde_json::from_value::<IcebergConfig>(serde_json::to_value(&values).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }

        if config.r#type == SINK_TYPE_UPSERT {
            if let Some(primary_key) = &config.primary_key {
                if primary_key.is_empty() {
                    return Err(SinkError::Config(anyhow!(
                        "`primary_key` must not be empty in {}",
                        SINK_TYPE_UPSERT
                    )));
                }
            } else {
                return Err(SinkError::Config(anyhow!(
                    "Must set `primary_key` in {}",
                    SINK_TYPE_UPSERT
                )));
            }
        }

        // All configs start with "catalog." will be treated as java configs.
        config.java_catalog_props = values
            .iter()
            .filter(|(k, _v)| {
                k.starts_with("catalog.")
                    && k != &"catalog.uri"
                    && k != &"catalog.type"
                    && k != &"catalog.name"
                    && k != &"catalog.header"
            })
            .map(|(k, v)| (k[8..].to_string(), v.clone()))
            .collect();

        if config.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }

        Ok(config)
    }

    pub fn catalog_type(&self) -> &str {
        self.common.catalog_type()
    }

    pub async fn load_table(&self) -> Result<Table> {
        self.common
            .load_table(&self.table, &self.java_catalog_props)
            .await
            .map_err(Into::into)
    }

    pub async fn create_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.common
            .create_catalog(&self.java_catalog_props)
            .await
            .map_err(Into::into)
    }

    pub fn full_table_name(&self) -> Result<TableIdent> {
        self.table.to_table_ident().map_err(Into::into)
    }

    pub fn catalog_name(&self) -> String {
        self.common.catalog_name()
    }

    pub fn compaction_interval_sec(&self) -> u64 {
        // default to 1 hour
        self.compaction_interval_sec.unwrap_or(3600)
    }

    /// Calculate the timestamp (in milliseconds) before which snapshots should be expired
    /// Returns `current_time_ms` - `max_age_millis`
    pub fn snapshot_expiration_timestamp_ms(&self, current_time_ms: i64) -> Option<i64> {
        self.snapshot_expiration_max_age_millis
            .map(|max_age_millis| current_time_ms - max_age_millis)
    }
}

pub struct IcebergSink {
    pub config: IcebergConfig,
    param: SinkParam,
    // In upsert mode, it never be None and empty.
    unique_column_ids: Option<Vec<usize>>,
}

impl EnforceSecret for IcebergSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            IcebergConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for IcebergSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = IcebergConfig::from_btreemap(param.properties.clone())?;
        IcebergSink::new(config, param)
    }
}

impl Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish()
    }
}

impl IcebergSink {
    pub async fn create_and_validate_table(&self) -> Result<Table> {
        if self.config.create_table_if_not_exists {
            self.create_table_if_not_exists().await?;
        }

        let table = self
            .config
            .load_table()
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        let sink_schema = self.param.schema();
        let iceberg_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        try_matches_arrow_schema(&sink_schema, &iceberg_arrow_schema)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        Ok(table)
    }

    pub async fn create_table_if_not_exists(&self) -> Result<()> {
        let catalog = self.config.create_catalog().await?;
        let namespace = if let Some(database_name) = self.config.table.database_name() {
            let namespace = NamespaceIdent::new(database_name.to_owned());
            if !catalog
                .namespace_exists(&namespace)
                .await
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
            {
                catalog
                    .create_namespace(&namespace, HashMap::default())
                    .await
                    .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                    .context("failed to create iceberg namespace")?;
            }
            namespace
        } else {
            bail!("database name must be set if you want to create table")
        };

        let table_id = self
            .config
            .full_table_name()
            .context("Unable to parse table name")?;
        if !catalog
            .table_exists(&table_id)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
        {
            let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
            // convert risingwave schema -> arrow schema -> iceberg schema
            let arrow_fields = self
                .param
                .columns
                .iter()
                .map(|column| {
                    Ok(iceberg_create_table_arrow_convert
                        .to_arrow_field(&column.name, &column.data_type)
                        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                        .context(format!(
                            "failed to convert {}: {} to arrow type",
                            &column.name, &column.data_type
                        ))?)
                })
                .collect::<Result<Vec<ArrowField>>>()?;
            let arrow_schema = arrow_schema_iceberg::Schema::new(arrow_fields);
            let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context("failed to convert arrow schema to iceberg schema")?;

            let location = {
                let mut names = namespace.clone().inner();
                names.push(self.config.table.table_name().to_owned());
                match &self.config.common.warehouse_path {
                    Some(warehouse_path) => {
                        let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                        let url = Url::parse(warehouse_path);
                        if url.is_err() || is_s3_tables {
                            // For rest catalog, the warehouse_path could be a warehouse name.
                            // In this case, we should specify the location when creating a table.
                            if self.config.common.catalog_type() == "rest"
                                || self.config.common.catalog_type() == "rest_rust"
                            {
                                None
                            } else {
                                bail!(format!("Invalid warehouse path: {}", warehouse_path))
                            }
                        } else if warehouse_path.ends_with('/') {
                            Some(format!("{}{}", warehouse_path, names.join("/")))
                        } else {
                            Some(format!("{}/{}", warehouse_path, names.join("/")))
                        }
                    }
                    None => None,
                }
            };

            let partition_spec = match &self.config.partition_by {
                Some(partition_by) => {
                    let mut partition_fields = Vec::<UnboundPartitionField>::new();
                    for (i, (column, transform)) in parse_partition_by_exprs(partition_by.clone())?
                        .into_iter()
                        .enumerate()
                    {
                        match iceberg_schema.field_id_by_name(&column) {
                            Some(id) => partition_fields.push(
                                UnboundPartitionField::builder()
                                    .source_id(id)
                                    .transform(transform)
                                    .name(format!("_p_{}", column))
                                    .field_id(i as i32)
                                    .build(),
                            ),
                            None => bail!(format!(
                                "Partition source column does not exist in schema: {}",
                                column
                            )),
                        };
                    }
                    Some(
                        UnboundPartitionSpec::builder()
                            .with_spec_id(0)
                            .add_partition_fields(partition_fields)
                            .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                            .context("failed to add partition columns")?
                            .build(),
                    )
                }
                None => None,
            };

            let table_creation_builder = TableCreation::builder()
                .name(self.config.table.table_name().to_owned())
                .schema(iceberg_schema);

            let table_creation = match (location, partition_spec) {
                (Some(location), Some(partition_spec)) => table_creation_builder
                    .location(location)
                    .partition_spec(partition_spec)
                    .build(),
                (Some(location), None) => table_creation_builder.location(location).build(),
                (None, Some(partition_spec)) => table_creation_builder
                    .partition_spec(partition_spec)
                    .build(),
                (None, None) => table_creation_builder.build(),
            };

            catalog
                .create_table(&namespace, table_creation)
                .await
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context("failed to create iceberg table")?;
        }
        Ok(())
    }

    pub fn new(config: IcebergConfig, param: SinkParam) -> Result<Self> {
        let unique_column_ids = if config.r#type == SINK_TYPE_UPSERT && !config.force_append_only {
            if let Some(pk) = &config.primary_key {
                let mut unique_column_ids = Vec::with_capacity(pk.len());
                for col_name in pk {
                    let id = param
                        .columns
                        .iter()
                        .find(|col| col.name.as_str() == col_name)
                        .ok_or_else(|| {
                            SinkError::Config(anyhow!(
                                "Primary key column {} not found in sink schema",
                                col_name
                            ))
                        })?
                        .column_id
                        .get_id() as usize;
                    unique_column_ids.push(id);
                }
                Some(unique_column_ids)
            } else {
                unreachable!()
            }
        } else {
            None
        };
        Ok(Self {
            config,
            param,
            unique_column_ids,
        })
    }
}

impl Sink for IcebergSink {
    type Coordinator = IcebergSinkCommitter;
    type LogSinker = CoordinatedLogSinker<IcebergSinkWriter>;

    const SINK_NAME: &'static str = ICEBERG_SINK;

    async fn validate(&self) -> Result<()> {
        if "snowflake".eq_ignore_ascii_case(self.config.catalog_type()) {
            bail!("Snowflake catalog only supports iceberg sources");
        }
        if "glue".eq_ignore_ascii_case(self.config.catalog_type()) {
            risingwave_common::license::Feature::IcebergSinkWithGlue
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        let _ = self.create_and_validate_table().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        let iceberg_config = IcebergConfig::from_btreemap(config.clone())?;

        if let Some(compaction_interval) = iceberg_config.compaction_interval_sec {
            if iceberg_config.enable_compaction && compaction_interval == 0 {
                bail!(
                    "`compaction_interval_sec` must be greater than 0 when `enable_compaction` is true"
                );
            } else {
                // log compaction_interval
                tracing::info!(
                    "Alter config compaction_interval set to {} seconds",
                    compaction_interval
                );
            }
        }

        if let Some(max_snapshots) = iceberg_config.max_snapshots_num_before_compaction
            && max_snapshots < 1
        {
            bail!(
                "`max_snapshots_num_before_compaction` must be greater than 0, got: {}",
                max_snapshots
            );
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let table = self.create_and_validate_table().await?;
        let inner = if let Some(unique_column_ids) = &self.unique_column_ids {
            IcebergSinkWriter::new_upsert(table, unique_column_ids.clone(), &writer_param).await?
        } else {
            IcebergSinkWriter::new_append_only(table, &writer_param).await?
        };

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );
        let writer = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            inner,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(writer)
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<Self::Coordinator> {
        let catalog = self.config.create_catalog().await?;
        let table = self.create_and_validate_table().await?;
        Ok(IcebergSinkCommitter {
            catalog,
            table,
            last_commit_epoch: 0,
            sink_id: self.param.sink_id.sink_id(),
            config: self.config.clone(),
            param: self.param.clone(),
            commit_retry_num: self.config.commit_retry_num,
            iceberg_compact_stat_sender,
        })
    }
}

/// None means no project.
/// Prepare represent the extra partition column idx.
/// Done represents the project idx vec.
///
/// The `ProjectIdxVec` will be late-evaluated. When we encounter the Prepare state first, we will use the data chunk schema
/// to create the project idx vec.
enum ProjectIdxVec {
    None,
    Prepare(usize),
    Done(Vec<usize>),
}

pub struct IcebergSinkWriter {
    writer: IcebergWriterDispatch,
    arrow_schema: SchemaRef,
    // See comments below
    metrics: IcebergWriterMetrics,
    // State of iceberg table for this writer
    table: Table,
    // For chunk with extra partition column, we should remove this column before write.
    // This project index vec is used to avoid create project idx each time.
    project_idx_vec: ProjectIdxVec,
}

#[allow(clippy::type_complexity)]
enum IcebergWriterDispatch {
    PartitionAppendOnly {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: MonitoredGeneralWriterBuilder<
            FanoutPartitionWriterBuilder<
                DataFileWriterBuilder<
                    ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                >,
            >,
        >,
    },
    NonPartitionAppendOnly {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: MonitoredGeneralWriterBuilder<
            DataFileWriterBuilder<
                ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
            >,
        >,
    },
    PartitionUpsert {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: MonitoredGeneralWriterBuilder<
            FanoutPartitionWriterBuilder<
                EqualityDeltaWriterBuilder<
                    DataFileWriterBuilder<
                        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                    >,
                    MonitoredPositionDeleteWriterBuilder<
                        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                    >,
                    EqualityDeleteFileWriterBuilder<
                        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                    >,
                >,
            >,
        >,
        arrow_schema_with_op_column: SchemaRef,
    },
    NonpartitionUpsert {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: MonitoredGeneralWriterBuilder<
            EqualityDeltaWriterBuilder<
                DataFileWriterBuilder<
                    ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                >,
                MonitoredPositionDeleteWriterBuilder<
                    ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                >,
                EqualityDeleteFileWriterBuilder<
                    ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
                >,
            >,
        >,
        arrow_schema_with_op_column: SchemaRef,
    },
}

impl IcebergWriterDispatch {
    pub fn get_writer(&mut self) -> Option<&mut Box<dyn IcebergWriter>> {
        match self {
            IcebergWriterDispatch::PartitionAppendOnly { writer, .. }
            | IcebergWriterDispatch::NonPartitionAppendOnly { writer, .. }
            | IcebergWriterDispatch::PartitionUpsert { writer, .. }
            | IcebergWriterDispatch::NonpartitionUpsert { writer, .. } => writer.as_mut(),
        }
    }
}

pub struct IcebergWriterMetrics {
    // NOTE: These 2 metrics are not used directly by us, but only kept for lifecycle management.
    // They are actually used in `PrometheusWriterBuilder`:
    //     WriterMetrics::new(write_qps.deref().clone(), write_latency.deref().clone())
    // We keep them here to let the guard cleans the labels from metrics registry when dropped
    _write_qps: LabelGuardedIntCounter,
    _write_latency: LabelGuardedHistogram,
    write_bytes: LabelGuardedIntCounter,
}

impl IcebergSinkWriter {
    pub async fn new_append_only(table: Table, writer_param: &SinkWriterParam) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();

        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();

        let parquet_writer_properties = WriterProperties::builder()
            .set_max_row_group_size(
                writer_param
                    .streaming_config
                    .developer
                    .iceberg_sink_write_parquet_max_row_group_rows,
            )
            .build();

        let parquet_writer_builder = ParquetWriterBuilder::new(
            parquet_writer_properties,
            schema.clone(),
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            DefaultFileNameGenerator::new(
                writer_param.actor_id.to_string(),
                Some(unique_uuid_suffix.to_string()),
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );
        let data_file_builder =
            DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id());
        if partition_spec.fields().is_empty() {
            let writer_builder = MonitoredGeneralWriterBuilder::new(
                data_file_builder,
                write_qps.clone(),
                write_latency.clone(),
            );
            let inner_writer = Some(Box::new(
                writer_builder
                    .clone()
                    .build()
                    .await
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ) as Box<dyn IcebergWriter>);
            Ok(Self {
                arrow_schema: Arc::new(
                    schema_to_arrow_schema(table.metadata().current_schema())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                ),
                metrics: IcebergWriterMetrics {
                    _write_qps: write_qps,
                    _write_latency: write_latency,
                    write_bytes,
                },
                writer: IcebergWriterDispatch::NonPartitionAppendOnly {
                    writer: inner_writer,
                    writer_builder,
                },
                table,
                project_idx_vec: {
                    if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                        ProjectIdxVec::Prepare(*extra_partition_col_idx)
                    } else {
                        ProjectIdxVec::None
                    }
                },
            })
        } else {
            let partition_builder = MonitoredGeneralWriterBuilder::new(
                FanoutPartitionWriterBuilder::new(
                    data_file_builder,
                    partition_spec.clone(),
                    schema.clone(),
                )
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                write_qps.clone(),
                write_latency.clone(),
            );
            let inner_writer = Some(Box::new(
                partition_builder
                    .clone()
                    .build()
                    .await
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ) as Box<dyn IcebergWriter>);
            Ok(Self {
                arrow_schema: Arc::new(
                    schema_to_arrow_schema(table.metadata().current_schema())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                ),
                metrics: IcebergWriterMetrics {
                    _write_qps: write_qps,
                    _write_latency: write_latency,
                    write_bytes,
                },
                writer: IcebergWriterDispatch::PartitionAppendOnly {
                    writer: inner_writer,
                    writer_builder: partition_builder,
                },
                table,
                project_idx_vec: {
                    if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                        ProjectIdxVec::Prepare(*extra_partition_col_idx)
                    } else {
                        ProjectIdxVec::None
                    }
                },
            })
        }
    }

    pub async fn new_upsert(
        table: Table,
        unique_column_ids: Vec<usize>,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];
        let unique_column_ids: Vec<_> = unique_column_ids.into_iter().map(|id| id as i32).collect();

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let position_delete_cache_num = GLOBAL_SINK_METRICS
            .iceberg_position_delete_cache_num
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        // Determine the schema id and partition spec id
        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();

        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();

        let parquet_writer_properties = WriterProperties::builder()
            .set_max_row_group_size(
                writer_param
                    .streaming_config
                    .developer
                    .iceberg_sink_write_parquet_max_row_group_rows,
            )
            .build();

        let data_file_builder = {
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties.clone(),
                schema.clone(),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(unique_uuid_suffix.to_string()),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id())
        };
        let position_delete_builder = {
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties.clone(),
                POSITION_DELETE_SCHEMA.clone(),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("pos-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            MonitoredPositionDeleteWriterBuilder::new(
                SortPositionDeleteWriterBuilder::new(
                    parquet_writer_builder,
                    writer_param
                        .streaming_config
                        .developer
                        .iceberg_sink_positional_delete_cache_size,
                    None,
                    None,
                ),
                position_delete_cache_num,
            )
        };
        let equality_delete_builder = {
            let config = EqualityDeleteWriterConfig::new(
                unique_column_ids.clone(),
                table.metadata().current_schema().clone(),
                None,
                partition_spec.spec_id(),
            )
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties.clone(),
                Arc::new(
                    arrow_schema_to_schema(config.projected_arrow_schema_ref())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                ),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("eq-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );

            EqualityDeleteFileWriterBuilder::new(parquet_writer_builder, config)
        };
        let delta_builder = EqualityDeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
        );
        if partition_spec.fields().is_empty() {
            let writer_builder = MonitoredGeneralWriterBuilder::new(
                delta_builder,
                write_qps.clone(),
                write_latency.clone(),
            );
            let inner_writer = Some(Box::new(
                writer_builder
                    .clone()
                    .build()
                    .await
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ) as Box<dyn IcebergWriter>);
            let original_arrow_schema = Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            );
            let schema_with_extra_op_column = {
                let mut new_fields = original_arrow_schema.fields().iter().cloned().collect_vec();
                new_fields.push(Arc::new(ArrowField::new(
                    "op".to_owned(),
                    ArrowDataType::Int32,
                    false,
                )));
                Arc::new(ArrowSchema::new(new_fields))
            };
            Ok(Self {
                arrow_schema: original_arrow_schema,
                metrics: IcebergWriterMetrics {
                    _write_qps: write_qps,
                    _write_latency: write_latency,
                    write_bytes,
                },
                table,
                writer: IcebergWriterDispatch::NonpartitionUpsert {
                    writer: inner_writer,
                    writer_builder,
                    arrow_schema_with_op_column: schema_with_extra_op_column,
                },
                project_idx_vec: {
                    if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                        ProjectIdxVec::Prepare(*extra_partition_col_idx)
                    } else {
                        ProjectIdxVec::None
                    }
                },
            })
        } else {
            let original_arrow_schema = Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            );
            let schema_with_extra_op_column = {
                let mut new_fields = original_arrow_schema.fields().iter().cloned().collect_vec();
                new_fields.push(Arc::new(ArrowField::new(
                    "op".to_owned(),
                    ArrowDataType::Int32,
                    false,
                )));
                Arc::new(ArrowSchema::new(new_fields))
            };
            let partition_builder = MonitoredGeneralWriterBuilder::new(
                FanoutPartitionWriterBuilder::new_with_custom_schema(
                    delta_builder,
                    schema_with_extra_op_column.clone(),
                    partition_spec.clone(),
                    table.metadata().current_schema().clone(),
                ),
                write_qps.clone(),
                write_latency.clone(),
            );
            let inner_writer = Some(Box::new(
                partition_builder
                    .clone()
                    .build()
                    .await
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ) as Box<dyn IcebergWriter>);
            Ok(Self {
                arrow_schema: original_arrow_schema,
                metrics: IcebergWriterMetrics {
                    _write_qps: write_qps,
                    _write_latency: write_latency,
                    write_bytes,
                },
                table,
                writer: IcebergWriterDispatch::PartitionUpsert {
                    writer: inner_writer,
                    writer_builder: partition_builder,
                    arrow_schema_with_op_column: schema_with_extra_op_column,
                },
                project_idx_vec: {
                    if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                        ProjectIdxVec::Prepare(*extra_partition_col_idx)
                    } else {
                        ProjectIdxVec::None
                    }
                },
            })
        }
    }
}

#[async_trait]
impl SinkWriter for IcebergSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // Just skip it.
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // Try to build writer if it's None.
        match &mut self.writer {
            IcebergWriterDispatch::PartitionAppendOnly {
                writer,
                writer_builder,
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .clone()
                            .build()
                            .await
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
            IcebergWriterDispatch::NonPartitionAppendOnly {
                writer,
                writer_builder,
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .clone()
                            .build()
                            .await
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
            IcebergWriterDispatch::PartitionUpsert {
                writer,
                writer_builder,
                ..
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .clone()
                            .build()
                            .await
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
            IcebergWriterDispatch::NonpartitionUpsert {
                writer,
                writer_builder,
                ..
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .clone()
                            .build()
                            .await
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
        };

        // Process the chunk.
        let (mut chunk, ops) = chunk.compact_vis().into_parts();
        match &self.project_idx_vec {
            ProjectIdxVec::None => {}
            ProjectIdxVec::Prepare(idx) => {
                if *idx >= chunk.columns().len() {
                    return Err(SinkError::Iceberg(anyhow!(
                        "invalid extra partition column index {}",
                        idx
                    )));
                }
                let project_idx_vec = (0..*idx)
                    .chain(*idx + 1..chunk.columns().len())
                    .collect_vec();
                chunk = chunk.project(&project_idx_vec);
                self.project_idx_vec = ProjectIdxVec::Done(project_idx_vec);
            }
            ProjectIdxVec::Done(idx_vec) => {
                chunk = chunk.project(idx_vec);
            }
        }
        if ops.is_empty() {
            return Ok(());
        }
        let write_batch_size = chunk.estimated_heap_size();
        let batch = match &self.writer {
            IcebergWriterDispatch::PartitionAppendOnly { .. }
            | IcebergWriterDispatch::NonPartitionAppendOnly { .. } => {
                // separate out insert chunk
                let filters =
                    chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
                chunk.set_visibility(filters);
                IcebergArrowConvert
                    .to_record_batch(self.arrow_schema.clone(), &chunk.compact_vis())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
            IcebergWriterDispatch::PartitionUpsert {
                arrow_schema_with_op_column,
                ..
            }
            | IcebergWriterDispatch::NonpartitionUpsert {
                arrow_schema_with_op_column,
                ..
            } => {
                let chunk = IcebergArrowConvert
                    .to_record_batch(self.arrow_schema.clone(), &chunk)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
                let ops = Arc::new(Int32Array::from(
                    ops.iter()
                        .map(|op| match op {
                            Op::UpdateInsert | Op::Insert => INSERT_OP,
                            Op::UpdateDelete | Op::Delete => DELETE_OP,
                        })
                        .collect_vec(),
                ));
                let mut columns = chunk.columns().to_vec();
                columns.push(ops);
                RecordBatch::try_new(arrow_schema_with_op_column.clone(), columns)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
        };

        let writer = self.writer.get_writer().unwrap();
        writer
            .write(batch)
            .instrument_await("iceberg_write")
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        self.metrics.write_bytes.inc_by(write_batch_size as _);
        Ok(())
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(None);
        }

        let close_result = match &mut self.writer {
            IcebergWriterDispatch::PartitionAppendOnly {
                writer,
                writer_builder,
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.clone().build().await {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
            IcebergWriterDispatch::NonPartitionAppendOnly {
                writer,
                writer_builder,
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.clone().build().await {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
            IcebergWriterDispatch::PartitionUpsert {
                writer,
                writer_builder,
                ..
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.clone().build().await {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
            IcebergWriterDispatch::NonpartitionUpsert {
                writer,
                writer_builder,
                ..
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.clone().build().await {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
        };

        match close_result {
            Some(Ok(result)) => {
                let version = self.table.metadata().format_version() as u8;
                let partition_type = self.table.metadata().default_partition_type();
                let data_files = result
                    .into_iter()
                    .map(|f| {
                        SerializedDataFile::try_from(f, partition_type, version == 1)
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(SinkMetadata::try_from(&IcebergCommitResult {
                    data_files,
                    schema_id: self.table.metadata().current_schema_id(),
                    partition_spec_id: self.table.metadata().default_partition_spec_id(),
                })?))
            }
            Some(Err(err)) => Err(SinkError::Iceberg(anyhow!(err))),
            None => Err(SinkError::Iceberg(anyhow!("No writer to close"))),
        }
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }
}

const SCHEMA_ID: &str = "schema_id";
const PARTITION_SPEC_ID: &str = "partition_spec_id";
const DATA_FILES: &str = "data_files";

#[derive(Default, Clone)]
struct IcebergCommitResult {
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
}

impl IcebergCommitResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let mut values = if let serde_json::Value::Object(v) =
                serde_json::from_slice::<serde_json::Value>(&v.metadata)
                    .context("Can't parse iceberg sink metadata")?
            {
                v
            } else {
                bail!("iceberg sink metadata should be an object");
            };

            let schema_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
                schema_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have schema_id");
            }

            let partition_spec_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
                partition_spec_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have partition_spec_id");
            }

            let data_files: Vec<SerializedDataFile>;
            if let serde_json::Value::Array(values) = values
                .remove(DATA_FILES)
                .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
            {
                data_files = values
                    .into_iter()
                    .map(from_value::<SerializedDataFile>)
                    .collect::<std::result::Result<_, _>>()
                    .unwrap();
            } else {
                bail!("iceberg sink metadata should have data_files object");
            }

            Ok(Self {
                schema_id: schema_id as i32,
                partition_spec_id: partition_spec_id as i32,
                data_files,
            })
        } else {
            bail!("Can't create iceberg sink write result from empty data!")
        }
    }

    fn try_from_serialized_bytes(value: Vec<u8>) -> Result<Self> {
        let mut values = if let serde_json::Value::Object(value) =
            serde_json::from_slice::<serde_json::Value>(&value)
                .context("Can't parse iceberg sink metadata")?
        {
            value
        } else {
            bail!("iceberg sink metadata should be an object");
        };

        let schema_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
            schema_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have schema_id");
        }

        let partition_spec_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
            partition_spec_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have partition_spec_id");
        }

        let data_files: Vec<SerializedDataFile>;
        if let serde_json::Value::Array(values) = values
            .remove(DATA_FILES)
            .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
        {
            data_files = values
                .into_iter()
                .map(from_value::<SerializedDataFile>)
                .collect::<std::result::Result<_, _>>()
                .unwrap();
        } else {
            bail!("iceberg sink metadata should have data_files object");
        }

        Ok(Self {
            schema_id: schema_id as i32,
            partition_spec_id: partition_spec_id as i32,
            data_files,
        })
    }
}

impl<'a> TryFrom<&'a IcebergCommitResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a IcebergCommitResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata {
                metadata: serde_json::to_vec(&json_value)
                    .context("Can't serialize iceberg sink metadata")?,
            })),
        })
    }
}

impl TryFrom<IcebergCommitResult> for Vec<u8> {
    type Error = SinkError;

    fn try_from(value: IcebergCommitResult) -> std::result::Result<Vec<u8>, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(serde_json::to_vec(&json_value).context("Can't serialize iceberg sink metadata")?)
    }
}
pub struct IcebergSinkCommitter {
    catalog: Arc<dyn Catalog>,
    table: Table,
    pub last_commit_epoch: u64,
    pub(crate) sink_id: u32,
    pub(crate) config: IcebergConfig,
    pub(crate) param: SinkParam,
    commit_retry_num: u32,
    pub(crate) iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
}

impl IcebergSinkCommitter {
    // Reload table and guarantee current schema_id and partition_spec_id matches
    // given `schema_id` and `partition_spec_id`
    async fn reload_table(
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        schema_id: i32,
        partition_spec_id: i32,
    ) -> Result<Table> {
        let table = catalog
            .load_table(table_ident)
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        if table.metadata().current_schema_id() != schema_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Schema evolution not supported, expect schema id {}, but got {}",
                schema_id,
                table.metadata().current_schema_id()
            )));
        }
        if table.metadata().default_partition_spec_id() != partition_spec_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Partition evolution not supported, expect partition spec id {}, but got {}",
                partition_spec_id,
                table.metadata().default_partition_spec_id()
            )));
        }
        Ok(table)
    }

    fn pre_commit_inner(
        &mut self,
        _epoch: u64,
        metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<Vec<IcebergCommitResult>> {
        if let Some(add_columns) = add_columns {
            return Err(anyhow!(
                "Iceberg sink not support add columns, but got: {:?}",
                add_columns
            )
            .into());
        }

        let write_results: Vec<IcebergCommitResult> = metadata
            .iter()
            .map(IcebergCommitResult::try_from)
            .collect::<Result<Vec<IcebergCommitResult>>>()?;

        // Skip if no data to commit
        if write_results.is_empty() || write_results.iter().all(|r| r.data_files.is_empty()) {
            return Ok(vec![]);
        }

        // guarantee that all write results has same schema_id and partition_spec_id
        if write_results
            .iter()
            .any(|r| r.schema_id != write_results[0].schema_id)
            || write_results
                .iter()
                .any(|r| r.partition_spec_id != write_results[0].partition_spec_id)
        {
            return Err(SinkError::Iceberg(anyhow!(
                "schema_id and partition_spec_id should be the same in all write results"
            )));
        }

        Ok(write_results)
    }
}

#[async_trait::async_trait]
impl SinkCommitCoordinator for IcebergSinkCommitter {
    fn strategy(&self) -> SinkCommitStrategy {
        if self.config.is_exactly_once.unwrap_or_default() {
            SinkCommitStrategy::TwoPhase
        } else {
            SinkCommitStrategy::SinglePhase
        }
    }

    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            "Sink id = {}: iceberg sink coordinator initing.",
            self.param.sink_id.sink_id
        );

        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<Vec<u8>> {
        tracing::info!("Starting iceberg pre commit in epoch {epoch}");

        let write_results = self.pre_commit_inner(epoch, metadata, add_columns)?;

        if write_results.is_empty() {
            return Ok(vec![]);
        }

        let mut pre_commit_metadata_bytes = Vec::new();
        for each_parallelism_write_result in write_results {
            let each_parallelism_write_result_bytes: Vec<u8> =
                each_parallelism_write_result.try_into()?;
            pre_commit_metadata_bytes.push(each_parallelism_write_result_bytes);
        }

        let pre_commit_metadata_bytes: Vec<u8> = serialize_metadata(pre_commit_metadata_bytes);
        Ok(pre_commit_metadata_bytes)
    }

    async fn commit(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::info!("Starting iceberg commit in epoch {epoch}");
        if commit_metadata.is_empty() {
            tracing::debug!(?epoch, "no data to commit");
            return Ok(());
        }

        let write_results_bytes = deserialize_metadata(commit_metadata);
        let mut write_results = vec![];

        for each in write_results_bytes {
            let write_result = IcebergCommitResult::try_from_serialized_bytes(each)?;
            write_results.push(write_result);
        }

        self.commit_iceberg_inner(epoch, write_results).await?;

        Ok(())
    }

    async fn abort(&mut self, _epoch: u64, _commit_metadata: Vec<u8>) {
        // TODO: Files that have been written but not committed should be deleted.
        tracing::debug!("Abort not implemented yet");
    }

    async fn commit_directly(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<()> {
        tracing::info!("Starting iceberg direct commit in epoch {epoch}");

        let write_results = self.pre_commit_inner(epoch, metadata, add_columns)?;

        if write_results.is_empty() {
            tracing::debug!(?epoch, "no data to commit");
            return Ok(());
        }

        self.commit_iceberg_inner(epoch, write_results).await
    }
}

/// Methods Required to Achieve Exactly Once Semantics
impl IcebergSinkCommitter {
    async fn commit_iceberg_inner(
        &mut self,
        epoch: u64,
        write_results: Vec<IcebergCommitResult>,
    ) -> Result<()> {
        // Empty write results should be handled before calling this function.
        assert!(
            !write_results.is_empty() && !write_results.iter().all(|r| r.data_files.is_empty())
        );

        // Check snapshot limit before proceeding with commit
        self.wait_for_snapshot_limit().await?;

        // If the provided `snapshot_id`` is not None, it indicates that this commit is a re commit
        // occurring during the recovery phase. In this case, we need to use the `snapshot_id`
        // that was previously persisted in the system table to commit.
        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // Load the latest table to avoid concurrent modification with the best effort.
        self.table = Self::reload_table(
            self.catalog.as_ref(),
            self.table.identifier(),
            expect_schema_id,
            expect_partition_spec_id,
        )
        .await?;
        let Some(schema) = self.table.metadata().schema_by_id(expect_schema_id) else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find schema by id {}",
                expect_schema_id
            )));
        };
        let Some(partition_spec) = self
            .table
            .metadata()
            .partition_spec_by_id(expect_partition_spec_id)
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find partition spec by id {}",
                expect_partition_spec_id
            )));
        };
        let partition_type = partition_spec
            .as_ref()
            .clone()
            .partition_type(schema)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        let txn = Transaction::new(&self.table);
        let snapshot_id = txn.generate_unique_snapshot_id();

        let data_files = write_results
            .into_iter()
            .flat_map(|r| {
                r.data_files.into_iter().map(|f| {
                    f.try_into(expect_partition_spec_id, &partition_type, schema)
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                })
            })
            .collect::<Result<Vec<DataFile>>>()?;
        // # TODO:
        // This retry behavior should be revert and do in iceberg-rust when it supports retry(Track in: https://github.com/apache/iceberg-rust/issues/964)
        // because retry logic involved reapply the commit metadata.
        // For now, we just retry the commit operation.
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(60))
            .map(jitter)
            .take(self.commit_retry_num as usize);
        let catalog = self.catalog.clone();
        let table_ident = self.table.identifier().clone();
        let table = Retry::spawn(retry_strategy, || async {
            let table = Self::reload_table(
                catalog.as_ref(),
                &table_ident,
                expect_schema_id,
                expect_partition_spec_id,
            )
            .await?;
            let txn = Transaction::new(&table);
            let mut append_action = txn
                .fast_append(Some(snapshot_id), None, vec![])
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
                .with_to_branch(commit_branch(
                    self.config.r#type.as_str(),
                    self.config.write_mode.as_str(),
                ));
            append_action
                .add_data_files(data_files.clone())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            let tx = append_action.apply().await.map_err(|err| {
                tracing::error!(error = %err.as_report(), "Failed to apply iceberg table");
                SinkError::Iceberg(anyhow!(err))
            })?;
            tx.commit(self.catalog.as_ref()).await.map_err(|err| {
                tracing::error!(error = %err.as_report(), "Failed to commit iceberg table");
                SinkError::Iceberg(anyhow!(err))
            })
        })
        .await?;
        self.table = table;

        let snapshot_num = self.table.metadata().snapshots().count();
        let catalog_name = self.config.common.catalog_name();
        let table_name = self.table.identifier().to_string();
        let metrics_labels = [&self.param.sink_name, &catalog_name, &table_name];
        GLOBAL_SINK_METRICS
            .iceberg_snapshot_num
            .with_guarded_label_values(&metrics_labels)
            .set(snapshot_num as i64);

        tracing::info!("Succeeded to commit to iceberg table in epoch {epoch}.");

        if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
            && self.config.enable_compaction
            && iceberg_compact_stat_sender
                .send(IcebergSinkCompactionUpdate {
                    sink_id: SinkId::new(self.sink_id),
                    compaction_interval: self.config.compaction_interval_sec(),
                    force_compaction: false,
                })
                .is_err()
        {
            warn!("failed to send iceberg compaction stats");
        }

        Ok(())
    }

    /// Check if the number of snapshots since the last rewrite/overwrite operation exceeds the limit
    /// Returns the number of snapshots since the last rewrite/overwrite
    fn count_snapshots_since_rewrite(&self) -> usize {
        let mut snapshots: Vec<_> = self.table.metadata().snapshots().collect();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ms()));

        // Iterate through snapshots in reverse order (newest first) to find the last rewrite/overwrite
        let mut count = 0;
        for snapshot in snapshots {
            // Check if this snapshot represents a rewrite or overwrite operation
            let summary = snapshot.summary();
            match &summary.operation {
                Operation::Replace => {
                    // Found a rewrite/overwrite operation, stop counting
                    break;
                }

                _ => {
                    // Increment count for each snapshot that is not a rewrite/overwrite
                    count += 1;
                }
            }
        }

        count
    }

    /// Wait until snapshot count since last rewrite is below the limit
    async fn wait_for_snapshot_limit(&mut self) -> Result<()> {
        if !self.config.enable_compaction {
            return Ok(());
        }

        if let Some(max_snapshots) = self.config.max_snapshots_num_before_compaction {
            loop {
                let current_count = self.count_snapshots_since_rewrite();

                if current_count < max_snapshots {
                    tracing::info!(
                        "Snapshot count check passed: {} < {}",
                        current_count,
                        max_snapshots
                    );
                    break;
                }

                tracing::info!(
                    "Snapshot count {} exceeds limit {}, waiting...",
                    current_count,
                    max_snapshots
                );

                if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
                    && iceberg_compact_stat_sender
                        .send(IcebergSinkCompactionUpdate {
                            sink_id: SinkId::new(self.sink_id),
                            compaction_interval: self.config.compaction_interval_sec(),
                            force_compaction: true,
                        })
                        .is_err()
                {
                    tracing::warn!("failed to send iceberg compaction stats");
                }

                // Wait for 30 seconds before checking again
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Reload table to get latest snapshots
                self.table = self.config.load_table().await?;
            }
        }
        Ok(())
    }
}

const MAP_KEY: &str = "key";
const MAP_VALUE: &str = "value";

fn get_fields<'a>(
    our_field_type: &'a risingwave_common::types::DataType,
    data_type: &ArrowDataType,
    schema_fields: &mut HashMap<&'a str, &'a risingwave_common::types::DataType>,
) -> Option<ArrowFields> {
    match data_type {
        ArrowDataType::Struct(fields) => {
            match our_field_type {
                risingwave_common::types::DataType::Struct(struct_fields) => {
                    struct_fields.iter().for_each(|(name, data_type)| {
                        let res = schema_fields.insert(name, data_type);
                        // This assert is to make sure there is no duplicate field name in the schema.
                        assert!(res.is_none())
                    });
                }
                risingwave_common::types::DataType::Map(map_fields) => {
                    schema_fields.insert(MAP_KEY, map_fields.key());
                    schema_fields.insert(MAP_VALUE, map_fields.value());
                }
                risingwave_common::types::DataType::List(list) => {
                    list.elem()
                        .as_struct()
                        .iter()
                        .for_each(|(name, data_type)| {
                            let res = schema_fields.insert(name, data_type);
                            // This assert is to make sure there is no duplicate field name in the schema.
                            assert!(res.is_none())
                        });
                }
                _ => {}
            };
            Some(fields.clone())
        }
        ArrowDataType::List(field) | ArrowDataType::Map(field, _) => {
            get_fields(our_field_type, field.data_type(), schema_fields)
        }
        _ => None, // not a supported complex type and unlikely to show up
    }
}

fn check_compatibility(
    schema_fields: HashMap<&str, &risingwave_common::types::DataType>,
    fields: &ArrowFields,
) -> anyhow::Result<bool> {
    for arrow_field in fields {
        let our_field_type = schema_fields
            .get(arrow_field.name().as_str())
            .ok_or_else(|| anyhow!("Field {} not found in our schema", arrow_field.name()))?;

        // Iceberg source should be able to read iceberg decimal type.
        let converted_arrow_data_type = IcebergArrowConvert
            .to_arrow_field("", our_field_type)
            .map_err(|e| anyhow!(e))?
            .data_type()
            .clone();

        let compatible = match (&converted_arrow_data_type, arrow_field.data_type()) {
            (ArrowDataType::Decimal128(_, _), ArrowDataType::Decimal128(_, _)) => true,
            (ArrowDataType::Binary, ArrowDataType::LargeBinary) => true,
            (ArrowDataType::LargeBinary, ArrowDataType::Binary) => true,
            (ArrowDataType::List(_), ArrowDataType::List(field))
            | (ArrowDataType::Map(_, _), ArrowDataType::Map(field, _)) => {
                let mut schema_fields = HashMap::new();
                get_fields(our_field_type, field.data_type(), &mut schema_fields)
                    .is_none_or(|fields| check_compatibility(schema_fields, &fields).unwrap())
            }
            // validate nested structs
            (ArrowDataType::Struct(_), ArrowDataType::Struct(fields)) => {
                let mut schema_fields = HashMap::new();
                our_field_type
                    .as_struct()
                    .iter()
                    .for_each(|(name, data_type)| {
                        let res = schema_fields.insert(name, data_type);
                        // This assert is to make sure there is no duplicate field name in the schema.
                        assert!(res.is_none())
                    });
                check_compatibility(schema_fields, fields)?
            }
            // cases where left != right (metadata, field name mismatch)
            //
            // all nested types: in iceberg `field_id` will always be present, but RW doesn't have it:
            // {"PARQUET:field_id": ".."}
            //
            // map: The standard name in arrow is "entries", "key", "value".
            // in iceberg-rs, it's called "key_value"
            (left, right) => left.equals_datatype(right),
        };
        if !compatible {
            bail!(
                "field {}'s type is incompatible\nRisingWave converted data type: {}\niceberg's data type: {}",
                arrow_field.name(),
                converted_arrow_data_type,
                arrow_field.data_type()
            );
        }
    }
    Ok(true)
}

/// Try to match our schema with iceberg schema.
pub fn try_matches_arrow_schema(rw_schema: &Schema, arrow_schema: &ArrowSchema) -> Result<()> {
    if rw_schema.fields.len() != arrow_schema.fields().len() {
        bail!(
            "Schema length mismatch, risingwave is {}, and iceberg is {}",
            rw_schema.fields.len(),
            arrow_schema.fields.len()
        );
    }

    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(field.name.as_str(), &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });

    check_compatibility(schema_fields, &arrow_schema.fields)?;
    Ok(())
}

pub fn serialize_metadata(metadata: Vec<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(&metadata).unwrap()
}

pub fn deserialize_metadata(bytes: Vec<u8>) -> Vec<Vec<u8>> {
    serde_json::from_slice(&bytes).unwrap()
}

pub fn parse_partition_by_exprs(
    expr: String,
) -> std::result::Result<Vec<(String, Transform)>, anyhow::Error> {
    // captures column, transform(column), transform(n,column), transform(n, column)
    let re = Regex::new(r"(?<transform>\w+)(\(((?<n>\d+)?(?:,|(,\s)))?(?<field>\w+)\))?").unwrap();
    if !re.is_match(&expr) {
        bail!(format!(
            "Invalid partition fields: {}\nHINT: Supported formats are column, transform(column), transform(n,column), transform(n, column)",
            expr
        ))
    }
    let caps = re.captures_iter(&expr);

    let mut partition_columns = vec![];

    for mat in caps {
        let (column, transform) = if mat.name("n").is_none() && mat.name("field").is_none() {
            (&mat["transform"], Transform::Identity)
        } else {
            let mut func = mat["transform"].to_owned();
            if func == "bucket" || func == "truncate" {
                let n = &mat
                    .name("n")
                    .ok_or_else(|| anyhow!("The `n` must be set with `bucket` and `truncate`"))?
                    .as_str();
                func = format!("{func}[{n}]");
            }
            (
                &mat["field"],
                Transform::from_str(&func)
                    .with_context(|| format!("invalid transform function {}", func))?,
            )
        };
        partition_columns.push((column.to_owned(), transform));
    }
    Ok(partition_columns)
}

pub fn commit_branch(sink_type: &str, write_mode: &str) -> String {
    if should_enable_iceberg_cow(sink_type, write_mode) {
        ICEBERG_COW_BRANCH.to_owned()
    } else {
        MAIN_BRANCH.to_owned()
    }
}

pub fn should_enable_iceberg_cow(sink_type: &str, write_mode: &str) -> bool {
    sink_type == SINK_TYPE_UPSERT && write_mode == ICEBERG_WRITE_MODE_COPY_ON_WRITE
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use risingwave_common::array::arrow::arrow_schema_iceberg::FieldRef as ArrowFieldRef;
    use risingwave_common::types::{DataType, MapType, StructType};

    use crate::connector_common::{IcebergCommon, IcebergTableIdentifier};
    use crate::sink::decouple_checkpoint_log_sink::ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL;
    use crate::sink::iceberg::{
        COMPACTION_INTERVAL_SEC, ENABLE_COMPACTION, ENABLE_SNAPSHOT_EXPIRATION,
        ICEBERG_WRITE_MODE_MERGE_ON_READ, IcebergConfig, MAX_SNAPSHOTS_NUM,
        SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES, SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA,
        SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS, SNAPSHOT_EXPIRATION_RETAIN_LAST, WRITE_MODE,
    };

    pub const DEFAULT_ICEBERG_COMPACTION_INTERVAL: u64 = 3600; // 1 hour

    #[test]
    fn test_compatible_arrow_schema() {
        use arrow_schema_iceberg::{DataType as ArrowDataType, Field as ArrowField};

        use super::*;
        let risingwave_schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "a"),
            Field::with_name(DataType::Int32, "b"),
            Field::with_name(DataType::Int32, "c"),
        ]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("b", ArrowDataType::Int32, false),
            ArrowField::new("c", ArrowDataType::Int32, false),
        ]);

        try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();

        let risingwave_schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "d"),
            Field::with_name(DataType::Int32, "c"),
            Field::with_name(DataType::Int32, "a"),
            Field::with_name(DataType::Int32, "b"),
        ]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("b", ArrowDataType::Int32, false),
            ArrowField::new("d", ArrowDataType::Int32, false),
            ArrowField::new("c", ArrowDataType::Int32, false),
        ]);
        try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();

        let risingwave_schema = Schema::new(vec![
            Field::with_name(
                DataType::Struct(StructType::new(vec![
                    ("a1", DataType::Int32),
                    (
                        "a2",
                        DataType::Struct(StructType::new(vec![
                            ("a21", DataType::Bytea),
                            (
                                "a22",
                                DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                            ),
                        ])),
                    ),
                ])),
                "a",
            ),
            Field::with_name(
                DataType::list(DataType::Struct(StructType::new(vec![
                    ("b1", DataType::Int32),
                    ("b2", DataType::Bytea),
                    (
                        "b3",
                        DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                    ),
                ]))),
                "b",
            ),
            Field::with_name(
                DataType::Map(MapType::from_kv(
                    DataType::Varchar,
                    DataType::list(DataType::Struct(StructType::new([
                        ("c1", DataType::Int32),
                        ("c2", DataType::Bytea),
                        (
                            "c3",
                            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                        ),
                    ]))),
                )),
                "c",
            ),
        ]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "a",
                ArrowDataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("a1", ArrowDataType::Int32, false),
                    ArrowField::new(
                        "a2",
                        ArrowDataType::Struct(ArrowFields::from(vec![
                            ArrowField::new("a21", ArrowDataType::LargeBinary, false),
                            ArrowField::new_map(
                                "a22",
                                "entries",
                                ArrowFieldRef::new(ArrowField::new(
                                    "key",
                                    ArrowDataType::Utf8,
                                    false,
                                )),
                                ArrowFieldRef::new(ArrowField::new(
                                    "value",
                                    ArrowDataType::Utf8,
                                    false,
                                )),
                                false,
                                false,
                            ),
                        ])),
                        false,
                    ),
                ])),
                false,
            ),
            ArrowField::new(
                "b",
                ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                    ArrowDataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("b1", ArrowDataType::Int32, false),
                        ArrowField::new("b2", ArrowDataType::LargeBinary, false),
                        ArrowField::new_map(
                            "b3",
                            "entries",
                            ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                            ArrowFieldRef::new(ArrowField::new(
                                "value",
                                ArrowDataType::Utf8,
                                false,
                            )),
                            false,
                            false,
                        ),
                    ])),
                    false,
                ))),
                false,
            ),
            ArrowField::new_map(
                "c",
                "entries",
                ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                ArrowFieldRef::new(ArrowField::new(
                    "value",
                    ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                        ArrowDataType::Struct(ArrowFields::from(vec![
                            ArrowField::new("c1", ArrowDataType::Int32, false),
                            ArrowField::new("c2", ArrowDataType::LargeBinary, false),
                            ArrowField::new_map(
                                "c3",
                                "entries",
                                ArrowFieldRef::new(ArrowField::new(
                                    "key",
                                    ArrowDataType::Utf8,
                                    false,
                                )),
                                ArrowFieldRef::new(ArrowField::new(
                                    "value",
                                    ArrowDataType::Utf8,
                                    false,
                                )),
                                false,
                                false,
                            ),
                        ])),
                        false,
                    ))),
                    false,
                )),
                false,
                false,
            ),
        ]);
        try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();
    }

    #[test]
    fn test_parse_iceberg_config() {
        let values = [
            ("connector", "iceberg"),
            ("type", "upsert"),
            ("primary_key", "v1"),
            ("partition_by", "v1, identity(v1), truncate(4,v2), bucket(5,v1), year(v3), month(v4), day(v5), hour(v6), void(v1)"),
            ("warehouse.path", "s3://iceberg"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.path.style.access", "true"),
            ("s3.region", "us-east-1"),
            ("catalog.type", "jdbc"),
            ("catalog.name", "demo"),
            ("catalog.uri", "jdbc://postgresql://postgres:5432/iceberg"),
            ("catalog.jdbc.user", "admin"),
            ("catalog.jdbc.password", "123456"),
            ("database.name", "demo_db"),
            ("table.name", "demo_table"),
            ("enable_compaction", "true"),
            ("compaction_interval_sec", "1800"),
            ("enable_snapshot_expiration", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

        let expected_iceberg_config = IcebergConfig {
            common: IcebergCommon {
                warehouse_path: Some("s3://iceberg".to_owned()),
                catalog_uri: Some("jdbc://postgresql://postgres:5432/iceberg".to_owned()),
                region: Some("us-east-1".to_owned()),
                endpoint: Some("http://127.0.0.1:9301".to_owned()),
                access_key: Some("hummockadmin".to_owned()),
                secret_key: Some("hummockadmin".to_owned()),
                gcs_credential: None,
                catalog_type: Some("jdbc".to_owned()),
                glue_id: None,
                catalog_name: Some("demo".to_owned()),
                path_style_access: Some(true),
                credential: None,
                oauth2_server_uri: None,
                scope: None,
                token: None,
                enable_config_load: None,
                rest_signing_name: None,
                rest_signing_region: None,
                rest_sigv4_enabled: None,
                hosted_catalog: None,
                azblob_account_name: None,
                azblob_account_key: None,
                azblob_endpoint_url: None,
                header: None,
                adlsgen2_account_name: None,
                adlsgen2_account_key: None,
                adlsgen2_endpoint: None,
                vended_credentials: None,
            },
            table: IcebergTableIdentifier {
                database_name: Some("demo_db".to_owned()),
                table_name: "demo_table".to_owned(),
            },
            r#type: "upsert".to_owned(),
            force_append_only: false,
            primary_key: Some(vec!["v1".to_owned()]),
            partition_by: Some("v1, identity(v1), truncate(4,v2), bucket(5,v1), year(v3), month(v4), day(v5), hour(v6), void(v1)".to_owned()),
            java_catalog_props: [("jdbc.user", "admin"), ("jdbc.password", "123456")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            commit_checkpoint_interval: ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL,
            create_table_if_not_exists: false,
            is_exactly_once: Some(true),
            commit_retry_num: 8,
            enable_compaction: true,
            compaction_interval_sec: Some(DEFAULT_ICEBERG_COMPACTION_INTERVAL / 2),
            enable_snapshot_expiration: true,
            write_mode: ICEBERG_WRITE_MODE_MERGE_ON_READ.to_owned(),
            snapshot_expiration_max_age_millis: None,
            snapshot_expiration_retain_last: None,
            snapshot_expiration_clear_expired_files: true,
            snapshot_expiration_clear_expired_meta_data: true,
            max_snapshots_num_before_compaction: None,
        };

        assert_eq!(iceberg_config, expected_iceberg_config);

        assert_eq!(
            &iceberg_config.full_table_name().unwrap().to_string(),
            "demo_db.demo_table"
        );
    }

    async fn test_create_catalog(configs: BTreeMap<String, String>) {
        let iceberg_config = IcebergConfig::from_btreemap(configs).unwrap();

        let table = iceberg_config.load_table().await.unwrap();

        println!("{:?}", table.identifier());
    }

    #[tokio::test]
    #[ignore]
    async fn test_storage_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "storage"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_rest_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "rest"),
            ("catalog.uri", "http://192.168.167.4:8181"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_jdbc_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "jdbc"),
            ("catalog.uri", "jdbc:postgresql://localhost:5432/iceberg"),
            ("catalog.jdbc.user", "admin"),
            ("catalog.jdbc.password", "123456"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_hive_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "hive"),
            ("catalog.uri", "thrift://localhost:9083"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        test_create_catalog(values).await;
    }

    #[test]
    fn test_config_constants_consistency() {
        // This test ensures our constants match the expected configuration names
        // If you change a constant, this test will remind you to update both places
        assert_eq!(ENABLE_COMPACTION, "enable_compaction");
        assert_eq!(COMPACTION_INTERVAL_SEC, "compaction_interval_sec");
        assert_eq!(ENABLE_SNAPSHOT_EXPIRATION, "enable_snapshot_expiration");
        assert_eq!(WRITE_MODE, "write_mode");
        assert_eq!(
            SNAPSHOT_EXPIRATION_RETAIN_LAST,
            "snapshot_expiration_retain_last"
        );
        assert_eq!(
            SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS,
            "snapshot_expiration_max_age_millis"
        );
        assert_eq!(
            SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES,
            "snapshot_expiration_clear_expired_files"
        );
        assert_eq!(
            SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA,
            "snapshot_expiration_clear_expired_meta_data"
        );
        assert_eq!(MAX_SNAPSHOTS_NUM, "max_snapshots_num_before_compaction");
    }
}
