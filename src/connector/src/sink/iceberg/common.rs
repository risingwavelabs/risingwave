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

// Shared Iceberg sink logic used by both the old IcebergSink and the new V3 executors.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, Transform, UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use regex::Regex;
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    self, DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
    Schema as ArrowSchema,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use url::Url;
use with_options::WithOptions;

use crate::connector_common::{IcebergCommon, IcebergTableIdentifier};
use crate::enforce_secret::EnforceSecret;
use crate::sink::decouple_checkpoint_log_sink::iceberg_default_commit_checkpoint_interval;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkParam,
};
use crate::{deserialize_bool_from_string, deserialize_optional_string_seq_from_string};

pub const ICEBERG_SINK: &str = "iceberg";
pub const ICEBERG_COW_BRANCH: &str = "ingestion";
pub const ICEBERG_WRITE_MODE_MERGE_ON_READ: &str = "merge-on-read";
pub const ICEBERG_WRITE_MODE_COPY_ON_WRITE: &str = "copy-on-write";
pub const ICEBERG_COMPACTION_TYPE_FULL: &str = "full";
pub const ICEBERG_COMPACTION_TYPE_SMALL_FILES: &str = "small-files";
pub const ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE: &str = "files-with-delete";
pub const PARTITION_DATA_ID_START: i32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IcebergWriteMode {
    #[default]
    MergeOnRead,
    CopyOnWrite,
}

impl IcebergWriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            IcebergWriteMode::MergeOnRead => ICEBERG_WRITE_MODE_MERGE_ON_READ,
            IcebergWriteMode::CopyOnWrite => ICEBERG_WRITE_MODE_COPY_ON_WRITE,
        }
    }
}

impl std::str::FromStr for IcebergWriteMode {
    type Err = SinkError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            ICEBERG_WRITE_MODE_MERGE_ON_READ => Ok(IcebergWriteMode::MergeOnRead),
            ICEBERG_WRITE_MODE_COPY_ON_WRITE => Ok(IcebergWriteMode::CopyOnWrite),
            _ => Err(SinkError::Config(anyhow!(format!(
                "invalid write_mode: {}, must be one of: {}, {}",
                s, ICEBERG_WRITE_MODE_MERGE_ON_READ, ICEBERG_WRITE_MODE_COPY_ON_WRITE
            )))),
        }
    }
}

impl TryFrom<&str> for IcebergWriteMode {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for IcebergWriteMode {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl std::fmt::Display for IcebergWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Configuration constants
pub const ENABLE_COMPACTION: &str = "enable_compaction";
pub const COMPACTION_INTERVAL_SEC: &str = "compaction_interval_sec";
pub const ENABLE_SNAPSHOT_EXPIRATION: &str = "enable_snapshot_expiration";
pub const WRITE_MODE: &str = "write_mode";
pub const FORMAT_VERSION: &str = "format_version";
pub const SNAPSHOT_EXPIRATION_RETAIN_LAST: &str = "snapshot_expiration_retain_last";
pub const SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS: &str = "snapshot_expiration_max_age_millis";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES: &str = "snapshot_expiration_clear_expired_files";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA: &str =
    "snapshot_expiration_clear_expired_meta_data";
pub const COMPACTION_MAX_SNAPSHOTS_NUM: &str = "compaction.max_snapshots_num";

pub const COMPACTION_SMALL_FILES_THRESHOLD_MB: &str = "compaction.small_files_threshold_mb";

pub const COMPACTION_DELETE_FILES_COUNT_THRESHOLD: &str = "compaction.delete_files_count_threshold";

pub const COMPACTION_TRIGGER_SNAPSHOT_COUNT: &str = "compaction.trigger_snapshot_count";

pub const COMPACTION_TARGET_FILE_SIZE_MB: &str = "compaction.target_file_size_mb";

pub const COMPACTION_TYPE: &str = "compaction.type";

pub const COMPACTION_WRITE_PARQUET_COMPRESSION: &str = "compaction.write_parquet_compression";
pub const COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS: &str =
    "compaction.write_parquet_max_row_group_rows";
pub const COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB: &str = "commit_checkpoint_size_threshold_mb";
pub const ICEBERG_DEFAULT_COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB: u64 = 128;

pub const PARQUET_CREATED_BY: &str = concat!("risingwave version ", env!("CARGO_PKG_VERSION"));

fn default_commit_retry_num() -> u32 {
    8
}

fn default_commit_checkpoint_size_threshold_mb() -> Option<u64> {
    Some(ICEBERG_DEFAULT_COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB)
}

fn default_iceberg_write_mode() -> IcebergWriteMode {
    IcebergWriteMode::MergeOnRead
}

fn default_iceberg_format_version() -> FormatVersion {
    FormatVersion::V2
}

fn default_true() -> bool {
    true
}

fn default_some_true() -> Option<bool> {
    Some(true)
}

fn parse_format_version_str(value: &str) -> std::result::Result<FormatVersion, String> {
    let parsed = value
        .trim()
        .parse::<u8>()
        .map_err(|_| "`format-version` must be one of 1, 2, or 3".to_owned())?;
    match parsed {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        _ => Err("`format-version` must be one of 1, 2, or 3".to_owned()),
    }
}

fn deserialize_format_version<'de, D>(
    deserializer: D,
) -> std::result::Result<FormatVersion, D::Error>
where
    D: Deserializer<'de>,
{
    struct FormatVersionVisitor;

    impl<'de> Visitor<'de> for FormatVersionVisitor {
        type Value = FormatVersion;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("format-version as 1, 2, or 3")
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            let value = u8::try_from(value)
                .map_err(|_| E::custom("`format-version` must be one of 1, 2, or 3"))?;
            parse_format_version_str(&value.to_string()).map_err(E::custom)
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            let value = u8::try_from(value)
                .map_err(|_| E::custom("`format-version` must be one of 1, 2, or 3"))?;
            parse_format_version_str(&value.to_string()).map_err(E::custom)
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_format_version_str(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(FormatVersionVisitor)
}

/// Compaction type for Iceberg sink
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionType {
    /// Full compaction - rewrites all data files
    #[default]
    Full,
    /// Small files compaction - only compact small files
    SmallFiles,
    /// Files with delete compaction - only compact files that have associated delete files
    FilesWithDelete,
}

impl CompactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompactionType::Full => ICEBERG_COMPACTION_TYPE_FULL,
            CompactionType::SmallFiles => ICEBERG_COMPACTION_TYPE_SMALL_FILES,
            CompactionType::FilesWithDelete => ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE,
        }
    }
}

impl std::str::FromStr for CompactionType {
    type Err = SinkError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            ICEBERG_COMPACTION_TYPE_FULL => Ok(CompactionType::Full),
            ICEBERG_COMPACTION_TYPE_SMALL_FILES => Ok(CompactionType::SmallFiles),
            ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE => Ok(CompactionType::FilesWithDelete),
            _ => Err(SinkError::Config(anyhow!(format!(
                "invalid compaction_type: {}, must be one of: {}, {}, {}",
                s,
                ICEBERG_COMPACTION_TYPE_FULL,
                ICEBERG_COMPACTION_TYPE_SMALL_FILES,
                ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE
            )))),
        }
    }
}

impl TryFrom<&str> for CompactionType {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for CompactionType {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl std::fmt::Display for CompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergConfig {
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(flatten)]
    pub common: IcebergCommon,

    #[serde(flatten)]
    pub table: IcebergTableIdentifier,

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

    /// Commit on the next checkpoint barrier after buffered write size exceeds this threshold.
    /// Default is 128 MB.
    #[serde(default = "default_commit_checkpoint_size_threshold_mb")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_size_threshold_mb: Option<u64>,

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
    pub write_mode: IcebergWriteMode,

    /// Iceberg format version for table creation.
    #[serde(
        rename = "format_version",
        default = "default_iceberg_format_version",
        deserialize_with = "deserialize_format_version"
    )]
    pub format_version: FormatVersion,

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
    #[serde(rename = "compaction.max_snapshots_num", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub max_snapshots_num_before_compaction: Option<usize>,

    #[serde(rename = "compaction.small_files_threshold_mb", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub small_files_threshold_mb: Option<u64>,

    #[serde(rename = "compaction.delete_files_count_threshold", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub delete_files_count_threshold: Option<usize>,

    #[serde(rename = "compaction.trigger_snapshot_count", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub trigger_snapshot_count: Option<usize>,

    #[serde(rename = "compaction.target_file_size_mb", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub target_file_size_mb: Option<u64>,

    /// Compaction type: `full`, `small-files`, or `files-with-delete`
    /// If not set, will default to `full`
    #[serde(rename = "compaction.type", default)]
    #[with_option(allow_alter_on_fly)]
    pub compaction_type: Option<CompactionType>,

    /// Parquet compression codec
    /// Supported values: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd
    /// Default is snappy
    #[serde(rename = "compaction.write_parquet_compression", default)]
    #[with_option(allow_alter_on_fly)]
    pub write_parquet_compression: Option<String>,

    /// Maximum number of rows in a Parquet row group
    /// Default is 122880 (from developer config)
    #[serde(rename = "compaction.write_parquet_max_row_group_rows", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub write_parquet_max_row_group_rows: Option<usize>,

    /// Whether to enable PK index for upsert sink. Default is false.
    /// It's only used for V3 upsert iceberg sink to generate delete vectors.
    #[serde(
        rename = "enable_pk_index",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    pub enable_pk_index: bool,
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
    /// Validate that append-only sinks use merge-on-read mode
    /// Copy-on-write is strictly worse than merge-on-read for append-only workloads
    pub(crate) fn validate_append_only_write_mode(
        sink_type: &str,
        write_mode: IcebergWriteMode,
    ) -> Result<()> {
        if sink_type == SINK_TYPE_APPEND_ONLY && write_mode == IcebergWriteMode::CopyOnWrite {
            return Err(SinkError::Config(anyhow!(
                "'copy-on-write' mode is not supported for append-only iceberg sink. \
                 Please use 'merge-on-read' instead, which is strictly better for append-only workloads."
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_enable_pk_index(&self) -> Result<()> {
        if !self.enable_pk_index {
            return Ok(());
        }

        if self.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink"
            )));
        }

        if self.write_mode != IcebergWriteMode::MergeOnRead {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink with merge-on-read mode"
            )));
        }

        if self.format_version < FormatVersion::V3 {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink with format version >= 3"
            )));
        }

        if self.force_append_only {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` cannot be true when `force_append_only` is true"
            )));
        }

        Ok(())
    }

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
                        "`primary-key` must not be empty in {}",
                        SINK_TYPE_UPSERT
                    )));
                }
            } else {
                return Err(SinkError::Config(anyhow!(
                    "Must set `primary-key` in {}",
                    SINK_TYPE_UPSERT
                )));
            }
        }

        // Enforce merge-on-read for append-only sinks
        Self::validate_append_only_write_mode(&config.r#type, config.write_mode)?;
        config.validate_enable_pk_index()?;

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
                "`commit-checkpoint-interval` must be greater than 0"
            )));
        }

        if config.commit_checkpoint_size_threshold_mb == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_size_threshold_mb` must be greater than 0"
            )));
        }

        // Validate table identifier (e.g., database.name should not contain dots)
        config
            .table
            .validate()
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        Ok(config)
    }

    pub fn catalog_type(&self) -> &str {
        self.common.catalog_type()
    }

    pub async fn load_table(&self) -> Result<iceberg::table::Table> {
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

    pub fn table_format_version(&self) -> FormatVersion {
        self.format_version
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

    pub fn trigger_snapshot_count(&self) -> usize {
        self.trigger_snapshot_count.unwrap_or(usize::MAX)
    }

    pub fn small_files_threshold_mb(&self) -> u64 {
        self.small_files_threshold_mb.unwrap_or(64)
    }

    pub fn delete_files_count_threshold(&self) -> usize {
        self.delete_files_count_threshold.unwrap_or(256)
    }

    pub fn target_file_size_mb(&self) -> u64 {
        self.target_file_size_mb.unwrap_or(1024)
    }

    pub fn commit_checkpoint_size_threshold_bytes(&self) -> Option<u64> {
        self.commit_checkpoint_size_threshold_mb
            .map(|threshold_mb| threshold_mb.saturating_mul(1024 * 1024))
    }

    /// Get the compaction type as an enum
    /// This method parses the string and returns the enum value
    pub fn compaction_type(&self) -> CompactionType {
        self.compaction_type.unwrap_or_default()
    }

    /// Get the parquet compression codec
    /// Default is "zstd"
    pub fn write_parquet_compression(&self) -> &str {
        self.write_parquet_compression.as_deref().unwrap_or("zstd")
    }

    /// Get the maximum number of rows in a Parquet row group
    /// Default is 122880 (from developer config default)
    pub fn write_parquet_max_row_group_rows(&self) -> usize {
        self.write_parquet_max_row_group_rows.unwrap_or(122880)
    }

    /// Parse the compression codec string into Parquet Compression enum
    /// Returns SNAPPY as default if parsing fails or not specified
    pub fn get_parquet_compression(&self) -> Compression {
        parse_parquet_compression(self.write_parquet_compression())
    }
}

/// Parse compression codec string to Parquet Compression enum
pub(crate) fn parse_parquet_compression(codec: &str) -> Compression {
    match codec.to_lowercase().as_str() {
        "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "lzo" => Compression::LZO,
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4" => Compression::LZ4,
        "zstd" => Compression::ZSTD(Default::default()),
        _ => {
            tracing::warn!(
                "Unknown compression codec '{}', falling back to SNAPPY",
                codec
            );
            Compression::SNAPPY
        }
    }
}

pub fn parse_partition_by_exprs(
    expr: String,
) -> std::result::Result<Vec<(String, Transform)>, anyhow::Error> {
    // captures column, transform(column), transform(n,column), transform(n, column)
    let re = Regex::new(r"(?<transform>\w+)(\(((?<n>\d+)?(?:,|(,\s)))?(?<field>\w+)\))?").unwrap();
    if !re.is_match(&expr) {
        anyhow::bail!(format!(
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

pub fn commit_branch(sink_type: &str, write_mode: IcebergWriteMode) -> String {
    if should_enable_iceberg_cow(sink_type, write_mode) {
        ICEBERG_COW_BRANCH.to_owned()
    } else {
        MAIN_BRANCH.to_owned()
    }
}

pub fn should_enable_iceberg_cow(sink_type: &str, write_mode: IcebergWriteMode) -> bool {
    sink_type == SINK_TYPE_UPSERT && write_mode == IcebergWriteMode::CopyOnWrite
}

pub async fn create_and_validate_table_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<Table> {
    if config.create_table_if_not_exists {
        create_table_if_not_exists_impl(config, param).await?;
    }

    let table = config
        .load_table()
        .await
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    let sink_schema = param.schema();
    let iceberg_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    try_matches_arrow_schema(&sink_schema, &iceberg_arrow_schema)
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    Ok(table)
}

pub async fn create_table_if_not_exists_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<()> {
    let catalog = config.create_catalog().await?;
    let namespace = if let Some(database_name) = config.table.database_name() {
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

    let table_id = config
        .full_table_name()
        .context("Unable to parse table name")?;
    if !catalog
        .table_exists(&table_id)
        .await
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
    {
        let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
        // convert risingwave schema -> arrow schema -> iceberg schema
        let arrow_fields = param
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
        let iceberg_schema = arrow_schema_to_schema(&arrow_schema)
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))
            .context("failed to convert arrow schema to iceberg schema")?;

        let location = {
            let mut names = namespace.clone().inner();
            names.push(config.table.table_name().to_owned());
            match &config.common.warehouse_path {
                Some(warehouse_path) => {
                    let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                    // BigLake catalog federation uses bq:// prefix for BigQuery-managed Iceberg tables
                    let is_bq_catalog_federation = warehouse_path.starts_with("bq://");
                    let url = Url::parse(warehouse_path);
                    if url.is_err() || is_s3_tables || is_bq_catalog_federation {
                        // For rest catalog, the warehouse_path could be a warehouse name.
                        // In this case, we should specify the location when creating a table.
                        if config.common.catalog_type() == "rest"
                            || config.common.catalog_type() == "rest_rust"
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

        let partition_spec = match &config.partition_by {
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
                                .field_id(PARTITION_DATA_ID_START + i as i32)
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

        // Put format-version into table properties, because catalog like jdbc extract format-version from table properties.
        let properties = HashMap::from([(
            iceberg::spec::TableProperties::PROPERTY_FORMAT_VERSION.to_owned(),
            (config.format_version as u8).to_string(),
        )]);

        let table_creation_builder = TableCreation::builder()
            .name(config.table.table_name().to_owned())
            .schema(iceberg_schema)
            .format_version(config.table_format_version())
            .properties(properties);

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

/// Maximum size for column statistics (min/max values) in bytes.
pub const MAX_COLUMN_STAT_SIZE: usize = 10240; // 10KB

/// Truncate large column statistics from a `DataFile` before serialization.
///
/// This function directly modifies `DataFile`'s `lower_bounds` and `upper_bounds`
/// to remove entries that exceed `MAX_COLUMN_STAT_SIZE`.
pub fn truncate_datafile(mut data_file: DataFile) -> DataFile {
    // Process lower_bounds - remove entries with large values
    data_file.lower_bounds.retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large lower_bound statistic"
            );
            return false;
        }
        true
    });

    // Process upper_bounds - remove entries with large values
    data_file.upper_bounds.retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large upper_bound statistic"
            );
            return false;
        }
        true
    });

    data_file
}

/// Serializable commit metadata for Iceberg data files.
///
/// Written by `RealIcebergWriter::flush` (V3 path) and read by `IcebergCommitHandler`
/// (meta side). The old sinker uses `IcebergCommitResult` (separate, coordinator-facing format).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataFileCommitMetadata {
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub data_files: Vec<serde_json::Value>,
}

/// Serializable commit metadata for Iceberg deletion vector files.
///
/// Written by `RealDvHandler::write_dv` (V3 path) and read by `IcebergCommitHandler`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DvFileCommitMetadata {
    pub dv_files: Vec<DvFileInfo>,
    /// Serialized `DataFile` entries for DV Puffin files, ready to add to the Iceberg snapshot.
    #[serde(default)]
    pub serialized_dv_data_files: Vec<serde_json::Value>,
}

/// Info about a single DV file written for a data file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DvFileInfo {
    pub data_file_path: String,
    pub dv_file_path: String,
    pub num_deletes: u64,
}

/// Envelope for commit metadata sent from V3 executors to the coordinator.
///
/// Each executor serializes its metadata into this enum so the coordinator
/// can distinguish data-file commits from DV-file commits.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum IcebergV3CommitPayload {
    DataFiles(DataFileCommitMetadata),
    DvFiles(DvFileCommitMetadata),
}

/// Commit data files to an Iceberg table using `FastAppend` with exponential backoff retry.
///
/// Reloads the table before each attempt. If `expected_schema_id` or
/// `expected_partition_spec_id` are `Some`, they are validated on reload; a mismatch
/// is treated as a non-retriable error.
///
/// Used by both `IcebergSinkCommitter` (old sinker) and `IcebergCommitHandler` (V3 meta).
pub async fn fast_append_with_retry(
    catalog: Arc<dyn Catalog>,
    table_ident: &TableIdent,
    data_files: Vec<DataFile>,
    snapshot_id: i64,
    branch: &str,
    expected_schema_id: Option<i32>,
    expected_partition_spec_id: Option<i32>,
    retry_num: u32,
) -> anyhow::Result<Table> {
    enum CommitError {
        ReloadTable(anyhow::Error),
        Commit(anyhow::Error),
    }

    let retry_strategy = ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(60))
        .map(jitter)
        .take(retry_num as usize);

    RetryIf::spawn(
        retry_strategy,
        || async {
            let table = catalog.load_table(table_ident).await.map_err(|e| {
                CommitError::ReloadTable(anyhow::anyhow!(e).context("failed to load iceberg table"))
            })?;

            if let Some(schema_id) = expected_schema_id
                && table.metadata().current_schema_id() != schema_id
            {
                return Err(CommitError::ReloadTable(anyhow::anyhow!(
                    "Schema evolution not supported, expect schema id {}, got {}",
                    schema_id,
                    table.metadata().current_schema_id()
                )));
            }
            if let Some(spec_id) = expected_partition_spec_id
                && table.metadata().default_partition_spec_id() != spec_id
            {
                return Err(CommitError::ReloadTable(anyhow::anyhow!(
                    "Partition evolution not supported, expect partition spec id {}, got {}",
                    spec_id,
                    table.metadata().default_partition_spec_id()
                )));
            }

            let txn = Transaction::new(&table);
            let append_action = txn
                .fast_append()
                .set_snapshot_id(snapshot_id)
                .set_target_branch(branch.to_owned())
                .add_data_files(data_files.clone());

            let tx = append_action.apply(txn).map_err(|e| {
                tracing::error!(error = %e.as_report(), "Failed to apply iceberg transaction");
                CommitError::Commit(
                    anyhow::anyhow!(e).context("failed to apply iceberg transaction"),
                )
            })?;

            tx.commit(catalog.as_ref()).await.map_err(|e| {
                tracing::error!(error = %e.as_report(), "Failed to commit iceberg table");
                CommitError::Commit(
                    anyhow::anyhow!(e).context("failed to commit iceberg transaction"),
                )
            })
        },
        |e: &CommitError| match e {
            CommitError::Commit(_) => {
                tracing::warn!("Commit failed, will retry");
                true
            }
            CommitError::ReloadTable(_) => {
                tracing::error!("reload_table failed, will not retry");
                false
            }
        },
    )
    .await
    .map_err(|e| match e {
        CommitError::ReloadTable(e) | CommitError::Commit(e) => e,
    })
}

/// Returns `true` if `snapshot_id` is already present in the Iceberg table.
///
/// Used for idempotency: if a snapshot was already committed (e.g. after recovery),
/// skip the commit.
pub async fn is_snapshot_committed(
    config: &IcebergConfig,
    snapshot_id: i64,
) -> anyhow::Result<bool> {
    let table = config
        .load_table()
        .await
        .context("failed to load iceberg table for idempotency check")?;
    Ok(table.metadata().snapshot_by_id(snapshot_id).is_some())
}

/// Build `WriterProperties` for Parquet files using settings from `IcebergConfig`.
///
/// Used by both the old sinker's `build_append_only`/`build_upsert` and the V3
/// `RealIcebergWriter::build`. Sets compression, max row group size, and `created_by` string.
pub fn build_parquet_writer_properties(config: &IcebergConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(config.get_parquet_compression())
        .set_max_row_group_size(config.write_parquet_max_row_group_rows())
        .set_created_by(PARQUET_CREATED_BY.to_owned())
        .build()
}

/// Build a `DataFileWriterBuilder` (Parquet → `RollingFile` → `DataFile`) from an `IcebergConfig`.
///
/// Returns the builder ready to use directly (V3 path) or to wrap in
/// `MonitoredGeneralWriterBuilder` + `TaskWriterBuilderWrapper` (old sinker path).
///
/// `actor_id_str` and `unique_suffix` are used for file name generation to avoid collisions.
pub fn build_data_file_writer_builder(
    config: &IcebergConfig,
    table: &Table,
    actor_id_str: &str,
    unique_suffix: &str,
) -> anyhow::Result<
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>,
> {
    let schema = table.metadata().current_schema();
    let parquet_writer_properties = build_parquet_writer_properties(config);
    let parquet_writer_builder =
        ParquetWriterBuilder::new(parquet_writer_properties, schema.clone());
    let target_file_size = (config.target_file_size_mb() * 1024 * 1024) as usize;
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| anyhow::anyhow!(e).context("failed to create location generator"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        actor_id_str.to_owned(),
        Some(unique_suffix.to_owned()),
        iceberg::spec::DataFileFormat::Parquet,
    );
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    Ok(DataFileWriterBuilder::new(rolling_builder))
}

impl crate::with_options::WithOptions for IcebergWriteMode {}

impl crate::with_options::WithOptions for FormatVersion {}

impl crate::with_options::WithOptions for CompactionType {}
