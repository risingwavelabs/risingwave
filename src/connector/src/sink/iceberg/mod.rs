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

#[cfg(test)]
mod test;

mod commit;
mod config;
mod create_table;
mod prometheus;
mod writer;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroU64;

use anyhow::{Context, anyhow};
pub use commit::*;
pub use config::*;
pub use create_table::*;
use iceberg::table::Table;
use risingwave_common::bail;
use tokio::sync::mpsc::UnboundedSender;
pub use writer::*;

use super::{
    GLOBAL_SINK_METRICS, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink,
    SinkError, SinkWriterParam,
};
use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};

pub const ICEBERG_SINK: &str = "iceberg";

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
        create_and_validate_table_impl(&self.config, &self.param).await
    }

    pub async fn create_table_if_not_exists(&self) -> Result<()> {
        create_table_if_not_exists_impl(&self.config, &self.param).await
    }

    pub fn new(config: IcebergConfig, param: SinkParam) -> Result<Self> {
        if let Some(order_key) = &config.order_key {
            validate_order_key_columns(
                order_key,
                param.columns.iter().map(|column| column.name.as_str()),
            )
            .context("invalid order_key")
            .map_err(SinkError::Config)?;
        }

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

        // Enforce merge-on-read for append-only tables
        IcebergConfig::validate_append_only_write_mode(
            &self.config.r#type,
            self.config.write_mode,
        )?;

        // Validate compaction type configuration
        let compaction_type = self.config.compaction_type();

        // Check COW mode constraints
        // COW mode only supports 'full' compaction type
        if self.config.write_mode == IcebergWriteMode::CopyOnWrite
            && compaction_type != CompactionType::Full
        {
            bail!(
                "'copy-on-write' mode only supports 'full' compaction type, got: '{}'",
                compaction_type
            );
        }

        match compaction_type {
            CompactionType::SmallFiles => {
                // 1. check license
                risingwave_common::license::Feature::IcebergCompaction
                    .check_available()
                    .map_err(|e| anyhow::anyhow!(e))?;

                // 2. check write mode
                if self.config.write_mode != IcebergWriteMode::MergeOnRead {
                    bail!(
                        "'small-files' compaction type only supports 'merge-on-read' write mode, got: '{}'",
                        self.config.write_mode
                    );
                }

                // 3. check conflicting parameters
                if self.config.delete_files_count_threshold.is_some() {
                    bail!(
                        "`compaction.delete-files-count-threshold` is not supported for 'small-files' compaction type"
                    );
                }
            }
            CompactionType::FilesWithDelete => {
                // 1. check license
                risingwave_common::license::Feature::IcebergCompaction
                    .check_available()
                    .map_err(|e| anyhow::anyhow!(e))?;

                // 2. check write mode
                if self.config.write_mode != IcebergWriteMode::MergeOnRead {
                    bail!(
                        "'files-with-delete' compaction type only supports 'merge-on-read' write mode, got: '{}'",
                        self.config.write_mode
                    );
                }

                // 3. check conflicting parameters
                if self.config.small_files_threshold_mb.is_some() {
                    bail!(
                        "`compaction.small-files-threshold-mb` must not be set for 'files-with-delete' compaction type"
                    );
                }
            }
            CompactionType::Full => {
                // Full compaction has no special requirements
            }
        }

        let _ = self.create_and_validate_table().await?;
        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        let iceberg_config = IcebergConfig::from_btreemap(config.clone())?;

        // Validate compaction interval
        if let Some(compaction_interval) = iceberg_config.compaction_interval_sec {
            if iceberg_config.enable_compaction && compaction_interval == 0 {
                bail!(
                    "`compaction-interval-sec` must be greater than 0 when `enable-compaction` is true"
                );
            }

            tracing::info!(
                "Alter config compaction_interval set to {} seconds",
                compaction_interval
            );
        }

        // Validate max snapshots
        if let Some(max_snapshots) = iceberg_config.max_snapshots_num_before_compaction
            && max_snapshots < 1
        {
            bail!(
                "`compaction.max-snapshots-num` must be greater than 0, got: {}",
                max_snapshots
            );
        }

        // Validate target file size
        if let Some(target_file_size_mb) = iceberg_config.target_file_size_mb
            && target_file_size_mb == 0
        {
            bail!("`compaction.target_file_size_mb` must be greater than 0");
        }

        // Validate parquet max row group rows
        if let Some(max_row_group_rows) = iceberg_config.write_parquet_max_row_group_rows
            && max_row_group_rows == 0
        {
            bail!("`compaction.write_parquet_max_row_group_rows` must be greater than 0");
        }

        // Validate parquet compression codec
        if let Some(ref compression) = iceberg_config.write_parquet_compression {
            let valid_codecs = [
                "uncompressed",
                "snappy",
                "gzip",
                "lzo",
                "brotli",
                "lz4",
                "zstd",
            ];
            if !valid_codecs.contains(&compression.to_lowercase().as_str()) {
                bail!(
                    "`compaction.write_parquet_compression` must be one of {:?}, got: {}",
                    valid_codecs,
                    compression
                );
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = IcebergSinkWriter::new(
            self.config.clone(),
            self.param.clone(),
            writer_param.clone(),
            self.unique_column_ids.clone(),
        );

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );
        let log_sinker = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(log_sinker)
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let catalog = self.config.create_catalog().await?;
        let table = self.create_and_validate_table().await?;
        let coordinator = IcebergSinkCommitter {
            catalog,
            table,
            last_commit_epoch: 0,
            sink_id: self.param.sink_id,
            config: self.config.clone(),
            param: self.param.clone(),
            commit_retry_num: self.config.commit_retry_num,
            iceberg_compact_stat_sender,
        };
        if self.config.is_exactly_once.unwrap_or_default() {
            Ok(SinkCommitCoordinator::TwoPhase(Box::new(coordinator)))
        } else {
            Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
        }
    }
}
