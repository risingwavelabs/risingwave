// Copyright 2026 RisingWave Labs
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

//! Manager for the Iceberg pk-index sink path. Owns per-sink commit coordinators that
//! drive iceberg `commit_epoch` ahead of hummock `commit_epoch` and persist
//! exactly-once state via `pending_sink_state`.
//!
//! This is intentionally separate from [`crate::manager::sink_coordination`]
//! (which serves V1/V2 sinks via gRPC). Future responsibilities such as
//! per-sink compaction will live alongside the per-sink commit coordinator here.

mod committed_epoch;
mod coordinator;
mod manager;

use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use iceberg::spec::SerializedDataFile;
pub use manager::IcebergPkIndexSinkManager;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::{ENABLE_PK_INDEX, IcebergConfig};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::catalog::PbSink;
use risingwave_pb::stream_service::barrier_complete_response::IcebergPkIndexSinkMetadata as PbIcebergPkIndexSinkMetadata;

#[derive(educe::Educe)]
#[educe(Debug)]
pub(crate) struct CompactionOverwrite {
    pub sink_id: SinkId,
    pub epoch: u64,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    #[educe(Debug(ignore))]
    pub output_files: Vec<SerializedDataFile>,
    pub input_file_paths: Vec<String>,
    pub read_snapshot_id: i64,
}

/// Metadata collected for one sink/epoch before the Hummock checkpoint is committed.
/// Ordinary reports and the optional compaction overwrite share one transport shape so the
/// barrier path does not need separate metadata variants.
#[derive(Debug)]
pub(crate) struct IcebergPkIndexPreCommitMetadata {
    pub sink_id: SinkId,
    pub prev_epoch: u64,
    pub reports: Vec<PbIcebergPkIndexSinkMetadata>,
    pub compaction: Option<CompactionOverwrite>,
}

impl From<PbIcebergPkIndexSinkMetadata> for IcebergPkIndexPreCommitMetadata {
    fn from(report: PbIcebergPkIndexSinkMetadata) -> Self {
        Self {
            sink_id: report.sink_id,
            prev_epoch: report.prev_epoch,
            reports: vec![report],
            compaction: None,
        }
    }
}

impl From<CompactionOverwrite> for IcebergPkIndexPreCommitMetadata {
    fn from(overwrite: CompactionOverwrite) -> Self {
        Self {
            sink_id: overwrite.sink_id,
            prev_epoch: overwrite.epoch,
            reports: vec![],
            compaction: Some(overwrite),
        }
    }
}

pub(crate) fn group_pre_commit_metadata(
    metadata: Vec<IcebergPkIndexPreCommitMetadata>,
) -> anyhow::Result<Vec<IcebergPkIndexPreCommitMetadata>> {
    let mut grouped = HashMap::<SinkId, IcebergPkIndexPreCommitMetadata>::new();
    for metadata in metadata {
        let IcebergPkIndexPreCommitMetadata {
            sink_id,
            prev_epoch,
            reports,
            compaction,
        } = metadata;
        let input = grouped
            .entry(sink_id)
            .or_insert_with(|| IcebergPkIndexPreCommitMetadata {
                sink_id,
                prev_epoch,
                reports: Vec::new(),
                compaction: None,
            });
        if input.prev_epoch != prev_epoch {
            anyhow::bail!(
                "iceberg v3 sink {} pre-commit metadata disagrees on prev_epoch: {} vs {}",
                sink_id,
                input.prev_epoch,
                prev_epoch
            );
        }
        input.reports.extend(reports);
        if let Some(overwrite) = compaction
            && input.compaction.replace(overwrite).is_some()
        {
            anyhow::bail!(
                "iceberg v3 sink {} has multiple compaction overwrites in one barrier",
                sink_id
            );
        }
    }
    Ok(grouped.into_values().collect())
}

/// Returns true if the given sink properties identify a Iceberg pk-index sink
/// (i.e. an iceberg sink with `enable_pk_index = 'true'`).
pub fn is_iceberg_pk_index_sink(properties: &BTreeMap<String, String>) -> bool {
    let connector_match = properties
        .get(UPSTREAM_SOURCE_KEY)
        .map(|v| v.eq_ignore_ascii_case("iceberg"))
        .unwrap_or(false);
    let pk_index_enabled = properties
        .get(ENABLE_PK_INDEX)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    connector_match && pk_index_enabled
}

/// Build an [`IcebergConfig`] from a [`PbSink`], filling secret refs along the
/// way. Used at CREATE SINK time and during recovery to (re-)register the
/// commit coordinator.
pub fn build_iceberg_config(pb_sink: &PbSink) -> anyhow::Result<IcebergConfig> {
    let properties: BTreeMap<String, String> = pb_sink.properties.clone().into_iter().collect();
    let secret_refs: BTreeMap<_, _> = pb_sink.secret_refs.clone().into_iter().collect();
    let with_secrets = LocalSecretManager::global()
        .fill_secrets(properties, secret_refs)
        .map_err(|e| anyhow!(e).context("fill secrets for iceberg"))?;
    IcebergConfig::from_btreemap(with_secrets)
        .map_err(|e| anyhow!(e).context("parse iceberg config"))
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;

    use super::*;

    fn report(
        sink_id: u32,
        prev_epoch: u64,
        role: PbIcebergPkIndexSinkRole,
    ) -> PbIcebergPkIndexSinkMetadata {
        PbIcebergPkIndexSinkMetadata {
            sink_id: SinkId::new(sink_id),
            prev_epoch,
            role: role as i32,
            ..Default::default()
        }
    }

    fn overwrite(sink_id: u32, epoch: u64) -> CompactionOverwrite {
        CompactionOverwrite {
            sink_id: SinkId::new(sink_id),
            epoch,
            schema_id: 3,
            partition_spec_id: 4,
            output_files: vec![],
            input_file_paths: vec![],
            read_snapshot_id: 1,
        }
    }

    #[test]
    fn group_pre_commit_metadata_combines_reports_and_compaction() {
        let inputs = group_pre_commit_metadata(vec![
            report(7, 60, PbIcebergPkIndexSinkRole::Writer).into(),
            overwrite(7, 60).into(),
            report(7, 60, PbIcebergPkIndexSinkRole::PositionDeleteMerger).into(),
        ])
        .unwrap();

        assert_eq!(inputs.len(), 1);
        let input = &inputs[0];
        assert_eq!(input.sink_id, SinkId::new(7));
        assert_eq!(input.prev_epoch, 60);
        assert_eq!(input.reports.len(), 2);
        let overwrite = input.compaction.as_ref().unwrap();
        assert_eq!(overwrite.schema_id, 3);
        assert_eq!(overwrite.partition_spec_id, 4);
    }

    #[test]
    fn group_pre_commit_metadata_preserves_ordinary_only_sink() {
        let inputs =
            group_pre_commit_metadata(vec![report(8, 61, PbIcebergPkIndexSinkRole::Writer).into()])
                .unwrap();

        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].sink_id, SinkId::new(8));
        assert_eq!(inputs[0].prev_epoch, 61);
        assert_eq!(inputs[0].reports.len(), 1);
        assert!(inputs[0].compaction.is_none());
    }

    #[test]
    fn group_pre_commit_metadata_rejects_duplicate_compaction() {
        let error =
            group_pre_commit_metadata(vec![overwrite(7, 60).into(), overwrite(7, 60).into()])
                .unwrap_err();
        assert!(error.to_string().contains("multiple compaction overwrites"));
    }

    #[test]
    fn group_pre_commit_metadata_rejects_epoch_mismatch() {
        let error = group_pre_commit_metadata(vec![
            report(7, 59, PbIcebergPkIndexSinkRole::Writer).into(),
            overwrite(7, 60).into(),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("disagrees on prev_epoch"));
    }
}
