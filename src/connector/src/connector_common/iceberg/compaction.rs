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

use ::iceberg::spec::{
    FormatVersion, MAIN_BRANCH, Operation, Snapshot, SnapshotRef, TableMetadata,
};
use risingwave_common::catalog::RISINGWAVE_ICEBERG_COMMIT_EPOCH;

use crate::sink::catalog::SinkId;

#[derive(Debug, Clone)]
pub struct IcebergCommittedSnapshot {
    pub branch: String,
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    /// Inclusive file sequence number boundary for this snapshot. Iceberg V1
    /// does not support file sequence numbers.
    pub max_file_sequence_number: Option<i64>,
}

impl IcebergCommittedSnapshot {
    /// Describes the head snapshot of `branch`, or returns `None` if the branch has no snapshot.
    pub fn from_branch_head(metadata: &TableMetadata, branch: &str) -> Option<Self> {
        metadata.snapshot_for_ref(branch).map(|snapshot| Self {
            branch: branch.to_owned(),
            snapshot_id: snapshot.snapshot_id(),
            timestamp_ms: snapshot.timestamp_ms(),
            max_file_sequence_number: (metadata.format_version() >= FormatVersion::V2)
                .then_some(snapshot.sequence_number()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSinkCompactionUpdate {
    // runtime event information
    pub sink_id: SinkId,
    pub force_compaction: bool,
    pub observed_snapshot: IcebergCommittedSnapshot,
}

fn commit_epoch_marker(snapshot: &Snapshot) -> Option<u64> {
    snapshot
        .summary()
        .additional_properties
        .get(RISINGWAVE_ICEBERG_COMMIT_EPOCH)?
        .parse()
        .ok()
}

fn parent_snapshot<'a>(
    metadata: &'a TableMetadata,
    snapshot: &Snapshot,
) -> Option<&'a SnapshotRef> {
    metadata.snapshot_by_id(snapshot.parent_snapshot_id()?)
}

/// Find the latest known RisingWave commit boundary. Iceberg compaction produces `replace`
/// snapshots without changing table contents, so the marker is inherited across a chain of those
/// snapshots. We deliberately stop at any other unmarked operation: an external or legacy append
/// cannot be assigned a safe log-store boundary.
pub fn risingwave_iceberg_commit_epoch(metadata: &TableMetadata, snapshot_id: i64) -> Option<u64> {
    let mut snapshot = metadata.snapshot_by_id(snapshot_id)?;
    loop {
        if let Some(epoch) = snapshot
            .summary()
            .additional_properties
            .get(RISINGWAVE_ICEBERG_COMMIT_EPOCH)
        {
            return epoch.parse().ok();
        }
        if snapshot.summary().operation != Operation::Replace {
            return None;
        }
        snapshot = parent_snapshot(metadata, snapshot)?;
    }
}

/// Counts non-`replace` snapshots on the lineage of `branch` since its latest `replace` snapshot.
pub fn count_snapshots_since_rewrite(metadata: &TableMetadata, branch: &str) -> usize {
    let mut next = metadata.snapshot_for_ref(branch);
    let mut count = 0;
    while let Some(snapshot) = next {
        if snapshot.summary().operation == Operation::Replace {
            break;
        }
        count += 1;
        next = parent_snapshot(metadata, snapshot);
    }
    count
}

/// Rebuilds the number of sink commits on `commit_branch` that automatic compaction has not
/// consumed yet. The compaction scheduler keeps this count only in memory, so meta uses this
/// estimate to restore schedules after a restart.
///
/// For copy-on-write sinks, `commit_branch` is the ingestion branch, and compaction publishes
/// the state of a planned ingestion snapshot to `main`. The publish inherits the
/// `risingwave.commit.epoch` marker of that snapshot, so the marker on `main` is the epoch of
/// the latest sink commit visible on `main`. Sink commits are appended in epoch order, so the
/// pending commits are exactly the non-`replace` ingestion snapshots with a larger epoch. When
/// that boundary cannot be established, the result is at least one so that one compaction
/// publishes any leftover data.
///
/// For merge-on-read sinks, committed data is already visible on `main`, and the count only
/// affects compaction timing. It counts the snapshots since the latest `replace` snapshot.
pub fn recover_pending_commit_count(metadata: &TableMetadata, commit_branch: &str) -> usize {
    if commit_branch == MAIN_BRANCH {
        return count_snapshots_since_rewrite(metadata, commit_branch);
    }

    let Some(ingestion_head) = metadata.snapshot_for_ref(commit_branch) else {
        return 0;
    };
    let published_epoch = match metadata.snapshot_for_ref(MAIN_BRANCH) {
        Some(main_head) => {
            match risingwave_iceberg_commit_epoch(metadata, main_head.snapshot_id()) {
                Some(epoch) => epoch,
                None => return 1,
            }
        }
        // Nothing has been published yet.
        None => 0,
    };

    let mut count = 0;
    let mut next = Some(ingestion_head);
    while let Some(snapshot) = next {
        // Compaction rewrites and manifest rewrites do not add sink commits.
        if snapshot.summary().operation != Operation::Replace {
            match commit_epoch_marker(snapshot) {
                Some(epoch) if epoch <= published_epoch => return count,
                Some(_) => count += 1,
                // An unmarked commit has an unknown position relative to `main`.
                None => break,
            }
        }
        next = parent_snapshot(metadata, snapshot);
    }
    // The published boundary was not reached, for example because older snapshots expired.
    count.max(1)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ::iceberg::spec::{
        NestedField, PrimitiveType, Schema, SnapshotReference, SnapshotRetention, SortOrder,
        Summary, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::*;

    const INGESTION: &str = "ingestion";

    /// `(snapshot_id, parent_snapshot_id, operation, commit_epoch)`
    type TestSnapshot = (i64, Option<i64>, Operation, Option<u64>);

    fn snapshot(
        &(snapshot_id, parent_snapshot_id, ref operation, epoch): &TestSnapshot,
    ) -> Snapshot {
        let additional_properties = epoch
            .map(|epoch| {
                HashMap::from([(
                    RISINGWAVE_ICEBERG_COMMIT_EPOCH.to_owned(),
                    epoch.to_string(),
                )])
            })
            .unwrap_or_default();
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(snapshot_id)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: operation.clone(),
                additional_properties,
            })
            .with_schema_id(0)
            .build()
    }

    fn metadata(snapshots: &[TestSnapshot], refs: &[(&str, i64)]) -> TableMetadata {
        let mut builder = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "id", Type::Primitive(PrimitiveType::Long), false).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/table".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for test_snapshot in snapshots {
            builder = builder.add_snapshot(snapshot(test_snapshot)).unwrap();
        }
        for &(branch, snapshot_id) in refs {
            builder = builder
                .set_ref(
                    branch,
                    SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                )
                .unwrap();
        }
        builder.build().unwrap().metadata
    }

    #[test]
    fn test_risingwave_commit_epoch_inherits_only_across_replace_snapshots() {
        let metadata = metadata(
            &[
                (1, None, Operation::Append, Some(42)),
                (2, Some(1), Operation::Replace, None),
                (3, Some(2), Operation::Append, None),
            ],
            &[(MAIN_BRANCH, 3)],
        );
        assert_eq!(risingwave_iceberg_commit_epoch(&metadata, 2), Some(42));
        assert_eq!(risingwave_iceberg_commit_epoch(&metadata, 3), None);
    }

    #[test]
    fn test_recover_copy_on_write_pending_commit_count() {
        use Operation::{Append, Overwrite, Replace};

        for (name, snapshots, refs, expected) in [
            (
                "published state",
                vec![
                    (1, None, Append, Some(10)),
                    (2, Some(1), Replace, Some(10)),
                    (3, None, Overwrite, Some(10)),
                ],
                vec![(INGESTION, 2), (MAIN_BRANCH, 3)],
                0,
            ),
            (
                "commits after the published state",
                vec![
                    (1, None, Append, Some(10)),
                    (2, Some(1), Replace, Some(10)),
                    (3, None, Overwrite, Some(10)),
                    (4, Some(2), Append, Some(20)),
                    (5, Some(4), Append, Some(30)),
                ],
                vec![(INGESTION, 5), (MAIN_BRANCH, 3)],
                2,
            ),
            (
                // The rewrite of snapshot 1 is committed on top of a later commit.
                "commit between planning and rewrite",
                vec![
                    (1, None, Append, Some(10)),
                    (2, Some(1), Append, Some(20)),
                    (3, Some(2), Replace, Some(10)),
                    (4, None, Overwrite, Some(10)),
                ],
                vec![(INGESTION, 3), (MAIN_BRANCH, 4)],
                1,
            ),
            (
                "rewrite committed without publish",
                vec![
                    (1, None, Append, Some(10)),
                    (2, None, Overwrite, Some(10)),
                    (3, Some(1), Append, Some(20)),
                    (4, Some(3), Replace, Some(20)),
                ],
                vec![(INGESTION, 4), (MAIN_BRANCH, 2)],
                1,
            ),
            (
                "manifest rewrite after a commit",
                vec![
                    (1, None, Append, Some(10)),
                    (2, None, Overwrite, Some(10)),
                    (3, Some(1), Append, Some(20)),
                    (4, Some(3), Replace, None),
                ],
                vec![(INGESTION, 4), (MAIN_BRANCH, 2)],
                1,
            ),
            (
                "nothing published",
                vec![(1, None, Append, Some(10)), (2, Some(1), Append, Some(20))],
                vec![(INGESTION, 2)],
                2,
            ),
            (
                "unmarked main",
                vec![(1, None, Append, Some(10)), (2, None, Overwrite, None)],
                vec![(INGESTION, 1), (MAIN_BRANCH, 2)],
                1,
            ),
            (
                "unmarked ingestion commit",
                vec![
                    (1, None, Append, Some(10)),
                    (2, None, Overwrite, Some(10)),
                    (3, Some(1), Append, None),
                ],
                vec![(INGESTION, 3), (MAIN_BRANCH, 2)],
                1,
            ),
            (
                "expired lineage",
                vec![
                    (2, None, Overwrite, Some(10)),
                    (3, Some(1), Replace, Some(20)),
                ],
                vec![(INGESTION, 3), (MAIN_BRANCH, 2)],
                1,
            ),
            (
                "empty ingestion branch",
                vec![(1, None, Overwrite, Some(10))],
                vec![(MAIN_BRANCH, 1)],
                0,
            ),
        ] {
            let metadata = metadata(&snapshots, &refs);
            assert_eq!(
                recover_pending_commit_count(&metadata, INGESTION),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn test_recover_merge_on_read_pending_commit_count() {
        let metadata = metadata(
            &[
                (1, None, Operation::Append, Some(10)),
                (2, Some(1), Operation::Replace, Some(10)),
                (3, Some(2), Operation::Append, Some(20)),
                (4, Some(3), Operation::Append, Some(30)),
            ],
            &[(MAIN_BRANCH, 4)],
        );
        assert_eq!(recover_pending_commit_count(&metadata, MAIN_BRANCH), 2);
    }
}
