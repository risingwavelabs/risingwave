// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use crate::error::Result;
use crate::spec::{
    DataFile, MIN_FORMAT_VERSION_ROW_LINEAGE, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriter, Operation,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::{DEFAULT_LOAD_CONCURRENCY_LIMIT, load_manifests};
use crate::{Error, ErrorKind};

const KEPT_MANIFESTS_COUNT: &str = "manifests-kept";
const CREATED_MANIFESTS_COUNT: &str = "manifests-created";
const REPLACED_MANIFESTS_COUNT: &str = "manifests-replaced";
/// Tracks entries processed during clustering. Always 0 for manual add/delete operations.
const PROCESSED_ENTRY_COUNT: &str = "entries-processed";

/// Function that maps a DataFile to a cluster key for grouping entries into manifests.
type ClusterByFunc = Box<dyn Fn(&DataFile) -> String + Send + Sync>;

/// Predicate function to select which manifests to rewrite.
type ManifestPredicate = Box<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// Transaction action for rewriting manifest files.
///
/// This action reorganizes manifest files without changing the underlying data files.
/// It can consolidate small manifests or re-cluster entries by partition values or
/// custom keys.
///
/// Manifests with delete content type are never rewritten.
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    snapshot_id: Option<i64>,
    target_branch: Option<String>,

    cluster_by_func: Option<ClusterByFunc>,
    manifest_predicate: Option<ManifestPredicate>,
    added_manifests: Vec<ManifestFile>,
    deleted_manifests: Vec<ManifestFile>,
}

impl RewriteManifestsAction {
    /// Creates a new rewrite manifests action with default settings.
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            snapshot_id: None,
            target_branch: None,

            cluster_by_func: None,
            manifest_predicate: None,
            added_manifests: Vec::new(),
            deleted_manifests: Vec::new(),
        }
    }

    /// Set a clustering function that determines how data file entries are grouped
    /// into new manifests. Files with the same cluster key will be written to the
    /// same manifest.
    pub fn cluster_by(mut self, func: ClusterByFunc) -> Self {
        self.cluster_by_func = Some(func);
        self
    }

    /// Set a predicate to filter which manifests should be rewritten.
    /// Manifests that don't match the predicate will be kept as-is.
    pub fn rewrite_if(mut self, predicate: ManifestPredicate) -> Self {
        self.manifest_predicate = Some(predicate);
        self
    }

    /// Manually add a manifest to the snapshot. The manifest must not contain
    /// any added or deleted file entries, and its `partition_spec_id` must
    /// reference a partition spec that exists in the table metadata.
    ///
    /// Manifests with unknown (None) file counts — such as V1 manifests — are
    /// rejected because the Iceberg spec treats None as "assumed non-zero",
    /// which conflicts with the requirement that added and deleted counts be
    /// zero.
    pub fn add_manifest(mut self, manifest: ManifestFile) -> Self {
        self.added_manifests.push(manifest);
        self
    }

    /// Manually remove a manifest from the snapshot. The manifest must exist
    /// in the current snapshot.
    pub fn delete_manifest(mut self, manifest: ManifestFile) -> Self {
        self.deleted_manifests.push(manifest);
        self
    }

    /// Set snapshot properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Set the target branch for this action.
    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id.
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }
}

impl Default for RewriteManifestsAction {
    fn default() -> Self {
        Self::new()
    }
}

/// Count of active (added + existing) files in a list of manifests.
///
/// Returns `None` if any manifest has unknown counts (`None`), since the
/// Iceberg spec says `None` means "assumed to be non-zero" and we cannot
/// compute a reliable total.
fn active_files_count(manifests: &[ManifestFile]) -> Option<u64> {
    let mut total: u64 = 0;
    for m in manifests {
        let added = m.added_files_count? as u64;
        let existing = m.existing_files_count? as u64;
        total += added + existing;
    }
    Some(total)
}

/// The operation implementation for rewrite manifests.
///
/// This holds the computed manifest lists after the rewrite logic has been applied,
/// so that `existing_manifest()` can return them to the `SnapshotProducer`.
struct RewriteManifestsOperation {
    /// Manifests that carry forward to the new snapshot (kept + newly written + manually added).
    result_manifests: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Rewrite manifests doesn't change data files, so no delete entries.
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Return the pre-computed manifest list.
        // Existing manifests come first (kept), then new manifests — the
        // SnapshotProducer will append any added-data-file manifests after these,
        // but for rewrite_manifests there are none.
        Ok(self.result_manifests.clone())
    }
}

#[async_trait::async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Reject rewrite_manifests for tables with row lineage (V3+).
        // Rewriting manifests creates new ManifestFiles with first_row_id unset,
        // causing ManifestListWriter to assign fresh row IDs and advance
        // next_row_id even though no new rows were added. This breaks row lineage
        // semantics. Until a strategy to preserve row IDs through manifest rewrites
        // is implemented, this operation is unsupported for V3 tables.
        if table.metadata().format_version() >= MIN_FORMAT_VERSION_ROW_LINEAGE {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "rewrite_manifests is not supported for tables with row lineage \
                     (format version >= {MIN_FORMAT_VERSION_ROW_LINEAGE}). Rewriting \
                     manifests would incorrectly advance next_row_id without adding \
                     new rows.",
                ),
            ));
        }

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);

        // Build a SnapshotProducer. Since rewrite_manifests doesn't add or remove
        // data files, all file vectors are empty. Snapshot properties are set later
        // (after computing rewrite metrics) via `set_snapshot_properties()`.
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            self.snapshot_id,
            HashMap::new(),
            vec![], // no added data files
            vec![], // no added delete files
            vec![], // no removed data files
            vec![], // no removed delete files
        );

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        let target_branch = snapshot_producer.target_branch();
        let metadata_ref = table.metadata_ref();
        let parent_snapshot = metadata_ref.snapshot_for_ref(target_branch);

        // Load current manifests from the parent snapshot
        let current_manifests = if let Some(snapshot) = parent_snapshot {
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), metadata_ref.as_ref())
                .await?;
            manifest_list
                .consume_entries()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Map paths to the actual ManifestFile in the snapshot for content-type checks
        // and existence lookups.
        let current_manifests_by_path: HashMap<&str, &ManifestFile> = current_manifests
            .iter()
            .map(|m| (m.manifest_path.as_str(), m))
            .collect();

        let deleted_paths: HashSet<&str> = self
            .deleted_manifests
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();

        // Check for duplicate paths in deleted_manifests
        if deleted_paths.len() != self.deleted_manifests.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deleted_manifests contains duplicate manifest paths",
            ));
        }

        // Check for duplicate paths in added_manifests
        let added_paths: HashSet<&str> = self
            .added_manifests
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();
        if added_paths.len() != self.added_manifests.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "added_manifests contains duplicate manifest paths",
            ));
        }

        // Validate deleted manifests exist in current snapshot and are not
        // delete-type manifests (which must never be removed by rewrite_manifests).
        for manifest in &self.deleted_manifests {
            let path = manifest.manifest_path.as_str();
            match current_manifests_by_path.get(path) {
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Deleted manifest does not exist in the current snapshot: {path}",),
                    ));
                }
                Some(current) if current.content == ManifestContentType::Deletes => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete a delete-type manifest via rewrite_manifests: {path}",
                        ),
                    ));
                }
                _ => {}
            }
        }

        // Validate added manifests don't already exist in the current snapshot
        // (unless they are also being deleted — i.e. swapped) and don't have
        // added/deleted files.
        // `None` counts (e.g. V1 manifests) are treated as non-zero per the
        // Iceberg spec, so manifests with unknown counts are rejected.
        for manifest in &self.added_manifests {
            if manifest.content == ManifestContentType::Deletes {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add a delete-type manifest via rewrite_manifests: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if metadata_ref
                .partition_spec_by_id(manifest.partition_spec_id)
                .is_none()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with unknown partition spec id {}: {}",
                        manifest.partition_spec_id, manifest.manifest_path
                    ),
                ));
            }
            if current_manifests_by_path.contains_key(manifest.manifest_path.as_str())
                && !deleted_paths.contains(manifest.manifest_path.as_str())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest that already exists in the current snapshot: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if manifest.has_added_files() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with added files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if manifest.has_deleted_files() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with deleted files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
        }

        let mut new_manifests: Vec<ManifestFile> = Vec::new();
        let mut kept_manifests: Vec<ManifestFile> = Vec::new();
        let mut rewritten_manifests: Vec<ManifestFile> = Vec::new();
        let mut entry_count: usize = 0;

        if let Some(cluster_func) = &self.cluster_by_func {
            // Writers keyed by (cluster_key, partition_spec_id).
            // BTreeMap ensures deterministic manifest ordering across runs.
            let mut writers: BTreeMap<(String, i32), ManifestWriter> = BTreeMap::new();

            // Filter out deleted manifests, then process remaining
            let remaining_manifests: Vec<ManifestFile> = current_manifests
                .into_iter()
                .filter(|m| !deleted_paths.contains(m.manifest_path.as_str()))
                .collect();

            // Separate manifests into kept (delete-type or filtered by predicate) and
            // to-be-rewritten, so we can load the latter concurrently.
            let mut manifests_to_rewrite: Vec<ManifestFile> = Vec::new();
            for manifest_file in remaining_manifests {
                if manifest_file.content == ManifestContentType::Deletes {
                    kept_manifests.push(manifest_file);
                    continue;
                }
                if let Some(ref predicate) = self.manifest_predicate
                    && !predicate(&manifest_file)
                {
                    kept_manifests.push(manifest_file);
                    continue;
                }
                rewritten_manifests.push(manifest_file.clone());
                manifests_to_rewrite.push(manifest_file);
            }

            // Load all manifests to rewrite concurrently.
            let loaded_manifests = load_manifests(
                table.file_io(),
                manifests_to_rewrite,
                DEFAULT_LOAD_CONCURRENCY_LIMIT,
            )
            .await?;

            // Route entries to writers (sequential — writers are stateful).
            for (manifest_file, manifest) in &loaded_manifests {
                let spec_id = &manifest_file.partition_spec_id;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }

                    let key = cluster_func(entry.data_file());
                    let writer_key = (key, *spec_id);

                    let writer = match writers.entry(writer_key) {
                        std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::btree_map::Entry::Vacant(e) => {
                            let w = snapshot_producer
                                .new_manifest_writer(ManifestContentType::Data, *spec_id)?;
                            e.insert(w)
                        }
                    };
                    writer.add_existing_entry(entry.as_ref().clone())?;
                    entry_count += 1;
                }
            }

            // Close all writers and collect new manifests (deterministic order)
            for (_key, writer) in writers {
                let manifest_file = writer.write_manifest_file().await?;
                new_manifests.push(manifest_file);
            }
        } else {
            // No clustering — just keep non-deleted manifests
            for manifest_file in current_manifests {
                if !deleted_paths.contains(manifest_file.manifest_path.as_str()) {
                    kept_manifests.push(manifest_file);
                }
            }
        }

        // Nothing was actually rewritten, added, or deleted — bail out instead
        // of creating a redundant snapshot identical to the parent.
        if new_manifests.is_empty()
            && self.added_manifests.is_empty()
            && rewritten_manifests.is_empty()
            && self.deleted_manifests.is_empty()
        {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Validate file counts when all manifests have known counts.
        // If any manifest has None counts (e.g. V1 format), we skip validation
        // because the Iceberg spec says None means "assumed non-zero" and we
        // cannot compute a reliable total.
        let created_count = active_files_count(&new_manifests)
            .and_then(|a| active_files_count(&self.added_manifests).map(|b| a + b));
        let replaced_count = active_files_count(&rewritten_manifests)
            .and_then(|a| active_files_count(&self.deleted_manifests).map(|b| a + b));

        if let (Some(created), Some(replaced)) = (created_count, replaced_count)
            && created != replaced
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Rewrite manifests file count mismatch: created {created} files but replaced {replaced} files",
                ),
            ));
        }

        // Inject rewrite-specific summary properties so they appear in the snapshot.
        // Internal metrics are inserted after user properties, so they take
        // precedence if a user sets a key like "manifests-created".
        let mut rewrite_properties = self.snapshot_properties.clone();
        rewrite_properties.insert(
            CREATED_MANIFESTS_COUNT.to_string(),
            (new_manifests.len() + self.added_manifests.len()).to_string(),
        );
        rewrite_properties.insert(
            KEPT_MANIFESTS_COUNT.to_string(),
            kept_manifests.len().to_string(),
        );
        rewrite_properties.insert(
            REPLACED_MANIFESTS_COUNT.to_string(),
            (rewritten_manifests.len() + self.deleted_manifests.len()).to_string(),
        );
        rewrite_properties.insert(PROCESSED_ENTRY_COUNT.to_string(), entry_count.to_string());
        snapshot_producer.set_snapshot_properties(rewrite_properties);

        // Assemble final manifest list: kept manifests first (existing), then new
        // manifests and manually added manifests. Existing manifests must come
        // before new ones to ensure correct first_row_id assignment by
        // ManifestListWriter.
        let mut result_manifests: Vec<ManifestFile> = Vec::new();
        result_manifests.extend(kept_manifests);
        result_manifests.extend(new_manifests);
        result_manifests.extend(self.added_manifests.clone());

        let operation = RewriteManifestsOperation { result_manifests };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{ManifestContentType, ManifestFile};
    use crate::table::Table;
    use crate::transaction::TransactionAction;
    use crate::transaction::rewrite_manifests::RewriteManifestsAction;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table};

    fn test_manifest(
        path: &str,
        added: Option<u32>,
        existing: Option<u32>,
        deleted: Option<u32>,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: added,
            existing_files_count: existing,
            deleted_files_count: deleted,
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// Helper to commit an action and assert it returns an error containing the expected message.
    async fn assert_commit_err(action: RewriteManifestsAction, table: &Table, expected_msg: &str) {
        let action = Arc::new(action);
        let result = action.commit(table).await;
        match result {
            Ok(_) => panic!("expected error containing '{expected_msg}', but commit succeeded"),
            Err(e) => assert!(
                e.to_string().contains(expected_msg),
                "expected error containing '{expected_msg}', got: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table() {
        let table = make_v3_minimal_table();
        // Even a no-op rewrite should be rejected for V3 tables.
        let action = RewriteManifestsAction::new();
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table_with_cluster_by() {
        let table = make_v3_minimal_table();
        let action = RewriteManifestsAction::new().cluster_by(Box::new(|_| "default".to_string()));
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table_with_add_manifest() {
        let table = make_v3_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-ok.avro", Some(0), Some(5), Some(0));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_delete_type_manifest() {
        let table = make_v2_minimal_table();
        let mut manifest =
            test_manifest("s3://bucket/manifest-del.avro", Some(0), Some(5), Some(0));
        manifest.content = ManifestContentType::Deletes;
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(
            action,
            &table,
            "Cannot add a delete-type manifest via rewrite_manifests",
        )
        .await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_unknown_partition_spec_id() {
        let table = make_v2_minimal_table();
        // The minimal table only has partition spec id 0.
        let mut manifest = test_manifest(
            "s3://bucket/manifest-bad-spec.avro",
            Some(0),
            Some(5),
            Some(0),
        );
        manifest.partition_spec_id = 9999;
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "unknown partition spec id 9999").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_added_files_count_some_positive() {
        let table = make_v2_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-1.avro", Some(5), Some(0), Some(0));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with added files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_deleted_files_count_some_positive() {
        let table = make_v2_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-1.avro", Some(0), Some(0), Some(3));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with deleted files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_none_added_files_count() {
        let table = make_v2_minimal_table();
        // None means "assumed non-zero" per Iceberg spec — should be rejected.
        let manifest = test_manifest("s3://bucket/manifest-v1.avro", None, Some(10), None);
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with added files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_none_deleted_files_count() {
        let table = make_v2_minimal_table();
        // added_files_count is known-zero, but deleted_files_count is None → rejected.
        let manifest = test_manifest("s3://bucket/manifest-v1.avro", Some(0), Some(10), None);
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with deleted files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_accepts_zero_counts() {
        let table = make_v2_minimal_table();
        // Both added and deleted are known-zero — should pass the validation.
        // (It will fail later during snapshot commit because there is no matching
        // deleted manifest, but the add_manifest validation itself should succeed.)
        let manifest = test_manifest("s3://bucket/manifest-ok.avro", Some(0), Some(5), Some(0));
        let action = Arc::new(RewriteManifestsAction::new().add_manifest(manifest));
        let result = action.commit(&table).await;
        // The error, if any, should NOT be about added/deleted files.
        if let Err(e) = &result {
            assert!(
                !e.to_string()
                    .contains("Cannot add manifest with added files")
                    && !e
                        .to_string()
                        .contains("Cannot add manifest with deleted files"),
                "unexpected rejection for zero-count manifest: {e}"
            );
        }
    }
}
