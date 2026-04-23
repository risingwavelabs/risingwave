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

//! Transaction action for removing snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;

use crate::error::Result;
use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadataRef};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::{DEFAULT_LOAD_CONCURRENCY_LIMIT, ancestors_of, load_manifest_lists};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Default value for max snapshot age in milliseconds.
pub const MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 5 * 24 * 60 * 60 * 1000; // 5 days
/// Default value for min snapshots to keep.
pub const MIN_SNAPSHOTS_TO_KEEP_DEFAULT: i32 = 1;
/// Default value for max reference age in milliseconds.
pub const MAX_REF_AGE_MS_DEFAULT: i64 = i64::MAX;

/// RemoveSnapshotAction is a transaction action for removing snapshot.
pub struct RemoveSnapshotAction {
    clear_expire_files: bool,
    ids_to_remove: HashSet<i64>,
    default_expired_older_than: i64,
    default_min_num_snapshots: i32,
    default_max_ref_age_ms: i64,
    clear_expired_meta_data: bool,

    now: i64,
}

impl Default for RemoveSnapshotAction {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoveSnapshotAction {
    /// Creates a new action.
    pub fn new() -> Self {
        let now = chrono::Utc::now().timestamp_millis();

        Self {
            clear_expire_files: false,
            ids_to_remove: HashSet::new(),
            default_expired_older_than: now - MAX_SNAPSHOT_AGE_MS_DEFAULT,
            default_min_num_snapshots: MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
            default_max_ref_age_ms: MAX_REF_AGE_MS_DEFAULT,
            now,
            clear_expired_meta_data: false,
        }
    }

    /// Finished building the action and apply it to the transaction.
    pub fn clear_expire_files(mut self, clear_expire_files: bool) -> Self {
        self.clear_expire_files = clear_expire_files;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn expire_snapshot_id(mut self, expire_snapshot_id: i64) -> Self {
        self.ids_to_remove.insert(expire_snapshot_id);
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.default_expired_older_than = timestamp_ms;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn retain_last(mut self, min_num_snapshots: i32) -> Self {
        self.default_min_num_snapshots = min_num_snapshots;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn clear_expired_meta_data(mut self, clear_expired_meta_data: bool) -> Self {
        self.clear_expired_meta_data = clear_expired_meta_data;
        self
    }

    fn compute_retained_refs(
        &self,
        snapshot_refs: &HashMap<String, SnapshotReference>,
        table_meta: &TableMetadataRef,
    ) -> HashMap<String, SnapshotReference> {
        let mut retained_refs = HashMap::new();

        for (ref_name, snapshot_ref) in snapshot_refs {
            if ref_name == MAIN_BRANCH {
                retained_refs.insert(ref_name.clone(), snapshot_ref.clone());
                continue;
            }

            let snapshot = table_meta.snapshot_by_id(snapshot_ref.snapshot_id);
            let max_ref_age_ms = match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: _,
                    max_snapshot_age_ms: _,
                    max_ref_age_ms,
                } => max_ref_age_ms,
                SnapshotRetention::Tag { max_ref_age_ms } => max_ref_age_ms,
            }
            .unwrap_or(self.default_max_ref_age_ms);

            if let Some(snapshot) = snapshot {
                let ref_age_ms = self.now - snapshot.timestamp_ms();
                if ref_age_ms <= max_ref_age_ms {
                    retained_refs.insert(ref_name.clone(), snapshot_ref.clone());
                }
            } else {
                tracing::warn!(
                    "Snapshot {} for reference {} not found, removing the reference",
                    snapshot_ref.snapshot_id,
                    ref_name
                );
            }
        }

        retained_refs
    }

    fn compute_all_branch_snapshots_to_retain(
        &self,
        refs: impl Iterator<Item = SnapshotReference>,
        table_meta: &TableMetadataRef,
    ) -> HashSet<i64> {
        let mut branch_snapshots_to_retain = HashSet::new();
        for snapshot_ref in refs {
            if snapshot_ref.is_branch() {
                let max_snapshot_age_ms = match snapshot_ref.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: _,
                        max_snapshot_age_ms,
                        max_ref_age_ms: _,
                    } => max_snapshot_age_ms,
                    SnapshotRetention::Tag { max_ref_age_ms: _ } => None,
                };

                let expire_snapshot_older_than =
                    if let Some(max_snapshot_age_ms) = max_snapshot_age_ms {
                        self.now - max_snapshot_age_ms
                    } else {
                        self.default_expired_older_than
                    };

                let min_snapshots_to_keep = match snapshot_ref.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms: _,
                        max_ref_age_ms: _,
                    } => min_snapshots_to_keep,
                    SnapshotRetention::Tag { max_ref_age_ms: _ } => None,
                }
                .unwrap_or(self.default_min_num_snapshots);

                branch_snapshots_to_retain.extend(self.compute_branch_snapshots_to_retain(
                    snapshot_ref.snapshot_id,
                    expire_snapshot_older_than,
                    min_snapshots_to_keep as usize,
                    table_meta,
                ));
            }
        }

        branch_snapshots_to_retain
    }

    fn compute_branch_snapshots_to_retain(
        &self,
        snapshot_id: i64,
        expire_snapshots_older_than: i64,
        min_snapshots_to_keep: usize,
        table_meta: &TableMetadataRef,
    ) -> HashSet<i64> {
        let mut ids_to_retain = HashSet::new();
        if let Some(snapshot) = table_meta.snapshot_by_id(snapshot_id) {
            let ancestors = ancestors_of(table_meta, snapshot.snapshot_id());
            for ancestor in ancestors {
                if ids_to_retain.len() < min_snapshots_to_keep
                    || ancestor.timestamp_ms() >= expire_snapshots_older_than
                {
                    ids_to_retain.insert(ancestor.snapshot_id());
                } else {
                    return ids_to_retain;
                }
            }
        }

        ids_to_retain
    }

    fn unreferenced_snapshots_to_retain(
        &self,
        refs: impl Iterator<Item = SnapshotReference>,
        table_meta: &TableMetadataRef,
    ) -> HashSet<i64> {
        let mut ids_to_retain = HashSet::new();
        let mut referenced_snapshots = HashSet::new();

        for snapshot_ref in refs {
            if snapshot_ref.is_branch() {
                if let Some(snapshot) = table_meta.snapshot_by_id(snapshot_ref.snapshot_id) {
                    let ancestors = ancestors_of(table_meta, snapshot.snapshot_id());
                    for ancestor in ancestors {
                        referenced_snapshots.insert(ancestor.snapshot_id());
                    }
                }
            } else {
                referenced_snapshots.insert(snapshot_ref.snapshot_id);
            }
        }

        for snapshot in table_meta.snapshots() {
            if !referenced_snapshots.contains(&snapshot.snapshot_id())
                && snapshot.timestamp_ms() >= self.default_expired_older_than
            {
                ids_to_retain.insert(snapshot.snapshot_id());
            }
        }

        ids_to_retain
    }
}

#[async_trait]
impl TransactionAction for RemoveSnapshotAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if table.metadata().refs.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let table_meta = table.metadata_ref();

        let mut ids_to_retain = HashSet::new();
        let retained_refs = self.compute_retained_refs(&table_meta.refs, &table_meta);
        let mut retained_id_to_refs = HashMap::new();
        for (ref_name, snapshot_ref) in &retained_refs {
            let snapshot_id = snapshot_ref.snapshot_id;
            retained_id_to_refs
                .entry(snapshot_id)
                .or_insert_with(Vec::new)
                .push(ref_name.clone());

            ids_to_retain.insert(snapshot_id);
        }

        for id_to_remove in &self.ids_to_remove {
            if let Some(refs_for_id) = retained_id_to_refs.get(id_to_remove) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot remove snapshot {id_to_remove:?} with retained references: {refs_for_id:?}"
                    ),
                ));
            }
        }

        ids_to_retain.extend(self.compute_all_branch_snapshots_to_retain(
            table_meta.refs.values().cloned(),
            &table_meta,
        ));
        ids_to_retain.extend(
            self.unreferenced_snapshots_to_retain(table_meta.refs.values().cloned(), &table_meta),
        );

        let mut updates = vec![];
        let mut requirements = vec![];

        for ref_name in table_meta.refs.keys() {
            if !retained_refs.contains_key(ref_name) {
                updates.push(TableUpdate::RemoveSnapshotRef {
                    ref_name: ref_name.clone(),
                });
            }
        }

        let mut snapshot_to_remove = Vec::from_iter(self.ids_to_remove.iter().cloned());
        for snapshot in table_meta.snapshots() {
            if !ids_to_retain.contains(&snapshot.snapshot_id()) {
                snapshot_to_remove.push(snapshot.snapshot_id());
            }
        }

        if !snapshot_to_remove.is_empty() {
            // TODO: batch remove when server supports it
            for snapshot_id in snapshot_to_remove {
                updates.push(TableUpdate::RemoveSnapshots {
                    snapshot_ids: vec![snapshot_id],
                });
            }
        }

        if self.clear_expired_meta_data {
            let mut reachable_specs = HashSet::new();
            reachable_specs.insert(table_meta.default_partition_spec_id());
            let mut reachable_schemas = HashSet::new();
            reachable_schemas.insert(table_meta.current_schema_id());

            let retained_snapshots: Vec<_> = table_meta
                .snapshots()
                .filter(|s| ids_to_retain.contains(&s.snapshot_id()))
                .cloned()
                .collect();

            for snapshot in &retained_snapshots {
                if let Some(schema_id) = snapshot.schema_id() {
                    reachable_schemas.insert(schema_id);
                }
            }

            let loaded_lists = load_manifest_lists(
                table.file_io(),
                &table_meta,
                retained_snapshots,
                DEFAULT_LOAD_CONCURRENCY_LIMIT,
            )
            .await?;

            for (_, manifest_list) in loaded_lists {
                for manifest in manifest_list.entries() {
                    reachable_specs.insert(manifest.partition_spec_id);
                }
            }

            let spec_to_remove: Vec<i32> = table_meta
                .partition_specs_iter()
                .filter_map(|spec| {
                    if !reachable_specs.contains(&spec.spec_id()) {
                        Some(spec.spec_id())
                    } else {
                        None
                    }
                })
                .unique()
                .collect();

            if !spec_to_remove.is_empty() {
                updates.push(TableUpdate::RemovePartitionSpecs {
                    spec_ids: spec_to_remove,
                });
            }

            let schema_to_remove: Vec<i32> = table_meta
                .schemas_iter()
                .filter_map(|schema| {
                    if !reachable_schemas.contains(&schema.schema_id()) {
                        Some(schema.schema_id())
                    } else {
                        None
                    }
                })
                .unique()
                .collect();

            if !schema_to_remove.is_empty() {
                updates.push(TableUpdate::RemoveSchemas {
                    schema_ids: schema_to_remove,
                });
            }
        }

        requirements.push(TableRequirement::UuidMatch {
            uuid: table_meta.uuid(),
        });
        requirements.push(TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: table_meta.current_snapshot_id(),
        });

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use crate::io::FileIOBuilder;
    use crate::spec::{MAIN_BRANCH, TableMetadata};
    use crate::table::Table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableIdent, TableRequirement};

    fn make_v2_table_with_mutli_snapshot() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMultiSnapshot.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_remove_snapshot_action() {
        let table = make_v2_table_with_mutli_snapshot();
        let table_meta = table.metadata().clone();
        assert_eq!(5, table_meta.snapshots().count());
        {
            let tx = Transaction::new(&table);
            let act = tx.expire_snapshot();
            let mut v = Arc::new(act).commit(&table).await.unwrap();

            let updates = v.take_updates();
            assert_eq!(4, updates.len());

            let requirements = v.take_requirements();
            assert_eq!(2, requirements.len());
            // assert_eq!(4, tx.updates.len());

            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: table.metadata().current_snapshot_id()
                    }
                ],
                requirements
            );
        }

        {
            let tx = Transaction::new(&table);
            let act = tx.expire_snapshot().retain_last(2);
            let mut action_commit = Arc::new(act).commit(&table).await.unwrap();

            let updates = action_commit.take_updates();
            let requirements = action_commit.take_requirements();

            assert_eq!(3, updates.len());
            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: table.metadata().current_snapshot_id(),
                    }
                ],
                requirements
            );
        }

        {
            let tx = Transaction::new(&table);
            let act = tx.expire_snapshot().retain_last(100).expire_older_than(100);

            let mut action_commit = Arc::new(act).commit(&table).await.unwrap();

            let updates = action_commit.take_updates();
            let requirements = action_commit.take_requirements();

            assert_eq!(0, updates.len());
            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: table.metadata().current_snapshot_id(),
                    }
                ],
                requirements
            );
        }

        {
            // test remove main current snapshot
            let tx = Transaction::new(&table);
            let act = tx
                .expire_snapshot()
                .expire_snapshot_id(table.metadata().current_snapshot_id().unwrap());

            let err = Arc::new(act).commit(&table).await.err().unwrap();
            assert_eq!(
                "DataInvalid => Cannot remove snapshot 3067729675574597004 with retained references: [\"main\"]",
                err.to_string()
            )
        }
    }
}
