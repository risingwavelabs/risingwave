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

use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_connector::source::iceberg::PersistedFileScanTask;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use serde::{Deserialize, Serialize};

use crate::executor::StreamExecutorResult;
use crate::executor::source::SourceStateTableHandler;

pub(super) type FileEntry = (String, JsonbVal);

#[derive(Serialize, Deserialize)]
enum FileAssignment {
    Pending(Box<PersistedFileScanTask>),
    Done,
}

/// The representation of a file to be fetched in the internal state table.
/// `attempt` is used to distinguish between:
/// - starting a new load from scratch
/// - recovering from the last interrupted load (if any)
#[derive(Serialize, Deserialize)]
struct StoredAssignment {
    attempt: u64,
    assignment: FileAssignment,
}

impl StoredAssignment {
    fn encode(self) -> StreamExecutorResult<JsonbVal> {
        Ok(serde_json::to_value(self)
            .map_err(anyhow::Error::from)?
            .into())
    }

    fn decode(value: JsonbVal) -> StreamExecutorResult<Self> {
        Ok(serde_json::from_value(value.take()).map_err(anyhow::Error::from)?)
    }
}

pub(super) struct FileAssignments<S: StateStore> {
    pub(super) handler: SourceStateTableHandler<S>,
    attempt: u64,
}

impl<S: StateStore> FileAssignments<S> {
    pub(super) fn new(handler: SourceStateTableHandler<S>) -> Self {
        Self {
            handler,
            attempt: 0,
        }
    }

    /// Sets the current attempt and clears entries belonging to other attempts.
    pub(super) async fn start(&mut self, attempt: u64) -> StreamExecutorResult<()> {
        self.attempt = attempt;

        for (path, stored) in self.entries().await? {
            if stored.attempt != attempt {
                self.handler.delete(&path).await?;
            }
        }

        Ok(())
    }

    /// Records `path` as pending unless it already has a row in this load, pending or done.
    ///
    /// Returns whether the file was newly assigned
    pub(super) async fn assign(
        &mut self,
        path: &str,
        task: JsonbVal,
    ) -> StreamExecutorResult<bool> {
        if self.handler.get(path).await?.is_some() {
            return Ok(false);
        }

        let task = serde_json::from_value(task.take()).map_err(anyhow::Error::from)?;

        self.set(path, FileAssignment::Pending(task)).await?;

        Ok(true)
    }

    pub(super) async fn mark_done(&mut self, path: &str) -> StreamExecutorResult<()> {
        self.set(path, FileAssignment::Done).await
    }

    async fn set(&mut self, path: &str, assignment: FileAssignment) -> StreamExecutorResult<()> {
        let stored = StoredAssignment {
            attempt: self.attempt,
            assignment,
        };

        self.handler.set(path, stored.encode()?).await
    }

    async fn entries(&self) -> StreamExecutorResult<Vec<(String, StoredAssignment)>> {
        let table = self.handler.state_table();
        let mut entries = vec![];
        for vnode in table.vnodes().iter_vnodes() {
            let iter = table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(iter);
            while let Some(row) = iter.next().await {
                let row = row?;
                let path = row.datum_at(0).unwrap().into_utf8().to_owned();
                let value = row.datum_at(1).unwrap().into_jsonb().to_owned_scalar();
                entries.push((path, StoredAssignment::decode(value)?));
            }
        }

        Ok(entries)
    }

    pub(super) async fn pending(&self) -> StreamExecutorResult<Vec<FileEntry>> {
        self.entries()
            .await?
            .into_iter()
            .filter_map(|(path, stored)| match stored.assignment {
                FileAssignment::Pending(task) => Some(
                    serde_json::to_value(task)
                        .map(|task| (path, task.into()))
                        .map_err(anyhow::Error::from)
                        .map_err(Into::into),
                ),
                FileAssignment::Done => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{DataContentType, DataFileFormat, Schema};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::PbTable;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;

    /// Re-assigning a path never overwrites its stored state, whether pending or done, before
    /// or after a commit.
    #[tokio::test]
    async fn test_iceberg_file_assignments_are_idempotent() {
        let mut assignments = assignments(MemoryStateStore::new(), 1).await;

        // The first assignment inserts a row; assigning the same file again does not.
        assert!(
            assignments
                .assign("a.parquet", task("a.parquet"))
                .await
                .unwrap()
        );
        assert!(
            !assignments
                .assign("a.parquet", task("a.parquet"))
                .await
                .unwrap()
        );
        assert_eq!(
            assignments.pending().await.unwrap(),
            vec![("a.parquet".into(), task("a.parquet"))]
        );

        // A done file stays done when assigned again within the same epoch.
        assignments.mark_done("a.parquet").await.unwrap();
        assert!(
            !assignments
                .assign("a.parquet", task("a.parquet"))
                .await
                .unwrap()
        );
        assert!(assignments.pending().await.unwrap().is_empty());

        // The done tombstone also holds after it is committed.
        assignments
            .handler
            .commit(EpochPair::new_test_epoch(test_epoch(2)))
            .await
            .unwrap();
        assert!(
            !assignments
                .assign("a.parquet", task("a.parquet"))
                .await
                .unwrap()
        );
        assert!(assignments.pending().await.unwrap().is_empty());
    }

    /// After recovery, an actor sees exactly the progress committed before the crash, and a
    /// re-list cannot revive a committed done file.
    #[tokio::test]
    async fn test_iceberg_file_assignments_recover_committed_progress() {
        let store = MemoryStateStore::new();
        let mut original = assignments(store.clone(), 1).await;

        for path in ["done.parquet", "pending.parquet", "uncommitted.parquet"] {
            assert!(original.assign(path, task(path)).await.unwrap());
        }
        original.mark_done("done.parquet").await.unwrap();
        original
            .handler
            .commit(EpochPair::new_test_epoch(test_epoch(2)))
            .await
            .unwrap();

        original.mark_done("uncommitted.parquet").await.unwrap();
        drop(original);

        let mut recovered = assignments(store, 2).await;
        assert_eq!(
            pending_paths(&recovered).await,
            vec!["pending.parquet", "uncommitted.parquet"]
        );

        assert!(
            !recovered
                .assign("done.parquet", task("done.parquet"))
                .await
                .unwrap()
        );
        assert_eq!(
            pending_paths(&recovered).await,
            vec!["pending.parquet", "uncommitted.parquet"]
        );
    }

    /// Starting the same attempt keeps its rows, so a recovered load resumes.
    ///
    /// Starting another attempt removes pending rows and done tombstones durably,
    /// so every file can be assigned again.
    #[tokio::test]
    async fn test_iceberg_file_assignments_start_attempt() {
        let old_attempt = 1;
        let new_attempt = 2;
        let store = MemoryStateStore::new();
        let mut original = assignments(store.clone(), 1).await;
        original.start(old_attempt).await.unwrap();

        for path in ["done.parquet", "pending.parquet"] {
            original.assign(path, task(path)).await.unwrap();
        }
        original.mark_done("done.parquet").await.unwrap();
        original
            .handler
            .commit(EpochPair::new_test_epoch(test_epoch(2)))
            .await
            .unwrap();
        drop(original);

        let mut resumed = assignments(store.clone(), 2).await;
        resumed.start(old_attempt).await.unwrap();
        assert_eq!(pending_paths(&resumed).await, vec!["pending.parquet"]);
        assert!(
            !resumed
                .assign("done.parquet", task("done.parquet"))
                .await
                .unwrap()
        );

        resumed.start(new_attempt).await.unwrap();
        assert!(resumed.pending().await.unwrap().is_empty());
        resumed
            .handler
            .commit(EpochPair::new_test_epoch(test_epoch(3)))
            .await
            .unwrap();
        drop(resumed);

        let mut restarted = assignments(store, 3).await;
        restarted.start(new_attempt).await.unwrap();
        assert!(restarted.pending().await.unwrap().is_empty());

        for path in ["done.parquet", "pending.parquet"] {
            assert!(restarted.assign(path, task(path)).await.unwrap());
        }
        assert_eq!(
            pending_paths(&restarted).await,
            vec!["done.parquet", "pending.parquet"]
        );
    }

    fn catalog() -> PbTable {
        let mut table = gen_pbtable_with_dist_key(
            TableId::new(2333),
            vec![
                ColumnDesc::unnamed(ColumnId::new(0), DataType::Varchar),
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Jsonb),
            ],
            vec![OrderType::ascending()],
            vec![0],
            1,
            vec![0],
        );
        table.maybe_vnode_count = Some(VirtualNode::COUNT_FOR_TEST as u32);
        table
    }

    async fn assignments(store: MemoryStateStore, epoch: u64) -> FileAssignments<MemoryStateStore> {
        let handler = SourceStateTableHandler::from_table_catalog_with_vnodes(
            &catalog(),
            store,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
        )
        .await;

        let mut assignments = FileAssignments::new(handler);
        assignments
            .handler
            .init_epoch(EpochPair::new_test_epoch(test_epoch(epoch)))
            .await
            .unwrap();

        assignments
    }

    fn task(path: &str) -> JsonbVal {
        serde_json::to_value(PersistedFileScanTask {
            start: 0,
            length: 100,
            record_count: Some(10),
            first_row_id: None,
            data_sequence_number: None,
            data_file_path: path.to_owned(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            partition_spec_id: 0,
            file_size_in_bytes: 100,
            case_sensitive: true,
        })
        .unwrap()
        .into()
    }

    async fn pending_paths(assignments: &FileAssignments<MemoryStateStore>) -> Vec<String> {
        let mut paths = assignments
            .pending()
            .await
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }
}
