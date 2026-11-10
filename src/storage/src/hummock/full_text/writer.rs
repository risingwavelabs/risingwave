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

//! One key-partitioned writer of a full-text index.
//!
//! Documents written since the last flush are buffered in one in-memory segment, which may span
//! several epochs and is sealed and uploaded at the next flush. Deletes of documents still in the
//! buffer are applied in place. Deletes of documents flushed earlier are recorded as delete
//! markers in a delete file. A background maintenance task resolves delete files into
//! per-segment deletion vectors and merges small or mostly-deleted segments. Its result is
//! committed with a later epoch.
//!
//! At most one maintenance task runs at a time, and it is the only producer of removed
//! segments, deletion vector updates and consumed delete files. Between its start and its
//! commit, the index only gains new segments and new delete files, so its result always
//! applies to the latest state.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::mem::take;

use bytes::Bytes;
use futures::FutureExt;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::full_text_index::{
    FullTextDeleteFileInfo, FullTextDeletionVectorInfo, FullTextIndex, FullTextIndexAdd,
    FullTextSegmentInfo,
};
use risingwave_hummock_sdk::{HummockEpoch, HummockFullTextSegmentId, ObjectIndexAdd};
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;

use crate::hummock::full_text::objects::{
    open_segment, read_delete_file, upload_delete_file, upload_deletion_vector, upload_segment,
};
use crate::hummock::full_text::{OpenedSegment, SegmentBuilder, merge_segments};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::object_index_writer::{
    ObjectIndexFileIdManagerRef, ObjectIndexWriterContext, ObjectIndexWriterImpl,
};
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};

/// Merge when at least this many small segments exist.
const MERGE_MIN_SMALL_SEGMENTS: usize = 8;
/// Maximum number of segments merged in one task.
const MERGE_MAX_INPUTS: usize = 16;
/// Segments smaller than this are candidates for size-tiered merging.
const SMALL_SEGMENT_BYTES: u64 = 256 << 20;
/// Rewrite a segment when more than this fraction of its documents is deleted.
const REWRITE_DELETED_RATIO: f64 = 0.5;
/// Readers apply every pending delete file to their results, so the number of pending delete
/// files is bounded. From this many on, resolving deletes takes priority over merging.
const PRIORITIZE_RESOLVE_PENDING_DELETE_FILES: usize = 8;
/// From this many pending delete files on, flushing waits for the running maintenance task.
const MAX_PENDING_DELETE_FILES: usize = 32;

#[derive(Default)]
struct MaintenanceResult {
    removed_segments: Vec<HummockFullTextSegmentId>,
    added_segments: Vec<FullTextSegmentInfo>,
    deletion_vector_updates: Vec<(HummockFullTextSegmentId, FullTextDeletionVectorInfo)>,
    consumed_delete_files: Vec<risingwave_hummock_sdk::HummockFullTextDeleteFileId>,
}

pub(crate) struct FullTextIndexWriter {
    table_id: TableId,
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIndexFileIdManagerRef,
    /// The committed manifest, including the files produced by every writer.
    state: FullTextIndex,
    committed_epoch: u64,
    maintenance_enabled: bool,
    /// A new owner must observe the previous owner's checkpoint before starting maintenance.
    maintenance_after_epoch: u64,
    current_epoch: u64,
    /// The first epoch whose writes are not flushed yet. Every flushed document has a smaller
    /// epoch, and every buffered one an equal or larger epoch.
    buffer_start_epoch: u64,

    builder: Option<SegmentBuilder>,
    /// Doc ids of alive documents in `builder`.
    builder_docs: HashMap<Bytes, u32>,
    /// Keys deleted since the last flush.
    deleted_keys: BTreeSet<Bytes>,

    flushed_segments: Vec<FullTextSegmentInfo>,
    flushed_delete_files: Vec<FullTextDeleteFileInfo>,

    maintenance: Option<JoinHandle<HummockResult<MaintenanceResult>>>,
    /// Result of a maintenance task that has been waited for but not committed yet.
    finished_maintenance: Option<MaintenanceResult>,
    /// Whether the last started maintenance task resolved deletes.
    last_maintenance_resolved_deletes: bool,
}

impl FullTextIndexWriter {
    pub(crate) fn new(
        table_id: TableId,
        index: &FullTextIndex,
        init_epoch: u64,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIndexFileIdManagerRef,
    ) -> Self {
        Self {
            table_id,
            sstable_store,
            object_id_manager,
            state: index.clone(),
            committed_epoch: 0,
            maintenance_enabled: true,
            maintenance_after_epoch: init_epoch,
            current_epoch: init_epoch,
            buffer_start_epoch: init_epoch,
            builder: None,
            builder_docs: HashMap::new(),
            deleted_keys: BTreeSet::new(),
            flushed_segments: vec![],
            flushed_delete_files: vec![],
            maintenance: None,
            finished_maintenance: None,
            last_maintenance_resolved_deletes: false,
        }
    }

    pub(crate) fn set_maintenance_enabled(&mut self, enabled: bool) {
        if enabled == self.maintenance_enabled {
            return;
        }
        self.maintenance_enabled = enabled;
        if enabled {
            self.maintenance_after_epoch = self.current_epoch;
        } else {
            if let Some(handle) = self.maintenance.take() {
                handle.abort();
            }
            self.finished_maintenance = None;
        }
    }

    fn refresh_committed(&mut self, version: &PinnedVersion) {
        self.committed_epoch = version.table_committed_epoch(self.table_id).unwrap();
        self.state = version.full_text_indexes[&self.table_id].clone();
    }

    pub(crate) fn insert_document(&mut self, key: Bytes, tokens: &[String]) -> HummockResult<()> {
        let builder = match &mut self.builder {
            Some(builder) => builder,
            None => self.builder.insert(SegmentBuilder::new()?),
        };
        if let Some(prev_doc) = self.builder_docs.remove(&key) {
            // A stream key has at most one alive row, so the previous row must have been
            // deleted. Treat an unexpected duplicate as an implicit update.
            builder.delete(prev_doc);
        }
        let doc_id = builder.add(&key, self.current_epoch, tokens)?;
        self.builder_docs.insert(key, doc_id);
        Ok(())
    }

    pub(crate) fn delete_document(&mut self, key: Bytes) -> HummockResult<()> {
        if let Some(doc_id) = self.builder_docs.remove(&key) {
            self.builder
                .as_mut()
                .expect("builder must exist when it has docs")
                .delete(doc_id);
        }
        // A document of the same key may also exist in a flushed segment.
        self.deleted_keys.insert(key);
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> HummockResult<usize> {
        if self.state.pending_delete_files.len() >= MAX_PENDING_DELETE_FILES
            && let Some(handle) = self.maintenance.take()
        {
            // Backpressure: let the running task finish so that its result, or a delete
            // resolution started after it, can be committed before more delete files pile up.
            self.finished_maintenance = self.check_maintenance_result(handle.await);
        }
        let mut flushed_size = 0;
        if let Some(builder) = self.builder.take() {
            self.builder_docs.clear();
            let built = tokio::task::spawn_blocking(move || builder.finish())
                .await
                .map_err(|e| HummockError::other(format!("failed to seal segment: {e}")))??;
            if let Some(built) = built
                && built
                    .deletion_vector
                    .as_ref()
                    .is_none_or(|(_, num_deleted)| *num_deleted < built.num_docs)
            {
                let info =
                    upload_segment(&self.sstable_store, &self.object_id_manager, built).await?;
                flushed_size += info.file_size as usize;
                self.flushed_segments.push(info);
            }
        }
        if !self.deleted_keys.is_empty() {
            let keys: Vec<Bytes> = take(&mut self.deleted_keys).into_iter().collect();
            // Deletes of buffered documents are already applied, so the markers must only
            // delete flushed documents. A marker deletes documents of smaller epochs, so the
            // buffer start epoch spares the documents re-inserted after the delete, including
            // those of later epochs in the buffer.
            let info = upload_delete_file(
                &self.sstable_store,
                &self.object_id_manager,
                self.buffer_start_epoch,
                &keys,
            )
            .await?;
            flushed_size += info.file_size as usize;
            self.flushed_delete_files.push(info);
        }
        Ok(flushed_size)
    }

    pub(crate) fn seal_current_epoch(&mut self, next_epoch: u64) -> Option<FullTextIndexAdd> {
        assert!(next_epoch > self.current_epoch);
        let sealed_epoch = self.current_epoch;
        self.current_epoch = next_epoch;
        if self.builder.is_none() && self.deleted_keys.is_empty() {
            self.buffer_start_epoch = next_epoch;
        }

        let mut add = FullTextIndexAdd {
            added_segments: take(&mut self.flushed_segments),
            added_delete_files: take(&mut self.flushed_delete_files),
            ..Default::default()
        };
        if let Some(result) = self.take_finished_maintenance() {
            // Do not process the same committed inputs again while their replacement is
            // awaiting commit. In particular, a delete file may only be consumed after all
            // writers' older segments are visible in the committed manifest.
            self.maintenance_after_epoch = sealed_epoch;
            add.removed_segments = result.removed_segments;
            // Merged segments are added before the segments flushed in this epoch.
            add.added_segments.splice(0..0, result.added_segments);
            add.deletion_vector_updates = result.deletion_vector_updates;
            add.consumed_delete_files = result.consumed_delete_files;
        }
        self.maybe_start_maintenance();
        if add.is_empty() { None } else { Some(add) }
    }

    fn take_finished_maintenance(&mut self) -> Option<MaintenanceResult> {
        if let Some(result) = self.finished_maintenance.take() {
            return Some(result);
        }
        if !self.maintenance.as_ref()?.is_finished() {
            return None;
        }
        let handle = self.maintenance.take().unwrap();
        self.check_maintenance_result(handle.now_or_never().expect("task is finished"))
    }

    fn check_maintenance_result(
        &self,
        result: Result<HummockResult<MaintenanceResult>, tokio::task::JoinError>,
    ) -> Option<MaintenanceResult> {
        match result {
            Ok(Ok(result)) => Some(result),
            Ok(Err(e)) => {
                tracing::warn!(table_id = %self.table_id, error = %e.as_report(), "full-text index maintenance failed");
                None
            }
            Err(e) => {
                tracing::warn!(table_id = %self.table_id, error = %e.as_report(), "full-text index maintenance panicked");
                None
            }
        }
    }

    fn maybe_start_maintenance(&mut self) {
        if !self.maintenance_enabled
            || self.committed_epoch < self.maintenance_after_epoch
            || self.maintenance.is_some()
            || self.finished_maintenance.is_some()
        {
            return;
        }
        let sstable_store = self.sstable_store.clone();
        let object_id_manager = self.object_id_manager.clone();
        let merge_inputs = pick_merge_inputs(&self.state.segments);
        // Delete files arrive in almost every epoch of an update-heavy table. Alternate with
        // merges when both are needed, so that merges are not starved. Merging before the
        // pending deletes are resolved is safe: merged docs keep their epochs, and the pending
        // markers are resolved against the merged segment later.
        let num_pending = self.state.pending_delete_files.len();
        let resolve_deletes_now = num_pending > 0
            && (merge_inputs.is_none()
                || !self.last_maintenance_resolved_deletes
                || num_pending >= PRIORITIZE_RESOLVE_PENDING_DELETE_FILES);
        if resolve_deletes_now {
            let segments = self.state.segments.clone();
            let delete_files = self.state.pending_delete_files.clone();
            self.maintenance = Some(tokio::spawn(async move {
                resolve_deletes(&sstable_store, &object_id_manager, segments, delete_files).await
            }));
            self.last_maintenance_resolved_deletes = true;
        } else if let Some(inputs) = merge_inputs {
            self.maintenance = Some(tokio::spawn(async move {
                merge(&sstable_store, &object_id_manager, inputs).await
            }));
            self.last_maintenance_resolved_deletes = false;
        }
    }
}

impl ObjectIndexWriterImpl for FullTextIndexWriter {
    async fn create(
        ctx: &ObjectIndexWriterContext,
        version: &PinnedVersion,
        init_epoch: HummockEpoch,
    ) -> HummockResult<Self> {
        let index = version
            .full_text_indexes
            .get(&ctx.table_id)
            .ok_or_else(|| {
                HummockError::other(format!("full-text index not found: {}", ctx.table_id))
            })?;
        Ok(Self::new(
            ctx.table_id,
            index,
            init_epoch,
            ctx.sstable_store.clone(),
            ctx.object_id_manager.clone(),
        ))
    }

    fn seal_current_epoch(
        &mut self,
        next_epoch: HummockEpoch,
        version: &PinnedVersion,
    ) -> Option<ObjectIndexAdd> {
        self.refresh_committed(version);
        FullTextIndexWriter::seal_current_epoch(self, next_epoch).map(ObjectIndexAdd::FullText)
    }

    fn flush(&mut self) -> impl Future<Output = HummockResult<usize>> + Send + '_ {
        FullTextIndexWriter::flush(self)
    }

    async fn try_flush(&mut self) -> HummockResult<()> {
        // A flush within an epoch would make later deletes of the same epoch unable to tell
        // the flushed documents from the re-inserted ones. The buffer is flushed at checkpoint
        // barriers only.
        Ok(())
    }
}

impl Drop for FullTextIndexWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.maintenance.take() {
            handle.abort();
        }
    }
}

fn pick_merge_inputs(segments: &[FullTextSegmentInfo]) -> Option<Vec<FullTextSegmentInfo>> {
    // Rewrite mostly-deleted segments together. Under continuous updates, such segments appear
    // in almost every epoch, so rewriting them one at a time would not keep up.
    let mostly_deleted = segments
        .iter()
        .filter(|segment| {
            segment.num_deleted() as f64 > segment.num_docs as f64 * REWRITE_DELETED_RATIO
        })
        .take(MERGE_MAX_INPUTS)
        .cloned()
        .collect::<Vec<_>>();
    if !mostly_deleted.is_empty() {
        return Some(mostly_deleted);
    }
    let mut small: Vec<_> = segments
        .iter()
        .filter(|segment| segment.file_size < SMALL_SEGMENT_BYTES)
        .collect();
    if small.len() < MERGE_MIN_SMALL_SEGMENTS {
        return None;
    }
    small.sort_by_key(|segment| segment.file_size);
    Some(small.into_iter().take(MERGE_MAX_INPUTS).cloned().collect())
}

async fn resolve_deletes(
    sstable_store: &SstableStoreRef,
    object_id_manager: &ObjectIndexFileIdManagerRef,
    segments: Vec<FullTextSegmentInfo>,
    delete_files: Vec<FullTextDeleteFileInfo>,
) -> HummockResult<MaintenanceResult> {
    let mut markers: Vec<(Bytes, u64)> = vec![];
    for file in &delete_files {
        let file = read_delete_file(sstable_store, file).await?;
        markers.extend(file.keys.iter().map(|key| (key.clone(), file.epoch)));
    }
    markers.sort_unstable();

    let mut result = MaintenanceResult {
        consumed_delete_files: delete_files.iter().map(|file| file.object_id).collect(),
        ..Default::default()
    };
    for segment in &segments {
        let start = markers.partition_point(|(key, _)| key < &segment.min_key);
        let end = markers.partition_point(|(key, _)| key <= &segment.max_key);
        let segment_markers: Vec<_> = markers[start..end]
            .iter()
            .filter(|(_, epoch)| *epoch > segment.min_epoch)
            .cloned()
            .collect();
        if segment_markers.is_empty() {
            continue;
        }
        let opened = open_segment(sstable_store, segment).await?;
        let resolved =
            tokio::task::spawn_blocking(move || opened.segment.resolve_deletes(&segment_markers))
                .await
                .map_err(|e| HummockError::other(format!("failed to resolve deletes: {e}")))??;
        if let Some((data, num_deleted)) = resolved {
            let dv =
                upload_deletion_vector(sstable_store, object_id_manager, data, num_deleted).await?;
            result.deletion_vector_updates.push((segment.object_id, dv));
        }
    }
    Ok(result)
}

async fn merge(
    sstable_store: &SstableStoreRef,
    object_id_manager: &ObjectIndexFileIdManagerRef,
    inputs: Vec<FullTextSegmentInfo>,
) -> HummockResult<MaintenanceResult> {
    let mut opened = Vec::with_capacity(inputs.len());
    for input in &inputs {
        opened.push(open_segment(sstable_store, input).await?);
    }
    let merged = tokio::task::spawn_blocking(move || {
        let segments: Vec<&OpenedSegment> = opened.iter().map(|s| &s.segment).collect();
        merge_segments(&segments)
    })
    .await
    .map_err(|e| HummockError::other(format!("failed to merge segments: {e}")))??;
    let added_segments = match merged {
        Some(merged) => vec![upload_segment(sstable_store, object_id_manager, merged).await?],
        None => vec![],
    };
    Ok(MaintenanceResult {
        removed_segments: inputs.iter().map(|input| input.object_id).collect(),
        added_segments,
        ..Default::default()
    })
}
