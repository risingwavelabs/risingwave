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

//! File selection and reservation ownership, independent of GC scheduling.
//!
//! The keeper holds one reference to each accounted file. Downloads, publications and readers
//! hold the others. Minor GC selects explicitly discarded files; full GC also discovers
//! abandoned files. Both wait for external owners to release their references. A selection
//! retains a reference until deletion finishes, preventing another pass from selecting it again.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use parking_lot::Mutex;
use risingwave_object_store::object::{ObjectMetadata, ObjectResult, ObjectStoreRef};

use super::{PinCacheFile, metric_bytes};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

mod executor;
#[cfg(test)]
pub(super) mod tests;

struct FileEntry {
    file: Arc<PinCacheFile>,
    // An unfinished upload requires an inventory of its temporary files. Completed uploads
    // and physical paths found during startup can be reclaimed by exact path alone.
    scan_upload: bool,
    changed_at: SystemTime,
}

#[derive(Default)]
struct PinCacheGcState {
    files: HashMap<String, FileEntry>,
    // A subset of `files`: exact paths of explicitly discarded, complete files.
    // Keep entries until deletion succeeds, including while readers or selections hold them.
    pending_deletes: HashSet<String>,
    accounted_bytes: u64,
}

impl PinCacheGcState {
    fn account(&mut self, size: u64) {
        self.accounted_bytes = self.accounted_bytes.saturating_add(size);
        self.report();
    }

    fn remove(&mut self, file: &PinCacheFile) {
        if self.files.remove(&file.path).is_some() {
            self.pending_deletes.remove(&file.path);
            self.accounted_bytes -= file.size;
            self.report();
        }
    }

    fn report(&self) {
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(self.accounted_bytes));
    }
}

struct Candidate {
    file: Arc<PinCacheFile>,
    paths: Vec<String>,
}

/// An owned selection. Dropping it without execution leaves all reservations in the keeper.
/// Files cannot be re-adopted after their last external owner releases them.
pub(super) struct PinCacheGcSelection {
    gc: Arc<PinCacheGc>,
    candidates: Vec<Candidate>,
}

impl PinCacheGcSelection {
    /// Execute minor and full selections through the same physical batch deleter.
    pub(super) async fn delete(self) -> ObjectResult<()> {
        let paths = self.candidates.iter().flat_map(|c| c.paths.iter().cloned());
        let result = executor::delete_files(&self.gc.store, paths).await;
        let mut end = 0;
        for candidate in &self.candidates {
            end += candidate.paths.len();
            if end > result.deleted {
                break;
            }
            // A reservation covers an entire upload, including any temporary files. Do not
            // release it for only a successfully deleted prefix of that upload's paths.
            self.gc.state.lock().remove(&candidate.file);
        }
        if let Some(error) = result.error {
            GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
            return Err(error);
        }
        Ok(())
    }
}

/// Owns accounted file leases. No timer, wakeup policy or background task lives here.
/// Run passes only while the cache is ready: after recovery and before releasing its index.
pub(super) struct PinCacheGc {
    store: ObjectStoreRef,
    capacity: u64,
    // Leaf lock: never call into the index or perform I/O while holding it.
    state: Mutex<PinCacheGcState>,
}

impl PinCacheGc {
    pub(super) fn new(store: ObjectStoreRef, capacity: u64) -> Self {
        GLOBAL_PIN_CACHE_METRICS
            .capacity_bytes
            .set(metric_bytes(capacity));
        GLOBAL_PIN_CACHE_METRICS.accounted_bytes.set(0);
        Self {
            store,
            capacity,
            state: Mutex::default(),
        }
    }

    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Register before starting any I/O. The caller owns the only external lease initially.
    pub(super) fn try_reserve(&self, path: String, size: u64) -> Result<Arc<PinCacheFile>, u64> {
        let mut state = self.state.lock();
        if state
            .accounted_bytes
            .checked_add(size)
            .is_none_or(|total| total > self.capacity)
        {
            return Err(state.accounted_bytes);
        }
        assert!(
            !state.files.contains_key(&path),
            "a cache path must not be reused"
        );
        let file = Arc::new(PinCacheFile {
            path: path.clone(),
            size,
        });
        state.files.insert(
            path,
            FileEntry {
                file: file.clone(),
                scan_upload: true,
                changed_at: SystemTime::now(),
            },
        );
        state.account(size);
        Ok(file)
    }

    /// Startup inventory accounts each physical path once, before any GC pass can run.
    pub(super) fn account_existing(&self, path: String, size: u64) -> Arc<PinCacheFile> {
        let mut state = self.state.lock();
        assert!(
            !state.files.contains_key(&path),
            "startup must inventory each path once"
        );
        let file = Arc::new(PinCacheFile {
            path: path.clone(),
            size,
        });
        state.files.insert(
            path,
            FileEntry {
                file: file.clone(),
                scan_upload: false,
                changed_at: UNIX_EPOCH,
            },
        );
        state.account(size);
        file
    }

    /// Only valid when local upload initialization has never been entered.
    pub(super) fn release_unused(&self, file: &PinCacheFile) {
        self.state.lock().remove(file);
    }

    pub(super) fn complete_upload(&self, file: &PinCacheFile) {
        let mut state = self.state.lock();
        let entry = state
            .files
            .get_mut(&file.path)
            .expect("upload owns a reservation");
        entry.scan_upload = false;
        entry.changed_at = SystemTime::now();
    }

    pub(super) fn finish_attempt(&self, file: &PinCacheFile) {
        let mut state = self.state.lock();
        let entry = state
            .files
            .get_mut(&file.path)
            .expect("upload owns a reservation");
        // Retention starts when the attempt stops, not when a possibly long upload began.
        entry.changed_at = SystemTime::now();
    }

    /// Hand off a complete, obsolete file after releasing its shard lock. This only records
    /// deletion intent; readers may still hold the file, and no GC pass is started here.
    pub(super) fn enqueue_delete(&self, file: Arc<PinCacheFile>) {
        let mut state = self.state.lock();
        debug_assert!(
            state
                .files
                .get(&file.path)
                .is_some_and(|entry| Arc::ptr_eq(&entry.file, &file) && !entry.scan_upload)
        );
        state.pending_deletes.insert(file.path.clone());
    }

    // Only snapshot files owned solely by the keeper. The cloned leases prevent another pass
    // from selecting them; candidate allocation and inventory processing happen outside the lock.
    fn snapshot_files(&self) -> Vec<Arc<PinCacheFile>> {
        self.state
            .lock()
            .files
            .values()
            .filter(|entry| Arc::strong_count(&entry.file) == 1)
            .map(|entry| entry.file.clone())
            .collect()
    }

    fn unreferenced_entry(&self, file: &Arc<PinCacheFile>) -> Option<(bool, SystemTime)> {
        let state = self.state.lock();
        let entry = state.files.get(&file.path)?;
        // Exactly the keeper and this snapshot. No external Weak references may resurrect a
        // file; only registration creates leases, and physical paths are never reused.
        (Arc::ptr_eq(&entry.file, file) && Arc::strong_count(file) == 2)
            .then_some((entry.scan_upload, entry.changed_at))
    }

    /// Only inspect explicitly discarded files, without scanning the keeper or storage.
    /// Files still held by readers remain pending for a later pass.
    pub(super) fn select_minor(self: &Arc<Self>) -> PinCacheGcSelection {
        let files: Vec<_> = {
            let state = self.state.lock();
            state
                .pending_deletes
                .iter()
                .map(|path| state.files[path].file.clone())
                .collect()
        };
        let candidates = files
            .into_iter()
            .filter(|file| self.unreferenced_entry(file).is_some())
            .map(|file| Candidate {
                paths: vec![file.path.clone()],
                file,
            })
            .collect();
        PinCacheGcSelection {
            gc: self.clone(),
            candidates,
        }
    }

    /// Inventory first, then consult live leases. The caller chooses the retention watermark;
    /// age alone never overrides an uploader, publication, reader or outstanding selection.
    /// The watermark must precede the scan so uploads stopped during it remain protected
    /// by their attempt timestamp, even if their temporary files were not listed.
    pub(super) async fn select_full(
        self: &Arc<Self>,
        modified_before: SystemTime,
    ) -> ObjectResult<PinCacheGcSelection> {
        let mut inventory = HashMap::new();
        let mut upload_paths: HashMap<String, Vec<String>> = HashMap::new();
        let mut objects = self.store.list("", None, None).await?;
        while let Some(metadata) = objects.try_next().await? {
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            if let Some(target) = upload_target(&metadata.key) {
                upload_paths
                    .entry(target.to_owned())
                    .or_default()
                    .push(metadata.key.clone());
            }
            inventory.insert(metadata.key.clone(), metadata);
        }
        let cutoff = modified_before
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let old_enough = |metadata: &ObjectMetadata| metadata.last_modified < cutoff;
        let mut candidates = Vec::new();
        for file in self.snapshot_files() {
            let Some((scan_upload, changed_at)) = self.unreferenced_entry(&file) else {
                continue;
            };
            if changed_at >= modified_before {
                continue;
            }
            let mut paths = vec![file.path.clone()];
            if scan_upload {
                paths.extend(upload_paths.remove(&file.path).unwrap_or_default());
            }
            if paths
                .iter()
                .any(|path| inventory.get(path).is_some_and(|m| !old_enough(m)))
            {
                continue;
            }
            for path in &paths {
                inventory.remove(path);
            }
            candidates.push(Candidate { file, paths });
        }
        // Discover files absent from the keeper (e.g. a late backend write after a failed
        // request). Never treat an active upload's temporary path as an unrelated zombie.
        for (path, metadata) in inventory {
            if !old_enough(&metadata) {
                continue;
            }
            let mut state = self.state.lock();
            if state.files.contains_key(&path)
                || upload_target(&path).is_some_and(|target| state.files.contains_key(target))
            {
                continue;
            }
            let file = Arc::new(PinCacheFile {
                path: path.clone(),
                size: metadata.total_size as u64,
            });
            state.files.insert(
                path.clone(),
                FileEntry {
                    file: file.clone(),
                    scan_upload: false,
                    changed_at: UNIX_EPOCH,
                },
            );
            state.account(file.size);
            drop(state);
            candidates.push(Candidate {
                file,
                paths: vec![path],
            });
        }
        Ok(PinCacheGcSelection {
            gc: self.clone(),
            candidates,
        })
    }
}

/// `OpenDAL` FS 0.58 stores atomic-upload files as `atomic_write_dir/<basename>.<8 alnum>`.
/// Keep this backend-specific association here, tested against a real FS upload. All Pin Cache
/// final paths are basenames. Unknown files remain subject to the full-GC retention watermark.
fn upload_target(path: &str) -> Option<&str> {
    let (target, suffix) = path.strip_prefix("atomic_write_dir/")?.rsplit_once('.')?;
    (suffix.len() == 8
        && suffix.bytes().all(|c| c.is_ascii_alphanumeric())
        && !target.contains('/'))
    .then_some(target)
}
