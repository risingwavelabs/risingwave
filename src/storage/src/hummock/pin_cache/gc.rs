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

//! Minor/full file selection and batched deletion. Storage owns file lifetimes and capacity.
//! No timer, wakeup policy or background task lives here. Run passes only after recovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use risingwave_object_store::object::{ObjectMetadata, ObjectResult};

use super::{PinCache, PinCacheFile};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

#[cfg(test)]
mod tests;

// Match Hummock GC's maximum number of paths per delete request.
const DELETE_BATCH_SIZE: usize = 1000;

/// One lease per physical path. Unknown garbage is owned only by this selection; dropping it
/// leaves cleanup to the next scan. Accounted objects stay in the keeper until deletion succeeds.
pub(super) struct PinCacheGcSelection {
    // Keep the storage state and read index alive for the entire selection and deletion.
    cache: Arc<PinCache>,
    files: Vec<Arc<PinCacheFile>>,
}

impl PinCacheGcSelection {
    /// Delete either selection in batches, releasing capacity after each successful batch.
    pub(super) async fn delete(self) -> ObjectResult<()> {
        let mut paths = Vec::with_capacity(DELETE_BATCH_SIZE);
        for batch in self.files.chunks(DELETE_BATCH_SIZE) {
            paths.extend(batch.iter().map(|file| file.path.clone()));
            self.cache
                .store
                .delete_objects(&paths)
                .await
                .inspect_err(|_| {
                    GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
                })?;
            // Acknowledge this batch before the next await so cancellation cannot lose its
            // capacity release. Failed or unconfirmed batches remain accounted for a later pass.
            {
                let mut state = self.cache.storage.lock();
                for path in &paths {
                    state.remove_file(path);
                }
            }
            paths.clear();
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

impl PinCache {
    /// Only inspect explicitly discarded files, without scanning all files or listing storage.
    /// Files still held by readers remain pending for a later pass.
    pub(super) fn select_minor(self: &Arc<Self>) -> PinCacheGcSelection {
        let files: Vec<_> = {
            let state = self.storage.lock();
            state
                .pending_deletes
                .iter()
                .map(|path| &state.files[path])
                .filter(|file| Arc::strong_count(file) == 1)
                .cloned()
                .collect()
        };
        PinCacheGcSelection {
            cache: self.clone(),
            files,
        }
    }

    /// LIST physical files, exclude live objects and uploads, then apply the retention watermark.
    /// Claim unreferenced objects before LIST so missing ones can safely release their accounting;
    /// objects created or abandoned during the scan must not be mistaken for missing files.
    pub(super) async fn select_full(
        self: &Arc<Self>,
        modified_before: SystemTime,
    ) -> ObjectResult<PinCacheGcSelection> {
        // Claim keeper-only leases under the storage lock. There are no external Weak
        // references, and physical paths are never reused.
        let unreferenced: Vec<_> = self
            .storage
            .lock()
            .files
            .values()
            .filter(|file| Arc::strong_count(file) == 1)
            .cloned()
            .collect();
        let mut inventory = HashMap::new();
        let mut objects = self.store.list("", None, None).await?;
        while let Some(metadata) = objects.try_next().await? {
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            inventory.insert(metadata.key.clone(), metadata);
        }
        let cutoff = modified_before
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let old_enough = |metadata: &ObjectMetadata| metadata.last_modified < cutoff;
        let mut files = Vec::new();
        for file in unreferenced {
            match inventory.remove(&file.path) {
                Some(metadata) if old_enough(&metadata) => files.push(file),
                Some(_) => {}
                None => self.storage.lock().remove_file(&file.path),
            }
        }
        // Files absent from the completed-object index are independent garbage. Protect only
        // active uploads, including their backend-generated temporary paths; never register a
        // scanned orphan as a logical object just to delete it.
        for (path, metadata) in inventory {
            if !old_enough(&metadata) {
                continue;
            }
            let state = self.storage.lock();
            if state.uploads.contains_key(&path)
                || upload_target(&path).is_some_and(|target| state.uploads.contains_key(target))
            {
                continue;
            }
            if let Some(file) = state.files.get(&path) {
                if Arc::strong_count(file) == 1 {
                    files.push(file.clone());
                }
            } else {
                drop(state);
                files.push(Arc::new(PinCacheFile {
                    path,
                    size: metadata.total_size as u64,
                }));
            }
        }
        Ok(PinCacheGcSelection {
            cache: self.clone(),
            files,
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
