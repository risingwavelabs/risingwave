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

//! Minor/full file selection and batched deletion. Storage owns physical file lifetimes.
//! The storage owner starts GC after recovery and aborts its worker on shutdown.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use risingwave_object_store::object::{ObjectMetadata, ObjectResult};
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{PinCache, PinCacheFile};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

#[cfg(test)]
mod tests;

// Match Hummock GC's maximum number of paths per delete request.
const DELETE_BATCH_SIZE: usize = 1000;
const MINOR_GC_INTERVAL: Duration = Duration::from_secs(1);
const FULL_GC_COOLDOWN: Duration = Duration::from_secs(30);
const FULL_GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// One lease per physical path. Unknown garbage is owned only by this selection; dropping it
/// leaves cleanup to the next scan. Tracked objects stay in the keeper until deletion succeeds.
pub(super) struct PinCacheGcSelection {
    // Keep the storage state and read index alive for the entire selection and deletion.
    cache: Arc<PinCache>,
    files: Vec<Arc<PinCacheFile>>,
    failed_uploads: Vec<String>,
}

impl PinCacheGcSelection {
    /// Delete either selection in batches, forgetting each confirmed batch.
    pub(super) async fn delete(self) -> ObjectResult<()> {
        let mut paths = Vec::with_capacity(DELETE_BATCH_SIZE);
        for batch in self.files.chunks(DELETE_BATCH_SIZE) {
            let capacity_gate = self.cache.capacity_gate.lock().await;
            paths.extend(batch.iter().map(|file| file.path.clone()));
            self.cache
                .store
                .delete_objects(&paths)
                .await
                .inspect_err(|_| {
                    GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
                })?;
            // Acknowledge this batch before the next await so cancellation cannot lose its
            // file removal. Failed or unconfirmed batches stay tracked for a later pass.
            {
                let mut state = self.cache.storage.lock();
                for path in &paths {
                    state.remove_file(path);
                }
            }
            paths.clear();
            drop(capacity_gate);
            tokio::task::yield_now().await;
        }
        if !self.failed_uploads.is_empty() {
            let targets: std::collections::HashSet<_> =
                self.failed_uploads.iter().map(String::as_str).collect();
            paths.extend(self.failed_uploads.iter().cloned());
            let mut listed = self
                .cache
                .store
                .list("atomic_write_dir/", None, None)
                .await?;
            while let Some(metadata) = listed.try_next().await? {
                if upload_target(&metadata.key).is_some_and(|target| targets.contains(target)) {
                    paths.push(metadata.key);
                }
            }
            let _gate = self.cache.capacity_gate.lock().await;
            for batch in paths.chunks(DELETE_BATCH_SIZE) {
                self.cache
                    .store
                    .delete_objects(batch)
                    .await
                    .inspect_err(|_| GLOBAL_PIN_CACHE_METRICS.gc_failures.inc())?;
            }
            let mut state = self.cache.storage.lock();
            for target in &self.failed_uploads {
                state.failed_uploads.remove(target);
                state.uploads.remove(target);
            }
        }
        Ok(())
    }
}

impl PinCache {
    /// Discover startup and runtime garbage in one storage-owned worker.
    /// Pressure hints can advance the next full scan after minor GC and a space recheck.
    pub(crate) fn start_gc(self: &Arc<Self>) -> JoinHandle<()> {
        let cache = self.clone();
        tokio::spawn(async move {
            cache.run_full_gc().await;
            let now = Instant::now();
            let mut next_pressure_check = now + FULL_GC_COOLDOWN;
            let mut next_periodic_full = now + FULL_GC_INTERVAL;
            let mut interval = tokio::time::interval(MINOR_GC_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = cache.select_minor().delete().await {
                    tracing::warn!(error = %error.as_report(), "Pin cache minor GC failed");
                }

                let now = Instant::now();
                let periodic = now >= next_periodic_full;
                if !periodic && now < next_pressure_check {
                    continue;
                }
                // Take one batch before doing I/O so arrivals during a recheck or scan stay
                // pending. This is a cleanup hint for attempts that have already ended.
                let required_bytes = cache.gc_pressure_bytes.swap(0, Ordering::Relaxed);
                if !periodic {
                    if required_bytes == 0 {
                        continue;
                    }
                    match cache.available_space().await {
                        Ok(Some(available_bytes)) if available_bytes < required_bytes => {}
                        Ok(_) => continue,
                        Err(error) => {
                            tracing::warn!(error = %error.as_report(), "Pin cache GC space check failed");
                            next_pressure_check = Instant::now() + FULL_GC_COOLDOWN;
                            continue;
                        }
                    }
                }
                cache.run_full_gc().await;
                // Full attempts share the same cooldown, even on failure or zero reclaim.
                let now = Instant::now();
                next_pressure_check = now + FULL_GC_COOLDOWN;
                next_periodic_full = now + FULL_GC_INTERVAL;
            }
        })
    }

    async fn run_full_gc(self: &Arc<Self>) {
        // Fix the age cutoff before the asynchronous listing starts.
        let selection = match self.select_full(SystemTime::now()).await {
            Ok(selection) => selection,
            Err(error) => {
                GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
                tracing::warn!(error = %error.as_report(), "Pin cache full GC listing failed");
                return;
            }
        };
        #[cfg(test)]
        {
            let observer = self.test_hooks.full_scan.lock().clone();
            if let Some(observer) = observer {
                let (resume, paused) = tokio::sync::oneshot::channel();
                let paths = selection
                    .files
                    .iter()
                    .map(|file| file.path.clone())
                    .collect();
                if observer.send((paths, resume)).is_ok() {
                    let _ = paused.await;
                }
            }
        }
        if let Err(error) = selection.delete().await {
            tracing::warn!(error = %error.as_report(), "Pin cache full GC failed");
        }
    }

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
        let failed_uploads = self.storage.lock().failed_uploads.iter().cloned().collect();
        PinCacheGcSelection {
            cache: self.clone(),
            files,
            failed_uploads,
        }
    }

    /// LIST physical files, exclude live objects and uploads, then apply the retention watermark.
    /// Claim unreferenced objects before LIST so missing ones can safely leave the index;
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
        let _capacity_gate = self.capacity_gate.lock().await;
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
            failed_uploads: Vec::new(),
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
