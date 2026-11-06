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

use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use risingwave_common::log::LogSuppressor;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectResult, ObjectStoreRef,
};

use super::{PinCache, PinCacheFile, PinCacheRefillToken};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

/// Download failures distinguish capacity pressure from object-store I/O errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PinCacheDownloadError {
    #[error("pin cache local capacity is exhausted")]
    CapacityRejected,
    #[error(transparent)]
    Io(#[from] ObjectError),
}

/// Owns a complete, validated file until the caller publishes or discards it.
/// Dropping this lease hands the unreferenced object to minor GC.
pub(crate) struct PinCacheDownload {
    file: Option<Arc<PinCacheFile>>,
    cache: Arc<PinCache>,
}

impl Drop for PinCacheDownload {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            self.cache.enqueue_delete(file);
        }
    }
}

pub(crate) struct PinCachePreparedDownload {
    cache: Arc<PinCache>,
    file: PinCacheFile,
    local_io_started: bool,
}

impl Drop for PinCachePreparedDownload {
    fn drop(&mut self) {
        // Before I/O, dropping preparation is a confirmed unused reservation. Once I/O
        // starts, only the physical owner may transfer it to a file or known cleanup.
        if !self.local_io_started {
            self.cache.finish_upload(&self.file.path);
        }
    }
}

impl PinCachePreparedDownload {
    pub(crate) async fn run(
        mut self,
        remote_store: ObjectStoreRef,
        remote_path: String,
    ) -> Result<PinCacheDownload, PinCacheDownloadError> {
        let reader = remote_store
            .streaming_read(&remote_path, ..)
            .await
            .inspect_err(|_| record_io_failure("remote_read_init"))?;
        self.local_io_started = true;
        // Owned native FS awaits each position job without a timeout that can detach it.
        // Catch copy panics at this physical owner boundary after the writer unwinds;
        // Drop also runs on cancellation and cannot establish physical completion.
        let result = AssertUnwindSafe(write_file(&self.cache, &self.file, reader))
            .rw_catch_unwind()
            .await;
        if !matches!(&result, Ok(Ok(_))) {
            let mut state = self.cache.storage.lock();
            if state.uploads.contains_key(&self.file.path) {
                // The owned writer has exited, including failure or unwind. Preserve
                // the full charge until minor confirms all associated paths are absent.
                state.failed_uploads.insert(self.file.path.clone());
            }
        }
        match result {
            Ok(result) => Ok(result?),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

/// Finish accepted writes on a remote error and hand completed failed files to minor GC.
/// Local I/O failures retain their reservation until known failure cleanup succeeds.
async fn write_file(
    cache: &Arc<PinCache>,
    file: &PinCacheFile,
    mut reader: MonitoredStreamingReader,
) -> ObjectResult<PinCacheDownload> {
    let mut writer = cache
        .store
        .streaming_upload_owned(&file.path)
        .await
        .inspect_err(|_| record_io_failure("local_upload_init"))?;
    let mut written = 0_u64;
    let mut read_error = None;
    while let Some(chunk) = reader.read_bytes().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                record_io_failure("remote_read");
                read_error = Some(error);
                break;
            }
        };
        let chunk_size = chunk.len() as u64;
        if chunk_size > file.size.saturating_sub(written) {
            record_io_failure("size_validation");
            read_error = Some(ObjectError::internal(
                "pinned SST is larger than its version metadata",
            ));
            break;
        }
        writer
            .write_bytes(chunk)
            .await
            .inspect_err(|_| record_io_failure("local_upload_write"))?;
        written += chunk_size;
    }
    #[cfg(test)]
    {
        let observer = cache.test_hooks.upload_finish.lock().clone();
        if let Some(observer) = observer {
            let (resume, paused) = tokio::sync::oneshot::channel();
            if observer.send((file.path.clone(), resume)).is_ok() {
                let _ = paused.await;
            }
        }
        assert!(
            !cache
                .test_hooks
                .panic_before_upload_finish
                .swap(false, Ordering::Relaxed),
            "injected pin upload panic before finish"
        );
    }
    writer
        .finish()
        .await
        .inspect_err(|_| record_io_failure("local_upload_finish"))?;
    // Register before metadata I/O so validation failure or cancellation retains cleanup intent.
    let completed = scopeguard::guard(cache.track_file(file.path.clone(), written), |file| {
        cache.enqueue_delete(file)
    });
    if let Some(error) = read_error {
        return Err(error);
    }
    let local_size = cache
        .store
        .metadata(&file.path)
        .await
        .inspect_err(|_| record_io_failure("local_metadata"))?
        .total_size as u64;
    if written != file.size || local_size != file.size {
        record_io_failure("size_validation");
        return Err(ObjectError::internal(
            "pinned SST size does not match its version metadata",
        ));
    }
    Ok(PinCacheDownload {
        file: Some(scopeguard::ScopeGuard::into_inner(completed)),
        cache: cache.clone(),
    })
}

impl PinCache {
    /// Captures admission for a needed object before the caller queues a refill.
    /// Execution must use this token with the same cache; it must not recapture admission after
    /// waiting in a queue or when retrying the same work. Revocation invalidates all previously
    /// issued tokens for the object.
    pub(crate) fn prepare_refill(
        &self,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheRefillToken> {
        let state = self.shard(object_id).state.read();
        let object = state.objects.get(&object_id)?;
        Some(PinCacheRefillToken {
            object_id,
            generation: object.generation,
        })
    }

    /// Revokes queued and active refills without withdrawing a published route.
    /// Running I/O is not aborted, but its result can no longer be published. A subsequent
    /// `prepare_refill` issues fresh admission for the object if it is still needed.
    pub(crate) fn revoke_refill(&self, object_id: HummockSstableObjectId) {
        self.shard(object_id).state.write().revoke_refill(object_id);
    }

    /// Reserves a unique target and full physical SST bytes before starting the copy.
    /// Uses physical `file_size` rather than the logical SST projection size.
    /// The caller filters membership, ownership and cache hits and serializes each object.
    /// Running this preparation produces a validated download for later publication.
    pub(crate) async fn prepare_download(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
        object_size: u64,
    ) -> Result<PinCachePreparedDownload, PinCacheDownloadError> {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        let file = PinCacheFile {
            path: format!("{}-{path_id}.sst", object_id.as_raw_id()),
            size: object_size,
        };
        if !self
            .admit_upload(&file)
            .await
            .inspect_err(|_| record_io_failure("local_space"))?
        {
            static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                LazyLock::new(|| LogSuppressor::per_minute(1));
            if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                tracing::warn!(
                    suppressed_count,
                    object_id = object_id.as_raw_id(),
                    object_size,
                    "skipping pin cache refill because its byte budget or filesystem space is insufficient"
                );
            }
            self.gc_pressure_bytes
                .fetch_max(object_size, Ordering::Relaxed);
            return Err(PinCacheDownloadError::CapacityRejected);
        }
        Ok(PinCachePreparedDownload {
            cache: self.clone(),
            file,
            local_io_started: false,
        })
    }

    #[cfg(test)]
    pub(crate) async fn download(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
        object_size: u64,
        remote_store: ObjectStoreRef,
        remote_path: String,
    ) -> Result<PinCacheDownload, PinCacheDownloadError> {
        let prepared = self.prepare_download(object_id, object_size).await?;
        tokio::spawn(prepared.run(remote_store, remote_path))
            .await
            .map_err(|error| {
                ObjectError::internal(format!("owned test download stopped: {error}"))
            })?
    }

    /// Installs a downloaded file only if its original admission is still current.
    /// Returns false if the task was revoked while downloading. The caller must use the token
    /// for this file and serialize downloads per object; an existing publication is a bug.
    /// A rejected download is handed to GC after releasing the lock.
    pub(crate) fn publish(
        &self,
        token: PinCacheRefillToken,
        mut download: PinCacheDownload,
    ) -> bool {
        let mut state = self.shard(token.object_id).state.write();
        let Some(object) = state.object_matching_token(token) else {
            // This download lost permission to publish when its object was unregistered or revoked.
            drop(state);
            return false;
        };
        object.publish(download.file.take().unwrap());
        true
    }
}

fn record_io_failure(phase: &'static str) {
    GLOBAL_PIN_CACHE_METRICS
        .io_failures
        .with_label_values(&[phase])
        .inc();
}

#[cfg(test)]
mod tests;
