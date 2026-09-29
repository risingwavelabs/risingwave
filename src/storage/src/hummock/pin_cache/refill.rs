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

use std::sync::Arc;
use std::sync::atomic::Ordering;

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectResult, ObjectStoreRef,
};

use super::{
    PinCache, PinCacheFile, PinCacheObjectState, PinCacheRefillOutcome, PinCacheRefillToken,
    PinCacheShard, allocate_generation,
};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

/// Owns an unpublished file. The index stays unchanged until successful publication.
pub(super) struct PinCacheDownloadGuard {
    pin_cache: Arc<PinCache>,
    token: PinCacheRefillToken,
    // Publication moves this reference into the read index.
    pub(super) file: Arc<PinCacheFile>,
}

impl PinCacheDownloadGuard {
    /// Allocates one unpublished file. The caller has checked admission and serializes attempts.
    pub(super) fn new(pin_cache: Arc<PinCache>, token: PinCacheRefillToken, size: u64) -> Self {
        let path_id = pin_cache.next_path_id.fetch_add(1, Ordering::Relaxed);
        let path = format!("{}-{path_id}.sst", token.object_id.as_raw_id());
        Self {
            pin_cache,
            token,
            file: Arc::new(PinCacheFile { path, size }),
        }
    }

    fn record_io_failure(&self, phase: &'static str) {
        GLOBAL_PIN_CACHE_METRICS
            .io_failures
            .with_label_values(&[phase])
            .inc();
    }

    /// Copies an existing SST stream into a unique local path using the object-store uploader.
    /// After finishing the upload, checks both the copied byte count and local file size before
    /// attempting publication. Publication still requires the current download token and membership.
    /// Errors and cancellation leave the index unchanged. A revoked download cannot publish.
    /// Physical file reclamation is added separately.
    pub(super) async fn write(
        self,
        mut reader: MonitoredStreamingReader,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let file = &self.file;
        let mut writer = self
            .pin_cache
            .store
            .streaming_upload(&file.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_init"))?;
        let mut written = 0_u64;
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk.inspect_err(|_| self.record_io_failure("remote_read"))?;
            written = written.saturating_add(chunk.len() as u64);
            if written > file.size {
                self.record_io_failure("size_validation");
                return Err(ObjectError::internal(
                    "pinned SST is larger than its version metadata",
                ));
            }
            writer
                .write_bytes(chunk)
                .await
                .inspect_err(|_| self.record_io_failure("local_upload_write"))?;
        }
        writer
            .finish()
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_finish"))?;
        let local_size = self
            .pin_cache
            .store
            .metadata(&file.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_metadata"))?
            .total_size as u64;
        if written != file.size || local_size != file.size {
            self.record_io_failure("size_validation");
            return Err(ObjectError::internal(
                "pinned SST size does not match its version metadata",
            ));
        }
        Ok(self.publish())
    }

    /// Publishes only while this admission is current. Removing the object or revoking its
    /// generation rejects publication. Success transfers the file to the index.
    pub(super) fn publish(self) -> PinCacheRefillOutcome {
        let mut state = self.pin_cache.shard(self.token.object_id).write();
        let Some(object) = state.object_for_refill(self.token) else {
            return PinCacheRefillOutcome::Obsolete;
        };
        object.publish(self.file);
        PinCacheRefillOutcome::Published
    }
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
        let state = self.shard(object_id).read();
        let object = state.objects.get(&object_id)?;
        Some(PinCacheRefillToken {
            object_id,
            generation: object.generation,
        })
    }

    /// Revokes queued and active refills without withdrawing a published route.
    /// Running I/O is not aborted, but its guard can no longer publish. A subsequent
    /// `prepare_refill` issues fresh admission for the object if it is still needed.
    pub(crate) fn revoke_refill(&self, object_id: HummockSstableObjectId) {
        let mut state = self.shard(object_id).write();
        let PinCacheShard {
            objects,
            next_generation,
            ..
        } = &mut *state;
        if let Some(object) = objects.get_mut(&object_id) {
            object.generation = allocate_generation(next_generation);
        }
    }

    /// Executes a queued refill using admission previously captured by this cache.
    /// The caller must serialize attempts per object, including across token revocation.
    /// Rechecks admission before starting a download.
    /// A stale token skips the download. The index changes only after a successful publication.
    pub(crate) async fn refill(
        self: &Arc<Self>,
        token: PinCacheRefillToken,
        remote_store: ObjectStoreRef,
        remote_path: String,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let size = {
            let state = self.shard(token.object_id).read();
            let Some(object) = state
                .objects
                .get(&token.object_id)
                .filter(|object| object.generation == token.generation)
            else {
                return Ok(PinCacheRefillOutcome::Obsolete);
            };
            match object.state {
                PinCacheObjectState::NotCached { size } => size,
                PinCacheObjectState::Published(_) => {
                    return Ok(PinCacheRefillOutcome::AlreadyPublished);
                }
            }
        };
        let download = PinCacheDownloadGuard::new(Arc::clone(self), token, size);
        let reader = remote_store
            .streaming_read(&remote_path, ..)
            .await
            .inspect_err(|_| download.record_io_failure("remote_read_init"))?;
        download.write(reader).await
    }
}
