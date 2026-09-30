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

/// Owns one unpublished file; it has no access to the cache index or refill admission.
struct PinCacheDownloadGuard {
    file: Arc<PinCacheFile>,
}

impl PinCacheDownloadGuard {
    fn new(file: PinCacheFile) -> Self {
        Self {
            file: Arc::new(file),
        }
    }

    fn into_file(self) -> Arc<PinCacheFile> {
        self.file
    }

    /// Copies and validates the complete file without publishing it. Success returns the owner
    /// for the caller to submit. Errors and cancellation leave the index unchanged.
    /// Physical file reclamation is added separately.
    async fn write(
        self,
        store: &ObjectStoreRef,
        mut reader: MonitoredStreamingReader,
    ) -> ObjectResult<Self> {
        let file = &self.file;
        let mut writer = store
            .streaming_upload(&file.path)
            .await
            .inspect_err(|_| record_io_failure("local_upload_init"))?;
        let mut written = 0_u64;
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk.inspect_err(|_| record_io_failure("remote_read"))?;
            written = written.saturating_add(chunk.len() as u64);
            if written > file.size {
                record_io_failure("size_validation");
                return Err(ObjectError::internal(
                    "pinned SST is larger than its version metadata",
                ));
            }
            writer
                .write_bytes(chunk)
                .await
                .inspect_err(|_| record_io_failure("local_upload_write"))?;
        }
        writer
            .finish()
            .await
            .inspect_err(|_| record_io_failure("local_upload_finish"))?;
        let local_size = store
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
        Ok(self)
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
    /// Running I/O is not aborted, but its result can no longer be published. A subsequent
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
        &self,
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
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        let path = format!("{}-{path_id}.sst", token.object_id.as_raw_id());
        let download = PinCacheDownloadGuard::new(PinCacheFile { path, size });
        let reader = remote_store
            .streaming_read(&remote_path, ..)
            .await
            .inspect_err(|_| record_io_failure("remote_read_init"))?;
        let download = download.write(&self.store, reader).await?;
        Ok(self.publish(token, download))
    }

    /// Checks admission and installs the complete file under the same shard lock.
    /// A rejected download is dropped after the lock, so cleanup never runs while it is held.
    fn publish(
        &self,
        token: PinCacheRefillToken,
        download: PinCacheDownloadGuard,
    ) -> PinCacheRefillOutcome {
        let mut state = self.shard(token.object_id).write();
        let Some(object) = state.object_for_refill(token) else {
            return PinCacheRefillOutcome::Obsolete;
        };
        object.publish(download.into_file());
        PinCacheRefillOutcome::Published
    }
}

fn record_io_failure(phase: &'static str) {
    GLOBAL_PIN_CACHE_METRICS
        .io_failures
        .with_label_values(&[phase])
        .inc();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream};
    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

    use super::{PinCache, PinCacheDownloadGuard, PinCacheFile, PinCacheRefillOutcome};
    use crate::hummock::pin_cache::tests::{in_memory_object_store, local_object_store};
    use crate::monitor::ObjectStoreMetrics;

    #[tokio::test]
    async fn test_revoked_download_cannot_publish_and_replacement_can_retry() {
        for revoke_by_unregister in [false, true] {
            let pin_cache = PinCache::new(in_memory_object_store(), 1, []);
            let object_id = HummockSstableObjectId::from(1001);
            pin_cache.register_objects([(object_id, 11)]);
            let token = pin_cache.prepare_refill(object_id).unwrap();
            let remote = in_memory_object_store();
            remote
                .upload("sst", Bytes::from_static(b"replacement"))
                .await
                .unwrap();
            let reader = remote.streaming_read("sst", ..).await.unwrap();
            let old = PinCacheDownloadGuard::new(PinCacheFile {
                path: "1001-0.sst".into(),
                size: 11,
            })
            .write(&pin_cache.store, reader)
            .await
            .unwrap();

            if revoke_by_unregister {
                pin_cache.unregister_objects([object_id]);
                pin_cache.register_objects([(object_id, 11)]);
            } else {
                pin_cache.revoke_refill(object_id);
            }
            let old_path = old.file.path.clone();
            // The executor waits for the revoked attempt to finish before starting its replacement.
            assert_eq!(
                pin_cache.publish(token, old),
                PinCacheRefillOutcome::Obsolete
            );
            assert!(pin_cache.get(object_id).is_none());

            assert_eq!(
                pin_cache
                    .pin_sst(remote, "sst".into(), object_id)
                    .await
                    .unwrap(),
                PinCacheRefillOutcome::Published
            );
            assert_ne!(old_path, pin_cache.get(object_id).unwrap().file.path);
            assert_eq!(
                pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
                Bytes::from_static(b"replacement")
            );
        }
    }

    #[tokio::test]
    async fn test_interrupted_fs_upload_cannot_publish() {
        for cancel in [false, true] {
            let (_dir, local_store) = local_object_store().await;
            let pin_cache = PinCache::new(local_store.clone(), 1, []);
            let object_id = HummockSstableObjectId::from(1001);
            pin_cache.register_objects([(object_id, 8)]);

            let download = PinCacheDownloadGuard::new(PinCacheFile {
                path: "1001-0.sst".into(),
                size: 8,
            });
            let final_path = download.file.path.clone();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
            let reader = MonitoredStreamingReader::new(
                "test",
                Box::pin(
                    stream::iter([
                        Ok(Bytes::from_static(b"half")),
                        Ok(Bytes::from_static(b"x")),
                    ])
                    .chain(stream::once(async move {
                        // Two writes flush the FS position writer's one-chunk buffer.
                        started_tx.send(()).unwrap();
                        let _ = fail_rx.await;
                        Err(ObjectError::internal("injected remote read failure"))
                    })),
                ),
                Arc::new(ObjectStoreMetrics::unused()),
                None,
            );
            let upload_store = local_store.clone();
            let task =
                tokio::spawn(async move { download.write(&upload_store, reader).await.map(drop) });
            tokio::time::timeout(Duration::from_secs(5), started_rx)
                .await
                .unwrap()
                .unwrap();
            // Wait for Tokio's buffered file write to reach the filesystem before cancellation.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let files: Vec<_> = local_store
                        .list("", None, None)
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    if files.iter().any(|file| file.total_size == 4) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(pin_cache.get(object_id).is_none());
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                fail_tx.send(()).unwrap();
                assert!(task.await.unwrap().is_err());
            }

            assert!(pin_cache.get(object_id).is_none());
            assert!(
                local_store
                    .metadata(&final_path)
                    .await
                    .unwrap_err()
                    .is_object_not_found_error()
            );
            let remote = in_memory_object_store();
            remote
                .upload("sst", Bytes::from_static(b"complete"))
                .await
                .unwrap();
            assert_eq!(
                pin_cache
                    .pin_sst(remote, "sst".into(), object_id)
                    .await
                    .unwrap(),
                PinCacheRefillOutcome::Published
            );
        }
    }
}
