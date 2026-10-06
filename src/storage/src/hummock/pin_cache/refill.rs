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

use super::{PinCache, PinCacheFile, PinCacheRefillToken};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

/// Owns a complete, validated file until the caller publishes or discards it.
/// Dropping it never changes the index. Physical reclamation is added separately.
pub(crate) struct PinCacheDownload {
    file: Arc<PinCacheFile>,
}

/// Copy and validate one file without reading or changing the cache index.
async fn write_file(
    store: &ObjectStoreRef,
    file: &PinCacheFile,
    mut reader: MonitoredStreamingReader,
) -> ObjectResult<()> {
    // TODO: Preallocate file.size bytes in the FS writer's temporary file once
    // OpenDAL supports physical space reservation.
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
    Ok(())
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
        self.shard(object_id).write().revoke_refill(object_id);
    }

    /// Copies and validates a whole SST without reading or changing the cache index.
    /// The caller checks membership, ownership and cache hits before downloading, and serializes
    /// attempts per object. Only a successful return can be submitted to `publish`.
    pub(crate) async fn download(
        &self,
        object_id: HummockSstableObjectId,
        size: u64,
        remote_store: ObjectStoreRef,
        remote_path: String,
    ) -> ObjectResult<PinCacheDownload> {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        let path = format!("{}-{path_id}.sst", object_id.as_raw_id());
        let file = PinCacheFile { path, size };
        let reader = remote_store
            .streaming_read(&remote_path, ..)
            .await
            .inspect_err(|_| record_io_failure("remote_read_init"))?;
        write_file(&self.store, &file, reader).await?;
        Ok(PinCacheDownload {
            file: Arc::new(file),
        })
    }

    /// Installs a downloaded file only if its original admission is still current.
    /// Returns false if the task was revoked while downloading. The caller must use the token
    /// for this file and serialize downloads per object; an existing publication is a bug.
    /// A rejected download is dropped after the lock, so cleanup never runs while it is held.
    pub(crate) fn publish(&self, token: PinCacheRefillToken, download: PinCacheDownload) -> bool {
        let mut state = self.shard(token.object_id).write();
        let Some(object) = state.object_matching_token(token) else {
            // This download lost permission to publish when its object was unregistered or revoked.
            return false;
        };
        object.publish(download.file);
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
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream};
    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

    use super::{PinCache, PinCacheFile, write_file};
    use crate::hummock::pin_cache::test_utils::{
        download_and_publish_for_test, in_memory_object_store, local_object_store,
    };
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
            let old = pin_cache
                .download(object_id, 11, remote.clone(), "sst".into())
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
            assert!(!pin_cache.publish(token, old));
            assert!(pin_cache.get(object_id).is_none());

            download_and_publish_for_test(&pin_cache, remote, "sst".into(), object_id)
                .await
                .unwrap();
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

            let file = PinCacheFile {
                path: "1001-0.sst".into(),
                size: 8,
            };
            let final_path = file.path.clone();
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
            let task = tokio::spawn(async move { write_file(&upload_store, &file, reader).await });
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
            download_and_publish_for_test(&pin_cache, remote, "sst".into(), object_id)
                .await
                .unwrap();
        }
    }
}
