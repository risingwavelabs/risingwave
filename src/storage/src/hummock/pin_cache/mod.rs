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

//! Local whole-SST storage with separate read, membership, and refill protocols.
//!
//! Callers register referenced objects and unregister obsolete ones at their chosen lifecycle
//! boundaries. The backend object registry does not track versions; the membership module
//! independently computes version-derived logical SST references.
//! Each object has an optional published file. The executor owns refill admission and
//! cancellation. Publication checks membership under the same lock as the index update.
//! Unregistering an object prevents new lookups; existing read handles retain their file.
//! Reads use `get` and never create refill work. Recovery completes before sharing the cache.
//! GC reclaims withdrawn files after their last reader releases them.
//! `storage` owns file and active-upload registration; `gc` selects and deletes files using that state.
//! They share one storage lock, separate from the shard locks used by the read index.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectResult, ObjectStoreRef};

mod gc;
pub(crate) mod membership;
mod read;
mod recovery;
mod refill;
mod storage;
pub(crate) use refill::PinCacheDownloadError;
#[cfg(test)]
pub(super) mod test_utils;
#[cfg(test)]
mod tests;

use self::storage::PinCacheStorageState;
use crate::hummock::SstableBlockIndex;
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

/// One download's local file, shared by the index and readers after publication.
struct PinCacheFile {
    path: String,
    size: u64,
}

/// One registered SST object; registration alone does not make it readable or start refill.
struct PinCacheObject {
    published: Option<Arc<PinCacheFile>>,
}

impl PinCacheObject {
    /// Withdraws the read route and returns the exact file to enqueue for deletion.
    /// Membership and refill admission remain valid.
    /// The caller must hand the returned reference to GC outside the shard lock.
    fn unpublish(&mut self) -> Option<Arc<PinCacheFile>> {
        let file = self.published.take()?;
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(file.size));
        Some(file)
    }

    fn publish(&mut self, file: Arc<PinCacheFile>) {
        assert!(self.published.is_none());
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(file.size));
        self.published = Some(file);
    }
}

/// One shard's object state and read flights, each protected by its own lock.
/// Hold at most one shard lock at a time. Release it before I/O, spawning tasks,
/// invoking callbacks, or acquiring the cache's storage lock.
#[derive(Default)]
struct PinCacheShard {
    objects: RwLock<HashMap<HummockSstableObjectId, PinCacheObject>>,
    read_requests: Mutex<HashMap<SstableBlockIndex, read::ReadRequest>>,
}

/// A local whole-SST cache. The remote object store remains authoritative.
/// Each object's state is protected by its shard lock. Reads, refills, and membership updates
/// may run concurrently; batch membership updates are not atomic across objects.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    shards: Box<[PinCacheShard]>,
    // Leaf lock: never acquire a shard lock or perform I/O while holding it.
    // Upload protection and physical file ownership share this lock.
    storage: Mutex<PinCacheStorageState>,
    // Serializes capacity probes/admission with GC path claims and deletion acknowledgment.
    capacity_gate: tokio::sync::Mutex<()>,
    // Largest physical SST rejected since the worker last took the pressure hint.
    gc_pressure_bytes: AtomicU64,
    #[cfg(test)]
    test_hooks: test_utils::PinCacheTestHooks,
}

/// A reference to one published file, retained even after its object leaves the index.
/// The file stays on disk until the last handle releases it. Reads never look up the object again.
/// Callers must support fallback if the selected local file becomes unavailable.
#[derive(Clone)]
pub(crate) struct PinCacheReadHandle {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    file: Arc<PinCacheFile>,
}

impl PinCache {
    /// Recovers local files selected by the initial pin-policy/version membership before sharing.
    /// An incomplete inventory fails initialization; no partially recovered cache is returned.
    pub(crate) async fn new(
        store: ObjectStoreRef,
        shard_num: usize,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) -> ObjectResult<Arc<Self>> {
        assert!(
            shard_num > 0,
            "pin cache shard count must be greater than zero"
        );
        let mut pin_cache = Self {
            store,
            storage: Mutex::default(),
            shards: (0..shard_num).map(|_| PinCacheShard::default()).collect(),
            capacity_gate: tokio::sync::Mutex::new(()),
            gc_pressure_bytes: AtomicU64::new(0),
            #[cfg(test)]
            test_hooks: test_utils::PinCacheTestHooks::default(),
        };
        let expected_sizes: HashMap<_, _> = objects.into_iter().collect();
        for &id in expected_sizes.keys() {
            pin_cache.shards[Self::shard_index(id, shard_num)]
                .objects
                .get_mut()
                .insert(id, PinCacheObject { published: None });
        }
        let metrics = &*GLOBAL_PIN_CACHE_METRICS;
        let objects = pin_cache.store.list("", None, None).await;
        let recovered = pin_cache
            .recover_local_files(objects, &expected_sizes)
            .await
            .inspect_err(|_| {
                metrics.recovery_failures.inc();
            })?;
        metrics
            .published_objects
            .set(metric_bytes(recovered.objects));
        metrics.published_bytes.set(metric_bytes(recovered.bytes));
        metrics.recovery_ready.set(1);
        Ok(Arc::new(pin_cache))
    }

    async fn available_space(&self) -> ObjectResult<Option<u64>> {
        #[cfg(test)]
        {
            let probe = self.test_hooks.space_probe.lock().clone();
            if let Some(probe) = probe {
                let result = probe();
                let observer = self.test_hooks.space_pause.lock().clone();
                if let Some(observer) = observer {
                    let (resume, paused) = tokio::sync::oneshot::channel();
                    if observer.send(resume).is_ok() {
                        let _ = paused.await;
                    }
                }
                return result;
            }
        }
        self.store.available_space().await
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheReadHandle> {
        let objects = self.shard(object_id).objects.read();
        let file = Arc::clone(objects.get(&object_id)?.published.as_ref()?);
        Some(PinCacheReadHandle {
            pin_cache: Arc::clone(self),
            object_id,
            file,
        })
    }

    fn shard_index(object_id: HummockSstableObjectId, shard_num: usize) -> usize {
        xxhash_rust::xxh64::xxh64(&object_id.as_raw_id().to_le_bytes(), 0) as usize % shard_num
    }

    fn shard(&self, object_id: HummockSstableObjectId) -> &PinCacheShard {
        &self.shards[Self::shard_index(object_id, self.shards.len())]
    }
}
