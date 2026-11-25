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

//! Dormant local whole-SST read index. The remote object store remains authoritative.
//!
//! Registration makes an object eligible for a local route but does not write or publish it.
//! Files use the fixed `<object_id>.sst` path. There is no download API in this layer:
//! safe path reuse requires the storage admission and GC unlink protections added later.
//! Unregistering prevents new lookups; existing handles retain their selected file.
//! Reads never enqueue refill work. The backend does not track version or refill identities.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectRangeBounds, ObjectResult, ObjectStoreRef};

mod membership;
#[cfg(test)]
pub(super) mod test_utils;
#[cfg(test)]
mod tests;

use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

/// One local file, shared by the index and readers after publication.
struct PinCacheFile {
    path: String,
    size: u64,
}

/// One registered SST object; registration alone does not make it readable or start refill.
struct PinCacheObject {
    published: Option<Arc<PinCacheFile>>,
}

impl PinCacheObject {
    /// Withdraws the read route and returns the detached file.
    /// Object registration remains valid.
    /// The caller must release the returned reference outside the shard lock.
    fn unpublish(&mut self) -> Option<Arc<PinCacheFile>> {
        let file = self.published.take()?;
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(file.size));
        Some(file)
    }

    #[cfg(test)]
    fn publish(&mut self, file: Arc<PinCacheFile>) {
        assert!(self.published.is_none());
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(file.size));
        self.published = Some(file);
    }
}

/// One shard's registered object index.
#[derive(Default)]
struct PinCacheShard {
    objects: HashMap<HummockSstableObjectId, PinCacheObject>,
}

impl PinCacheShard {
    fn register_object(&mut self, id: HummockSstableObjectId) {
        self.objects
            .entry(id)
            .or_insert(PinCacheObject { published: None });
    }
}

/// A local whole-SST cache. The remote object store remains authoritative.
/// Each object's state is protected by its shard lock. Reads and membership updates
/// may run concurrently; batch membership updates are not atomic across objects.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    // Hold only one shard lock at a time. Never perform I/O or call back into the controller
    // or refill executor while locked. Construction finishes before this cache is shared.
    shards: Box<[RwLock<PinCacheShard>]>,
}

/// A reference to one published file, retained even after its object leaves the index.
/// Reads never look up the object a second time.
/// Callers must support fallback if the selected local file becomes unavailable.
#[derive(Clone)]
pub(crate) struct PinCacheReadHandle {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    file: Arc<PinCacheFile>,
}

impl PinCacheReadHandle {
    /// Withdraws this specific publication, preserving registration.
    /// Use after its file fails to read or decode, or is no longer eligible for local reads.
    /// A shared cache fetch can fail on another request's route; such an error alone must not
    /// invalidate this handle's publication.
    pub(crate) fn invalidate(&self) {
        let mut state = self.pin_cache.shard(self.object_id).write();
        // A late failure must not invalidate a newer publication of the same object.
        if let Some(object) = state.objects.get_mut(&self.object_id)
            && object
                .published
                .as_ref()
                .is_some_and(|file| Arc::ptr_eq(file, &self.file))
        {
            let file = object.unpublish().unwrap();
            drop(state);
            drop(file);
        }
    }

    /// Reads the selected publication through the local object store. On failure, invalidates only
    /// this publication and returns the error so the caller can fall back to its normal read path.
    pub(crate) async fn read(&self, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        self.pin_cache
            .store
            .read(&self.file.path, range)
            .await
            .inspect_err(|_| self.invalidate())
    }
}

impl PinCache {
    /// Registers initial objects without reading or writing local files.
    /// Recovery is added separately before this backend is connected to production reads.
    pub(crate) fn new(
        store: ObjectStoreRef,
        shard_num: usize,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) -> Arc<Self> {
        assert!(
            shard_num > 0,
            "pin cache shard count must be greater than zero"
        );
        let mut pin_cache = Self {
            store,
            shards: (0..shard_num)
                .map(|_| RwLock::new(PinCacheShard::default()))
                .collect(),
        };
        for id in objects {
            pin_cache.shards[Self::shard_index(id, shard_num)]
                .get_mut()
                .register_object(id);
        }
        GLOBAL_PIN_CACHE_METRICS.published_objects.set(0);
        GLOBAL_PIN_CACHE_METRICS.published_bytes.set(0);
        Arc::new(pin_cache)
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheReadHandle> {
        let state = self.shard(object_id).read();
        let file = Arc::clone(state.objects.get(&object_id)?.published.as_ref()?);
        Some(PinCacheReadHandle {
            pin_cache: Arc::clone(self),
            object_id,
            file,
        })
    }

    fn shard_index(object_id: HummockSstableObjectId, shard_num: usize) -> usize {
        xxhash_rust::xxh64::xxh64(&object_id.as_raw_id().to_le_bytes(), 0) as usize % shard_num
    }

    fn shard(&self, object_id: HummockSstableObjectId) -> &RwLock<PinCacheShard> {
        &self.shards[Self::shard_index(object_id, self.shards.len())]
    }
}
