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
//! The caller registers refill candidates and unregisters obsolete objects derived from version
//! deltas. The refiller owns the ordering of these operations relative to version publication;
//! this index does not track versions.
//! A refill captures a `PinCacheRefillToken` with `prepare_refill` before it is queued;
//! the caller checks it before I/O, and `publish` checks it atomically with the index update.
//! Each object has one file state: `NotCached` or `Published`. The executor owns running attempts; downloads leave the
//! index unchanged until publication. Revocation changes the admission identity; unregistering
//! invalidates all object tokens.
//! Unregistering an object prevents new lookups; existing read handles retain their file.
//! Reads use `get` and never create refill work. Startup recovery, capacity accounting,
//! and physical file reclamation are added separately before production activation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectRangeBounds, ObjectResult, ObjectStoreRef};

mod membership;
mod refill;
#[cfg(test)]
pub(super) mod test_utils;
#[cfg(test)]
mod tests;

use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

/// One download's local file, shared by the index and readers after publication.
struct PinCacheFile {
    path: String,
    size: u64,
}

/// One object's file lifecycle. Only `Published` can be read. A revoked download may
/// still be doing I/O, but its guard owns that obsolete file, not this index.
/// `NotCached` means no readable copy; obsolete files may still await reclamation.
enum PinCacheObjectState {
    NotCached { size: u64 },
    Published(Arc<PinCacheFile>),
}

/// One registered SST object; registration alone does not make it readable or start refill.
struct PinCacheObject {
    // Admission identity, shared by queued work and its download. Stable across retries;
    // replaced on revocation. It is not a version ID or a persisted file sequence.
    generation: u64,
    state: PinCacheObjectState,
}

impl PinCacheObject {
    fn size(&self) -> u64 {
        match &self.state {
            PinCacheObjectState::NotCached { size } => *size,
            PinCacheObjectState::Published(file) => file.size,
        }
    }

    fn published(&self) -> Option<&Arc<PinCacheFile>> {
        match &self.state {
            PinCacheObjectState::Published(file) => Some(file),
            _ => None,
        }
    }

    /// Detaches a read route. Membership and refill admission remain valid.
    fn take_published(&mut self) -> Option<Arc<PinCacheFile>> {
        let size = self.published()?.size;
        let PinCacheObjectState::Published(file) =
            std::mem::replace(&mut self.state, PinCacheObjectState::NotCached { size })
        else {
            unreachable!()
        };
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(size));
        Some(file)
    }

    fn publish(&mut self, file: Arc<PinCacheFile>) {
        assert!(self.published().is_none());
        assert_eq!(self.size(), file.size);
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(file.size));
        self.state = PinCacheObjectState::Published(file);
    }
}

/// One shard's object index and refill generation allocator.
#[derive(Default)]
struct PinCacheShard {
    objects: HashMap<HummockSstableObjectId, PinCacheObject>,
    // Never reset on removal: an old token must not match a reintroduced object.
    next_generation: u64,
}

fn allocate_generation(counter: &mut u64) -> u64 {
    *counter += 1;
    *counter
}

impl PinCacheShard {
    fn register_object(&mut self, id: HummockSstableObjectId, size: u64) {
        let object = self.objects.entry(id).or_insert_with(|| PinCacheObject {
            generation: allocate_generation(&mut self.next_generation),
            state: PinCacheObjectState::NotCached { size },
        });
        assert_eq!(
            object.size(),
            size,
            "one object must have one physical size"
        );
    }

    /// Finds the registered object whose ID and generation both match the token.
    /// Returns None after unregistration or token revocation, including removal followed by
    /// re-registration of the same ID. This does not check whether a file is already published.
    fn object_matching_token(&mut self, token: PinCacheRefillToken) -> Option<&mut PinCacheObject> {
        self.objects
            .get_mut(&token.object_id)
            .filter(|object| object.generation == token.generation)
    }
}

/// A local whole-SST cache. The remote object store remains authoritative.
/// Each object's state is protected by its shard lock. Reads, refills, and membership updates
/// may run concurrently; batch membership updates are not atomic across objects.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    // Hold only one shard lock at a time. Never perform I/O or call back into the controller
    // or refill executor while locked. Construction finishes before this cache is shared.
    shards: Box<[RwLock<PinCacheShard>]>,
    next_path_id: AtomicU64,
}

/// Admission captured before queuing a refill. Its identity is private to the issuing cache.
/// This token reserves no disk space, starts no I/O, and needs no cleanup when discarded.
/// Copies preserve the same admission for retries; revocation invalidates every copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PinCacheRefillToken {
    object_id: HummockSstableObjectId,
    generation: u64,
}

impl PinCacheRefillToken {
    /// The SST whose remote path the caller should use for this refill.
    pub(crate) fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
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
    /// Withdraws this specific publication, preserving registration and refill tokens.
    /// Use after its file fails to read or decode, or is no longer eligible for local reads.
    /// A shared cache fetch can fail on another request's route; such an error alone must not
    /// invalidate this handle's publication.
    pub(crate) fn invalidate(&self) {
        let mut state = self.pin_cache.shard(self.object_id).write();
        // A late failure must not invalidate a newer publication of the same object.
        if let Some(object) = state.objects.get_mut(&self.object_id)
            && object
                .published()
                .is_some_and(|file| Arc::ptr_eq(file, &self.file))
        {
            object.take_published();
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
    /// Registers the initial objects without downloading them.
    /// The caller must supply an empty local store; existing-file recovery is added separately.
    pub(crate) fn new(
        store: ObjectStoreRef,
        shard_num: usize,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
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
            next_path_id: AtomicU64::new(rand::random()),
        };
        for (id, size) in objects {
            let state = pin_cache.shards[Self::shard_index(id, shard_num)].get_mut();
            state.register_object(id, size);
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
        let file = Arc::clone(state.objects.get(&object_id)?.published()?);
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
