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
//! The caller inserts refill candidates and removes obsolete objects derived from version
//! deltas. The refiller owns the ordering of these operations relative to version publication;
//! this index does not track versions. Policy changes and full rebuilds use `replace_objects`.
//! A refill captures a `PinCacheRefillToken` with `prepare_refill` before it is queued;
//! `refill` checks the token before I/O and again at publication. Each object has one file
//! stage: Missing -> Downloading -> Published. Failure/cancellation returns it to Missing;
//! revocation also changes its admission identity. Removing the object invalidates all tokens.
//! Removing an object prevents new lookups; existing read handles retain their file entry.
//! Reads use `get` and never create refill work. Startup recovery, capacity accounting,
//! and physical file reclamation are added separately before production activation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectRangeBounds, ObjectResult, ObjectStoreRef};

mod membership;
mod refill;
#[cfg(test)]
mod tests;

use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

struct PinCacheEntry {
    path: String,
    size: u64,
}

/// One object's file lifecycle. Only `Published` can be read. A revoked download may
/// still be doing I/O, but its guard owns that obsolete file, not this index.
enum PinCacheFile {
    Missing { size: u64 },
    Downloading { size: u64 },
    Published(Arc<PinCacheEntry>),
}

struct PinCacheObject {
    // Admission identity, shared by queued work and its download. Stable across retries;
    // replaced on revocation. It is not a version ID or a persisted file sequence.
    generation: u64,
    file: PinCacheFile,
}

impl PinCacheObject {
    fn size(&self) -> u64 {
        match &self.file {
            PinCacheFile::Missing { size } | PinCacheFile::Downloading { size } => *size,
            PinCacheFile::Published(entry) => entry.size,
        }
    }

    fn published(&self) -> Option<&Arc<PinCacheEntry>> {
        match &self.file {
            PinCacheFile::Published(entry) => Some(entry),
            _ => None,
        }
    }

    /// Detaches a read route. Membership and refill admission remain valid.
    fn take_published(&mut self) -> Option<Arc<PinCacheEntry>> {
        let size = self.published()?.size;
        let PinCacheFile::Published(entry) =
            std::mem::replace(&mut self.file, PinCacheFile::Missing { size })
        else {
            unreachable!()
        };
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(size));
        Some(entry)
    }

    fn publish(&mut self, entry: Arc<PinCacheEntry>) {
        assert!(self.published().is_none());
        assert_eq!(self.size(), entry.size);
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(entry.size));
        self.file = PinCacheFile::Published(entry);
    }

    fn cancel_download(&mut self) {
        if let PinCacheFile::Downloading { size } = self.file {
            self.file = PinCacheFile::Missing { size };
        }
    }
}

/// One record per needed object; removing it revokes both reads and refills.
#[derive(Default)]
struct PinCacheState {
    objects: HashMap<HummockSstableObjectId, PinCacheObject>,
    // Never reset on removal: an old token must not match a reintroduced object.
    next_generation: u64,
}

impl PinCacheState {
    fn insert_object(&mut self, id: HummockSstableObjectId, size: u64) {
        let object = self.objects.entry(id).or_insert_with(|| {
            self.next_generation += 1;
            PinCacheObject {
                generation: self.next_generation,
                file: PinCacheFile::Missing { size },
            }
        });
        assert_eq!(
            object.size(),
            size,
            "one object must have one physical size"
        );
    }

    fn refill_object(&mut self, token: PinCacheRefillToken) -> Option<&mut PinCacheObject> {
        self.objects
            .get_mut(&token.object_id)
            .filter(|object| object.generation == token.generation)
    }
}

/// A local whole-SST cache. The remote object store remains authoritative.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    shards: Box<[RwLock<PinCacheState>]>,
    // Serializes batch membership updates, never acquired by foreground lookups.
    // A batch becomes visible shard by shard; each object's transition remains atomic.
    // Nested state locks follow membership_update -> one shard. Never hold
    // two shard locks together. Construction finishes before this cache is shared.
    // Shard code must not call back into membership or the refill executor, or perform I/O
    // while locked. The executor may hold its own state lock while calling shard operations.
    membership_update: Mutex<()>,
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

/// Describes the work performed or skipped by one refill attempt.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PinCacheRefillOutcome {
    /// This attempt copied and validated the SST, then published its local read route.
    Published,
    /// A local read route already existed, so this attempt skipped the download.
    AlreadyPublished,
    /// Another download for this object is still in flight; this attempt skipped the download.
    InProgress,
    /// This attempt is no longer eligible to publish, for example after its generation is revoked.
    Obsolete,
}

/// A reference to one published file, retained even after its object leaves the index.
/// Reads never look up the object a second time.
/// Callers must support fallback if the selected local file becomes unavailable.
#[derive(Clone)]
pub(crate) struct PinCacheReadHandle {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    entry: Arc<PinCacheEntry>,
}

impl PinCacheReadHandle {
    /// Withdraws this publication after a read or decode of its file fails.
    /// Call this only for data actually read through this handle: a shared cache fetch may fail
    /// on another request's route without ever reading this file.
    pub(crate) fn invalidate(&self) {
        let mut state = self.pin_cache.shard(self.object_id).write();
        // A late failure must not invalidate a newer publication of the same object.
        if let Some(object) = state.objects.get_mut(&self.object_id)
            && object
                .published()
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            object.take_published();
        }
    }

    /// Reads the selected publication through the local object store. On failure, invalidates only
    /// this publication and returns the error so the caller can fall back to its normal read path.
    pub(crate) async fn read(&self, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        self.pin_cache
            .store
            .read(&self.entry.path, range)
            .await
            .inspect_err(|_| self.invalidate())
    }
}

impl PinCache {
    /// Creates an index with the objects initially admitted by the caller.
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
                .map(|_| RwLock::new(PinCacheState::default()))
                .collect(),
            membership_update: Mutex::new(()),
            next_path_id: AtomicU64::new(rand::random()),
        };
        for (id, size) in objects {
            let state = pin_cache.shards[Self::shard_index(id, shard_num)].get_mut();
            state.insert_object(id, size);
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
        let entry = Arc::clone(state.objects.get(&object_id)?.published()?);
        Some(PinCacheReadHandle {
            pin_cache: Arc::clone(self),
            object_id,
            entry,
        })
    }

    fn shard_index(object_id: HummockSstableObjectId, shard_num: usize) -> usize {
        xxhash_rust::xxh64::xxh64(&object_id.as_raw_id().to_le_bytes(), 0) as usize % shard_num
    }

    fn shard(&self, object_id: HummockSstableObjectId) -> &RwLock<PinCacheState> {
        &self.shards[Self::shard_index(object_id, self.shards.len())]
    }
}
