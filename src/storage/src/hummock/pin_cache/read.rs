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

use std::collections::hash_map::Entry;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::Shared;
use risingwave_object_store::object::{ObjectRangeBounds, ObjectResult};
use tokio::sync::oneshot;

use super::PinCacheReadHandle;
use crate::hummock::block_cache::HybridCachedBlockEntry;
use crate::hummock::{HummockError, HummockResult, SstableBlockIndex};

pub(super) type ReadRequest = Shared<oneshot::Receiver<HummockResult<HybridCachedBlockEntry>>>;

impl PinCacheReadHandle {
    /// Withdraws this specific publication, preserving registration and refill tokens.
    /// Use after its file fails to read or decode, or is no longer eligible for local reads.
    /// Call only for this handle's own I/O or decode failure, not an error shared by a fetch.
    pub(crate) fn invalidate(&self) {
        let mut state = self.pin_cache.shard(self.object_id).state.write();
        // A late failure must not invalidate a newer publication of the same object.
        if let Some(object) = state.objects.get_mut(&self.object_id)
            && object
                .published
                .as_ref()
                .is_some_and(|file| Arc::ptr_eq(file, &self.file))
        {
            let file = object.unpublish().unwrap();
            drop(state);
            self.pin_cache.enqueue_delete(file);
        }
    }

    /// Reads the selected publication. A failed read invalidates only this publication.
    pub(crate) async fn read(&self, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        self.pin_cache
            .store
            .read(&self.file.path, range)
            .await
            .inspect_err(|_| self.invalidate())
    }

    /// Coalesces the complete block fetch, including decoding and memory-cache insertion.
    /// The factory is called only for the leader, outside the request lock.
    /// The fetch must own its selected file handle, recheck memory before I/O, and handle
    /// failures against that file. It completes independently of cancelled waiters.
    pub(crate) fn get_or_fetch<Fut>(
        &self,
        block_index: usize,
        fetch: impl FnOnce() -> Fut,
    ) -> impl Future<Output = HummockResult<HybridCachedBlockEntry>> + Send + 'static
    where
        Fut: Future<Output = HummockResult<HybridCachedBlockEntry>> + Send + 'static,
    {
        let key = SstableBlockIndex {
            sst_id: self.object_id,
            block_idx: block_index as _,
        };
        let (request, sender) = {
            let mut requests = self.pin_cache.shard(self.object_id).read_requests.lock();
            match requests.entry(key) {
                Entry::Occupied(entry) => (entry.get().clone(), None),
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    (entry.insert(rx.shared()).clone(), Some(tx))
                }
            }
        };
        if let Some(sender) = sender {
            // Spawn outside the lock: a closed runtime may drop the task synchronously.
            // Capture the guard before spawn so an unpolled task also removes its request.
            let cleanup = scopeguard::guard((self.pin_cache.clone(), key), |(pin_cache, key)| {
                let request = pin_cache
                    .shard(key.sst_id)
                    .read_requests
                    .lock()
                    .remove(&key);
                drop(request);
            });
            let future = fetch();
            tokio::spawn(async move {
                let _cleanup = cleanup;
                let result = future.await;
                let _ = sender.send(result);
            });
        }
        async move {
            request
                .await
                .map_err(|_| HummockError::other("pin read task ended without a result"))?
        }
    }
}
