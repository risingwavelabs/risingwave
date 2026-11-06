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

//! Physical file ownership for the local cache.
//!
//! Files stay owned here until confirmed absent or deleted, even after publication is
//! withdrawn. Publications, readers and GC selections hold external file leases.
//! Filesystem available space, rather than this index, determines refill admission.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{PinCache, PinCacheFile};

#[derive(Default)]
pub(super) struct PinCacheStorageState {
    pub(super) files: HashMap<String, Arc<PinCacheFile>>,
    // Protect live copies and their backend-generated temporary paths from GC.
    pub(super) uploads: HashSet<String>,
    // A subset of files: exact paths explicitly discarded, including reader-held files.
    pub(super) pending_deletes: HashSet<String>,
}

impl PinCacheStorageState {
    /// Forget a confirmed absent file and its deletion intent at most once.
    pub(super) fn remove_file(&mut self, path: &str) {
        self.files.remove(path);
        self.pending_deletes.remove(path);
    }
}

impl PinCache {
    /// Protect the unique target before submitting local I/O.
    pub(super) fn start_upload(&self, file: &PinCacheFile) {
        let mut state = self.storage.lock();
        assert!(
            !state.files.contains_key(&file.path) && state.uploads.insert(file.path.clone()),
            "a cache path must not be reused"
        );
    }

    /// End the copy owner's GC protection. Backend leftovers remain physical garbage.
    pub(super) fn finish_upload(&self, path: &str) {
        assert!(
            self.storage.lock().uploads.remove(path),
            "upload owns its path"
        );
    }

    /// Track one physical path, retaining its file until readers and GC release it.
    pub(super) fn track_file(&self, path: String, size: u64) -> Arc<PinCacheFile> {
        let mut state = self.storage.lock();
        assert!(
            !state.files.contains_key(&path),
            "each path is registered once"
        );
        let file = Arc::new(PinCacheFile {
            path: path.clone(),
            size,
        });
        state.files.insert(path, file.clone());
        file
    }

    /// Record deletion intent after releasing its shard lock; no GC pass starts here.
    pub(super) fn enqueue_delete(&self, file: Arc<PinCacheFile>) {
        let mut state = self.storage.lock();
        debug_assert!(
            state
                .files
                .get(&file.path)
                .is_some_and(|entry| Arc::ptr_eq(entry, &file))
        );
        state.pending_deletes.insert(file.path.clone());
    }
}
