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

use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::PinCache;

impl PinCache {
    /// Registers objects for explicit refill without downloading them.
    /// Registering an existing object preserves its state and refill tokens; it does not repair
    /// an invalidated file. A registered object is readable only after publication.
    pub(crate) fn register_objects(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let mut objects_by_shard = vec![Vec::new(); self.shards.len()];
        for (id, size) in objects {
            objects_by_shard[Self::shard_index(id, self.shards.len())].push((id, size));
        }
        for (shard, objects) in self.shards.iter().zip_eq_fast(objects_by_shard) {
            if objects.is_empty() {
                continue;
            }
            let mut state = shard.write();
            for (id, size) in objects {
                state.register_object(id, size);
            }
        }
    }

    /// Unregisters objects, withdrawing their publications and revoking all refill tokens.
    /// Existing read handles retain their files. The refiller decides when to apply delta
    /// deletions relative to version publication; this method does not wait for a version.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        let mut objects_by_shard = vec![Vec::new(); self.shards.len()];
        for id in objects {
            objects_by_shard[Self::shard_index(id, self.shards.len())].push(id);
        }
        let mut stale = Vec::new();
        for (shard, objects) in self.shards.iter().zip_eq_fast(objects_by_shard) {
            if objects.is_empty() {
                continue;
            }
            let mut state = shard.write();
            for id in objects {
                if let Some(mut object) = state.objects.remove(&id) {
                    stale.extend(object.take_published());
                }
            }
        }
        for file in stale {
            file.retire();
        }
    }

    /// Whether an object is registered, regardless of whether it has a readable local file.
    pub(crate) fn is_registered(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .read()
            .objects
            .contains_key(&object_id)
    }
}
