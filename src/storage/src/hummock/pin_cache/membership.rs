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

use risingwave_hummock_sdk::HummockSstableObjectId;

use super::PinCache;

impl PinCache {
    /// Registers objects for explicit refill without downloading them.
    /// Registering an existing object preserves its state and refill tokens; it does not repair
    /// an invalidated file. A registered object is readable only after publication.
    pub(crate) fn register_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        // Release the shard between objects so a large update does not hold up lookups.
        for id in objects {
            self.shard(id).write().register_object(id);
        }
    }

    /// Unregisters objects, withdrawing their publications and revoking all refill tokens.
    /// Existing read handles retain their files. The refiller decides when to apply delta
    /// deletions relative to version publication; this method does not wait for a version.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        for id in objects {
            // Release the shard before dropping the removed publication.
            let object = self.shard(id).write().objects.remove(&id);
            if let Some(mut object) = object {
                object.unpublish();
            }
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
