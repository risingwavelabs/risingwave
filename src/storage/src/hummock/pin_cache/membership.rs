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

use std::collections::HashMap;

use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::PinCache;

impl PinCache {
    fn partition_objects(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) -> Vec<HashMap<HummockSstableObjectId, u64>> {
        let mut shards = vec![HashMap::new(); self.shards.len()];
        for (id, size) in objects {
            if let Some(previous) =
                shards[Self::shard_index(id, self.shards.len())].insert(id, size)
            {
                assert_eq!(previous, size, "one object must have one physical size");
            }
        }
        shards
    }

    /// Admits objects for refill without downloading them or removing existing objects.
    /// Reinserting an admitted object preserves its file and refill tokens.
    pub(crate) fn insert_objects(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let objects = self.partition_objects(objects);
        for (shard, objects) in self.shards.iter().zip_eq_fast(objects) {
            if objects.is_empty() {
                continue;
            }
            let mut state = shard.write();
            for (id, size) in objects {
                state.insert_object(id, size);
            }
        }
    }

    /// Removes objects from the index and revokes their refill tokens immediately.
    /// Existing read handles retain their entries. The refiller decides when to apply delta
    /// deletions relative to version publication; this method does not wait for a version.
    pub(crate) fn remove_objects(&self, objects: impl IntoIterator<Item = HummockSstableObjectId>) {
        let mut objects_by_shard = vec![Vec::new(); self.shards.len()];
        for id in objects {
            objects_by_shard[Self::shard_index(id, self.shards.len())].push(id);
        }
        for (shard, objects) in self.shards.iter().zip_eq_fast(objects_by_shard) {
            if objects.is_empty() {
                continue;
            }
            let mut state = shard.write();
            for id in objects {
                if let Some(mut object) = state.objects.remove(&id) {
                    object.take_published();
                }
            }
        }
    }

    /// Replaces all admitted objects for a policy change or a full membership rebuild.
    /// Matching objects keep their file and refill tokens; removed or resized objects lose both.
    /// Normal version deltas use `insert_objects` and `remove_objects` instead.
    pub(crate) fn replace_objects(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let objects = self.partition_objects(objects);
        for (shard, objects) in self.shards.iter().zip_eq_fast(objects) {
            let mut state = shard.write();
            for (_, mut object) in state
                .objects
                .extract_if(|id, object| objects.get(id) != Some(&object.size()))
            {
                object.take_published();
            }
            for (id, size) in objects {
                state.insert_object(id, size);
            }
        }
    }

    /// Whether an object is admitted, including files not yet downloaded or published.
    pub(crate) fn contains_object(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .read()
            .objects
            .contains_key(&object_id)
    }
}
