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

use risingwave_object_store::object::ObjectMetadata;

use super::*;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;

fn metadata(id: HummockSstableObjectId, path_id: usize, size: usize) -> ObjectMetadata {
    ObjectMetadata {
        key: format!("{}-{path_id}.sst", id.as_raw_id()),
        last_modified: 0.0,
        total_size: size,
    }
}

fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

async fn cache(shard_num: usize, ids: &[HummockSstableObjectId]) -> PinCache {
    Arc::try_unwrap(
        PinCache::new(
            in_memory_object_store(),
            shard_num,
            2,
            ids.iter().map(|&id| (id, 8)),
        )
        .await
        .unwrap(),
    )
    .ok()
    .unwrap()
}

#[tokio::test]
async fn test_recovery_keeps_first_valid_file() {
    let id = HummockSstableObjectId::from(1001);
    let mut cache = cache(1, &[id]).await;
    let files = stream::iter((0..5000).map(move |path_id| {
        // The first candidate is invalid; the next must win over all later duplicates.
        Ok(metadata(id, path_id, if path_id == 0 { 4 } else { 8 }))
    }));
    let stats = cache
        .recover_local_files(Ok(files.boxed()), 2)
        .await
        .unwrap();
    assert_eq!(stats.objects, 1);
    assert_eq!(stats.bytes, 8);
    let object = &cache.shards[0].get_mut().objects[&id];
    assert_eq!(object.published().unwrap().path, "1001-1.sst");
}

#[tokio::test]
async fn test_recovery_scan_failure_leaves_index_unchanged() {
    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let files = stream::iter(
        (0..5000)
            .map(move |i| Ok(metadata(ids[i % ids.len()], i, 8)))
            .chain([Err(ObjectError::internal("injected inventory failure"))]),
    );
    let error = cache
        .recover_local_files(Ok(files.boxed()), 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected inventory failure"));
    // Even a late listing error must leave all registered objects untouched.
    for id in ids {
        let shard = cache.shard(id).read();
        let object = &shard.objects[&id];
        assert_eq!(object.size(), 8);
        assert!(object.published().is_none());
    }
}

#[tokio::test]
async fn test_recovery_handles_sparse_shards_at_different_concurrency() {
    for concurrency in [1, 2, 8, 32] {
        for occupied in [vec![0, 8, 16], (0..17).collect()] {
            let ids: Vec<_> = occupied.iter().map(|&i| object_in_shard(i, 17)).collect();
            let mut cache = cache(17, &ids).await;
            for round in 0..2 {
                let files = stream::iter(
                    ids.iter()
                        .flat_map(|&id| {
                            [
                                Ok(metadata(id, 0, 4)),
                                Ok(metadata(id, 1, 8)),
                                Ok(metadata(id, 2, 8)),
                            ]
                        })
                        .collect::<Vec<_>>(),
                );
                let stats = cache
                    .recover_local_files(Ok(files.boxed()), concurrency)
                    .await
                    .unwrap();
                let expected = if round == 0 { ids.len() as u64 } else { 0 };
                assert_eq!(stats.objects, expected);
                assert_eq!(stats.bytes, expected * 8);
                for &id in &ids {
                    let shard = cache.shard(id).read();
                    assert_eq!(
                        shard.objects[&id].published().unwrap().path,
                        format!("{}-1.sst", id.as_raw_id())
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_recovery_cancelled_during_scan_leaves_index_unchanged() {
    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let files = stream::iter(ids.map(|id| Ok(metadata(id, 1, 8)))).chain(stream::pending());
    let mut recovery = Box::pin(cache.recover_local_files(Ok(files.boxed()), 2));
    assert!(futures::poll!(&mut recovery).is_pending());
    drop(recovery);
    for id in ids {
        let shard = cache.shard(id).read();
        let object = &shard.objects[&id];
        assert_eq!(object.size(), 8);
        assert!(object.published().is_none());
    }
}
