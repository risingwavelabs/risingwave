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
            u64::MAX,
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
                                Ok(metadata(id, round * 3, 4)),
                                Ok(metadata(id, round * 3 + 1, 8)),
                                Ok(metadata(id, round * 3 + 2, 8)),
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

#[tokio::test]
async fn test_recovery_reclaims_rejected_files_across_shards() {
    use bytes::Bytes;

    use crate::hummock::pin_cache::gc::tests::accounted_bytes;

    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let mut files = Vec::new();
    let mut retained = Vec::new();
    for id in ids {
        for (path_id, size) in [(0, 4), (1, 8), (2, 8)] {
            files.push(metadata(id, path_id, size));
        }
        retained.push(format!("{}-1.sst", id.as_raw_id()));
    }
    files.push(metadata(10000.into(), 1, 8));
    files.push(ObjectMetadata {
        key: "unfinished.tmp".into(),
        last_modified: 0.0,
        total_size: 4,
    });
    for file in &files {
        cache
            .store
            .upload(&file.key, Bytes::from(vec![b'x'; file.total_size]))
            .await
            .unwrap();
    }
    let paths: Vec<_> = files.iter().map(|file| file.key.clone()).collect();
    let stats = cache
        .recover_local_files(Ok(stream::iter(files.into_iter().map(Ok)).boxed()), 2)
        .await
        .unwrap();
    assert_eq!(stats.objects, 2);
    assert_eq!(stats.bytes, 16);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while accounted_bytes(&cache.gc) != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for path in paths {
        if retained.contains(&path) {
            assert_eq!(
                cache.store.read(&path, ..).await.unwrap(),
                Bytes::from_static(b"xxxxxxxx")
            );
        } else {
            assert!(
                cache
                    .store
                    .metadata(&path)
                    .await
                    .unwrap_err()
                    .is_object_not_found_error()
            );
        }
    }
    for id in ids {
        assert_eq!(
            cache.shard(id).read().objects[&id]
                .published()
                .unwrap()
                .path,
            format!("{}-1.sst", id.as_raw_id())
        );
    }
}

#[tokio::test]
async fn test_incomplete_recovery_preserves_files_and_accounting() {
    use bytes::Bytes;

    use crate::hummock::pin_cache::gc::tests::accounted_bytes;

    for cancel in [false, true] {
        let id = HummockSstableObjectId::from(1001);
        let mut cache = cache(3, &[id]).await;
        let files = [
            metadata(id, 1, 8),
            metadata(10000.into(), 1, 8),
            ObjectMetadata {
                key: "unfinished.tmp".into(),
                last_modified: 0.0,
                total_size: 8,
            },
        ];
        let paths: Vec<_> = files.iter().map(|file| file.key.clone()).collect();
        for path in &paths {
            cache
                .store
                .upload(path, Bytes::from_static(b"complete"))
                .await
                .unwrap();
        }
        let prefix = stream::iter(files.into_iter().map(Ok));
        if cancel {
            let objects = prefix.chain(stream::pending()).boxed();
            let mut recovery = Box::pin(cache.recover_local_files(Ok(objects), 2));
            assert!(futures::poll!(&mut recovery).is_pending());
            drop(recovery);
        } else {
            let objects = prefix
                .chain(stream::once(async {
                    Err(ObjectError::internal("injected scan error"))
                }))
                .boxed();
            assert!(cache.recover_local_files(Ok(objects), 2).await.is_err());
        }
        tokio::task::yield_now().await;
        assert_eq!(accounted_bytes(&cache.gc), 24);
        assert!(cache.shard(id).read().objects[&id].published().is_none());
        for path in paths {
            assert_eq!(
                cache.store.read(&path, ..).await.unwrap(),
                Bytes::from_static(b"complete")
            );
        }
    }
}
