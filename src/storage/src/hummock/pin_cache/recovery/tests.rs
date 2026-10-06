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
use crate::hummock::pin_cache::test_utils::{in_memory_object_store, object_in_shard};

fn metadata(id: HummockSstableObjectId, path_id: usize, size: usize) -> ObjectMetadata {
    ObjectMetadata {
        key: format!("{}-{path_id}.sst", id.as_raw_id()),
        last_modified: 0.0,
        total_size: size,
    }
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
async fn test_recovery_handles_sparse_shards_at_different_concurrency() {
    // Serial, parallel, and more workers than occupied shards all restore the original indices.
    for concurrency in [1, 2, 8] {
        let ids = [0, 2, 4].map(|i| object_in_shard(i, 5));
        let mut cache = cache(5, &ids).await;
        let files = stream::iter(ids.map(|id| Ok(metadata(id, 1, 8))));
        let stats = cache
            .recover_local_files(Ok(files.boxed()), concurrency)
            .await
            .unwrap();
        assert_eq!(stats.objects, 3);
        assert_eq!(stats.bytes, 24);
        for id in ids {
            let shard = cache.shard(id).read();
            assert_eq!(
                shard.objects[&id].published().unwrap().path,
                format!("{}-1.sst", id.as_raw_id())
            );
        }
    }
}

#[tokio::test]
async fn test_recovery_keeps_first_valid_file_across_shards() {
    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let mut files = Vec::new();
    for id in ids {
        for (path_id, size) in [(0, 4), (1, 8), (2, 8)] {
            files.push(metadata(id, path_id, size));
        }
    }
    files.push(metadata(10000.into(), 1, 8));
    files.push(ObjectMetadata {
        key: "unfinished.tmp".into(),
        last_modified: 0.0,
        total_size: 4,
    });
    let stats = cache
        .recover_local_files(Ok(stream::iter(files.into_iter().map(Ok)).boxed()), 2)
        .await
        .unwrap();
    assert_eq!(stats.objects, 2);
    assert_eq!(stats.bytes, 16);
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
async fn test_incomplete_recovery_preserves_files_and_index() {
    use bytes::Bytes;

    for cancel in [false, true] {
        let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
        let mut cache = cache(3, &ids).await;
        let files = [
            metadata(ids[0], 1, 8),
            metadata(ids[1], 1, 8),
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
            let error = cache.recover_local_files(Ok(objects), 2).await.unwrap_err();
            assert!(error.to_string().contains("injected scan error"));
        }
        for id in ids {
            let shard = cache.shard(id).read();
            let object = &shard.objects[&id];
            assert_eq!(object.size(), 8);
            assert!(object.published().is_none());
        }
        for path in paths {
            assert_eq!(
                cache.store.read(&path, ..).await.unwrap(),
                Bytes::from_static(b"complete")
            );
        }
    }
}
