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

use futures::{StreamExt, stream};
use risingwave_object_store::object::{ObjectError, ObjectMetadata};

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
        PinCache::new(in_memory_object_store(), shard_num, ids.iter().copied())
            .await
            .unwrap(),
    )
    .ok()
    .unwrap()
}

#[tokio::test]
async fn test_recovery_keeps_first_file_across_shards() {
    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let mut files = Vec::new();
    for id in ids {
        for (path_id, size) in [(0, 4), (1, 8)] {
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
        .recover_local_files(Ok(stream::iter(files.into_iter().map(Ok)).boxed()))
        .await
        .unwrap();
    assert_eq!(stats.objects, 2);
    assert_eq!(stats.bytes, 8);
    for id in ids {
        assert_eq!(
            cache.shard(id).read().objects[&id]
                .published
                .as_ref()
                .unwrap()
                .path,
            format!("{}-0.sst", id.as_raw_id())
        );
    }
}

#[tokio::test]
async fn test_incomplete_recovery_preserves_files() {
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
            tokio::task::yield_now().await;
            let mut recovery = Box::pin(cache.recover_local_files(Ok(objects)));
            assert!(futures::poll!(&mut recovery).is_pending());
            drop(recovery);
        } else {
            let objects = prefix
                .chain(stream::once(async {
                    Err(ObjectError::internal("injected scan error"))
                }))
                .boxed();
            let error = cache.recover_local_files(Ok(objects)).await.unwrap_err();
            assert!(error.to_string().contains("injected scan error"));
        }
        for path in paths {
            assert_eq!(
                cache.store.read(&path, ..).await.unwrap(),
                Bytes::from_static(b"complete")
            );
        }
    }
}
