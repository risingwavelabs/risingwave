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

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::TryStreamExt;

use super::super::test_utils::{in_memory_object_store, local_object_store};
use super::{DELETE_BATCH_SIZE, PinCache, PinCacheFile, PinCacheGcSelection};

#[tokio::test]
async fn test_selection_protects_files_and_can_be_abandoned() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 16, 1, []).await.unwrap();
    let file = cache.account_existing("1001-1.sst".into(), 8);
    store
        .upload(&file.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let abandoned = cache.account_existing("1002-2.sst".into(), 8);
    store
        .upload(&abandoned.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    drop(abandoned);
    // Even a complete, unreferenced file is not a minor candidate without deletion intent.
    assert!(cache.select_minor().files.is_empty());
    cache.enqueue_delete(file.clone());
    // The explicit candidate still has a reader; enqueueing it must not bypass that lease.
    assert!(cache.select_minor().files.is_empty());
    drop(file);
    let selected = cache.select_minor();
    assert_eq!(selected.files.len(), 1);
    assert!(cache.select_minor().files.is_empty());
    let full = cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(full.files.len(), 1);
    assert_eq!(full.files[0].path, "1002-2.sst");
    drop(full);
    assert!(
        cache
            .try_reserve(&PinCacheFile {
                path: "1003-3.sst".into(),
                size: 1,
            })
            .is_err()
    );
    drop(selected);
    assert_eq!(cache.storage.lock().accounted_bytes, 16);
    cache.select_minor().delete().await.unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 8);
    assert!(
        store
            .metadata("1001-1.sst")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
    assert!(store.metadata("1002-2.sst").await.is_ok());
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 0);
}

#[tokio::test]
async fn test_failed_deletion_keeps_capacity_and_full_gc_retries() {
    let (dir, store) = local_object_store().await;
    let cache = PinCache::new(store.clone(), 8, 1, []).await.unwrap();
    let path = "1001-42.sst";
    // A nonempty directory makes deletion fail even when the test runs as root.
    std::fs::create_dir(dir.path().join(path)).unwrap();
    std::fs::write(dir.path().join(path).join("child"), b"complete").unwrap();
    cache.enqueue_delete(cache.account_existing(path.into(), 8));
    assert!(cache.select_minor().delete().await.is_err());
    assert_eq!(cache.storage.lock().accounted_bytes, 8);
    assert_eq!(cache.select_minor().files.len(), 1);
    std::fs::remove_file(dir.path().join(path).join("child")).unwrap();
    std::fs::remove_dir(dir.path().join(path)).unwrap();
    // Missing files are idempotent deletes, and the reservation is released only once.
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 0);
    cache.select_minor().delete().await.unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 0);
}

#[tokio::test]
async fn test_full_gc_respects_retention_and_discovers_zombies() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 8, 1, []).await.unwrap();
    store
        .upload("orphan.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    assert!(
        cache
            .select_full(SystemTime::UNIX_EPOCH)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    let selection = cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(selection.files.len(), 1);
    assert_eq!(cache.storage.lock().accounted_bytes, 0);
    selection.delete().await.unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 0);
    assert!(
        store
            .metadata("orphan.tmp")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[tokio::test]
async fn test_full_gc_preserves_objects_but_reclaims_stale_temporary_files() {
    let store = in_memory_object_store();
    let cache = PinCache::new(store.clone(), 8, 1, []).await.unwrap();
    let final_path = "1001-1.sst";
    let temporary_path = "atomic_write_dir/1001-1.sst.12345678";
    store
        .upload(final_path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    store
        .upload(temporary_path, Bytes::from_static(b"temp"))
        .await
        .unwrap();
    let published = cache.account_existing(final_path.into(), 8);

    // A completed object protects only its own path, not temporary residue from an old upload.
    cache
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(cache.storage.lock().accounted_bytes, 8);
    assert!(store.metadata(&published.path).await.is_ok());
    assert!(
        store
            .metadata(temporary_path)
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[tokio::test]
async fn test_partial_batch_releases_only_deleted_objects() {
    let (dir, store) = local_object_store().await;
    let cache = PinCache::new(store.clone(), u64::MAX, 1, []).await.unwrap();
    let mut files: Vec<_> = (0..DELETE_BATCH_SIZE)
        .map(|id| cache.account_existing(format!("missing-{id}.sst"), 1))
        .collect();
    let failed = "failed.sst";
    std::fs::create_dir(dir.path().join(failed)).unwrap();
    std::fs::write(dir.path().join(failed).join("child"), b"x").unwrap();
    files.push(cache.account_existing(failed.into(), 1));
    // The first batch succeeds; the second fails. Only acknowledged paths release capacity.
    let selection = PinCacheGcSelection {
        cache: cache.clone(),
        files,
    };
    assert!(selection.delete().await.is_err());
    assert_eq!(cache.storage.lock().accounted_bytes, 1);
    let state = cache.storage.lock();
    assert_eq!(state.files.len(), 1);
    assert!(state.files.contains_key(failed));
}

#[tokio::test]
async fn test_cancelled_gc_keeps_completed_batches_released() {
    for full in [false, true] {
        let store = in_memory_object_store();
        let cache = PinCache::new(store.clone(), u64::MAX, 1, []).await.unwrap();
        for id in 0..=DELETE_BATCH_SIZE {
            let path = format!("1001-{id}.sst");
            store.upload(&path, Bytes::from_static(b"x")).await.unwrap();
            let file = cache.account_existing(path, 1);
            if !full {
                cache.enqueue_delete(file);
            }
        }
        let selection = if full {
            cache
                .select_full(SystemTime::now() + Duration::from_secs(1))
                .await
                .unwrap()
        } else {
            cache.select_minor()
        };
        // Start with a fresh cooperative budget after fixture setup. InMem deletion is ready,
        // so the next Pending is the deletion's batch yield, with one file still pending.
        tokio::task::yield_now().await;
        let mut deletion = Box::pin(selection.delete());
        assert!(futures::poll!(deletion.as_mut()).is_pending());
        drop(deletion);
        let remaining: Vec<_> = store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(cache.storage.lock().accounted_bytes, 1);
        assert_eq!(cache.storage.lock().files.len(), 1);

        // A later pass collects the remainder without refunding the completed batch twice.
        cache
            .select_full(SystemTime::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .delete()
            .await
            .unwrap();
        assert_eq!(cache.storage.lock().accounted_bytes, 0);
        assert!(
            store
                .metadata(&remaining[0].key)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
    }
}
