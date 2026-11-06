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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use risingwave_common::config::{RwConfig, extract_storage_memory_config};
use risingwave_common::system_param::system_params_for_test;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::ObjectError;

use super::PinCache;
use super::test_utils::{
    download_and_publish_for_test, in_memory_object_store, local_object_store, object_in_shard,
    publish_pin_cache,
};
use crate::opts::StorageOpts;

#[tokio::test]
#[should_panic(expected = "pin cache shard count must be greater than zero")]
async fn test_zero_shards_rejected() {
    PinCache::new(in_memory_object_store(), 0, [])
        .await
        .unwrap();
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_read_and_unregister_lifecycle() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store, 1, []).await.unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    let remote_path = "remote.sst";
    let original = Bytes::from_static(b"complete sst");
    remote_store
        .upload(remote_path, original.clone())
        .await
        .unwrap();

    pin_cache.register_objects([object_id]);
    download_and_publish_for_test(
        &pin_cache,
        remote_store.clone(),
        remote_path.to_owned(),
        object_id,
        original.len() as u64,
    )
    .await
    .unwrap();
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        original
    );
    let old_read = pin_cache.get(object_id).unwrap();
    let old_file = Arc::downgrade(&old_read.file);
    pin_cache.unregister_objects([object_id]);
    assert!(!pin_cache.is_registered(object_id));
    assert!(pin_cache.prepare_refill(object_id).is_none());
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(old_read.read(..).await.unwrap(), original);

    pin_cache.register_objects([object_id]);
    download_and_publish_for_test(
        &pin_cache,
        remote_store.clone(),
        remote_path.to_owned(),
        object_id,
        original.len() as u64,
    )
    .await
    .unwrap();
    let current = pin_cache.get(object_id).unwrap();
    assert!(!Arc::ptr_eq(&old_read.file, &current.file));
    assert_eq!(current.read(..).await.unwrap(), original);
    // Unregistering and registering the same object does not redirect an existing reader.
    assert_eq!(old_read.read(..).await.unwrap(), original);
    old_read.invalidate();
    assert!(pin_cache.get(object_id).is_some());
    drop(old_read);
    pin_cache.select_minor().delete().await.unwrap();
    assert!(old_file.upgrade().is_none());
    assert_eq!(current.read(..).await.unwrap(), original);
}

#[tokio::test]
async fn test_failed_download_can_be_retried() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 1, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([object_id]);
    let token = pin_cache.prepare_refill(object_id).unwrap();
    assert!(
        pin_cache
            .download(object_id, 8, remote_store.clone(), "sst".into())
            .await
            .is_err()
    );
    // Remote initialization failed before any local upload, so no GC is needed to retry.

    assert!(pin_cache.get(object_id).is_none());

    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let download = pin_cache
        .download(object_id, 8, remote_store, "sst".into())
        .await
        .unwrap();
    assert!(
        pin_cache.get(object_id).is_none(),
        "download alone must not publish"
    );
    assert!(pin_cache.publish(token, download));
    assert!(pin_cache.get(object_id).is_some());
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_completed_invalid_fs_upload_is_collected() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 1, []).await.unwrap();
    let remote_store = in_memory_object_store();
    remote_store
        .upload("sst", Bytes::from_static(b"half"))
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([object_id]);
    assert!(
        download_and_publish_for_test(&pin_cache, remote_store, "sst".into(), object_id, 8)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    // The completed failed file is handed to the existing minor GC.

    pin_cache.select_minor().delete().await.unwrap();

    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(files.iter().all(|file| file.key.ends_with('/')));
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_disk_space_rejection_precedes_local_io() {
    let (_dir, store) = local_object_store().await;
    let free = store.available_space().await.unwrap().unwrap();
    let cache = PinCache::new(store.clone(), 1, [(1001.into(), 8)])
        .await
        .unwrap();
    // No disk allocation is needed: this must be rejected before opening the missing remote.
    let size = free.checked_mul(2).unwrap().max(1 << 30);
    assert!(matches!(
        cache
            .download(
                1001.into(),
                size,
                in_memory_object_store(),
                "missing".into()
            )
            .await,
        Err(super::PinCacheDownloadError::CapacityRejected)
    ));

    assert!(cache.storage.lock().uploads.is_empty());
    assert!(
        store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .all(|file| file.key.ends_with('/'))
    );
    // Remote initialization failure also precedes any local I/O.
    assert!(
        cache
            .download(1001.into(), 8, in_memory_object_store(), "missing".into())
            .await
            .is_err()
    );

    assert!(cache.storage.lock().uploads.is_empty());
}

#[tokio::test]
async fn test_cancelled_reads_keep_file_until_producer_completes() {
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_default_test_sstable};
    use crate::hummock::{CachePolicy, SstableBlockIndex};
    use crate::monitor::StoreLocalStatistic;

    for prefetch in [false, true] {
        let store = mock_sstable_store().await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        let id = info.object_id;
        let (store, cache) = publish_pin_cache(store, id, in_memory_object_store()).await;
        let handle = cache.get(id).unwrap();
        let file = Arc::downgrade(&handle.file);
        let path = handle.file.path.clone();
        drop(handle);
        {
            let mut stats = StoreLocalStatistic::default();
            let mut read = Box::pin(async {
                if prefetch {
                    store
                        .prefetch_blocks(
                            &sst,
                            0,
                            sst.block_count(),
                            CachePolicy::default(),
                            &mut stats,
                        )
                        .await
                        .map(|_| ())
                } else {
                    store
                        .get(&sst, 0, CachePolicy::default(), &mut stats)
                        .await
                        .map(|_| ())
                }
            });
            assert!(futures::poll!(&mut read).is_pending());
            if !prefetch {
                // A cancelled follower must join without constructing another fetch.
                let handle = cache.get(id).unwrap();
                let follower = handle.get_or_fetch::<std::future::Pending<_>>(0, || {
                    panic!("a follower must not construct a fetch")
                });
                drop(follower);
            }
        }
        cache.unregister_objects([id]);
        // The caller is gone, but the unpolled producer must still protect its file from GC.
        cache.select_minor().delete().await.unwrap();
        assert!(cache.store.metadata(&path).await.is_ok());
        assert!(file.strong_count() > 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while file.strong_count() > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled readers must not strand a Pin producer");
        assert!(cache.shards[0].read_requests.lock().is_empty());
        if !prefetch {
            // A cancelled Fill waiter must not prevent the producer from filling memory.
            assert!(store.block_cache().memory().contains(&SstableBlockIndex {
                sst_id: id,
                block_idx: 0,
            }));
        }
        cache.select_minor().delete().await.unwrap();
        assert!(file.upgrade().is_none());
        assert!(cache.store.metadata(&path).await.is_err());
        assert_eq!(store.get_prefetch_memory_usage(), 0);
    }
}

#[cfg(not(madsim))]
#[test]
fn test_read_cleanup_on_runtime_shutdown() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let id = object_in_shard(2, 3);
        let cache = runtime.block_on(async {
            let store = in_memory_object_store();
            store
                .upload(
                    &format!("{}-1.sst", id.as_raw_id()),
                    Bytes::from_static(b"complete"),
                )
                .await
                .unwrap();
            PinCache::new(store, 3, [(id, 8)]).await.unwrap()
        });
        let read_handle = cache.get(id).unwrap();
        let runtime_handle = runtime.handle().clone();
        drop(runtime);
        let _entered = runtime_handle.enter();
        // Neither registration nor cleanup may lock another object's request shard.
        let _other_shard = cache.shards[0].read_requests.lock();
        {
            // A captured read handle also needs no membership lock in its own shard.
            let _state = cache.shards[2].state.write();
            assert!(
                futures::executor::block_on(read_handle.get_or_fetch(0, || {
                    assert!(cache.shard(id).read_requests.try_lock().is_some());
                    std::future::pending()
                }))
                .is_err()
            );
            assert!(cache.shards[2].read_requests.lock().is_empty());
        }
        // Runtime cancellation did not read the file and must not withdraw its publication.
        assert!(cache.get(id).is_some());
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("read cleanup must not deadlock during runtime shutdown");
    worker.join().unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_other_shard_does_not_block_object_operations() {
    let mut config = RwConfig::default();
    config.storage.cache.pin_cache_shard_num = 3;
    let system_params = system_params_for_test().into();
    let memory = extract_storage_memory_config(&config);
    let opts = StorageOpts::from((&config, &system_params, &memory));
    let cache = PinCache::new(in_memory_object_store(), opts.pin_cache_shard_num, [])
        .await
        .unwrap();
    assert_eq!(cache.shards.len(), 3);

    let blocked = object_in_shard(0, 3);
    let available = object_in_shard(2, 3);
    cache.register_objects([blocked, available]);
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();

    // Hold an unrelated shard until the other thread completes. On timeout, release it
    // before joining so an accidental cross-shard dependency fails instead of hanging.
    let result = std::thread::scope(|scope| {
        let shard = cache.shard(blocked).state.write();
        let (tx, rx) = std::sync::mpsc::channel();
        let cache = &cache;
        scope.spawn(move || {
            assert!(cache.is_registered(available));
            assert!(cache.get(available).is_none());
            let token = cache.prepare_refill(available).unwrap();
            let download = runtime
                .block_on(cache.download(available, 8, remote, "sst".into()))
                .unwrap();
            assert!(cache.publish(token, download));
            let token = cache.prepare_refill(available).unwrap();
            cache.revoke_refill(available);
            assert_ne!(cache.prepare_refill(available), Some(token));
            cache.get(available).unwrap().invalidate();
            assert!(cache.get(available).is_none());
            cache.unregister_objects([available]);
            assert!(!cache.is_registered(available));
            cache.register_objects([available]);
            assert!(cache.prepare_refill(available).is_some());
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(5));
        drop(shard);
        result
    });
    result.expect("an unrelated shard blocked the object lifecycle");
}

#[tokio::test]
async fn test_object_membership_across_shards() {
    let cache = PinCache::new(in_memory_object_store(), 3, [])
        .await
        .unwrap();
    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    cache.register_objects(objects);
    let tokens = objects.map(|id| cache.prepare_refill(id).unwrap());
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    for id in objects {
        download_and_publish_for_test(&cache, remote.clone(), "sst".into(), id, 8)
            .await
            .unwrap();
    }

    // Repeated registration, including duplicates in one batch, preserves the publication
    // and refill identity.
    let first = cache.get(objects[0]).unwrap();
    cache.register_objects([objects[0], objects[0]]);
    assert_eq!(cache.prepare_refill(objects[0]), Some(tokens[0]));
    assert!(Arc::ptr_eq(
        &first.file,
        &cache.get(objects[0]).unwrap().file
    ));

    // Unregistering one object leaves the other shard unchanged.
    cache.unregister_objects([objects[0]]);
    assert!(!cache.is_registered(objects[0]));
    assert!(cache.get(objects[0]).is_none());
    assert!(cache.prepare_refill(objects[0]).is_none());
    assert_eq!(
        first.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
    assert_eq!(cache.prepare_refill(objects[1]), Some(tokens[1]));
    assert!(cache.get(objects[1]).is_some());

    cache.register_objects([objects[0]]);
    assert_ne!(cache.prepare_refill(objects[0]), Some(tokens[0]));
    assert!(cache.get(objects[0]).is_none());
    cache.unregister_objects(objects);
    assert!(objects.iter().all(|&id| !cache.is_registered(id)));
    assert!(objects.iter().all(|&id| cache.prepare_refill(id).is_none()));
    assert!(objects.iter().all(|&id| cache.get(id).is_none()));
}

#[test]
fn test_parse_finalized_object_path() {
    assert_eq!(
        PinCache::parse_object_id("1001-42.sst"),
        Some(HummockSstableObjectId::from(1001))
    );
    assert_eq!(PinCache::parse_object_id("1001-recovered.sst"), None);
    assert_eq!(PinCache::parse_object_id("1001-42.tmp"), None);
    assert_eq!(PinCache::parse_object_id("nested/1001-42.sst"), None);
}

#[tokio::test]
async fn test_recovery_rejects_inventory_initialization_error() {
    let local_store = in_memory_object_store();
    let mut cache = PinCache::new(local_store.clone(), 1, []).await.unwrap();
    local_store
        .upload("1001-42.sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let error = Arc::get_mut(&mut cache)
        .unwrap()
        .recover_local_files(
            Err(ObjectError::internal("injected inventory failure")),
            &Default::default(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected inventory failure"));
    assert!(cache.get(1001.into()).is_none());
    assert!(local_store.metadata("1001-42.sst").await.is_ok());
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_recovery_returns_ready_routes_across_shards() {
    let (_dir, local) = local_object_store().await;
    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    for id in objects {
        local
            .upload(
                &format!("{}-1.sst", id.as_raw_id()),
                Bytes::from_static(b"complete"),
            )
            .await
            .unwrap();
    }
    let cache = PinCache::new(local.clone(), 3, objects.into_iter().map(|id| (id, 8)))
        .await
        .unwrap();

    for id in objects {
        assert!(cache.is_registered(id));
        assert_eq!(
            cache.get(id).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );
    }
}

#[tokio::test]
async fn test_gc_waits_for_readers_and_preserves_shutdown_files() {
    let local = in_memory_object_store();
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    for invalidate in [false, true] {
        let cache = PinCache::new(local.clone(), 1, [(1001.into(), 8)])
            .await
            .unwrap();
        download_and_publish_for_test(&cache, remote.clone(), "sst".into(), 1001.into(), 8)
            .await
            .unwrap();
        let reader = cache.get(1001.into()).unwrap();
        let path = reader.file.path.clone();
        if invalidate {
            reader.invalidate();
        } else {
            cache.unregister_objects([1001.into()]);
        }
        assert!(cache.get(1001.into()).is_none());
        cache.select_minor().delete().await.unwrap();
        cache
            .select_full(std::time::SystemTime::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .delete()
            .await
            .unwrap();
        assert_eq!(
            reader.read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );

        drop(reader);
        cache.select_minor().delete().await.unwrap();
        assert!(
            local
                .metadata(&path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
    }
    let cache = PinCache::new(local.clone(), 1, [(1001.into(), 8)])
        .await
        .unwrap();
    download_and_publish_for_test(&cache, remote, "sst".into(), 1001.into(), 8)
        .await
        .unwrap();
    let path = cache.get(1001.into()).unwrap().file.path.clone();
    drop(cache);
    tokio::task::yield_now().await;
    assert!(local.metadata(&path).await.is_ok());
    let recovered = PinCache::new(local, 1, [(1001.into(), 8)]).await.unwrap();
    assert!(recovered.get(1001.into()).is_some());
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_recovery_rejects_failed_short_download() {
    let (_dir, local) = local_object_store().await;
    let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
    cache.register_objects([1001.into(), 1002.into()]);
    let remote = in_memory_object_store();
    remote
        .upload("short", Bytes::from_static(b"half"))
        .await
        .unwrap();
    remote
        .upload("valid", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    assert!(
        cache
            .download(1001.into(), 8, remote.clone(), "short".into())
            .await
            .is_err()
    );
    download_and_publish_for_test(&cache, remote, "valid".into(), 1002.into(), 8)
        .await
        .unwrap();
    let files: Vec<_> = local
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(files.iter().any(|file| file.key.starts_with("1001-")
        && file.key.ends_with(".sst")
        && file.total_size == 4));
    // No GC runs between the failed download and recovery.
    drop(cache);
    let recovered = PinCache::new(local, 1, [(1001.into(), 8), (1002.into(), 8)])
        .await
        .unwrap();
    assert!(
        recovered.get(1001.into()).is_none(),
        "a failed short download must not become a recovered publication"
    );
    assert_eq!(
        recovered.get(1002.into()).unwrap().read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_disk_space_query_failure_does_not_start_local_io() {
    let (dir, local) = local_object_store().await;
    let cache = PinCache::new(local, 1, []).await.unwrap();
    cache.register_objects([1001.into()]);
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    std::fs::remove_dir_all(dir.path()).unwrap();
    let error = cache
        .download(1001.into(), 8, remote, "sst".into())
        .await
        .err()
        .unwrap();
    assert!(matches!(&error, super::PinCacheDownloadError::Io(_)));
    assert!(error.to_string().contains("filesystem space query failed"));
    assert!(
        !dir.path().exists(),
        "space-query failure must not recreate the upload root"
    );
    assert!(cache.storage.lock().uploads.is_empty());
    assert!(cache.get(1001.into()).is_none());
}
