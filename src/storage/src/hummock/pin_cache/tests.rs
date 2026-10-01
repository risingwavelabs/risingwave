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
use super::gc::tests::{accounted_bytes, wait_for_reclaim};
use super::test_utils::{
    download_and_publish_for_test, in_memory_object_store, local_object_store, object_in_shard,
};
use crate::opts::StorageOpts;

#[tokio::test]
#[should_panic(expected = "pin cache shard count must be greater than zero")]
async fn test_zero_shards_rejected() {
    PinCache::new(in_memory_object_store(), u64::MAX, 0, 2, [])
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "pin cache recovery concurrency must be greater than zero")]
async fn test_zero_recovery_concurrency_is_rejected() {
    PinCache::new(in_memory_object_store(), u64::MAX, 1, 0, [])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_read_and_unregister_lifecycle() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store, u64::MAX, 1, 2, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    let remote_path = "remote.sst";
    let original = Bytes::from_static(b"complete sst");
    remote_store
        .upload(remote_path, original.clone())
        .await
        .unwrap();

    pin_cache.register_objects([(object_id, original.len() as u64)]);
    download_and_publish_for_test(
        &pin_cache,
        remote_store.clone(),
        remote_path.to_owned(),
        object_id,
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

    pin_cache.register_objects([(object_id, original.len() as u64)]);
    download_and_publish_for_test(
        &pin_cache,
        remote_store.clone(),
        remote_path.to_owned(),
        object_id,
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
    assert!(old_file.upgrade().is_none());
}

#[tokio::test]
async fn test_failed_download_can_be_retried() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 8, 1, 2, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    let token = pin_cache.prepare_refill(object_id).unwrap();
    assert!(
        pin_cache
            .download(object_id, 8, remote_store.clone(), "sst".into())
            .await
            .is_err()
    );
    // Remote initialization failed before any local upload, so no GC is needed to retry.
    assert_eq!(accounted_bytes(&pin_cache.gc), 0);
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

#[tokio::test]
async fn test_completed_invalid_fs_upload_reclaims_capacity() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 8, 1, 2, [])
        .await
        .unwrap();
    let remote_store = in_memory_object_store();
    remote_store
        .upload("sst", Bytes::from_static(b"half"))
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    assert!(
        download_and_publish_for_test(&pin_cache, remote_store, "sst".into(), object_id)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    wait_for_reclaim(&pin_cache).await;
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(files.iter().all(|file| file.key.ends_with('/')));
}

#[tokio::test]
async fn test_read_failure_only_invalidates_selected_publication() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, 2, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    download_and_publish_for_test(&pin_cache, remote_store.clone(), "sst".into(), object_id)
        .await
        .unwrap();
    let old = pin_cache.get(object_id).unwrap();
    pin_cache.store.delete(&old.file.path).await.unwrap();
    assert!(old.read(..).await.is_err());
    assert!(pin_cache.get(object_id).is_none());

    download_and_publish_for_test(&pin_cache, remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    // This handle stays on the old path and must not remove the new route.
    assert!(old.read(..).await.is_err());
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[tokio::test]
async fn test_other_shard_does_not_block_object_operations() {
    let mut config = RwConfig::default();
    config.storage.cache.pin_cache_shard_num = 3;
    config.storage.cache.pin_cache_recover_concurrency = 2;
    let system_params = system_params_for_test().into();
    let memory = extract_storage_memory_config(&config);
    let opts = StorageOpts::from((&config, &system_params, &memory));
    let cache = PinCache::new(
        in_memory_object_store(),
        u64::MAX,
        opts.pin_cache_shard_num,
        opts.pin_cache_recover_concurrency,
        [],
    )
    .await
    .unwrap();
    assert_eq!(cache.shards.len(), 3);

    let blocked = object_in_shard(0, 3);
    let available = object_in_shard(2, 3);
    cache.register_objects([(blocked, 8), (available, 8)]);
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();

    // Hold an unrelated shard until the other thread completes. On timeout, release it
    // before joining so an accidental cross-shard dependency fails instead of hanging.
    let result = std::thread::scope(|scope| {
        let shard = cache.shard(blocked).write();
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
            cache.register_objects([(available, 8)]);
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
    let cache = PinCache::new(in_memory_object_store(), u64::MAX, 3, 2, [])
        .await
        .unwrap();
    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    cache.register_objects(objects.into_iter().map(|id| (id, 8)));
    let tokens = objects.map(|id| cache.prepare_refill(id).unwrap());
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    for id in objects {
        download_and_publish_for_test(&cache, remote.clone(), "sst".into(), id)
            .await
            .unwrap();
    }

    // Repeated registration preserves the publication and refill identity.
    let first = cache.get(objects[0]).unwrap();
    cache.register_objects([(objects[0], 8)]);
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

    cache.register_objects([(objects[0], 8)]);
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
    let mut cache = PinCache::new(local_store.clone(), u64::MAX, 1, 2, [])
        .await
        .unwrap();
    local_store
        .upload("1001-42.sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let error = Arc::get_mut(&mut cache)
        .unwrap()
        .recover_local_files(Err(ObjectError::internal("injected inventory failure")), 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected inventory failure"));
    assert!(cache.get(1001.into()).is_none());
    assert!(local_store.metadata("1001-42.sst").await.is_ok());
}

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
    let cache = PinCache::new(
        local.clone(),
        u64::MAX,
        3,
        2,
        objects.into_iter().map(|id| (id, 8)),
    )
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
async fn test_recovery_reclaims_files_outside_initial_membership() {
    let local_store = in_memory_object_store();
    for path in ["1001-42.sst", "unfinished.tmp"] {
        local_store
            .upload(path, Bytes::from_static(b"stale"))
            .await
            .unwrap();
    }
    let pin_cache = PinCache::new(local_store.clone(), 1024, 1, 2, [])
        .await
        .unwrap();
    wait_for_reclaim(&pin_cache).await;
    for path in ["1001-42.sst", "unfinished.tmp"] {
        assert!(
            local_store
                .metadata(path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
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
        let cache = PinCache::new(local.clone(), 8, 1, 2, [(1001.into(), 8)])
            .await
            .unwrap();
        download_and_publish_for_test(&cache, remote.clone(), "sst".into(), 1001.into())
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
        tokio::task::yield_now().await;
        assert_eq!(
            reader.read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );
        assert_eq!(accounted_bytes(&cache.gc), 8);
        drop(reader);
        wait_for_reclaim(&cache).await;
        assert!(
            local
                .metadata(&path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
    }
    let cache = PinCache::new(local.clone(), 8, 1, 2, [(1001.into(), 8)])
        .await
        .unwrap();
    download_and_publish_for_test(&cache, remote, "sst".into(), 1001.into())
        .await
        .unwrap();
    let path = cache.get(1001.into()).unwrap().file.path.clone();
    drop(cache);
    tokio::task::yield_now().await;
    assert!(local.metadata(&path).await.is_ok());
    let recovered = PinCache::new(local, 8, 1, 2, [(1001.into(), 8)])
        .await
        .unwrap();
    assert!(recovered.get(1001.into()).is_some());
}
