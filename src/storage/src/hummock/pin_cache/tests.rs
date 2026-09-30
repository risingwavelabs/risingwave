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
use risingwave_common::config::{ObjectStoreConfig, RwConfig, extract_storage_memory_config};
use risingwave_common::system_param::system_params_for_test;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectResult, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
    build_remote_object_store,
};

use super::{PinCache, PinCacheRefillOutcome};
use crate::monitor::ObjectStoreMetrics;
use crate::opts::StorageOpts;

impl PinCache {
    pub(crate) async fn pin_sst(
        self: &Arc<Self>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let token = self
            .prepare_refill(object_id)
            .expect("test object must be needed");
        self.refill(token, remote_store, remote_path).await
    }
}

pub(super) fn in_memory_object_store() -> ObjectStoreRef {
    Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::for_test().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    ))
}

pub(super) async fn local_object_store() -> (tempfile::TempDir, ObjectStoreRef) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    };
    config.set_atomic_write_dir();
    let store = Arc::new(
        build_remote_object_store(
            &format!("fs://{}", dir.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "test pin cache",
            Arc::new(config),
        )
        .await,
    );
    (dir, store)
}

#[test]
#[should_panic(expected = "pin cache shard count must be greater than zero")]
fn test_zero_shards_rejected() {
    PinCache::new(in_memory_object_store(), 0, []);
}

#[tokio::test]
async fn test_read_and_unregister_lifecycle() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store, 1, []);
    let object_id = HummockSstableObjectId::from(1001);
    let remote_path = "remote.sst";
    let original = Bytes::from_static(b"complete sst");
    remote_store
        .upload(remote_path, original.clone())
        .await
        .unwrap();

    pin_cache.register_objects([(object_id, original.len() as u64)]);
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert_eq!(
        pin_cache
            .pin_sst(in_memory_object_store(), "unused".into(), object_id)
            .await
            .unwrap(),
        PinCacheRefillOutcome::AlreadyPublished
    );
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
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
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
async fn test_revoked_token_cannot_begin_a_late_download() {
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let object = HummockSstableObjectId::from(911);
    for revoke_by_unregister in [false, true] {
        let cache = PinCache::new(in_memory_object_store(), 1, []);
        cache.register_objects([(object, 8)]);
        let token = cache.prepare_refill(object).unwrap();
        assert_eq!(token.object_id(), object);

        if revoke_by_unregister {
            cache.unregister_objects([object]);
            cache.register_objects([(object, 8)]);
        } else {
            cache.revoke_refill(object);
        }
        let replacement = cache.prepare_refill(object).unwrap();
        assert_eq!(
            cache
                .refill(token, remote.clone(), "sst".into())
                .await
                .unwrap(),
            PinCacheRefillOutcome::Obsolete
        );
        assert_eq!(
            cache
                .refill(replacement, remote.clone(), "sst".into())
                .await
                .unwrap(),
            PinCacheRefillOutcome::Published
        );
    }
}

#[tokio::test]
async fn test_failed_download_can_be_retried() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 1, []);
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    let token = pin_cache.prepare_refill(object_id).unwrap();
    assert!(
        pin_cache
            .refill(token, remote_store.clone(), "sst".into())
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());

    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    assert_eq!(
        pin_cache
            .refill(token, remote_store, "sst".into())
            .await
            .unwrap(),
        PinCacheRefillOutcome::Published
    );
    assert!(pin_cache.get(object_id).is_some());
}

#[tokio::test]
async fn test_completed_invalid_fs_upload_cannot_publish() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 1, []);
    let remote_store = in_memory_object_store();
    remote_store
        .upload("sst", Bytes::from_static(b"half"))
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    assert!(
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
}

#[tokio::test]
async fn test_read_failure_only_invalidates_selected_publication() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 1, []);
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([(object_id, 8)]);
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store.clone(), "sst".into(), object_id)
        .await
        .unwrap();
    let old = pin_cache.get(object_id).unwrap();
    pin_cache.store.delete(&old.file.path).await.unwrap();
    assert!(old.read(..).await.is_err());
    assert!(pin_cache.get(object_id).is_none());

    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    // This handle stays on the old path and must not remove the new route.
    assert!(old.read(..).await.is_err());
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

#[tokio::test]
async fn test_other_shard_does_not_block_object_operations() {
    let mut config = RwConfig::default();
    config.storage.cache.pin_cache_shard_num = 3;
    let system_params = system_params_for_test().into();
    let memory = extract_storage_memory_config(&config);
    let opts = StorageOpts::from((&config, &system_params, &memory));
    let cache = PinCache::new(in_memory_object_store(), opts.pin_cache_shard_num, []);
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
            assert_eq!(
                runtime
                    .block_on(cache.refill(token, remote, "sst".into()))
                    .unwrap(),
                PinCacheRefillOutcome::Published
            );
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
    let cache = PinCache::new(in_memory_object_store(), 3, []);
    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    cache.register_objects(objects.into_iter().map(|id| (id, 8)));
    let tokens = objects.map(|id| cache.prepare_refill(id).unwrap());
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    for id in objects {
        assert_eq!(
            cache
                .pin_sst(remote.clone(), "sst".into(), id)
                .await
                .unwrap(),
            PinCacheRefillOutcome::Published
        );
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
