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
use risingwave_common::config::{RwConfig, extract_storage_memory_config};
use risingwave_common::system_param::system_params_for_test;

use super::PinCache;
use super::test_utils::{
    in_memory_object_store, install_file_for_test, local_object_store, object_in_shard,
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
    let (_dir, store) = local_object_store().await;
    let cache = PinCache::new(store, 1, [1001.into()]).await.unwrap();
    assert!(cache.is_registered(1001.into()));
    assert!(cache.get(1001.into()).is_none());
    let bytes = Bytes::from_static(b"complete sst");
    install_file_for_test(&cache, 1001.into(), bytes.clone()).await;
    let reader = cache.get(1001.into()).unwrap();
    assert_eq!(reader.file.path, "1001.sst");
    assert_eq!(reader.read(..).await.unwrap(), bytes);
    assert_eq!(
        reader.read(0..8).await.unwrap(),
        Bytes::from_static(b"complete")
    );
    cache.unregister_objects([1001.into()]);
    assert!(!cache.is_registered(1001.into()));
    assert!(cache.get(1001.into()).is_none());
    assert_eq!(reader.read(..).await.unwrap(), bytes);
    cache.register_objects([1001.into()]);
    assert!(cache.get(1001.into()).is_none());
    reader.invalidate();
    assert!(cache.is_registered(1001.into()));
    assert!(cache.get(1001.into()).is_none());
}

#[tokio::test]
async fn test_read_failure_withdraws_route_without_unregistering() {
    let cache = PinCache::new(in_memory_object_store(), 1, [1001.into()])
        .await
        .unwrap();
    install_file_for_test(&cache, 1001.into(), Bytes::from_static(b"complete")).await;
    let reader = cache.get(1001.into()).unwrap();
    cache.store.delete(&reader.file.path).await.unwrap();
    assert!(reader.read(..).await.is_err());
    assert!(cache.get(1001.into()).is_none());
    assert!(cache.is_registered(1001.into()));
    cache.register_objects([1001.into()]);
    assert!(cache.get(1001.into()).is_none());
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
    install_file_for_test(&cache, available, Bytes::from_static(b"complete")).await;
    let result = std::thread::scope(|scope| {
        let shard = cache.shard(blocked).write();
        let (tx, rx) = std::sync::mpsc::channel();
        let cache = &cache;
        scope.spawn(move || {
            assert!(cache.is_registered(available));
            cache.get(available).unwrap().invalidate();
            assert!(cache.get(available).is_none());
            cache.unregister_objects([available]);
            assert!(!cache.is_registered(available));
            cache.register_objects([available]);
            assert!(cache.is_registered(available));
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
    for id in objects {
        install_file_for_test(&cache, id, Bytes::from_static(b"complete")).await;
    }
    let first = cache.get(objects[0]).unwrap();
    cache.register_objects([objects[0], objects[0]]);
    assert!(Arc::ptr_eq(
        &first.file,
        &cache.get(objects[0]).unwrap().file
    ));
    cache.unregister_objects([objects[0]]);
    assert!(!cache.is_registered(objects[0]));
    assert!(cache.get(objects[0]).is_none());
    assert_eq!(
        first.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
    assert!(cache.get(objects[1]).is_some());
    cache.register_objects([objects[0]]);
    assert!(cache.get(objects[0]).is_none());
    cache.unregister_objects(objects);
    assert!(objects.iter().all(|&id| !cache.is_registered(id)));
    assert!(objects.iter().all(|&id| cache.get(id).is_none()));
}
