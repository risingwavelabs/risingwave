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
use futures::{StreamExt, TryStreamExt, stream};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

use super::{PinCache, PinCacheFile, write_file};
use crate::hummock::pin_cache::test_utils::{
    download_and_publish_for_test, in_memory_object_store, local_object_store,
};
use crate::monitor::ObjectStoreMetrics;

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_live_fs_upload_gc_protection_and_partial_remote_error_cleanup() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 1, []).await.unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.register_objects([object_id]);
    let file = PinCacheFile {
        path: "1001-0.sst".into(),
        size: 8,
    };
    let final_path = file.path.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
    let reader = MonitoredStreamingReader::new(
        "test",
        Box::pin(
            stream::iter([
                Ok(Bytes::from_static(b"half")),
                Ok(Bytes::from_static(b"x")),
            ])
            .chain(stream::once(async move {
                started_tx.send(()).unwrap();
                let _ = fail_rx.await;
                Err(ObjectError::internal("injected remote read failure"))
            })),
        ),
        Arc::new(ObjectStoreMetrics::unused()),
        None,
    );
    pin_cache.admit_upload(&file).await.unwrap();
    // Isolate a partial remote-read failure. Cancellation is covered by the real
    // executor tests, where the physical owner outlives canceled waiters.
    let upload_cache = pin_cache.clone();
    let task = tokio::spawn(async move { write_file(&upload_cache, &file, reader).await });
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    pin_cache.select_minor().delete().await.unwrap();
    pin_cache
        .select_full(std::time::SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files.iter().any(|file| file.total_size == 4),
        "a live upload's temporary file must survive GC"
    );
    fail_tx.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        local_store.metadata(&final_path).await.unwrap().total_size,
        5
    );
    assert!(pin_cache.get(object_id).is_none());
    assert!(pin_cache.storage.lock().uploads.is_empty());
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    // Leave the failed copy's bytes on disk while admitting the replacement.
    download_and_publish_for_test(&pin_cache, remote, "sst".into(), object_id, 8)
        .await
        .unwrap();
    let published = pin_cache.get(object_id).unwrap();
    pin_cache.select_minor().delete().await.unwrap();
    pin_cache
        .select_full(std::time::SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files
            .iter()
            .all(|file| file.key.ends_with('/') || file.key == published.file.path)
    );
    assert_eq!(
        published.read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[cfg_attr(madsim, ignore = "OpenDAL FS requires a native Tokio runtime")]
#[tokio::test]
async fn test_close_error_preserves_original_error_with_successful_or_failed_abort() {
    use risingwave_common::config::ObjectStoreConfig;

    use crate::hummock::pin_cache::test_utils::local_object_store_with_config;
    for abort_fails in [false, true] {
        let mut config = ObjectStoreConfig {
            upload_part_size: 1,
            ..Default::default()
        };
        config.retry.delete_objects_retry_attempts = 0;
        let (dir, local) = local_object_store_with_config(config).await;
        let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
        cache.register_objects([1001.into(), 1002.into()]);
        cache.set_available_space_for_test(|| Ok(Some(8)));
        let remote = in_memory_object_store();
        remote
            .upload("sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        let mut finish = cache.watch_upload_finish_for_test();
        let task = tokio::spawn({
            let cache = cache.clone();
            let remote = remote.clone();
            async move { cache.download(1001.into(), 8, remote, "sst".into()).await }
        });
        let (target, resume) = tokio::time::timeout(Duration::from_secs(5), finish.recv())
            .await
            .unwrap()
            .unwrap();
        let inventory: Vec<_> = local
            .list("atomic_write_dir/", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let prefix = format!("atomic_write_dir/{target}.");
        let actual = inventory
            .iter()
            .find(|m| m.key.starts_with(&prefix) && !m.key.ends_with('/'))
            .unwrap()
            .key
            .clone();
        std::fs::create_dir(dir.path().join(&target)).unwrap();
        if abort_fails {
            // Close and abort fail differently. Use the discovered backend name.
            std::fs::remove_file(dir.path().join(&actual)).unwrap();
            std::fs::create_dir(dir.path().join(&actual)).unwrap();
            for path in [&actual, &target] {
                std::fs::write(dir.path().join(path).join("blocked"), b"owned fault").unwrap();
            }
        }
        // The same failed rename can report EEXIST or ENOTEMPTY on different platforms.
        let close_error =
            std::fs::rename(dir.path().join(&actual), dir.path().join(&target)).unwrap_err();
        if abort_fails {
            let abort_error = std::fs::remove_file(dir.path().join(&actual)).unwrap_err();
            assert_ne!(close_error.kind(), abort_error.kind());
        }
        resume.send(()).unwrap();
        drop(finish);
        let error = match task.await.unwrap() {
            Err(error) => error,
            Ok(_) => panic!("close must fail"),
        };
        let io_error =
            std::iter::successors(Some(&error as &dyn std::error::Error), |e| e.source())
                .find_map(|e| e.downcast_ref::<std::io::Error>())
                .expect("close must preserve its underlying I/O error");
        assert_eq!(io_error.kind(), close_error.kind(), "{error}");
        assert_eq!(
            io_error.raw_os_error(),
            close_error.raw_os_error(),
            "{error}"
        );
        assert!(cache.get(1001.into()).is_none());
        assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), 8);
        if abort_fails {
            assert!(cache.select_minor().delete().await.is_err());
            assert!(cache.storage.lock().failed_uploads.contains(&target));
            assert!(matches!(
                cache.prepare_download(1002.into(), 1).await,
                Err(super::PinCacheDownloadError::CapacityRejected)
            ));
            for path in [&actual, &target] {
                std::fs::remove_file(dir.path().join(path).join("blocked")).unwrap();
                std::fs::remove_dir(dir.path().join(path)).unwrap();
            }
            // A later minor rediscovers this actual random name after the fault clears.
            std::fs::write(dir.path().join(&actual), b"leftover").unwrap();
        } else {
            assert!(
                !dir.path().join(&actual).exists(),
                "abort must remove its atomic temp"
            );
            std::fs::remove_dir(dir.path().join(&target)).unwrap();
        }
        cache.select_minor().delete().await.unwrap();
        assert!(cache.storage.lock().failed_uploads.is_empty());
        assert!(cache.storage.lock().uploads.is_empty());
        assert!(!dir.path().join(&actual).exists());
        assert!(!dir.path().join(&target).exists());
        download_and_publish_for_test(&cache, remote, "sst".into(), 1001.into(), 8)
            .await
            .unwrap();
        assert_eq!(
            cache.get(1001.into()).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );
    }
}

#[cfg(not(madsim))]
#[tokio::test]
#[ignore = "requires an owned bounded filesystem in RW_PIN_ENOSPC_MOUNT"]
async fn test_real_enospc_after_admission_cleans_failed_upload() {
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;

    use risingwave_common::config::ObjectStoreConfig;
    use risingwave_object_store::object::build_remote_object_store;

    use crate::monitor::ObjectStoreMetrics;

    let mount =
        PathBuf::from(std::env::var_os("RW_PIN_ENOSPC_MOUNT").expect("owned mount required"));
    assert_eq!(
        std::fs::read(mount.join("pin-enospc-owned")).unwrap(),
        b"pin-root-fix-test"
    );
    assert_ne!(
        mount.metadata().unwrap().dev(),
        mount.parent().unwrap().metadata().unwrap().dev(),
        "never fill the host filesystem"
    );
    let dir = tempfile::tempdir_in(&mount).unwrap();
    let mut config = ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    };
    config.set_atomic_write_dir();
    let local = Arc::new(
        build_remote_object_store(
            &format!("fs://{}", dir.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "bounded ENOSPC test",
            Arc::new(config),
        )
        .await,
    );
    let initial = local.available_space().await.unwrap().unwrap();
    assert!(
        (16 * 1024 * 1024..96 * 1024 * 1024).contains(&initial),
        "bounded filesystem required: {initial}"
    );
    let cache = PinCache::new(local.clone(), 1, []).await.unwrap();
    cache.register_objects([1001.into()]);
    let remote = in_memory_object_store();
    let bytes = Bytes::from(vec![7; 8 * 1024 * 1024]);
    remote.upload("sst", bytes.clone()).await.unwrap();
    // Admission succeeds before an independent disk user consumes physical space.
    let prepared = cache
        .prepare_download(1001.into(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let filler_path = mount.join("owned-external-filler");
    let mut filler = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&filler_path)
        .unwrap();
    let block = vec![1; 1024 * 1024];
    let mut filled = 0;
    while local.available_space().await.unwrap().unwrap() > 1024 * 1024 {
        assert!(filled < 96 * 1024 * 1024, "bounded filler limit");
        if let Err(error) = filler.write_all(&block) {
            assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
            break;
        }
        filled += block.len();
    }
    drop(filler);
    println!(
        "bounded ENOSPC: initial_free={initial}, external_filler={filled}, remaining_free={}",
        local.available_space().await.unwrap().unwrap()
    );
    let task = tokio::spawn(prepared.run(remote.clone(), "sst".into()));
    let error = match tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
    {
        Err(error) => error,
        Ok(_) => panic!("external writer must cause ENOSPC"),
    };
    assert!(
        (error.to_string().to_lowercase().contains("no space")
            || error
                .to_string()
                .to_lowercase()
                .contains("no storage space")),
        "original error: {error}"
    );
    assert_eq!(
        cache.storage.lock().uploads.values().sum::<u64>(),
        8 * 1024 * 1024
    );
    cache.select_minor().delete().await.unwrap();
    assert!(cache.storage.lock().uploads.is_empty());
    let files: Vec<_> = local
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        files.iter().all(|f| f.key.ends_with('/')),
        "known failed paths remain: {files:?}"
    );
    std::fs::remove_file(filler_path).unwrap();
    download_and_publish_for_test(
        &cache,
        remote,
        "sst".into(),
        1001.into(),
        bytes.len() as u64,
    )
    .await
    .unwrap();
    assert_eq!(
        cache.get(1001.into()).unwrap().read(..).await.unwrap(),
        bytes
    );
    println!(
        "bounded ENOSPC: original error preserved, minor cleanup confirmed, explicit retry published"
    );
}
