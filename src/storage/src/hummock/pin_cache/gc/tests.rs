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
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use super::super::test_utils::{in_memory_object_store, local_object_store};
use super::executor::DELETE_BATCH_SIZE;
use super::{Candidate, PinCacheGc, PinCacheGcSelection};

pub(in crate::hummock::pin_cache) fn accounted_bytes(gc: &PinCacheGc) -> u64 {
    gc.state.lock().accounted_bytes
}

#[tokio::test]
async fn test_selection_protects_files_and_can_be_abandoned() {
    let store = in_memory_object_store();
    let gc = Arc::new(PinCacheGc::new(store.clone(), 8));
    let file = gc.try_reserve("1001-1.sst".into(), 8).unwrap();
    store
        .upload(&file.path, Bytes::from_static(b"complete"))
        .await
        .unwrap();
    gc.complete_upload(&file);
    assert!(gc.select_minor().candidates.is_empty());
    drop(file);
    let selected = gc.select_minor();
    assert_eq!(selected.candidates.len(), 1);
    assert!(gc.select_minor().candidates.is_empty());
    assert!(
        gc.select_full(SystemTime::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    assert!(gc.try_reserve("1002-2.sst".into(), 1).is_err());
    drop(selected);
    assert_eq!(accounted_bytes(&gc), 8);
    gc.select_minor().delete().await.unwrap();
    assert_eq!(accounted_bytes(&gc), 0);
    assert!(
        store
            .metadata("1001-1.sst")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[tokio::test]
async fn test_failed_deletion_keeps_capacity_and_full_gc_retries() {
    let (dir, store) = local_object_store().await;
    let gc = Arc::new(PinCacheGc::new(store.clone(), 8));
    let path = "1001-42.sst";
    // A nonempty directory makes deletion fail even when the test runs as root.
    std::fs::create_dir(dir.path().join(path)).unwrap();
    std::fs::write(dir.path().join(path).join("child"), b"complete").unwrap();
    drop(gc.account_existing(path.into(), 8));
    assert!(gc.select_minor().delete().await.is_err());
    assert_eq!(accounted_bytes(&gc), 8);
    std::fs::remove_file(dir.path().join(path).join("child")).unwrap();
    std::fs::remove_dir(dir.path().join(path)).unwrap();
    // Missing files are idempotent deletes, and the reservation is released only once.
    gc.select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(accounted_bytes(&gc), 0);
    gc.select_minor().delete().await.unwrap();
    assert_eq!(accounted_bytes(&gc), 0);
}

#[tokio::test]
async fn test_full_gc_respects_retention_and_discovers_zombies() {
    let store = in_memory_object_store();
    let gc = Arc::new(PinCacheGc::new(store.clone(), 8));
    store
        .upload("orphan.tmp", Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    assert!(
        gc.select_full(SystemTime::UNIX_EPOCH)
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    let selection = gc
        .select_full(SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(selection.candidates.len(), 1);
    assert_eq!(accounted_bytes(&gc), 6);
    selection.delete().await.unwrap();
    assert_eq!(accounted_bytes(&gc), 0);
    assert!(
        store
            .metadata("orphan.tmp")
            .await
            .unwrap_err()
            .is_object_not_found_error()
    );
}

#[tokio::test]
async fn test_partial_batch_keeps_the_upload_reservation() {
    let (dir, store) = local_object_store().await;
    let gc = Arc::new(PinCacheGc::new(store.clone(), 11));
    let complete = gc.account_existing("complete.sst".into(), 3);
    let upload = gc.try_reserve("upload.sst".into(), 8).unwrap();
    let mut upload_paths: Vec<_> = (0..DELETE_BATCH_SIZE - 1)
        .map(|id| format!("missing-{id}.tmp"))
        .collect();
    let failed = "failed.sst";
    std::fs::create_dir(dir.path().join(failed)).unwrap();
    std::fs::write(dir.path().join(failed).join("child"), b"x").unwrap();
    upload_paths.push(failed.into());
    // The first delete batch covers the complete file and only part of the upload family.
    // The second batch fails. Only the complete file's three bytes can be returned.
    let selection = PinCacheGcSelection {
        gc: gc.clone(),
        candidates: vec![
            Candidate {
                paths: vec![complete.path.clone()],
                file: complete,
            },
            Candidate {
                file: upload,
                paths: upload_paths,
            },
        ],
    };
    assert!(selection.delete().await.is_err());
    assert_eq!(accounted_bytes(&gc), 8);
    assert!(!gc.state.lock().files.contains_key("complete.sst"));
    assert!(gc.state.lock().files.contains_key("upload.sst"));
}
