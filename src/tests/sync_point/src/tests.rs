// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use itertools::Itertools;
use risingwave_common::util::sync_point;
use risingwave_common::util::sync_point::WaitForSignal;
use risingwave_rpc_client::HummockMetaClient;
use serial_test::serial;

use crate::test_utils::*;

/// With support of sync point, this test is executed in the following sequential order:
/// 1. Block compaction scheduler, thus no compaction will be automatically scheduled.
/// 2. Import data with risedev slt. The slt is modified so that it will not drop MVs in the end.
/// 3. Schedule exactly one compaction.
/// 4. Wait until compactor has uploaded its output to object store. It doesn't report task result
/// to meta, until we tell it to do so in step 6.
/// 5. Verify GC logic.
/// 6. Compactor reports task result to meta.
/// 7. Verify GC logic.
#[tokio::test]
#[serial]
async fn test_gc_watermark() {
    setup_env();

    let (join_handle, tx) = start_cluster().await;
    let object_store_client = get_object_store_client().await;
    let meta_client = get_meta_client().await;

    // Activate predefined sync points with customized actions
    sync_point::activate_sync_point(
        "BEFORE_COMPACT_REPORT",
        vec![
            sync_point::Action::EmitSignal("SIG_DONE_COMPACT_UPLOAD".to_owned()),
            sync_point::Action::WaitForSignal(WaitForSignal {
                signal: "SIG_START_COMPACT_REPORT".to_owned(),
                relay_signal: false,
                timeout: Duration::from_secs(3600),
            }),
        ],
        1,
    );
    sync_point::activate_sync_point(
        "AFTER_COMPACT_REPORT",
        vec![sync_point::Action::EmitSignal(
            "SIG_DONE_COMPACT_REPORT".to_owned(),
        )],
        1,
    );
    sync_point::activate_sync_point(
        "AFTER_REPORT_VACUUM",
        vec![sync_point::Action::EmitSignal(
            "SIG_DONE_REPORT_VACUUM".to_owned(),
        )],
        2,
    );
    sync_point::activate_sync_point(
        "AFTER_SCHEDULE_VACUUM",
        vec![sync_point::Action::EmitSignal(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
        )],
        u64::MAX,
    );
    // Block compaction scheduler so that we can control scheduling explicitly
    sync_point::activate_sync_point(
        "BEFORE_SCHEDULE_COMPACTION_TASK",
        vec![sync_point::Action::WaitForSignal(WaitForSignal {
            signal: "SIG_SCHEDULE_COMPACTION_TASK".to_owned(),
            relay_signal: false,
            timeout: Duration::from_secs(3600),
        })],
        u64::MAX,
    );

    // Import data
    let run_slt = run_slt();
    assert!(run_slt.status.success());

    let before_compaction = object_store_client.list("").await.unwrap();
    assert!(!before_compaction.is_empty());

    // Schedule a compaction task
    emit_now("SIG_SCHEDULE_COMPACTION_TASK".to_owned()).await;

    // Wait until SSTs have been written to object store
    wait_now(
        "SIG_DONE_COMPACT_UPLOAD".to_owned(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    let after_compaction_upload = object_store_client.list("").await.unwrap();
    let new_objects = after_compaction_upload
        .iter()
        .filter(|after| {
            !before_compaction
                .iter()
                .any(|before| before.key == after.key)
        })
        .cloned()
        .collect_vec();
    assert!(!new_objects.is_empty());

    // Upload a garbage object
    let sst_id = meta_client.get_new_sst_ids(1).await.unwrap().start_id;
    object_store_client
        .upload(
            &format!("{}/{}.data", get_object_store_bucket(), sst_id),
            bytes::Bytes::from(vec![1, 2, 3]),
        )
        .await
        .unwrap();
    let after_garbage_upload = object_store_client.list("").await.unwrap();
    assert_eq!(
        after_garbage_upload.len(),
        after_compaction_upload.len() + 1
    );

    meta_client.trigger_full_gc(0).await.unwrap();
    // Wait until VACUUM is scheduled and reported
    for _ in 0..2 {
        wait_now(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
    // Expect timeout aka no SST is deleted, because the garbage SST has greater id than watermark,
    // which is held by the on-going compaction.
    wait_now("SIG_DONE_REPORT_VACUUM".to_owned(), Duration::from_secs(10))
        .await
        .unwrap_err();
    let after_gc = object_store_client.list("").await.unwrap();
    assert_eq!(after_gc.len(), after_compaction_upload.len() + 1);

    // Signal to continue compaction report
    emit_now("SIG_START_COMPACT_REPORT".to_owned()).await;

    // Wait until SSts have been written to hummock version
    wait_now(
        "SIG_DONE_COMPACT_REPORT".to_owned(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    // Wait until VACUUM is scheduled and reported
    for _ in 0..2 {
        wait_now(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
    // Expect some stale SSTs as the result of compaction are deleted.
    wait_now("SIG_DONE_REPORT_VACUUM".to_owned(), Duration::from_secs(10))
        .await
        .unwrap();
    let after_gc = object_store_client.list("").await.unwrap();
    assert!(after_gc.len() < after_compaction_upload.len());

    meta_client.trigger_full_gc(0).await.unwrap();
    // Wait until VACUUM is scheduled and reported
    for _ in 0..2 {
        wait_now(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
    // Expect the garbage SST is deleted.
    wait_now("SIG_DONE_REPORT_VACUUM".to_owned(), Duration::from_secs(10))
        .await
        .unwrap();
    let after_gc_2 = object_store_client.list("").await.unwrap();
    assert_eq!(after_gc.len(), after_gc_2.len() + 1);

    stop_cluster(join_handle, tx).await;
}

#[tokio::test]
#[serial]
async fn test_gc_sst_retention_time() {
    setup_env();

    let (join_handle, tx) = start_cluster().await;
    let object_store_client = get_object_store_client().await;
    let meta_client = get_meta_client().await;

    sync_point::activate_sync_point(
        "AFTER_SCHEDULE_VACUUM",
        vec![sync_point::Action::EmitSignal(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
        )],
        u64::MAX,
    );
    // Activate predefined sync points with customized actions
    sync_point::activate_sync_point(
        "AFTER_REPORT_VACUUM",
        vec![sync_point::Action::EmitSignal(
            "SIG_DONE_REPORT_VACUUM".to_owned(),
        )],
        1,
    );

    let before_garbage_upload = object_store_client.list("").await.unwrap();
    assert_eq!(before_garbage_upload.len(), 0);

    // Upload a garbage object
    let sst_id = meta_client.get_new_sst_ids(1).await.unwrap().start_id;
    object_store_client
        .upload(
            &format!("{}/{}.data", get_object_store_bucket(), sst_id),
            bytes::Bytes::from(vec![1, 2, 3]),
        )
        .await
        .unwrap();
    let after_garbage_upload = object_store_client.list("").await.unwrap();
    assert_eq!(after_garbage_upload.len(), 1);

    // With large sst_retention_time
    meta_client.trigger_full_gc(3600).await.unwrap();
    // Wait until VACUUM is scheduled and reported
    for _ in 0..2 {
        wait_now(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
    // Expect timeout aka no SST is deleted, because all SSTs are within sst_retention_time, even
    // the garbage one.
    wait_now("SIG_DONE_REPORT_VACUUM".to_owned(), Duration::from_secs(10))
        .await
        .unwrap_err();
    let after_gc = object_store_client.list("").await.unwrap();
    // Garbage is not deleted.
    assert_eq!(after_gc, after_garbage_upload);

    // Ensure SST's last modified is less than now
    tokio::time::sleep(Duration::from_secs(1)).await;
    // With 0 sst_retention_time
    meta_client.trigger_full_gc(0).await.unwrap();
    // Wait until VACUUM is scheduled and reported
    for _ in 0..2 {
        wait_now(
            "SIG_DONE_SCHEDULE_VACUUM".to_owned(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    }
    wait_now("SIG_DONE_REPORT_VACUUM".to_owned(), Duration::from_secs(10))
        .await
        .unwrap();
    let after_gc = object_store_client.list("").await.unwrap();
    // Garbage is deleted.
    assert_eq!(after_gc, before_garbage_upload);

    stop_cluster(join_handle, tx).await;
}
