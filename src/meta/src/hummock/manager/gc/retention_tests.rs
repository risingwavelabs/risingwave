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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use risingwave_backup::MetaBackupJobId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::id::{SchemaId, UserId};
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, gen_key_from_str};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::version::MAX_HUMMOCK_VERSION_ID;
use risingwave_hummock_sdk::{HummockObjectId, SyncResult, get_object_data_path};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::table::{self, HandleConflictBehavior, TableType};
use risingwave_pb::backup_service::BackupJobStatus;
use risingwave_rpc_client::HummockMetaClient;
use sea_orm::{ActiveModelTrait, Set, TransactionTrait};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

use super::GcManager;
use crate::MetaOpts;
use crate::backup_restore::{BackupManager, BackupManagerRef};
use crate::controller::catalog::CatalogController;
use crate::controller::streaming_job::TableChangeLogTruncateInfo;
use crate::hummock::manager::tests::setup_compute_env_with_meta_opts;
use crate::hummock::test_utils::{
    get_sst_ids, register_table_ids_to_compaction_group, to_local_sstable_info,
};
use crate::hummock::{HummockManager, HummockManagerRef, MockHummockMetaClient};

type GcHandoffGate = (oneshot::Sender<()>, oneshot::Receiver<()>);
static GC_HANDOFF_GATES: LazyLock<Mutex<HashMap<usize, GcHandoffGate>>> =
    LazyLock::new(Default::default);

pub(super) async fn pause_gc_retention_handoff(manager: &HummockManager) {
    let gate = GC_HANDOFF_GATES
        .lock()
        .unwrap()
        .remove(&(std::ptr::from_ref(manager) as usize));
    if let Some((entered, resume)) = gate {
        entered.send(()).unwrap();
        resume.await.unwrap();
    }
}

#[derive(Clone, Copy)]
enum GcKind {
    Full,
    Minor,
}

impl GcKind {
    async fn run(self, manager: &HummockManager, backup: BackupManagerRef) {
        match self {
            Self::Full => {
                manager
                    .start_full_gc(Duration::ZERO, None, Some(backup))
                    .await
            }
            Self::Minor => manager.try_start_minor_gc(backup).await,
        }
        .unwrap();
    }
}

#[tokio::test]
async fn test_full_gc_preserves_new_backup() {
    publish_while_gc_is_running(GcKind::Full, RetainedBy::Backup).await;
}

#[tokio::test]
async fn test_full_gc_preserves_new_time_travel_snapshot() {
    publish_while_gc_is_running(GcKind::Full, RetainedBy::TimeTravel).await;
}

#[tokio::test]
async fn test_minor_gc_preserves_new_backup() {
    publish_while_gc_is_running(GcKind::Minor, RetainedBy::Backup).await;
}

enum RetainedBy {
    Backup,
    TimeTravel,
}

async fn publish_while_gc_is_running(kind: GcKind, retained_by: RetainedBy) {
    let fixture = RetentionFixture::new(kind, false).await;
    let RetentionFixture {
        manager,
        client,
        backup,
        table_id,
        object_id,
        old_value_id,
    } = &fixture;
    if matches!(kind, GcKind::Minor) {
        fixture.truncate_changelog().await;
    }

    let (entered_tx, entered_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    GC_HANDOFF_GATES
        .lock()
        .unwrap()
        .insert(Arc::as_ptr(manager) as usize, (entered_tx, resume_rx));
    let gc_task = {
        let manager = manager.clone();
        let backup = backup.clone();
        tokio::spawn(async move { kind.run(&manager, backup).await })
    };
    timeout(Duration::from_secs(10), entered_rx)
        .await
        .expect("GC did not reach the retention handoff")
        .unwrap();

    let snapshot_epoch = test_epoch(2);
    let backup_job = match retained_by {
        RetainedBy::Backup => {
            let existing_safe_points = manager.context_info.read().await.version_safe_points.len();
            let job = backup.start_backup_job(None).await.unwrap();
            wait_for_backup(backup, job).await;
            // The backup's own safe point must be released before we checkpoint.
            timeout(Duration::from_secs(10), async {
                while manager.context_info.read().await.version_safe_points.len()
                    > existing_safe_points
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            Some(job)
        }
        RetainedBy::TimeTravel => {
            manager
                .env
                .system_params_manager_impl_ref()
                .set_param(
                    "time_travel_retention_ms",
                    Some((10 * 60 * 1000).to_string()),
                )
                .await
                .unwrap();
            client
                .commit_epoch(snapshot_epoch, SyncResult::default())
                .await
                .unwrap();
            None
        }
    };
    manager.unregister_table_ids([*table_id]).await.unwrap();
    manager.create_version_checkpoint(1).await.unwrap();
    assert!(
        !manager
            .get_current_version()
            .await
            .get_object_ids()
            .any(|id| id == *object_id)
    );

    resume_tx.send(()).unwrap();
    gc_task.await.unwrap();
    if backup_job.is_some() {
        assert!(
            backup
                .list_pinned_object_ids()
                .await
                .contains(&object_id.as_raw())
        );
    } else {
        assert!(
            manager
                .epoch_to_version(snapshot_epoch, *table_id)
                .await
                .unwrap()
                .get_object_ids()
                .any(|id| id == *object_id)
        );
    }

    let gc = &manager.gc_manager;
    let path = object_path(gc, *object_id);
    assert!(
        gc.store.metadata(&path).await.is_ok(),
        "GC deleted a retained object"
    );
    if let Some(old_value_id) = old_value_id {
        let old_path = object_path(gc, *old_value_id);
        assert!(
            gc.store.metadata(&old_path).await.is_err(),
            "minor GC did not reclaim an unretained old-value SST"
        );
    }
    // The full-GC batch must release its safe point so a later GC can reclaim the object.
    timeout(Duration::from_secs(10), async {
        while manager.get_min_pinned_version_id().await != MAX_HUMMOCK_VERSION_ID {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if let (GcKind::Full, Some(job)) = (kind, backup_job) {
        backup.delete_backups(&[job]).await.unwrap();
        kind.run(manager, backup.clone()).await;
        assert!(
            gc.store.metadata(&path).await.is_err(),
            "released object was never reclaimed"
        );
    }
}

#[tokio::test]
async fn test_minor_gc_preserves_old_value_during_backup() {
    gc_after_old_value_capture(GcKind::Minor, Publication::AfterGc).await;
}

#[tokio::test]
async fn test_full_gc_preserves_old_value_during_backup() {
    gc_after_old_value_capture(GcKind::Full, Publication::AfterGc).await;
}

#[tokio::test]
async fn test_minor_gc_preserves_published_backup_old_value() {
    gc_after_old_value_capture(GcKind::Minor, Publication::BeforeGc).await;
}

enum Publication {
    BeforeGc,
    AfterGc,
}

async fn gc_after_old_value_capture(kind: GcKind, publication: Publication) {
    let fixture = RetentionFixture::new(kind, true).await;
    let manager = &fixture.manager;
    let backup = &fixture.backup;
    let gc = &manager.gc_manager;
    let old_id = fixture.old_value_id.unwrap();
    let old_path = object_path(gc, old_id);
    assert!(gc.store.metadata(&old_path).await.is_ok());
    assert!(
        !manager
            .get_current_version()
            .await
            .get_object_ids()
            .any(|id| id == old_id)
    );

    let (captured_tx, captured_rx) = oneshot::channel();
    let (publish_tx, publish_rx) = oneshot::channel();
    *backup.snapshot_captured.lock().await = Some((captured_tx, publish_rx));
    let job = backup.start_backup_job(None).await.unwrap();
    timeout(Duration::from_secs(10), captured_rx)
        .await
        .unwrap()
        .unwrap();

    assert!(
        !backup
            .list_pinned_object_ids()
            .await
            .contains(&old_id.as_raw())
    );
    fixture.truncate_changelog().await;

    if matches!(publication, Publication::AfterGc) {
        kind.run(manager, backup.clone()).await;
    }
    publish_tx.send(()).unwrap();
    wait_for_backup(backup, job).await;
    if matches!(publication, Publication::BeforeGc) {
        kind.run(manager, backup.clone()).await;
    }

    assert!(
        backup
            .list_pinned_object_ids()
            .await
            .contains(&old_id.as_raw())
    );
    assert!(
        gc.store.metadata(&old_path).await.is_ok(),
        "successful backup references a deleted old-value SST"
    );
}

async fn wait_for_backup(backup: &BackupManager, job: MetaBackupJobId) {
    timeout(Duration::from_secs(10), async {
        loop {
            let (status, message) = backup.get_backup_job_status(job);
            match status {
                BackupJobStatus::Succeeded => break,
                BackupJobStatus::Failed => panic!("backup failed: {message}"),
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("backup did not finish");
}

struct RetentionFixture {
    manager: HummockManagerRef,
    client: MockHummockMetaClient,
    backup: BackupManagerRef,
    table_id: risingwave_common::catalog::TableId,
    object_id: HummockObjectId,
    old_value_id: Option<HummockObjectId>,
}

impl RetentionFixture {
    async fn new(kind: GcKind, capture_old_value: bool) -> Self {
        let mut opts = MetaOpts::test(false);
        opts.min_sst_retention_time_sec = 0;
        let (env, manager, _, worker_id) = setup_compute_env_with_meta_opts(80, opts).await;
        let client = MockHummockMetaClient::new(manager.clone(), worker_id as _);
        let txn = env.meta_store_ref().conn.begin().await.unwrap();
        let table_id = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            UserId::new(1),
            Some(SchemaId::new(2).as_object_id()),
        )
        .await
        .unwrap()
        .oid
        .as_table_id();
        table::ActiveModel {
            table_id: Set(table_id),
            name: Set("gc_retention_probe".to_owned()),
            optional_associated_source_id: Set(None),
            table_type: Set(TableType::Table),
            belongs_to_job_id: Set(None),
            columns: Set(vec![].into()),
            pk: Set(vec![].into()),
            distribution_key: Set(Vec::<i32>::new().into()),
            stream_key: Set(Vec::<i32>::new().into()),
            append_only: Set(false),
            fragment_id: Set(None),
            vnode_col_index: Set(None),
            row_id_index: Set(None),
            value_indices: Set(Vec::<i32>::new().into()),
            definition: Set(String::new()),
            handle_pk_conflict_behavior: Set(HandleConflictBehavior::NoCheck),
            version_column_indices: Set(None),
            read_prefix_len_hint: Set(0),
            watermark_indices: Set(Vec::<i32>::new().into()),
            dist_key_in_pk: Set(Vec::<i32>::new().into()),
            dml_fragment_id: Set(None),
            cardinality: Set(None),
            cleaned_by_watermark: Set(false),
            description: Set(None),
            version: Set(None),
            retention_seconds: Set(None),
            cdc_table_id: Set(None),
            vnode_count: Set(1),
            webhook_info: Set(None),
            engine: Set(None),
            clean_watermark_index_in_pk: Set(None),
            clean_watermark_indices: Set(None),
            refreshable: Set(false),
            vector_index_info: Set(None),
            cdc_table_type: Set(None),
        }
        .insert(&txn)
        .await
        .unwrap();
        txn.commit().await.unwrap();
        let group_id = StaticCompactionGroupId::StateDefault;
        register_table_ids_to_compaction_group(&manager, &[table_id], group_id).await;
        let epoch = test_epoch(1);
        let with_changelog = matches!(kind, GcKind::Minor) || capture_old_value;
        // Minor GC runs once the changelog has queued a real batch of candidates.
        let sst_count = match kind {
            GcKind::Minor => 1000,
            GcKind::Full if with_changelog => 2,
            GcKind::Full => 1,
        };
        let mut ssts = get_sst_ids(&manager, sst_count)
            .await
            .into_iter()
            .enumerate()
            .map(|(idx, sst_id)| {
                let key = |suffix| {
                    FullKey::new(
                        table_id,
                        gen_key_from_str(VirtualNode::ZERO, &format!("{idx:04}{suffix}")),
                        epoch,
                    )
                    .encode()
                    .into()
                };
                SstableInfoInner {
                    object_id: sst_id.into(),
                    sst_id: sst_id.into(),
                    key_range: KeyRange::new(key('a'), key('z')),
                    table_ids: vec![table_id],
                    file_size: 128,
                    sst_size: 128,
                    uncompressed_file_size: 128,
                    total_key_count: 1,
                    min_epoch: epoch,
                    max_epoch: epoch,
                    ..Default::default()
                }
                .into()
            })
            .collect::<Vec<SstableInfo>>();
        let gc = &manager.gc_manager;
        // Probe contents are irrelevant to ownership filtering; this test does not decode SSTs.
        for sst in &ssts {
            gc.store
                .upload(
                    &object_path(gc, HummockObjectId::Sstable(sst.object_id)),
                    bytes::Bytes::from_static(&[0; 128]),
                )
                .await
                .unwrap();
        }
        let object_id = HummockObjectId::Sstable(ssts[0].object_id);
        let old_value = with_changelog.then(|| ssts.pop().unwrap());
        client
            .commit_epoch_with_change_log(
                epoch,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&ssts),
                    old_value_ssts: to_local_sstable_info(old_value.as_slice()),
                    ..Default::default()
                },
                with_changelog.then_some(vec![epoch]),
            )
            .await
            .unwrap();
        if matches!(kind, GcKind::Full) {
            // Age the probe past the full GC clock. Channels force the interleavings.
            sleep(Duration::from_secs(2)).await;
        }
        let backup = Arc::new(BackupManager::for_test(env, manager.clone()).await);
        Self {
            manager,
            client,
            backup,
            table_id,
            object_id,
            old_value_id: old_value.map(|sst| HummockObjectId::Sstable(sst.object_id)),
        }
    }

    async fn truncate_changelog(&self) {
        self.manager
            .truncate_table_change_log(TableChangeLogTruncateInfo {
                subscription_retention_seconds: [(self.table_id, 60)].into(),
                committed_epoch_truncate_jobs: vec![],
                progress_table_subscribers: vec![],
            })
            .await
            .unwrap();
    }
}

fn object_path(gc: &GcManager, object_id: HummockObjectId) -> String {
    let prefix = gc.store.get_object_prefix(
        object_id.as_raw().as_raw_id(),
        gc.use_new_object_prefix_strategy,
    );
    get_object_data_path(&prefix, &gc.path_prefix, object_id)
}
