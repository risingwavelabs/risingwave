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
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use prometheus::Registry;
use risingwave_common::catalog::TableId;
use risingwave_common::id::{JobId, SchemaId, SinkId, UserId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::{CreateType, JobStatus, StreamingParallelism, sink, streaming_job};
use risingwave_pb::catalog::{PbSink, PbSinkType};
use sea_orm::{ActiveModelTrait, Set, TransactionTrait};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;

use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::{BarrierScheduler, Command};
use crate::controller::catalog::CatalogController;
use crate::hummock::IcebergCompactorManager;
use crate::hummock::test_utils::{register_table_ids_to_compaction_group, setup_compute_env};
use crate::manager::iceberg_compaction::IcebergCompactionManager;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::notification::Notifier;
use crate::rpc::metrics::MetaMetrics;
use crate::stream::{GlobalRefreshManager, GlobalStreamManager, ScaleController, SourceManager};

type StopRequests = UnboundedReceiver<(Vec<JobId>, Notifier)>;

async fn test_manager() -> (Arc<GlobalStreamManager>, ScheduledBarriers, StopRequests) {
    let (env, hummock_manager, _, _) = setup_compute_env(1234).await;
    let metadata_manager = hummock_manager.metadata_manager().clone();
    let (barrier_scheduler, scheduled) = BarrierScheduler::new_pair(hummock_manager.clone());
    let metrics = Arc::new(MetaMetrics::for_test(&Registry::new()));
    let source_manager = Arc::new(
        SourceManager::new(
            barrier_scheduler.clone(),
            metadata_manager.clone(),
            metrics.clone(),
            env.clone(),
        )
        .await
        .unwrap(),
    );
    let scale_controller = Arc::new(ScaleController::new(
        &metadata_manager,
        source_manager.clone(),
        env.clone(),
    ));
    let (refresh_manager, refresh_task, refresh_shutdown) = GlobalRefreshManager::start(
        metadata_manager.clone(),
        barrier_scheduler.clone(),
        scale_controller.clone(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    refresh_shutdown.send(()).unwrap();
    refresh_task.await.unwrap();
    let (compaction_manager, _) = IcebergCompactionManager::build(
        env.clone(),
        metadata_manager.clone(),
        Arc::new(IcebergCompactorManager::new()),
        metrics,
        barrier_scheduler.clone(),
    );
    let iceberg_pk_index_sink_manager = IcebergPkIndexSinkManager::new(
        metadata_manager.catalog_controller.clone(),
        compaction_manager,
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let (sink_manager, stop_requests) = SinkCoordinatorManager::for_test();
    let manager = GlobalStreamManager::new(
        env,
        metadata_manager,
        barrier_scheduler,
        hummock_manager,
        source_manager,
        refresh_manager,
        sink_manager,
        iceberg_pk_index_sink_manager,
        scale_controller,
    )
    .unwrap();
    (Arc::new(manager), scheduled, stop_requests)
}

async fn insert_sink(manager: &GlobalStreamManager, status: JobStatus) -> SinkId {
    let catalog = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;
    let txn = catalog.db.begin().await.unwrap();
    let object = CatalogController::create_object(
        &txn,
        ObjectType::Sink,
        UserId::new(1),
        Some(SchemaId::new(2).as_object_id()),
    )
    .await
    .unwrap();
    let sink_id = object.oid.as_sink_id();
    sink::ActiveModel::from(PbSink {
        id: sink_id,
        name: format!("test_sink_{sink_id}"),
        schema_id: 2.into(),
        database_id: object.database_id.unwrap(),
        owner: 1.into(),
        sink_type: PbSinkType::AppendOnly as i32,
        ..Default::default()
    })
    .insert(&txn)
    .await
    .unwrap();
    streaming_job::ActiveModel {
        job_id: Set(sink_id.as_job_id()),
        job_status: Set(status),
        create_type: Set(CreateType::Foreground),
        parallelism: Set(StreamingParallelism::Adaptive),
        max_parallelism: Set(1),
        is_serverless_backfill: Set(false),
        ..Default::default()
    }
    .insert(&txn)
    .await
    .unwrap();
    txn.commit().await.unwrap();
    sink_id
}

#[tokio::test]
async fn dropped_job_drains_coordinator_before_releasing_state() {
    for fail_barrier in [false, true] {
        let (manager, scheduled, mut stops) = test_manager().await;
        let job_id = JobId::new(100);
        let table_id = TableId::new(101);
        register_table_ids_to_compaction_group(
            &manager.hummock_manager,
            &[table_id],
            CompactionGroupId::new(2),
        )
        .await;
        let task = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .drop_streaming_jobs(1.into(), vec![job_id], vec![table_id], HashMap::new())
                    .await;
            }
        });
        let command = scheduled.next_scheduled().await;
        let mut notifier = command.notifier.start();
        let collected = notifier.add_notify();
        notifier.started();
        assert!(matches!(stops.try_recv(), Err(TryRecvError::Empty)));
        if fail_barrier {
            collected.notify_collection_failed(anyhow!("injected drop barrier failure").into());
        } else {
            collected.notify_collected();
        }
        let (jobs, notifier) = stops.recv().await.unwrap();
        assert_eq!(jobs, vec![job_id]);
        let mut notifier = notifier.start();
        let exited = notifier.add_notify();
        notifier.started();
        assert!(!task.is_finished());
        assert!(
            manager
                .hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&table_id)
        );
        exited.notify_collected();
        task.await.unwrap();
        assert_eq!(
            manager
                .hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&table_id),
            fail_barrier,
        );
    }
}

#[tokio::test]
async fn cancelled_creating_job_drains_coordinator_even_when_barrier_fails() {
    for fail_barrier in [false, true] {
        let (manager, scheduled, mut stops) = test_manager().await;
        let sink_id = insert_sink(&manager, JobStatus::Creating).await;
        let task = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .cancel_streaming_jobs(vec![sink_id.as_job_id()])
                    .await
            }
        });
        let command = scheduled.next_scheduled().await;
        assert!(matches!(
            command.command,
            Command::DropStreamingJobs { streaming_job_ids, .. }
                if streaming_job_ids.contains(&sink_id.as_job_id())
        ));
        if fail_barrier {
            command
                .notifier
                .notify_start_failed(anyhow!("injected cancel barrier failure").into());
        } else {
            command.notifier.start().started();
        }
        let (jobs, notifier) = stops.recv().await.unwrap();
        assert_eq!(jobs, vec![sink_id.as_job_id()]);
        let mut notifier = notifier.start();
        let exited = notifier.add_notify();
        notifier.started();
        assert!(!task.is_finished());
        exited.notify_collected();
        let result = task.await.unwrap();
        if fail_barrier {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected cancel barrier failure")
            );
        } else {
            assert_eq!(result.unwrap(), vec![sink_id.as_job_id()]);
        }
    }
}

#[tokio::test]
async fn aborted_job_without_cancel_barrier_drains_coordinator() {
    let (manager, _, mut stops) = test_manager().await;
    let sink_id = insert_sink(&manager, JobStatus::Initial).await;
    let abort = manager
        .metadata_manager
        .catalog_controller
        .try_abort_creating_streaming_job(sink_id.as_job_id(), false)
        .await
        .unwrap();
    assert!(abort.aborted);
    assert!(abort.cancel_info.is_none());
    let task = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cleanup_aborted_streaming_job(abort).await }
    });
    let (jobs, notifier) = stops.recv().await.unwrap();
    assert_eq!(jobs, vec![sink_id.as_job_id()]);
    let mut notifier = notifier.start();
    let exited = notifier.add_notify();
    notifier.started();
    assert!(!task.is_finished());
    exited.notify_collected();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn retained_job_does_not_stop_its_coordinator() {
    let (manager, _, mut stops) = test_manager().await;
    for (status, is_cancelled) in [(JobStatus::Creating, false), (JobStatus::Created, true)] {
        let sink_id = insert_sink(&manager, status).await;
        let abort = manager
            .metadata_manager
            .catalog_controller
            .try_abort_creating_streaming_job(sink_id.as_job_id(), is_cancelled)
            .await
            .unwrap();
        assert!(!abort.aborted);
        manager.cleanup_aborted_streaming_job(abort).await.unwrap();
        assert!(matches!(stops.try_recv(), Err(TryRecvError::Empty)));
    }
}
