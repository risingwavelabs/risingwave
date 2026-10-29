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

use std::collections::BTreeMap;

use risingwave_common::id::{SchemaId, UserId};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::Sink;
use risingwave_meta_model::{CreateType, JobStatus, StreamingParallelism, sink, streaming_job};
use risingwave_pb::catalog::{PbSink, PbSinkType};
use sea_orm::{ActiveModelTrait, EntityTrait, Set, TransactionTrait};
use tokio::sync::oneshot;

use super::*;
use crate::barrier::to_partial_graph_id;
use crate::controller::catalog::CatalogController;
use crate::manager::iceberg_compaction::test_utils::test_managers;

async fn insert_sink(manager: &IcebergPkIndexSinkManager, status: JobStatus) -> SinkId {
    let catalog = manager
        .inner
        .catalog_controller
        .get_inner_write_guard()
        .await;
    let txn = catalog.db.begin().await.unwrap();
    let schema_id = SchemaId::new(2);
    let object = CatalogController::create_object(
        &txn,
        ObjectType::Sink,
        UserId::new(1),
        Some(schema_id.as_object_id()),
    )
    .await
    .unwrap();
    let sink_id = object.oid.as_sink_id();
    Sink::insert(sink::ActiveModel::from(PbSink {
        id: sink_id,
        name: format!("test_sink_{sink_id}"),
        schema_id,
        database_id: object.database_id.unwrap(),
        owner: 1.into(),
        sink_type: PbSinkType::Upsert as i32,
        properties: BTreeMap::from([
            ("connector".to_owned(), "iceberg".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
            ("primary_key".to_owned(), "id".to_owned()),
            ("enable_pk_index".to_owned(), "true".to_owned()),
            (
                "warehouse.path".to_owned(),
                "memory://registration".to_owned(),
            ),
            ("catalog.type".to_owned(), "storage".to_owned()),
            ("database.name".to_owned(), "db".to_owned()),
            ("table.name".to_owned(), "table".to_owned()),
        ]),
        ..Default::default()
    }))
    .exec(&txn)
    .await
    .unwrap();
    streaming_job::ActiveModel {
        job_id: Set(sink_id.as_job_id()),
        job_status: Set(status),
        create_type: Set(CreateType::Foreground),
        timezone: Set(None),
        config_override: Set(None),
        adaptive_parallelism_strategy: Set(None),
        parallelism: Set(StreamingParallelism::Adaptive),
        backfill_parallelism: Set(None),
        backfill_adaptive_parallelism_strategy: Set(None),
        backfill_orders: Set(None),
        max_parallelism: Set(1),
        specific_resource_group: Set(None),
        is_serverless_backfill: Set(false),
        refresh_interval_sec: Set(None),
    }
    .insert(&txn)
    .await
    .unwrap();
    txn.commit().await.unwrap();
    sink_id
}

fn graph(sink_id: SinkId) -> PartialGraphId {
    to_partial_graph_id(1.into(), Some(sink_id.as_job_id()))
}

#[tokio::test]
async fn owns_compaction_manager_without_a_reference_cycle() {
    let (compaction, manager) = test_managers().await;
    let weak = Arc::downgrade(&compaction);
    drop(compaction);
    assert!(weak.upgrade().is_some());
    drop(manager);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn cancelled_initialization_removes_its_registration_and_cursor() {
    let (compaction, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Initial).await;
    let (started_tx, started_rx) = oneshot::channel();
    let task = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .register_sink_with(sink_id, graph(sink_id), |_, _, _| async move {
                    started_tx.send(()).unwrap();
                    std::future::pending().await
                })
                .await
        }
    });
    started_rx.await.unwrap();
    assert!(manager.coordinator(sink_id).is_err());
    assert_eq!(manager.partial_graph_of(sink_id).unwrap(), graph(sink_id));
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert!(compaction.list_compaction_statuses().is_empty());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(manager.partial_graph_of(sink_id).is_err());
    assert!(manager.wait_epoch(sink_id, 1).await.is_err());
}

#[tokio::test]
async fn failed_old_initialization_does_not_remove_replacement() {
    let (_, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Initial).await;
    let (old_tx, old_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let old = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .register_sink_with(sink_id, graph(sink_id), |_, _, _| async move {
                    old_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Err(anyhow!("injected initialization failure"))
                })
                .await
        }
    });
    old_rx.await.unwrap();
    let (new_tx, new_rx) = oneshot::channel();
    let new = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .register_sink_with(sink_id, graph(sink_id), |_, _, _| async move {
                    new_tx.send(()).unwrap();
                    std::future::pending().await
                })
                .await
        }
    });
    new_rx.await.unwrap();
    let replacement = manager.inner.coordinators.read()[&sink_id]
        .registration
        .clone();
    release_tx.send(()).unwrap();
    assert!(old.await.unwrap().is_err());
    assert!(Arc::ptr_eq(
        &replacement,
        &manager.inner.coordinators.read()[&sink_id].registration
    ));
    assert_eq!(manager.partial_graph_of(sink_id).unwrap(), graph(sink_id));
    new.abort();
    assert!(new.await.unwrap_err().is_cancelled());
    assert!(manager.partial_graph_of(sink_id).is_err());
}

#[tokio::test]
async fn deleted_catalog_cannot_start_a_fresh_registration() {
    let (compaction, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Initial).await;
    let abort = manager
        .inner
        .catalog_controller
        .try_abort_creating_streaming_job(sink_id.as_job_id(), true)
        .await
        .unwrap();
    assert!(abort.aborted);
    assert_eq!(abort.aborted_sink_ids, vec![sink_id]);
    manager.retire_sinks(abort.aborted_sink_ids);
    let result = manager
        .register_sink_with(sink_id, graph(sink_id), |_, _, _| async {
            panic!("a deleted catalog must not reach coordinator initialization")
        })
        .await;
    assert!(result.unwrap_err().to_string().contains("no longer exists"));
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert!(manager.partial_graph_of(sink_id).is_err());
    assert!(compaction.list_compaction_statuses().is_empty());
}

#[cfg(not(madsim))]
async fn initialized_coordinator(
    sink_id: SinkId,
    _config: IcebergConfig,
    db: sea_orm::DatabaseConnection,
) -> anyhow::Result<IcebergPkIndexSinkCoordinator> {
    Box::pin(coordinator_with_snapshot(sink_id, db, None)).await
}

#[cfg(not(madsim))]
async fn coordinator_with_snapshot(
    sink_id: SinkId,
    db: sea_orm::DatabaseConnection,
    snapshot_id: Option<i64>,
) -> anyhow::Result<IcebergPkIndexSinkCoordinator> {
    use iceberg::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, Runtime, TableIdent};

    let catalog = IcebergConfig::from_btreemap(BTreeMap::from([
        ("type".to_owned(), "append-only".to_owned()),
        ("catalog.type".to_owned(), "mock".to_owned()),
        ("database.name".to_owned(), "mock_namespace".to_owned()),
        ("table.name".to_owned(), "range_table".to_owned()),
    ]))?
    .create_catalog()
    .await?;
    let mut table = catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("mock_namespace".to_owned()),
            "range_table".to_owned(),
        ))
        .await?;
    if let Some(snapshot_id) = snapshot_id {
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(
                Snapshot::builder()
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(snapshot_id)
                    .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
                    .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
                    .with_summary(Summary {
                        operation: Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .with_schema_id(table.metadata().current_schema_id())
                    .build(),
            )?
            .set_ref(
                "main",
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )?
            .build()?
            .metadata;
        table = Table::builder()
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .runtime(Runtime::try_current()?)
            .metadata(metadata)
            .build()?;
    }
    Ok(IcebergPkIndexSinkCoordinator::for_test(
        sink_id, table, catalog, db,
    ))
}

#[cfg(not(madsim))]
#[tokio::test]
async fn retirement_rejects_late_successful_initialization() {
    let (compaction, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Initial).await;
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .register_sink_with(sink_id, graph(sink_id), |id, config, db| async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Box::pin(initialized_coordinator(id, config, db)).await
                })
                .await
        }
    });
    started_rx.await.unwrap();
    manager.retire_sinks([sink_id]);
    release_tx.send(()).unwrap();
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("retired")
    );
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert!(manager.coordinator(sink_id).is_err());
    assert!(manager.partial_graph_of(sink_id).is_err());
    assert!(compaction.list_compaction_statuses().is_empty());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn readiness_config_and_retirement_follow_the_current_registration() {
    let (compaction, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Created).await;
    manager
        .register_sink_with(sink_id, graph(sink_id), initialized_coordinator)
        .await
        .unwrap();
    assert!(compaction.list_compaction_statuses().is_empty());
    let registration = manager.inner.coordinators.read()[&sink_id]
        .registration
        .clone();
    let mut config = (*registration.config()).clone();
    config.compaction_interval_sec = Some(42);
    manager.on_sink_config_changed(sink_id, config.clone());
    manager.on_jobs_ready(&HashSet::new()).await;
    assert!(!registration.is_ready());
    assert!(compaction.list_compaction_statuses().is_empty());
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert!(registration.is_ready());
    assert_eq!(
        compaction.list_compaction_statuses()[0].trigger_interval_sec,
        42
    );
    config.compaction_interval_sec = Some(43);
    manager.on_sink_config_changed(sink_id, config);
    assert_eq!(registration.config().compaction_interval_sec(), 43);
    assert_eq!(
        compaction.list_compaction_statuses()[0].trigger_interval_sec,
        43
    );

    // An unsuccessful cancellation of an already Created sink must preserve the live instance.
    let abort = manager
        .inner
        .catalog_controller
        .try_abort_creating_streaming_job(sink_id.as_job_id(), true)
        .await
        .unwrap();
    assert!(!abort.aborted);
    manager.retire_sinks(abort.aborted_sink_ids);
    assert!(registration.is_ready());
    assert_eq!(compaction.list_compaction_statuses().len(), 1);
    manager.retire_sinks([sink_id]);
    assert!(!registration.is_ready());
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert!(compaction.list_compaction_statuses().is_empty());
    assert!(manager.wait_epoch(sink_id, 1).await.is_err());
    compaction
        .check_and_expire_snapshots(sink_id)
        .await
        .unwrap();
    compaction
        .check_and_rewrite_manifests(sink_id)
        .await
        .unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn recovery_readiness_and_retirement_are_scoped_to_running_jobs() {
    let (compaction, manager) = test_managers().await;
    let first = insert_sink(&manager, JobStatus::Created).await;
    let second = insert_sink(&manager, JobStatus::Created).await;
    for sink_id in [first, second] {
        manager
            .register_sink_with(sink_id, graph(sink_id), initialized_coordinator)
            .await
            .unwrap();
    }
    let first_registration = manager.inner.coordinators.read()[&first]
        .registration
        .clone();
    let second_registration = manager.inner.coordinators.read()[&second]
        .registration
        .clone();
    // A failed independent graph remains registered for commits, but cannot schedule compaction.
    manager
        .on_jobs_ready(&HashSet::from([first.as_job_id()]))
        .await;
    assert!(first_registration.is_ready());
    assert!(!second_registration.is_ready());
    assert_eq!(compaction.list_compaction_statuses().len(), 1);
    manager.start_recovery(Some(&HashSet::from([first.as_job_id()])));
    assert!(!first_registration.is_ready());
    assert!(manager.coordinator(first).is_err());
    assert!(manager.coordinator(second).is_ok());
    assert!(compaction.list_compaction_statuses().is_empty());
    manager
        .on_jobs_ready(&HashSet::from([second.as_job_id()]))
        .await;
    assert!(second_registration.is_ready());
    assert_eq!(compaction.list_compaction_statuses().len(), 1);
    manager.start_recovery(None);
    assert!(!second_registration.is_ready());
    assert!(manager.coordinator(second).is_err());
    assert!(manager.wait_epoch(first, 1).await.is_err());
    assert!(manager.wait_epoch(second, 1).await.is_err());
    manager
        .on_jobs_ready(&HashSet::from([first.as_job_id(), second.as_job_id()]))
        .await;
    assert!(compaction.list_compaction_statuses().is_empty());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn readiness_reads_the_current_head_instead_of_the_initial_head() {
    let (compaction, manager) = test_managers().await;
    let sink_id = insert_sink(&manager, JobStatus::Created).await;
    manager
        .register_sink_with(sink_id, graph(sink_id), initialized_coordinator)
        .await
        .unwrap();
    let coordinator = manager.coordinator(sink_id).unwrap();
    assert!(
        coordinator
            .lock()
            .await
            .latest_observed_snapshot()
            .is_none()
    );
    // Model ingestion between initialization and graph readiness. No manager cache is updated.
    let mut config = (*manager.inner.coordinators.read()[&sink_id]
        .registration
        .config())
    .clone();
    config.enable_compaction = true;
    manager.on_sink_config_changed(sink_id, config);
    let db = compaction.env.meta_store_ref().conn.clone();
    *coordinator.lock().await = Box::pin(coordinator_with_snapshot(sink_id, db, Some(42)))
        .await
        .unwrap();
    manager
        .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
        .await;
    assert_eq!(compaction.get_top_n_iceberg_commit_sink_ids(1).len(), 1);
}

#[cfg(not(madsim))]
#[tokio::test]
async fn retirement_or_replacement_while_readiness_waits_cannot_publish_the_old_instance() {
    for replace in [false, true] {
        let (compaction, manager) = test_managers().await;
        let sink_id = insert_sink(&manager, JobStatus::Created).await;
        manager
            .register_sink_with(sink_id, graph(sink_id), initialized_coordinator)
            .await
            .unwrap();
        let coordinator = manager.coordinator(sink_id).unwrap();
        let guard = coordinator.lock().await;
        let jobs = HashSet::from([sink_id.as_job_id()]);
        let ready = manager.on_jobs_ready(&jobs);
        tokio::pin!(ready);
        assert!(futures::poll!(ready.as_mut()).is_pending());
        assert!(compaction.list_compaction_statuses().is_empty());
        // This must not wait for the coordinator mutex held above.
        manager.retire_sinks([sink_id]);
        if replace {
            manager
                .register_sink_with(sink_id, graph(sink_id), initialized_coordinator)
                .await
                .unwrap();
        }
        drop(guard);
        ready.await;
        assert!(compaction.list_compaction_statuses().is_empty());
        if replace {
            let registration = manager.inner.coordinators.read()[&sink_id]
                .registration
                .clone();
            assert!(!registration.is_ready());
            manager
                .on_jobs_ready(&HashSet::from([sink_id.as_job_id()]))
                .await;
            assert!(registration.is_ready());
            assert_eq!(compaction.list_compaction_statuses().len(), 1);
        }
    }
}
