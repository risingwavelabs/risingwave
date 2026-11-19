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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::anyhow;
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use risingwave_common::id::{JobId, PartialGraphId};
use risingwave_connector::connector_common::{
    IcebergCommittedSnapshot, IcebergSinkCompactionUpdate,
};
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::IcebergConfig;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

use super::committed_epoch::PartialGraphCommittedEpochs;
use super::coordinator::IcebergPkIndexSinkCoordinator;
use super::{IcebergPkIndexPreCommitMetadata, build_iceberg_config, is_iceberg_pk_index_sink};
use crate::controller::catalog::CatalogControllerRef;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;

type CoordinatorRef = Arc<Mutex<IcebergPkIndexSinkCoordinator>>;

/// Scheduling holds this registration, not an independent copy of sink liveness.
pub(crate) struct IcebergPkIndexSinkRegistration {
    ready: AtomicBool,
    config: ArcSwap<IcebergConfig>,
}

impl std::fmt::Debug for IcebergPkIndexSinkRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergPkIndexSinkRegistration")
            .field("ready", &self.is_ready())
            .finish_non_exhaustive()
    }
}

impl IcebergPkIndexSinkRegistration {
    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn config(&self) -> Arc<IcebergConfig> {
        self.config.load_full()
    }

    #[cfg(test)]
    pub(crate) fn for_test(config: IcebergConfig) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(true),
            config: ArcSwap::from_pointee(config),
        })
    }

    #[cfg(test)]
    pub(crate) fn retire_for_test(&self) {
        self.ready.store(false, Ordering::Release);
    }
}

struct RegisteredSink {
    partial_graph_id: PartialGraphId,
    registration: Arc<IcebergPkIndexSinkRegistration>,
    coordinator: Option<CoordinatorRef>,
}

/// Manager for the Iceberg pk-index sink path, cheap to clone.
#[derive(Clone)]
pub struct IcebergPkIndexSinkManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    catalog_controller: CatalogControllerRef,
    compaction_manager: IcebergCompactionManagerRef,
    /// A registration exists before async initialization starts. Completion can only fill that
    /// same live entry. Registry -> compaction is the control-plane lock order; neither lock is
    /// held across coordinator I/O.
    coordinators: RwLock<HashMap<SinkId, RegisteredSink>>,
    /// Per-partial-graph committed-epoch cursor. A cursor entry exists exactly while a partial graph has
    /// a registered pk-index sink: created in `register_sink` and removed on retirement or recovery,
    /// all under the `coordinators` write lock. Advanced on
    /// every checkpoint completion via `advance_committed_epochs` (a no-op for partial graphs with no
    /// registered sink).
    committed_epochs: PartialGraphCommittedEpochs,
    compaction_stat_tx: UnboundedSender<IcebergSinkCompactionUpdate>,
}

impl IcebergPkIndexSinkManager {
    pub fn new(
        catalog_controller: CatalogControllerRef,
        compaction_manager: IcebergCompactionManagerRef,
        compaction_stat_tx: UnboundedSender<IcebergSinkCompactionUpdate>,
    ) -> Self {
        IcebergPkIndexSinkManager {
            inner: Arc::new(ManagerInner {
                catalog_controller,
                compaction_manager,
                coordinators: RwLock::new(HashMap::new()),
                committed_epochs: PartialGraphCommittedEpochs::default(),
                compaction_stat_tx,
            }),
        }
    }

    /// Reserve a live catalog-backed registration before loading/replaying the coordinator.
    /// Retirement, replacement, failure or cancellation prevents late initialization from
    /// installing a new coordinator. Graph readiness is published separately by Barrier.
    pub async fn register_sink(
        &self,
        sink_id: SinkId,
        partial_graph_id: PartialGraphId,
    ) -> anyhow::Result<()> {
        self.register_sink_with(
            sink_id,
            partial_graph_id,
            IcebergPkIndexSinkCoordinator::init,
        )
        .await
    }

    async fn register_sink_with<F, Fut>(
        &self,
        sink_id: SinkId,
        partial_graph_id: PartialGraphId,
        initialize: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(SinkId, IcebergConfig, sea_orm::DatabaseConnection) -> Fut + Send,
        Fut: Future<Output = anyhow::Result<IcebergPkIndexSinkCoordinator>> + Send,
    {
        let (registration, config, db) = {
            // Deletion cannot pass the reservation. Do not reacquire the catalog read lock via
            // CatalogController's wrapper while holding this guard.
            let catalog = self.inner.catalog_controller.get_inner_read_guard().await;
            let sink = catalog
                .get_sink_by_id(sink_id)
                .await?
                .ok_or_else(|| anyhow!("iceberg pk-index sink {} no longer exists", sink_id))?;
            anyhow::ensure!(
                is_iceberg_pk_index_sink(&sink.properties),
                "sink {} is not an iceberg pk-index sink",
                sink_id
            );
            let config = build_iceberg_config(&sink)?;
            let registration = Arc::new(IcebergPkIndexSinkRegistration {
                ready: AtomicBool::new(false),
                config: ArcSwap::from_pointee(config.clone()),
            });
            let mut coordinators = self.inner.coordinators.write();
            Self::remove_sink(&mut coordinators, sink_id, &self.inner.committed_epochs);
            coordinators.insert(
                sink_id,
                RegisteredSink {
                    partial_graph_id,
                    registration: registration.clone(),
                    coordinator: None,
                },
            );
            self.inner
                .compaction_manager
                .on_recovery_start(Some(&[sink_id]));
            self.inner.committed_epochs.ensure(partial_graph_id);
            (registration, config, catalog.db.clone())
        };
        let cleanup = scopeguard::guard(registration, |registration| {
            let mut coordinators = self.inner.coordinators.write();
            if coordinators
                .get(&sink_id)
                .is_some_and(|sink| Arc::ptr_eq(&sink.registration, &registration))
            {
                Self::remove_sink(&mut coordinators, sink_id, &self.inner.committed_epochs);
            }
        });
        let coordinator = initialize(sink_id, config, db).await?;
        {
            let mut coordinators = self.inner.coordinators.write();
            let sink = coordinators
                .get_mut(&sink_id)
                .filter(|sink| Arc::ptr_eq(&sink.registration, &cleanup))
                .ok_or_else(|| {
                    anyhow!("iceberg pk-index sink {} registration was retired", sink_id)
                })?;
            sink.coordinator = Some(Arc::new(Mutex::new(coordinator)));
        }
        scopeguard::ScopeGuard::into_inner(cleanup);
        Ok(())
    }

    /// Barrier calls this only after the jobs' initial graphs and command queues are ready.
    /// Read the settled head once, then validate and publish under the registry lock.
    pub(crate) async fn on_jobs_ready(&self, job_ids: &HashSet<JobId>) {
        let sinks = {
            let coordinators = self.inner.coordinators.read();
            coordinators
                .iter()
                .filter(|(sink_id, sink)| {
                    job_ids.contains(&sink_id.as_job_id()) && !sink.registration.is_ready()
                })
                .filter_map(|(&sink_id, sink)| {
                    sink.coordinator.as_ref().map(|coordinator| {
                        (sink_id, sink.registration.clone(), coordinator.clone())
                    })
                })
                .collect::<Vec<_>>()
        };
        for (sink_id, registration, coordinator) in sinks {
            // Do not hold the registry lock while waiting. Retain the coordinator guard through
            // publication so commits cannot fall between observing the head and enabling updates.
            let coordinator = coordinator.lock().await;
            let coordinators = self.inner.coordinators.write();
            if !coordinators
                .get(&sink_id)
                .is_some_and(|sink| Arc::ptr_eq(&sink.registration, &registration))
                || registration.ready.swap(true, Ordering::AcqRel)
            {
                continue;
            }
            self.inner.compaction_manager.on_sink_ready(
                sink_id,
                registration,
                coordinator.latest_observed_snapshot(),
            );
        }
    }

    pub(crate) fn on_sink_config_changed(&self, sink_id: SinkId, config: IcebergConfig) {
        let mut coordinators = self.inner.coordinators.write();
        if let Some(sink) = coordinators.get_mut(&sink_id) {
            sink.registration.config.store(Arc::new(config));
            if sink.registration.is_ready() {
                self.inner
                    .compaction_manager
                    .on_sink_config_changed(sink_id, (*sink.registration.config()).clone());
            }
        }
    }

    /// Pre-commit one epoch, with an optional compaction overwrite folded into the same pending row.
    /// The barrier-complete path awaits this before issuing Hummock `commit_epoch`.
    pub(crate) async fn pre_commit(
        &self,
        input: IcebergPkIndexPreCommitMetadata,
    ) -> anyhow::Result<()> {
        let coordinator = self.coordinator(input.sink_id)?;
        coordinator
            .lock()
            .await
            .pre_commit(input.prev_epoch, input.reports, input.compaction)
            .await
    }

    /// Commit phase for one epoch: run an iceberg `overwrite_files` transaction and mark its pending row
    /// committed. The barrier-complete path awaits this AFTER hummock `commit_epoch`.
    pub async fn commit_epoch(&self, sink_id: SinkId) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        // Keep commit completion and scheduler notifications in epoch order.
        let mut coordinator_guard = coordinator.lock().await;
        let result = coordinator_guard.commit().await?;
        if let Some((observed_snapshot, has_new_data)) = result {
            let coordinators = self.inner.coordinators.read();
            if let Some(sink) = coordinators.get(&sink_id)
                && sink
                    .coordinator
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &coordinator))
                && sink.registration.is_ready()
            {
                self.notify_compaction_scheduler(sink_id, Some(observed_snapshot), has_new_data);
            }
        }
        drop(coordinator_guard);
        Ok(())
    }

    fn notify_compaction_scheduler(
        &self,
        sink_id: SinkId,
        observed_snapshot: Option<IcebergCommittedSnapshot>,
        has_new_data: bool,
    ) {
        let Some(observed_snapshot) = observed_snapshot else {
            return;
        };
        if self
            .inner
            .compaction_stat_tx
            .send(IcebergSinkCompactionUpdate {
                sink_id,
                force_compaction: false,
                has_new_data,
                observed_snapshot,
            })
            .is_err()
        {
            warn!(%sink_id, "failed to notify iceberg compaction scheduler");
        }
    }

    /// Advance the per-partial-graph committed epoch after a checkpoint completion.
    pub fn advance_committed_epochs(
        &self,
        epochs: impl IntoIterator<Item = (PartialGraphId, u64)>,
    ) {
        self.inner.committed_epochs.advance_all(epochs);
    }

    /// Block until `sink_id`'s partial graph has committed through `target_epoch`, then return the
    /// coordinator's committed iceberg snapshot id (a lower bound the caller must observe when it
    /// reloads the table). Does NOT hold the coordinator lock while waiting.
    pub async fn wait_epoch(
        &self,
        sink_id: SinkId,
        target_epoch: u64,
    ) -> anyhow::Result<Option<i64>> {
        let partial_graph_id = self.partial_graph_of(sink_id)?;
        self.inner
            .committed_epochs
            .wait(partial_graph_id, target_epoch)
            .await;
        let coordinator = self.coordinator(sink_id)?;
        let snapshot_id = coordinator.lock().await.current_snapshot_id();
        Ok(snapshot_id)
    }

    /// Permanently retire the sinks actually removed from the catalog, including legacy sinks
    /// which only have compaction state. Idempotent, and serialized with ready publication.
    pub fn retire_sinks(&self, sink_ids: impl IntoIterator<Item = SinkId>) {
        let mut coordinators = self.inner.coordinators.write();
        for sink_id in sink_ids {
            Self::remove_sink(&mut coordinators, sink_id, &self.inner.committed_epochs);
            self.inner
                .compaction_manager
                .clear_iceberg_maintenance_by_sink_id(sink_id);
        }
    }

    pub(crate) fn start_recovery(&self, job_ids: Option<&HashSet<JobId>>) {
        let mut coordinators = self.inner.coordinators.write();
        if let Some(job_ids) = job_ids {
            for job_id in job_ids {
                Self::remove_sink(
                    &mut coordinators,
                    job_id.as_sink_id(),
                    &self.inner.committed_epochs,
                );
            }
        } else {
            for sink in coordinators.values() {
                sink.registration.ready.store(false, Ordering::Release);
            }
            coordinators.clear();
            self.inner.committed_epochs.clear();
        }
        let scope = job_ids.map(|ids| ids.iter().map(|id| id.as_sink_id()).collect::<Vec<_>>());
        self.inner
            .compaction_manager
            .on_recovery_start(scope.as_deref());
    }

    fn remove_sink(
        coordinators: &mut HashMap<SinkId, RegisteredSink>,
        sink_id: SinkId,
        committed_epochs: &PartialGraphCommittedEpochs,
    ) {
        if let Some(sink) = coordinators.remove(&sink_id) {
            sink.registration.ready.store(false, Ordering::Release);
            if !coordinators
                .values()
                .any(|other| other.partial_graph_id == sink.partial_graph_id)
            {
                committed_epochs.remove(sink.partial_graph_id);
            }
        }
    }

    fn coordinator(&self, sink_id: SinkId) -> anyhow::Result<CoordinatorRef> {
        self.inner
            .coordinators
            .read()
            .get(&sink_id)
            .and_then(|sink| sink.coordinator.clone())
            .ok_or_else(|| {
                anyhow!(
                    "iceberg pk-index sink coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }

    fn partial_graph_of(&self, sink_id: SinkId) -> anyhow::Result<PartialGraphId> {
        self.inner
            .coordinators
            .read()
            .get(&sink_id)
            .map(|sink| sink.partial_graph_id)
            .ok_or_else(|| {
                anyhow!(
                    "iceberg pk-index sink coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }
}

#[cfg(test)]
#[path = "manager_test.rs"]
mod tests;
