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

mod gc;
mod manual;
mod schedule;
mod stream;
#[cfg(test)]
pub(crate) mod test_utils;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use parking_lot::{Mutex, RwLock};
use risingwave_common::id::WorkerId;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use risingwave_pb::id::IcebergCompactionTaskId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::Streaming;

use super::MetaSrvEnv;
use crate::MetaResult;
use crate::barrier::BarrierScheduler;
use crate::hummock::IcebergCompactorManagerRef;
use crate::manager::MetadataManager;
use crate::rpc::metrics::MetaMetrics;

pub type IcebergCompactionManagerRef = Arc<IcebergCompactionManager>;

pub(crate) type CompactorChangeTx =
    UnboundedSender<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

pub(crate) type CompactorChangeRx =
    UnboundedReceiver<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

type ManualCompactionWaiter = tokio::sync::oneshot::Sender<MetaResult<IcebergCompactionTaskId>>;

use schedule::CompactionTrack;
pub use schedule::IcebergCompactionScheduleStatus;

pub struct IcebergCompactionManager {
    pub env: MetaSrvEnv,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,

    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    pub iceberg_compactor_manager: IcebergCompactorManagerRef,

    compactor_streams_change_tx: CompactorChangeTx,

    orphan_file_cleanup_locks: Mutex<HashMap<SinkId, Arc<tokio::sync::Mutex<()>>>>,

    pub metrics: Arc<MetaMetrics>,
}

struct IcebergCompactionManagerInner {
    sink_schedules: HashMap<SinkId, CompactionTrack>,
    /// Recovery isolation survives discarding a track. Generations fence async config loads.
    sink_recovery: HashMap<SinkId, SinkRecoveryState>,
    /// Never reuse a generation, including after DROP removes a sink's recovery entry.
    next_recovery_generation: u64,
    snapshot_expiration_sink_ids: HashSet<SinkId>,
    manifest_rewrite_sink_ids: HashSet<SinkId>,
    orphan_file_cleanup_sink_ids: HashSet<SinkId>,
    manual_compaction_waiters: HashMap<SinkId, ManualCompactionWaiter>,
}

#[derive(Default)]
struct SinkRecoveryState {
    generation: u64,
    recovering: bool,
}

impl IcebergCompactionManagerInner {
    fn next_recovery_generation(&mut self) -> u64 {
        self.next_recovery_generation += 1;
        self.next_recovery_generation
    }

    fn recovery_generation(&self, sink_id: SinkId) -> u64 {
        self.sink_recovery
            .get(&sink_id)
            .map_or(0, |state| state.generation)
    }

    fn is_recovering(&self, sink_id: SinkId) -> bool {
        self.sink_recovery
            .get(&sink_id)
            .is_some_and(|state| state.recovering)
    }
}

impl IcebergCompactionManager {
    fn report_timeout(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_report_timeout_sec)
    }

    fn config_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_config_refresh_interval_sec)
    }

    pub fn build(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        iceberg_compactor_manager: IcebergCompactorManagerRef,
        metrics: Arc<MetaMetrics>,
        barrier_scheduler: BarrierScheduler,
    ) -> (Arc<Self>, CompactorChangeRx) {
        let (compactor_streams_change_tx, compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                env,
                inner: Arc::new(RwLock::new(IcebergCompactionManagerInner {
                    sink_schedules: HashMap::default(),
                    sink_recovery: HashMap::default(),
                    next_recovery_generation: 0,
                    snapshot_expiration_sink_ids: HashSet::default(),
                    manifest_rewrite_sink_ids: HashSet::default(),
                    orphan_file_cleanup_sink_ids: HashSet::default(),
                    manual_compaction_waiters: HashMap::default(),
                })),
                metadata_manager,
                barrier_scheduler,
                iceberg_compactor_manager,
                compactor_streams_change_tx,
                orphan_file_cleanup_locks: Mutex::new(HashMap::default()),
                metrics,
            }),
            compactor_streams_change_rx,
        )
    }

    async fn get_sink_param(&self, sink_id: SinkId) -> MetaResult<SinkParam> {
        let prost_sink_catalog = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(sink_id)
            .await?
            .ok_or_else(|| anyhow!("Sink not found: {}", sink_id))?;
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(param)
    }

    async fn load_iceberg_config(&self, sink_id: SinkId) -> MetaResult<IcebergConfig> {
        let registration = self
            .inner
            .read()
            .sink_schedules
            .get(&sink_id)
            .and_then(|track| track.pk_index_registration().cloned());
        if let Some(registration) = registration {
            if !registration.is_ready() {
                return Err(anyhow!("iceberg pk-index sink {} is not ready", sink_id).into());
            }
            return Ok((*registration.config()).clone());
        }
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(iceberg_config)
    }

    fn orphan_file_cleanup_lock(&self, sink_id: SinkId) -> Arc<tokio::sync::Mutex<()>> {
        self.orphan_file_cleanup_locks
            .lock()
            .entry(sink_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

/// User-created iceberg sinks have arbitrary names, so identify them by the
/// connector property instead of the `__iceberg_sink_` name prefix.
pub fn is_iceberg_sink(properties: &BTreeMap<String, String>) -> bool {
    properties
        .get(UPSTREAM_SOURCE_KEY)
        .is_some_and(|connector| connector.eq_ignore_ascii_case(ICEBERG_SINK))
}
