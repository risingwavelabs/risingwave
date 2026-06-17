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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use parking_lot::RwLock;
use risingwave_common::id::{FragmentId, WorkerId};
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::Streaming;

use super::MetaSrvEnv;
use crate::MetaResult;
use crate::barrier::BarrierScheduler;
use crate::hummock::IcebergCompactorManagerRef;
use crate::manager::MetadataManager;
use crate::manager::iceberg_v3_sink::IcebergV3SinkManager;
use crate::rpc::metrics::MetaMetrics;

/// Tracks an attached transient Iceberg-V3 compaction-resolve pipeline so a later DETACH (a
/// separate task) can target the exact fragment created on attach. Fields are consumed by
/// `detach_v3_resolve`, driven by the resolve-completion loop.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AttachedResolve {
    pub writer_fragment_id: FragmentId,
    pub resolve_fragment_id: FragmentId,
}

pub type IcebergCompactionManagerRef = Arc<IcebergCompactionManager>;

pub(crate) type CompactorChangeTx =
    UnboundedSender<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

pub(crate) type CompactorChangeRx =
    UnboundedReceiver<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

type ManualCompactionWaiter = tokio::sync::oneshot::Sender<MetaResult<u64>>;

use schedule::CompactionTrack;
pub use schedule::IcebergCompactionScheduleStatus;

pub struct IcebergCompactionManager {
    pub env: MetaSrvEnv,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,

    metadata_manager: MetadataManager,
    pub iceberg_compactor_manager: IcebergCompactorManagerRef,

    compactor_streams_change_tx: CompactorChangeTx,

    pub metrics: Arc<MetaMetrics>,

    /// Manager for V3 sink commit coordinators. Used at ATTACH time to arm the coordinator with the
    /// held compaction state, and by the completion trigger to fire `End` + DETACH once the bracketing
    /// epoch's overwrite has committed.
    iceberg_v3_sink_manager: IcebergV3SinkManager,

    /// Used to schedule the V3 compaction-resolve ATTACH/DETACH commands onto the streaming graph
    /// via the barrier worker. `None` in unit tests that do not exercise the barrier path.
    barrier_scheduler: Option<BarrierScheduler>,
}

struct IcebergCompactionManagerInner {
    sink_schedules: HashMap<SinkId, CompactionTrack>,
    snapshot_expiration_sink_ids: HashSet<SinkId>,
    manual_compaction_waiters: HashMap<SinkId, ManualCompactionWaiter>,
    /// Resolve pipelines currently attached, keyed by sink. Populated on ATTACH so a later DETACH
    /// (a separate task) can target the exact resolve fragment.
    attached_resolves: HashMap<SinkId, AttachedResolve>,
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
        iceberg_v3_sink_manager: IcebergV3SinkManager,
        barrier_scheduler: Option<BarrierScheduler>,
    ) -> (Arc<Self>, CompactorChangeRx) {
        let (compactor_streams_change_tx, compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                env,
                inner: Arc::new(RwLock::new(IcebergCompactionManagerInner {
                    sink_schedules: HashMap::default(),
                    snapshot_expiration_sink_ids: HashSet::default(),
                    manual_compaction_waiters: HashMap::default(),
                    attached_resolves: HashMap::default(),
                })),
                metadata_manager,
                iceberg_compactor_manager,
                compactor_streams_change_tx,
                metrics,
                iceberg_v3_sink_manager,
                barrier_scheduler,
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
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(iceberg_config)
    }
}
