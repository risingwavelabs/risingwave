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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use risingwave_common::id::WorkerId;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::Streaming;

use super::MetaSrvEnv;
use crate::MetaResult;
use crate::controller::streaming_job::AbortCreatingJobResult;
use crate::hummock::IcebergCompactorManagerRef;
use crate::manager::MetadataManager;
use crate::rpc::metrics::MetaMetrics;

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
}

struct IcebergCompactionManagerInner {
    sink_schedules: HashMap<SinkId, CompactionTrack>,
    snapshot_expiration_sink_ids: HashSet<SinkId>,
    manual_compaction_waiters: HashMap<SinkId, ManualCompactionWaiter>,
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
                })),
                metadata_manager,
                iceberg_compactor_manager,
                compactor_streams_change_tx,
                metrics,
            }),
            compactor_streams_change_rx,
        )
    }

    /// `Ok(None)` means the sink no longer exists in the catalog; transient
    /// metastore errors are propagated as `Err`.
    async fn get_sink_param(&self, sink_id: SinkId) -> MetaResult<Option<SinkParam>> {
        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(sink_id)
            .await?
        else {
            return Ok(None);
        };
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(Some(param))
    }

    /// `Ok(None)` means the sink no longer exists in the catalog.
    async fn load_iceberg_config(&self, sink_id: SinkId) -> MetaResult<Option<IcebergConfig>> {
        let Some(sink_param) = self.get_sink_param(sink_id).await? else {
            return Ok(None);
        };
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(Some(iceberg_config))
    }

    /// Removes maintenance entries for a sink that no longer exists in the
    /// catalog, so a removal path that missed cleanup cannot leave permanent
    /// orphans. Kept observable via an error log and a counter metric.
    fn remove_orphan_iceberg_maintenance(&self, sink_id: SinkId, path: &str) {
        tracing::error!(
            iceberg_component = "compaction_scheduler",
            iceberg_operation = path,
            sink_id = %sink_id,
            "iceberg_maintenance_orphan_sink_removed",
        );
        self.metrics
            .iceberg_compaction_orphan_sink_removed_count
            .with_label_values(&[path])
            .inc();
        self.clear_iceberg_maintenance_by_sink_id(sink_id);
    }

    /// Clear the iceberg maintenance state of the sink aborted by
    /// `try_abort_creating_streaming_job`, if any.
    pub fn clear_maintenance_for_aborted_job(&self, abort_result: &AbortCreatingJobResult) {
        if let Some(sink_id) = abort_result.aborted_sink_id {
            self.clear_iceberg_maintenance_by_sink_id(sink_id);
        }
    }

    /// Clear the iceberg maintenance state of the sink aborted by
    /// `try_abort_creating_streaming_job`, if any.
    pub fn clear_maintenance_for_aborted_job(&self, abort_result: &AbortCreatingJobResult) {
        if let Some(sink_id) = abort_result.aborted_sink_id {
            self.clear_iceberg_maintenance_by_sink_id(sink_id);
        }
    }
}

/// User-created iceberg sinks have arbitrary names, so identify them by the
/// connector property instead of the `__iceberg_sink_` name prefix.
pub fn is_iceberg_sink(properties: &BTreeMap<String, String>) -> bool {
    properties
        .get(UPSTREAM_SOURCE_KEY)
        .is_some_and(|connector| connector.eq_ignore_ascii_case(ICEBERG_SINK))
}

/// User-created iceberg sinks have arbitrary names, so identify them by the
/// connector property instead of the `__iceberg_sink_` name prefix.
pub fn is_iceberg_sink(properties: &BTreeMap<String, String>) -> bool {
    properties
        .get(UPSTREAM_SOURCE_KEY)
        .is_some_and(|connector| connector.eq_ignore_ascii_case(ICEBERG_SINK))
}
