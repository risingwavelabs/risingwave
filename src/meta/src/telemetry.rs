// Copyright 2023 RisingWave Labs
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

use risingwave_common::config::MetaBackend;
use risingwave_common::telemetry::report::{TelemetryInfoFetcher, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use risingwave_pb::common::WorkerType;
use serde::{Deserialize, Serialize};

use crate::manager::ClusterManager;
use crate::model::ClusterId;
use crate::storage::MetaStore;

#[derive(Debug, Serialize, Deserialize)]
struct NodeCount {
    meta_count: u64,
    compute_count: u64,
    frontend_count: u64,
    compactor_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MetaTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
    node_count: NodeCount,
    // At this point, it will always be etcd, but we will enable telemetry when using memory.
    meta_backend: MetaBackend,
}

impl TelemetryReport for MetaTelemetryReport {
    fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
}

pub(crate) struct MetaTelemetryInfoFetcher {
    tracking_id: ClusterId,
}

impl MetaTelemetryInfoFetcher {
    pub(crate) fn new(tracking_id: ClusterId) -> Self {
        Self { tracking_id }
    }
}

#[async_trait::async_trait]
impl TelemetryInfoFetcher for MetaTelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> anyhow::Result<Option<String>> {
        Ok(Some(self.tracking_id.clone().into()))
    }
}

#[derive(Clone)]
pub(crate) struct MetaReportCreator<S: MetaStore> {
    cluster_mgr: Arc<ClusterManager<S>>,
    meta_backend: MetaBackend,
}

impl<S: MetaStore> MetaReportCreator<S> {
    pub(crate) fn new(cluster_mgr: Arc<ClusterManager<S>>, meta_backend: MetaBackend) -> Self {
        Self {
            cluster_mgr,
            meta_backend,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> TelemetryReportCreator for MetaReportCreator<S> {
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> anyhow::Result<MetaTelemetryReport> {
        let node_map = self.cluster_mgr.count_worker_node().await;
        Ok(MetaTelemetryReport {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
            },
            node_count: NodeCount {
                meta_count: *node_map.get(&WorkerType::Meta).unwrap_or(&0),
                compute_count: *node_map.get(&WorkerType::ComputeNode).unwrap_or(&0),
                frontend_count: *node_map.get(&WorkerType::Frontend).unwrap_or(&0),
                compactor_count: *node_map.get(&WorkerType::Compactor).unwrap_or(&0),
            },
            meta_backend: self.meta_backend,
        })
    }

    fn report_type(&self) -> &str {
        "meta"
    }
}
