// Copyright 2024 RisingWave Labs
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

use prost::Message;
use risingwave_common::config::MetaBackend;
use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
use risingwave_common::telemetry::report::{TelemetryInfoFetcher, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, report_event_common, telemetry_cluster_type_from_env_var, SystemData,
    TelemetryNodeType, TelemetryReportBase, TelemetryResult,
};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_pb::common::WorkerType;
use risingwave_pb::telemetry::{
    PbTelemetryClusterType, PbTelemetryDatabaseObject, PbTelemetryEventStage,
};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::manager::MetadataManager;
use crate::model::ClusterId;

const TELEMETRY_META_REPORT_TYPE: &str = "meta";

pub(crate) fn report_event(
    event_stage: PbTelemetryEventStage,
    event_name: &str,
    catalog_id: i64,
    connector_name: Option<String>,
    component: Option<PbTelemetryDatabaseObject>,
    attributes: Option<jsonbb::Value>, // any json string
) {
    report_event_common(
        event_stage,
        event_name,
        catalog_id,
        connector_name,
        component,
        attributes,
        TELEMETRY_META_REPORT_TYPE.to_string(),
    );
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeCount {
    meta_count: u64,
    compute_count: u64,
    frontend_count: u64,
    compactor_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RwVersion {
    version: String,
    git_sha: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PlanOptimization {
    // todo: add optimization applied to each job
    Placeholder,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaTelemetryJobDesc {
    pub table_id: i32,
    pub connector: Option<String>,
    pub optimization: Vec<PlanOptimization>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
    node_count: NodeCount,
    streaming_job_count: u64,
    meta_backend: MetaBackend,
    rw_version: RwVersion,
    job_desc: Vec<MetaTelemetryJobDesc>,

    // Get the ENV from key `TELEMETRY_CLUSTER_TYPE`
    cluster_type: PbTelemetryClusterType,
    object_store_media_type: &'static str,
}

impl From<MetaTelemetryJobDesc> for risingwave_pb::telemetry::StreamJobDesc {
    fn from(val: MetaTelemetryJobDesc) -> Self {
        risingwave_pb::telemetry::StreamJobDesc {
            table_id: val.table_id,
            connector_name: val.connector,
            plan_optimizations: val
                .optimization
                .iter()
                .map(|opt| match opt {
                    PlanOptimization::Placeholder => {
                        risingwave_pb::telemetry::PlanOptimization::TableOptimizationUnspecified
                            as i32
                    }
                })
                .collect(),
        }
    }
}

impl TelemetryToProtobuf for MetaTelemetryReport {
    fn to_pb_bytes(self) -> Vec<u8> {
        let pb_report = risingwave_pb::telemetry::MetaReport {
            base: Some(self.base.into()),
            meta_backend: match self.meta_backend {
                MetaBackend::Mem => risingwave_pb::telemetry::MetaBackend::Memory as i32,
                MetaBackend::Sql
                | MetaBackend::Sqlite
                | MetaBackend::Postgres
                | MetaBackend::Mysql => risingwave_pb::telemetry::MetaBackend::Rdb as i32,
            },
            node_count: Some(risingwave_pb::telemetry::NodeCount {
                meta: self.node_count.meta_count as u32,
                compute: self.node_count.compute_count as u32,
                frontend: self.node_count.frontend_count as u32,
                compactor: self.node_count.compactor_count as u32,
            }),
            rw_version: Some(risingwave_pb::telemetry::RwVersion {
                rw_version: self.rw_version.version,
                git_sha: self.rw_version.git_sha,
            }),
            stream_job_count: self.streaming_job_count as u32,
            stream_jobs: self.job_desc.into_iter().map(|job| job.into()).collect(),
            cluster_type: self.cluster_type as i32,
            object_store_media_type: self.object_store_media_type.to_string(),
        };
        pb_report.encode_to_vec()
    }
}

pub struct MetaTelemetryInfoFetcher {
    tracking_id: ClusterId,
}

impl MetaTelemetryInfoFetcher {
    pub fn new(tracking_id: ClusterId) -> Self {
        Self { tracking_id }
    }
}

#[async_trait::async_trait]
impl TelemetryInfoFetcher for MetaTelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> TelemetryResult<Option<String>> {
        Ok(Some(self.tracking_id.clone().into()))
    }
}

#[derive(Clone)]
pub struct MetaReportCreator {
    metadata_manager: MetadataManager,
    object_store_media_type: &'static str,
}

impl MetaReportCreator {
    pub fn new(metadata_manager: MetadataManager, object_store_media_type: &'static str) -> Self {
        Self {
            metadata_manager,
            object_store_media_type,
        }
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for MetaReportCreator {
    #[expect(refining_impl_trait)]
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> TelemetryResult<MetaTelemetryReport> {
        let node_map = self
            .metadata_manager
            .count_worker_node()
            .await
            .map_err(|err| err.as_report().to_string())?;

        let streaming_job_count = self
            .metadata_manager
            .count_streaming_job()
            .await
            .map_err(|err| err.as_report().to_string())? as u64;
        let stream_job_desc = self
            .metadata_manager
            .list_stream_job_desc()
            .await
            .map_err(|err| err.as_report().to_string())?;

        Ok(MetaTelemetryReport {
            rw_version: RwVersion {
                version: RW_VERSION.to_string(),
                git_sha: GIT_SHA.to_string(),
            },
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
                is_test: false,
            },
            node_count: NodeCount {
                meta_count: *node_map.get(&WorkerType::Meta).unwrap_or(&0),
                compute_count: *node_map.get(&WorkerType::ComputeNode).unwrap_or(&0),
                frontend_count: *node_map.get(&WorkerType::Frontend).unwrap_or(&0),
                compactor_count: *node_map.get(&WorkerType::Compactor).unwrap_or(&0),
            },
            streaming_job_count,
            meta_backend: MetaBackend::Sql,
            job_desc: stream_job_desc,
            cluster_type: telemetry_cluster_type_from_env_var(),
            object_store_media_type: self.object_store_media_type,
        })
    }

    fn report_type(&self) -> &str {
        TELEMETRY_META_REPORT_TYPE
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::config::MetaBackend;
    use risingwave_common::telemetry::{
        current_timestamp, SystemData, TelemetryNodeType, TelemetryReportBase,
    };
    use risingwave_pb::telemetry::PbTelemetryClusterType;

    use crate::telemetry::{MetaTelemetryReport, NodeCount, RwVersion};

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_meta_telemetry_report() {
        use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
        use risingwave_common::telemetry::{post_telemetry_report_pb, TELEMETRY_REPORT_URL};

        use crate::telemetry::TELEMETRY_META_REPORT_TYPE;

        // we don't call `create_report` here because it relies on the metadata manager
        let report = MetaTelemetryReport {
            base: TelemetryReportBase {
                tracking_id: "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_owned(),
                session_id: "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_owned(),
                system_data: SystemData::new(),
                up_time: 100,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
                is_test: true,
            },
            node_count: NodeCount {
                meta_count: 1,
                compute_count: 2,
                frontend_count: 3,
                compactor_count: 4,
            },
            streaming_job_count: 5,
            meta_backend: MetaBackend::Sql,
            rw_version: RwVersion {
                version: "version".to_owned(),
                git_sha: "git_sha".to_owned(),
            },
            job_desc: vec![],
            cluster_type: PbTelemetryClusterType::Unspecified,
            object_store_media_type: "s3",
        };

        let pb_bytes = report.to_pb_bytes();
        let url = (TELEMETRY_REPORT_URL.to_owned() + "/" + TELEMETRY_META_REPORT_TYPE).to_owned();
        let result = post_telemetry_report_pb(&url, pb_bytes).await;
        assert!(result.is_ok());
    }
}
