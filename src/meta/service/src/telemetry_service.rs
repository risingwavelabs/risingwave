// Copyright 2025 RisingWave Labs
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

use risingwave_common::telemetry::telemetry_cluster_type_from_env_var;
use risingwave_meta::controller::SqlMetaStore;
use risingwave_meta_model::prelude::Cluster;
use risingwave_pb::meta::telemetry_info_service_server::TelemetryInfoService;
use risingwave_pb::meta::{GetTelemetryInfoRequest, TelemetryInfoResponse};
use sea_orm::EntityTrait;
use tonic::{Request, Response, Status};

use crate::MetaResult;
use crate::model::ClusterId;

pub struct TelemetryInfoServiceImpl {
    meta_store_impl: SqlMetaStore,
}

impl TelemetryInfoServiceImpl {
    pub fn new(meta_store_impl: SqlMetaStore) -> Self {
        Self { meta_store_impl }
    }

    async fn get_tracking_id(&self) -> MetaResult<Option<ClusterId>> {
        let cluster = Cluster::find().one(&self.meta_store_impl.conn).await?;
        let cluster_id = cluster.map(|c| c.cluster_id.to_string().into());
        Ok(cluster_id)
    }
}

#[async_trait::async_trait]
impl TelemetryInfoService for TelemetryInfoServiceImpl {
    async fn get_telemetry_info(
        &self,
        _request: Request<GetTelemetryInfoRequest>,
    ) -> Result<Response<TelemetryInfoResponse>, Status> {
        if telemetry_cluster_type_from_env_var().is_err() {
            return Ok(Response::new(TelemetryInfoResponse { tracking_id: None }));
        }
        match self.get_tracking_id().await? {
            Some(tracking_id) => Ok(Response::new(TelemetryInfoResponse {
                tracking_id: Some(tracking_id.into()),
            })),
            None => Ok(Response::new(TelemetryInfoResponse { tracking_id: None })),
        }
    }
}
