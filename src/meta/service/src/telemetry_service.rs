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

use risingwave_meta_model_v2::prelude::Cluster;
use risingwave_pb::meta::telemetry_info_service_server::TelemetryInfoService;
use risingwave_pb::meta::{GetTelemetryInfoRequest, TelemetryInfoResponse};
use sea_orm::EntityTrait;
use tonic::{Request, Response, Status};

use crate::controller::SqlMetaStore;
use crate::model::ClusterId;
use crate::storage::MetaStoreRef;
use crate::MetaResult;

pub struct TelemetryInfoServiceImpl {
    meta_store: MetaStoreRef,
    sql_meta_store: Option<SqlMetaStore>,
}

impl TelemetryInfoServiceImpl {
    pub fn new(meta_store: MetaStoreRef, sql_meta_store: Option<SqlMetaStore>) -> Self {
        Self {
            meta_store,
            sql_meta_store,
        }
    }

    async fn get_tracking_id(&self) -> MetaResult<Option<ClusterId>> {
        if let Some(store) = &self.sql_meta_store {
            let cluster = Cluster::find().one(&store.conn).await?;
            return Ok(cluster.map(|c| c.cluster_id.to_string().into()));
        }

        Ok(ClusterId::from_meta_store(&self.meta_store)
            .await
            .ok()
            .flatten())
    }
}

#[async_trait::async_trait]
impl TelemetryInfoService for TelemetryInfoServiceImpl {
    async fn get_telemetry_info(
        &self,
        _request: Request<GetTelemetryInfoRequest>,
    ) -> Result<Response<TelemetryInfoResponse>, Status> {
        match self.get_tracking_id().await? {
            Some(tracking_id) => Ok(Response::new(TelemetryInfoResponse {
                tracking_id: Some(tracking_id.into()),
            })),
            None => Ok(Response::new(TelemetryInfoResponse { tracking_id: None })),
        }
    }
}
