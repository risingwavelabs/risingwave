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
// limitations under the License.use risingwave

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use risingwave_connector::source::kafka::private_link::insert_privatelink_broker_rewrite_map;
use risingwave_connector::source::{ConnectorProperties, SplitEnumeratorImpl};
use risingwave_pb::catalog::connection::Info::PrivateLinkService;
use risingwave_pb::connector_service::{SourceType, ValidationError};
use risingwave_pb::meta::cloud_service_server::CloudService;
use risingwave_pb::meta::{RwCloudValidateSourceRequest, RwCloudValidateSourceResponse};
use tonic::{Request, Response, Status};

use crate::manager::CatalogManagerRef;
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::storage::MetaStore;
use crate::MetaError;

pub struct CloudServiceImpl<S>
where
    S: MetaStore,
{
    catalog_manager: CatalogManagerRef<S>,
    aws_client: Option<AwsEc2Client>,
}

impl<S: MetaStore> CloudServiceImpl<S> {
    pub fn new(catalog_manager: CatalogManagerRef<S>, aws_client: Option<AwsEc2Client>) -> Self {
        Self {
            catalog_manager,
            aws_client,
        }
    }
}

#[async_trait]
impl<S> CloudService for CloudServiceImpl<S>
where
    S: MetaStore,
{
    async fn rw_cloud_validate_source(
        &self,
        request: Request<RwCloudValidateSourceRequest>,
    ) -> Result<Response<RwCloudValidateSourceResponse>, Status> {
        let req = request.into_inner();
        if req.source_type() != SourceType::Kafka {
            return Err(Status::invalid_argument(
                "unexpected source type, only kafka source is supported",
            ));
        }
        if self.aws_client.is_none() {
            return Err(Status::internal("AWS client is not configured"));
        }
        let cli = self.aws_client.as_ref().unwrap();
        let mut source_cfg: BTreeMap<String, String> =
            req.source_config.into_iter().map(|(k, v)| (k, v)).collect();
        // if connection_name provided, check whether endpoint service is available
        // currently only support aws privatelink connection
        if let Some(connection_name) = source_cfg.get("connection.name") {
            let connection = self
                .catalog_manager
                .get_connection_by_name(connection_name)
                .await
                .map_err(Status::from)?;
            if let Some(info) = connection.info {
                match info {
                    PrivateLinkService(service) => {
                        if !cli
                            .is_vpc_endpoint_ready(service.endpoint_id.as_str())
                            .await
                            .map_err(Status::from)?
                        {
                            return Err(MetaError::unavailable(format!(
                                "Private link endpoint {} is not ready",
                                service.endpoint_id
                            ))
                            .into());
                        }
                        insert_privatelink_broker_rewrite_map(&service, &mut source_cfg)
                            .map_err(|e| Status::from(MetaError::from(e)))?;
                    }
                }
            } else {
                return Err(Status::from(MetaError::unavailable(format!(
                    "connection {} has no info available",
                    connection_name
                ))));
            }
        }
        // try fetch kafka metadata, return error message on failure
        let source_cfg: HashMap<String, String> =
            source_cfg.into_iter().map(|(k, v)| (k, v)).collect();
        let props = ConnectorProperties::extract(source_cfg)
            .map_err(|e| Status::from(MetaError::from(e)))?;
        let mut enumerator = SplitEnumeratorImpl::create(props)
            .await
            .map_err(|e| Status::from(MetaError::from(e)))?;
        if let Err(e) = enumerator.list_splits().await {
            return Ok(Response::new(RwCloudValidateSourceResponse {
                error: Some(ValidationError {
                    error_message: e.to_string(),
                }),
            }));
        }
        Ok(Response::new(RwCloudValidateSourceResponse { error: None }))
    }
}
