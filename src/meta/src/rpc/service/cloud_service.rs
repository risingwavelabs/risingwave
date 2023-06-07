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
use std::sync::LazyLock;

use async_trait::async_trait;
use regex::Regex;
use risingwave_connector::source::kafka::private_link::insert_privatelink_broker_rewrite_map;
use risingwave_connector::source::{ConnectorProperties, SplitEnumeratorImpl};
use risingwave_pb::catalog::connection::Info::PrivateLinkService;
use risingwave_pb::cloud_service::cloud_service_server::CloudService;
use risingwave_pb::cloud_service::rw_cloud_validate_source_response::{Error, ErrorType};
use risingwave_pb::cloud_service::{
    RwCloudValidateSourceRequest, RwCloudValidateSourceResponse, SourceType,
};
use tonic::{Request, Response, Status};

use crate::manager::CatalogManagerRef;
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::storage::MetaStore;

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

#[inline(always)]
fn new_rw_cloud_validate_source_err(
    error_type: ErrorType,
    error_message: String,
) -> Response<RwCloudValidateSourceResponse> {
    Response::new(RwCloudValidateSourceResponse {
        error: Some(Error {
            error_type: error_type.into(),
            error_message,
        }),
    })
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
        // if connection_name provided, check whether endpoint service is available and resolve
        // broker rewrite map currently only support aws privatelink connection
        if let Some(connection_name) = source_cfg.get("connection.name") {
            let connection = self
                .catalog_manager
                .get_connection_by_name(connection_name)
                .await;
            if let Err(e) = connection {
                return Ok(new_rw_cloud_validate_source_err(
                    ErrorType::PrivatelinkConnectionNotFound,
                    e.to_string(),
                ));
            }
            if let Some(info) = connection.unwrap().info {
                match info {
                    PrivateLinkService(service) => {
                        let privatelink_status = cli
                            .is_vpc_endpoint_ready(service.endpoint_id.as_str())
                            .await;
                        if let Err(e) = privatelink_status {
                            return Ok(new_rw_cloud_validate_source_err(
                                ErrorType::PrivatelinkUnavailable,
                                e.to_string(),
                            ));
                        }
                        if !privatelink_status.unwrap() {
                            return Ok(new_rw_cloud_validate_source_err(
                                ErrorType::PrivatelinkUnavailable,
                                format!(
                                    "Private link endpoint {} is not ready",
                                    service.endpoint_id,
                                ),
                            ));
                        }
                        if let Err(e) =
                            insert_privatelink_broker_rewrite_map(&service, &mut source_cfg)
                        {
                            return Ok(new_rw_cloud_validate_source_err(
                                ErrorType::PrivatelinkResolveErr,
                                e.to_string(),
                            ));
                        }
                    }
                }
            } else {
                return Ok(new_rw_cloud_validate_source_err(
                    ErrorType::PrivatelinkResolveErr,
                    format!("connection {} has no info available", connection_name),
                ));
            }
        }
        // try fetch kafka metadata, return error message on failure
        let source_cfg: HashMap<String, String> =
            source_cfg.into_iter().map(|(k, v)| (k, v)).collect();
        let props = ConnectorProperties::extract(source_cfg);
        if let Err(e) = props {
            return Ok(new_rw_cloud_validate_source_err(
                ErrorType::KafkaInvalidProperties,
                e.to_string(),
            ));
        };
        let enumerator = SplitEnumeratorImpl::create(props.unwrap()).await;
        if let Err(e) = enumerator {
            return Ok(Response::new(RwCloudValidateSourceResponse {
                error: Some(Error {
                    error_type: ErrorType::KafkaInvalidProperties.into(),
                    error_message: e.to_string(),
                }),
            }));
        }
        if let Err(e) = enumerator.unwrap().list_splits().await {
            let error_message = e.to_string();
            if error_message.contains("BrokerTransportFailure") {
                return Ok(Response::new(RwCloudValidateSourceResponse {
                    error: Some(Error {
                        error_type: ErrorType::KafkaBrokerUnreachable.into(),
                        error_message: e.to_string(),
                    }),
                }));
            }
            static TOPIC_NOT_FOUND: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r"topic .* not found").unwrap());
            if TOPIC_NOT_FOUND.is_match(error_message.as_str()) {
                return Ok(Response::new(RwCloudValidateSourceResponse {
                    error: Some(Error {
                        error_type: ErrorType::KafkaTopicNotFound.into(),
                        error_message: e.to_string(),
                    }),
                }));
            }
            return Ok(Response::new(RwCloudValidateSourceResponse {
                error: Some(Error {
                    error_type: ErrorType::KafkaOther.into(),
                    error_message: e.to_string(),
                }),
            }));
        }
        Ok(Response::new(RwCloudValidateSourceResponse { error: None }))
    }
}
