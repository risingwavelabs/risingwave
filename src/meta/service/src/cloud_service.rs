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

use std::collections::BTreeMap;
use std::sync::LazyLock;

use async_trait::async_trait;
use regex::Regex;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::kafka::private_link::insert_privatelink_broker_rewrite_map;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceProperties, SplitEnumerator,
};
use risingwave_connector::{dispatch_source_prop, WithOptionsSecResolved};
use risingwave_meta::manager::{ConnectionId, MetadataManager};
use risingwave_pb::catalog::connection::Info::PrivateLinkService;
use risingwave_pb::cloud_service::cloud_service_server::CloudService;
use risingwave_pb::cloud_service::rw_cloud_validate_source_response::{Error, ErrorType};
use risingwave_pb::cloud_service::{
    RwCloudValidateSourceRequest, RwCloudValidateSourceResponse, SourceType,
};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

use crate::rpc::cloud_provider::AwsEc2Client;

pub struct CloudServiceImpl {
    metadata_manager: MetadataManager,
    aws_client: Option<AwsEc2Client>,
}

impl CloudServiceImpl {
    pub fn new(metadata_manager: MetadataManager, aws_client: Option<AwsEc2Client>) -> Self {
        Self {
            metadata_manager,
            aws_client,
        }
    }
}

#[inline(always)]
fn new_rwc_validate_fail_response(
    error_type: ErrorType,
    error_message: String,
) -> Response<RwCloudValidateSourceResponse> {
    Response::new(RwCloudValidateSourceResponse {
        ok: false,
        error: Some(Error {
            error_type: error_type.into(),
            error_message,
        }),
    })
}

#[async_trait]
impl CloudService for CloudServiceImpl {
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
        let mut source_cfg: BTreeMap<String, String> = req.source_config.into_iter().collect();
        // if connection_id provided, check whether endpoint service is available and resolve
        // broker rewrite map currently only support aws privatelink connection
        if let Some(connection_id_str) = source_cfg.get("connection.id") {
            let connection_id = connection_id_str.parse::<ConnectionId>().map_err(|e| {
                Status::invalid_argument(format!(
                    "connection.id is not an integer: {}",
                    e.as_report()
                ))
            })?;

            let connection = self
                .metadata_manager
                .catalog_controller
                .get_connection_by_id(connection_id as _)
                .await;

            if let Err(e) = connection {
                return Ok(new_rwc_validate_fail_response(
                    ErrorType::PrivatelinkConnectionNotFound,
                    e.to_report_string(),
                ));
            }
            if let Some(PrivateLinkService(service)) = connection.unwrap().info {
                if self.aws_client.is_none() {
                    return Ok(new_rwc_validate_fail_response(
                        ErrorType::AwsClientNotConfigured,
                        "AWS client is not configured".to_string(),
                    ));
                }
                let cli = self.aws_client.as_ref().unwrap();
                let privatelink_status = cli
                    .is_vpc_endpoint_ready(service.endpoint_id.as_str())
                    .await;
                match privatelink_status {
                    Err(e) => {
                        return Ok(new_rwc_validate_fail_response(
                            ErrorType::PrivatelinkUnavailable,
                            e.to_report_string(),
                        ));
                    }
                    Ok(false) => {
                        return Ok(new_rwc_validate_fail_response(
                            ErrorType::PrivatelinkUnavailable,
                            format!("Private link endpoint {} is not ready", service.endpoint_id,),
                        ));
                    }
                    _ => (),
                };
                if let Err(e) =
                    insert_privatelink_broker_rewrite_map(&mut source_cfg, Some(&service), None)
                {
                    return Ok(new_rwc_validate_fail_response(
                        ErrorType::PrivatelinkResolveErr,
                        e.to_report_string(),
                    ));
                }
            } else {
                return Ok(new_rwc_validate_fail_response(
                    ErrorType::PrivatelinkResolveErr,
                    format!("connection {} has no info available", connection_id),
                ));
            }
        }

        // XXX: We can't use secret in cloud validate source.
        let source_cfg = WithOptionsSecResolved::without_secrets(source_cfg);

        // try fetch kafka metadata, return error message on failure
        let props = ConnectorProperties::extract(source_cfg, false);
        if let Err(e) = props {
            return Ok(new_rwc_validate_fail_response(
                ErrorType::KafkaInvalidProperties,
                e.to_report_string(),
            ));
        };

        async fn new_enumerator<P: SourceProperties>(
            props: P,
        ) -> ConnectorResult<P::SplitEnumerator> {
            P::SplitEnumerator::new(props, SourceEnumeratorContext::dummy().into()).await
        }

        dispatch_source_prop!(props.unwrap(), props, {
            let enumerator = new_enumerator(*props).await;
            if let Err(e) = enumerator {
                return Ok(new_rwc_validate_fail_response(
                    ErrorType::KafkaInvalidProperties,
                    e.to_report_string(),
                ));
            }
            if let Err(e) = enumerator.unwrap().list_splits().await {
                let error_message = e.to_report_string();
                if error_message.contains("BrokerTransportFailure") {
                    return Ok(new_rwc_validate_fail_response(
                        ErrorType::KafkaBrokerUnreachable,
                        e.to_report_string(),
                    ));
                }
                static TOPIC_NOT_FOUND: LazyLock<Regex> =
                    LazyLock::new(|| Regex::new(r"topic .* not found").unwrap());
                if TOPIC_NOT_FOUND.is_match(error_message.as_str()) {
                    return Ok(new_rwc_validate_fail_response(
                        ErrorType::KafkaTopicNotFound,
                        e.to_report_string(),
                    ));
                }
                return Ok(new_rwc_validate_fail_response(
                    ErrorType::KafkaOther,
                    e.to_report_string(),
                ));
            }
        });
        Ok(Response::new(RwCloudValidateSourceResponse {
            ok: true,
            error: None,
        }))
    }
}
