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

use std::collections::BTreeMap;
use std::sync::LazyLock;

use async_trait::async_trait;
use regex::Regex;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};
use risingwave_pb::cloud_service::cloud_service_server::CloudService;
use risingwave_pb::cloud_service::rw_cloud_validate_source_response::{Error, ErrorType};
use risingwave_pb::cloud_service::{
    RwCloudValidateSourceRequest, RwCloudValidateSourceResponse, SourceType,
};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};
pub struct CloudServiceImpl {}

impl CloudServiceImpl {
    pub fn new() -> Self {
        Self {}
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
        let source_cfg: BTreeMap<String, String> = req.source_config.into_iter().collect();

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
        let props = props.unwrap();

        let enumerator = props
            .create_split_enumerator(SourceEnumeratorContext::dummy().into())
            .await;
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

        Ok(Response::new(RwCloudValidateSourceResponse {
            ok: true,
            error: None,
        }))
    }
}
