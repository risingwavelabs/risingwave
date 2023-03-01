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

use std::collections::HashMap;
use std::iter;
use std::path::Path;

use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use risingwave_common::error::ErrorCode::{InternalError, InvalidConfigValue, ProtocolError};
use risingwave_common::error::{Result, RwError};
use url::Url;

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use crate::parser::schema_registry::Client;
use crate::parser::util::download_from_http;

const PB_SCHEMA_LOCATION_S3_REGION: &str = "region";

// TODO(Tao): Probably we should never allow to use S3 URI.
pub(super) async fn load_file_descriptor_from_s3(
    location: &Url,
    properties: &HashMap<String, String>,
) -> Result<Vec<u8>> {
    let bucket = location.domain().ok_or_else(|| {
        RwError::from(InternalError(format!(
            "Illegal Protobuf schema path {}",
            location
        )))
    })?;
    if properties.get(PB_SCHEMA_LOCATION_S3_REGION).is_none() {
        return Err(RwError::from(InvalidConfigValue {
            config_entry: PB_SCHEMA_LOCATION_S3_REGION.to_string(),
            config_value: "NONE".to_string(),
        }));
    }
    let key = location.path().replace('/', "");
    let config = AwsConfigV2::from(properties.clone());
    let sdk_config = config.load_config(None).await;
    let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
    let response = s3_client
        .get_object()
        .bucket(bucket.to_string())
        .key(&key)
        .send()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;

    let body = response.body.collect().await.map_err(|e| {
        RwError::from(InternalError(format!(
            "Read Protobuf schema file from s3 {}",
            e
        )))
    })?;
    Ok(body.into_bytes().to_vec())
}

pub(super) async fn load_file_descriptor_from_http(location: &Url) -> Result<Vec<u8>> {
    let schema_bytes = download_from_http(location).await?;
    Ok(schema_bytes.to_vec())
}

// Pull protobuf schema and all it's deps from the confluent schema regitry,
// and compile then into one file descriptor
pub(super) async fn compile_file_descriptor_from_schema_registry(
    subject_name: &str,
    client: &Client,
) -> Result<Vec<u8>> {
    let (primary_subject, dependency_subjects) =
        client.get_subject_and_references(subject_name).await?;

    // Compile .proto files into a file descriptor set.
    let mut source_tree = VirtualSourceTree::new();
    for subject in iter::once(&primary_subject).chain(dependency_subjects.iter()) {
        source_tree.as_mut().add_file(
            Path::new(&subject.name),
            subject.schema.content.as_bytes().to_vec(),
        );
    }
    let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
    let fds = db
        .as_mut()
        .build_file_descriptor_set(&[Path::new(&primary_subject.name)])
        .map_err(|e| {
            RwError::from(ProtocolError(format!(
                "build_file_descriptor_set failed, {}",
                e
            )))
        })?;
    fds.serialize()
        .map_err(|_| RwError::from(InternalError("serialize descriptor set failed".to_owned())))
}
