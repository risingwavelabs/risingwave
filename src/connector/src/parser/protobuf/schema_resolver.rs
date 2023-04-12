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

use std::iter;
use std::path::Path;

use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use url::Url;

use crate::parser::schema_registry::Client;
use crate::parser::util::download_from_http;

const PB_SCHEMA_LOCATION_S3_REGION: &str = "region";

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
