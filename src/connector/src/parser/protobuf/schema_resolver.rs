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

use itertools::Itertools;
use protobuf_native::compiler::{
    SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree,
};
use protobuf_native::MessageLite;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use url::Url;

use crate::parser::util::download_from_http;
use crate::schema::schema_registry::Client;

const PB_SCHEMA_LOCATION_S3_REGION: &str = "region";

pub(super) async fn load_file_descriptor_from_http(location: &Url) -> Result<Vec<u8>> {
    let schema_bytes = download_from_http(location).await?;
    Ok(schema_bytes.to_vec())
}

macro_rules! embed_wkts {
    [$( $path:literal ),+ $(,)?] => {
        &[$(
            (
                concat!("google/protobuf/", $path),
                include_bytes!(concat!(env!("PROTO_INCLUDE"), "/google/protobuf/", $path)).as_slice(),
            )
        ),+]
    };
}
const WELL_KNOWN_TYPES: &[(&str, &[u8])] = embed_wkts![
    "any.proto",
    "api.proto",
    "compiler/plugin.proto",
    "descriptor.proto",
    "duration.proto",
    "empty.proto",
    "field_mask.proto",
    "source_context.proto",
    "struct.proto",
    "timestamp.proto",
    "type.proto",
    "wrappers.proto",
];

// Pull protobuf schema and all it's deps from the confluent schema registry,
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
    for (path, bytes) in WELL_KNOWN_TYPES {
        source_tree
            .as_mut()
            .add_file(Path::new(path), bytes.to_vec());
    }

    let mut error_collector = SimpleErrorCollector::new();
    // `db` needs to be dropped before we can iterate on `error_collector`.
    let fds = {
        let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
        db.as_mut().record_errors_to(error_collector.as_mut());
        db.as_mut()
            .build_file_descriptor_set(&[Path::new(&primary_subject.name)])
    }
    .map_err(|_| {
        RwError::from(ProtocolError(format!(
            "build_file_descriptor_set failed. Errors:\n{}",
            error_collector.as_mut().join("\n")
        )))
    })?;
    fds.serialize()
        .map_err(|_| RwError::from(InternalError("serialize descriptor set failed".to_owned())))
}
