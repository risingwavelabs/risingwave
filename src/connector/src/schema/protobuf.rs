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

use itertools::Itertools as _;
use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};

use super::loader::LoadedSchema;
use super::schema_registry::Subject;
use super::{
    invalid_option_error, InvalidOptionError, SchemaFetchError, MESSAGE_NAME_KEY,
    SCHEMA_LOCATION_KEY,
};
use crate::common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<MessageDescriptor, SchemaFetchError> {
    let row_schema_location = format_options
        .get(SCHEMA_LOCATION_KEY)
        .ok_or_else(|| invalid_option_error!("{SCHEMA_LOCATION_KEY} required"))?
        .clone();
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| invalid_option_error!("{MESSAGE_NAME_KEY} required"))?
        .clone();

    if row_schema_location.starts_with("s3") && aws_auth_props.is_none() {
        return Err(invalid_option_error!("s3 URL not supported yet").into());
    }

    let enc = EncodingProperties::Protobuf(ProtobufProperties {
        use_schema_registry: false,
        row_schema_location,
        message_name,
        aws_auth_props: aws_auth_props.cloned(),
        // name_strategy, topic, key_message_name, enable_upsert, client_config
        ..Default::default()
    });
    // Ideally, we should extract the schema loading logic from source parser to this place,
    // and call this in both source and sink.
    // But right now this function calls into source parser for its schema loading functionality.
    // This reversed dependency will be fixed when we support schema registry.
    let conf = ProtobufParserConfig::new(enc)
        .await
        .map_err(SchemaFetchError::YetToMigrate)?;
    Ok(conf.message_descriptor)
}

impl LoadedSchema for FileDescriptor {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError> {
        let primary_name = primary.name.clone();
        match compile_pb(primary, references) {
            Err(e) => Err(SchemaFetchError::SchemaCompile(e.into())),
            Ok(b) => {
                let pool = DescriptorPool::decode(b.as_slice())
                    .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))?;
                pool.get_file_by_name(&primary_name).ok_or_else(|| {
                    SchemaFetchError::SchemaCompile(
                        anyhow::anyhow!("{primary_name} lost after compilation").into(),
                    )
                })
            }
        }
    }
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

#[derive(Debug, thiserror::Error)]
pub enum PbCompileError {
    #[error("build_file_descriptor_set failed\n{}", errs.iter().map(|e| format!("\t{e}")).join("\n"))]
    Build {
        errs: Vec<protobuf_native::compiler::FileLoadError>,
    },
    #[error("serialize descriptor set failed")]
    Serialize,
}

pub fn compile_pb(
    primary_subject: Subject,
    dependency_subjects: Vec<Subject>,
) -> Result<Vec<u8>, PbCompileError> {
    use std::iter;
    use std::path::Path;

    use protobuf_native::compiler::{
        SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree,
    };
    use protobuf_native::MessageLite;

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
    .map_err(|_| PbCompileError::Build {
        errs: error_collector.as_mut().collect(),
    })?;
    fds.serialize().map_err(|_| PbCompileError::Serialize)
}
