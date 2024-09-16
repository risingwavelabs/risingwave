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
use std::path::PathBuf;

use anyhow::Context as _;
use itertools::Itertools as _;
use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};
use risingwave_connector_codec::common::protobuf::compile_pb;

use super::loader::{LoadedSchema, SchemaLoader};
use super::schema_registry::Subject;
use super::{
    invalid_option_error, InvalidOptionError, SchemaFetchError, MESSAGE_NAME_KEY,
    SCHEMA_LOCATION_KEY, SCHEMA_REGISTRY_KEY,
};
use crate::connector_common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    topic: &str,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<(MessageDescriptor, Option<i32>), SchemaFetchError> {
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| invalid_option_error!("{MESSAGE_NAME_KEY} required"))?
        .clone();
    let schema_location = format_options.get(SCHEMA_LOCATION_KEY);
    let schema_registry = format_options.get(SCHEMA_REGISTRY_KEY);
    let row_schema_location = match (schema_location, schema_registry) {
        (Some(_), Some(_)) => {
            return Err(invalid_option_error!(
                "cannot use {SCHEMA_LOCATION_KEY} and {SCHEMA_REGISTRY_KEY} together"
            )
            .into())
        }
        (None, None) => {
            return Err(invalid_option_error!(
                "requires one of {SCHEMA_LOCATION_KEY} or {SCHEMA_REGISTRY_KEY}"
            )
            .into())
        }
        (None, Some(_)) => {
            let (md, sid) = fetch_from_registry(&message_name, format_options, topic).await?;
            return Ok((md, Some(sid)));
        }
        (Some(url), None) => url.clone(),
    };

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
    Ok((conf.message_descriptor, None))
}

pub async fn fetch_from_registry(
    message_name: &str,
    format_options: &BTreeMap<String, String>,
    topic: &str,
) -> Result<(MessageDescriptor, i32), SchemaFetchError> {
    let loader = SchemaLoader::from_format_options(topic, format_options)?;

    let (vid, vpb) = loader.load_val_schema::<FileDescriptor>().await?;

    Ok((
        vpb.parent_pool().get_message_by_name(message_name).unwrap(),
        vid,
    ))
}

impl LoadedSchema for FileDescriptor {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError> {
        let primary_name = primary.name.clone();
        match protox_impl::compile_pb(primary, references) {
            Err(e) => Err(SchemaFetchError::SchemaCompile(e.into())),
            Ok(b) => DescriptorPool::from_file_descriptor_set(b)
                .context("failed to convert fd set to descriptor pool")
                .and_then(|pool| {
                    pool.get_file_by_name(&primary_name)
                        .context("file lost after compilation")
                })
                .map_err(|e| SchemaFetchError::SchemaCompile(e.into())),
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

mod protox_impl {
    use std::collections::HashMap;

    use prost_types::FileDescriptorSet;
    use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
    use protox::Error;

    use crate::schema::schema_registry::Subject;

    pub fn compile_pb(
        primary_subject: Subject,
        dependency_subjects: Vec<Subject>,
    ) -> Result<FileDescriptorSet, Error> {
        struct MyResolver {
            map: HashMap<String, String>,
        }

        impl MyResolver {
            fn new(primary_subject: Subject, dependency_subjects: Vec<Subject>) -> Self {
                let map = std::iter::once(primary_subject)
                    .chain(dependency_subjects)
                    .map(|s| (s.name, s.schema.content))
                    .collect();

                Self { map }
            }
        }

        impl FileResolver for MyResolver {
            fn open_file(&self, name: &str) -> Result<File, Error> {
                if let Some(content) = self.map.get(name) {
                    Ok(File::from_source(name, content)?)
                } else {
                    Err(Error::file_not_found(name))
                }
            }
        }

        let mut resolver = ChainFileResolver::new();
        resolver.add(GoogleFileResolver::new());
        resolver.add(MyResolver::new(primary_subject, dependency_subjects));

        let fd = protox::Compiler::with_file_resolver(resolver)
            .include_imports(true)
            .file_descriptor_set();

        Ok(fd)
    }
}

pub fn compile_pb(
    primary_subject: Subject,
    dependency_subjects: Vec<Subject>,
) -> Result<Vec<u8>, SchemaFetchError> {
    compile_pb(
        (
            PathBuf::from(&primary_subject.name),
            primary_subject.schema.content.as_bytes().to_vec(),
        ),
        dependency_subjects
            .into_iter()
            .map(|s| (PathBuf::from(&s.name), s.schema.content.as_bytes().to_vec())),
    )
    .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))
}
