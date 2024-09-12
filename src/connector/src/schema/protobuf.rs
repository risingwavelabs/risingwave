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

use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};
use risingwave_connector_codec::decoder::protobuf::compile_pb;

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
        let compiled_pb = compile_pb_subject(primary, references)?;
        let pool = DescriptorPool::decode(compiled_pb.as_slice())
            .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))?;
        pool.get_file_by_name(&primary_name).ok_or_else(|| {
            SchemaFetchError::SchemaCompile(
                anyhow::anyhow!("{primary_name} lost after compilation").into(),
            )
        })
    }
}

fn compile_pb_subject(
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
