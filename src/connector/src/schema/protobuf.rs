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

use std::collections::BTreeMap;

use prost_reflect::MessageDescriptor;

use super::{SchemaFetchError, MESSAGE_NAME_KEY, SCHEMA_LOCATION_KEY};
use crate::common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<MessageDescriptor, SchemaFetchError> {
    let row_schema_location = format_options
        .get(SCHEMA_LOCATION_KEY)
        .ok_or_else(|| SchemaFetchError(format!("{SCHEMA_LOCATION_KEY} required")))?
        .clone();
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| SchemaFetchError(format!("{MESSAGE_NAME_KEY} required")))?
        .clone();

    if row_schema_location.starts_with("s3") && aws_auth_props.is_none() {
        return Err(SchemaFetchError("s3 URL not supported yet".into()));
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
        .map_err(|e| SchemaFetchError(e.to_string()))?;
    Ok(conf.message_descriptor)
}
