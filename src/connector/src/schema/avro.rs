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
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use risingwave_pb::catalog::PbSchemaRegistryNameStrategy;

use super::schema_registry::{
    get_subject_by_strategy, handle_sr_list, name_strategy_from_str, Client, ConfluentSchema,
    SchemaRegistryAuth,
};
use super::{
    SchemaFetchError, KEY_MESSAGE_NAME_KEY, MESSAGE_NAME_KEY, NAME_STRATEGY_KEY,
    SCHEMA_REGISTRY_KEY,
};

pub struct SchemaWithId {
    pub schema: Arc<AvroSchema>,
    pub id: i32,
}

impl TryFrom<ConfluentSchema> for SchemaWithId {
    type Error = SchemaFetchError;

    fn try_from(fetched: ConfluentSchema) -> Result<Self, Self::Error> {
        let parsed =
            AvroSchema::parse_str(&fetched.content).map_err(|e| SchemaFetchError(e.to_string()))?;
        Ok(Self {
            schema: Arc::new(parsed),
            id: fetched.id,
        })
    }
}

/// Schema registry only
pub async fn fetch_schema(
    format_options: &BTreeMap<String, String>,
    topic: &str,
) -> Result<(SchemaWithId, SchemaWithId), SchemaFetchError> {
    let schema_location = format_options
        .get(SCHEMA_REGISTRY_KEY)
        .ok_or_else(|| SchemaFetchError(format!("{SCHEMA_REGISTRY_KEY} required")))?
        .clone();
    let client_config = format_options.into();
    let name_strategy = format_options
        .get(NAME_STRATEGY_KEY)
        .map(|s| {
            name_strategy_from_str(s)
                .ok_or_else(|| SchemaFetchError(format!("unrecognized strategy {s}")))
        })
        .transpose()?
        .unwrap_or_default();
    let key_record_name = format_options
        .get(KEY_MESSAGE_NAME_KEY)
        .map(std::ops::Deref::deref);
    let val_record_name = format_options
        .get(MESSAGE_NAME_KEY)
        .map(std::ops::Deref::deref);

    let (key_schema, val_schema) = fetch_schema_inner(
        &schema_location,
        &client_config,
        &name_strategy,
        topic,
        key_record_name,
        val_record_name,
    )
    .await
    .map_err(|e| SchemaFetchError(e.to_string()))?;

    Ok((key_schema.try_into()?, val_schema.try_into()?))
}

async fn fetch_schema_inner(
    schema_location: &str,
    client_config: &SchemaRegistryAuth,
    name_strategy: &PbSchemaRegistryNameStrategy,
    topic: &str,
    key_record_name: Option<&str>,
    val_record_name: Option<&str>,
) -> Result<(ConfluentSchema, ConfluentSchema), risingwave_common::error::RwError> {
    let urls = handle_sr_list(schema_location)?;
    let client = Client::new(urls, client_config)?;

    let key_subject = get_subject_by_strategy(name_strategy, topic, key_record_name, true)?;
    let key_schema = client.get_schema_by_subject(&key_subject).await?;

    let val_subject = get_subject_by_strategy(name_strategy, topic, val_record_name, false)?;
    let val_schema = client.get_schema_by_subject(&val_subject).await?;

    Ok((key_schema, val_schema))
}
