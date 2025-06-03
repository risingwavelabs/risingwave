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

use super::*;

pub fn json_schema_infer_use_schema_registry(schema_config: &Option<(AstString, bool)>) -> bool {
    match schema_config {
        None => false,
        Some((_, use_registry)) => *use_registry,
    }
}

/// Map a JSON schema to a relational schema
pub async fn extract_json_table_schema(
    schema_config: &Option<(AstString, bool)>,
    with_properties: &BTreeMap<String, String>,
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Option<Vec<ColumnCatalog>>> {
    match schema_config {
        None => Ok(None),
        Some((schema_location, use_schema_registry)) => {
            let schema_registry_auth = use_schema_registry.then(|| {
                let config: SchemaRegistryConfig =
                    SchemaRegistryConfig::from(&*format_encode_options);
                try_consume_schema_registry_config_from_options(format_encode_options);
                config
            });
            Ok(Some(
                fetch_json_schema_and_map_to_columns(
                    &schema_location.0,
                    schema_registry_auth,
                    with_properties,
                )
                .await?
                .into_iter()
                .map(|col| ColumnCatalog {
                    column_desc: ColumnDesc::from_field_without_column_id(&col),
                    is_hidden: false,
                })
                .collect_vec(),
            ))
        }
    }
}

pub fn get_json_schema_location(
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Option<(AstString, bool)>> {
    let schema_location = try_consume_string_from_options(format_encode_options, "schema.location");
    let schema_registry = try_consume_string_from_options(format_encode_options, "schema.registry");
    match (schema_location, schema_registry) {
        (None, None) => Ok(None),
        (None, Some(schema_registry)) => Ok(Some((schema_registry, true))),
        (Some(schema_location), None) => Ok(Some((schema_location, false))),
        (Some(_), Some(_)) => Err(RwError::from(ProtocolError(
            "only need either the schema location or the schema registry".to_owned(),
        ))),
    }
}
