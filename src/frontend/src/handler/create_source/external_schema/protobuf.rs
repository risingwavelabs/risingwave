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

use super::*;

/// Map a protobuf schema to a relational schema.
pub async fn extract_protobuf_table_schema(
    schema: &ProtobufSchema,
    with_properties: &WithOptionsSecResolved,
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let info = StreamSourceInfo {
        proto_message_name: schema.message_name.0.clone(),
        row_schema_location: schema.row_schema_location.0.clone(),
        use_schema_registry: schema.use_schema_registry,
        format: FormatType::Plain.into(),
        row_encode: EncodeType::Protobuf.into(),
        format_encode_options: format_encode_options.clone(),
        ..Default::default()
    };
    let parser_config = SpecificParserConfig::new(&info, with_properties)?;
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_USERNAME);
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_PASSWORD);
    consume_aws_config_from_options(format_encode_options);

    let conf = ProtobufParserConfig::new(parser_config.encoding_config).await?;

    let column_descs = conf.map_to_columns()?;

    Ok(column_descs
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}
