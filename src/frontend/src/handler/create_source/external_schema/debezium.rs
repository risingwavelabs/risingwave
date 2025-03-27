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

use risingwave_connector::source::cdc::CDC_MONGODB_STRONG_SCHEMA_KEY;

use super::*;

pub async fn extract_debezium_avro_table_pk_columns(
    info: &StreamSourceInfo,
    with_properties: &WithOptionsSecResolved,
) -> Result<Vec<String>> {
    let parser_config = SpecificParserConfig::new(info, with_properties)?;
    let conf = DebeziumAvroParserConfig::new(parser_config.encoding_config).await?;
    Ok(conf.extract_pks()?.drain(..).map(|c| c.name).collect())
}

pub fn check_mongodb_cdc_encode(
    props: &WithOptionsSecResolved,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let strong_schema = props
        .get(CDC_MONGODB_STRONG_SCHEMA_KEY)
        .map(|t| t.to_ascii_lowercase())
        .is_some_and(|t| t == "true");
    let non_user_generated = columns.iter().filter(|c| !c.is_generated()).collect_vec();

    // 2025-03-25:
    // Though already checked if contains a '_id' field
    // in `risingwave_frontend::handler::create_source::bind_create_source_or_table_with_connector`
    // with `risingwave_frontend::handler::create_source::bind_all_columns`
    //
    // Still we check it if we met a change in the future
    let type_matches = [
        DataType::Varchar,
        DataType::Int32,
        DataType::Int64,
        DataType::Jsonb,
    ]
    .contains(&columns[0].column_desc.data_type);
    if non_user_generated.len() < 1
        || non_user_generated[0].column_desc.name != "_id"
        || !type_matches
    {
        return Err(RwError::from(ProtocolError(format!(
            "the not generated columns of the source with row format DebeziumMongoJson must contains key _id [Jsonb | Varchar | Int32 | Int64] as its very first field, got: {:?}",
            non_user_generated
                .iter()
                .map(|c| &c.column_desc.name)
                .collect_vec()
        ))));
    }

    // for strong_schema enabled sources, we don't check value columns
    if strong_schema {
        return Ok(());
    }

    // for non strong_schema enabled sources, ensure the second column is 'payload' Jsonb
    if non_user_generated.len() != 2
        || non_user_generated[0].column_desc.name != "_id"
        || non_user_generated[1].column_desc.name != "payload"
    {
        return Err(RwError::from(ProtocolError(format!(
            "the not generated columns of the source with row format DebeziumMongoJson must be (_id [Jsonb | Varchar | Int32 | Int64], payload Jsonb), got: {:?}",
            non_user_generated
                .iter()
                .map(|c| &c.column_desc.name)
                .collect_vec()
        ))));
    }

    let value_data_type = &non_user_generated[1].column_desc.data_type;
    if *value_data_type != DataType::Jsonb {
        return Err(RwError::from(ProtocolError(format!(
            "the payload column of the source with row format DebeziumMongoJson must be Jsonb, got: {:?}",
            value_data_type
        ))));
    }

    Ok(())
}

pub fn check_kafka_encode(props: &WithOptionsSecResolved, columns: &[ColumnCatalog]) -> Result<()> {
    // check if user is connecting to mongodb through kafka with DebeziumMongoJson
    match props
        .get("connector")
        .map(|c| c.to_ascii_lowercase())
        .is_some_and(|c| c == "mongodb")
    {
        true => check_mongodb_cdc_encode(props, columns),
        false => Ok(()),
    }
}
