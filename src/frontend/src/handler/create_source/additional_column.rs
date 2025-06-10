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

// check the additional column compatibility with the format and encode
fn check_additional_column_compatibility(
    column_def: &IncludeOptionItem,
    format_encode: Option<&FormatEncodeOptions>,
) -> Result<()> {
    // only allow header column have inner field
    if column_def.inner_field.is_some()
        && !column_def
            .column_type
            .real_value()
            .eq_ignore_ascii_case("header")
    {
        return Err(RwError::from(ProtocolError(format!(
            "Only header column can have inner field, but got {:?}",
            column_def.column_type.real_value(),
        ))));
    }

    // Payload column only allowed when encode is JSON
    if let Some(schema) = format_encode
        && column_def
            .column_type
            .real_value()
            .eq_ignore_ascii_case("payload")
        && !matches!(schema.row_encode, Encode::Json)
    {
        return Err(RwError::from(ProtocolError(format!(
            "INCLUDE payload is only allowed when using ENCODE JSON, but got ENCODE {:?}",
            schema.row_encode
        ))));
    }
    Ok(())
}

/// add connector-spec columns to the end of column catalog
pub fn handle_addition_columns(
    format_encode: Option<&FormatEncodeOptions>,
    with_properties: &BTreeMap<String, String>,
    mut additional_columns: IncludeOption,
    columns: &mut Vec<ColumnCatalog>,
    is_cdc_backfill_table: bool,
) -> Result<()> {
    let connector_name = with_properties.get_connector().unwrap(); // there must be a connector in source

    if get_supported_additional_columns(connector_name.as_str(), is_cdc_backfill_table).is_none()
        && !additional_columns.is_empty()
    {
        return Err(RwError::from(ProtocolError(format!(
            "Connector {} accepts no additional column but got {:?}",
            connector_name, additional_columns
        ))));
    }

    while let Some(item) = additional_columns.pop() {
        check_additional_column_compatibility(&item, format_encode)?;

        let data_type = item
            .header_inner_expect_type
            .map(|dt| bind_data_type(&dt))
            .transpose()?;
        if let Some(dt) = &data_type
            && !matches!(dt, DataType::Bytea | DataType::Varchar)
        {
            return Err(
                ErrorCode::BindError(format!("invalid additional column data type: {dt}")).into(),
            );
        }

        if item.column_alias.is_none() {
            // still need column_alias be an Option because we still need derive column for kafka timestamp and offset
            return Err(RwError::from(ProtocolError(
                "additional column alias is required, you can use `INCLUDE ... AS <alias>` to specify an alias".to_owned(),
            )));
        }

        let col = build_additional_column_desc(
            ColumnId::placeholder(),
            connector_name.as_str(),
            item.column_type.real_value().as_str(),
            item.column_alias.map(|alias| alias.real_value()),
            item.inner_field.as_deref(),
            data_type.as_ref(),
            true,
            is_cdc_backfill_table,
        )?;
        columns.push(ColumnCatalog::visible(col));
    }

    Ok(())
}

// Add a hidden column `_rw_kafka_timestamp` to each message from Kafka source.
pub fn check_and_add_timestamp_column(
    with_properties: &WithOptions,
    columns: &mut Vec<ColumnCatalog>,
) {
    if with_properties.is_kafka_connector() {
        if columns.iter().any(|col| {
            matches!(
                col.column_desc.additional_column.column_type,
                Some(AdditionalColumnType::Timestamp(_))
            )
        }) {
            // already has timestamp column, no need to add a new one
            return;
        }

        // add a hidden column `_rw_kafka_timestamp` to each message from Kafka source
        let col = build_additional_column_desc(
            ColumnId::placeholder(),
            KAFKA_CONNECTOR,
            "timestamp",
            Some(KAFKA_TIMESTAMP_COLUMN_NAME.to_owned()),
            None,
            None,
            true,
            false,
        )
        .unwrap();
        columns.push(ColumnCatalog::hidden(col));
    }
}
