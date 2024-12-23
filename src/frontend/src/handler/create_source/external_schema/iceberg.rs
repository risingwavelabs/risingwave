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

pub async fn extract_iceberg_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::Iceberg(properties) = props {
        let table = properties.load_table_v2().await?;
        let iceberg_schema: arrow_schema_iceberg::Schema =
            ::iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;

        let mut columns: Vec<ColumnCatalog> = iceberg_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column_desc = ColumnDesc::named(
                    field.name(),
                    ColumnId::new((i + 1).try_into().unwrap()),
                    IcebergArrowConvert.type_from_field(field).unwrap(),
                );
                ColumnCatalog {
                    column_desc,
                    // hide the _row_id column for iceberg engine table
                    // This column is auto generated when users define a table without primary key
                    is_hidden: field.name() == ROWID_PREFIX,
                }
            })
            .collect();
        columns.push(ColumnCatalog::iceberg_sequence_num_column());

        Ok(columns)
    } else {
        Err(anyhow!(format!(
            "Invalid properties for iceberg source: {:?}",
            props
        )))
    }
}

pub async fn check_iceberg_source(
    props: &WithOptionsSecResolved,
    columns: &[ColumnCatalog],
) -> anyhow::Result<()> {
    let props = ConnectorProperties::extract(props.clone(), true)?;
    let ConnectorProperties::Iceberg(properties) = props else {
        return Err(anyhow!(format!(
            "Invalid properties for iceberg source: {:?}",
            props
        )));
    };

    let schema = Schema {
        fields: columns
            .iter()
            .filter(|&c| c.column_desc.name != ICEBERG_SEQUENCE_NUM_COLUMN_NAME)
            .cloned()
            .map(|c| c.column_desc.into())
            .collect(),
    };

    let table = properties.load_table_v2().await?;

    let iceberg_schema =
        ::iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;

    for f1 in schema.fields() {
        if !iceberg_schema.fields.iter().any(|f2| f2.name() == &f1.name) {
            return Err(anyhow::anyhow!(format!(
                "Column {} not found in iceberg table",
                f1.name
            )));
        }
    }

    let new_iceberg_field = iceberg_schema
        .fields
        .iter()
        .filter(|f1| schema.fields.iter().any(|f2| f1.name() == &f2.name))
        .cloned()
        .collect::<Vec<_>>();
    let new_iceberg_schema = arrow_schema_iceberg::Schema::new(new_iceberg_field);

    risingwave_connector::sink::iceberg::try_matches_arrow_schema(&schema, &new_iceberg_schema)?;

    Ok(())
}
