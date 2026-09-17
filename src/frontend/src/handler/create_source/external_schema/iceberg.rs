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

use anyhow::Context;

use super::*;

/// TODO: make hidden columns additional columns, instead of normal columns?
pub async fn extract_iceberg_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::Iceberg(properties) = props {
        let table = properties.load_table().await?;
        columns_from_table(&table, properties.streaming_updates)
    } else {
        anyhow::bail!("invalid connector for Iceberg schema inference")
    }
}

fn columns_from_table(
    table: &::iceberg::table::Table,
    streaming_updates: bool,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let iceberg_schema: arrow_schema_iceberg::Schema =
        ::iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;

    let mut columns: Vec<ColumnCatalog> = iceberg_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| -> anyhow::Result<ColumnCatalog> {
            let column_desc = ColumnDesc::named(
                field.name(),
                ColumnId::new((i + 1).try_into().unwrap()),
                IcebergArrowConvert
                    .type_from_field(field)
                    .with_context(|| {
                        format!(
                            "failed to infer RisingWave type for Iceberg field {}",
                            field.name()
                        )
                    })?,
            );
            Ok(ColumnCatalog {
                column_desc,
                // hide the _row_id column for iceberg engine table
                // This column is auto generated when users define a table without primary key
                is_hidden: field.name() == ROW_ID_COLUMN_NAME,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if streaming_updates {
        anyhow::ensure!(
            !columns.iter().any(ColumnCatalog::is_iceberg_hidden_column),
            "Iceberg update sources cannot expose reserved physical metadata columns"
        );
        risingwave_connector::source::iceberg::update_planner::IcebergUpdateBinding::bind_columns(
            table, &columns,
        )?;
    } else {
        columns.extend(ColumnCatalog::iceberg_hidden_cols());
    }

    tracing::info!("iceberg columns: {:?}", columns);

    Ok(columns)
}

pub async fn extract_iceberg_key(
    properties: &WithOptionsSecResolved,
    columns: &[ColumnCatalog],
) -> anyhow::Result<Vec<String>> {
    let ConnectorProperties::Iceberg(properties) =
        ConnectorProperties::extract(properties.clone(), true)?
    else {
        anyhow::bail!("update source requires Iceberg")
    };
    let table = properties.load_table().await?;
    // Check the inferred schema again with the key, rather than mixing two catalog versions.
    anyhow::ensure!(
        columns_from_table(&table, true)? == columns,
        "Iceberg schema changed during source creation"
    );
    let schema = table.metadata().current_schema();
    let contract = risingwave_connector::connector_common::IcebergSourceContract::from_properties(
        table.metadata().properties(),
        schema,
    )?
    .context("Iceberg update source requires a writer contract")?;
    Ok(contract
        .key_field_ids()
        .iter()
        .map(|id| schema.field_by_id(*id).expect("validated key").name.clone())
        .collect())
}
