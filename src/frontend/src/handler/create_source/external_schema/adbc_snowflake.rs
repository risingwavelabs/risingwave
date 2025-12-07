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

use anyhow::anyhow;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::adbc_snowflake::AdbcSnowflakeArrowConvert;

use crate::WithOptionsSecResolved;

/// Extract column schema from ADBC Snowflake query result.
/// This function infers the schema by executing the query and examining the result schema.
pub async fn extract_adbc_snowflake_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::AdbcSnowflake(properties) = props {
        // Get the Arrow schema from Snowflake
        let arrow_schema = properties.get_arrow_schema()?;

        // Convert Arrow schema to RisingWave columns
        let converter = AdbcSnowflakeArrowConvert;
        let columns: Vec<ColumnCatalog> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column_desc = ColumnDesc::named(
                    field.name(),
                    ColumnId::new((i + 1).try_into().unwrap()),
                    converter.type_from_field(field).unwrap(),
                );
                ColumnCatalog {
                    column_desc,
                    is_hidden: false,
                }
            })
            .collect();

        tracing::info!(
            "ADBC Snowflake inferred {} columns from table {}",
            columns.len(),
            properties.table
        );

        Ok(columns)
    } else {
        Err(anyhow!(format!(
            "Invalid properties for ADBC Snowflake source: {:?}",
            props
        )))
    }
}
