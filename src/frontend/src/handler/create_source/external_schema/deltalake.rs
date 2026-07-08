// Copyright 2026 RisingWave Labs
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

pub async fn extract_deltalake_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::DeltaLake(properties) = props {
        let schema = properties.load_table_arrow_schema().await?;

        let columns = schema
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
                    is_hidden: field.name() == ROW_ID_COLUMN_NAME,
                }
            })
            .collect();

        tracing::info!("deltalake columns: {:?}", columns);

        Ok(columns)
    } else {
        Err(anyhow!(format!(
            "Invalid properties for deltalake source: {:?}",
            props
        )))
    }
}
