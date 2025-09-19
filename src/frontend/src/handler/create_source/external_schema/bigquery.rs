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

use anyhow::Context;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::sink::big_query::BigQueryCommon;
use risingwave_connector::source::ConnectorProperties;

use crate::catalog::ColumnId;

pub async fn extract_bigquery_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::BatchBigQuery(properties) = props {
        let client = properties
            .common
            .build_client(&properties.aws_auth_props)
            .await?;
        let table = properties.common.get_table(&client).await?;

        let mut columns: Vec<ColumnCatalog> = vec![];
        for (i, field) in table
            .schema
            .fields
            .context("no fields found")?
            .into_iter()
            .enumerate()
        {
            let column_desc = ColumnDesc::named(
                &field.name,
                ColumnId::new((i + 1).try_into().unwrap()),
                BigQueryCommon::bigquery_type_to_rw_type(&field)?,
            );
            let column_desc = ColumnCatalog {
                column_desc,
                is_hidden: false,
            };
            columns.push(column_desc);
        }

        Ok(columns)
    } else {
        Err(anyhow::anyhow!(format!(
            "Invalid properties for bigquery source: {:?}",
            props
        )))
    }
}
