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

use std::collections::HashMap;
use std::ops::Deref;

use anyhow::anyhow;
use icelake::Table;
use risingwave_common::types::Fields;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::WithPropertiesExt;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwIcebergFiles {
    #[primary_key]
    source_id: i32,
    schema_name: String,
    source_name: String,
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    pub content: i32,
    /// Full URI for the file with FS scheme
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: String,
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is `EqualityDeletes` and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    pub sort_order_id: Option<i32>,
}

#[system_catalog(table, "rw_catalog.rw_iceberg_files")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwIcebergFiles>> {
    let iceberg_sources = {
        let catalog_reader = reader.catalog_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

        let mut iceberg_sources = vec![];
        for schema in schemas {
            for source in schema.iter_source() {
                if source.with_properties.is_iceberg_connector() {
                    iceberg_sources.push((schema.name.clone(), source.deref().clone()))
                }
            }
        }
        iceberg_sources
    };

    let mut result = vec![];
    for (schema_name, source) in iceberg_sources {
        let source_props: HashMap<String, String> =
            HashMap::from_iter(source.with_properties.clone());
        let config = ConnectorProperties::extract(source_props, false)?;
        if let ConnectorProperties::Iceberg(iceberg_properties) = config {
            let iceberg_config: IcebergConfig = iceberg_properties.to_iceberg_config();
            let table: Table = iceberg_config.load_table().await?;
            result.extend(
                table
                    .current_data_files()
                    .await
                    .map_err(|e| anyhow!(e))?
                    .iter()
                    .map(|file| RwIcebergFiles {
                        source_id: source.id as i32,
                        schema_name: schema_name.clone(),
                        source_name: source.name.clone(),
                        content: file.content as i32,
                        file_path: file.file_path.clone(),
                        file_format: file.file_format.to_string(),
                        record_count: file.record_count,
                        file_size_in_bytes: file.file_size_in_bytes,
                        equality_ids: file.equality_ids.clone(),
                        sort_order_id: file.sort_order_id,
                    }),
            );
        } else {
            unreachable!()
        }
    }
    Ok(result)
}
