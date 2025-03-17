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

use std::ops::Deref;

use anyhow::anyhow;
use futures::StreamExt;
use iceberg::spec::ManifestList;
use iceberg::table::Table;
use risingwave_common::types::Fields;
use risingwave_connector::WithPropertiesExt;
use risingwave_connector::source::ConnectorProperties;
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
    pub equality_ids: Vec<i32>,
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
        let config = ConnectorProperties::extract(source.with_properties.clone(), false)?;
        if let ConnectorProperties::Iceberg(iceberg_properties) = config {
            let table: Table = iceberg_properties.load_table().await?;
            if let Some(snapshot) = table.metadata().current_snapshot() {
                let manifest_list: ManifestList = snapshot
                    .load_manifest_list(table.file_io(), table.metadata())
                    .await
                    .map_err(|e| anyhow!(e))?;
                for entry in manifest_list.entries() {
                    let manifest = entry
                        .load_manifest(table.file_io())
                        .await
                        .map_err(|e| anyhow!(e))?;
                    let mut manifest_entries_stream =
                        futures::stream::iter(manifest.entries().iter().filter(|e| e.is_alive()));

                    while let Some(manifest_entry) = manifest_entries_stream.next().await {
                        let file = manifest_entry.data_file();
                        result.push(RwIcebergFiles {
                            source_id: source.id as i32,
                            schema_name: schema_name.clone(),
                            source_name: source.name.clone(),
                            content: file.content_type() as i32,
                            file_path: file.file_path().to_owned(),
                            file_format: file.file_format().to_string(),
                            record_count: file.record_count() as i64,
                            file_size_in_bytes: file.file_size_in_bytes() as i64,
                            equality_ids: file.equality_ids().to_vec(),
                            sort_order_id: file.sort_order_id(),
                        });
                    }
                }
            }
        } else {
            unreachable!()
        }
    }
    Ok(result)
}
