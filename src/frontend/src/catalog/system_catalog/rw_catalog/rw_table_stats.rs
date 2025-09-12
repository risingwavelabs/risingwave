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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwTableStats {
    #[primary_key]
    id: i32,
    storage_key_count: i64,
    storage_key_size_uncompressed: i64,
    storage_value_size_uncompressed: i64,
}

#[system_catalog(table, "rw_catalog.rw_table_stats")]
fn read_table_stats(reader: &SysCatalogReaderImpl) -> Result<Vec<RwTableStats>> {
    let catalog = reader.catalog_reader.read_guard();
    let table_stats = catalog.table_stats();
    let mut rows = vec![];
    for (id, stats) in &table_stats.table_stats {
        rows.push(RwTableStats {
            id: *id as i32,
            storage_key_count: stats.total_key_count,
            storage_key_size_uncompressed: stats.total_key_size,
            storage_value_size_uncompressed: stats.total_value_size,
        });
    }
    Ok(rows)
}
