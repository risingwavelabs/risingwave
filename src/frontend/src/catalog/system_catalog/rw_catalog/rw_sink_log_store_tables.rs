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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwSinkLogStoreTable {
    #[primary_key]
    sink_id: i32,
    internal_table_id: i32,
}

#[system_catalog(table, "rw_catalog.rw_sink_log_store_tables")]
async fn read_rw_sink_log_store_tables(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwSinkLogStoreTable>> {
    let tables = reader.meta_client.list_sink_log_store_tables().await?;
    Ok(tables
        .into_iter()
        .map(|table| RwSinkLogStoreTable {
            sink_id: table.sink_id as i32,
            internal_table_id: table.internal_table_id as i32,
        })
        .collect())
}
