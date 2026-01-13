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

use risingwave_common::id::TableId;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(table_id)]
struct RwTableRefill {
    table_id: i32,
    mode: String,
}

#[system_catalog(table, "rw_catalog.rw_table_refill")]
async fn read_rw_table_refill(reader: &SysCatalogReaderImpl) -> Result<Vec<RwTableRefill>> {
    let configs = reader.meta_client.list_table_refill_config().await?;

    Ok(configs
        .into_iter()
        .filter_map(|config| {
            let mode = config.mode?;
            Some(RwTableRefill {
                table_id: TableId::new(config.table_id).as_i32_id(),
                mode,
            })
        })
        .collect())
}
