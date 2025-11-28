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
#[primary_key(table_id)]
struct RwRefreshTableState {
    table_id: i32,
    current_status: String,
    last_trigger_time: Option<String>,
    last_success_time: Option<String>,
    trigger_interval_secs: Option<i64>,
}

#[system_catalog(table, "rw_catalog.rw_refresh_table_state")]
async fn read_rw_refresh_table_state(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwRefreshTableState>> {
    let refresh_table_states = reader.meta_client.list_refresh_table_states().await?;
    Ok(refresh_table_states
        .into_iter()
        .map(|state| RwRefreshTableState {
            table_id: state.table_id.as_raw_id() as i32,
            current_status: state.current_status,
            last_trigger_time: state.last_trigger_time,
            trigger_interval_secs: state.trigger_interval_secs,
            last_success_time: state.last_success_time,
        })
        .collect())
}
