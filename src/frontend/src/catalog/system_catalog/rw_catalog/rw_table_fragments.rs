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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::{extract_parallelism_from_table_state, SysCatalogReaderImpl};
use crate::error::Result;

#[derive(Fields)]
struct RwTableFragment {
    #[primary_key]
    table_id: i32,
    status: String,
    parallelism: String,
    max_parallelism: i32,
}

#[system_catalog(table, "rw_catalog.rw_table_fragments")]
async fn read_rw_table_fragments_info(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwTableFragment>> {
    let states = reader.meta_client.list_table_fragment_states().await?;

    Ok(states
        .into_iter()
        .map(|state| {
            let parallelism = extract_parallelism_from_table_state(&state);
            RwTableFragment {
                table_id: state.table_id as i32,
                status: state.state().as_str_name().into(),
                parallelism: parallelism.to_uppercase(),
                max_parallelism: state.max_parallelism as i32,
            }
        })
        .collect())
}
