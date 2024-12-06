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

use super::rw_fragments::extract_fragment_type_flag;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(fragment_id, node_name)]
struct RwRateLimit {
    fragment_id: i32,
    fragment_type: Vec<String>,
    node_name: String,
    table_id: i32,
    rate_limit: i32,
}

#[system_catalog(table, "rw_catalog.rw_rate_limit")]
async fn read_rw_rate_limit(reader: &SysCatalogReaderImpl) -> Result<Vec<RwRateLimit>> {
    let rate_limits = reader.meta_client.list_rate_limits().await?;

    Ok(rate_limits
        .into_iter()
        .map(|info| RwRateLimit {
            fragment_id: info.fragment_id as i32,
            fragment_type: extract_fragment_type_flag(info.fragment_type_mask)
                .into_iter()
                .flat_map(|t| t.as_str_name().strip_prefix("FRAGMENT_TYPE_FLAG_"))
                .map(|s| s.into())
                .collect(),
            table_id: info.job_id as i32,
            rate_limit: info.rate_limit as i32,
            node_name: info.node_name,
        })
        .collect())
}
