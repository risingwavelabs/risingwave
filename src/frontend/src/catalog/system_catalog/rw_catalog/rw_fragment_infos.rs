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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use super::rw_fragments::extract_fragment_type_flag;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwFragmentInfo {
    #[primary_key]
    fragment_id: i32,
    table_id: i32,
    distribution_type: String,
    state_table_ids: Vec<i32>,
    upstream_fragment_ids: Vec<i32>,
    flags: Vec<String>,
    parallelism: i32,
    max_parallelism: i32,
    parallelism_policy: String,
    node: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_fragment_infos")]
async fn read_rw_fragment_infos(reader: &SysCatalogReaderImpl) -> Result<Vec<RwFragmentInfo>> {
    let distributions = reader.meta_client.list_fragment_distribution(true).await?;

    Ok(distributions
        .into_iter()
        .map(|distribution| RwFragmentInfo {
            fragment_id: distribution.fragment_id.as_i32_id(),
            table_id: distribution.table_id.as_i32_id(),
            distribution_type: distribution.distribution_type().as_str_name().into(),
            state_table_ids: distribution
                .state_table_ids
                .into_iter()
                .map(|id| id.as_i32_id())
                .collect(),
            upstream_fragment_ids: distribution
                .upstream_fragment_ids
                .into_iter()
                .map(|id| id.as_i32_id())
                .collect(),
            flags: extract_fragment_type_flag(distribution.fragment_type_mask)
                .into_iter()
                .map(|t| t.as_str_name().to_owned())
                .collect(),
            parallelism: distribution.parallelism as i32,
            max_parallelism: distribution.vnode_count as i32,
            parallelism_policy: distribution.parallelism_policy,
            node: json!(distribution.node).into(),
        })
        .collect())
}
