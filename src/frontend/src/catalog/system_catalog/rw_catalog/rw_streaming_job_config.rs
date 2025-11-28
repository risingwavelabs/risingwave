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
use risingwave_pb::meta::list_streaming_job_states_response::PbStreamingJobState;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::session::current::notice_to_user;

#[derive(Fields)]
#[primary_key(id, key)]
struct RwStreamingJobConfig {
    id: i32,
    name: String,
    database_id: i32,
    key: String, // dot-separated key, e.g., `streaming.developer.chunk_size`
    value: String,
}

/// Collect all config override as dot-separated key and stringified value.
fn collect_config_kv(state: &PbStreamingJobState) -> Vec<(String, String)> {
    let table = match state.config_override.parse() {
        Ok(table) => table,
        Err(_) => {
            notice_to_user(format!(
                "Failed to parse config override for streaming job {} (id = {}):\n{}",
                state.name, state.table_id, state.config_override
            ));
            return Vec::new();
        }
    };
    let mut res = Vec::new();

    fn collect<'a>(
        path: &mut Vec<&'a str>,
        table: &'a toml::Table,
        res: &mut Vec<(String, String)>,
    ) {
        for (k, v) in table {
            path.push(k);
            match v {
                toml::Value::Table(sub_table) => {
                    collect(path, sub_table, res);
                }
                value => {
                    res.push((path.join("."), value.to_string()));
                }
            }
            path.pop();
        }
    }

    collect(&mut Vec::new(), &table, &mut res);
    res
}

#[system_catalog(table, "rw_catalog.rw_streaming_job_config")]
async fn read_rw_streaming_job_config(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwStreamingJobConfig>> {
    let states = reader.meta_client.list_streaming_job_states().await?;

    Ok(states
        .into_iter()
        .flat_map(|state| {
            let id = state.table_id.as_i32_id();
            let name = state.name.clone();
            let database_id = state.database_id.as_i32_id();

            collect_config_kv(&state)
                .into_iter()
                .map(move |(key, value)| RwStreamingJobConfig {
                    id,
                    name: name.clone(),
                    database_id,
                    key,
                    value,
                })
        })
        .collect())
}
