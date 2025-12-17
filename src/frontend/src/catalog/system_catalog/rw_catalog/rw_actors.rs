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
struct RwActor {
    #[primary_key]
    actor_id: i32,
    fragment_id: i32,
    worker_id: i32,
    state: String,
}

#[system_catalog(table, "rw_catalog.rw_actors")]
async fn read_rw_actors(reader: &SysCatalogReaderImpl) -> Result<Vec<RwActor>> {
    let states = reader.meta_client.list_actor_states().await?;

    Ok(states
        .into_iter()
        .map(|state| RwActor {
            actor_id: state.actor_id.as_i32_id(),
            fragment_id: state.fragment_id.as_i32_id(),
            worker_id: state.worker_id.as_i32_id(),
            state: "RUNNING".into(),
        })
        .collect())
}
