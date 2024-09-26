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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(actor_id, split_id, source_id)]
struct RwActorSplits {
    actor_id: i32,
    split_id: i32,
    source_id: i32,
    fragment_id: i32,
}

#[system_catalog(table, "rw_catalog.rw_actor_splits")]
async fn read_rw_actor_splits(reader: &SysCatalogReaderImpl) -> Result<Vec<RwActorSplits>> {
    let actor_splits = reader.meta_client.list_actor_splits().await?;

    // actor_id, fragment_id, source_id, split_id

    // Ok(states
    //     .into_iter()
    //     .map(|state| RwActor {
    //         actor_id: state.actor_id as i32,
    //         fragment_id: state.fragment_id as i32,
    //         worker_id: state.worker_id as i32,
    //         state: state.state().as_str_name().into(),
    //     })
    //     .collect())

    Ok(vec![])
}
