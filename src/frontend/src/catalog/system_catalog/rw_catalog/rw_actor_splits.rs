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
use risingwave_pb::meta::list_actor_splits_response::{ActorSplit, FragmentType};

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(actor_id, split_id, source_id)]
struct RwActorSplit {
    actor_id: i32,
    split_id: String,
    source_id: i32,
    fragment_id: i32,
    fragment_type: String,
}

impl From<ActorSplit> for RwActorSplit {
    fn from(actor_split: ActorSplit) -> Self {
        Self {
            actor_id: actor_split.actor_id as _,
            split_id: actor_split.split_id,
            source_id: actor_split.source_id as _,
            fragment_id: actor_split.fragment_id.as_raw_id() as _,
            fragment_type: FragmentType::try_from(actor_split.fragment_type)
                .unwrap_or(FragmentType::Unspecified)
                .as_str_name()
                .to_owned(),
        }
    }
}

#[system_catalog(table, "rw_catalog.rw_actor_splits")]
async fn read_rw_actor_splits(reader: &SysCatalogReaderImpl) -> Result<Vec<RwActorSplit>> {
    let actor_splits = reader.meta_client.list_actor_splits().await?;
    Ok(actor_splits.into_iter().map(RwActorSplit::from).collect())
}
