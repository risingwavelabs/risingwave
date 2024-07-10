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

use itertools::Itertools;
use risingwave_common::hash::{ActorMapping, VnodeBitmapExt};
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwVnodeMapping {
    #[primary_key]
    actor_id: i32,
    fragment_id: i32,
    ranges: String,
    virtual_nodes: Vec<i32>,
}

#[system_catalog(table, "rw_catalog.rw_vnode_mappings")]
async fn read_rw_vnode_mappings(reader: &SysCatalogReaderImpl) -> Result<Vec<RwVnodeMapping>> {
    let mappings = reader.meta_client.list_fragment_mappings().await?;

    let rw_vnode_mappings = mappings
        .into_iter()
        .flat_map(|(fragment_id, actor_mapping)| {
            let actor_mapping = ActorMapping::from_protobuf(&actor_mapping);
            let bitmaps = actor_mapping.to_bitmaps();
            bitmaps
                .into_iter()
                .map(move |(actor_id, bitmap)| RwVnodeMapping {
                    actor_id: actor_id as i32,
                    fragment_id: fragment_id as i32,
                    ranges: bitmap
                        .high_ranges()
                        .map(|range| format!("{:?}", range))
                        .join(","),
                    virtual_nodes: bitmap
                        .iter_vnodes()
                        .map(|vnode| vnode.to_index() as i32)
                        .collect(),
                })
        })
        .collect();

    Ok(rw_vnode_mappings)
}
