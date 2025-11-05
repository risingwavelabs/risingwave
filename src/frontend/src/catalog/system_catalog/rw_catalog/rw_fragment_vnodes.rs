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

use std::collections::HashMap;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwFragmentVnode {
    #[primary_key]
    fragment_id: i32,
    vnode_count: i32,
    actor_vnodes: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_fragment_vnodes")]
async fn read_rw_fragment_vnodes(reader: &SysCatalogReaderImpl) -> Result<Vec<RwFragmentVnode>> {
    // Get actor vnodes from meta (streaming vnode mapping)
    let actor_vnodes_list = reader.meta_client.list_actor_vnodes().await?;

    // Group by fragment_id
    let mut fragment_map: HashMap<u32, Vec<(u32, Bitmap)>> = HashMap::new();

    for actor_vnode in actor_vnodes_list {
        let actor_id = actor_vnode.actor_id;
        let fragment_id = actor_vnode.fragment_id;

        if let Some(vnode_bitmap_pb) = actor_vnode.vnode_bitmap {
            let bitmap = Bitmap::from(&vnode_bitmap_pb);
            fragment_map
                .entry(fragment_id)
                .or_default()
                .push((actor_id, bitmap));
        }
    }

    // Build result
    let mut result = Vec::new();

    for (fragment_id, actors) in fragment_map {
        let vnode_count = actors.first().map(|(_, bitmap)| bitmap.len()).unwrap_or(0);

        // Build actor_id -> vnodes mapping
        let mut actor_vnodes_map: HashMap<i32, Vec<i32>> = HashMap::new();

        for (actor_id, bitmap) in actors {
            let vnodes: Vec<i32> = bitmap
                .iter_ones()
                .map(|vnode_idx| vnode_idx as i32)
                .collect();
            actor_vnodes_map.insert(actor_id as i32, vnodes);
        }

        result.push(RwFragmentVnode {
            fragment_id: fragment_id as i32,
            vnode_count: vnode_count as i32,
            actor_vnodes: json!(actor_vnodes_map).into(),
        });
    }

    Ok(result)
}
