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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockVersionDelta {
    #[primary_key]
    id: i64,
    prev_id: i64,
    trivial_move: bool,
    group_deltas: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_hummock_version_deltas")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockVersionDelta>> {
    let deltas = reader.meta_client.list_version_deltas().await?;
    let rows = deltas
        .into_iter()
        .map(|d| RwHummockVersionDelta {
            id: d.id.to_u64() as _,
            prev_id: d.prev_id.to_u64() as _,
            trivial_move: d.trivial_move,
            group_deltas: json!(
                d.group_deltas
                    .into_iter()
                    .map(|(group_id, deltas)| (group_id, PbGroupDeltas::from(deltas)))
                    .collect::<HashMap<u64, PbGroupDeltas>>()
            )
            .into(),
        })
        .collect();
    Ok(rows)
}
