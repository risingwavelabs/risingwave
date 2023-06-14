// Copyright 2023 RisingWave Labs
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

use comfy_table::{Cell, Row, Table};
use itertools::Itertools;

use crate::CtlContext;

pub async fn list_serving_fragment_mappings(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let mappings = meta_client.list_serving_vnode_mappings().await?;

    let mut table = Table::new();
    // Parallel Unit, Frag 1, Frag 2, ..., Frag N
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Parallel Unit".into());
        for &fid in mappings.keys() {
            let cell = Cell::new(format!("Frag {fid}"));
            row.add_cell(cell);
        }
        row
    });

    let all_parallel_units = mappings
        .values()
        .flat_map(|m| m.iter_unique())
        .unique()
        .sorted()
        .collect_vec();
    for pu in &all_parallel_units {
        let mut row = Row::new();
        row.add_cell((*pu).into());
        for mapping in mappings.values() {
            let mut weight = 0;
            for pu_id in mapping.iter() {
                if pu_id == *pu {
                    weight += 1;
                }
            }
            row.add_cell(weight.into());
        }
        table.add_row(row);
    }
    println!("{table}");
    Ok(())
}
