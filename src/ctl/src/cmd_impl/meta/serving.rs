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

use std::collections::HashMap;

use comfy_table::{Row, Table};
use itertools::Itertools;
use risingwave_common::hash::{ParallelUnitId, VirtualNode};
use risingwave_pb::common::{WorkerNode, WorkerType};

use crate::CtlContext;

pub async fn list_serving_fragment_mappings(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let mappings = meta_client.list_serving_vnode_mappings().await?;
    let workers = meta_client
        .list_worker_nodes(WorkerType::ComputeNode)
        .await?;
    let mut pu_to_worker: HashMap<ParallelUnitId, &WorkerNode> = HashMap::new();
    for w in &workers {
        for pu in &w.parallel_units {
            pu_to_worker.insert(pu.id, w);
        }
    }

    let mut table = Table::new();
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Table Id".into());
        row.add_cell("Fragment Id".into());
        row.add_cell("Parallel Unit Id".into());
        row.add_cell("Virtual Node".into());
        row.add_cell("Worker".into());
        row
    });

    let rows = mappings
        .iter()
        .flat_map(|(fragment_id, (table_id, mapping))| {
            let mut pu_vnodes: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();
            for (vnode, pu) in mapping.iter_with_vnode() {
                pu_vnodes.entry(pu).or_insert(vec![]).push(vnode);
            }
            pu_vnodes.into_iter().map(|(pu_id, vnodes)| {
                (
                    *table_id,
                    *fragment_id,
                    pu_id,
                    vnodes,
                    pu_to_worker.get(&pu_id),
                )
            })
        })
        .collect_vec();
    for (table_id, fragment_id, pu_id, vnodes, worker) in
        rows.into_iter().sorted_by_key(|(t, f, p, ..)| (*t, *f, *p))
    {
        let mut row = Row::new();
        row.add_cell(table_id.into());
        row.add_cell(fragment_id.into());
        row.add_cell(pu_id.into());
        row.add_cell(
            format!(
                "{} in total: {}",
                vnodes.len(),
                vnodes
                    .into_iter()
                    .sorted()
                    .map(|v| v.to_index().to_string())
                    .join(",")
            )
            .into(),
        );
        if let Some(w) = worker && let Some(addr) = w.host.as_ref() {
            row.add_cell(format!("id: {}; {}:{}", w.id, addr.host, addr.port).into());
        } else {
            row.add_cell("".into());
        }
        table.add_row(row);
    }
    println!("{table}");
    Ok(())
}
