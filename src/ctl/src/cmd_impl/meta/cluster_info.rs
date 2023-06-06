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

use std::collections::{BTreeMap, HashMap};

use comfy_table::{Attribute, Cell, Row, Table};
use itertools::Itertools;
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::{CordonWorkerNodeResponse, GetClusterInfoResponse};
use risingwave_pb::source::ConnectorSplits;
use risingwave_pb::stream_plan::FragmentTypeFlag;

use crate::CtlContext;

pub async fn get_cluster_info(context: &CtlContext) -> anyhow::Result<GetClusterInfoResponse> {
    let meta_client = context.meta_client().await?;
    let response = meta_client.get_cluster_info().await?;
    Ok(response)
}

pub async fn cordon_worker(
    context: &CtlContext,
    addr: HostAddress,
) -> anyhow::Result<CordonWorkerNodeResponse> {
    let meta_client = context.meta_client().await?;
    let response = meta_client.cordon_worker(addr).await?;
    Ok(response)
}

pub async fn source_split_info(context: &CtlContext) -> anyhow::Result<()> {
    let GetClusterInfoResponse {
        worker_nodes: _,
        source_infos: _,
        table_fragments,
        mut actor_splits,
    } = get_cluster_info(context).await?;

    for table_fragment in &table_fragments {
        if table_fragment.actor_splits.is_empty() {
            continue;
        }

        println!("Table #{}", table_fragment.table_id);

        for fragment in table_fragment.fragments.values() {
            if fragment.fragment_type_mask & FragmentTypeFlag::Source as u32 == 0 {
                continue;
            }

            println!("\tFragment #{}", fragment.fragment_id);
            for actor in &fragment.actors {
                let ConnectorSplits { splits } = actor_splits.remove(&actor.actor_id).unwrap();
                let splits = splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .map(|split| split.id())
                    .collect_vec();

                println!(
                    "\t\tActor #{} ({}): [{}]",
                    actor.actor_id,
                    splits.len(),
                    splits.join(",")
                );
            }
        }
    }

    Ok(())
}

pub async fn cluster_info(context: &CtlContext) -> anyhow::Result<()> {
    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments,
        actor_splits: _,
        source_infos: _,
    } = get_cluster_info(context).await?;

    // Fragment ID -> [Parallel Unit ID -> (Parallel Unit, Actor)]
    let mut fragments = BTreeMap::new();
    // Fragment ID -> Table Fragments' State
    let mut fragment_states = HashMap::new();

    for table_fragment in &table_fragments {
        for (&id, fragment) in &table_fragment.fragments {
            for actor in &fragment.actors {
                let parallel_unit = table_fragment
                    .actor_status
                    .get(&actor.actor_id)
                    .unwrap()
                    .get_parallel_unit()
                    .unwrap();
                fragments
                    .entry(id)
                    .or_insert_with(HashMap::new)
                    .insert(parallel_unit.id, (parallel_unit, actor));
            }
            fragment_states.insert(id, table_fragment.state());
        }
    }

    // Parallel Unit ID -> Worker Node
    let all_parallel_units: BTreeMap<_, _> = worker_nodes
        .iter()
        .flat_map(|worker_node| {
            worker_node
                .parallel_units
                .iter()
                .map(|parallel_unit| (parallel_unit.id, worker_node.clone()))
        })
        .collect();

    let mut table = Table::new();

    let cross_out_if_creating = |cell: Cell, fid: u32| -> Cell {
        match fragment_states[&fid] {
            State::Unspecified => unreachable!(),
            State::Creating => cell.add_attribute(Attribute::CrossedOut),
            State::Created | State::Initial => cell,
        }
    };

    // Compute Node, Parallel Unit, Frag 1, Frag 2, ..., Frag N
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Compute Node".into());
        row.add_cell("Parallel Unit".into());
        for &fid in fragments.keys() {
            let cell = Cell::new(format!("Frag {fid}"));
            let cell = cross_out_if_creating(cell, fid);
            row.add_cell(cell);
        }
        row
    });

    let mut last_worker_id = None;
    for (pu, worker) in all_parallel_units {
        // Compute Node, Parallel Unit, Actor 1, Actor 11, -, ..., Actor N
        let mut row = Row::new();
        row.add_cell(if last_worker_id == Some(worker.id) {
            "".into()
        } else {
            last_worker_id = Some(worker.id);
            Cell::new(format!(
                "{}@{}",
                worker.id,
                HostAddr::from(worker.get_host().unwrap())
            ))
            .add_attribute(Attribute::Bold)
        });
        row.add_cell(pu.into());
        for (&fid, f) in &fragments {
            let cell = if let Some((_pu, actor)) = f.get(&pu) {
                actor.actor_id.into()
            } else {
                "-".into()
            };
            let cell = cross_out_if_creating(cell, fid);
            row.add_cell(cell);
        }
        table.add_row(row);
    }

    println!("{table}");

    Ok(())
}
