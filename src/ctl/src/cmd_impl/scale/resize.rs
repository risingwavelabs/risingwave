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

use std::collections::{HashMap, HashSet};
use std::process::exit;

use inquire::Confirm;
use itertools::Itertools;
use risingwave_pb::meta::get_reschedule_plan_request::{
    PbPolicy, StableResizePolicy, WorkerChanges,
};
use risingwave_pb::meta::{GetClusterInfoResponse, GetReschedulePlanResponse};
use risingwave_stream::task::FragmentId;

use crate::common::CtlContext;
use crate::ScaleResizeCommands;

pub async fn resize(context: &CtlContext, resize: ScaleResizeCommands) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments,
        actor_splits: _actor_splits,
        source_infos: _source_infos,
        revision,
    } = meta_client.get_cluster_info().await?;

    if worker_nodes.is_empty() {
        println!("No worker nodes found");
        return Ok(());
    }

    if table_fragments.is_empty() {
        println!("No tables found");
        return Ok(());
    }

    println!("Cluster info fetched, revision: {}", revision);
    println!("Table fragments: {}", table_fragments.len());

    println!("Worker nodes: {}", worker_nodes.len());

    let streaming_worker_map = worker_nodes
        .into_iter()
        .filter(|worker| {
            worker
                .property
                .as_ref()
                .map(|property| property.is_streaming)
                .unwrap_or(false)
        })
        .map(|worker| (worker.id, worker))
        .collect::<HashMap<_, _>>();

    println!("Streaming workers found: {}", streaming_worker_map.len());

    let ScaleResizeCommands {
        exclude_workers,
        include_workers,
        generate,
        output: _,
        yes,
        fragments,
    } = resize;

    let worker_changes = match (exclude_workers, include_workers) {
        (None, None) => WorkerChanges {
            include_worker_ids: streaming_worker_map.keys().cloned().collect(),
            exclude_worker_ids: vec![],
        },
        (exclude, include) => {
            if let Some(exclude) = exclude.as_ref() {
                for worker_id in exclude {
                    if !streaming_worker_map.contains_key(worker_id) {
                        anyhow::bail!("Invalid worker id: {}", worker_id);
                    }
                }
            }

            if let Some(include) = include.as_ref() {
                for worker_id in include {
                    if !streaming_worker_map.contains_key(worker_id) {
                        anyhow::bail!("Invalid worker id: {}", worker_id);
                    }
                }
            }

            WorkerChanges {
                include_worker_ids: include.unwrap_or_default(),
                exclude_worker_ids: exclude.unwrap_or_default(),
            }
        }
    };

    if worker_changes.exclude_worker_ids.is_empty() && worker_changes.include_worker_ids.is_empty()
    {
        println!("No worker nodes provided");
        exit(1)
    }

    let all_fragment_ids: HashSet<_> = table_fragments
        .iter()
        .flat_map(|table_fragments| table_fragments.fragments.keys().cloned())
        .collect();

    let target_fragment_ids = match fragments {
        None => all_fragment_ids.into_iter().collect_vec(),
        Some(fragment_ids) => {
            let provide_fragment_ids: HashSet<_> = fragment_ids.into_iter().collect();
            if provide_fragment_ids
                .iter()
                .any(|fragment_id| !all_fragment_ids.contains(fragment_id))
            {
                anyhow::bail!(
                    "Invalid fragment ids: {:?}",
                    provide_fragment_ids
                        .difference(&all_fragment_ids)
                        .collect_vec()
                );
            }

            provide_fragment_ids.into_iter().collect()
        }
    };

    let policy = PbPolicy::StableResizePolicy(StableResizePolicy {
        fragment_worker_changes: target_fragment_ids
            .iter()
            .map(|id| (*id as FragmentId, worker_changes.clone()))
            .collect(),
    });

    let GetReschedulePlanResponse {
        revision,
        reschedules,
        success,
    } = meta_client.get_reschedule_plan(policy, revision).await?;

    if !success {
        println!("Failed to generate plan, current revision is {}", revision);
        exit(1);
    }

    println!(
        "Successfully generated plan, current revision is {}",
        revision
    );

    if generate {
        // todo, This needs to be implemented together with the --from option of the reschedule
        // command.
        println!("{:#?}", reschedules);
    } else {
        if !yes {
            match Confirm::new("Will perform actions on the cluster, are you sure?")
                .with_default(false)
                .with_help_message("Use the --yes or -y option to skip this prompt")
                .prompt()
            {
                Ok(true) => {}
                Ok(false) => {
                    println!("Abort.");
                    exit(1);
                }
                Err(_) => {
                    println!("Error with questionnaire, try again later");
                    exit(-1);
                }
            }
        }

        let (success, next_revision) = meta_client.reschedule(reschedules, revision).await?;
        if !success {
            println!("Failed to execute plan, current revision is {}", revision);
            return Ok(());
        }
        println!(
            "Successfully executed plan, current revision is {}",
            next_revision
        );
    }

    Ok(())
}
