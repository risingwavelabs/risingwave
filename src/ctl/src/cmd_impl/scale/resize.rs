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
use serde_yaml;

use crate::cmd_impl::meta::ReschedulePayload;
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
    } = match meta_client.get_cluster_info().await {
        Ok(resp) => resp,
        Err(e) => {
            println!("Failed to fetch cluster info: {}", e);
            exit(1);
        }
    };

    if worker_nodes.is_empty() {
        println!("No worker nodes found");
        return Ok(());
    }

    if table_fragments.is_empty() {
        println!("No tables found");
        return Ok(());
    }

    println!("Cluster info fetched, revision: {}", revision);
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
        output,
        yes,
        fragments,
    } = resize;

    let worker_changes = match (exclude_workers, include_workers) {
        (None, None) => unreachable!(),
        (exclude, include) => {
            let exclude_worker_ids = exclude.unwrap_or_default();
            let include_worker_ids = include.unwrap_or_default();

            for worker_id in exclude_worker_ids.iter().chain(include_worker_ids.iter()) {
                if !streaming_worker_map.contains_key(worker_id) {
                    println!("Invalid worker id: {}", worker_id);
                    exit(1);
                }
            }

            for include_worker_id in &include_worker_ids {
                let worker_is_unschedulable = streaming_worker_map
                    .get(include_worker_id)
                    .and_then(|worker| worker.property.as_ref())
                    .map(|property| property.is_unschedulable)
                    .unwrap_or(false);

                if worker_is_unschedulable {
                    println!(
                        "Worker {} is unschedulable, should not be included",
                        include_worker_id
                    );
                    exit(1);
                }
            }

            WorkerChanges {
                include_worker_ids,
                exclude_worker_ids,
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
                println!(
                    "Invalid fragment ids: {:?}",
                    provide_fragment_ids
                        .difference(&all_fragment_ids)
                        .collect_vec()
                );
                exit(1);
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

    let response = meta_client.get_reschedule_plan(policy, revision).await;

    let GetReschedulePlanResponse {
        revision,
        reschedules,
        success,
    } = match response {
        Ok(response) => response,
        Err(e) => {
            println!("Failed to generate plan: {:?}", e);
            exit(1);
        }
    };

    if !success {
        println!("Failed to generate plan, current revision is {}", revision);
        exit(1);
    }

    if reschedules.is_empty() {
        println!(
            "No reschedule plan generated, no action required, current revision is {}",
            revision
        );
        return Ok(());
    }

    println!(
        "Successfully generated plan, current revision is {}",
        revision
    );

    if generate {
        let payload = ReschedulePayload {
            reschedule_revision: revision,
            reschedule_plan: reschedules
                .into_iter()
                .map(|(fragment_id, reschedule)| (fragment_id, reschedule.into()))
                .collect(),
        };

        if let Some(output) = output.as_ref() {
            println!("Writing plan to file: {}", output);
            let writer = std::fs::File::create(output)?;
            serde_yaml::to_writer(writer, &payload)?;
            println!("Writing plan to file: {} done", output);
            println!("You can use the `risectl meta reschedule --from {}` command to execute the generated plan", output);
        } else {
            println!("Option `--output` is not provided, the result plan will be output to the current command line.");
            println!("#=========== Payload ==============#");
            serde_yaml::to_writer(std::io::stdout(), &payload)?;
            println!("#=========== Payload ==============#");
        }
    } else {
        if !yes {
            match Confirm::new("Will perform actions on the cluster, are you sure?")
                .with_default(false)
                .with_help_message("Use the --generate flag to view the generated plan. Use the --yes or -y option to skip this prompt")
                .with_placeholder("no")
                .prompt()
            {
                Ok(true) => println!("Processing..."),
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

        let (success, next_revision) = match meta_client.reschedule(reschedules, revision).await {
            Ok(response) => response,
            Err(e) => {
                println!("Failed to execute plan: {:?}", e);
                exit(1);
            }
        };

        if !success {
            println!("Failed to execute plan, current revision is {}", revision);
            exit(1);
        }

        println!(
            "Successfully executed plan, current revision is {}",
            next_revision
        );
    }

    Ok(())
}

pub async fn unregister_workers(context: &CtlContext, workers: &[u32]) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client.unregister_workers(workers).await?;
    Ok(())
}
