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
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
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

    let streaming_workers_index_by_id = worker_nodes
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

    let streaming_workers_index_by_host = streaming_workers_index_by_id
        .values()
        .map(|worker| {
            let host = worker.get_host().expect("worker host must be set");
            (format!("{}:{}", host.host, host.port), worker.clone())
        })
        .collect::<HashMap<_, _>>();

    let worker_input_to_worker_id = |inputs: Vec<String>| -> Vec<u32> {
        let mut result: HashSet<_> = HashSet::new();

        for input in inputs {
            let worker_id = input.parse::<u32>().ok().or_else(|| {
                streaming_workers_index_by_host
                    .get(&input)
                    .map(|worker| worker.id)
            });

            if let Some(worker_id) = worker_id {
                if !result.insert(worker_id) {
                    println!("warn: {} and {} are the same worker", input, worker_id);
                }
            } else {
                println!("Invalid worker input: {}", input);
                exit(1);
            }
        }

        result.into_iter().collect()
    };

    println!(
        "Streaming workers found: {}",
        streaming_workers_index_by_id.len()
    );

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
            let excludes = worker_input_to_worker_id(exclude.unwrap_or_default());
            let includes = worker_input_to_worker_id(include.unwrap_or_default());

            for worker_input in excludes.iter().chain(includes.iter()) {
                if !streaming_workers_index_by_id.contains_key(worker_input) {
                    println!("Invalid worker id: {}", worker_input);
                    exit(1);
                }
            }

            for include_worker_id in &includes {
                let worker_is_unschedulable = streaming_workers_index_by_id
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
                include_worker_ids: includes,
                exclude_worker_ids: excludes,
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

pub async fn update_schedulability(
    context: &CtlContext,
    workers: Vec<String>,
    target: Schedulability,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let GetClusterInfoResponse { worker_nodes, .. } = match meta_client.get_cluster_info().await {
        Ok(resp) => resp,
        Err(e) => {
            println!("Failed to get cluster info: {:?}", e);
            exit(1);
        }
    };

    let worker_ids: HashSet<_> = worker_nodes.iter().map(|worker| worker.id).collect();

    let worker_index_by_host: HashMap<_, _> = worker_nodes
        .iter()
        .map(|worker| {
            let host = worker.get_host().expect("worker host must be set");
            (format!("{}:{}", host.host, host.port), worker.id)
        })
        .collect();

    let mut target_worker_ids = HashSet::new();

    for worker in workers {
        let worker_id = worker
            .parse::<u32>()
            .ok()
            .or_else(|| worker_index_by_host.get(&worker).cloned());

        if let Some(worker_id) = worker_id && worker_ids.contains(&worker_id){
            if !target_worker_ids.insert(worker_id) {
                println!("Warn: {} and {} are the same worker", worker, worker_id);
            }
        } else {
            println!("Invalid worker id: {}", worker);
            exit(1);
        }
    }

    let target_worker_ids = target_worker_ids.into_iter().collect_vec();

    meta_client
        .update_schedulability(&target_worker_ids, target)
        .await?;

    Ok(())
}
