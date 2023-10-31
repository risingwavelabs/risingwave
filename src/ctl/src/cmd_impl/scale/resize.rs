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
use crate::{ScaleCommon, ScaleHorizonCommands, ScaleVerticalCommands};

macro_rules! fail {
    ($($arg:tt)*) => {{
        println!($($arg)*);
        exit(1);
    }};
}

impl From<ScaleHorizonCommands> for ScaleCommandContext {
    fn from(value: ScaleHorizonCommands) -> Self {
        let ScaleHorizonCommands {
            exclude_workers,
            include_workers,
            target_parallelism,
            common:
                ScaleCommon {
                    generate,
                    output,
                    yes,
                    fragments,
                },
        } = value;

        Self {
            exclude_workers,
            include_workers,
            target_parallelism,
            generate,
            output,
            yes,
            fragments,
            target_parallelism_per_worker: None,
        }
    }
}

impl From<ScaleVerticalCommands> for ScaleCommandContext {
    fn from(value: ScaleVerticalCommands) -> Self {
        let ScaleVerticalCommands {
            workers,
            target_parallelism_per_worker,
            common:
                ScaleCommon {
                    generate,
                    output,
                    yes,
                    fragments,
                },
        } = value;

        Self {
            exclude_workers: None,
            include_workers: workers,
            target_parallelism: None,
            generate,
            output,
            yes,
            fragments,
            target_parallelism_per_worker,
        }
    }
}

pub struct ScaleCommandContext {
    exclude_workers: Option<Vec<String>>,
    include_workers: Option<Vec<String>>,
    target_parallelism: Option<u32>,
    generate: bool,
    output: Option<String>,
    yes: bool,
    fragments: Option<Vec<u32>>,
    target_parallelism_per_worker: Option<u32>,
}

pub async fn resize(ctl_ctx: &CtlContext, scale_ctx: ScaleCommandContext) -> anyhow::Result<()> {
    let meta_client = ctl_ctx.meta_client().await?;

    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments,
        actor_splits: _actor_splits,
        source_infos: _source_infos,
        revision,
    } = match meta_client.get_cluster_info().await {
        Ok(resp) => resp,
        Err(e) => {
            fail!("Failed to fetch cluster info: {}", e);
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

    let worker_input_to_worker_ids = |inputs: Vec<String>, support_all: bool| -> Vec<u32> {
        let mut result: HashSet<_> = HashSet::new();

        if inputs.len() == 1 && inputs[0].to_lowercase() == "all" && support_all {
            return streaming_workers_index_by_id.keys().cloned().collect();
        }

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
                fail!("Invalid worker input: {}", input);
            }
        }

        result.into_iter().collect()
    };

    println!(
        "Streaming workers found: {}",
        streaming_workers_index_by_id.len()
    );

    let ScaleCommandContext {
        exclude_workers,
        include_workers,
        target_parallelism,
        target_parallelism_per_worker,
        generate,
        output,
        yes,
        fragments,
    } = scale_ctx;

    let worker_changes = {
        let exclude_worker_ids =
            worker_input_to_worker_ids(exclude_workers.unwrap_or_default(), false);
        let include_worker_ids =
            worker_input_to_worker_ids(include_workers.unwrap_or_default(), true);

        match (target_parallelism, target_parallelism_per_worker) {
            (Some(_), Some(_)) => {
                fail!("Cannot specify both target parallelism and target parallelism per worker")
            }
            (_, Some(_)) if include_worker_ids.is_empty() => {
                fail!("Cannot specify target parallelism per worker without including any worker")
            }
            (Some(0), _) => fail!("Target parallelism must be greater than 0"),
            _ => {}
        }

        for worker_id in exclude_worker_ids.iter().chain(include_worker_ids.iter()) {
            if !streaming_workers_index_by_id.contains_key(worker_id) {
                fail!("Invalid worker id: {}", worker_id);
            }
        }

        for include_worker_id in &include_worker_ids {
            let worker_is_unschedulable = streaming_workers_index_by_id
                .get(include_worker_id)
                .and_then(|worker| worker.property.as_ref())
                .map(|property| property.is_unschedulable)
                .unwrap_or(false);

            if worker_is_unschedulable {
                fail!(
                    "Worker {} is unschedulable, should not be included",
                    include_worker_id
                );
            }
        }

        WorkerChanges {
            include_worker_ids,
            exclude_worker_ids,
            target_parallelism,
            target_parallelism_per_worker,
        }
    };

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
                fail!(
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

    let response = meta_client.get_reschedule_plan(policy, revision).await;

    let GetReschedulePlanResponse {
        revision,
        reschedules,
        success,
    } = match response {
        Ok(response) => response,
        Err(e) => {
            fail!("Failed to generate plan: {:?}", e);
        }
    };

    if !success {
        fail!("Failed to generate plan, current revision is {}", revision);
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
                    fail!("Abort.");
                }
                Err(_) => {
                    fail!("Error with questionnaire, try again later");
                }
            }
        }

        let (success, next_revision) =
            match meta_client.reschedule(reschedules, revision, false).await {
                Ok(response) => response,
                Err(e) => {
                    fail!("Failed to execute plan: {:?}", e);
                }
            };

        if !success {
            fail!("Failed to execute plan, current revision is {}", revision);
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
            fail!("Failed to get cluster info: {:?}", e);
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

        if let Some(worker_id) = worker_id
            && worker_ids.contains(&worker_id)
        {
            if !target_worker_ids.insert(worker_id) {
                println!("Warn: {} and {} are the same worker", worker, worker_id);
            }
        } else {
            fail!("Invalid worker id: {}", worker);
        }
    }

    let target_worker_ids = target_worker_ids.into_iter().collect_vec();

    meta_client
        .update_schedulability(&target_worker_ids, target)
        .await?;

    Ok(())
}
