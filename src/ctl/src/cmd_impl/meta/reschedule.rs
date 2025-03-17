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

use std::collections::{HashMap, HashSet};
use std::process::exit;

use anyhow::{Result, anyhow};
use inquire::Confirm;
use itertools::Itertools;
use regex::Regex;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::{GetClusterInfoResponse, PbWorkerReschedule};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::CtlContext;

#[derive(Serialize, Deserialize, Debug)]
pub struct ReschedulePayload {
    #[serde(rename = "reschedule_revision")]
    pub reschedule_revision: u64,

    #[serde(rename = "reschedule_plan")]
    pub worker_reschedule_plan: HashMap<u32, WorkerReschedulePlan>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerReschedulePlan {
    #[serde(rename = "actor_count_diff")]
    pub actor_count_diff: HashMap<WorkerId, i32>,
}

#[derive(Debug)]
pub enum RescheduleInput {
    String(String),
    FilePath(String),
}

impl From<WorkerReschedulePlan> for PbWorkerReschedule {
    fn from(value: WorkerReschedulePlan) -> Self {
        let WorkerReschedulePlan { actor_count_diff } = value;

        PbWorkerReschedule {
            worker_actor_diff: actor_count_diff
                .into_iter()
                .map(|(k, v)| (k as _, v as _))
                .collect(),
        }
    }
}

impl From<PbWorkerReschedule> for WorkerReschedulePlan {
    fn from(value: PbWorkerReschedule) -> Self {
        let PbWorkerReschedule {
            worker_actor_diff: actor_count_diff,
        } = value;

        WorkerReschedulePlan {
            actor_count_diff: actor_count_diff
                .into_iter()
                .map(|(k, v)| (k as _, v as _))
                .collect(),
        }
    }
}

pub async fn reschedule(
    context: &CtlContext,
    plan: Option<String>,
    revision: Option<u64>,
    from: Option<String>,
    dry_run: bool,
    resolve_no_shuffle: bool,
) -> Result<()> {
    let meta_client = context.meta_client().await?;

    let (reschedules, revision) = match (plan, revision, from) {
        (Some(plan), Some(revision), None) => (parse_plan(plan)?, revision),
        (None, None, Some(path)) => {
            let file = std::fs::File::open(path)?;
            let ReschedulePayload {
                reschedule_revision,
                worker_reschedule_plan,
            } = serde_yaml::from_reader(file)?;
            (
                worker_reschedule_plan
                    .into_iter()
                    .map(|(fragment_id, worker_reschedule_plan)| {
                        (fragment_id, worker_reschedule_plan.into())
                    })
                    .collect(),
                reschedule_revision,
            )
        }
        _ => unreachable!(),
    };

    if reschedules.is_empty() {
        return Ok(());
    }

    for (fragment_id, reschedule) in &reschedules {
        println!("For fragment #{}", fragment_id);
        if !reschedule.get_worker_actor_diff().is_empty() {
            println!("\tChange: {:?}", reschedule.get_worker_actor_diff());
        }

        println!();
    }

    if !dry_run {
        println!("---------------------------");
        let (success, revision) = meta_client
            .reschedule(reschedules, revision, resolve_no_shuffle)
            .await?;

        if !success {
            println!(
                "Reschedule failed, please check the plan or the revision, current revision is {}",
                revision
            );

            return Err(anyhow!("reschedule failed"));
        }

        println!("Reschedule success, current revision is {}", revision);
    }

    Ok(())
}

// It will match formats like `1:[1:+1,2:-1,3:1];2:[1:1,2:1]`, indicating which workers' actors need to change in quantity for each fragment.
fn parse_plan(mut plan: String) -> Result<HashMap<u32, PbWorkerReschedule>> {
    let mut reschedules = HashMap::new();
    let regex = Regex::new(r"^(\d+):\[((?:\d+:[+-]?\d+,?)+)]$")?;
    plan.retain(|c| !c.is_whitespace());

    for fragment_reschedule_plan in plan.split(';') {
        if fragment_reschedule_plan.is_empty() {
            continue;
        }

        let captures = regex
            .captures(fragment_reschedule_plan)
            .ok_or_else(|| anyhow!("plan \"{}\" format illegal", fragment_reschedule_plan))?;

        let fragment_id = captures
            .get(1)
            .and_then(|mat| mat.as_str().parse::<u32>().ok())
            .ok_or_else(|| anyhow!("plan \"{}\" does not have a valid fragment id", plan))?;

        let worker_changes: Vec<&str> = captures[2].split(',').collect();

        let mut worker_actor_diff = HashMap::new();
        for worker_change in &worker_changes {
            let (worker_id, count) = worker_change
                .split(':')
                .map(|v| v.parse::<i32>().unwrap())
                .collect_tuple::<(_, _)>()
                .unwrap();

            if let Some(dup_change) = worker_actor_diff.insert(worker_id as u32, count) {
                anyhow::bail!(
                    "duplicate worker id {worker_id} in plan, prev {worker_id} -> {dup_change}",
                );
            }
        }

        if !worker_actor_diff.is_empty() {
            reschedules.insert(fragment_id, PbWorkerReschedule { worker_actor_diff });
        }
    }
    Ok(reschedules)
}

pub async fn unregister_workers(
    context: &CtlContext,
    workers: Vec<String>,
    yes: bool,
    ignore_not_found: bool,
    check_fragment_occupied: bool,
) -> Result<()> {
    let meta_client = context.meta_client().await?;

    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments: all_table_fragments,
        ..
    } = match meta_client.get_cluster_info().await {
        Ok(info) => info,
        Err(e) => {
            println!("Failed to get cluster info: {}", e.as_report());
            exit(1);
        }
    };

    let worker_index_by_host: HashMap<_, _> = worker_nodes
        .iter()
        .map(|worker| {
            let host = worker.get_host().expect("host should not be empty");
            (format!("{}:{}", host.host, host.port), worker.id)
        })
        .collect();

    let mut target_worker_ids: HashSet<_> = HashSet::new();

    let worker_ids: HashSet<_> = worker_nodes.iter().map(|worker| worker.id).collect();

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
            if ignore_not_found {
                println!("Warn: worker {} not found, ignored", worker);
                continue;
            }

            println!("Could not find worker {}", worker);
            exit(1);
        }
    }

    if target_worker_ids.is_empty() {
        if ignore_not_found {
            println!("Warn: No worker provided, ignored");
            return Ok(());
        }
        println!("No worker provided");
        exit(1);
    }

    let target_workers = worker_nodes
        .into_iter()
        .filter(|worker| target_worker_ids.contains(&worker.id))
        .collect_vec();

    for table_fragments in &all_table_fragments {
        for (fragment_id, fragment) in &table_fragments.fragments {
            let occupied_worker_ids: HashSet<_> = fragment
                .actors
                .iter()
                .map(|actor| {
                    table_fragments
                        .actor_status
                        .get(&actor.actor_id)
                        .map(|actor_status| actor_status.worker_id())
                        .unwrap()
                })
                .collect();

            let intersection_worker_ids: HashSet<_> = occupied_worker_ids
                .intersection(&target_worker_ids)
                .collect();

            if check_fragment_occupied && !intersection_worker_ids.is_empty() {
                println!(
                    "worker ids {:?} are still occupied by fragment #{}",
                    intersection_worker_ids, fragment_id
                );
                exit(1);
            }
        }
    }

    if !yes {
        match Confirm::new("Will perform actions on the cluster, are you sure?")
            .with_default(false)
            .with_help_message("Use the --yes or -y option to skip this prompt")
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

    for WorkerNode { id, host, .. } in target_workers {
        let host = match host {
            None => {
                println!("Worker #{} does not have a host, skipping", id);
                continue;
            }
            Some(host) => host,
        };

        println!("Unregistering worker #{}, address: {:?}", id, host);
        if let Err(e) = meta_client.delete_worker_node(host).await {
            println!("Failed to delete worker #{}: {}", id, e.as_report());
        };
    }

    println!("Done");

    Ok(())
}
