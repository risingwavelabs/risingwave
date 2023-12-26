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

use anyhow::{anyhow, Error, Result};
use inquire::Confirm;
use itertools::Itertools;
use regex::{Match, Regex};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::get_reschedule_plan_request::PbPolicy;
use risingwave_pb::meta::table_fragments::ActorStatus;
use risingwave_pb::meta::{GetClusterInfoResponse, GetReschedulePlanResponse, Reschedule};
use serde::{Deserialize, Serialize};
use serde_yaml;
use thiserror_ext::AsReport;

use crate::CtlContext;

#[derive(Serialize, Deserialize, Debug)]
pub struct ReschedulePayload {
    #[serde(rename = "reschedule_revision")]
    pub reschedule_revision: u64,

    #[serde(rename = "reschedule_plan")]
    pub reschedule_plan: HashMap<u32, FragmentReschedulePlan>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentReschedulePlan {
    #[serde(rename = "added_parallel_units")]
    pub added_parallel_units: Vec<u32>,

    #[serde(rename = "removed_parallel_units")]
    pub removed_parallel_units: Vec<u32>,
}

#[derive(Debug)]
pub enum RescheduleInput {
    String(String),
    FilePath(String),
}

impl From<FragmentReschedulePlan> for Reschedule {
    fn from(value: FragmentReschedulePlan) -> Self {
        let FragmentReschedulePlan {
            added_parallel_units,
            removed_parallel_units,
        } = value;

        Reschedule {
            added_parallel_units,
            removed_parallel_units,
        }
    }
}

impl From<Reschedule> for FragmentReschedulePlan {
    fn from(value: Reschedule) -> Self {
        let Reschedule {
            added_parallel_units,
            removed_parallel_units,
        } = value;

        FragmentReschedulePlan {
            added_parallel_units,
            removed_parallel_units,
        }
    }
}

const RESCHEDULE_MATCH_REGEXP: &str =
    r"^(?P<fragment>\d+)(?:-\[(?P<removed>\d+(?:,\d+)*)])?(?:\+\[(?P<added>\d+(?:,\d+)*)])?$";
const RESCHEDULE_FRAGMENT_KEY: &str = "fragment";
const RESCHEDULE_REMOVED_KEY: &str = "removed";
const RESCHEDULE_ADDED_KEY: &str = "added";

// For plan `100-[1,2,3]+[4,5];101-[1];102+[3]`, the following reschedule request will be generated
// {
//     100: Reschedule {
//         added_parallel_units: [
//             4,
//             5,
//         ],
//         removed_parallel_units: [
//             1,
//             2,
//             3,
//         ],
//     },
//     101: Reschedule {
//         added_parallel_units: [],
//         removed_parallel_units: [
//             1,
//         ],
//     },
//     102: Reschedule {
//         added_parallel_units: [
//             3,
//         ],
//         removed_parallel_units: [],
//     },
// }
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
                reschedule_plan,
            } = serde_yaml::from_reader(file)?;
            (
                reschedule_plan
                    .into_iter()
                    .map(|(fragment_id, fragment_reschedule_plan)| {
                        (fragment_id, fragment_reschedule_plan.into())
                    })
                    .collect(),
                reschedule_revision,
            )
        }
        _ => unreachable!(),
    };

    for (fragment_id, reschedule) in &reschedules {
        println!("For fragment #{}", fragment_id);
        if !reschedule.removed_parallel_units.is_empty() {
            println!("\tRemove: {:?}", reschedule.removed_parallel_units);
        }

        if !reschedule.added_parallel_units.is_empty() {
            println!("\tAdd:    {:?}", reschedule.added_parallel_units);
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

fn parse_plan(mut plan: String) -> Result<HashMap<u32, Reschedule>, Error> {
    let mut reschedules = HashMap::new();

    let regex = Regex::new(RESCHEDULE_MATCH_REGEXP)?;

    plan.retain(|c| !c.is_whitespace());

    for fragment_reschedule_plan in plan.split(';') {
        let captures = regex
            .captures(fragment_reschedule_plan)
            .ok_or_else(|| anyhow!("plan \"{}\" format illegal", fragment_reschedule_plan))?;

        let fragment_id = captures
            .name(RESCHEDULE_FRAGMENT_KEY)
            .and_then(|mat| mat.as_str().parse::<u32>().ok())
            .ok_or_else(|| anyhow!("plan \"{}\" does not have a valid fragment id", plan))?;

        let split_fn = |mat: Match<'_>| {
            mat.as_str()
                .split(',')
                .map(|id_str| id_str.parse::<u32>().map_err(Error::msg))
                .collect::<Result<Vec<_>>>()
        };

        let removed_parallel_units = captures
            .name(RESCHEDULE_REMOVED_KEY)
            .map(split_fn)
            .transpose()?
            .unwrap_or_default();
        let added_parallel_units = captures
            .name(RESCHEDULE_ADDED_KEY)
            .map(split_fn)
            .transpose()?
            .unwrap_or_default();

        if !(removed_parallel_units.is_empty() && added_parallel_units.is_empty()) {
            reschedules.insert(
                fragment_id,
                Reschedule {
                    added_parallel_units,
                    removed_parallel_units,
                },
            );
        }
    }
    Ok(reschedules)
}

pub async fn get_reschedule_plan(
    context: &CtlContext,
    policy: PbPolicy,
    revision: u64,
) -> Result<GetReschedulePlanResponse> {
    let meta_client = context.meta_client().await?;
    let response = meta_client.get_reschedule_plan(policy, revision).await?;
    Ok(response)
}

pub async fn unregister_workers(
    context: &CtlContext,
    workers: Vec<String>,
    yes: bool,
    ignore_not_found: bool,
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
                        .and_then(|ActorStatus { parallel_unit, .. }| parallel_unit.clone())
                        .unwrap()
                        .worker_node_id
                })
                .collect();

            let intersection_worker_ids: HashSet<_> = occupied_worker_ids
                .intersection(&target_worker_ids)
                .collect();

            if !intersection_worker_ids.is_empty() {
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
