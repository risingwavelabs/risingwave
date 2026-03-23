// Copyright 2022 RisingWave Labs
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

use anyhow::Result;
use inquire::Confirm;
use itertools::Itertools;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::GetClusterInfoResponse;
use thiserror_ext::AsReport;

use crate::CtlContext;

const LEGACY_MANUAL_RESCHEDULE_REMOVED: &str = "legacy manual reschedule has been removed; use ALTER ... SET PARALLELISM / ALTER FRAGMENT ... instead";

pub async fn reschedule(
    _context: &CtlContext,
    _plan: Option<String>,
    _revision: Option<u64>,
    _from: Option<String>,
    _dry_run: bool,
    _resolve_no_shuffle: bool,
) -> Result<()> {
    anyhow::bail!(LEGACY_MANUAL_RESCHEDULE_REMOVED)
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
            .parse::<WorkerId>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reschedule_returns_removed_error_without_meta_access() {
        let context = CtlContext::default();
        let err = reschedule(
            &context,
            Some("1:[1:+1]".to_owned()),
            Some(0),
            None,
            false,
            false,
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), LEGACY_MANUAL_RESCHEDULE_REMOVED);
    }
}
