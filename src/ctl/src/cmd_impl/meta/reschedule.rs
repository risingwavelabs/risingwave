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

use anyhow::{anyhow, Error, Result};
use itertools::Itertools;
use regex::{Match, Regex};
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::reschedule_request::Reschedule;

use crate::CtlContext;

const RESCHEDULE_MATCH_REGEXP: &str =
    r"^(?P<fragment>\d+)(?:-\[(?P<removed>\d+(?:,\d+)*)])?(?:\+\[(?P<added>\d+(?:,\d+)*)])?$";
const RESCHEDULE_FRAGMENT_KEY: &str = "fragment";
const RESCHEDULE_REMOVED_KEY: &str = "removed";
const RESCHEDULE_ADDED_KEY: &str = "added";

fn str_to_addr(ip: &str) -> anyhow::Result<HostAddress> {
    let ip_addr_regex = r"(\b25[0-5]|\b2[0-4][0-9]|\b[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}:\d+";
    let re = Regex::new(ip_addr_regex).unwrap();
    if !re.is_match(ip) {
        return Err(anyhow!("Please provide a valid IP, e.g. 127.0.0.1:1234"));
    }
    let splits = ip.split(':').map(|s| s.to_owned()).collect_vec();
    let host = splits[0].clone();
    let port = splits[1].clone().parse::<i32>()?;
    Ok(HostAddress { host, port })
}

// Mark a compute node as unschedulable
pub async fn cordon(context: &CtlContext, ip: String) -> anyhow::Result<()> {
    let addr = str_to_addr(ip.as_str())?;
    let meta_client = context.meta_client().await?;
    meta_client.cordon_worker(addr).await?;
    Ok(())
}

// Mark a compute node as schedulable
pub async fn uncordon(context: &CtlContext, ip: String) -> anyhow::Result<()> {
    let addr = str_to_addr(ip.as_str())?;
    tracing::info!("trying to uncordon {:?}", addr);
    let meta_client = context.meta_client().await?;
    meta_client.uncordon_worker(addr).await?;

    Ok(())
}

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
pub async fn reschedule(context: &CtlContext, mut plan: String, dry_run: bool) -> Result<()> {
    let meta_client = context.meta_client().await?;

    let regex = Regex::new(RESCHEDULE_MATCH_REGEXP)?;
    let mut reschedules = HashMap::new();

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
        let resp = meta_client.reschedule(reschedules).await?;
        println!("Response from meta {}", resp);
    }

    Ok(())
}
