// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use anyhow::{anyhow, Error, Result};
use regex::{Match, Regex};
use risingwave_pb::meta::reschedule_request::Reschedule;

use crate::common::MetaServiceOpts;

const RESCHEDULE_MATCH_REGEXP: &str =
    r"^(?P<fragment>\d+)(?:-\[(?P<removed>\d+(?:,\d+)*)])?(?:\+\[(?P<added>\d+(?:,\d+)*)])?$";
const RESCHEDULE_FRAGMENT_KEY: &str = "fragment";
const RESCHEDULE_REMOVED_KEY: &str = "removed";
const RESCHEDULE_ADDED_KEY: &str = "added";

pub async fn reschedule(plan: String, dry_run: bool) -> Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let regex = Regex::new(RESCHEDULE_MATCH_REGEXP)?;
    let mut reschedules = HashMap::new();

    let plan = {
        let mut plan = plan;
        plan.retain(|c| !c.is_whitespace());
        plan
    };

    for fragment_reschedule_plan in plan.split(';') {
        let captures = regex
            .captures(fragment_reschedule_plan)
            .ok_or_else(|| anyhow!("plan \"{}\" format illegal", fragment_reschedule_plan))?;

        let fragment_id = captures
            .name(RESCHEDULE_FRAGMENT_KEY)
            .and_then(|mat| mat.as_str().parse::<u32>().ok())
            .ok_or_else(|| anyhow!("plan \"{}\" does not have a valid fragment id", plan))?;

        let split_fn = |mat: Match| {
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
    }

    if !dry_run {
        println!("---------------------------");
        let resp = meta_client.reschedule(reschedules).await?;
        println!("Response from meta {}", resp);
    }

    Ok(())
}
