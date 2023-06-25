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
use regex::{Match, Regex};
use risingwave_pb::meta::reschedule_request::Reschedule;
use serde::{Deserialize, Serialize};
use serde_yaml;

use crate::CtlContext;

#[derive(Serialize, Deserialize, Debug)]
pub struct ReschedulePayload {
    #[serde(rename = "reschedule_revision")]
    reschedule_revision: u64,

    #[serde(rename = "reschedule_plan")]
    reschedule_plan: HashMap<u32, FragmentReschedulePlan>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentReschedulePlan {
    #[serde(rename = "added_parallel_units")]
    added_parallel_units: Vec<u32>,

    #[serde(rename = "removed_parallel_units")]
    removed_parallel_units: Vec<u32>,
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
        let (success, revision) = meta_client.reschedule(reschedules, revision).await?;

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
