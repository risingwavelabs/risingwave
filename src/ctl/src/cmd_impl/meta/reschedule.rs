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
use risingwave_common::bail;
use risingwave_pb::meta::reschedule_request::Reschedule;
use crate::common::MetaServiceOpts;


pub async fn reschedule(plan: String) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let mut reschedules = HashMap::new();
    for fragment_reschedule_plan in plan.split(";") {
        let slice: Vec<_> = fragment_reschedule_plan.split(",").collect();

        if slice.len() != 3 {
            bail!("should be in format frag_id:removed:added;")
        }

        let fragment_id = slice[0].parse::<u32>()?;
        let removed_parallel_units = slice[1].split(",").map(|id_str| id_str.parse::<u32>().unwrap()).collect();
        let added_parallel_units = slice[2].split(",").map(|id_str| id_str.parse::<u32>().unwrap()).collect();

        reschedules.insert(fragment_id, Reschedule {
            added_parallel_units,
            removed_parallel_units,
        });
    }


    for (fragment_id, reschedule) in &reschedules {
        println!("For fragment #{}", fragment_id);
        println!("\tRemove: [{:?}]", reschedule.removed_parallel_units);
        println!("\tAdd:    [{:?}]", reschedule.added_parallel_units);
    }

    let resp = meta_client.reschedule(reschedules).await?;
    println!("Response from meta {}", resp);

    Ok(())
}