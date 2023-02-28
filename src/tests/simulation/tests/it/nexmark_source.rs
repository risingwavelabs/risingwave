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

#![cfg(madsim)]

use std::time::Duration;

use anyhow::Result;
use madsim::time::sleep;
use risingwave_simulation::cluster::Configuration;
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;

/// Check that everything works well after scaling of source-related executor.
#[madsim::test]
async fn nexmark_source() -> Result<()> {
    let events = 20 * THROUGHPUT;

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(events), false).await?;

    // Materialize all sources so that we can also check whether the row id generator is working
    // correctly after scaling.
    // https://github.com/risingwavelabs/risingwave/issues/7103
    for table in ["person", "auction", "bid"] {
        cluster
            .run(&format!(
                "create materialized view materialized_{table} as select * from {table};"
            ))
            .await?;
    }

    macro_rules! reschedule {
        () => {
            let fragments = cluster
                .locate_fragments([identity_contains("StreamSource")])
                .await?;
            assert_eq!(fragments.len(), 3);
            for fragment in fragments {
                cluster.reschedule(fragment.random_reschedule()).await?;
            }
        };
    }

    sleep(Duration::from_secs(5)).await;
    reschedule!();

    sleep(Duration::from_secs(5)).await;
    reschedule!();

    sleep(Duration::from_secs(30)).await;

    // Check the total number of events.
    cluster
        .run(
            r#"
with count_p as (select count(*) count_p from materialized_person),
     count_a as (select count(*) count_a from materialized_auction),
     count_b as (select count(*) count_b from materialized_bid)
select count_p + count_a + count_b from count_p, count_a, count_b;"#,
        )
        .await?
        .assert_result_eq(events.to_string());

    Ok(())
}
