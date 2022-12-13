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

#![cfg(madsim)]

use std::time::Duration;

use anyhow::Result;
use madsim::time::sleep;
use risingwave_simulation::cluster::Configuration;
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;

/// Check that the number of generated events is correct after scaling of source executor.
#[madsim::test]
async fn nexmark_source() -> Result<()> {
    let events = 20 * THROUGHPUT;

    let mut cluster = NexmarkCluster::new(Configuration::for_scale(), 6, Some(events)).await?;

    for table in ["person", "auction", "bid"] {
        cluster
            .run(&format!(
                "create materialized view count_{table} as select count(*) count_{table} from {table};"
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
    cluster
        .run(
            "select count_person + count_auction + count_bid from count_person, count_auction, count_bid;",
        )
        .await?
        .assert_result_eq(events.to_string());

    Ok(())
}
