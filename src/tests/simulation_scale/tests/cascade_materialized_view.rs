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

use anyhow::Result;
use madsim::time::sleep;
use std::time::Duration;
use risingwave_simulation_scale::cluster::{Cluster, Configuration};
use risingwave_simulation_scale::ctl_ext::predicate::{identity_contains, identity_not_contains};

const ROOT_TABLE_CREATE: &str = "create table t1 (v1 int);";
const MV1: &str = "create materialized view m1 as select * from t1 where v1 > 10;";
const MV2: &str = "create materialized view m2 as select * from t1 where v1 > 20;";
const MV3: &str = "create materialized view m3 as select * from m2 where v1 < 70;";
const MV4: &str = "create materialized view m4 as select m1.v1 as m1v, m3.v1 as m3v from m1 join m3 on m1.v1 = m3.v1;";
const MV5: &str = "create materialized view m5 as select * from m4;";

#[madsim::test]
async fn test_cascade_materialized_view() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::default()).await?;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(MV1).await?;
    cluster.run(MV2).await?;
    cluster.run(MV3).await?;
    cluster.run(MV4).await?;
    cluster.run(MV5).await?;

    let fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_not_contains("chain"),
            identity_not_contains("hashjoin"),
        ])
        .await?;

    let id = fragment.id();

    // Let parallel unit 0 handle all groups.
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    let fragment = cluster.locate_fragment_by_id(id).await?;

    assert_eq!(fragment.inner.actors.len(), 1);

    Ok(())
}
