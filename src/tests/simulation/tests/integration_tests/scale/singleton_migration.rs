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

use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const ROOT_TABLE_CREATE: &str = "create table t (v1 int);";
const ROOT_MV: &str = "create materialized view m1 as select count(*) as c1 from t;";
const CASCADE_MV: &str = "create materialized view m2 as select * from m1;";

async fn test_singleton_migration_helper(configuration: Configuration) -> Result<()> {
    let mut cluster = Cluster::start(configuration).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;
    session.run(ROOT_MV).await?;
    session.run(CASCADE_MV).await?;

    for (idx, ids) in [[1, 2], [2, 3], [3, 1]].iter().enumerate() {
        let nodes = ids.iter().map(|id| format!("compute-{}", id)).collect_vec();
        cluster.simple_kill_nodes(nodes.clone()).await;
        sleep(Duration::from_secs(100)).await;
        session
            .run(&format!(
                "insert into t values {}",
                (1..=10).map(|x| format!("({x})")).join(",")
            ))
            .await?;
        session.run("flush").await?;
        cluster.simple_restart_nodes(nodes).await;
        sleep(Duration::from_secs(100)).await;
        session
            .run("select * from m2")
            .await?
            .assert_result_eq(format!("{}", (idx + 1) * 10));
    }

    Ok(())
}

#[tokio::test]
async fn test_singleton_migration() -> Result<()> {
    test_singleton_migration_helper(Configuration::for_scale()).await
}

#[tokio::test]
async fn test_singleton_migration_for_no_shuffle() -> Result<()> {
    test_singleton_migration_helper(Configuration::for_scale_no_shuffle()).await
}
