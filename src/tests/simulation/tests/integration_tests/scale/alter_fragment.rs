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
use madsim::time::sleep;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;

#[tokio::test]
async fn test_alter_fragment_no_shuffle() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale_no_shuffle()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    cluster.run("create table t1 (c1 int, c2 int);").await?;
    let upstream_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;

    let upstream_fragment_id = upstream_fragment.id();
    cluster
        .run("create materialized view m as select * from t1;")
        .await?;

    let downstream_fragment = cluster
        .locate_one_fragment([identity_contains("StreamTableScan")])
        .await?;

    let downstream_fragment_id = downstream_fragment.id();

    let new_parallelism = default_parallelism + 1;

    assert!(
        cluster
            .run(&format!(
                "alter fragment {downstream_fragment_id} set parallelism = {new_parallelism};"
            ))
            .await
            .is_err()
    );

    cluster
        .run(&format!(
            "alter fragment {upstream_fragment_id} set parallelism = {new_parallelism};"
        ))
        .await?;

    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {upstream_fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{new_parallelism}"));

    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {downstream_fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{new_parallelism}"));

    Ok(())
}

#[tokio::test]
async fn test_alter_fragment() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    cluster.run("create table t1 (c1 int, c2 int);").await?;
    let materialize_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;

    let fragment_id = materialize_fragment.id();
    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{default_parallelism}"));

    let new_parallelism = default_parallelism + 1;

    cluster
        .run(&format!(
            "alter fragment {fragment_id} set parallelism = {new_parallelism};"
        ))
        .await?;

    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{new_parallelism}"));

    cluster
        .run(&"alter table t1 set parallelism = 1;".to_owned())
        .await?;

    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{new_parallelism}"));

    cluster.run(&"recover;".to_owned()).await?;

    sleep(Duration::from_secs(10)).await;

    cluster
        .run(&format!(
            "select parallelism from rw_fragment_parallelism where fragment_id = {fragment_id};"
        ))
        .await?
        .assert_result_eq(format!("{new_parallelism}"));

    Ok(())
}
