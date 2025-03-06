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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 1000;

#[tokio::test]
async fn test_isolation_simple_two_databases() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(
        session
            .run("insert into t1 select * from generate_series(1, 100);")
            .await
            .is_err()
    );

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(1, 100);")
        .await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;

    session
        .run("select count(*) from group1.public.t1;")
        .await?
        .assert_result_eq("100");

    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join t2 on t1.v = t2.v;")
        .await?;

    session
        .run("select count(*) from mv_join;")
        .await?
        .assert_result_eq("50");

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    session
        .run("select max(v) from t2")
        .await?
        .assert_result_eq("120");

    session
        .run("select count(*) from mv_join;")
        .await?
        .assert_result_eq("100");

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC * 2)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    session.run("use group2").await?;

    // flush is only oriented to the current database, so flush is required here
    session.run("flush").await?;

    session
        .run("select count(*) from mv_join;")
        .await?
        .assert_result_eq("110");

    Ok(())
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join_in_other() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    // group1
    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    // group2
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;
    session
        .run("create subscription sub2 from t2 with(retention = '1D');")
        .await?;

    // group3
    session.run("use group3").await?;
    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join group2.public.t2 on t1.v = t2.v;")
        .await?;

    session
        .run("select count(*) from mv_join;")
        .await?
        .assert_result_eq("50");

    cluster.simple_kill_nodes(["compute-1", "compute-3"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    // should fail
    assert!(session.run("flush").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    session.run("flush").await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;

    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    cluster.simple_restart_nodes(["compute-3"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group3").await?;

    session
        .run("select count(*) from mv_join;")
        .await?
        .assert_result_eq("110");

    Ok(())
}

async fn prepare_isolation_env() -> Result<(Cluster, Session)> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group2".to_owned()),
        (3, "group3".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await?;
    session
        .run("create database group2 with resource_group='group2'")
        .await?;
    session
        .run("create database group3 with resource_group='group3'")
        .await?;

    session.run("set rw_implicit_flush = true;").await?;

    Ok((cluster, session))
}
