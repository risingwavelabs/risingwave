// Copyright 2024 RisingWave Labs
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
use risingwave_common::hash::VirtualNode;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;

use crate::scale::auto_parallelism::MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE;

#[tokio::test]
async fn test_streaming_parallelism_default() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    cluster.run("create table t1 (c1 int, c2 int);").await?;
    let materialize_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    assert_eq!(materialize_fragment.inner.actors.len(), default_parallelism);
    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_set_some() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    let target_parallelism = default_parallelism - 1;
    assert!(target_parallelism > 0);

    let mut session = cluster.start_session();
    session
        .run(format!("set streaming_parallelism={};", target_parallelism))
        .await?;
    session.run("create table t1 (c1 int, c2 int);").await?;

    let materialize_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    assert_eq!(materialize_fragment.inner.actors.len(), target_parallelism);
    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_set_zero() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;

    let mut session = cluster.start_session();
    session.run("set streaming_parallelism=0;").await?;
    session.run("create table t1 (c1 int, c2 int);").await?;

    let materialize_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    assert_eq!(materialize_fragment.inner.actors.len(), default_parallelism);
    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_mv_on_mv() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    let target_parallelism = default_parallelism - 1;
    assert!(target_parallelism - 1 > 0);

    let mut session = cluster.start_session();
    session
        .run(format!("set streaming_parallelism={};", target_parallelism))
        .await?;
    session.run("create table t1 (c1 int, c2 int);").await?;
    session
        .run(format!(
            "set streaming_parallelism={};",
            target_parallelism - 1
        ))
        .await?;
    session
        .run("create materialized view mv1 as select c1,count(*) as cc from t1 group by c1;")
        .await?;

    let materialize_fragments = cluster
        .locate_fragments([identity_contains("materialize")])
        .await?;
    assert_eq!(materialize_fragments.len(), 2);
    assert_eq!(
        materialize_fragments[0].inner.actors.len(),
        target_parallelism
    );
    assert_eq!(
        materialize_fragments[1].inner.actors.len(),
        target_parallelism - 1
    );
    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_index() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    let target_parallelism = default_parallelism - 1;
    assert!(target_parallelism - 1 > 0);

    let mut session = cluster.start_session();
    session
        .run(format!("set streaming_parallelism={};", target_parallelism))
        .await?;
    session.run("create table t1 (c1 int, c2 int);").await?;
    session
        .run(format!(
            "set streaming_parallelism={};",
            target_parallelism - 1
        ))
        .await?;
    session.run("create index idx1 on t1(c2);").await?;

    let materialize_fragments = cluster
        .locate_fragments([identity_contains("materialize")])
        .await?;
    assert_eq!(materialize_fragments.len(), 2);
    assert_eq!(
        materialize_fragments[0].inner.actors.len(),
        target_parallelism
    );
    assert_eq!(
        materialize_fragments[1].inner.actors.len(),
        target_parallelism - 1
    );
    Ok(())
}

#[tokio::test]
async fn test_parallelism_exceed_virtual_node_max_create() -> Result<()> {
    let vnode_max = VirtualNode::COUNT_FOR_COMPAT;
    let mut configuration = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        true,
    );

    configuration.compute_nodes = 1;
    configuration.compute_node_cores = vnode_max + 1;
    let mut cluster = Cluster::start(configuration).await?;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let mut session = cluster.start_session();
    session.run("create table t(v int)").await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't'")
        .await?
        .assert_result_eq("ADAPTIVE");

    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 't'")
        .await?
        .assert_result_eq(format!("{}", vnode_max));

    Ok(())
}

#[tokio::test]
async fn test_parallelism_exceed_virtual_node_max_alter_fixed() -> Result<()> {
    let vnode_max = VirtualNode::COUNT_FOR_COMPAT;
    let mut configuration = Configuration::for_scale();
    configuration.compute_nodes = 1;
    configuration.compute_node_cores = vnode_max + 100;
    let mut cluster = Cluster::start(configuration).await?;
    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 1").await?;
    session.run("create table t(v int)").await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't'")
        .await?
        .assert_result_eq("FIXED(1)");

    session
        .run(format!("alter table t set parallelism = {}", vnode_max + 1))
        .await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't'")
        .await?
        .assert_result_eq(format!("FIXED({})", vnode_max));
    Ok(())
}

#[tokio::test]
async fn test_parallelism_exceed_virtual_node_max_alter_adaptive() -> Result<()> {
    let vnode_max = VirtualNode::COUNT_FOR_COMPAT;
    let mut configuration = Configuration::for_scale();
    configuration.compute_nodes = 1;
    configuration.compute_node_cores = vnode_max + 100;
    let mut cluster = Cluster::start(configuration).await?;
    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 1").await?;
    session.run("create table t(v int)").await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't'")
        .await?
        .assert_result_eq("FIXED(1)");

    session
        .run("alter table t set parallelism = adaptive")
        .await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't'")
        .await?
        .assert_result_eq("ADAPTIVE");

    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 't'")
        .await?
        .assert_result_eq(format!("{}", vnode_max));

    Ok(())
}
