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

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;

#[madsim::test]
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

#[madsim::test]
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

#[madsim::test]
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

#[madsim::test]
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

#[madsim::test]
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
