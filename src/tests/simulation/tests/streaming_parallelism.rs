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
use risingwave_simulation::client::RisingWave;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use sqllogictest::runner::AsyncDB;

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

async fn run_sqls_in_session(cluster: &Cluster, sqls: Vec<String>) {
    cluster
        .run_on_client(async move {
            let mut session = RisingWave::connect("frontend".into(), "dev".into())
                .await
                .expect("failed to connect to RisingWave");
            for sql in sqls {
                session.run(&sql).await.unwrap();
            }
        })
        .await;
}

#[madsim::test]
async fn test_streaming_parallelism_set_some() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let default_parallelism = cluster.config().compute_nodes * cluster.config().compute_node_cores;
    let target_parallelism = default_parallelism - 1;
    assert!(target_parallelism > 0);
    run_sqls_in_session(
        &cluster,
        vec![
            format!("set streaming_parallelism={};", target_parallelism),
            "create table t1 (c1 int, c2 int);".to_string(),
        ],
    )
    .await;
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
    run_sqls_in_session(
        &cluster,
        vec![
            "set streaming_parallelism=0;".to_string(),
            "create table t1 (c1 int, c2 int);".to_string(),
        ],
    )
    .await;
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
    run_sqls_in_session(
        &cluster,
        vec![
            format!("set streaming_parallelism={};", target_parallelism),
            "create table t1 (c1 int, c2 int);".to_string(),
            format!("set streaming_parallelism={};", target_parallelism - 1),
            "create materialized view mv3 as select c1,count(*) as cc from t1 group by c1;"
                .to_string(),
        ],
    )
    .await;
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
    run_sqls_in_session(
        &cluster,
        vec![
            format!("set streaming_parallelism={};", target_parallelism),
            "create table t1 (c1 int, c2 int);".to_string(),
            format!("set streaming_parallelism={};", target_parallelism - 1),
            "create index idx1 on t1(c2);".to_string(),
        ],
    )
    .await;
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
