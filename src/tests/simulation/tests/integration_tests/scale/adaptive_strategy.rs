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

use anyhow::Result;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;

#[tokio::test]
async fn test_streaming_parallelism_adaptive_variants() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = adaptive").await?;
    session.run("create table t_auto(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_auto' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    session
        .run("set streaming_parallelism = 'bounded(2)'")
        .await?;
    session.run("create table t_bounded(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_bounded' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    session
        .run("set streaming_parallelism = 'ratio(0.5)'")
        .await?;
    session.run("create table t_ratio(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_ratio' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("set streaming_parallelism = 'ratio(0.00001)'")
        .await?;
    session.run("create table t_ratio_min(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_ratio_min' and distribution_type = 'HASH';").await?.assert_result_eq("1");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_type_override() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_parallelism = 'ratio(0.5)'")
        .await?;
    session.run("create table t_base(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_base' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("set streaming_parallelism_for_materialized_view = 'bounded(2)'")
        .await?;
    session
        .run("create materialized view m_override as select * from t_base")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm_override' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    Ok(())
}

#[tokio::test]
async fn test_rw_streaming_parallelism_display_uses_unified_dsl() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_parallelism = 'ratio(0.5)'")
        .await?;
    session.run("create table t_display(v int)").await?;
    session
        .run("create materialized view m_display as select * from t_display")
        .await?;

    session
        .run("select parallelism from rw_streaming_parallelism where name = 't_display'")
        .await?
        .assert_result_eq("bounded(4)");
    session
        .run("select parallelism from rw_streaming_parallelism where name = 'm_display'")
        .await?
        .assert_result_eq("ratio(0.5)");

    Ok(())
}

#[tokio::test]
async fn test_alter_parallelism_accepts_unified_dsl() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("create table t_alter(v int)").await?;

    session
        .run("alter table t_alter set parallelism = bounded(2)")
        .await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't_alter'")
        .await?
        .assert_result_eq("bounded(2)");
    session
        .run(
            "select distinct parallelism from rw_fragment_parallelism where name = 't_alter' and distribution_type = 'HASH'",
        )
        .await?
        .assert_result_eq("2");

    session
        .run("alter table t_alter set parallelism = ratio(0.5)")
        .await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't_alter'")
        .await?
        .assert_result_eq("ratio(0.5)");
    session
        .run(
            "select distinct parallelism from rw_fragment_parallelism where name = 't_alter' and distribution_type = 'HASH'",
        )
        .await?
        .assert_result_eq("3");

    session
        .run("alter table t_alter set parallelism = adaptive")
        .await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't_alter'")
        .await?
        .assert_result_eq("adaptive");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_default_fallback() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_parallelism = 'ratio(0.5)'")
        .await?;
    session
        .run("set streaming_parallelism_for_materialized_view = default")
        .await?;
    session.run("create table t_base(v int)").await?;
    session
        .run("create materialized view m_fallback as select * from t_base")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm_fallback' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_persistence() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_parallelism = 'ratio(0.5)'")
        .await?;
    session.run("create table t_persist(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_persist' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("set streaming_parallelism = 'bounded(2)'")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_persist' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_adaptive_resource_group_scale() -> Result<()> {
    let mut config = Configuration::for_arrangement_backfill();
    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, DEFAULT_RESOURCE_GROUP.to_owned()),
        (2, "test".to_owned()),
        (3, "test".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = adaptive").await?;
    session
        .run("set streaming_use_arrangement_backfill = true")
        .await?;
    session.run("create table t(v int)").await?;
    session
        .run("create materialized view m as select * from t")
        .await?;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't' and distribution_type = 'HASH';").await?.assert_result_eq("2");
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    session
        .run("alter materialized view m set resource_group to 'test'")
        .await?;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm' and distribution_type = 'HASH';").await?.assert_result_eq("4");

    Ok(())
}
