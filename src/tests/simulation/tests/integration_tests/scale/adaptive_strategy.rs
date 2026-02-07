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
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

#[tokio::test]
async fn test_adaptive_strategy_create() -> Result<()> {
    // 3cn * 2core
    let config = Configuration::for_auto_parallelism(10, true);

    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;
    session
        .run("alter system set adaptive_parallelism_strategy to AUTO")
        .await?;
    session.run("create table t_auto(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_auto' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    session
        .run("alter system set adaptive_parallelism_strategy to FULL")
        .await?;
    session.run("create table t_full(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_full' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;
    session.run("create table t_bounded_2(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_bounded_2' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    session
        .run("alter system set adaptive_parallelism_strategy to 'RATIO(0.5)'")
        .await?;
    session.run("create table t_ratio_half(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_ratio_half' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("alter system set adaptive_parallelism_strategy to 'RATIO(0.00001)'")
        .await?;
    session.run("create table t_ratio_min(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_ratio_min' and distribution_type = 'HASH';").await?.assert_result_eq("1");

    Ok(())
}

#[tokio::test]
async fn test_adaptive_strategy_alter() -> Result<()> {
    // 3cn * 2core
    let config = Configuration::for_auto_parallelism(10, true);

    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to AUTO")
        .await?;
    session.run("create table t(v int)").await?;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    session
        .run("alter system set adaptive_parallelism_strategy to FULL")
        .await?;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;

    sleep(Duration::from_secs(100)).await;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    session
        .run("alter system set adaptive_parallelism_strategy to 'RATIO(0.5)'")
        .await?;

    sleep(Duration::from_secs(100)).await;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_adaptive_strategy_alter_resource_group() -> Result<()> {
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

    session
        .run("SET STREAMING_USE_ARRANGEMENT_BACKFILL = true;")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to AUTO")
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

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;

    sleep(Duration::from_secs(100)).await;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_strategy_session_override() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;
    session.run("create table t_sys(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_sys' and distribution_type = 'HASH';").await?.assert_result_eq("2");

    session
        .run("set streaming_parallelism_strategy = 'RATIO(0.5)'")
        .await?;
    session.run("create table t_session(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_session' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_strategy_for_materialized_view() -> Result<()> {
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;
    session
        .run("set streaming_parallelism_strategy = 'RATIO(0.5)'")
        .await?;
    session.run("create table t_base(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_base' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("set streaming_parallelism_strategy_for_materialized_view = 'FULL'")
        .await?;
    session
        .run("create materialized view m_strategy as select * from t_base")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm_strategy' and distribution_type = 'HASH';").await?.assert_result_eq("6");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_strategy_persistence() -> Result<()> {
    // Session-level strategy should persist for created jobs even after altering system strategy.
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;
    session
        .run("set streaming_parallelism_strategy = 'RATIO(0.5)'")
        .await?;
    session.run("create table t_persist(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_persist' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    // Change system-wide strategy; existing job should keep its session-level strategy.
    session
        .run("alter system set adaptive_parallelism_strategy to 'FULL'")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_persist' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_fixed_ignores_strategy() -> Result<()> {
    // Fixed parallelism should ignore adaptive strategy settings.
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session.run("set streaming_parallelism = 4").await?;
    session
        .run("set streaming_parallelism_strategy = 'RATIO(0.5)'")
        .await?;
    session.run("create table t_fixed(v int)").await?;

    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_fixed' and distribution_type = 'HASH';").await?.assert_result_eq("4");

    Ok(())
}

#[tokio::test]
async fn test_streaming_parallelism_strategy_default_fallback() -> Result<()> {
    // `Default` should fall back to session/global strategy chain.
    let config = Configuration::for_auto_parallelism(10, true);
    let total_cores = config.total_streaming_cores();
    assert_eq!(total_cores, 6u32);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    session
        .run("set streaming_parallelism_strategy_for_table = 'DEFAULT'")
        .await?;

    session
        .run("alter system set adaptive_parallelism_strategy to 'BOUNDED(2)'")
        .await?;
    session
        .run("set streaming_parallelism_strategy = 'RATIO(0.5)'")
        .await?;
    session.run("create table t_fallback_base(v int)").await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 't_fallback_base' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    session
        .run("set streaming_parallelism_strategy_for_materialized_view = 'DEFAULT'")
        .await?;
    session
        .run("create materialized view m_fallback as select * from t_fallback_base")
        .await?;
    session.run("select distinct parallelism from rw_fragment_parallelism where name = 'm_fallback' and distribution_type = 'HASH';").await?.assert_result_eq("3");

    Ok(())
}
