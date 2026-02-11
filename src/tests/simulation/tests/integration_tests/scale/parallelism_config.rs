// Copyright 2026 RisingWave Labs
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

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;

#[tokio::test]
async fn test_streaming_parallelism_from_system_params() -> Result<()> {
    let config =
        Configuration::for_auto_parallelism_system_params(std::iter::empty::<(&str, &str)>());
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("ALTER SYSTEM SET streaming_parallelism_for_table TO 2;")
        .await?;
    session
        .run("ALTER SYSTEM SET streaming_parallelism_for_materialized_view TO 3;")
        .await?;
    session
        .run("ALTER SYSTEM SET streaming_parallelism_for_sink TO 1;")
        .await?;
    session
        .run("ALTER SYSTEM SET streaming_parallelism_for_index TO 4;")
        .await?;

    session.run("CREATE TABLE t1 (v1 int);").await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1;")
        .await?;
    session
        .run("CREATE SINK s1 FROM t1 WITH (connector = 'blackhole');")
        .await?;
    session.run("CREATE INDEX idx1 ON t1(v1);").await?;

    session
        .run("SELECT parallelism FROM rw_streaming_parallelism WHERE name = 't1' AND relation_type = 'table'")
        .await?
        .assert_result_eq("FIXED(2)");
    session
        .run("SELECT parallelism FROM rw_streaming_parallelism WHERE name = 'mv1' AND relation_type = 'materialized view'")
        .await?
        .assert_result_eq("FIXED(3)");
    session
        .run("SELECT parallelism FROM rw_streaming_parallelism WHERE name = 's1' AND relation_type = 'sink'")
        .await?
        .assert_result_eq("FIXED(1)");
    session
        .run("SELECT parallelism FROM rw_streaming_parallelism WHERE name = 'idx1' AND relation_type = 'index'")
        .await?
        .assert_result_eq("FIXED(4)");

    Ok(())
}

#[tokio::test]
async fn test_adaptive_parallelism_strategy_from_alter_system() -> Result<()> {
    let config =
        Configuration::for_auto_parallelism_system_params(std::iter::empty::<(&str, &str)>());
    let expected_parallelism = std::cmp::max(1, config.total_streaming_cores() / 2);
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run(
            "ALTER SYSTEM SET streaming_parallelism_strategy_for_materialized_view TO 'Ratio(0.5)';",
        )
        .await?;
    session.run("CREATE TABLE t_base (v1 int);").await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t_base;")
        .await?;

    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'mv1' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq(expected_parallelism.to_string());
    session
        .run(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'streaming_parallelism_strategy_for_materialized_view';",
        )
        .await?
        .assert_result_eq("RATIO(0.5)");

    Ok(())
}

#[tokio::test]
async fn test_parallelism_and_strategy_fallback_chain() -> Result<()> {
    let config =
        Configuration::for_auto_parallelism_system_params(std::iter::empty::<(&str, &str)>());
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("ALTER SYSTEM SET streaming_parallelism_for_table TO 3;")
        .await?;
    session
        .run("ALTER SYSTEM SET streaming_parallelism_strategy TO 'BOUNDED(2)';")
        .await?;
    session
        .run("ALTER SYSTEM SET streaming_parallelism_strategy_for_sink TO 'BOUNDED(5)';")
        .await?;

    session
        .run("SET streaming_parallelism_for_table = 4;")
        .await?;
    session
        .run("SET streaming_parallelism_strategy_for_sink = 'BOUNDED(4)';")
        .await?;
    session.run("CREATE TABLE t_session (v int);").await?;
    session
        .run("CREATE SINK sink_session FROM t_session WITH (connector = 'blackhole');")
        .await?;
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 't_session' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq("4");
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'sink_session' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq("4");

    let mut session = cluster.start_session();
    session.run("CREATE TABLE t_system (v int);").await?;
    session
        .run("CREATE MATERIALIZED VIEW mv_system AS SELECT * FROM t_system;")
        .await?;
    session
        .run("CREATE SINK sink_system FROM t_system WITH (connector = 'blackhole');")
        .await?;
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 't_system' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq("3");
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'mv_system' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq("2");
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'sink_system' and distribution_type = 'HASH';")
        .await?
        .assert_result_eq("5");

    Ok(())
}
