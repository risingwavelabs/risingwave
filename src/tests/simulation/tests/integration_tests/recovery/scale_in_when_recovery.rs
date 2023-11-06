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

use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn test_scale_in_when_recovery() -> Result<()> {
    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        // no ANSI color codes when output to file
        .with_ansi(console::colors_enabled_stderr() && console::colors_enabled())
        .with_writer(std::io::stderr)
        .try_init();

    let config = Configuration::for_auto_scale();
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int);").await?;
    session
        .run("INSERT INTO t1 select * from generate_series(1, 100)")
        .await?;
    session.run("flush").await?;

    sleep(Duration::from_secs(5)).await;

    let fragment = cluster
        .locate_one_fragment(vec![identity_contains("materialize")])
        .await?;

    let (all_parallel_units, used_parallel_units) = fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    let initialized_parallelism = used_parallel_units.len();

    assert_eq!(
        initialized_parallelism,
        config.compute_nodes * config.compute_node_cores
    );

    // ensure the restart delay is longer than config in `risingwave-auto-scale.toml`
    let restart_delay = 30;

    cluster
        .kill_nodes_and_restart(vec!["compute-1"], restart_delay)
        .await;

    let fragment = cluster
        .locate_one_fragment(vec![identity_contains("materialize")])
        .await?;

    let (_, used_parallel_units) = fragment.parallel_unit_usage();

    assert_eq!(
        initialized_parallelism - config.compute_node_cores,
        used_parallel_units.len()
    );

    Ok(())
}
