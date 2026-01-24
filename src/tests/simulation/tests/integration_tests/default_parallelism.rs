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

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;

#[tokio::test]
async fn test_no_default_parallelism() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::default()).await?;

    let mut session = cluster.start_session();

    session.run("create table t(v int);").await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't';")
        .await?
        .assert_result_eq("ADAPTIVE");

    Ok(())
}

#[tokio::test]
async fn test_default_parallelism() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_default_parallelism(2)).await?;

    let mut session = cluster.start_session();

    session.run("create table t(v int);").await?;
    session
        .run("select parallelism from rw_streaming_parallelism where name = 't';")
        .await?
        .assert_result_eq("FIXED(2)");

    session
        .run("alter table t set parallelism = adaptive;")
        .await?;

    session
        .run("select parallelism from rw_streaming_parallelism where name = 't';")
        .await?
        .assert_result_eq("ADAPTIVE");

    Ok(())
}
