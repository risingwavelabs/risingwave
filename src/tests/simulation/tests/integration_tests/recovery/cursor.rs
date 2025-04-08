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
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time::sleep;

use crate::utils::kill_cn_and_meta_and_wait_recover;

const TABLE_CREATE: &str = "create table t1 (v1 int, v2 int);";
const TABLE_DROP: &str = "drop table t1;";
const SUBSCRIPTION_CREATE: &str = "create subscription sub from t1 with (retention = '1d');";
const SUBSCRIPTION_DROP: &str = "drop subscription sub;";

#[madsim::test]
async fn test_cursor_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run(TABLE_CREATE).await?;
    session.run(SUBSCRIPTION_CREATE).await?;
    sleep(Duration::from_secs(1)).await;

    session.run("INSERT INTO t1 VALUES (1, 1);").await?;
    session.run("INSERT INTO t1 VALUES (2, 2);").await?;

    session
        .run("DECLARE test_cursor SUBSCRIPTION CURSOR FOR sub SINCE begin();")
        .await?;

    let result1 = session.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 0);

    kill_cn_and_meta_and_wait_recover(&cluster).await;

    let mut session2 = cluster.start_session();

    session2
        .run("DECLARE test_cursor SUBSCRIPTION CURSOR FOR sub SINCE begin();")
        .await?;

    session2.run("INSERT INTO t1 VALUES (3, 3);").await?;
    session2.run("INSERT INTO t1 VALUES (4, 4);").await?;

    let result1 = session2.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session2.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session2.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session2.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 1);
    let result1 = session.run("FETCH NEXT FROM test_cursor;").await?;
    assert_eq!(result1.len(), 0);

    session.run(SUBSCRIPTION_DROP).await?;
    session.run(TABLE_DROP).await?;
    Ok(())
}
