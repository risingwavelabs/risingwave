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

use std::thread::sleep;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration};

const SET_PARALLELISM: &str = "SET STREAMING_PARALLELISM=1;";
const ROOT_TABLE_CREATE: &str = "create table t1 (_id int, data jsonb);";
const INSERT_SEED_SQL: &str =
    r#"insert into t1 values (1, '{"orders": {"id": 1, "price": "2.30", "customer_id": 2}}');"#;
const INSERT_RECURSE_SQL: &str = "insert into t1 select _id + 1, data from t1;";
const MV1: &str = r#"
create materialized view mv1 as
with p1 as (
	select
		_id as id,
		(data ->> 'orders')::jsonb as orders
	from t1
),
p2 as (
	select
	 id,
	 orders ->> 'id' as order_id,
	 orders ->> 'price' as order_price,
	 orders ->> 'customer_id' as order_customer_id
	from p1
)
select
    id,
    order_id,
    order_price,
    order_customer_id
from p2;
"#;

#[tokio::test]
async fn test_backfill_with_upstream_and_snapshot_read() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_backfill()).await?;
    let mut session = cluster.start_session();

    session.run(SET_PARALLELISM).await?;
    session.run(ROOT_TABLE_CREATE).await?;

    session.run(INSERT_SEED_SQL).await?;
    session.run("flush").await?;

    // Create snapshot
    for _ in 0..18 {
        session.run(INSERT_RECURSE_SQL).await?;
        session.run("flush").await?;
    }

    let mut tasks = vec![];

    // Create sessions for running updates concurrently.
    let sessions = (0..3).map(|_| cluster.start_session()).collect_vec();

    // Create lots of base table update
    for mut session in sessions {
        let task = tokio::spawn(async move {
            session.run(INSERT_RECURSE_SQL).await?;
            anyhow::Ok(())
        });
        tasks.push(task);
    }

    // Create sessions for running updates concurrently.
    let sessions = (0..10).map(|_| cluster.start_session()).collect_vec();

    // Create lots of base table update
    for mut session in sessions {
        let task = tokio::spawn(async move {
            for _ in 0..10 {
                session.run("FLUSH;").await?;
            }
            anyhow::Ok(())
        });
        tasks.push(task);
    }

    // ... Concurrently run create mv async
    let mv1_task = tokio::spawn(async move {
        session.run(SET_PARALLELISM).await?;
        session.run(MV1).await
    });

    mv1_task.await??;
    for task in tasks {
        task.await??;
    }
    Ok(())
}

/// The replication scenario is tested:
/// 1. Upstream yields some chunk downstream. The chunk values are in the range 301-400.
/// 2. Backfill snapshot read is until 300.
/// 3. Receive barrier, 301-400 must be replicated, if not these records are discarded.
/// 4. Next epoch is not a checkpoint epoch.
/// 5. Next Snapshot Read occurs, checkpointed data is from 400-500.
///
/// In order to reproduce this scenario, we rely on a few things:
/// 1. Large checkpoint period, so checkpoint takes a long time to occur.
/// 2. We insert 2 partitions of records for the snapshot,
///    one at the lower bound, one at the upper bound.
/// 3. We insert a chunk of records in between.
#[tokio::test]
async fn test_arrangement_backfill_replication() -> Result<()> {
    // Initialize cluster with config which has larger checkpoint interval,
    // so it will rely on replication.
    let mut cluster = Cluster::start(Configuration::for_arrangement_backfill()).await?;
    let mut session = cluster.start_session();

    // Create a table with parallelism = 1;
    session.run("SET STREAMING_PARALLELISM=1;").await?;
    session.run("CREATE TABLE t (v1 int primary key)").await?;
    // TODO(kwannoel): test the parallelism of the table.

    // Ingest snapshot data
    session
        .run("INSERT INTO t select * from generate_series(1, 100)")
        .await?;
    session.run("FLUSH;").await?;
    session
        .run("INSERT INTO t select * from generate_series(201, 300)")
        .await?;
    session.run("FLUSH;").await?;

    // Start update data thread
    let mut session2 = cluster.start_session();
    let upstream_task = tokio::spawn(async move {
        // The initial 100 records will take approx 3s
        // After that we start ingesting upstream records.
        sleep(Duration::from_secs(3));
        for i in 101..=200 {
            session2
                .run(format!("insert into t values ({})", i))
                .await
                .unwrap();
        }
        session2.run("FLUSH;").await.unwrap();
    });

    // Create a materialized view with parallelism = 3;
    session.run("SET STREAMING_PARALLELISM=3").await?;
    session
        .run("SET STREAMING_ENABLE_ARRANGEMENT_BACKFILL=true")
        .await?;
    session.run("SET STREAMING_RATE_LIMIT=30").await?;
    session
        .run("create materialized view m1 as select * from t")
        .await?;

    upstream_task.await?;

    // Verify its parallelism
    // TODO(kwannoel): test the parallelism of the table.

    // Verify all data has been ingested, with no extra data in m1.
    let result = session
        .run("select t.v1 from t where t.v1 not in (select v1 from m1)")
        .await?;
    assert_eq!(result, "");
    let result = session.run("select count(*) from m1").await?;
    assert_eq!(result.parse::<usize>().unwrap(), 300);
    Ok(())
}

// TODO(kwannoel): Test arrangement backfill background recovery.
