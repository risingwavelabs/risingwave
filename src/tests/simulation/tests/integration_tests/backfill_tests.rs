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

use anyhow::Result;
use itertools::{any, Itertools};
use risingwave_simulation::cluster::{Cluster, Configuration};

const SET_PARALLELISM: &str = "SET STREAMING_PARALLELISM=1;";
const ROOT_TABLE_CREATE: &str = "create table t1 (_id int, data jsonb);";
const INSERT_SEED_SQL: &str =
    r#"insert into t1 values (1, '{"orders": {"id": 1, "price": "2.30", "customer_id": 2}}');"#;
const INSERT_AND_FLUSH_SQL: &str = r#"insert into t1 values (1, '{"orders": {"id": 1, "price": "2.30", "customer_id": 2}}'); FLUSH;"#;
const INSERT_RECURSE_SQL: &str = "insert into t1 select _id + 1, data from t1;";
const INSERT_RECURSE_AND_FLUSH_SQL: &str = "insert into t1 select _id + 1, data from t1;";
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

#[madsim::test]
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
    for mut session in sessions.into_iter() {
        let task = tokio::spawn(async move {
            session.run(INSERT_RECURSE_SQL).await?;
            anyhow::Ok(())
        });
        tasks.push(task);
    }

    // Create sessions for running updates concurrently.
    let sessions = (0..10).map(|_| cluster.start_session()).collect_vec();

    // Create lots of base table update
    for mut session in sessions.into_iter() {
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
