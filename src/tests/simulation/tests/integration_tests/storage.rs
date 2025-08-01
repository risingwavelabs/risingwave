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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng, rng as thread_rng};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;

const NUM_ROWS: usize = 500;
const NUM_OVERWRITES: usize = 5000;
const MAX_STRING_LEN: usize = 150;

#[tokio::test]
async fn test_storage_with_random_writes() -> Result<()> {
    // TODO: Use backfill configuration for now
    let mut cluster = Cluster::start(Configuration::for_backfill()).await?;
    let mut session = cluster.start_session();

    session.run("SET STREAMING_PARALLELISM=3;").await?;
    session
        .run("create table t1 (id int, val varchar, primary key(id))")
        .await?;
    session
        .run("create materialized view mv1 as select id, val from t1")
        .await?;

    // Insert NUM_ROWS random generated rows
    for id in 0..NUM_ROWS {
        let mut rng = SmallRng::seed_from_u64(id as u64);
        let rand_len: usize = (rng.next_u32() as usize) % MAX_STRING_LEN;
        let rand_str = Alphanumeric.sample_string(&mut rng, rand_len);
        session
            .run(&format!("insert into t1 values ({}, '{}')", id, rand_str))
            .await?;
    }

    let finished = Arc::new(AtomicBool::new(false));

    // Run a concurrent session to check the consistency of the MV and table
    let finished_clone = finished.clone();
    let reader_task = tokio::spawn(async move {
        let mut session: risingwave_simulation::cluster::Session = cluster.start_session();
        while !finished_clone.load(Ordering::Acquire) {
            session
                .run("select * from t1 full outer join mv1 where t1.id = mv1.id and t1.val is distinct from mv1.val")
                .await?
                .assert_result_eq("");
        }
        anyhow::Ok(())
    });

    // Overwrite random rows
    let mut rng = thread_rng();
    for _ in 0..NUM_OVERWRITES {
        let id = (rng.next_u32() as usize) % NUM_ROWS;
        let rand_len: usize = (rng.next_u32() as usize) % MAX_STRING_LEN;
        let rand_str = Alphanumeric.sample_string(&mut rng, rand_len);
        session
            .run(&format!("insert into t1 values ({}, '{}')", id, rand_str))
            .await?;
    }

    finished.store(true, Ordering::Release);
    reader_task.await??;

    Ok(())
}
