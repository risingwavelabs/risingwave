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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use futures::{StreamExt, stream};
use risingwave_common::array::StreamChunk;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::test_source::{BoxSource, TestSourceSplit, register_test_source};
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time::{sleep, timeout};

use crate::utils::{kill_cn_and_meta_and_wait_recover, wait_jobs_running};

async fn wait_for_reader_builds(
    reader_splits: &Arc<Mutex<Vec<Vec<TestSourceSplit>>>>,
    expected: usize,
) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if reader_splits.lock().unwrap().len() >= expected {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timed out waiting for {expected} source reader builds; observed {:?}",
            reader_splits.lock().unwrap()
        )
    })
}

#[tokio::test]
async fn test_snapshot_backfill_recovers_embedded_source_splits() -> Result<()> {
    let reader_splits = Arc::new(Mutex::new(Vec::new()));
    let reader_splits_ref = reader_splits.clone();
    let _source_guard = register_test_source(BoxSource::new(
        |_, _| {
            Ok(vec![TestSourceSplit {
                id: "split-0".into(),
                properties: Default::default(),
                offset: String::new(),
            }])
        },
        move |_, splits, _, _, _| {
            reader_splits_ref.lock().unwrap().push(splits);
            stream::pending::<ConnectorResult<StreamChunk>>().boxed()
        },
    ));

    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 1;").await?;
    session
        .run("set streaming_use_shared_source = false;")
        .await?;
    session.run("create table t (id int primary key);").await?;
    session.run("insert into t values (1);").await?;
    session.flush().await?;
    session
        .run("create materialized view upstream_mv as select * from t;")
        .await?;
    session
        .run(
            "create source test_source (id int) with (connector = 'test') \
             format plain encode json;",
        )
        .await?;

    session
        .run("set streaming_use_snapshot_backfill = true;")
        .await?;
    session.run("set background_ddl = true;").await?;
    // Keep the creating job in its independent partial graph throughout recovery.
    session.run("set backfill_rate_limit = 0;").await?;
    session
        .run(
            "create materialized view result_mv as \
             select upstream_mv.id \
             from upstream_mv join test_source \
             on upstream_mv.id = test_source.id;",
        )
        .await?;

    wait_jobs_running(&mut session).await?;
    wait_for_reader_builds(&reader_splits, 1).await?;
    let reader_builds_before_recovery = reader_splits.lock().unwrap().len();

    kill_cn_and_meta_and_wait_recover(&mut cluster).await;

    wait_for_reader_builds(&reader_splits, reader_builds_before_recovery + 1).await?;
    let reader_splits = reader_splits.lock().unwrap();
    let recovered_splits = &reader_splits[reader_builds_before_recovery];
    assert_eq!(recovered_splits.len(), 1);
    assert_eq!(recovered_splits[0].id.as_ref(), "split-0");
    assert_eq!(recovered_splits[0].offset, "");

    Ok(())
}
