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

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;

use crate::sink::utils::{
    start_sink_test_cluster, SimulationTestSink, SimulationTestSource, CREATE_SINK, CREATE_SOURCE,
    DROP_SINK, DROP_SOURCE,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

#[tokio::test]
async fn test_sink_decouple_rate_limit() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new();
    // There will be 1000 * 0.2 = 200 rows generated
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..1000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 2").await?;
    session.run("set sink_decouple = true").await?;
    session.run("set sink_rate_limit = 0").await?;
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    test_sink.wait_initial_parallelism(2).await?;

    // sink should not write anything due to 0 rate limit
    sleep(Duration::from_secs(1)).await;
    assert_eq!(0, test_sink.store.id_count());

    // sink should write small number of rows with rate limit set to 1
    // the estimated sink throughput is 2 rows per second and we use 8 rows as the upper bound to check
    session
        .run("alter sink test_sink set sink_rate_limit to 1")
        .await?;
    sleep(Duration::from_secs(1)).await;
    assert!(test_sink.store.id_count() > 0);
    assert!(test_sink.store.id_count() <= 8);

    // sink should be paused
    session
        .run("alter sink test_sink set sink_rate_limit to 0")
        .await?;
    sleep(Duration::from_secs(1)).await;
    let id_count = test_sink.store.id_count();
    sleep(Duration::from_secs(1)).await;
    assert_eq!(id_count, test_sink.store.id_count());

    // sink should be running without rate limit
    session
        .run("alter sink test_sink set sink_rate_limit to default")
        .await?;
    sleep(Duration::from_secs(1)).await;
    assert!(test_sink.store.id_count() > id_count);

    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.inner().checkpoint_count > 0);

    Ok(())
}
