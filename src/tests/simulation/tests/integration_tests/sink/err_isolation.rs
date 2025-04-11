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
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;

use crate::sink::utils::{
    CREATE_SINK, CREATE_SOURCE, DROP_SINK, DROP_SOURCE, SimulationTestSink, SimulationTestSource,
    start_sink_test_cluster,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

#[tokio::test]
async fn test_sink_decouple_err_isolation() -> Result<()> {
    test_sink_decouple_err_isolation_inner(false).await
}

#[tokio::test]
async fn test_coordinated_sink_decouple_err_isolation() -> Result<()> {
    test_sink_decouple_err_isolation_inner(true).await
}

async fn test_sink_decouple_err_isolation_inner(is_coordinated_sink: bool) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new(is_coordinated_sink);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    test_sink.wait_initial_parallelism(6).await?;

    test_sink.set_err_rate(0.002);

    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    // Due to sink failure isolation, source stream should not be recreated
    assert_eq!(
        source_parallelism,
        test_source.create_stream_count.load(Relaxed)
    );

    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.checkpoint_count() > 0);

    Ok(())
}

#[tokio::test]
async fn test_sink_error_event_logs() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new(false);
    test_sink.set_err_rate(1.0);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    test_sink.wait_initial_parallelism(6).await?;

    test_sink.store.wait_for_err(1).await?;

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    // Due to sink failure isolation, source stream should not be recreated
    assert_eq!(
        source_parallelism,
        test_source.create_stream_count.load(Relaxed)
    );

    // Sink error should be recorded in rw_event_logs
    let result = session
        .run("select * from rw_event_logs where event_type = 'SINK_FAIL'")
        .await?;
    assert!(!result.is_empty());
    println!("Sink fail event logs: {:?}", result);

    Ok(())
}
