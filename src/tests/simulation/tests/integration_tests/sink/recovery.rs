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

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, KillOpts};
use tokio::time::sleep;

use crate::sink::utils::{
    start_sink_test_cluster, SimulationTestSink, SimulationTestSource, CREATE_SINK, CREATE_SOURCE,
    DROP_SINK, DROP_SOURCE,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

async fn kill_and_check(
    cluster: &mut Cluster,
    test_sink: &SimulationTestSink,
    target: usize,
) -> anyhow::Result<()> {
    let mut prev_count = 0;
    sleep(Duration::from_secs(2)).await;
    for i in 0..5 {
        let curr_count = test_sink.store.id_count();
        if curr_count == target {
            assert!(i > 0, "test finish without kill");
            break;
        }
        assert!(
            curr_count > prev_count,
            "not make progress between kill. Prev count {}, curr count {}, i {}",
            prev_count,
            curr_count,
            i
        );
        prev_count = curr_count;
        cluster.kill_node(&KillOpts::ALL).await;
        sleep(Duration::from_secs(10)).await;
    }
    Ok(())
}

async fn recovery_test_inner(is_decouple: bool) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let test_sink = SimulationTestSink::register_new();
    let test_source = SimulationTestSource::register_new(6, 0..100000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    if is_decouple {
        session.run("set sink_decouple = true").await?;
    } else {
        session.run("set sink_decouple = false").await?;
    }
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    assert_eq!(6, test_sink.parallelism_counter.load(Relaxed));

    let count = test_source.id_list.len();

    kill_and_check(&mut cluster, &test_sink, count).await?;

    test_sink.store.wait_for_count(count).await?;

    let mut session = cluster.start_session();
    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    assert!(test_sink.store.inner().checkpoint_count > 0);

    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.inner().checkpoint_count > 0);

    Ok(())
}

#[tokio::test]
async fn test_sink_recovery() -> Result<()> {
    recovery_test_inner(false).await
}

#[tokio::test]
async fn test_sink_decouple_recovery() -> Result<()> {
    recovery_test_inner(true).await
}
