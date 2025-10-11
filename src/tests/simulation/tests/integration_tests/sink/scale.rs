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
use itertools::Itertools;
use rand::{Rng, rng as thread_rng};
use risingwave_common::hash::WorkerSlotId;
use risingwave_simulation::cluster::{Cluster, KillOpts};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use tokio::time::sleep;

use crate::sink::utils::*;
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

async fn scale_and_check(
    cluster: &mut Cluster,
    test_sink: &SimulationTestSink,
    target_count: usize,
    schedule_plan: impl Iterator<Item = (String, usize)>,
) -> Result<()> {
    for (plan, expected_parallelism) in schedule_plan {
        let prev_count = test_sink.store.id_count();
        assert!(prev_count <= target_count);
        if prev_count == target_count {
            return Ok(());
        }
        cluster.run(plan).await?;
        let after_count = test_sink.store.id_count();
        sleep(Duration::from_secs(10)).await;
        if thread_rng().random_bool(0.5) {
            sleep(Duration::from_secs(10)).await;
            let before_kill_count = test_sink.store.id_count();
            cluster.kill_node(&KillOpts::ALL).await;
            sleep(Duration::from_secs(10)).await;
        }
    }
    Ok(())
}

async fn scale_test_inner(is_decouple: bool, test_type: TestSinkType) -> Result<()> {
    init_logger();
    let mut cluster = start_sink_test_cluster().await?;

    // todo, make it configurable
    let total_cores = 6;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new(test_type);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    if is_decouple {
        session.run("set sink_decouple = true").await?;
    } else {
        session.run("set sink_decouple = false").await?;
    }
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    test_sink.wait_initial_parallelism(6).await?;

    let mut sink_fragments = cluster
        .locate_fragments([identity_contains("Sink")])
        .await?;

    assert_eq!(sink_fragments.len(), 1);
    let fragment = sink_fragments.pop().unwrap();
    let id = fragment.id();

    let count = test_source.id_list.len();
    let workers = fragment.all_worker_count().into_keys().collect_vec();

    scale_and_check(
        &mut cluster,
        &test_sink,
        count,
        vec![
            (
                format!("alter sink test_sink set parallelism = {}", total_cores - 3),
                3,
            ),
            (
                format!("alter sink test_sink set parallelism = {}", total_cores - 4),
                2,
            ),
            (
                format!("alter sink test_sink set parallelism = {}", total_cores),
                6,
            ),
        ]
        .into_iter(),
    )
    .await?;

    test_sink.store.wait_for_count(count).await?;

    let mut session = cluster.start_session();
    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert!(source_parallelism <= test_source.create_stream_count.load(Relaxed));

    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    assert!(test_sink.store.checkpoint_count() > 0);

    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.checkpoint_count() > 0);

    Ok(())
}

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

macro_rules! define_tests {
    ($($test_type:ident,)+) => {
        $(
            paste::paste! {
                #[tokio::test]
                async fn [<test_ $test_type:snake _scale>]() -> Result<()> {
                    scale_test_inner(false, TestSinkType::$test_type).await
                }

                #[tokio::test]
                async fn [<test_ $test_type:snake _decouple_scale>]() -> Result<()> {
                    scale_test_inner(true, TestSinkType::$test_type).await
                }
            }
        )+
    };
    () => {
        $crate::for_all_sink_types! {
            define_tests
        }
    }
}

define_tests!();
