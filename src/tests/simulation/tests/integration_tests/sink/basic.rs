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
use tokio::time::sleep;

use crate::sink::utils::{
    CREATE_SINK, CREATE_SOURCE, DROP_SINK, DROP_SOURCE, SimulationTestSink, SimulationTestSource,
    start_sink_test_cluster,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

async fn basic_test_inner(is_decouple: bool, is_coordinated_sink: bool) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new(is_coordinated_sink);
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

    let internal_tables = session.run("show internal tables").await?;

    let table_name_prefix = "__internal_test_sink_";

    let sink_internal_table_name: String = TryInto::<[&str; 1]>::try_into(
        internal_tables
            .split("\n")
            .filter(|line| {
                line.contains(table_name_prefix)
                    && line
                        .strip_prefix(table_name_prefix)
                        .unwrap()
                        .contains("sink")
            })
            .collect_vec(),
    )
    .unwrap()[0]
        .to_string();

    let _result = session
        .run(format!(
            "select * from {} limit 10",
            sink_internal_table_name
        ))
        .await?;

    let _result = session
        .run(format!(
            "select * from {} where kv_log_store_vnode = 0 limit 10",
            sink_internal_table_name
        ))
        .await?;

    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;

    let result: String = session.run("select * from rw_sink_decouple").await?;
    let [_, is_sink_decouple_str, vnode_count_str] =
        TryInto::<[&str; 3]>::try_into(result.split(" ").collect_vec()).unwrap();
    if is_decouple {
        assert_eq!(is_sink_decouple_str, "t");
        assert_eq!(vnode_count_str, "256");
    } else {
        assert_eq!(is_sink_decouple_str, "f");
    }

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

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
async fn test_sink_basic() -> Result<()> {
    basic_test_inner(false, false).await
}

#[tokio::test]
async fn test_sink_decouple_basic() -> Result<()> {
    basic_test_inner(true, false).await
}

#[tokio::test]
async fn test_coordinated_sink_basic() -> Result<()> {
    basic_test_inner(false, true).await
}

#[tokio::test]
async fn test_coordinated_sink_decouple_basic() -> Result<()> {
    basic_test_inner(true, true).await
}

#[tokio::test]
async fn test_sink_decouple_blackhole() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session.run(CREATE_SOURCE).await?;
    session
        .run("create sink test_sink from test_source with (connector = 'blackhole')")
        .await?;

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(
        source_parallelism,
        test_source.create_stream_count.load(Relaxed)
    );

    Ok(())
}
