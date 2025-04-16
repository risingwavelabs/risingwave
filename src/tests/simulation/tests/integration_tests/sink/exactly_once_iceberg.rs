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
use risingwave_connector::sink::catalog::SinkId;
use risingwave_simulation::cluster::{Cluster, KillOpts};
use tokio::time::sleep;

use crate::sink::exactly_once_utils::SimulationTestIcebergExactlyOnceSink;
use crate::sink::utils::{
    CREATE_SINK, CREATE_SOURCE, DROP_SINK, DROP_SOURCE, SimulationTestSource,
    start_sink_test_cluster,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

#[tokio::test]
async fn test_exactly_once_sink_basic() -> Result<()> {
    let err_rate_list = vec![0.1, 0.1, 0.1, 0.1, 0.1];
    test_exactly_once_sink_inner(err_rate_list).await
}

#[tokio::test]
async fn test_exactly_once_sink_corner_case() {
    // If, during a re-commit, a new snapshot_id is generated instead of using the previously persisted snapshot_id, it can lead to duplicate data when consecutive "23" appears in err_events.
    let err_rate_list = vec![0.0, 0.0, 0.3, 0.3, 0.0];
    let _ = test_exactly_once_sink_inner(err_rate_list).await;
}

async fn test_exactly_once_sink_inner(err_rate_list: Vec<f64>) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestIcebergExactlyOnceSink::register_new_with_err_rate(err_rate_list);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;

    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;

    let count = test_source.id_list.len();

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
        .wait_for_count_and_check_duplicate(test_source.id_list.len())
        .await?;

    let result: String = session.run("select * from rw_sink_decouple").await?;
    let [_, is_sink_decouple_str, vnode_count_str] =
        TryInto::<[&str; 3]>::try_into(result.split(" ").collect_vec()).unwrap();
    assert_eq!(is_sink_decouple_str, "t");

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(
        source_parallelism,
        test_source.create_stream_count.load(Relaxed)
    );
    println!(
        "total id nums in external sink is {:?}, test_source.id_list len is {:?}",
        test_sink.store.count_total_keys(),
        test_source.id_list.len()
    );
    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.checkpoint_count() > 0);

    Ok(())
}
