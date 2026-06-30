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

use anyhow::Result;
use itertools::Itertools;

use crate::sink::utils::*;
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

async fn basic_test_inner(is_decouple: bool, test_type: TestSinkType) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

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

    let internal_tables = session.run("show internal tables").await?;

    let table_name_prefix = "public.__internal_test_sink_";

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

macro_rules! define_tests {
    ($($test_type:ident,)+) => {
        $(
            paste::paste! {
                #[tokio::test]
                async fn [<test_ $test_type:snake _basic>]() -> Result<()> {
                    basic_test_inner(false, TestSinkType::$test_type).await
                }

                #[tokio::test]
                async fn [<test_ $test_type:snake _decouple_basic>]() -> Result<()> {
                    basic_test_inner(true, TestSinkType::$test_type).await
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

#[tokio::test]
async fn test_sink_log_store_internal_table_point_get_after_flush() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 2;
    let test_sink = SimulationTestSink::register_new(TestSinkType::RetainLog);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..64, 1.0, 64);

    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 2").await?;
    session.run("set sink_decouple = true").await?;
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    test_sink.wait_initial_parallelism(2).await?;
    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;
    session.flush().await?;

    let log_store_table_id = session
        .run("select internal_table_id from rw_sink_log_store_tables")
        .await?
        .trim()
        .parse::<u32>()?;

    let point_get_key = session
        .run(format!(
            "select kv_log_store_epoch, kv_log_store_seq_id, kv_log_store_vnode \
             from rw_table({log_store_table_id}) \
             where kv_log_store_seq_id is not null \
             limit 1"
        ))
        .await?;
    let [epoch, seq_id, vnode] =
        TryInto::<[&str; 3]>::try_into(point_get_key.split_whitespace().collect_vec()).unwrap();

    let point_get_count = session
        .run(format!(
            "select count(*) from rw_table({log_store_table_id}) \
             where kv_log_store_epoch = {epoch} \
               and kv_log_store_seq_id = {seq_id} \
               and kv_log_store_vnode = {vnode}"
        ))
        .await?
        .trim()
        .parse::<usize>()?;
    assert_eq!(1, point_get_count);

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    Ok(())
}

#[tokio::test]
async fn test_sink_since_timestamp_starts_from_changelog() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;
    let test_sink = SimulationTestSink::register_new(TestSinkType::SinkWriter);

    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 1").await?;
    session.run("set sink_decouple = false").await?;
    session
        .run("create table t_since_sink (id int primary key, name varchar)")
        .await?;
    session
        .run("create subscription sub_since_sink from t_since_sink with(retention = '1D')")
        .await?;
    session.flush().await?;

    session
        .run("insert into t_since_sink values (1, 'name-1')")
        .await?;
    session.flush().await?;
    let since_timestamp = session.run("select now()::varchar").await?;

    session
        .run("insert into t_since_sink values (2, 'name-2')")
        .await?;
    session.flush().await?;

    session
        .run(format!(
            "create sink test_sink from t_since_sink with (\
             connector = 'test', \
             type = 'upsert', \
             snapshot = 'false', \
             since_timestamp = '{}')",
            since_timestamp.trim()
        ))
        .await?;
    test_sink.wait_initial_parallelism(1).await?;
    session.flush().await?;
    test_sink.store.wait_for_count(1).await?;
    test_sink.store.check_result_rows(&[(2, "name-2")])?;

    session.run("drop sink test_sink").await?;
    drop(test_sink);

    let test_sink_as_select = SimulationTestSink::register_new(TestSinkType::SinkWriter);
    session
        .run(format!(
            "create sink test_sink as \
             select id, name from t_since_sink \
             with (\
             connector = 'test', \
             type = 'upsert', \
             snapshot = 'false', \
             since_timestamp = '{}')",
            since_timestamp.trim()
        ))
        .await?;
    test_sink_as_select.wait_initial_parallelism(1).await?;
    session.flush().await?;
    test_sink_as_select.store.wait_for_count(1).await?;
    test_sink_as_select
        .store
        .check_result_rows(&[(2, "name-2")])?;

    session.run("drop sink test_sink").await?;
    session.run("drop subscription sub_since_sink").await?;
    session.run("drop table t_since_sink").await?;

    Ok(())
}
