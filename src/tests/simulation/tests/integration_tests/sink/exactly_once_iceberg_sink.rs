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

pub const CREATE_SOURCE: &str = "
    CREATE SOURCE s1 (v INT) WITH (
    connector = 'datagen',
    fields.v.kind = 'sequence',
    fields.v.start = '1',
    fields.v.end = '5',
    datagen.rows.per.second='10',
    datagen.split.num = '1'
) FORMAT PLAIN ENCODE JSON;
";
pub const DROP_SINK: &str = "drop sink test_sink";
pub const DROP_SOURCE: &str = "drop source iceberg_sink";
pub const CREATE_ICEBERG_SINK_WITHOUT_RECOVERY: &str = "
    CREATE SINK iceberg_sink AS select * from mv1 WITH (
    connector = 'iceberg',
    type = 'append-only',
    database.name = 'demo_db',
    table.name = 'test_sink_without_recovery',
    catalog.name = 'demo',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'custom',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 1,
    primary_key = 'i1,i2',
    force_append_only='true',
    is_exactly_once = 'true',
);
";
pub const CREATE_ICEBERG_SINK_WITH_RECOVERY: &str = "
    CREATE SINK iceberg_sink AS select * from mv1 WITH (
    connector = 'iceberg',
    type = 'append-only',
    database.name = 'demo_db',
    table.name = 'test_sink_with_recovery',
    catalog.name = 'demo',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'custom',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 1,
    primary_key = 'i1,i2',
    force_append_only='true',
    is_exactly_once = 'true',
);
";

#[tokio::test]
async fn test_exactly_once_iceberg_sink() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new();
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
    assert!(test_sink.store.inner().checkpoint_count > 0);

    Ok(())
}

#[tokio::test]
async fn test_sink_error_event_logs() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestSink::register_new();
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
