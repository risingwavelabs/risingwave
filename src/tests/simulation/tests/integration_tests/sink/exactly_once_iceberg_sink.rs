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

pub const CREATE_TABLE: &str = "
    CREATE TABLE datagen_table (i1 int, i2 int) WITH (
    connector = 'datagen',
    fields.i1.kind = 'sequence',
    fields.i1.start = '1',
    fields.i1.end  = '1000',
    fields.i2.kind = 'sequence',
    fields.i2.start = '1',
    fields.i2.end  = '1000',
    datagen.rows.per.second='10',
    datagen.split.num = '1'
) FORMAT PLAIN ENCODE JSON;
";

pub const CREATE_MV: &str = "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM datagen_table;";
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
);";

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

pub const CREATE_ICEBERG_SOURCE_WITHOUT_RECOVERY: &str = "
   CREATE SOURCE iceberg_source
WITH (
    connector = 'iceberg',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    database.name = 'demo_db',
    table.name = 'test_sink_without_recovery',
);";

pub const CREATE_ICEBERG_SOURCE_WITH_RECOVERY: &str = "
   CREATE SOURCE iceberg_source
    WITH (
        connector = 'iceberg',
        s3.endpoint = 'http://127.0.0.1:9301',
        s3.region = 'us-east-1',
        s3.access.key = 'hummockadmin',
        s3.secret.key = 'hummockadmin',
        s3.path.style.access = 'true',
        catalog.type = 'storage',
        warehouse.path = 's3a://hummock001/iceberg-data',
        database.name = 'demo_db',
        table.name = 'test_sink_with_recovery',
);";

#[tokio::test]
async fn test_exactly_once_iceberg_sink() -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink_with_error = SimulationTestSink::register_new();
    let test_sink_without_error = SimulationTestSink::register_new();
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 0);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = false").await?;
    session.run(CREATE_TABLE).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM datagen_table;")
        .await?;
    session.run(CREATE_ICEBERG_SINK_WITH_RECOVERY).await?;
    test_sink.wait_initial_parallelism(6).await?;

    test_sink.set_err_rate(0.02);

    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;
    session.run(CREATE_ICEBERG_SOURCE_WITH_RECOVERY).await?;
    let total_count = session.run("select count(*) from iceberg_source").await?;
    assert_eq!(total_count, 1000);

    Ok(())
}
