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

use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, Session};
use tokio::time::sleep;

use crate::sink::utils::*;

async fn start_schema_change_test_cluster() -> Result<Cluster> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 1,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        ..Default::default()
    })
    .await
}

async fn find_log_store_table_name(session: &mut Session, sink_name: &str) -> Result<String> {
    let table_name_prefix = format!("public.__internal_{}_", sink_name);
    let internal_tables = session.run("show internal tables").await?;
    internal_tables
        .lines()
        .find(|line| {
            line.contains(&table_name_prefix)
                && line
                    .strip_prefix(&table_name_prefix)
                    .map(|rest| rest.to_ascii_lowercase().contains("sink"))
                    .unwrap_or(false)
        })
        .map(str::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "failed to find log store internal table for {sink_name}. tables: {}",
                internal_tables
            )
        })
}

/// Test that value_indices is properly updated after auto schema change on the kv log store table.
///
/// Bug: On `main`, when ALTER TABLE adds a column, the kv log store internal table gets the new
/// column added to its `columns` list, but `value_indices` is NOT updated in the catalog.
/// This means the state table's serialization/deserialization is broken:
/// - Writes after schema change silently drop the new column's data
/// - Reads hit type mismatch panics because stale value_indices cause column misalignment
///
/// This test:
/// 1. Creates a table and a decoupled sink with auto.schema.change enabled
/// 2. Inserts initial data
/// 3. ALTER TABLE ADD COLUMN to trigger schema change
/// 4. Waits for schema change to propagate (new column visible in describe)
/// 5. Inserts data with the new column populated
/// 6. Flushes the new epoch so the retained log rows become queryable
/// 7. Queries the retained kv log store internal table directly
///
/// On main (buggy): the query fails with a type mismatch panic because value_indices is stale.
/// After fix: the query succeeds and returns the actual inserted age values.
#[tokio::test]
async fn test_schema_change_kv_log_store_value_indices() -> Result<()> {
    let mut cluster = start_schema_change_test_cluster().await?;
    let test_sink = SimulationTestSink::register_new(TestSinkType::RetainLog);

    let mut session = cluster.start_session();

    // Use decoupled mode to ensure kv log store is used
    session.run("set streaming_parallelism = 1").await?;
    session.run("set sink_decouple = true").await?;

    session
        .run("create table test_table (id int primary key, name varchar)")
        .await?;

    session
        .run(
            "create sink test_schema_change_sink from test_table \
             with (connector = 'test', type = 'upsert', auto.schema.change = 'true')",
        )
        .await?;

    // Insert initial data (2 columns: id, name)
    session
        .run("insert into test_table values (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .await?;
    test_sink.store.wait_for_count(3).await?;
    session.run("flush").await?;

    // Find the log store internal table
    let log_store_table =
        find_log_store_table_name(&mut session, "test_schema_change_sink").await?;

    // ALTER TABLE to add a new column — triggers auto-refresh schema change
    session
        .run("alter table test_table add column age int")
        .await?;

    // Wait for schema change to propagate to the log store table
    for i in 0..60 {
        match session.run(format!("describe {}", log_store_table)).await {
            Ok(desc) => {
                if desc.to_ascii_lowercase().contains("age") {
                    break;
                }
            }
            Err(_) => {}
        }
        if i == 59 {
            return Err(anyhow::anyhow!(
                "Timed out waiting for schema change to propagate to log store table"
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }

    // Re-find the log store table (could have been replaced during schema change)
    let log_store_table =
        find_log_store_table_name(&mut session, "test_schema_change_sink").await?;

    // Insert data WITH the new column populated.
    session
        .run("insert into test_table values (4, 'dave', 40), (5, 'eve', 50), (6, 'frank', 60)")
        .await?;
    test_sink.store.wait_for_count(6).await?;
    session.run("flush").await?;

    // Find the age column name in the log store table (prefixed with upstream table name)
    let describe_final = session.run(format!("describe {}", log_store_table)).await?;
    let age_column_name = describe_final
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("age"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find age column in log store table after schema change:\n{}",
                describe_final
            )
        })?;

    // Query the log store table for the new column.
    let query = format!(
        "select {age_col}, count(*) from {table} where {age_col} is not null group by {age_col} order by {age_col}",
        age_col = age_column_name,
        table = log_store_table,
    );
    let mut last_error = None;
    let mut last_rows = String::new();
    for attempt in 0..60 {
        match session.run(&query).await {
            Ok(rows) => {
                let non_null_count = rows.trim().lines().count();
                if non_null_count > 0 {
                    last_rows = rows;
                    last_error = None;
                    break;
                }
                last_rows = rows;
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
        if attempt % 5 == 4 {
            session.run("flush").await?;
        }
        sleep(Duration::from_secs(1)).await;
    }
    if let Some(e) = last_error {
        return Err(anyhow::anyhow!(
            "Query on log store internal table failed after schema change \
             (likely stale value_indices causing column misalignment): {}",
            e
        ));
    }
    let non_null_count = last_rows.trim().lines().count();
    if non_null_count == 0 {
        return Err(anyhow::anyhow!(
            "After schema change, the kv log store internal table has NULL for the age column. \
             value_indices was not updated during schema change."
        ));
    }

    session.run("drop sink test_schema_change_sink").await?;
    session.run("drop table test_table").await?;

    Ok(())
}
