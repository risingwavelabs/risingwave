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
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, Session};
use tempfile::NamedTempFile;
use tokio::time::sleep;

use crate::assert_eq_with_err_returned as assert_eq;
use crate::sink::utils::*;

async fn start_error_table_test_cluster() -> Result<Cluster> {
    let config_path = {
        let mut file = NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.write_all(b"\n[streaming.developer]\nmemory_controller_update_interval_ms = 600000\n")
            .expect("failed to write test config override");
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

async fn find_error_table_names(session: &mut Session, sink_name: &str) -> Result<Vec<String>> {
    let table_name_prefix = format!("public.__internal_{}_", sink_name);
    let internal_tables = session.run("show internal tables").await?;
    let table_names = internal_tables
        .lines()
        .filter(|line| {
            line.contains(&table_name_prefix) && line.to_ascii_lowercase().contains("sinkerror")
        })
        .map(str::to_owned)
        .sorted()
        .collect_vec();
    if table_names.is_empty() {
        return Err(anyhow::anyhow!(
            "failed to find sink error internal table for {sink_name}"
        ));
    }
    Ok(table_names)
}

async fn find_log_store_table_name(session: &mut Session, sink_name: &str) -> Result<String> {
    let table_name_prefix = format!("public.__internal_{}_", sink_name);
    let internal_tables = session.run("show internal tables").await?;
    internal_tables
        .lines()
        .filter(|line| {
            line.contains(&table_name_prefix)
                && !line.to_ascii_lowercase().contains("sinkerror")
                && line
                    .strip_prefix(&table_name_prefix)
                    .is_some_and(|suffix| suffix.contains("sink"))
        })
        .map(str::to_owned)
        .max()
        .ok_or_else(|| {
            anyhow::anyhow!("failed to find sink log store internal table for {sink_name}")
        })
}

async fn find_error_table_name(
    session: &mut Session,
    sink_name: &str,
    required_columns: &[&str],
) -> Result<String> {
    let mut last_describes = Vec::new();
    for attempt in 0..100 {
        let table_names = find_error_table_names(session, sink_name).await?;
        last_describes.clear();
        for table_name in table_names.iter().rev() {
            let describe = session.run(format!("describe {}", table_name)).await?;
            last_describes.push(format!("{table_name}:\n{describe}"));
            if required_columns.iter().all(|column| {
                describe
                    .lines()
                    .any(|line| line.split_whitespace().next() == Some(*column))
            }) {
                return Ok(table_name.clone());
            }
        }
        if attempt % 5 == 4 {
            session.run("flush").await?;
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow::anyhow!(
        "failed to find sink error internal table for {} with required columns {:?}. candidates:\n{}",
        sink_name,
        required_columns,
        last_describes.join("\n---\n")
    ))
}

async fn wait_for_error_table_count(
    session: &mut Session,
    error_table_name: &str,
    expected_count: usize,
) -> Result<()> {
    let count_sql = format!("select count(*) from {}", error_table_name);
    let ids_sql = format!("select id from {} order by id", error_table_name);
    let mut last_count = String::new();
    for attempt in 0..100 {
        let count_result = session.run(&count_sql).await?;
        last_count = count_result.trim().to_owned();
        if count_result.trim() == expected_count.to_string() {
            return Ok(());
        }
        if attempt % 5 == 4 {
            session.run("flush").await?;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let persisted_ids = session.run(&ids_sql).await?;
    Err(anyhow::anyhow!(
        "timed out waiting for error table count {} in {} (last count = {}, ids = [{}])",
        expected_count,
        error_table_name,
        last_count,
        persisted_ids.trim()
    ))
}

async fn query_error_table_rows_with_age(
    session: &mut Session,
    error_table_name: &str,
) -> Result<String> {
    let sql = format!(
        "select sink_error_row_op, id, name, \
                coalesce(age::varchar, 'NULL'), \
                sink_error_extra_info->>'column_count' \
         from {} order by id",
        error_table_name
    );
    let mut last_error = None;
    for attempt in 0..100 {
        match session.run(&sql).await {
            Ok(rows) => return Ok(rows),
            Err(error) => {
                last_error = Some(error);
                if attempt % 5 == 4 {
                    session.run("flush").await?;
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_error.expect("should record query error"))
}

fn format_insert_rows(rows: impl IntoIterator<Item = (i32, String, Option<i32>)>) -> String {
    rows.into_iter()
        .map(|(id, name, age)| match age {
            Some(age) => format!("({}, '{}', {})", id, name, age),
            None => format!("({}, '{}')", id, name),
        })
        .join(", ")
}

async fn run_sink_error_table_test(decouple: bool) -> Result<()> {
    let mut cluster = start_error_table_test_cluster().await?;
    let test_sink = SimulationTestSink::register_new(TestSinkType::ReportError);

    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 1").await?;
    session
        .run(format!("set sink_decouple = {}", decouple))
        .await?;
    session
        .run("create table test_table_error (id int primary key, name varchar)")
        .await?;
    session
        .run("create sink test_sink_error from test_table_error with (connector = 'test', type = 'upsert', skip_error = 'true', auto.schema.change = 'true')")
        .await?;

    let initial_ids = 0..5;
    session
        .run(format!(
            "insert into {} values {}",
            "test_table_error",
            format_insert_rows(
                initial_ids
                    .clone()
                    .map(|id| (id, simple_name_of_id(id), None))
            )
        ))
        .await?;
    test_sink.store.wait_for_count(initial_ids.len()).await?;

    session.run("flush").await?;
    let initial_error_table_name =
        find_error_table_name(&mut session, "test_sink_error", &["id", "name"]).await?;
    wait_for_error_table_count(&mut session, &initial_error_table_name, initial_ids.len()).await?;

    let initial_rows = session
        .run(format!(
            "select sink_error_row_op, id, name, \
                    sink_error_extra_info->>'id', \
                    sink_error_extra_info->>'name', \
                    sink_error_extra_info->>'column_count' \
             from {} order by id",
            initial_error_table_name
        ))
        .await?;
    let expected_initial_rows = initial_ids
        .clone()
        .map(|id| {
            let name = simple_name_of_id(id);
            format!("1 {} {} {} {} 2", id, name, id, name)
        })
        .join("\n");
    assert_eq!(initial_rows.trim(), expected_initial_rows);

    session
        .run("alter table test_table_error add column age int")
        .await?;
    let mut schema_session = cluster.start_session();
    let log_store_table_name =
        find_log_store_table_name(&mut schema_session, "test_sink_error").await?;
    let log_store_describe = schema_session
        .run(format!("describe {}", log_store_table_name))
        .await?;
    assert!(
        log_store_describe
            .lines()
            .any(|line| line.split_whitespace().next() == Some("test_table_error_age")),
        "log store table schema is not updated:\n{}",
        log_store_describe
    );
    let latest_error_table_name = find_error_table_name(
        &mut schema_session,
        "test_sink_error",
        &["id", "name", "age"],
    )
    .await?;

    let new_ids = 5..10;
    session
        .run(format!(
            "insert into {} values {}",
            "test_table_error",
            format_insert_rows(new_ids.clone().map(|id| {
                let age = id + 100;
                (id, simple_name_of_id(id), Some(age))
            }))
        ))
        .await?;
    test_sink.store.wait_for_count(new_ids.end as usize).await?;

    session.run("flush").await?;
    let expected_row_count = if latest_error_table_name == initial_error_table_name {
        new_ids.end as usize
    } else {
        new_ids.len()
    };
    wait_for_error_table_count(&mut session, &latest_error_table_name, expected_row_count).await?;

    let mut verify_session = cluster.start_session();
    let rows =
        query_error_table_rows_with_age(&mut verify_session, &latest_error_table_name).await?;
    let expected_rows = if latest_error_table_name == initial_error_table_name {
        initial_ids
            .clone()
            .map(|id| format!("1 {} {} NULL 2", id, simple_name_of_id(id)))
            .chain(
                new_ids
                    .clone()
                    .map(|id| format!("1 {} {} {} 3", id, simple_name_of_id(id), id + 100)),
            )
            .join("\n")
    } else {
        new_ids
            .clone()
            .map(|id| format!("1 {} {} {} 3", id, simple_name_of_id(id), id + 100))
            .join("\n")
    };
    assert_eq!(rows.trim(), expected_rows);

    session.run("drop sink test_sink_error").await?;
    session.run("drop table test_table_error").await?;
    assert_eq!(
        0,
        test_sink
            .parallelism_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    Ok(())
}

#[tokio::test]
async fn test_sink_error_table_nondecouple() -> Result<()> {
    run_sink_error_table_test(false).await
}

#[tokio::test]
async fn test_sink_error_table_decouple() -> Result<()> {
    run_sink_error_table_test(true).await
}
