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

use anyhow::{anyhow, bail};
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
#[cfg(madsim)]
use rand_chacha::ChaChaRng;
use risingwave_sqlparser::ast::Statement;
use tokio::time::{Duration, sleep, timeout};
use tokio_postgres::error::Error as PgError;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::config::Configuration;
use crate::utils::read_file_contents;
use crate::validation::{is_permissible_error, is_recovery_in_progress_error};
use crate::{
    Table, generate_update_statements, insert_sql_gen, mview_sql_gen,
    parse_create_table_statements, parse_sql, session_sql_gen, sql_gen,
};

pub(super) type PgResult<A> = std::result::Result<A, PgError>;
pub(super) type Result<A> = anyhow::Result<A>;

pub(super) async fn update_base_tables<R: Rng>(
    client: &Client,
    rng: &mut R,
    base_tables: &[Table],
    inserts: &[Statement],
    config: &Configuration,
) {
    let update_statements = generate_update_statements(rng, base_tables, inserts, config).unwrap();
    for update_statement in update_statements {
        let sql = update_statement.to_string();
        tracing::info!("[EXECUTING UPDATES]: {}", &sql);
        client.simple_query(&sql).await.unwrap();
    }
}

pub(super) async fn populate_tables<R: Rng>(
    client: &Client,
    rng: &mut R,
    base_tables: Vec<Table>,
    row_count: usize,
    config: &Configuration,
) -> Vec<Statement> {
    let inserts = insert_sql_gen(rng, base_tables, row_count, config);
    for insert in &inserts {
        tracing::info!("[EXECUTING INSERT]: {}", insert);
        client.simple_query(insert).await.unwrap();
    }
    inserts
        .iter()
        .map(|s| parse_sql(s).into_iter().next().unwrap())
        .collect_vec()
}

pub(super) async fn set_variable(client: &Client, variable: &str, value: &str) -> String {
    let s = format!("SET {variable} TO {value}");
    tracing::info!("[EXECUTING SET_VAR]: {}", s);
    client.simple_query(&s).await.unwrap();
    s
}

/// Sanity checks for sqlsmith
pub(super) async fn test_sqlsmith<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    base_tables: Vec<Table>,
    row_count: usize,
    config: &Configuration,
) {
    // Test inserted rows should be at least 50% population count,
    // otherwise we don't have sufficient data in our system.
    // ENABLE: https://github.com/risingwavelabs/risingwave/issues/3844
    test_population_count(client, base_tables, row_count).await;
    tracing::info!("passed population count test");

    let threshold = 0.50; // permit at most 50% of queries to be skipped.
    let sample_size = 20;

    let skipped_percentage = test_batch_queries(client, rng, tables.clone(), sample_size, config)
        .await
        .unwrap();
    tracing::info!(
        "percentage of skipped batch queries = {}, threshold: {}",
        skipped_percentage,
        threshold
    );
    if skipped_percentage > threshold {
        panic!("skipped batch queries exceeded threshold.");
    }

    let skipped_percentage = test_stream_queries(client, rng, tables.clone(), sample_size, config)
        .await
        .unwrap();
    tracing::info!(
        "percentage of skipped stream queries = {}, threshold: {}",
        skipped_percentage,
        threshold
    );
    if skipped_percentage > threshold {
        panic!("skipped stream queries exceeded threshold.");
    }
}

pub(super) async fn test_session_variable<R: Rng>(client: &Client, rng: &mut R) -> String {
    let session_sql = session_sql_gen(rng);
    tracing::info!("[EXECUTING TEST SESSION_VAR]: {}", session_sql);
    client.simple_query(session_sql.as_str()).await.unwrap();
    session_sql
}

/// Expects at least 50% of inserted rows included.
pub(super) async fn test_population_count(
    client: &Client,
    base_tables: Vec<Table>,
    expected_count: usize,
) {
    let mut actual_count = 0;
    for t in base_tables {
        let q = format!("select * from {};", t.name);
        let rows = client.simple_query(&q).await.unwrap();
        actual_count += rows.len();
    }
    if actual_count < expected_count / 2 {
        panic!(
            "expected at least 50% rows included.\
             Total {} rows, only had {} rows",
            expected_count, actual_count,
        )
    }
}

/// Test batch queries, returns skipped query statistics
/// Runs in distributed mode, since queries can be complex and cause overflow in local execution
/// mode.
pub(super) async fn test_batch_queries<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    sample_size: usize,
    config: &Configuration,
) -> Result<f64> {
    let mut skipped = 0;
    for _ in 0..sample_size {
        test_session_variable(client, rng).await;
        let sql = sql_gen(rng, tables.clone(), config);
        tracing::info!("[TEST BATCH]: {}", sql);
        skipped += run_query(30, client, &sql).await?;
    }
    Ok(skipped as f64 / sample_size as f64)
}

/// Test stream queries, returns skipped query statistics
pub(super) async fn test_stream_queries<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    sample_size: usize,
    config: &Configuration,
) -> Result<f64> {
    let mut skipped = 0;

    for _ in 0..sample_size {
        test_session_variable(client, rng).await;
        let (sql, table) = mview_sql_gen(rng, tables.clone(), "stream_query", config);
        tracing::info!("[TEST STREAM]: {}", sql);
        skipped += run_query(12, client, &sql).await?;
        tracing::info!("[TEST DROP MVIEW]: {}", &format_drop_mview(&table));
        drop_mview_table(&table, client).await;
    }
    Ok(skipped as f64 / sample_size as f64)
}

pub(super) fn get_seed_table_sql(testdata: &str) -> String {
    let seed_files = ["tpch.sql", "nexmark.sql", "alltypes.sql"];
    seed_files
        .iter()
        .map(|filename| read_file_contents(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>()
}

/// Create the tables defined in testdata, along with some mviews.
/// TODO: Generate indexes and sinks.
pub(super) async fn create_base_tables(testdata: &str, client: &Client) -> Result<Vec<Table>> {
    tracing::info!("Preparing tables...");

    let sql = get_seed_table_sql(testdata);
    let (base_tables, statements) = parse_create_table_statements(sql);
    let mut mvs_and_base_tables = vec![];
    mvs_and_base_tables.extend_from_slice(&base_tables);

    for stmt in &statements {
        let create_sql = stmt.to_string();
        tracing::info!("[EXECUTING CREATE TABLE]: {}", &create_sql);
        client.simple_query(&create_sql).await.unwrap();
    }

    Ok(base_tables)
}

/// Create the tables defined in testdata, along with some mviews.
/// TODO: Generate indexes and sinks.
pub(super) async fn create_mviews(
    rng: &mut impl Rng,
    mvs_and_base_tables: Vec<Table>,
    client: &Client,
    config: &Configuration,
) -> Result<(Vec<Table>, Vec<Table>)> {
    let mut mvs_and_base_tables = mvs_and_base_tables;
    let mut mviews = vec![];
    // Generate some mviews
    for i in 0..20 {
        let (create_sql, table) =
            mview_sql_gen(rng, mvs_and_base_tables.clone(), &format!("m{}", i), config);
        tracing::info!("[EXECUTING CREATE MVIEW]: {}", &create_sql);
        let skip_count = run_query(6, client, &create_sql).await?;
        if skip_count == 0 {
            mvs_and_base_tables.push(table.clone());
            mviews.push(table);
        }
    }
    Ok((mvs_and_base_tables, mviews))
}

pub(super) fn format_drop_mview(mview: &Table) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}", mview.name)
}

/// Drops mview tables.
pub(super) async fn drop_mview_table(mview: &Table, client: &Client) {
    client
        .simple_query(&format_drop_mview(mview))
        .await
        .unwrap();
}

/// Drops mview tables and seed tables
pub(super) async fn drop_tables(mviews: &[Table], testdata: &str, client: &Client) {
    tracing::info!("Cleaning tables...");

    for mview in mviews.iter().rev() {
        drop_mview_table(mview, client).await;
    }

    let seed_files = ["drop_tpch.sql", "drop_nexmark.sql", "drop_alltypes.sql"];
    let sql = seed_files
        .iter()
        .map(|filename| read_file_contents(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>();

    for stmt in sql.lines() {
        client.simple_query(stmt).await.unwrap();
    }
}

/// Validate client responses, returning a count of skipped queries, number of result rows.
pub(super) fn validate_response(
    response: PgResult<Vec<SimpleQueryMessage>>,
) -> Result<(i64, Vec<SimpleQueryMessage>)> {
    match response {
        Ok(rows) => Ok((0, rows)),
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error()
                && is_permissible_error(&e.to_string())
            {
                tracing::info!("[SKIPPED ERROR]: {:#?}", e);
                return Ok((1, vec![]));
            }
            // consolidate error reason for deterministic test
            tracing::info!("[UNEXPECTED ERROR]: {:#?}", e);
            Err(anyhow!("Encountered unexpected error: {e}"))
        }
    }
}

pub(super) async fn run_query(timeout_duration: u64, client: &Client, query: &str) -> Result<i64> {
    let (skipped_count, _) = run_query_inner(timeout_duration, client, query, true).await?;
    Ok(skipped_count)
}

/// Run query, handle permissible errors
/// For recovery error, just do bounded retry.
/// For other errors, validate them accordingly, skipping if they are permitted.
/// Otherwise just return success.
/// If takes too long return the query which timed out + execution time + timeout error
/// Returns: Number of skipped queries, number of rows returned.
pub(super) async fn run_query_inner(
    timeout_duration: u64,
    client: &Client,
    query: &str,
    skip_timeout: bool,
) -> Result<(i64, Vec<SimpleQueryMessage>)> {
    let query_task = client.simple_query(query);
    let result = timeout(Duration::from_secs(timeout_duration), query_task).await;
    let response = match result {
        Ok(r) => r,
        Err(_) => {
            if skip_timeout {
                return Ok((1, vec![]));
            } else {
                bail!(
                    "[UNEXPECTED ERROR] Query timeout after {timeout_duration}s:\n{:?}",
                    query
                )
            }
        }
    };
    if let Err(e) = &response
        && let Some(e) = e.as_db_error()
    {
        if is_recovery_in_progress_error(&e.to_string()) {
            let tries = 5;
            let interval = 1;
            for _ in 0..tries {
                // retry 5 times
                sleep(Duration::from_secs(interval)).await;
                let query_task = client.simple_query(query);
                let response = timeout(Duration::from_secs(timeout_duration), query_task).await;
                match response {
                    Ok(Ok(r)) => {
                        return Ok((0, r));
                    }
                    Err(_) => bail!(
                        "[UNEXPECTED ERROR] Query timeout after {timeout_duration}s:\n{:?}",
                        query
                    ),
                    _ => {}
                }
            }
            bail!(
                "[UNEXPECTED ERROR] Failed to recover after {tries} tries with interval {interval}s"
            )
        } else {
            return validate_response(response);
        }
    }
    let rows = response?;
    Ok((0, rows))
}

pub(super) fn generate_rng(seed: Option<u64>) -> impl Rng {
    #[cfg(madsim)]
    if let Some(seed) = seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_rng(&mut SmallRng::from_os_rng())
    }
    #[cfg(not(madsim))]
    if let Some(seed) = seed {
        SmallRng::seed_from_u64(seed)
    } else {
        SmallRng::from_os_rng()
    }
}
