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

//! Provides E2E Test runner functionality.

use anyhow::{anyhow, bail};
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
#[cfg(madsim)]
use rand_chacha::ChaChaRng;
use risingwave_sqlparser::ast::Statement;
use tokio::time::{sleep, timeout, Duration};
use tokio_postgres::error::Error as PgError;
use tokio_postgres::Client;

use crate::utils::read_file_contents;
use crate::validation::{is_permissible_error, is_recovery_in_progress_error};
use crate::{
    generate_update_statements, insert_sql_gen, mview_sql_gen, parse_create_table_statements,
    parse_sql, session_sql_gen, sql_gen, Table,
};

type PgResult<A> = std::result::Result<A, PgError>;
type Result<A> = anyhow::Result<A>;

/// e2e test runner for pre-generated queries from sqlsmith
pub async fn run_pre_generated(client: &Client, outdir: &str) {
    let timeout_duration = 12; // allow for some variance.
    let queries_path = format!("{}/queries.sql", outdir);
    let queries = read_file_contents(queries_path).unwrap();
    for statement in parse_sql(&queries) {
        let sql = statement.to_string();
        tracing::info!("[EXECUTING STATEMENT]: {}", sql);
        run_query(timeout_duration, client, &sql).await.unwrap();
    }
    tracing::info!("[EXECUTION SUCCESS]");
}

/// Query Generator
/// If we encounter an expected error, just skip.
/// If we encounter an unexpected error,
/// Sqlsmith should stop execution, but writeout ddl and queries so far.
/// If query takes too long -> cancel it, **mark it as error**.
/// NOTE(noel): It will still fail if DDL creation fails.
pub async fn generate(
    client: &Client,
    testdata: &str,
    count: usize,
    _outdir: &str,
    seed: Option<u64>,
) {
    let timeout_duration = 5;

    set_variable(client, "RW_IMPLICIT_FLUSH", "TRUE").await;
    set_variable(client, "QUERY_MODE", "DISTRIBUTED").await;
    tracing::info!("Set session variables");

    let mut rng = generate_rng(seed);
    let base_tables = create_base_tables(testdata, client).await.unwrap();

    let rows_per_table = 50;
    let max_rows_inserted = rows_per_table * base_tables.len();
    let inserts = populate_tables(client, &mut rng, base_tables.clone(), rows_per_table).await;
    tracing::info!("Populated base tables");

    let (tables, mviews) = create_mviews(&mut rng, base_tables.clone(), client)
        .await
        .unwrap();

    // Generate an update for some inserts, on the corresponding table.
    update_base_tables(client, &mut rng, &base_tables, &inserts).await;

    test_sqlsmith(
        client,
        &mut rng,
        tables.clone(),
        base_tables.clone(),
        max_rows_inserted,
    )
    .await;
    tracing::info!("Passed sqlsmith tests");

    tracing::info!("Ran updates");

    let mut generated_queries = 0;
    for _ in 0..count {
        test_session_variable(client, &mut rng).await;
        let sql = sql_gen(&mut rng, tables.clone());
        tracing::info!("[EXECUTING TEST_BATCH]: {}", sql);
        let result = run_query(timeout_duration, client, sql.as_str()).await;
        match result {
            Err(_e) => {
                generated_queries += 1;
                tracing::info!("Generated {} batch queries", generated_queries);
                tracing::error!("Unrecoverable error encountered.");
                return;
            }
            Ok(skipped) if skipped == 0 => {
                generated_queries += 1;
            }
            _ => {}
        }
    }
    tracing::info!("Generated {} batch queries", generated_queries);

    let mut generated_queries = 0;
    for _ in 0..count {
        test_session_variable(client, &mut rng).await;
        let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
        tracing::info!("[EXECUTING TEST_STREAM]: {}", sql);
        let result = run_query(timeout_duration, client, sql.as_str()).await;
        match result {
            Err(_e) => {
                generated_queries += 1;
                tracing::info!("Generated {} stream queries", generated_queries);
                tracing::error!("Unrecoverable error encountered.");
                return;
            }
            Ok(skipped) if skipped == 0 => {
                generated_queries += 1;
            }
            _ => {}
        }
        tracing::info!("[EXECUTING DROP MVIEW]: {}", &format_drop_mview(&table));
        drop_mview_table(&table, client).await;
    }
    tracing::info!("Generated {} stream queries", generated_queries);

    drop_tables(&mviews, testdata, client).await;
}

/// e2e test runner for sqlsmith
pub async fn run(client: &Client, testdata: &str, count: usize, seed: Option<u64>) {
    let mut rng = generate_rng(seed);

    set_variable(client, "RW_IMPLICIT_FLUSH", "TRUE").await;
    set_variable(client, "QUERY_MODE", "DISTRIBUTED").await;
    tracing::info!("Set session variables");

    let base_tables = create_base_tables(testdata, client).await.unwrap();

    let rows_per_table = 50;
    let inserts = populate_tables(client, &mut rng, base_tables.clone(), rows_per_table).await;
    tracing::info!("Populated base tables");

    let (tables, mviews) = create_mviews(&mut rng, base_tables.clone(), client)
        .await
        .unwrap();
    tracing::info!("Created tables");

    // Generate an update for some inserts, on the corresponding table.
    update_base_tables(client, &mut rng, &base_tables, &inserts).await;
    tracing::info!("Ran updates");

    let max_rows_inserted = rows_per_table * base_tables.len();
    test_sqlsmith(
        client,
        &mut rng,
        tables.clone(),
        base_tables.clone(),
        max_rows_inserted,
    )
    .await;
    tracing::info!("Passed sqlsmith tests");

    test_batch_queries(client, &mut rng, tables.clone(), count)
        .await
        .unwrap();
    tracing::info!("Passed batch queries");
    test_stream_queries(client, &mut rng, tables.clone(), count)
        .await
        .unwrap();
    tracing::info!("Passed stream queries");

    drop_tables(&mviews, testdata, client).await;
    tracing::info!("[EXECUTION SUCCESS]");
}

fn generate_rng(seed: Option<u64>) -> impl Rng {
    #[cfg(madsim)]
    if let Some(seed) = seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_rng(SmallRng::from_entropy()).unwrap()
    }
    #[cfg(not(madsim))]
    if let Some(seed) = seed {
        SmallRng::seed_from_u64(seed)
    } else {
        SmallRng::from_entropy()
    }
}

async fn update_base_tables<R: Rng>(
    client: &Client,
    rng: &mut R,
    base_tables: &[Table],
    inserts: &[Statement],
) {
    let update_statements = generate_update_statements(rng, base_tables, inserts).unwrap();
    for update_statement in update_statements {
        let sql = update_statement.to_string();
        tracing::info!("[EXECUTING UPDATES]: {}", &sql);
        client.simple_query(&sql).await.unwrap();
    }
}

async fn populate_tables<R: Rng>(
    client: &Client,
    rng: &mut R,
    base_tables: Vec<Table>,
    row_count: usize,
) -> Vec<Statement> {
    let inserts = insert_sql_gen(rng, base_tables, row_count);
    for insert in &inserts {
        tracing::info!("[EXECUTING INSERT]: {}", insert);
        client.simple_query(insert).await.unwrap();
    }
    inserts
        .iter()
        .map(|s| parse_sql(s).into_iter().next().unwrap())
        .collect_vec()
}

/// Sanity checks for sqlsmith
async fn test_sqlsmith<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    base_tables: Vec<Table>,
    row_count: usize,
) {
    // Test inserted rows should be at least 50% population count,
    // otherwise we don't have sufficient data in our system.
    // ENABLE: https://github.com/risingwavelabs/risingwave/issues/3844
    test_population_count(client, base_tables, row_count).await;
    tracing::info!("passed population count test");

    let threshold = 0.50; // permit at most 50% of queries to be skipped.
    let sample_size = 20;

    let skipped_percentage = test_batch_queries(client, rng, tables.clone(), sample_size)
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

    let skipped_percentage = test_stream_queries(client, rng, tables.clone(), sample_size)
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

async fn set_variable(client: &Client, variable: &str, value: &str) -> String {
    let s = format!("SET {variable} TO {value}");
    tracing::info!("[EXECUTING SET_VAR]: {}", s);
    client.simple_query(&s).await.unwrap();
    s
}

async fn test_session_variable<R: Rng>(client: &Client, rng: &mut R) -> String {
    let session_sql = session_sql_gen(rng);
    tracing::info!("[EXECUTING TEST SESSION_VAR]: {}", session_sql);
    client.simple_query(session_sql.as_str()).await.unwrap();
    session_sql
}

/// Expects at least 50% of inserted rows included.
async fn test_population_count(client: &Client, base_tables: Vec<Table>, expected_count: usize) {
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
async fn test_batch_queries<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    sample_size: usize,
) -> Result<f64> {
    let mut skipped = 0;
    for _ in 0..sample_size {
        test_session_variable(client, rng).await;
        let sql = sql_gen(rng, tables.clone());
        tracing::info!("[TEST BATCH]: {}", sql);
        skipped += run_query(30, client, &sql).await?;
    }
    Ok(skipped as f64 / sample_size as f64)
}

/// Test stream queries, returns skipped query statistics
async fn test_stream_queries<R: Rng>(
    client: &Client,
    rng: &mut R,
    tables: Vec<Table>,
    sample_size: usize,
) -> Result<f64> {
    let mut skipped = 0;

    for _ in 0..sample_size {
        test_session_variable(client, rng).await;
        let (sql, table) = mview_sql_gen(rng, tables.clone(), "stream_query");
        tracing::info!("[TEST STREAM]: {}", sql);
        skipped += run_query(12, client, &sql).await?;
        tracing::info!("[TEST DROP MVIEW]: {}", &format_drop_mview(&table));
        drop_mview_table(&table, client).await;
    }
    Ok(skipped as f64 / sample_size as f64)
}

fn get_seed_table_sql(testdata: &str) -> String {
    let seed_files = vec!["tpch.sql", "nexmark.sql", "alltypes.sql"];
    seed_files
        .iter()
        .map(|filename| read_file_contents(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>()
}

/// Create the tables defined in testdata, along with some mviews.
/// TODO: Generate indexes and sinks.
async fn create_base_tables(testdata: &str, client: &Client) -> Result<Vec<Table>> {
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
async fn create_mviews(
    rng: &mut impl Rng,
    mvs_and_base_tables: Vec<Table>,
    client: &Client,
) -> Result<(Vec<Table>, Vec<Table>)> {
    let mut mvs_and_base_tables = mvs_and_base_tables;
    let mut mviews = vec![];
    // Generate some mviews
    for i in 0..20 {
        let (create_sql, table) =
            mview_sql_gen(rng, mvs_and_base_tables.clone(), &format!("m{}", i));
        tracing::info!("[EXECUTING CREATE MVIEW]: {}", &create_sql);
        let skip_count = run_query(6, client, &create_sql).await?;
        if skip_count == 0 {
            mvs_and_base_tables.push(table.clone());
            mviews.push(table);
        }
    }
    Ok((mvs_and_base_tables, mviews))
}

fn format_drop_mview(mview: &Table) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}", mview.name)
}

/// Drops mview tables.
async fn drop_mview_table(mview: &Table, client: &Client) {
    client
        .simple_query(&format_drop_mview(mview))
        .await
        .unwrap();
}

/// Drops mview tables and seed tables
async fn drop_tables(mviews: &[Table], testdata: &str, client: &Client) {
    tracing::info!("Cleaning tables...");

    for mview in mviews.iter().rev() {
        drop_mview_table(mview, client).await;
    }

    let seed_files = vec!["drop_tpch.sql", "drop_nexmark.sql", "drop_alltypes.sql"];
    let sql = seed_files
        .iter()
        .map(|filename| read_file_contents(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>();

    for stmt in sql.lines() {
        client.simple_query(stmt).await.unwrap();
    }
}

/// Validate client responses, returning a count of skipped queries.
fn validate_response<_Row>(response: PgResult<_Row>) -> Result<i64> {
    match response {
        Ok(_) => Ok(0),
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error() && is_permissible_error(&e.to_string()) {
                tracing::info!("[SKIPPED ERROR]: {:#?}", e);
                return Ok(1);
            }
            // consolidate error reason for deterministic test
            tracing::info!("[UNEXPECTED ERROR]: {:#?}", e);
            Err(anyhow!("Encountered unexpected error: {e}"))
        }
    }
}

/// Run query, handle permissible errors
/// For recovery error, just do bounded retry.
/// For other errors, validate them accordingly, skipping if they are permitted.
/// Otherwise just return success.
/// If takes too long return the query which timed out + execution time + timeout error
/// Returns: Number of skipped queries.
async fn run_query(timeout_duration: u64, client: &Client, query: &str) -> Result<i64> {
    let query_task = client.simple_query(query);
    let result = timeout(Duration::from_secs(timeout_duration), query_task).await;
    let response = match result {
        Ok(r) => r,
        Err(_) => bail!(
            "[UNEXPECTED ERROR] Query timeout after {timeout_duration}s:\n{:?}",
            query
        ),
    };
    if let Err(e) = &response
    && let Some(e) = e.as_db_error() {
        if is_recovery_in_progress_error(&e.to_string()) {
            let tries = 5;
            let interval = 1;
            for _ in 0..tries { // retry 5 times
                sleep(Duration::from_secs(interval)).await;
                let query_task = client.simple_query(query);
                let response = timeout(Duration::from_secs(timeout_duration), query_task).await;
                match response {
                    Ok(r) if r.is_ok() => { return Ok(0); }
                    Err(_) => bail!(
                        "[UNEXPECTED ERROR] Query timeout after {timeout_duration}s:\n{:?}",
                        query
                    ),
                    _ => {}
                }
            }
            bail!("[UNEXPECTED ERROR] Failed to recover after {tries} tries with interval {interval}s")
        } else {
            return validate_response(response);
        }
    }
    Ok(0)
}
