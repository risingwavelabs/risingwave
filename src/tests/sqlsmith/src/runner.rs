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
use itertools::Itertools;
use rand::{Rng, SeedableRng};
use tokio_postgres::error::Error as PgError;
use tokio_postgres::Error;

use crate::validation::is_permissible_error;
use crate::{
    create_table_statement_to_table, mview_sql_gen, parse_sql, session_sql_gen, sql_gen, Table,
};

/// e2e test runner for pre-generated queries from sqlsmith
pub async fn run_pre_generated(client: &tokio_postgres::Client, ddl: &str, queries: &str) {
    let mut setup_sql = String::with_capacity(1000);
    for ddl_statement in parse_sql(ddl) {
        let sql = ddl_statement.to_string();
        let response = client.execute(&sql, &[]).await;
        if let Err(e) = response {
            panic!("{}", format_failed_sql(&setup_sql, &sql, &e))
        }
        setup_sql.push_str(&sql);
    }
    for statement in parse_sql(queries) {
        let sql = statement.to_string();
        let response = client.execute(&sql, &[]).await;
        if let Err(e) = response {
            panic!("{}", format_failed_sql(&setup_sql, &sql, &e))
        }
    }
}

/// e2e query generator
/// The goal is to generate NON-FAILING queries.
/// If we encounter an expected error, just skip.
/// If we panic or encounter an unexpected error, query generation
/// should still fail.
/// Returns ddl and queries.
pub async fn generate(
    client: &tokio_postgres::Client,
    testdata: &str,
    count: usize,
) -> (String, String) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let (tables, mviews, setup_sql) = create_tables(&mut rng, testdata, client).await;

    test_sqlsmith(client, &mut rng, tables.clone(), &setup_sql).await;
    tracing::info!("Passed sqlsmith tests");

    let mut queries = String::with_capacity(10000);
    set_distributed_query_mode(client).await;
    for _ in 0..count {
        // ENABLE: https://github.com/risingwavelabs/risingwave/issues/7928
        // test_session_variable(client, rng).await;
        let sql = sql_gen(&mut rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.execute(sql.as_str(), &[]).await;
        let skipped = validate_response(&setup_sql, &format!("{};", sql), response);
        if skipped == 0 {
            queries.push_str(&sql);
        }
    }

    for _ in 0..count {
        // ENABLE: https://github.com/risingwavelabs/risingwave/issues/7928
        // test_session_variable(client, rng).await;
        let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
        tracing::info!("Executing: {}", sql);
        let response = client.execute(&sql, &[]).await;
        let skipped = validate_response(&setup_sql, &format!("{};", sql), response);
        drop_mview_table(&table, client).await;
        if skipped == 0 {
            queries.push_str(&sql);
            queries.push_str(&format_drop_mview(&table));
        }
    }

    drop_tables(&mviews, testdata, client).await;
    (setup_sql, queries)
}

/// e2e test runner for sqlsmith
pub async fn run(client: &tokio_postgres::Client, testdata: &str, count: usize) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let (tables, mviews, setup_sql) = create_tables(&mut rng, testdata, client).await;

    test_sqlsmith(client, &mut rng, tables.clone(), &setup_sql).await;
    tracing::info!("Passed sqlsmith tests");
    test_batch_queries(client, &mut rng, tables.clone(), &setup_sql, count).await;
    tracing::info!("Passed batch queries");
    test_stream_queries(client, &mut rng, tables.clone(), &setup_sql, count).await;
    tracing::info!("Passed stream queries");

    drop_tables(&mviews, testdata, client).await;
}

/// Sanity checks for sqlsmith
async fn test_sqlsmith<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    tables: Vec<Table>,
    setup_sql: &str,
) {
    // Test percentage of skipped queries <=5% of sample size.
    let threshold = 0.20; // permit at most 20% of queries to be skipped.
    let sample_size = 50;

    let skipped_percentage =
        test_batch_queries(client, rng, tables.clone(), setup_sql, sample_size).await;
    if skipped_percentage > threshold {
        panic!(
            "percentage of skipped batch queries = {}, threshold: {}",
            skipped_percentage, threshold
        );
    }

    let skipped_percentage =
        test_stream_queries(client, rng, tables.clone(), setup_sql, sample_size).await;
    if skipped_percentage > threshold {
        panic!(
            "percentage of skipped stream queries = {}, threshold: {}",
            skipped_percentage, threshold
        );
    }
}

/// `SET QUERY_MODE TO DISTRIBUTED`.
/// Panics if it fails.
async fn set_distributed_query_mode(client: &tokio_postgres::Client) {
    client
        .query("SET query_mode TO distributed;", &[])
        .await
        .unwrap();
}

#[allow(dead_code)]
async fn test_session_variable<R: Rng>(client: &tokio_postgres::Client, rng: &mut R) {
    let session_sql = session_sql_gen(rng);
    tracing::info!("Executing: {}", session_sql);
    client.execute(session_sql.as_str(), &[]).await.unwrap();
}

/// Test batch queries, returns skipped query statistics
/// Runs in distributed mode, since queries can be complex and cause overflow in local execution
/// mode.
async fn test_batch_queries<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    tables: Vec<Table>,
    setup_sql: &str,
    sample_size: usize,
) -> f64 {
    set_distributed_query_mode(client).await;
    let mut skipped = 0;
    for _ in 0..sample_size {
        // ENABLE: https://github.com/risingwavelabs/risingwave/issues/7928
        // test_session_variable(client, rng).await;
        let sql = sql_gen(rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.execute(sql.as_str(), &[]).await;
        skipped += validate_response(setup_sql, &format!("{};", sql), response);
    }
    skipped as f64 / sample_size as f64
}

/// Test stream queries, returns skipped query statistics
async fn test_stream_queries<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    tables: Vec<Table>,
    setup_sql: &str,
    sample_size: usize,
) -> f64 {
    let mut skipped = 0;
    for _ in 0..sample_size {
        // ENABLE: https://github.com/risingwavelabs/risingwave/issues/7928
        // test_session_variable(client, rng).await;
        let (sql, table) = mview_sql_gen(rng, tables.clone(), "stream_query");
        tracing::info!("Executing: {}", sql);
        let response = client.execute(&sql, &[]).await;
        skipped += validate_response(setup_sql, &format!("{};", sql), response);
        drop_mview_table(&table, client).await;
    }
    skipped as f64 / sample_size as f64
}

fn get_seed_table_sql(testdata: &str) -> String {
    let seed_files = vec!["tpch.sql", "nexmark.sql", "alltypes.sql"];
    seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>()
}

async fn create_tables(
    rng: &mut impl Rng,
    testdata: &str,
    client: &tokio_postgres::Client,
) -> (Vec<Table>, Vec<Table>, String) {
    tracing::info!("Preparing tables...");

    let mut setup_sql = String::with_capacity(1000);
    let sql = get_seed_table_sql(testdata);
    let statements = parse_sql(&sql);
    let mut tables = statements
        .iter()
        .map(create_table_statement_to_table)
        .collect_vec();

    for stmt in &statements {
        let create_sql = stmt.to_string();
        setup_sql.push_str(&format!("{};", &create_sql));
        client.execute(&create_sql, &[]).await.unwrap();
    }

    let mut mviews = vec![];
    // Generate some mviews
    for i in 0..10 {
        let (create_sql, table) = mview_sql_gen(rng, tables.clone(), &format!("m{}", i));
        tracing::info!("Executing MView Setup: {}", &create_sql);
        let response = client.execute(&create_sql, &[]).await;
        let skip_count = validate_response(&setup_sql, &create_sql, response);
        if skip_count == 0 {
            setup_sql.push_str(&format!("{};", &create_sql));
            tables.push(table.clone());
            mviews.push(table);
        }
    }
    (tables, mviews, setup_sql)
}

fn format_drop_mview(mview: &Table) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}", mview.name)
}

/// Drops mview tables.
async fn drop_mview_table(mview: &Table, client: &tokio_postgres::Client) {
    client
        .execute(&format_drop_mview(mview), &[])
        .await
        .unwrap();
}

/// Drops mview tables and seed tables
async fn drop_tables(mviews: &[Table], testdata: &str, client: &tokio_postgres::Client) {
    tracing::info!("Cleaning tables...");

    for mview in mviews.iter().rev() {
        drop_mview_table(mview, client).await;
    }

    let seed_files = vec!["drop_tpch.sql", "drop_nexmark.sql", "drop_alltypes.sql"];
    let sql = seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>();

    for stmt in sql.lines() {
        client.execute(stmt, &[]).await.unwrap();
    }
}

fn format_failed_sql(setup_sql: &str, query: &str, e: &Error) -> String {
    format!(
        "
Query failed:
---- START
-- Setup
{}
-- Query
{}
---- END

Reason:
{}
",
        setup_sql, query, e
    )
}

/// Validate client responses, returning a count of skipped queries.
fn validate_response<_Row>(setup_sql: &str, query: &str, response: Result<_Row, PgError>) -> i64 {
    match response {
        Ok(_) => 0,
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error()
                && is_permissible_error(&e.to_string())
            {
                return 1;
            }
            let error_msg = format_failed_sql(setup_sql, query, &e);
            // consolidate error reason for deterministic test
            tracing::info!(error_msg);
            panic!("{}", error_msg)
        }
    }
}
