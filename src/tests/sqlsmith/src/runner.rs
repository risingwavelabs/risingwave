// Copyright 2023 Singularity Data
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

use crate::validation::is_permissible_error;
use crate::{create_table_statement_to_table, mview_sql_gen, parse_sql, sql_gen, Table};

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
    client
        .query("SET query_mode TO distributed;", &[])
        .await
        .unwrap();
    let mut skipped = 0;
    for _ in 0..sample_size {
        let sql = sql_gen(rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.query(sql.as_str(), &[]).await;
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
        setup_sql.push_str(&format!("{};", &create_sql));
        tracing::info!("Executing MView Setup: {}", &create_sql);
        let response = client.execute(&create_sql, &[]).await;
        let skip_count = validate_response(&setup_sql, &create_sql, response);
        if skip_count == 0 {
            tables.push(table.clone());
            mviews.push(table);
        }
    }
    (tables, mviews, setup_sql)
}

/// Drops mview tables.
async fn drop_mview_table(mview: &Table, client: &tokio_postgres::Client) {
    client
        .execute(
            &format!("DROP MATERIALIZED VIEW IF EXISTS {}", mview.name),
            &[],
        )
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
            panic!(
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
            );
        }
    }
}
