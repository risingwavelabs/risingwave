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
use std::fs::File;
use std::io::Write;
use std::path::Path;

use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
#[cfg(madsim)]
use rand_chacha::ChaChaRng;
use tokio_postgres::error::Error as PgError;
use tokio_postgres::Error;

use crate::validation::is_permissible_error;
use crate::{
    create_table_statement_to_table, insert_sql_gen, mview_sql_gen, parse_sql, session_sql_gen,
    sql_gen, Table,
};

/// e2e test runner for pre-generated queries from sqlsmith
pub async fn run_pre_generated(client: &tokio_postgres::Client, outdir: &str) {
    let ddl_path = format!("{}/ddl.sql", outdir);
    let queries_path = format!("{}/queries.sql", outdir);
    let ddl = std::fs::read_to_string(ddl_path).unwrap();
    let queries = std::fs::read_to_string(queries_path).unwrap();
    let mut setup_sql = String::with_capacity(1000);
    for ddl_statement in parse_sql(&ddl) {
        let sql = ddl_statement.to_string();
        tracing::info!("Executing: {}", sql);
        let response = client.execute(&sql, &[]).await;
        if let Err(e) = response {
            panic!("{}", format_fail_reason(&setup_sql, &sql, &e))
        }
        setup_sql.push_str(&sql);
    }
    for statement in parse_sql(&queries) {
        let sql = statement.to_string();
        tracing::info!("Executing: {}", sql);
        let response = client.simple_query(&sql).await;
        if let Err(e) = response {
            panic!("{}", format_fail_reason(&setup_sql, &sql, &e))
        }
    }
}

/// e2e query generator
/// The goal is to generate NON-FAILING queries.
/// If we encounter an expected error, just skip.
/// If we panic or encounter an unexpected error, query generation
/// should still fail.
/// Returns ddl and queries.
pub async fn generate(client: &tokio_postgres::Client, testdata: &str, count: usize, outdir: &str) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let (tables, base_tables, mviews, setup_sql) = create_tables(&mut rng, testdata, client).await;

    let rows_per_table = 10;
    let max_rows_inserted = rows_per_table * base_tables.len();
    test_sqlsmith(
        client,
        &mut rng,
        tables.clone(),
        &setup_sql,
        base_tables,
        max_rows_inserted,
    )
    .await;
    tracing::info!("Passed sqlsmith tests");

    let mut queries = String::with_capacity(10000);
    let mut generated_queries = 0;
    for _ in 0..count {
        let session_sql = test_session_variable(client, &mut rng).await;
        let sql = sql_gen(&mut rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.simple_query(sql.as_str()).await;
        let skipped =
            validate_response(&setup_sql, &format!("{};\n{};", session_sql, sql), response);
        if skipped == 0 {
            generated_queries += 1;
            queries.push_str(&format!("{};\n", &sql));
        }
    }
    tracing::info!("Generated {} batch queries", generated_queries);

    let mut generated_queries = 0;
    for _ in 0..count {
        let session_sql = test_session_variable(client, &mut rng).await;
        let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
        tracing::info!("Executing: {}", sql);
        let response = client.simple_query(&sql).await;
        let skipped =
            validate_response(&setup_sql, &format!("{};\n{};", session_sql, sql), response);
        drop_mview_table(&table, client).await;
        if skipped == 0 {
            generated_queries += 1;
            queries.push_str(&format!("{};\n", &sql));
            queries.push_str(&format!("{};\n", format_drop_mview(&table)));
        }
    }
    tracing::info!("Generated {} stream queries", generated_queries);

    drop_tables(&mviews, testdata, client).await;
    write_to_file(outdir, "ddl.sql", &setup_sql);
    write_to_file(outdir, "queries.sql", &queries);
}

fn write_to_file(outdir: &str, name: &str, sql: &str) {
    let resolved = format!("{}/{}", outdir, name);
    let path = Path::new(&resolved);
    let mut file = match File::create(path) {
        Err(e) => panic!("couldn't create {}: {}", path.display(), e),
        Ok(file) => file,
    };
    match file.write_all(sql.as_bytes()) {
        Err(why) => panic!("couldn't write to {}: {}", path.display(), why),
        Ok(_) => tracing::info!("successfully wrote to {}", path.display()),
    }
}

/// e2e test runner for sqlsmith
pub async fn run(client: &tokio_postgres::Client, testdata: &str, count: usize, seed: Option<u64>) {
    #[cfg(madsim)]
    let mut rng = if let Some(seed) = seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_rng(SmallRng::from_entropy()).unwrap()
    };
    #[cfg(not(madsim))]
    let mut rng = if let Some(seed) = seed {
        SmallRng::seed_from_u64(seed)
    } else {
        SmallRng::from_entropy()
    };
    let (tables, base_tables, mviews, mut setup_sql) =
        create_tables(&mut rng, testdata, client).await;
    tracing::info!("Created tables");

    let session_sql = set_variable(client, "RW_IMPLICIT_FLUSH", "TRUE").await;
    setup_sql.push_str(&session_sql);
    let session_sql = set_variable(client, "QUERY_MODE", "DISTRIBUTED").await;
    setup_sql.push_str(&session_sql);
    tracing::info!("Set session variables");

    let rows_per_table = 10;
    // ENABLE: https://github.com/risingwavelabs/risingwave/issues/3844
    // let populate_sql = populate_tables(client, &mut rng, base_tables.clone(),
    // rows_per_table).await; let setup_sql = format!("{}\n{}", setup_sql, populate_sql);
    tracing::info!("Populated base tables");

    let max_rows_inserted = rows_per_table * base_tables.len();

    test_sqlsmith(
        client,
        &mut rng,
        tables.clone(),
        &setup_sql,
        base_tables,
        max_rows_inserted,
    )
    .await;
    tracing::info!("Passed sqlsmith tests");
    test_batch_queries(client, &mut rng, tables.clone(), &setup_sql, count).await;
    tracing::info!("Passed batch queries");
    test_stream_queries(client, &mut rng, tables.clone(), &setup_sql, count).await;
    tracing::info!("Passed stream queries");

    drop_tables(&mviews, testdata, client).await;
}

#[allow(dead_code)]
async fn populate_tables<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    base_tables: Vec<Table>,
    row_count: usize,
) -> String {
    let inserts = insert_sql_gen(rng, base_tables, row_count);
    for insert in &inserts {
        tracing::info!("[EXECUTING POPULATION]: {}", insert);
        client.simple_query(insert).await.unwrap();
    }
    inserts.into_iter().map(|i| format!("{};\n", i)).collect()
}

/// Sanity checks for sqlsmith
async fn test_sqlsmith<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    tables: Vec<Table>,
    setup_sql: &str,
    _base_tables: Vec<Table>,
    _row_count: usize,
) {
    // Test inserted rows should be at least 50% population count,
    // otherwise we don't have sufficient data in our system.
    // ENABLE: https://github.com/risingwavelabs/risingwave/issues/3844
    // test_population_count(client, base_tables, row_count).await;
    // tracing::info!("passed population count test");

    // Test percentage of skipped queries <=5% of sample size.
    let threshold = 0.40; // permit at most 40% of queries to be skipped.
    let sample_size = 50;

    let skipped_percentage =
        test_batch_queries(client, rng, tables.clone(), setup_sql, sample_size).await;
    tracing::info!(
        "percentage of skipped batch queries = {}, threshold: {}",
        skipped_percentage,
        threshold
    );
    if skipped_percentage > threshold {
        panic!("skipped batch queries exceeded threshold.");
    }

    let skipped_percentage =
        test_stream_queries(client, rng, tables.clone(), setup_sql, sample_size).await;
    tracing::info!(
        "percentage of skipped stream queries = {}, threshold: {}",
        skipped_percentage,
        threshold
    );
    if skipped_percentage > threshold {
        panic!("skipped stream queries exceeded threshold.");
    }
}

async fn set_variable(client: &tokio_postgres::Client, variable: &str, value: &str) -> String {
    let s = format!("SET {variable} TO {value};");
    tracing::info!("[EXECUTING SET_VAR]: {}", s);
    client.simple_query(&s).await.unwrap();
    s
}

async fn test_session_variable<R: Rng>(client: &tokio_postgres::Client, rng: &mut R) -> String {
    let session_sql = session_sql_gen(rng);
    tracing::info!("[EXECUTING TEST SESSION_VAR]: {}", session_sql);
    client.simple_query(session_sql.as_str()).await.unwrap();
    session_sql
}

/// Expects at least 50% of inserted rows included.
#[allow(dead_code)]
async fn test_population_count(
    client: &tokio_postgres::Client,
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
async fn test_batch_queries<R: Rng>(
    client: &tokio_postgres::Client,
    rng: &mut R,
    tables: Vec<Table>,
    setup_sql: &str,
    sample_size: usize,
) -> f64 {
    let mut skipped = 0;
    for _ in 0..sample_size {
        let session_sql = test_session_variable(client, rng).await;
        let sql = sql_gen(rng, tables.clone());
        tracing::info!("[EXECUTING TEST_BATCH]: {}", sql);
        let response = client.simple_query(sql.as_str()).await;
        skipped += validate_response(setup_sql, &format!("{};\n{};", session_sql, sql), response);
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
        let session_sql = test_session_variable(client, rng).await;
        let (sql, table) = mview_sql_gen(rng, tables.clone(), "stream_query");
        tracing::info!("[EXECUTING TEST_STREAM]: {}", sql);
        let response = client.simple_query(&sql).await;
        skipped += validate_response(setup_sql, &format!("{};\n{};", session_sql, sql), response);
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

/// Create the tables defined in testdata, along with some mviews.
/// TODO: Generate indexes and sinks.
async fn create_tables(
    rng: &mut impl Rng,
    testdata: &str,
    client: &tokio_postgres::Client,
) -> (Vec<Table>, Vec<Table>, Vec<Table>, String) {
    tracing::info!("Preparing tables...");

    let mut setup_sql = String::with_capacity(1000);
    let sql = get_seed_table_sql(testdata);
    let statements = parse_sql(&sql);
    let mut mvs_and_base_tables = vec![];
    let base_tables = statements
        .iter()
        .map(create_table_statement_to_table)
        .collect_vec();
    mvs_and_base_tables.extend_from_slice(&base_tables);

    for stmt in &statements {
        let create_sql = stmt.to_string();
        tracing::info!("[EXECUTING CREATE TABLE]: {}", &create_sql);
        client.simple_query(&create_sql).await.unwrap();
        setup_sql.push_str(&format!("{};\n", &create_sql));
    }

    let mut mviews = vec![];
    // Generate some mviews
    for i in 0..10 {
        let (create_sql, table) =
            mview_sql_gen(rng, mvs_and_base_tables.clone(), &format!("m{}", i));
        tracing::info!("[EXECUTING CREATE MVIEW]: {}", &create_sql);
        let response = client.simple_query(&create_sql).await;
        let skip_count = validate_response(&setup_sql, &create_sql, response);
        if skip_count == 0 {
            setup_sql.push_str(&format!("{};\n", &create_sql));
            mvs_and_base_tables.push(table.clone());
            mviews.push(table);
        }
    }
    (mvs_and_base_tables, base_tables, mviews, setup_sql)
}

fn format_drop_mview(mview: &Table) -> String {
    format!("DROP MATERIALIZED VIEW IF EXISTS {}", mview.name)
}

/// Drops mview tables.
async fn drop_mview_table(mview: &Table, client: &tokio_postgres::Client) {
    client
        .simple_query(&format_drop_mview(mview))
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
        client.simple_query(stmt).await.unwrap();
    }
}

fn format_fail_reason(setup_sql: &str, query: &str, e: &Error) -> String {
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
                tracing::info!("[SKIPPED ERROR]: {:?}", e);
                return 1;
            }
            // consolidate error reason for deterministic test
            let error_msg = format_fail_reason(setup_sql, query, &e);
            tracing::info!(error_msg);
            panic!("{}", error_msg);
        }
    }
}
