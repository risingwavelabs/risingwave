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

//! Provides E2E Test runner functionality.

use tokio_postgres::Client;

use crate::config::SqlWeightOptions;
use crate::test_runners::utils::{
    create_base_tables, create_mviews, drop_mview_table, drop_tables, format_drop_mview,
    generate_rng, populate_tables, run_query, set_variable, test_batch_queries,
    test_session_variable, test_sqlsmith, test_stream_queries, update_base_tables,
};
use crate::utils::read_file_contents;
use crate::{mview_sql_gen, parse_sql, sql_gen};

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
    config: &SqlWeightOptions,
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
            Ok(0) => {
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
            Ok(0) => {
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
