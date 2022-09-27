// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use rand::{Rng, SeedableRng};
use risingwave_expr::error::ExprError;
use tokio_postgres::error::{DbError, Error as PgError, SqlState};

use crate::{create_table_statement_to_table, mview_sql_gen, parse_sql, sql_gen, Table};

pub async fn run(client: &tokio_postgres::Client, testdata: &str, count: usize) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let (tables, mviews, setup_sql) = create_tables(&mut rng, testdata, client).await;

    // Test batch
    // Queries we generate are complex, can cause overflow in
    // local execution mode.
    client
        .query("SET query_mode TO distributed;", &[])
        .await
        .unwrap();
    for _ in 0..count {
        let sql = sql_gen(&mut rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.query(sql.as_str(), &[]).await;
        validate_response(&setup_sql, &format!("{};", sql), response);
    }

    // Test stream
    for _ in 0..count {
        let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
        tracing::info!("Executing: {}", sql);
        let response = client.execute(&sql, &[]).await;
        validate_response(&setup_sql, &format!("{};", sql), response);
        drop_mview_table(&table, client).await;
    }

    drop_tables(&mviews, testdata, client).await;
}

fn get_seed_table_sql(testdata: &str) -> String {
    let seed_files = vec!["tpch.sql", "nexmark.sql"];
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
        client.execute(&create_sql, &[]).await.unwrap();
        tables.push(table.clone());
        mviews.push(table);
    }
    (tables, mviews, setup_sql)
}

async fn drop_mview_table(mview: &Table, client: &tokio_postgres::Client) {
    client
        .execute(&format!("DROP MATERIALIZED VIEW {}", mview.name), &[])
        .await
        .unwrap();
}

async fn drop_tables(mviews: &[Table], testdata: &str, client: &tokio_postgres::Client) {
    tracing::info!("Cleaning tables...");

    for mview in mviews.iter().rev() {
        drop_mview_table(mview, client).await;
    }

    let seed_files = vec!["drop_tpch.sql", "drop_nexmark.sql"];
    let sql = seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(format!("{}/{}", testdata, filename)).unwrap())
        .collect::<String>();

    for stmt in sql.lines() {
        client.execute(stmt, &[]).await.unwrap();
    }
}

fn is_division_by_zero_err(db_error: &DbError) -> bool {
    db_error
        .message()
        .contains(&ExprError::DivisionByZero.to_string())
}

fn is_numeric_out_of_range_err(db_error: &DbError) -> bool {
    db_error
        .message()
        .contains(&ExprError::NumericOutOfRange.to_string())
}

/// Workaround to permit runtime errors not being propagated through channels.
/// FIXME: This also means some internal system errors won't be caught.
/// Tracked by: <https://github.com/risingwavelabs/risingwave/issues/3908#issuecomment-1186782810>
fn is_broken_chan_err(db_error: &DbError) -> bool {
    db_error
        .message()
        .contains("internal error: broken fifo_channel")
}

fn is_permissible_error(db_error: &DbError) -> bool {
    let is_internal_error = *db_error.code() == SqlState::INTERNAL_ERROR;
    is_internal_error
        && (is_numeric_out_of_range_err(db_error)
            || is_broken_chan_err(db_error)
            || is_division_by_zero_err(db_error))
}

/// Validate client responses
fn validate_response<_Row>(setup_sql: &str, query: &str, response: Result<_Row, PgError>) {
    match response {
        Ok(_) => {}
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error()
                && is_permissible_error(e)
            {
                return;
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
