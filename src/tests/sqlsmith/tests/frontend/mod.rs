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

use std::sync::Arc;
use std::{env, panic};

use itertools::Itertools;
use rand::{Rng, SeedableRng};
use risingwave_frontend::binder::Binder;
use risingwave_frontend::planner::Planner;
use risingwave_frontend::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use risingwave_frontend::test_utils::LocalFrontend;
use risingwave_frontend::{handler, FrontendOpts};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlsmith::{
    create_table_statement_to_table, mview_sql_gen, parse_sql, sql_gen, Table,
};
use rand::rngs::SmallRng;

pub struct SqlsmithEnv {
    session: Arc<SessionImpl>,
    tables: Vec<Table>,
    rng: SmallRng,
    setup_sql: String,
}

/// Executes sql queries
/// It captures panics so it can recover and print failing sql query.
async fn handle(session: Arc<SessionImpl>, stmt: Statement, setup_sql: &str, sql: &str) {
    reproduce_failing_queries(setup_sql, sql);
    handler::handle(session.clone(), stmt, sql, false)
        .await
        .unwrap_or_else(|e| panic!("Error Reason:\n{}", e));
}

fn get_seed_table_sql() -> String {
    let seed_files = vec!["tests/testdata/tpch.sql", "tests/testdata/nexmark.sql"];
    seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(filename).unwrap())
        .collect::<String>()
}

/// Prints failing queries and their setup code.
/// NOTE: This depends on common convention of test suites
/// not writing to stderr, unless the test fails.
/// (This applies to nexmark).
fn reproduce_failing_queries(setup: &str, failing: &str) {
    eprintln!(
        "
---- START

-- Failing SQL setup code:
{}
-- Failing SQL query:
{}

---- END
",
        setup, failing
    );
}

/// Create the tables defined in testdata.
async fn create_tables(session: Arc<SessionImpl>, rng: &mut impl Rng) -> (Vec<Table>, String) {
    let mut setup_sql = String::with_capacity(1000);
    let sql = get_seed_table_sql();
    setup_sql.push_str(&sql);

    let statements = parse_sql(&sql);
    let mut tables = statements
        .iter()
        .map(create_table_statement_to_table)
        .collect_vec();

    for s in statements.into_iter() {
        let create_sql = s.to_string();
        handle(session.clone(), s, &setup_sql, &create_sql).await;
    }

    // Generate some mviews
    for i in 0..10 {
        let (sql, table) = mview_sql_gen(rng, tables.clone(), &format!("m{}", i));
        setup_sql.push_str(&sql);
        let stmts = parse_sql(&sql);
        let stmt = stmts[0].clone();
        handle(session.clone(), stmt, &setup_sql, &sql).await;
        tables.push(table);
    }
    (tables, setup_sql)
}

#[allow(dead_code)]
#[allow(unreachable_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
fn test_batch_queries(
    session: Arc<SessionImpl>,
    tables: Vec<Table>,
    rng: &mut impl Rng,
    setup_sql: &str,
) {
    let sql = sql_gen(rng, tables.clone());
    reproduce_failing_queries(setup_sql, &sql);

    // The generated SQL must be parsable.
    let statements = parse_sql(&sql);
    let stmt = statements[0].clone();
    let context: OptimizerContextRef =
        OptimizerContext::new(session.clone(), Arc::from(sql.clone())).into();

    match stmt.clone() {
        Statement::Query(_) => {
            // nextest will only print to stderr if test fails
            let mut binder = Binder::new(
                session.env().catalog_reader().read_guard(),
                session.database().to_string(),
            );
            panic!("Failed to bind");
            let bound = todo!();
            // let bound = binder
            //     .bind(stmt.clone())
            //     .unwrap_or_else(|e| panic!("Failed to bind:\nReason:\n{}", e));
            let mut planner = Planner::new(context.clone());
            let logical_plan = planner
                .plan(bound)
                .unwrap_or_else(|e| panic!("Failed to generate logical plan:\nReason:\n{}", e));
            logical_plan
                .gen_batch_query_plan()
                .unwrap_or_else(|e| panic!("Failed to generate batch plan:\nReason:\n{}", e));
        }
        _ => panic!("Invalid Query: {}", stmt),
    }
}

/// Test stream queries by evaluating create mview statements
// async fn test_stream_queries(
//     session: Arc<SessionImpl>,
//     mut tables: Vec<Table>,
//     rng: &mut impl Rng,
//     setup_sql: &str,
// ) {
//     let (sql, table) = mview_sql_gen(rng, tables.clone(), &format!("sq{}", i));
//     let stmts = parse_sql(&sql);
//     let stmt = stmts[0].clone();
//     handle(session.clone(), stmt, setup_sql, &sql).await;
//     tables.push(table);
// }

pub async fn setup_sqlsmith_with_seed(seed: u64) -> SqlsmithEnv {
    let frontend = LocalFrontend::new(FrontendOpts::default()).await;
    let session = frontend.session_ref();

    let mut rng;
    if let Ok(x) = env::var("RW_RANDOM_SEED_SQLSMITH") && x == "true" {
        rng = SmallRng::from_entropy();
    } else {
        rng = SmallRng::seed_from_u64(seed);
    }
    let (tables, setup_sql) = create_tables(session.clone(), &mut rng).await;
    SqlsmithEnv {
        session,
        tables,
        rng,
        setup_sql,
    }
}

pub async fn run_sqlsmith_with_seed(seed: u64) {
    let SqlsmithEnv { session, tables, mut rng, setup_sql } = setup_sqlsmith_with_seed(seed).await;
    test_batch_queries(session.clone(), tables.clone(), &mut rng, &setup_sql);
    // test_stream_queries(session.clone(), tables.clone(), &mut rng, &setup_sql).await;
}

// macro_rules! generate_sqlsmith_test {
//     ($seed:expr) => {
//         paste::paste! {
//             #[tokio::test]
//             async fn [<run_sqlsmith_on_frontend_ $seed>]() {
//                 run_sqlsmith_with_seed($seed).await;
//             }
//         }
//     };
// }

// generate_sqlsmith_test! { 0 }
// generate_sqlsmith_test! { 1 }
// generate_sqlsmith_test! { 2 }
// generate_sqlsmith_test! { 3 }
// generate_sqlsmith_test! { 4 }
// generate_sqlsmith_test! { 5 }
// generate_sqlsmith_test! { 6 }
// generate_sqlsmith_test! { 7 }
// generate_sqlsmith_test! { 8 }
// generate_sqlsmith_test! { 9 }
// generate_sqlsmith_test! { 10 }
// generate_sqlsmith_test! { 11 }
// generate_sqlsmith_test! { 12 }
// generate_sqlsmith_test! { 13 }
// generate_sqlsmith_test! { 14 }
// generate_sqlsmith_test! { 15 }
// generate_sqlsmith_test! { 16 }
// generate_sqlsmith_test! { 17 }
// generate_sqlsmith_test! { 18 }
// generate_sqlsmith_test! { 19 }
// generate_sqlsmith_test! { 20 }
// generate_sqlsmith_test! { 21 }
// generate_sqlsmith_test! { 22 }
// generate_sqlsmith_test! { 23 }
// generate_sqlsmith_test! { 24 }
// generate_sqlsmith_test! { 25 }
// generate_sqlsmith_test! { 26 }
// generate_sqlsmith_test! { 27 }
// generate_sqlsmith_test! { 28 }
// generate_sqlsmith_test! { 29 }
// generate_sqlsmith_test! { 30 }
// generate_sqlsmith_test! { 31 }
