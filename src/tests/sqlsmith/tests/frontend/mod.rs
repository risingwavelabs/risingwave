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
use libtest_mimic::{run_tests, Arguments, Outcome, Test};
use rand::rngs::SmallRng;
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

/// Environment for Sqlsmith to generate and test queries
pub struct SqlsmithEnv {
    session: Arc<SessionImpl>,
    tables: Vec<Table>,
    setup_sql: String,
}

lazy_static::lazy_static! {
    static ref SQLSMITH_ENV: SqlsmithEnv = setup_sqlsmith_with_seed(0);
}

/// Executes sql queries, prints recoverable errors.
/// Panic recovery happens separately.
async fn handle(session: Arc<SessionImpl>, stmt: Statement, sql: &str) {
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
/// NOTE: This depends on convention of test suites
/// not writing to stderr, unless the test fails.
/// (This applies to nexmark).
fn reproduce_failing_queries(setup: &str, failing: &str) {
    eprintln!(
        "
---- START

-- Failing SQL setup code:
{}

-- Failing SQL query:
{};

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
        handle(session.clone(), s, &create_sql).await;
    }

    // Generate some mviews
    for i in 0..10 {
        let (sql, table) = mview_sql_gen(rng, tables.clone(), &format!("m{}", i));
        setup_sql.push_str(&format!("{};", &sql));
        let stmts = parse_sql(&sql);
        let stmt = stmts[0].clone();
        handle(session.clone(), stmt, &sql).await;
        tables.push(table);
    }
    (tables, setup_sql)
}

async fn test_stream_query(
    session: Arc<SessionImpl>,
    tables: Vec<Table>,
    seed: u64,
    setup_sql: &str,
) {
    let mut rng;
    if let Ok(x) = env::var("RW_RANDOM_SEED_SQLSMITH") && x == "true" {
        rng = SmallRng::from_entropy();
    } else {
        rng = SmallRng::seed_from_u64(seed);
    }

    let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
    reproduce_failing_queries(setup_sql, &sql);
    // The generated SQL must be parsable.
    let statements = parse_sql(&sql);
    let stmt = statements[0].clone();
    handle(session.clone(), stmt, &sql).await;

    let drop_sql = format!("DROP MATERIALIZED VIEW {}", table.name);
    let drop_stmts = parse_sql(&drop_sql);
    let drop_stmt = drop_stmts[0].clone();
    handle(session.clone(), drop_stmt, &drop_sql).await;
}

fn test_batch_query(session: Arc<SessionImpl>, tables: Vec<Table>, seed: u64, setup_sql: &str) {
    let mut rng;
    if let Ok(x) = env::var("RW_RANDOM_SEED_SQLSMITH") && x == "true" {
        rng = SmallRng::from_entropy();
    } else {
        rng = SmallRng::seed_from_u64(seed);
    }

    let sql = sql_gen(&mut rng, tables);
    reproduce_failing_queries(setup_sql, &sql);

    // The generated SQL must be parsable.
    let statements = parse_sql(&sql);
    let stmt = statements[0].clone();
    let context: OptimizerContextRef =
        OptimizerContext::new(session.clone(), Arc::from(sql)).into();

    match stmt {
        Statement::Query(_) => {
            let mut binder = Binder::new(&session);
            let bound = binder
                .bind(stmt)
                .unwrap_or_else(|e| panic!("Failed to bind:\nReason:\n{}", e));
            let mut planner = Planner::new(context);
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

/// Setup schema, session for sqlsmith query tests to run.
/// It is synchronous as constrained by the `libtest_mimic` framework.
fn setup_sqlsmith_with_seed(seed: u64) -> SqlsmithEnv {
    // tokio runtime is required by frontend to execute query phases.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(setup_sqlsmith_with_seed_inner(seed))
}

async fn setup_sqlsmith_with_seed_inner(seed: u64) -> SqlsmithEnv {
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
        setup_sql,
    }
}

pub fn run() {
    let args = Arguments::from_args();

    let num_tests = 512;
    let tests = (0..num_tests)
        .map(|i| Test {
            name: format!("run_sqlsmith_on_frontend_{}", i),
            kind: "".into(),
            is_ignored: false,
            is_bench: false,
            data: i,
        })
        .collect();

    run_tests(&args, tests, |test| {
        let SqlsmithEnv {
            session,
            tables,
            setup_sql,
        } = &*SQLSMITH_ENV;
        test_batch_query(session.clone(), tables.clone(), test.data, setup_sql);
        let test_stream_query =
            test_stream_query(session.clone(), tables.clone(), test.data, setup_sql);
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(test_stream_query);
        Outcome::Passed
    })
    .exit();
}
