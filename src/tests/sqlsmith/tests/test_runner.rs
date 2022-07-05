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

#![feature(let_chains)]

#[cfg(test)]
mod tests {
    use std::env;
    use std::panic;
    use std::sync::Arc;

    use rand::SeedableRng;
    use risingwave_frontend::binder::Binder;
    use risingwave_frontend::handler::create_table;
    use risingwave_frontend::planner::Planner;
    use risingwave_frontend::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
    use risingwave_frontend::test_utils::LocalFrontend;
    use risingwave_frontend::FrontendOpts;
    use risingwave_sqlparser::ast::Statement;
    use risingwave_sqlparser::parser::Parser;
    use risingwave_sqlsmith::{sql_gen, Table};

    /// Create the tables defined in testdata.
    async fn create_tables(session: Arc<SessionImpl>) -> Vec<Table> {
        let sql = std::fs::read_to_string("tests/testdata/tpch.sql").unwrap();
        let statements =
            Parser::parse_sql(&sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql));

        let mut tables = vec![];
        for s in statements.into_iter() {
            match s {
                Statement::CreateTable {
                    name,
                    columns,
                    with_options,
                    ..
                } => {
                    let context = OptimizerContext::new(session.clone(), Arc::from(sql.clone()));
                    create_table::handle_create_table(
                        context,
                        name.clone(),
                        columns.clone(),
                        with_options,
                    )
                    .await
                    .unwrap();
                    tables.push(Table {
                        name: name.0[0].value.clone(),
                        columns: columns.iter().map(|c| c.clone().into()).collect(),
                    })
                }
                _ => panic!("Unexpected statement: {}", s),
            }
        }
        tables
    }

    async fn run_sqlsmith_with_seed(seed: u64) {
        let frontend = LocalFrontend::new(FrontendOpts::default()).await;
        let session = frontend.session_ref();
        let tables = create_tables(session.clone()).await;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

        if let Ok(x) = env::var("RW_RANDOM_SEED_SQLSMITH") && x == "true" {
            log::info!("random seed");
            rng = rand::thread_rng();
        }

        for _ in 0..512 {
            let sql = sql_gen(&mut rng, tables.clone());

            let sql_copy = sql.clone();
            panic::set_hook(Box::new(move |e| {
                println!("Panic on SQL:\n{}\nReason:\n{}", sql_copy.clone(), e);
            }));

            // The generated SQL must be parsable.
            let statements =
                Parser::parse_sql(&sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql));
            let stmt = statements[0].clone();
            let context: OptimizerContextRef =
                OptimizerContext::new(session.clone(), Arc::from(sql.clone())).into();

            match stmt.clone() {
                Statement::Query(_) => {
                    let mut binder = Binder::new(
                        session.env().catalog_reader().read_guard(),
                        session.database().to_string(),
                    );
                    let bound = binder
                        .bind(stmt.clone())
                        .unwrap_or_else(|e| panic!("Failed to bind:\n{}\nReason:\n{}", sql, e));
                    let mut planner = Planner::new(context.clone());
                    let logical_plan = planner.plan(bound).unwrap_or_else(|e| {
                        panic!("Failed to generate logical plan:\n{}\nReason:\n{}", sql, e)
                    });
                    logical_plan.gen_batch_query_plan().unwrap_or_else(|e| {
                        panic!("Failed to generate batch plan:\n{}\nReason:\n{}", sql, e)
                    });
                }
                _ => unreachable!(),
            }
        }
    }

    macro_rules! generate_sqlsmith_test {
        ($seed:expr) => {
            paste::paste! {
                #[tokio::test]
                async fn [<run_sqlsmith_on_frontend_ $seed>]() {
                    run_sqlsmith_with_seed($seed).await;
                }
            }
        };
    }

    generate_sqlsmith_test! { 0 }
    generate_sqlsmith_test! { 1 }
    generate_sqlsmith_test! { 2 }
    generate_sqlsmith_test! { 3 }
    generate_sqlsmith_test! { 4 }
    generate_sqlsmith_test! { 5 }
    generate_sqlsmith_test! { 6 }
    generate_sqlsmith_test! { 7 }
    generate_sqlsmith_test! { 8 }
    generate_sqlsmith_test! { 9 }
    generate_sqlsmith_test! { 10 }
    generate_sqlsmith_test! { 11 }
    generate_sqlsmith_test! { 12 }
    generate_sqlsmith_test! { 13 }
    generate_sqlsmith_test! { 14 }
    generate_sqlsmith_test! { 15 }
    generate_sqlsmith_test! { 16 }
    generate_sqlsmith_test! { 17 }
    generate_sqlsmith_test! { 18 }
    generate_sqlsmith_test! { 19 }
    generate_sqlsmith_test! { 20 }
    generate_sqlsmith_test! { 21 }
    generate_sqlsmith_test! { 22 }
    generate_sqlsmith_test! { 23 }
    generate_sqlsmith_test! { 24 }
    generate_sqlsmith_test! { 25 }
    generate_sqlsmith_test! { 26 }
    generate_sqlsmith_test! { 27 }
    generate_sqlsmith_test! { 28 }
    generate_sqlsmith_test! { 29 }
    generate_sqlsmith_test! { 30 }
    generate_sqlsmith_test! { 31 }
}
