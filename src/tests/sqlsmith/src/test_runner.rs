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

#[cfg(test)]
mod tests {
    use risingwave_frontend::binder::Binder;
    use risingwave_frontend::planner::Planner;
    use risingwave_frontend::session::{OptimizerContext, OptimizerContextRef};
    use risingwave_frontend::test_utils::LocalFrontend;
    use risingwave_frontend::FrontendOpts;
    use risingwave_sqlparser::ast::Statement;
    use risingwave_sqlparser::parser::Parser;

    use crate::SqlGenerator;

    #[tokio::test]
    async fn run_sqlsmith_on_frontend() {
        let frontend = LocalFrontend::new(FrontendOpts::default()).await;
        let session = frontend.session_ref();

        let mut sql_gen = SqlGenerator::new(vec![]);

        for _ in 0..5000 {
            let sql = sql_gen.gen();

            // The generated SQL must be parsable.
            let statements =
                Parser::parse_sql(&sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql));
            let stmt = statements[0].clone();
            let context: OptimizerContextRef = OptimizerContext::new(session.clone()).into();
            match stmt.clone() {
                Statement::Query(_) => {
                    let mut binder = Binder::new(
                        session.env().catalog_reader().read_guard(),
                        session.database().to_string(),
                    );
                    let bound = match binder.bind(stmt.clone()) {
                        Ok(bound) => bound,
                        Err(_) => continue,
                    };
                    let mut planner = Planner::new(context.clone());
                    let logical_plan = match planner.plan(bound) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let _ = logical_plan.gen_batch_query_plan();
                }
                _ => unreachable!(),
            }
        }
    }
}
