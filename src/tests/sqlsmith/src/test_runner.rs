#[cfg(test)]
mod tests {
    use risingwave_frontend::handler::query::handle_query;
    use risingwave_frontend::session::OptimizerContext;
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

        for _ in 0..10 {
            let sql = sql_gen.gen();

            // The generated SQL must be parsable.
            let statements =
                Parser::parse_sql(&sql).expect(&format!("Failed to parse SQL: {}", sql));
            let stmt = statements[0].clone();
            let context = OptimizerContext::new(session.clone());
            match stmt.clone() {
                Statement::Query(_) => {
                    let _ = handle_query(context, stmt).await;
                }
                _ => unreachable!(),
            }
        }
    }
}
