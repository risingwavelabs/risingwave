#![feature(let_chains)]

// Data-driven tests.

use std::cell::RefCell;
use std::rc::Rc;

use anyhow::{anyhow, Result};
use risingwave_frontend::binder::Binder;
use risingwave_frontend::handler::{create_table, drop_table};
use risingwave_frontend::optimizer::PlanRef;
use risingwave_frontend::planner::Planner;
use risingwave_frontend::session::{QueryContext, QueryContextRef};
use risingwave_frontend::test_utils::LocalFrontend;
use risingwave_sqlparser::ast::{ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    pub sql: String,
    pub logical_plan: Option<String>,
    pub batch_plan: Option<String>,
    pub stream_plan: Option<String>,
    pub binder_error: Option<String>,
    pub planner_error: Option<String>,
    pub optimizer_error: Option<String>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCaseResult {
    pub logical_plan: Option<String>,
    pub batch_plan: Option<String>,
    pub stream_plan: Option<String>,
    pub binder_error: Option<String>,
    pub planner_error: Option<String>,
    pub optimizer_error: Option<String>,
}

impl TestCaseResult {
    /// Convert a result to test case
    pub fn as_test_case(self, sql: &str) -> TestCase {
        TestCase {
            sql: sql.to_string(),
            logical_plan: self.logical_plan,
            batch_plan: self.batch_plan,
            stream_plan: self.stream_plan,
            planner_error: self.planner_error,
            optimizer_error: self.optimizer_error,
            binder_error: self.binder_error,
        }
    }
}

impl TestCase {
    /// Run the test case, and return the expected output.
    pub async fn run(&self, do_check_result: bool) -> Result<TestCaseResult> {
        let frontend = LocalFrontend::new().await;
        let session = frontend.session_ref();
        let statements = Parser::parse_sql(&self.sql).unwrap();

        let mut result = None;

        for stmt in statements {
            let context = QueryContext::new(session.clone());
            match stmt.clone() {
                Statement::Query(_) | Statement::Insert { .. } | Statement::Delete { .. } => {
                    if result.is_some() {
                        panic!("two queries in one test case");
                    }
                    let ret = self.apply_query(&stmt, Rc::new(RefCell::new(context)))?;
                    if do_check_result {
                        check_result(self, &ret)?;
                    }
                    result = Some(ret);
                }
                Statement::CreateTable { name, columns, .. } => {
                    create_table::handle_create_table(context, name, columns).await?;
                }
                Statement::Drop(drop_statement) => {
                    let table_object_name = ObjectName(vec![drop_statement.name]);
                    drop_table::handle_drop_table(context, table_object_name).await?;
                }
                _ => return Err(anyhow!("Unsupported statement type")),
            }
        }
        Ok(result.expect("no queries in this test case"))
    }

    fn apply_query(&self, stmt: &Statement, context: QueryContextRef) -> Result<TestCaseResult> {
        let session = context.borrow().session_ctx.clone();
        let catalog = session
            .env()
            .catalog_mgr()
            .get_database_snapshot(session.database())
            .unwrap();
        let mut binder = Binder::new(catalog);

        let mut ret = TestCaseResult::default();

        let bound = match binder.bind(stmt.clone()) {
            Ok(bound) => bound,
            Err(err) => {
                ret.binder_error = Some(err.to_string());
                return Ok(ret);
            }
        };

        let logical_plan = match Planner::new(context).plan(bound) {
            Ok(logical_plan) => {
                if self.logical_plan.is_some() {
                    ret.logical_plan = Some(explain_plan(&logical_plan.clone().as_subplan()));
                }
                logical_plan
            }
            Err(err) => {
                ret.planner_error = Some(err.to_string());
                return Ok(ret);
            }
        };

        // Only generate batch_plan if it is specified in test case
        if self.batch_plan.is_some() {
            ret.batch_plan = Some(explain_plan(&logical_plan.gen_batch_query_plan()));
        }

        // Only generate stream_plan if it is specified in test case
        if self.stream_plan.is_some() {
            ret.stream_plan = Some(explain_plan(&logical_plan.gen_create_mv_plan()));
        }

        Ok(ret)
    }
}

fn explain_plan(plan: &PlanRef) -> String {
    let mut actual = String::new();
    plan.explain(0, &mut actual).unwrap();
    actual
}

fn check_result(expected: &TestCase, actual: &TestCaseResult) -> Result<()> {
    check_err("binder", &expected.binder_error, &actual.binder_error)?;
    check_err("planner", &expected.planner_error, &actual.planner_error)?;
    check_err(
        "optimizer",
        &expected.optimizer_error,
        &actual.optimizer_error,
    )?;
    check_logical_plan("logical_plan", &expected.logical_plan, &actual.logical_plan)?;
    check_logical_plan("batch_plan", &expected.batch_plan, &actual.batch_plan)?;
    check_logical_plan("stream_plan", &expected.stream_plan, &actual.stream_plan)?;

    Ok(())
}

fn check_logical_plan(
    ctx: &str,
    expected_plan: &Option<String>,
    actual_plan: &Option<String>,
) -> Result<()> {
    match (expected_plan, actual_plan) {
        (Some(expected_plan), Some(actual_plan)) => check_plan_eq(ctx, expected_plan, actual_plan),
        (None, None) => Ok(()),
        (None, Some(_)) => Ok(()),
        (Some(expected_plan), None) => Err(anyhow!(
            "Expected {}:\n{},\nbut failure occurred.",
            ctx,
            *expected_plan
        )),
    }
}

fn check_plan_eq(ctx: &str, expected: &String, actual: &String) -> Result<()> {
    if expected != actual {
        Err(anyhow!(
            "Expected {}:\n{}\nActual {}:\n{}",
            ctx,
            expected,
            ctx,
            actual,
        ))
    } else {
        Ok(())
    }
}

/// Compare the error with the expected error, fail if they are mismatched.
fn check_err(ctx: &str, expected_err: &Option<String>, actual_err: &Option<String>) -> Result<()> {
    match (expected_err, actual_err) {
        (None, None) => Ok(()),
        (None, Some(e)) => Err(anyhow!("unexpected {} error: {}", ctx, e)),
        (Some(e), None) => Err(anyhow!("expected {} error: {}", ctx, e)),
        (Some(l), Some(r)) => {
            let expected_err = l.trim().to_string();
            let actual_err = r.trim().to_string();
            if expected_err == actual_err {
                Ok(())
            } else {
                return Err(anyhow!(
                    "Expected {context} error: {}\n  Actual {context} error: {}",
                    expected_err,
                    actual_err,
                    context = ctx
                ));
            }
        }
    }
}

pub async fn run_test_file(file_name: &str, file_content: &str) {
    println!("-- running {} --", file_name);

    let mut failed_num = 0;
    let cases: Vec<TestCase> = serde_yaml::from_str(file_content).unwrap();
    for c in cases {
        if let Err(e) = c.run(true).await {
            println!("\nTest case failed, the input SQL:\n{}\n{}", c.sql, e);
            failed_num += 1;
        }
    }
    if failed_num > 0 {
        println!("\n");
        panic!("{} test cases failed", failed_num);
    }
}
