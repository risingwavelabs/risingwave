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

#![feature(label_break_value)]
#![allow(clippy::derive_partial_eq_without_eq)]
//! Data-driven tests.

mod resolve_id;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use resolve_id::*;
use risingwave_frontend::handler::{
    create_index, create_mv, create_source, create_table, drop_table, variable,
};
use risingwave_frontend::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use risingwave_frontend::test_utils::{create_proto_file, LocalFrontend};
use risingwave_frontend::{
    build_graph, explain_stream_graph, Binder, FrontendOpts, PlanRef, Planner, WithOptions,
};
use risingwave_sqlparser::ast::{ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    /// Id of the test case, used in before.
    pub id: Option<String>,

    /// A brief description of the test case.
    pub name: Option<String>,

    /// Before running the SQL statements, the test runner will execute the specified test cases
    pub before: Option<Vec<String>>,

    /// The resolved statements of the before ids
    #[serde(skip_serializing)]
    before_statements: Option<Vec<String>>,

    /// The SQL statements
    pub sql: String,

    /// The original logical plan
    pub logical_plan: Option<String>,

    /// Logical plan with optimization `.gen_optimized_logical_plan()`
    pub optimized_logical_plan: Option<String>,

    /// Distributed batch plan `.gen_batch_query_plan()`
    pub batch_plan: Option<String>,

    /// Proto JSON of generated batch plan
    pub batch_plan_proto: Option<String>,

    /// Batch plan for local execution `.gen_batch_local_plan()`
    pub batch_local_plan: Option<String>,

    /// Create MV plan `.gen_create_mv_plan()`
    pub stream_plan: Option<String>,

    /// Create MV fragments plan
    pub stream_dist_plan: Option<String>,

    // TODO: uncomment for Proto JSON of generated stream plan
    //  was: "stream_plan_proto": Option<String>
    // pub plan_graph_proto: Option<String>,
    /// Error of binder
    pub binder_error: Option<String>,

    /// Error of planner
    pub planner_error: Option<String>,

    /// Error of optimizer
    pub optimizer_error: Option<String>,

    /// Error of `.gen_batch_query_plan()`
    pub batch_error: Option<String>,

    /// Error of `.gen_batch_local_plan()`
    pub batch_local_error: Option<String>,

    /// Error of `.gen_stream_plan()`
    pub stream_error: Option<String>,

    /// Support using file content or file location to create source.
    pub create_source: Option<CreateSource>,

    /// Provide config map to frontend
    pub with_config_map: Option<BTreeMap<String, String>>,
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CreateSource {
    row_format: String,
    name: String,
    file: Option<String>,
    materialized: Option<bool>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCaseResult {
    /// The original logical plan
    pub logical_plan: Option<String>,

    /// Logical plan with optimization `.gen_optimized_logical_plan()`
    pub optimized_logical_plan: Option<String>,

    /// Distributed batch plan `.gen_batch_query_plan()`
    pub batch_plan: Option<String>,

    /// Proto JSON of generated batch plan
    pub batch_plan_proto: Option<String>,

    /// Batch plan for local execution `.gen_batch_local_plan()`
    pub batch_local_plan: Option<String>,

    /// Create MV plan `.gen_create_mv_plan()`
    pub stream_plan: Option<String>,

    /// Create MV fragments plan
    pub stream_dist_plan: Option<String>,

    /// Error of binder
    pub binder_error: Option<String>,

    /// Error of planner
    pub planner_error: Option<String>,

    /// Error of optimizer
    pub optimizer_error: Option<String>,

    /// Error of `.gen_batch_query_plan()`
    pub batch_error: Option<String>,

    /// Error of `.gen_batch_local_plan()`
    pub batch_local_error: Option<String>,

    /// Error of `.gen_stream_plan()`
    pub stream_error: Option<String>,
}

impl TestCaseResult {
    /// Convert a result to test case
    pub fn into_test_case(self, original_test_case: &TestCase) -> Result<TestCase> {
        if original_test_case.binder_error.is_none() && let Some(ref err) = self.binder_error {
            return Err(anyhow!("unexpected binder error: {}", err));
        }
        if original_test_case.planner_error.is_none() && let Some(ref err) = self.planner_error {
            return Err(anyhow!("unexpected planner error: {}", err));
        }
        if original_test_case.optimizer_error.is_none() && let Some(ref err) = self.optimizer_error {
            return Err(anyhow!("unexpected optimizer error: {}", err));
        }

        let case = TestCase {
            id: original_test_case.id.clone(),
            name: original_test_case.name.clone(),
            before: original_test_case.before.clone(),
            sql: original_test_case.sql.to_string(),
            before_statements: original_test_case.before_statements.clone(),
            logical_plan: self.logical_plan,
            optimized_logical_plan: self.optimized_logical_plan,
            batch_plan: self.batch_plan,
            batch_local_plan: self.batch_local_plan,
            stream_plan: self.stream_plan,
            batch_plan_proto: self.batch_plan_proto,
            planner_error: self.planner_error,
            optimizer_error: self.optimizer_error,
            batch_error: self.batch_error,
            batch_local_error: self.batch_local_error,
            stream_error: self.stream_error,
            binder_error: self.binder_error,
            create_source: original_test_case.create_source.clone(),
            with_config_map: original_test_case.with_config_map.clone(),
            stream_dist_plan: self.stream_dist_plan,
        };
        Ok(case)
    }
}

impl TestCase {
    /// Run the test case, and return the expected output.
    pub async fn run(&self, do_check_result: bool) -> Result<TestCaseResult> {
        let frontend = LocalFrontend::new(FrontendOpts::default()).await;
        let session = frontend.session_ref();

        if let Some(ref config_map) = self.with_config_map {
            for (key, val) in config_map {
                session.set_config(key, val).unwrap();
            }
        }

        let placeholder_empty_vec = vec![];

        // Since temp file will be deleted when it goes out of scope, so create source in advance.
        self.create_source(session.clone()).await?;

        let mut result: Option<TestCaseResult> = None;
        for sql in self
            .before_statements
            .as_ref()
            .unwrap_or(&placeholder_empty_vec)
            .iter()
            .chain(std::iter::once(&self.sql))
        {
            result = self
                .run_sql(sql, session.clone(), do_check_result, result)
                .await?;
        }

        Ok(result.unwrap_or_default())
    }

    // If testcase have create source info, run sql to create source.
    // Support create source by file content or file location.
    async fn create_source(&self, session: Arc<SessionImpl>) -> Result<Option<TestCaseResult>> {
        match self.create_source.clone() {
            Some(source) => {
                if let Some(content) = source.file {
                    let materialized = if let Some(true) = source.materialized {
                        "materialized".to_string()
                    } else {
                        "".to_string()
                    };
                    let sql = format!(
                        r#"CREATE {} SOURCE {}
    WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT {} MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://"#,
                        materialized, source.name, source.row_format
                    );
                    let temp_file = create_proto_file(content.as_str());
                    self.run_sql(
                        &(sql + temp_file.path().to_str().unwrap() + "'"),
                        session.clone(),
                        false,
                        None,
                    )
                    .await
                } else {
                    panic!(
                        "{:?} create source must include `file` for the file content",
                        self.id
                    );
                }
            }
            None => Ok(None),
        }
    }

    async fn run_sql(
        &self,
        sql: &str,
        session: Arc<SessionImpl>,
        do_check_result: bool,
        mut result: Option<TestCaseResult>,
    ) -> Result<Option<TestCaseResult>> {
        let statements = Parser::parse_sql(sql).unwrap();
        for stmt in statements {
            let context = OptimizerContext::new(
                session.clone(),
                Arc::from(sql),
                WithOptions::try_from(&stmt)?,
            );
            context.explain_verbose.store(true, Ordering::Relaxed); // use explain verbose in planner tests
            match stmt.clone() {
                Statement::Query(_)
                | Statement::Insert { .. }
                | Statement::Delete { .. }
                | Statement::Update { .. } => {
                    if result.is_some() {
                        panic!("two queries in one test case");
                    }
                    let ret = self.apply_query(&stmt, context.into())?;
                    if do_check_result {
                        check_result(self, &ret)?;
                    }
                    result = Some(ret);
                }
                Statement::CreateTable {
                    name,
                    columns,
                    constraints,
                    ..
                } => {
                    create_table::handle_create_table(context, name, columns, constraints).await?;
                }
                Statement::CreateSource {
                    is_materialized,
                    stmt,
                } => {
                    create_source::handle_create_source(context, is_materialized, stmt).await?;
                }
                Statement::CreateIndex {
                    name,
                    table_name,
                    columns,
                    include,
                    distributed_by,
                    // TODO: support unique and if_not_exist in planner test
                    ..
                } => {
                    create_index::handle_create_index(
                        context,
                        false,
                        name,
                        table_name,
                        columns,
                        include,
                        distributed_by,
                    )
                    .await?;
                }
                Statement::CreateView {
                    materialized: true,
                    or_replace: false,
                    name,
                    query,
                    ..
                } => {
                    create_mv::handle_create_mv(context, name, *query).await?;
                }
                Statement::Drop(drop_statement) => {
                    drop_table::handle_drop_table(context, drop_statement.object_name).await?;
                }
                Statement::SetVariable {
                    local: _,
                    variable,
                    value,
                } => {
                    variable::handle_set(context, variable, value).unwrap();
                }
                _ => return Err(anyhow!("Unsupported statement type")),
            }
        }
        Ok(result)
    }

    fn apply_query(
        &self,
        stmt: &Statement,
        context: OptimizerContextRef,
    ) -> Result<TestCaseResult> {
        let session = context.inner().session_ctx.clone();
        let mut ret = TestCaseResult::default();

        let bound = {
            let mut binder = Binder::new(&session);
            match binder.bind(stmt.clone()) {
                Ok(bound) => bound,
                Err(err) => {
                    ret.binder_error = Some(err.to_string());
                    return Ok(ret);
                }
            }
        };

        let mut planner = Planner::new(context.clone());

        let mut logical_plan = match planner.plan(bound) {
            Ok(logical_plan) => {
                if self.logical_plan.is_some() {
                    ret.logical_plan = Some(explain_plan(&logical_plan.clone().into_subplan()));
                }
                logical_plan
            }
            Err(err) => {
                ret.planner_error = Some(err.to_string());
                return Ok(ret);
            }
        };

        if self.optimized_logical_plan.is_some() || self.optimizer_error.is_some() {
            let optimized_logical_plan = match logical_plan.gen_optimized_logical_plan() {
                Ok(optimized_logical_plan) => optimized_logical_plan,
                Err(err) => {
                    ret.optimizer_error = Some(err.to_string());
                    return Ok(ret);
                }
            };

            // Only generate optimized_logical_plan if it is specified in test case
            if self.optimized_logical_plan.is_some() {
                ret.optimized_logical_plan = Some(explain_plan(&optimized_logical_plan));
            }
        }

        'batch: {
            if self.batch_plan.is_some()
                || self.batch_plan_proto.is_some()
                || self.batch_error.is_some()
            {
                let batch_plan = match logical_plan.gen_batch_distributed_plan() {
                    Ok(batch_plan) => batch_plan,
                    Err(err) => {
                        ret.batch_error = Some(err.to_string());
                        break 'batch;
                    }
                };

                // Only generate batch_plan if it is specified in test case
                if self.batch_plan.is_some() {
                    ret.batch_plan = Some(explain_plan(&batch_plan));
                }

                // Only generate batch_plan_proto if it is specified in test case
                if self.batch_plan_proto.is_some() {
                    ret.batch_plan_proto = Some(serde_yaml::to_string(
                        &batch_plan.to_batch_prost_identity(false),
                    )?);
                }
            }
        }

        'local_batch: {
            if self.batch_local_plan.is_some() || self.batch_local_error.is_some() {
                let batch_plan = match logical_plan.gen_batch_local_plan() {
                    Ok(batch_plan) => batch_plan,
                    Err(err) => {
                        ret.batch_local_error = Some(err.to_string());
                        break 'local_batch;
                    }
                };

                // Only generate batch_plan if it is specified in test case
                if self.batch_local_plan.is_some() {
                    ret.batch_local_plan = Some(explain_plan(&batch_plan));
                }
            }
        }

        'stream: {
            if self.stream_plan.is_some()
                || self.stream_error.is_some()
                || self.stream_dist_plan.is_some()
            {
                let q = if let Statement::Query(q) = stmt {
                    q.as_ref().clone()
                } else {
                    return Err(anyhow!("expect a query"));
                };

                let stream_plan = match create_mv::gen_create_mv_plan(
                    &session,
                    context,
                    q,
                    ObjectName(vec!["test".into()]),
                    false,
                ) {
                    Ok((stream_plan, _)) => stream_plan,
                    Err(err) => {
                        ret.stream_error = Some(err.to_string());
                        break 'stream;
                    }
                };

                // Only generate stream_plan if it is specified in test case
                if self.stream_plan.is_some() {
                    ret.stream_plan = Some(explain_plan(&stream_plan));
                }

                // Only generate stream_dist_plan if it is specified in test case
                if self.stream_dist_plan.is_some() {
                    let graph = build_graph(stream_plan);
                    ret.stream_dist_plan = Some(explain_stream_graph(&graph, false).unwrap());
                }
            }
        }

        Ok(ret)
    }
}

fn explain_plan(plan: &PlanRef) -> String {
    plan.explain_to_string().expect("failed to explain")
}

fn check_result(expected: &TestCase, actual: &TestCaseResult) -> Result<()> {
    check_err("binder", &expected.binder_error, &actual.binder_error)?;
    check_err("planner", &expected.planner_error, &actual.planner_error)?;
    check_err(
        "optimizer",
        &expected.optimizer_error,
        &actual.optimizer_error,
    )?;
    check_err("batch", &expected.batch_error, &actual.batch_error)?;
    check_err(
        "batch_local",
        &expected.batch_local_error,
        &actual.batch_local_error,
    )?;
    check_option_plan_eq("logical_plan", &expected.logical_plan, &actual.logical_plan)?;
    check_option_plan_eq(
        "optimized_logical_plan",
        &expected.optimized_logical_plan,
        &actual.optimized_logical_plan,
    )?;
    check_option_plan_eq("batch_plan", &expected.batch_plan, &actual.batch_plan)?;
    check_option_plan_eq(
        "batch_local_plan",
        &expected.batch_local_plan,
        &actual.batch_local_plan,
    )?;
    check_option_plan_eq("stream_plan", &expected.stream_plan, &actual.stream_plan)?;
    check_option_plan_eq(
        "stream_dist_plan",
        &expected.stream_dist_plan,
        &actual.stream_dist_plan,
    )?;
    check_option_plan_eq(
        "batch_plan_proto",
        &expected.batch_plan_proto,
        &actual.batch_plan_proto,
    )?;

    Ok(())
}

fn check_option_plan_eq(
    ctx: &str,
    expected_plan: &Option<String>,
    actual_plan: &Option<String>,
) -> Result<()> {
    match (expected_plan, actual_plan) {
        (Some(expected_plan), Some(actual_plan)) => check_plan_eq(ctx, expected_plan, actual_plan),
        (None, None) => Ok(()),
        (None, Some(_)) => Ok(()),
        (Some(expected_plan), None) => Err(anyhow!(
            "Expected {}:\n{},\nbut failure occurred or no statement executed.",
            ctx,
            *expected_plan
        )),
    }
}

fn check_plan_eq(ctx: &str, expected: &String, actual: &String) -> Result<()> {
    if expected.trim() != actual.trim() {
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
        (Some(e), None) => Err(anyhow!(
            "expected {} error: {}, but there's no error during execution",
            ctx,
            e
        )),
        (Some(l), Some(r)) => {
            let expected_err = l.trim().to_string();
            let actual_err = r.trim().to_string();
            if expected_err == actual_err {
                Ok(())
            } else {
                Err(anyhow!(
                    "Expected {context} error: {}\n  Actual {context} error: {}",
                    expected_err,
                    actual_err,
                    context = ctx
                ))
            }
        }
    }
}

pub async fn run_test_file(file_path: &Path, file_content: &str) -> Result<()> {
    let file_name = file_path.file_name().unwrap().to_str().unwrap();
    println!("-- running {file_name} --");

    let mut failed_num = 0;
    let cases: Vec<TestCase> = serde_yaml::from_str(file_content).map_err(|e| {
        if let Some(loc) = e.location() {
            anyhow!(
                "failed to parse yaml: {e}, at {}:{}:{}",
                file_path.display(),
                loc.line(),
                loc.column()
            )
        } else {
            anyhow!("failed to parse yaml: {e}")
        }
    })?;
    let cases = resolve_testcase_id(cases).expect("failed to resolve");

    for (i, c) in cases.into_iter().enumerate() {
        println!(
            "Running test #{i} (id: {}), SQL:\n{}",
            c.id.clone().unwrap_or_else(|| "<none>".to_string()),
            c.sql
        );
        if let Err(e) = c.run(true).await {
            eprintln!(
                "Test #{i} (id: {}) failed, SQL:\n{}Error: {}",
                c.id.clone().unwrap_or_else(|| "<none>".to_string()),
                c.sql,
                e
            );
            failed_num += 1;
        }
    }
    if failed_num > 0 {
        println!("\n");
        bail!(format!("{} test cases failed", failed_num));
    }
    Ok(())
}
