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

//! Run parser tests from yaml files in `tests/testdata`.
//!
//! Set `UPDATE_PARSER_TEST=1` to update the yaml files.

#![feature(register_tool)]
#![register_tool(rw)]
#![allow(rw::format_error)]

use std::fmt::{Display, Write as _};
use std::fs::File;
use std::io::Write;

use anyhow::{Context, Result, anyhow, bail};
use console::style;
use libtest_mimic::{Arguments, Failed, Trial};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};

/// `TestCase` will be deserialized from yaml.
#[serde_with::skip_serializing_none]
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    pub input: String,
    pub formatted_sql: Option<String>,
    pub error_msg: Option<String>,
    pub formatted_ast: Option<String>,
}

impl Display for TestCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_yaml::to_string(self).unwrap())
    }
}

fn run_test_case(c: TestCase) -> Result<()> {
    let result = Parser::parse_sql(&c.input);
    if let Some(error_msg) = c.error_msg.map(|s| s.trim().to_owned()) {
        if result.is_ok() {
            return Err(anyhow!("Expected failure:\n  {}", error_msg));
        }
        let actual_error_msg = result.unwrap_err().to_string();
        if actual_error_msg != error_msg {
            return Err(anyhow!(
                "Expected error message: {}\n  Actual error message: {}",
                style(&error_msg).green(),
                style(&actual_error_msg).red(),
            ));
        }
    } else {
        if c.formatted_sql.is_none() {
            return Err(anyhow!("Illegal test case without given the formatted sql"));
        }
        if let Err(e) = result {
            return Err(anyhow!("Unexpected failure:\n  {}", e));
        }
        let ast = &result.unwrap()[0];
        let formatted_sql = format!("{}", ast);
        let expected_formatted_sql = c.formatted_sql.as_ref().unwrap();
        if &formatted_sql != expected_formatted_sql {
            return Err(anyhow!(
                "Expected formatted sql:\n  {}\nActual:\n  {}",
                style(&expected_formatted_sql).green(),
                style(&formatted_sql).red(),
            ));
        }
        if let Some(expected_formatted_ast) = c.formatted_ast.map(|s| s.trim().to_owned()) {
            let formatted_ast = format!("{:?}", ast);
            if formatted_ast != expected_formatted_ast {
                return Err(anyhow!(
                    "Expected formatted ast:\n  {}\nActual:\n  {}",
                    style(&expected_formatted_ast).green(),
                    style(&formatted_ast).red(),
                ));
            }
        }
    }
    Ok(())
}

fn run_test_file(file_name: &str) -> Result<()> {
    let file_content = std::fs::read_to_string(file_name).unwrap();
    let cases: Vec<TestCase> =
        serde_yaml::from_str(&file_content).context("failed to parse yaml")?;
    let mut error_msg = String::new();
    for c in cases {
        let input = c.input.clone();
        if let Err(e) = run_test_case(c) {
            write!(&mut error_msg, "With input:\n  {}\n{}\n", input, e)?;
        }
    }
    if !error_msg.is_empty() {
        bail!("{}", error_msg);
    }
    Ok(())
}

fn update_test_file(file_name: &str) -> Result<()> {
    let file_content = std::fs::read_to_string(file_name)?;

    let cases: Vec<TestCase> = serde_yaml::from_str(&file_content)?;

    let mut new_cases = Vec::with_capacity(cases.len());

    for case in cases {
        let input = &case.input;
        let ast = Parser::parse_sql(input);
        let actual_case = match ast {
            Ok(ast) => {
                let [ast]: [Statement; 1] = ast
                    .try_into()
                    .expect("Only one statement is supported now.");

                let actual_formatted_sql = case.formatted_sql.as_ref().map(|_| format!("{}", ast));
                let actual_formatted_ast =
                    case.formatted_ast.as_ref().map(|_| format!("{:?}", ast));

                TestCase {
                    input: input.clone(),
                    formatted_sql: actual_formatted_sql,
                    formatted_ast: actual_formatted_ast,
                    error_msg: None,
                }
            }
            Err(err) => {
                let actual_error_msg = format!("{}", err);
                TestCase {
                    input: input.clone(),
                    formatted_sql: None,
                    formatted_ast: None,
                    error_msg: Some(actual_error_msg),
                }
            }
        };

        if actual_case != case {
            println!("{}\n{}\n", style(&case).red(), style(&actual_case).green())
        }

        new_cases.push(actual_case);
    }

    let mut output_file = File::create(file_name)?;
    writeln!(
        output_file,
        "# This file is automatically generated by `src/sqlparser/tests/parser_test.rs`."
    )?;
    write!(output_file, "{}", serde_yaml::to_string(&new_cases)?)?;
    Ok(())
}

/// The entry point of `test_runner`.
fn main() {
    let run_tests_args = &Arguments::from_args();
    let mut tests = vec![];
    let update = std::env::var("UPDATE_PARSER_TEST").is_ok();
    std::env::set_current_dir("tests/testdata").unwrap();

    use walkdir::WalkDir;
    for entry in WalkDir::new(".") {
        let entry = entry.unwrap();
        if !(entry.path().is_file()) {
            continue;
        }
        if !(entry
            .path()
            .extension()
            .is_some_and(|p| p.eq_ignore_ascii_case("yaml")))
        {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().to_string();
        tests.push(Trial::test(file_name.clone(), move || {
            let f = if update {
                update_test_file
            } else {
                run_test_file
            };
            f(&file_name).map_err(|e| Failed::from(e.to_string()))
        }));
    }

    libtest_mimic::run(run_tests_args, tests).exit();
}
