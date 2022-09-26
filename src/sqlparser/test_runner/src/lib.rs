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

// Data-driven tests.

use anyhow::{anyhow, Result};
use risingwave_sqlparser::parser::Parser;
use serde::Deserialize;

/// `TestCase` will be deserialized from yaml.
#[derive(PartialEq, Eq, Debug, Deserialize)]
struct TestCase {
    input: String,
    formatted_sql: Option<String>,
    error_msg: Option<String>,
    formatted_ast: Option<String>,
}

fn run_test_case(c: TestCase) -> Result<()> {
    let result = Parser::parse_sql(&c.input);
    if let Some(error_msg) = c.error_msg.map(|s| s.trim().to_string()) {
        if result.is_ok() {
            return Err(anyhow!("Expected failure:\n  {}", error_msg));
        }
        let actual_error_msg = result.unwrap_err().to_string();
        if actual_error_msg != error_msg {
            return Err(anyhow!(
                "Expected error message: {}\n  Actual error message: {}",
                error_msg,
                actual_error_msg
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
                "Expected formatted sql:\n  {}\n  Actual formatted sql:\n  {}",
                expected_formatted_sql,
                formatted_sql
            ));
        }
        if let Some(expected_formatted_ast) = c.formatted_ast.map(|s| s.trim().to_string()) {
            let formatted_ast = format!("{:?}", ast);
            if formatted_ast != expected_formatted_ast {
                return Err(anyhow!(
                    "Expected formatted ast:\n  {}\n  Actual formatted ast:\n  {}",
                    expected_formatted_ast,
                    formatted_ast
                ));
            }
        }
    }
    Ok(())
}

fn run_test_file(file_name: &str, file_content: &str) {
    let cases: Vec<TestCase> = serde_yaml::from_str(file_content)
        .unwrap_or_else(|_| panic!("Failed to parse {}", file_name));
    let mut failed_num = 0;
    for c in cases {
        let input = c.input.clone();
        if let Err(e) = run_test_case(c) {
            println!(
                "\nThe input SQL from file {}:\n  {}\n{}",
                file_name, input, e
            );
            failed_num += 1;
        }
    }
    if failed_num > 0 {
        println!("\n");
        panic!("{} test cases failed", failed_num);
    }
}

/// The entry point of `test_runner`.
pub fn run_all_test_files() {
    use walkdir::WalkDir;
    for entry in WalkDir::new("../tests/testdata/") {
        let entry = entry.unwrap();
        if !entry.path().is_file() {
            continue;
        }
        let file_content = std::fs::read_to_string(entry.path()).unwrap();
        run_test_file(entry.path().to_str().unwrap(), &file_content);
    }
}
