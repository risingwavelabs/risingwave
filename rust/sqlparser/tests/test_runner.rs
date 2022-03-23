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
use itertools::Itertools;
use risingwave_sqlparser::parser::Parser;

// 1. The input sql.
// 2. ---
// 3. If sql parsing succeeds, the line is the formatted sql.
//    Otherwise, it is the error message.
// 4. => No exist if the parsing is expected to fail.
// 5. The formatted ast.
#[derive(PartialEq, Eq, Debug)]
struct TestCase {
    input: String,
    formatted_sql: Option<String>,
    error_msg: Option<String>,
    formatted_ast: Option<String>,
}

fn parse_test_case(case_str: &str) -> TestCase {
    let bulks: Vec<String> = case_str
        .lines()
        .collect_vec()
        .split(|l| *l == "---")
        .map(|slice| slice.join("\n"))
        .collect();
    assert_eq!(bulks.len(), 2);
    let input = bulks[0].to_string();
    let remain = bulks[1].to_string();
    let bulks: Vec<String> = remain
        .lines()
        .collect_vec()
        .split(|l| *l == "=>")
        .map(|slice| slice.join("\n"))
        .collect();
    if bulks.len() == 1 {
        TestCase {
            input,
            error_msg: Some(bulks[0].to_string()),
            formatted_sql: None,
            formatted_ast: None,
        }
    } else {
        assert_eq!(bulks.len(), 2);
        TestCase {
            input,
            error_msg: None,
            formatted_sql: Some(bulks[0].to_string()),
            formatted_ast: Some(bulks[1].to_string()),
        }
    }
}

fn run_test_case(c: &TestCase) -> Result<()> {
    let result = Parser::parse_sql(&c.input);
    if let Some(error_msg) = &c.error_msg {
        if result.is_ok() {
            return Err(anyhow!("Expected failure:\n  {}", error_msg));
        }
        let actual_error_msg = result.unwrap_err().to_string();
        if &actual_error_msg != error_msg {
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
        if c.formatted_ast.is_none() {
            return Err(anyhow!("Illegal test case without given the formatted ast"));
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
        let formatted_ast = format!("{:?}", ast);
        let expected_formatted_ast = c.formatted_ast.as_ref().unwrap();
        if &formatted_ast != expected_formatted_ast {
            return Err(anyhow!(
                "Expected formatted ast: {}\n  Actual formatted ast: {}",
                expected_formatted_ast,
                formatted_ast
            ));
        }
    }
    Ok(())
}

fn run_test_file(file_name: &str, file_content: &str) {
    let file_content = remove_comments(file_content);
    let cases = split_test_cases(&file_content);
    let mut failed_num = 0;
    for case_str in cases {
        let c = parse_test_case(&case_str);
        if let Err(e) = run_test_case(&c) {
            println!(
                "\nThe input SQL from file {}:\n  {}\n{}",
                file_name, c.input, e
            );
            failed_num += 1;
        }
    }
    if failed_num > 0 {
        println!("\n");
        panic!("{} test cases failed", failed_num);
    }
}

/// Remove the '#' commented parts.
fn remove_comments(file_content: &str) -> String {
    let mut lines = file_content
        .lines()
        .map(|l| l.trim())
        .filter(|line| !line.starts_with('#'))
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    lines.iter_mut().for_each(|line| {
        if let Some(pos) = line.find('#') {
            line.truncate(pos);
        }
    });
    lines.join("\n")
}

// The test-cases are separated by a blank line.
fn split_test_cases(file_content: &str) -> Vec<String> {
    file_content
        .lines()
        .map(|l| l.trim().to_string())
        .collect_vec()
        .split(|l| l.is_empty())
        .map(|slice| slice.join("\n"))
        .filter(|case| !case.is_empty())
        .collect_vec()
}

// Traverses the 'testdata/' directory and runs all files.
#[test]
fn run_all_test_files() {
    use walkdir::WalkDir;
    for entry in WalkDir::new("./tests/testdata/") {
        let entry = entry.unwrap();
        if !entry.path().is_file() {
            continue;
        }
        let file_content = std::fs::read_to_string(entry.path()).unwrap();
        run_test_file(entry.path().to_str().unwrap(), &file_content);
    }
}

#[test]
fn test_remove_comments() {
    assert_eq!(
        remove_comments("### line 1\nline 2###comments\nline 3"),
        "line 2\nline 3"
    );
}

#[test]
fn test_split_test_cases() {
    assert_eq!(
        split_test_cases("line 1\n   \n  \nline 2\nline 3"),
        vec!["line 1".to_string(), "line 2\nline 3".to_string(),]
    );
}

#[test]
fn test_parse_test_case() {
    assert_eq!(
        parse_test_case("abc\n---\nsyntax error"),
        TestCase {
            input: "abc".to_string(),
            formatted_sql: None,
            error_msg: Some("syntax error".to_string()),
            formatted_ast: None
        }
    );
    assert_eq!(
        parse_test_case("abc\n---\nabc\n=>\nSql { abc }"),
        TestCase {
            input: "abc".to_string(),
            formatted_sql: Some("abc".to_string()),
            error_msg: None,
            formatted_ast: Some("Sql { abc }".to_string()),
        }
    );
}
