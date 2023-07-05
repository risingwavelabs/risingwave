// Copyright 2023 RisingWave Labs
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

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use console::style;
use futures::future::try_join_all;
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use risingwave_sqlparser_test_runner::TestCase;
use walkdir::WalkDir;

#[tokio::main]
async fn main() -> Result<()> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let dir = Path::new(manifest_dir)
        .parent()
        .unwrap()
        .join("tests")
        .join("testdata");
    println!("Using test cases from {:?}", dir);

    let mut futs = vec![];

    let log_lock = Arc::new(Mutex::new(()));

    for entry in WalkDir::new(dir) {
        let entry = entry.unwrap();
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if path.is_file()
            && path.extension().map_or(false, |p| {
                p.eq_ignore_ascii_case("yml") || p.eq_ignore_ascii_case("yaml")
            })
            && !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".apply.yaml")
        {
            let target = path.with_extension("apply.yaml");

            let path = path.to_path_buf();
            let log_lock = Arc::clone(&log_lock);
            futs.push(async move {
                let file_content = tokio::fs::read_to_string(&path).await?;

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

                            let actual_formatted_sql =
                                case.formatted_sql.as_ref().map(|_| format!("{}", ast));
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
                        let _guard = log_lock.lock();
                        println!("{}\n{}\n", style(&case).red(), style(&actual_case).green())
                    }

                    new_cases.push(actual_case);
                }

                let output_content = serde_yaml::to_string(&new_cases)?;

                tokio::fs::write(target, output_content).await?;

                Ok::<_, anyhow::Error>(())
            });
        }
    }

    let _res = try_join_all(futs).await?;

    Ok(())
}
