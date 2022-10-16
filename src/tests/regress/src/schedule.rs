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

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, error, info};

use crate::schedule::TestResult::{Different, Same};
use crate::{init_env, DatabaseMode, FileManager, Opts, Psql};

/// Result of each test case.
#[derive(PartialEq)]
enum TestResult {
    /// Execution of the test case succeeded, and results are same.
    Same,
    /// Execution of the test case succeeded, but outputs are different from expected result.
    Different,
}

struct TestCase {
    test_name: String,
    opts: Opts,
    psql: Arc<Psql>,
    file_manager: Arc<FileManager>,
}

pub(crate) struct Schedule {
    opts: Opts,
    file_manager: Arc<FileManager>,
    psql: Arc<Psql>,
    /// Schedules of test names.
    ///
    /// Each item is called a parallel schedule, which runs parallel.
    schedules: Vec<Vec<String>>,
}

impl Schedule {
    pub(crate) fn new(opts: Opts) -> anyhow::Result<Self> {
        Ok(Self {
            opts: opts.clone(),
            file_manager: Arc::new(FileManager::new(opts.clone())),
            psql: Arc::new(Psql::new(opts.clone())),
            schedules: Schedule::parse_from(opts.schedule_file_path())?,
        })
    }

    async fn do_init(self) -> anyhow::Result<Self> {
        init_env();

        self.file_manager.init()?;
        self.psql.init().await?;

        Ok(self)
    }

    fn parse_from<P: AsRef<Path>>(path: P) -> anyhow::Result<Vec<Vec<String>>> {
        let file = File::options()
            .read(true)
            .open(path.as_ref())
            .with_context(|| format!("Failed to open schedule file: {:?}", path.as_ref()))?;

        let reader = BufReader::new(file);
        let mut schedules = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.starts_with("test: ") {
                schedules.push(
                    line[5..]
                        .split_whitespace()
                        .map(ToString::to_string)
                        .collect(),
                );
                debug!("Add one parallel schedule: {:?}", schedules.last().unwrap());
            }
        }

        Ok(schedules)
    }

    /// Run all test schedules.
    ///
    /// # Returns
    ///
    /// `Ok` If no error happens and all outputs are expected,
    /// `Err` If any error happens, or some outputs are unexpected. Details are logged in log file.
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let s = self.do_init().await?;
        s.do_run().await
    }

    async fn do_run(self) -> anyhow::Result<()> {
        let mut different_tests = Vec::new();
        for parallel_schedule in &self.schedules {
            info!("Running parallel schedule: {:?}", parallel_schedule);
            let ret = self
                .run_one_schedule(parallel_schedule.iter().map(String::as_str))
                .await?;

            let mut diff_test = ret
                .iter()
                .filter(|(_test_name, test_result)| **test_result == Different)
                .map(|t| t.0.clone())
                .collect::<Vec<String>>();

            if !diff_test.is_empty() {
                error!(
                    "Parallel schedule failed, these tests are different: {:?}",
                    diff_test
                );
                different_tests.append(&mut diff_test);
            } else {
                info!("Parallel schedule succeeded!");
            }
        }

        if !different_tests.is_empty() {
            info!(
                "Risingwave regress tests failed, these tests are different from expected output: {:?}",
                different_tests
            );
            bail!(
                "Risingwave regress tests failed, these tests are different from expected output: {:?}",
                different_tests
            )
        } else {
            info!("Risingwave regress tests passed.");
            Ok(())
        }
    }

    async fn run_one_schedule(
        &self,
        tests: impl Iterator<Item = &str>,
    ) -> anyhow::Result<HashMap<String, TestResult>> {
        let mut join_handles = HashMap::new();

        for test_name in tests {
            let test_case = self.create_test_case(test_name);
            let join_handle = tokio::spawn(async move { test_case.run().await });
            join_handles.insert(test_name, join_handle);
        }

        let mut result = HashMap::new();

        for (test_name, join_handle) in join_handles {
            let ret = join_handle
                .await
                .with_context(|| format!("Running test case {} panicked!", test_name))??;

            result.insert(test_name.to_string(), ret);
        }

        Ok(result)
    }

    fn create_test_case(&self, test_name: &str) -> TestCase {
        TestCase {
            test_name: test_name.to_string(),
            opts: self.opts.clone(),
            psql: self.psql.clone(),
            file_manager: self.file_manager.clone(),
        }
    }
}

impl TestCase {
    async fn run(self) -> anyhow::Result<TestResult> {
        let host = &self.opts.host();
        let port = &self.opts.port().to_string();
        let database_name = self.opts.database_name();
        let pg_user_name = self.opts.pg_user_name();
        let args: Vec<&str> = vec![
            "-X",
            "-a",
            "-q",
            "-h",
            host,
            "-p",
            port,
            "-d",
            database_name,
            "-U",
            pg_user_name,
            "-v",
            "HIDE_TABLEAM=on",
            "-v",
            "HIDE_TOAST_COMPRESSION=on",
        ];
        println!(
            "Ready to run command:\npsql {}\n for test case:{}",
            args.join(" "),
            self.test_name
        );

        let extra_lines_added_to_input = match self.opts.database_mode() {
            DatabaseMode::Risingwave => {
                vec![
                    "SET RW_IMPLICIT_FLUSH TO true;\n",
                    "SET CREATE_COMPACTION_GROUP_FOR_MV TO true;\n",
                ]
            }
            DatabaseMode::Postgres => vec![],
        };

        let actual_output_path = self.file_manager.output_of(&self.test_name)?;
        let actual_output_file = File::options()
            .create_new(true)
            .write(true)
            .open(&actual_output_path)
            .with_context(|| {
                format!(
                    "Failed to create {:?} for writing output.",
                    actual_output_path
                )
            })?;

        let mut command = Command::new("psql");
        command.env(
            "PGAPPNAME",
            format!("risingwave_regress/{}", self.test_name),
        );
        command.args(args);
        info!(
            "Starting to execute test case: {}, command: {:?}",
            self.test_name, command
        );
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(actual_output_file.try_clone().with_context(|| {
                format!("Failed to clone output file: {:?}", actual_output_path)
            })?)
            .stderr(actual_output_file)
            .spawn()
            .with_context(|| format!("Failed to spawn child for test case: {}", self.test_name))?;

        let child_stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow!("Cannot get the stdin handle of the child process."))?;
        for extra_line in &extra_lines_added_to_input {
            child_stdin.write_all(extra_line.as_bytes()).await?;
        }

        let read_all_lines_from = |path: PathBuf| -> anyhow::Result<String> {
            let input_lines = std::io::BufReader::new(File::open(path)?).lines();
            let mut input_file_content = String::new();
            for line in input_lines {
                input_file_content.push_str(line?.as_str());
                input_file_content.push('\n');
            }
            Ok(input_file_content)
        };

        let input_path = self.file_manager.source_of(&self.test_name)?;
        let input_file_content = read_all_lines_from(input_path)?;
        info!("input_file_content:{}", input_file_content);
        child_stdin.write_all(input_file_content.as_bytes()).await?;

        let status = child.wait().await.with_context(|| {
            format!("Failed to wait for finishing test case: {}", self.test_name)
        })?;

        if !status.success() {
            let error_output = read_all_lines_from(actual_output_path)?;
            let error_msg = format!(
                "Execution of test case {} failed, reason:\n{}",
                self.test_name, error_output
            );
            error!("{}", error_msg);
            bail!(error_msg);
        }

        let expected_output_path = self.file_manager.expected_output_of(&self.test_name)?;
        let expected_output = read_lines(&expected_output_path, 0).with_context(|| {
            format!(
                "Failed to read expected output file: {:?}",
                expected_output_path
            )
        })?;

        // Since we may add some lines to the beginning of each file, and these lines will be
        // part of the actual output. So here we ignore those lines.
        let actual_output = read_lines(&actual_output_path, extra_lines_added_to_input.len())
            .with_context(|| {
                format!(
                    "Failed to read actual output file: {:?}",
                    actual_output_path
                )
            })?;

        if compare_results(&actual_output, &expected_output) {
            Ok(Same)
        } else {
            // If the output is not as expected, we output the diff for easier human reading.
            let diff_path = self.file_manager.diff_of(&self.test_name)?;
            let mut diff_file = File::options()
                .create_new(true)
                .write(true)
                .open(&diff_path)
                .with_context(|| format!("Failed to create {:?} for writing diff.", diff_path))?;
            let (expected_output, actual_output) =
                { (expected_output.join(""), actual_output.join("")) };
            let diffs = format_diff(&expected_output, &actual_output);
            diff_file.write_all(diffs.as_bytes())?;
            error!("Diff:\n{}", diffs);
            Ok(Different)
        }
    }
}

/// Since PG and RW may output the results by different order when there is no `order by`
/// in the SQL. We, by default, interpret the results of a select query by no order, aka we sort
/// the results by ourselves. We remark that we have already filtered out all the comments.
fn compare_results(actual: &[String], expected: &[String]) -> bool {
    if actual.len() != expected.len() {
        error!(
            "actual output has {} lines\nexpected output has {} lines",
            actual.len(),
            expected.len()
        );
        return false;
    }

    let len = actual.len();
    let mut al_start = 0;
    let mut ed_start = 0;
    while al_start < len && ed_start < len {
        // Find the beginning line of a query.
        al_start += actual[al_start..].iter().position(|s| s != "\n").unwrap();
        ed_start += expected[ed_start..].iter().position(|s| s != "\n").unwrap();
        if al_start != ed_start {
            error!(
                "Different starts:\nactual:{:?}\nexpected:{:?}",
                actual[al_start], expected[ed_start],
            );
            return false;
        }
        // Find the empty line after the ending line of a query.
        let al_end = al_start
            + actual[al_start..]
                .iter()
                .position(|s| s == "\n")
                .unwrap_or(len - al_start);
        let ed_end = ed_start
            + expected[ed_start..]
                .iter()
                .position(|s| s == "\n")
                .unwrap_or(len - ed_start);
        if al_end != ed_end {
            error!(
                "Different number of lines:\nactual:{:?}\nexpected:{:?}",
                actual[al_start..al_end].to_vec(),
                expected[ed_start..ed_end].to_vec(),
            );
            return false;
        }
        let actual_query = &actual[al_start..al_end];
        let expected_query = &expected[ed_start..ed_end];
        if !compare_query(actual_query, expected_query) {
            error!("actual_query:{:?}", actual_query);
            error!("expected_query:{:?}", expected_query);
            return false;
        }
        al_start = al_end + 1;
        ed_start = ed_end + 1;
    }
    true
}

/// This function accepts a raw line instead of a line trimmed at the end.
fn is_comment(line: &str) -> bool {
    // We assume that the line indicating the result set of a select query must have more than two
    // `-`.
    line.starts_with("-- ") || line == "--\n"
}

/// We first determine whether this query is a select query, i.e. returning results.
/// Then we determine whether it contains the `order by` keywords.
/// Finally, we determine which lines contain the results and thus need to be actually compared,
/// with/without order.
///
/// The caller should make sure that these two inputs have the same length.
fn compare_query(actual: &[String], expected: &[String]) -> bool {
    assert_eq!(actual.len(), expected.len());
    let len = actual.len();
    let is_select = actual[0].clone().to_lowercase().starts_with("select");
    if is_select {
        let mut is_order_by = false;
        let mut result_line_idx = usize::MAX;
        for (idx, line) in actual.iter().enumerate() {
            if is_comment(line) {
                continue;
            } else {
                // This could fail in the corner case.
                // For example, a subquery has a `order by` clause, which should not be allowed and
                // is meaningless as only the outermost `order by` clause is effective.
                if line.to_lowercase().contains("order by") {
                    is_order_by = true;
                }
                // The separator line used to show the result set
                // We remark that the comment should start with only two "--".
                if line.starts_with("---") {
                    result_line_idx = idx;
                    break;
                }
            }
        }
        // The select query must have a line indicating result set.
        assert_ne!(result_line_idx, usize::MAX);
        if !expected[result_line_idx].starts_with("---") {
            return false;
        }
        if actual[..result_line_idx] != expected[..result_line_idx] {
            return false;
        }
        // The output must contain at least one row that specifies how many rows in total are
        // returned from the database, thus we directly unwrap here.
        if actual.last().unwrap() != expected.last().unwrap() {
            return false;
        }
        if is_order_by {
            actual[result_line_idx + 1..] == expected[result_line_idx + 1..]
        } else {
            let mut actual_sorted = actual[result_line_idx + 1..len].to_vec();
            actual_sorted.sort();
            let mut expected_sorted = expected[result_line_idx + 1..len].to_vec();
            expected_sorted.sort();
            actual_sorted == expected_sorted
        }
    } else {
        actual == expected
    }
}

/// This function ignores the comments and empty lines. They are not compared between
/// expected output file and actual output file.
fn read_lines<P>(filename: P, mut skip: usize) -> std::io::Result<Vec<String>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let mut input = std::io::BufReader::new(file);
    let mut res = vec![];
    let mut last_line_is_empty = false;
    let mut last_is_select = false;
    let mut line = String::new();
    // This gives a line without being trimmed at the end.
    while input
        .read_line(&mut line)
        .expect("reading from cursor won't fail")
        != 0
    {
        let line = std::mem::take(&mut line);
        if is_comment(&line) {
            last_line_is_empty = false;
            last_is_select = false;
            continue;
        } else if line == "\n" {
            // Multiple empty lines are combined into one empty line only.
            // This is for the ease of comparing output file by strings directly.
            last_line_is_empty = true;
            last_is_select = false;
        } else if skip == 0 {
            if line.to_lowercase().starts_with("select") {
                last_is_select = true;
            }
            if last_line_is_empty {
                res.push("\n".to_string());
                last_line_is_empty = false;
            }
            res.push(line.clone());
            if line.ends_with(";\n") {
                if !last_is_select {
                    // We manually add a new line at the end of a select query
                    // to determine where the result set ends.
                    res.push("\n".to_string());
                    last_line_is_empty = true;
                }
                last_is_select = false;
            }
        } else {
            skip -= 1;
        }
    }
    Ok(res)
}

fn format_diff(expected_output: &String, actual_output: &String) -> String {
    use std::fmt::Write;

    use similar::{ChangeTag, TextDiff};
    let diff = TextDiff::from_lines(expected_output, actual_output);

    let mut diff_str = "".to_string();
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        write!(diff_str, "{}{}", sign, change).unwrap();
    }
    diff_str
}
