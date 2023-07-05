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

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
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

// Test queries commented out with `--@ ` are ignored for comparison.
// Unlike normal comment `--`, this does not require modification to expected output file.
// We can simplify toggle the case on/off by just updating the input sql file.
const PREFIX_IGNORE: &str = "--@ ";

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
                    "SET QUERY_MODE TO LOCAL;\n",
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

        let read_all_lines_from = std::fs::read_to_string;

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

        let input_lines = input_file_content
            .lines()
            .filter(|s| !s.is_empty() && *s != PREFIX_IGNORE);
        let mut expected_lines = std::io::BufReader::new(File::open(expected_output_path)?)
            .lines()
            .map(|s| s.unwrap())
            .filter(|s| !s.is_empty());
        let mut actual_lines = std::io::BufReader::new(File::open(actual_output_path)?)
            .lines()
            .skip(extra_lines_added_to_input.len())
            .map(|s| s.unwrap())
            .filter(|s| !s.is_empty() && s != PREFIX_IGNORE);

        // We split the output lines (either expected or actual) based on matching lines from input.
        // For example:
        //     input      |    output
        // --+------------+--+-----------------------------------
        //  1| select v1  | 1| select v1
        //  2| from t;    | 2| from t;
        //   |            | 3|  v1
        //   |            | 4| ----
        //   |            | 5|   1
        //   |            | 6|   2
        //   |            | 7| (2 rows)
        //   |            | 8|
        //  3| select v1; | 9| select v1;
        //   |            |10| ERROR:  column "v1" does not exist
        //   |            |11| LINE 1: select v1;
        //   |            |12|                ^
        //
        // We would split the output lines into 2 chunks:
        // * query 1..=2 and output  3..= 7
        // * query 9..=9 and output 10..=12
        let mut is_diff = false;
        let mut pending_input = vec![];
        for input_line in input_lines {
            let original_input_line = input_line.strip_prefix(PREFIX_IGNORE).unwrap_or(input_line);

            // Find the matching output line, and collect lines before the next matching line.
            let mut expected_output = vec![];
            while let Some(line) = expected_lines.next() && line != original_input_line {
                expected_output.push(line);
            }

            let mut actual_output = vec![];
            while let Some(line) = actual_lines.next() && line != input_line {
                actual_output.push(line);
            }

            // If no unmatched lines skipped, this input line belongs to the same query as previous
            // lines.
            if expected_output.is_empty() && actual_output.is_empty() {
                pending_input.push(input_line);
                continue;
            }

            let query_input = std::mem::replace(&mut pending_input, vec![input_line]);

            is_diff = !compare_output(&query_input, &expected_output, &actual_output) || is_diff;
        }
        // There may be more lines after the final matching lines.
        let expected_output: Vec<_> = expected_lines.collect();
        let actual_output: Vec<_> = actual_lines.collect();
        is_diff = !compare_output(&pending_input, &expected_output, &actual_output) || is_diff;

        Ok(if is_diff { Different } else { Same })
    }
}

fn compare_output(query: &[&str], expected: &[String], actual: &[String]) -> bool {
    let compare_lines = |expected: &[String], actual: &[String]| {
        let eq = expected == actual;
        if !eq {
            error!("query input:\n{}", query.join("\n"));

            let (expected_output, actual_output) = (expected.join("\n"), actual.join("\n"));
            let diffs = format_diff(&expected_output, &actual_output);
            error!("Diff:\n{}", diffs);
        }
        eq
    };

    if let Some(l) = query.last() && l.starts_with(PREFIX_IGNORE) {
        return true;
    }
    if !expected.is_empty()
        && !actual.is_empty()
        && expected[0].starts_with("ERROR:  ")
        && actual[0].starts_with("ERROR:  ")
    {
        return true;
    }

    let is_select = query
        .iter()
        .any(|line| line.to_lowercase().starts_with("select"));
    if !is_select {
        // no special handling when we do not recognize it
        return compare_lines(expected, actual);
    }

    // This could fail in the corner case.
    // For example, a subquery has a `order by` clause, which should not be allowed and
    // is meaningless as only the outermost `order by` clause is effective.
    let is_order_by = query
        .iter()
        .any(|line| line.to_lowercase().contains("order by"));

    // The output of `select` is assumed to have the following format:
    //
    // line of output column names
    // line of separator starting with "---" (comment assumed to start with only two "--".)
    // x lines of output rows
    // line of summary "(x rows)"
    //
    // Note that zero column output (`select;`) has no title line and only 2 hyphens. This would not
    // be recognized and thus compared as-is without special treatment, which is expected.
    // --
    // (x rows)

    if expected.len() < 3
        || !expected[1].starts_with("---")
        || !matches!(expected.last(), Some(l) if l.ends_with(" rows)") || l == "(1 row)")
        || actual.len() < 3
        || !actual[1].starts_with("---")
        || !matches!(actual.last(), Some(l) if l.ends_with(" rows)") || l == "(1 row)")
    {
        // no special handling when we do not recognize it
        return compare_lines(expected, actual);
    }

    if is_order_by {
        compare_lines(expected, actual)
    } else {
        let mut expected_sorted = expected.to_vec();
        expected_sorted[2..expected.len() - 1].sort();
        let mut actual_sorted = actual.to_vec();
        actual_sorted[2..actual.len() - 1].sort();

        compare_lines(&expected_sorted, &actual_sorted)
    }
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
