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

use std::ffi::OsStr;

use libtest_mimic::{Arguments, Trial};
use risingwave_planner_test::run_test_file;
use tokio::runtime::Runtime;
use walkdir::WalkDir;

fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn main() {
    let run_tests_args = &Arguments::from_args();
    let mut tests = vec![];

    for entry in WalkDir::new("tests/testdata") {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        if (path.extension() == Some(OsStr::new("yml"))
            || path.extension() == Some(OsStr::new("yaml")))
            && !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".apply.yaml")
        {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let test_case_name = file_name.split('.').next().unwrap().to_string();

            tests.push(Trial::test(format!("{test_case_name}_test"), move || {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("testdata")
                    .join(file_name);

                let file_content = std::fs::read_to_string(path).unwrap();
                build_runtime().block_on(run_test_file(&test_case_name, &file_content))?;
                Ok(())
            }));
        }
    }

    if tests.is_empty() {
        panic!("no test case found in planner test!");
    }

    libtest_mimic::run(run_tests_args, tests).exit();
}
