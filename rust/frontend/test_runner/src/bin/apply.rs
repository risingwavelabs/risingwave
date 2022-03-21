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
//
use std::ffi::OsStr;
use std::path::Path;

use anyhow::Result;
use risingwave_frontend_test_runner::TestCase;

#[tokio::main]
async fn main() -> Result<()> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let dir = Path::new(manifest_dir).join("tests").join("testdata");
    println!("Using test cases from {:?}", dir);

    use walkdir::WalkDir;
    for entry in WalkDir::new(dir) {
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
            let target = path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .split('.')
                .next()
                .unwrap()
                .to_string()
                + ".apply.yaml";

            println!(".. {:?} -> {:?}", entry.file_name(), target);

            let file_content = tokio::fs::read_to_string(path).await?;
            let cases: Vec<TestCase> = serde_yaml::from_str(&file_content)?;
            let mut updated_cases = vec![];

            for case in cases {
                let result = case.run(false).await?;
                let updated_case = result.as_test_case(&case.sql);
                updated_cases.push(updated_case);
            }

            let contents = serde_yaml::to_string(&updated_cases)?;

            tokio::fs::write(path.parent().unwrap().join(target), &contents).await?;
        }
    }

    Ok(())
}
