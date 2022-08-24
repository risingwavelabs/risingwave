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
use std::path::Path;
use std::thread::available_parallelism;

use anyhow::{anyhow, Context, Result};
use console::style;
use futures::StreamExt;
use risingwave_frontend_test_runner::{resolve_testcase_id, TestCase};

#[tokio::main]
async fn main() -> Result<()> {
    std::panic::set_hook(Box::new(move |e| {
        println!(
            "{}{}{}{}{}\n{e}",
            style("ERROR: ").red().bold(),
            style("apply-planner-test").yellow(),
            style(" panicked! Try ").red().bold(),
            style("run-planner-test --no-fail-fast").yellow(),
            style(" to find which test case panicked.").red().bold()
        );
        std::process::abort();
    }));

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let dir = Path::new(manifest_dir).join("tests").join("testdata");
    println!("Using test cases from {:?}", dir);

    let mut futures = vec![];

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

            let path = path.to_path_buf();
            let filename = entry.file_name().to_os_string();

            futures.push(async move {
                let func = async {
                    let file_content = tokio::fs::read_to_string(&path).await?;
                    let cases: Vec<TestCase> = serde_yaml::from_str(&file_content)?;
                    let cases = resolve_testcase_id(cases)?;
                    let mut updated_cases = vec![];

                    for (idx, case) in cases.into_iter().enumerate() {
                        let case_desc = format!(
                            "failed on case #{} (id: {})",
                            idx,
                            case.id.clone().unwrap_or_else(|| "<none>".to_string())
                        );
                        let result = case.run(false).await.context(case_desc.clone())?;
                        let updated_case = result.as_test_case(&case).context(case_desc)?;
                        updated_cases.push(updated_case);
                    }

                    let contents = serde_yaml::to_string(&updated_cases)?;

                    tokio::fs::write(path.parent().unwrap().join(&target), &contents).await?;

                    Ok::<_, anyhow::Error>(())
                };

                match func.await {
                    Ok(_) => {
                        println!(
                            "{} {} -> {}",
                            style("success").green().bold(),
                            filename.to_string_lossy(),
                            target,
                        );
                        true
                    }
                    Err(err) => {
                        println!(
                            "{} {} \n        {:#}",
                            style(" failed").red().bold(),
                            filename.to_string_lossy(),
                            err
                        );
                        false
                    }
                }
            });
        }
    }

    let result = futures::stream::iter(futures)
        .buffer_unordered(
            available_parallelism()
                .map(|x| x.get())
                .unwrap_or_default()
                .max(2),
        )
        .collect::<Vec<_>>()
        .await
        .iter()
        .all(|x| *x);

    if result {
        Ok(())
    } else {
        Err(anyhow!("some test case failed"))
    }
}
