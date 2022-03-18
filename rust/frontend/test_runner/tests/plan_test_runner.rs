use std::ffi::OsStr;

/// Traverses the 'testdata/' directory and runs all files.
/// This is the entry point of `plan_test_runner`.
#[tokio::test]
async fn run_all_test_files() {
    use walkdir::WalkDir;
    let mut any_file = false;
    for entry in WalkDir::new("./tests/testdata/") {
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
            let file_content = tokio::fs::read_to_string(path).await.unwrap();
            risingwave_frontend_test_runner::run_test_file(path.to_str().unwrap(), &file_content)
                .await;
            any_file = true;
        }
    }
    if !any_file {
        panic!("no test found!");
    }
}
