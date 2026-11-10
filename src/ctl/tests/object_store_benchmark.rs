// Copyright 2026 RisingWave Labs
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

#![cfg(not(madsim))]

use clap::Parser;
use risingwave_ctl::CliOpts;
use risingwave_ctl::cmd_impl::bench::object_store::{ObjectStoreArgs, run};

#[derive(Parser)]
struct Standalone {
    #[command(flatten)]
    args: ObjectStoreArgs,
}

#[test]
fn parses_sizes_profiles_and_root_command() {
    let args = Standalone::try_parse_from([
        "test",
        "--store",
        "s3://bucket",
        "--object-size",
        "256MiB",
        "--short-get-size",
        "64KiB",
        "--basic-duration-ms",
        "0",
        "--basic-max-operations",
        "500",
        "--profile",
        "compactor",
        "--trace-output",
        "reads.jsonl",
        "--reuse-prefix",
        "risectl-bench/retained/",
    ])
    .unwrap()
    .args;
    assert_eq!(
        args.trace_output.unwrap(),
        std::path::PathBuf::from("reads.jsonl")
    );
    assert_eq!(args.object_size, Some(256 << 20));
    assert_eq!(args.short_get_size, Some(64 << 10));
    assert_eq!(args.basic_duration_ms, Some(0));
    assert_eq!(args.basic_max_operations, Some(500));
    assert!(
        CliOpts::try_parse_from([
            "risectl",
            "bench",
            "object-store",
            "--store",
            "s3://bucket",
            "--backend",
            "opendal"
        ])
        .is_ok()
    );
    assert!(
        Standalone::try_parse_from(["test", "--object-size", "18446744073709551615GiB"]).is_err()
    );
    assert!(Standalone::try_parse_from(["test", "--profile", "typo"]).is_err());
}

#[tokio::test]
async fn invalid_parameters_fail_before_client_initialization() {
    let args = Standalone::try_parse_from(["test", "--store", "s3://bucket", "--concurrency", "0"])
        .unwrap()
        .args;
    let error = run(args, Default::default()).await.unwrap_err();
    assert!(error.to_string().contains("positive"));
    let args = Standalone::try_parse_from(["test", "--store", "memory"])
        .unwrap()
        .args;
    let error = run(args, Default::default()).await.unwrap_err();
    assert!(error.to_string().contains("S3/MinIO"));
}

#[tokio::test]
async fn invalid_report_path_fails_before_client_initialization() {
    let directory = tempfile::tempdir().unwrap();
    for path in [
        directory.path().join("missing/result.json"),
        directory.path().to_path_buf(),
    ] {
        let args = Standalone::try_parse_from([
            "test",
            "--store",
            "s3://bucket",
            "--output",
            path.to_str().unwrap(),
        ])
        .unwrap()
        .args;
        let error = run(args, Default::default()).await.unwrap_err();
        assert!(error.to_string().contains("report"), "{error:#}");
    }
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn output_preparation_preserves_existing_report_on_setup_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("result.json");
    std::fs::write(&path, "previous report").unwrap();
    let args = Standalone::try_parse_from([
        "test",
        "--store",
        "memory",
        "--output",
        path.to_str().unwrap(),
    ])
    .unwrap()
    .args;
    let error = run(args, Default::default()).await.unwrap_err();
    assert!(error.to_string().contains("S3/MinIO"));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "previous report");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn trace_paths_are_checked_before_client_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let existing = directory.path().join("existing.jsonl");
    std::fs::write(&existing, "previous trace").unwrap();
    for path in [
        existing.clone(),
        directory.path().join("missing/trace.jsonl"),
    ] {
        let args = Standalone::try_parse_from([
            "test",
            "--store",
            "s3://bucket",
            "--trace-output",
            path.to_str().unwrap(),
        ])
        .unwrap()
        .args;
        let error = run(args, Default::default()).await.unwrap_err();
        assert!(error.to_string().contains("trace"), "{error:#}");
    }
    assert_eq!(std::fs::read_to_string(existing).unwrap(), "previous trace");
    let same = directory.path().join("same.json");
    let args = Standalone::try_parse_from([
        "test",
        "--store",
        "s3://bucket",
        "--trace-output",
        same.to_str().unwrap(),
        "--output",
        same.to_str().unwrap(),
    ])
    .unwrap()
    .args;
    let error = run(args, Default::default()).await.unwrap_err();
    assert!(error.to_string().contains("different files"), "{error:#}");
}

#[test]
fn parses_pause_schedule() {
    let args =
        Standalone::try_parse_from(["test", "--pause-every-chunks", "16", "--max-pauses", "1"])
            .unwrap()
            .args;
    assert_eq!(args.pause_every_chunks, Some(16));
    assert_eq!(args.max_pauses, Some(1));
    let args = Standalone::try_parse_from(["test"]).unwrap().args;
    assert_eq!(args.max_pauses, None);
    assert_eq!(args.pause_every_chunks, None);
}

#[tokio::test]
async fn pause_parameters_validate_and_cli_overrides_json_before_client_setup() {
    let args =
        Standalone::try_parse_from(["test", "--store", "memory", "--pause-every-chunks", "0"])
            .unwrap()
            .args;
    let error = run(args, Default::default()).await.unwrap_err();
    assert!(
        error.to_string().contains("pause_every_chunks"),
        "{error:#}"
    );
    let directory = tempfile::tempdir().unwrap();
    let parameters = directory.path().join("params.json");
    std::fs::write(&parameters, r#"{"pause_every_chunks":0,"max_pauses":1}"#).unwrap();
    let args = Standalone::try_parse_from([
        "test",
        "--store",
        "memory",
        "--parameters",
        parameters.to_str().unwrap(),
        "--pause-every-chunks",
        "2",
        "--max-pauses",
        "0",
    ])
    .unwrap()
    .args;
    let error = run(args, Default::default()).await.unwrap_err();
    // All config checks passed; the CLI rejects this URL before client setup.
    assert!(error.to_string().contains("S3/MinIO"), "{error:#}");
}
