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
        "--consume-size",
        "64KiB",
        "--basic-duration-ms",
        "0",
        "--basic-max-operations",
        "500",
        "--profile",
        "compactor",
        "--reuse-prefix",
        "risectl-bench/retained/",
    ])
    .unwrap()
    .args;
    assert_eq!(args.object_size, Some(256 << 20));
    assert_eq!(args.consume_size, Some(64 << 10));
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
