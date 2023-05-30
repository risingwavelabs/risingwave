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

use std::process::exit;

use clap::Parser;
use risingwave_e2e_extended_mode_test::{run_test_suit, Opts};

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    let opts = Opts::parse();

    tracing_subscriber::fmt::init();

    exit(
        run_test_suit(
            opts.pg_db_name,
            opts.pg_user_name,
            opts.pg_server_host,
            opts.pg_server_port,
            opts.pg_password,
        )
        .await,
    )
}
