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

mod opts;
mod test;
pub use opts::Opts;
use test::TestSuite;
use tracing::{error, info};

pub async fn run_test_suit(
    db_name: String,
    user_name: String,
    server_host: String,
    server_port: u16,
    password: String,
) -> i32 {
    let test_suite = TestSuite::new(db_name, user_name, server_host, server_port, password);

    match test_suite.test().await {
        Ok(_) => {
            info!("Risingwave e2e extended mode test completed successfully!");
            0
        }
        Err(e) => {
            error!("Risingwave e2e extended mode test failed: {:?}. Please ensure that your psql version is larger than 14.1", e);
            1
        }
    }
}
