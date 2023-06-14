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

#![cfg_attr(coverage, feature(no_coverage))]

use risingwave_backup::error::BackupResult;

#[cfg_attr(coverage, no_coverage)]
fn main() -> BackupResult<()> {
    use clap::Parser;
    let opts = risingwave_meta::backup_restore::RestoreOpts::parse();
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new("backup_restore"));
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(risingwave_meta::backup_restore::restore(opts))
}
