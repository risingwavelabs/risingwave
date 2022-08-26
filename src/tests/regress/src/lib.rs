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

#![allow(dead_code)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::unused_async)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(path_file_prefix)]

mod opts;

use clap::Parser;
pub(crate) use opts::*;
mod psql;
pub(crate) use psql::*;
mod env;
pub(crate) use env::*;
mod file;
pub(crate) use file::*;
mod schedule;
pub(crate) use schedule::*;
use tracing::{error, info};

/// Exit code of this process
pub async fn regress_main() -> i32 {
    let opts = Opts::parse();

    tracing_subscriber::fmt::init();

    match run_schedules(opts).await {
        Ok(_) => {
            info!("Risingwave regress test completed successfully!");
            0
        }
        Err(e) => {
            error!("Risingwave regress test failed: {:?}. Please ensure that your psql version is larger than 14.1", e);
            1
        }
    }
}

async fn run_schedules(opts: Opts) -> anyhow::Result<()> {
    let schedule = Schedule::new(opts)?;
    schedule.run().await
}
