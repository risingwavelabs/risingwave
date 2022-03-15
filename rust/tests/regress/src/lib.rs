#![allow(dead_code)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
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
use log::{error, info};
pub(crate) use schedule::*;

/// Exit code of this process
pub async fn regress_main() -> i32 {
    let opts = Opts::parse();

    env_logger::init();

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
