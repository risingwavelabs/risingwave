#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
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
    log4rs::init_file(opts.log4rs_config_path(), Default::default()).unwrap();

    match run_schedules(opts).await {
        Ok(_) => {
            info!("Risingwave regress test completed successfully!");
            0
        }
        Err(e) => {
            error!("Risingwave regress test failed: {:?}", e);
            1
        }
    }
}

async fn run_schedules(opts: Opts) -> anyhow::Result<()> {
    let schedule = Schedule::new(opts)?;
    schedule.run().await
}
