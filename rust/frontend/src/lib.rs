#![feature(map_try_insert)]

#[macro_use]
pub mod catalog;
pub mod binder;
pub mod expr;
pub mod handler;
pub mod optimizer;
pub mod planner;
pub mod session;

extern crate log;
#[cfg(test)]
mod test_utils;

use clap::Parser;
#[derive(Parser, Clone)]
pub struct FrontendOpts {
    #[clap(long, default_value = "127.0.0.1:4566")]
    pub host: String,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_addr: String,
}
