#![feature(map_try_insert)]

#[macro_use]
pub mod catalog;
pub mod expr;
pub mod optimizer;
pub mod pgwire;
extern crate log;
