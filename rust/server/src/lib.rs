#![allow(dead_code)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(test)]
#![feature(map_first_last)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate risingwave_common;
extern crate test;

mod execution;
mod executor;
mod stream;
mod stream_op;
mod task;

pub mod rpc;
