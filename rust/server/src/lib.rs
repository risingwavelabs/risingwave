#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(test)]
#![feature(map_first_last)]

#[macro_use]
extern crate log;
extern crate risingwave_common;
extern crate test;
#[macro_use]
extern crate risingwave_storage;

mod execution;
mod executor;
mod source;
mod stream;
mod stream_op;
mod task;

pub mod rpc;
