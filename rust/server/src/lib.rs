#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
// Enable this rule if https://github.com/brendanzab/approx/issues/73 resolved.
#![allow(clippy::if_then_panic)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate risingwave_common;

mod execution;
mod executor;
mod source;
mod storage;
mod stream;
mod stream_op;
mod task;

pub mod rpc;
